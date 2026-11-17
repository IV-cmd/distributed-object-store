#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <set>

#include "client/client.h"
#include "common/chunk.h"
#include "fst/service.h"
#include "mgm/service.h"

namespace objectstore {
namespace {

namespace fs = std::filesystem;

class BlockingTarget final : public v1::FstService::Service {
 public:
  explicit BlockingTarget(FstService& store) : store_(store) {}

  void Block() {
    const std::lock_guard lock(mutex_);
    blocked_ = true;
    entered_ = false;
  }
  bool Entered() {
    std::unique_lock lock(mutex_);
    return wake_.wait_for(lock, std::chrono::seconds(5), [&] { return entered_; });
  }
  void Release() {
    const std::lock_guard lock(mutex_);
    blocked_ = false;
    wake_.notify_all();
  }
  grpc::Status InstallChunk(grpc::ServerContext* context,
                             const v1::InstallChunkRequest* request,
                             google::protobuf::Empty* response) override {
    {
      std::unique_lock lock(mutex_);
      entered_ = true;
      wake_.notify_all();
      wake_.wait(lock, [&] { return !blocked_; });
    }
    return store_.InstallChunk(context, request, response);
  }
  grpc::Status ReadChunk(grpc::ServerContext* context,
                          const v1::ReadChunkRequest* request,
                          v1::ReadChunkResponse* response) override {
    return store_.ReadChunk(context, request, response);
  }
  grpc::Status CopyChunk(grpc::ServerContext* context,
                          const v1::CopyChunkRequest* request,
                          google::protobuf::Empty* response) override {
    return store_.CopyChunk(context, request, response);
  }

 private:
  FstService& store_;
  std::mutex mutex_;
  std::condition_variable wake_;
  bool blocked_ = false;
  bool entered_ = false;
};

class RepairTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::temp_directory_path() /
        ("objectstore-repair-" + std::to_string(
             HealthClock::now().time_since_epoch().count()));
    fs::create_directories(root_);
    int port = 0;
    grpc::ServerBuilder manager;
    manager.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    manager.RegisterService(&mgm_);
    manager_ = manager.BuildAndStart();
    ASSERT_NE(manager_, nullptr);
    client_ = std::make_unique<ReplicatedClient>(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    for (std::size_t index = 0; index < stores_.size(); ++index) {
      stores_[index] = std::make_unique<FstService>(Dir(index));
      grpc::ServerBuilder builder;
      builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
      if (index == 3) {
        target_ = std::make_unique<BlockingTarget>(*stores_[index]);
        builder.RegisterService(target_.get());
      } else {
        builder.RegisterService(stores_[index].get());
      }
      servers_[index] = builder.BuildAndStart();
      ASSERT_NE(servers_[index], nullptr);
      nodes_[index].set_node_id("fst" + std::to_string(index + 1));
      nodes_[index].set_endpoint("127.0.0.1:" + std::to_string(port));
      v1::RegisterNodeRequest request;
      *request.mutable_node() = nodes_[index];
      v1::RegisterNodeResponse response;
      ASSERT_TRUE(mgm_.RegisterNode(nullptr, &request, &response).ok());
    }
  }
  void TearDown() override {
    if (target_) {
      target_->Release();
    }
    for (auto& server : servers_) {
      if (server) {
        server->Shutdown();
        server->Wait();
      }
    }
    if (manager_) {
      manager_->Shutdown();
      manager_->Wait();
    }
    std::error_code ignored;
    fs::remove_all(root_, ignored);
  }
  fs::path Dir(std::size_t index) const {
    return root_ / ("fst" + std::to_string(index + 1));
  }
  static void Write(const fs::path& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(file.good());
  }
  static std::string Read(const fs::path& path) {
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
  }
  void Put(std::size_t size = 17) {
    bytes_.assign(size, 'x');
    Write(root_ / "input", bytes_);
    ASSERT_TRUE(client_->Put(root_ / "input", "sample").ok());
  }
  void Rejoin(std::size_t index) {
    v1::HeartbeatRequest heartbeat;
    heartbeat.set_node_id(nodes_[index].node_id());
    heartbeat.set_endpoint(nodes_[index].endpoint());
    v1::HeartbeatResponse response;
    ASSERT_TRUE(mgm_.Heartbeat(nullptr, &heartbeat, &response).ok());
    ASSERT_TRUE(response.report_inventory());
    auto inventory = stores_[index]->Inventory();
    inventory.set_node_id(nodes_[index].node_id());
    inventory.set_endpoint(nodes_[index].endpoint());
    inventory.set_incarnation(response.incarnation());
    google::protobuf::Empty empty;
    ASSERT_TRUE(mgm_.ReportChunkStatus(nullptr, &inventory, &empty).ok());
  }
  void LoseSecondNode() {
    servers_[1]->Shutdown();
    servers_[1]->Wait();
    servers_[1].reset();
    mgm_.Maintain(HealthClock::now() + std::chrono::seconds(7));
    Rejoin(0);
    Rejoin(2);
    Rejoin(3);
  }
  v1::FileMetadata Stat() {
    v1::FileMetadata info;
    EXPECT_TRUE(client_->Stat("sample", info).ok());
    return info;
  }
  MgmService mgm_;
  fs::path root_;
  std::string bytes_;
  std::unique_ptr<grpc::Server> manager_;
  std::array<v1::NodeInfo, 4> nodes_;
  std::array<std::unique_ptr<FstService>, 4> stores_;
  std::array<std::unique_ptr<grpc::Server>, 4> servers_;
  std::unique_ptr<BlockingTarget> target_;
  std::unique_ptr<ReplicatedClient> client_;
};

TEST_F(RepairTest, RepairsToSpareAndReconcilesCorruptMissingAndUnexpectedCopies) {
  Put(2 * kChunkSize + 7);
  LoseSecondNode();
  for (int index = 0; index < 3; ++index) {
    mgm_.Maintain();
  }
  auto info = Stat();
  ASSERT_EQ(info.chunks_size(), 3);
  for (const auto& chunk : info.chunks()) {
    ASSERT_EQ(chunk.replicas_size(), 3);
    EXPECT_EQ(chunk.replicas(2).node_id(), "fst4");
  }
  auto damaged = Read(Dir(1) / "sample" / "0.chunk");
  damaged.front() ^= 1;
  Write(Dir(1) / "sample" / "0.chunk", damaged);
  fs::remove(Dir(1) / "sample" / "1.chunk");
  fs::remove(Dir(1) / "sample" / "1.crc");
  fs::copy(Dir(1) / "sample", Dir(1) / "unexpected", fs::copy_options::recursive);
  Rejoin(1);
  info = Stat();
  EXPECT_EQ(info.chunks(0).replicas_size(), 3);
  EXPECT_EQ(info.chunks(1).replicas_size(), 3);
  EXPECT_EQ(info.chunks(2).replicas_size(), 4);
  ASSERT_EQ(info.chunks(0).invalid_replicas_size(), 1);
  EXPECT_EQ(info.chunks(0).invalid_replicas(0), "fst2");
  EXPECT_TRUE(fs::exists(Dir(1) / "unexpected"));
  mgm_.Maintain();
  EXPECT_EQ(Stat().chunks(2).replicas_size(), 4);
  ASSERT_TRUE(client_->Get("sample", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes_);
}

TEST_F(RepairTest, CorruptSourceIsNotPropagatedAndAnotherSourceIsTried) {
  Put();
  LoseSecondNode();
  auto damaged = bytes_;
  damaged.front() ^= 1;
  Write(Dir(0) / "sample" / "0.chunk", damaged);
  mgm_.Maintain();
  const auto info = Stat();
  ASSERT_EQ(info.chunks_size(), 1);
  EXPECT_EQ(info.chunks(0).replicas_size(), 2);
  ASSERT_EQ(info.chunks(0).invalid_replicas_size(), 1);
  EXPECT_EQ(info.chunks(0).invalid_replicas(0), "fst1");
  EXPECT_EQ(Read(Dir(3) / "sample" / "0.chunk"), bytes_);
  ASSERT_TRUE(client_->Get("sample", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes_);
}

TEST_F(RepairTest, RejectsBadInstallChecksumWithoutCreatingReplica) {
  v1::InstallChunkRequest request;
  request.set_size_bytes(3);
  request.mutable_write()->mutable_chunk()->set_object_id("bad");
  request.mutable_write()->set_data("abc");
  request.mutable_write()->set_checksum(Checksum("wrong"));
  google::protobuf::Empty empty;
  EXPECT_EQ(stores_[3]->InstallChunk(nullptr, &request, &empty).error_code(),
            grpc::StatusCode::DATA_LOSS);
  EXPECT_FALSE(fs::exists(Dir(3) / "bad"));
}

TEST_F(RepairTest, RepairCannotWriteIntoConcurrentUpload) {
  Put();
  v1::BeginObjectRequest begin;
  begin.set_object_id("sample");
  begin.set_size_bytes(bytes_.size());
  google::protobuf::Empty empty;
  ASSERT_TRUE(stores_[3]->BeginObject(nullptr, &begin, &empty).ok());
  v1::CopyChunkRequest copy;
  copy.mutable_chunk()->set_object_id("sample");
  *copy.mutable_target() = nodes_[3];
  copy.set_size_bytes(bytes_.size());
  copy.set_checksum(Checksum(bytes_));
  EXPECT_EQ(stores_[0]->CopyChunk(nullptr, &copy, &empty).error_code(),
            grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_FALSE(fs::exists(Dir(3) / "sample" / "0.chunk"));
}

TEST_F(RepairTest, DeletedAndRecreatedFileCannotReceiveStaleRepairMetadata) {
  Put();
  LoseSecondNode();
  target_->Block();
  auto worker = std::async(std::launch::async, [&] { mgm_.Maintain(); });
  const bool entered = target_->Entered();
  if (!entered) {
    target_->Release();
    worker.get();
    FAIL() << "repair did not reach target";
  }
  const auto generation = Stat().generation();
  v1::ObjectRequest remove;
  remove.set_object_id("sample");
  google::protobuf::Empty empty;
  EXPECT_TRUE(mgm_.DeleteFile(nullptr, &remove, &empty).ok());
  v1::PutRequest create;
  create.set_object_id("sample");
  create.set_size_bytes(3);
  v1::PutResponse response;
  EXPECT_TRUE(mgm_.Put(nullptr, &create, &response).ok());
  target_->Release();
  worker.get();
  const auto info = Stat();
  EXPECT_GT(info.generation(), generation);
  EXPECT_FALSE(info.complete());
  EXPECT_EQ(info.chunks_size(), 0);
}

TEST_F(RepairTest, RestartedTargetFencesOldCopyAcknowledgement) {
  Put();
  LoseSecondNode();
  target_->Block();
  auto worker = std::async(std::launch::async, [&] { mgm_.Maintain(); });
  const bool entered = target_->Entered();
  if (!entered) {
    target_->Release();
    worker.get();
    FAIL() << "repair did not reach target";
  }
  v1::RegisterNodeRequest registration;
  *registration.mutable_node() = nodes_[3];
  v1::RegisterNodeResponse registered;
  EXPECT_TRUE(mgm_.RegisterNode(nullptr, &registration, &registered).ok());
  Rejoin(3);
  target_->Release();
  worker.get();
  EXPECT_EQ(Stat().chunks(0).replicas_size(), 2);
  mgm_.Maintain();
  EXPECT_EQ(Stat().chunks(0).replicas_size(), 3);
}

}  // namespace
}  // namespace objectstore
