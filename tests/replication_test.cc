#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "client/client.h"
#include "common/chunk.h"
#include "common/layout.h"
#include "fst/service.h"
#include "mgm/service.h"

namespace objectstore {
namespace {

namespace fs = std::filesystem;
using grpc::StatusCode;

class FaultyFst final : public v1::FstService::Service {
 public:
  explicit FaultyFst(FstService& store) : store_(store) {}

  std::atomic<bool> reject_second_chunk{false};
  std::atomic<bool> lie_on_read{false};
  std::atomic<unsigned> reads{0};

  grpc::Status BeginObject(grpc::ServerContext* context,
                           const v1::BeginObjectRequest* request,
                           google::protobuf::Empty* response) override {
    return store_.BeginObject(context, request, response);
  }
  grpc::Status FinishObject(grpc::ServerContext* context,
                            const v1::ObjectRequest* request,
                            google::protobuf::Empty* response) override {
    return store_.FinishObject(context, request, response);
  }
  grpc::Status DeleteObject(grpc::ServerContext* context,
                            const v1::ObjectRequest* request,
                            google::protobuf::Empty* response) override {
    return store_.DeleteObject(context, request, response);
  }
  grpc::Status WriteChunk(grpc::ServerContext* context,
                          const v1::WriteChunkRequest* request,
                          google::protobuf::Empty* response) override {
    if (reject_second_chunk.load() && request->chunk().index() == 1) {
      return {StatusCode::INTERNAL, "injected second chunk write failure"};
    }
    return store_.WriteChunk(context, request, response);
  }
  grpc::Status ReadChunk(grpc::ServerContext* context,
                         const v1::ReadChunkRequest* request,
                         v1::ReadChunkResponse* response) override {
    ++reads;
    const auto status = store_.ReadChunk(context, request, response);
    if (status.ok() && lie_on_read.load() && !response->data().empty()) {
      response->mutable_data()->front() ^= 1;
      response->set_checksum(Checksum(response->data()));
    }
    return status;
  }

 private:
  FstService& store_;
};

class ReplicationTest : public testing::Test {
 protected:
  virtual std::size_t NodeCount() const { return 3; }

  void SetUp() override {
    static std::atomic<unsigned> sequence{0};
    root_ = fs::temp_directory_path() /
            ("objectstore-replication-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()) +
             "-" + std::to_string(sequence.fetch_add(1)));
    fs::create_directories(root_);
    int mgm_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &mgm_port);
    builder.RegisterService(&mgm_);
    mgm_server_ = builder.BuildAndStart();
    ASSERT_NE(mgm_server_, nullptr);
    const auto channel =
        grpc::CreateChannel("127.0.0.1:" + std::to_string(mgm_port),
                            grpc::InsecureChannelCredentials());
    client_ = std::make_unique<ReplicatedClient>(channel);
    auto registry = v1::MgmService::NewStub(channel);
    stores_.resize(NodeCount());
    services_.resize(NodeCount());
    servers_.resize(NodeCount());
    for (std::size_t index = 0; index < stores_.size(); ++index) {
      stores_[index] = std::make_unique<FstService>(NodeDir(index));
      services_[index] = std::make_unique<FaultyFst>(*stores_[index]);
      int port = 0;
      grpc::ServerBuilder fst;
      fst.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
      fst.RegisterService(services_[index].get());
      servers_[index] = fst.BuildAndStart();
      ASSERT_NE(servers_[index], nullptr);
      v1::RegisterNodeRequest request;
      request.mutable_node()->set_node_id("fst" + std::to_string(index + 1));
      request.mutable_node()->set_endpoint("127.0.0.1:" + std::to_string(port));
      request.mutable_node()->set_capacity_bytes(100 * kChunkSize);
      v1::RegisterNodeResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(5));
      ASSERT_TRUE(registry->RegisterNode(&context, request, &response).ok());
      ASSERT_TRUE(response.accepted());
    }
  }

  void TearDown() override {
    for (std::size_t index = 0; index < servers_.size(); ++index) {
      Stop(index);
    }
    if (mgm_server_) {
      mgm_server_->Shutdown();
      mgm_server_->Wait();
    }
    if (!root_.empty()) {
      std::error_code ignored;
      fs::remove_all(root_, ignored);
    }
  }

  fs::path NodeDir(std::size_t index) const {
    return root_ / ("fst" + std::to_string(index + 1));
  }

  void Stop(std::size_t index) {
    if (servers_[index]) {
      servers_[index]->Shutdown();
      servers_[index]->Wait();
      servers_[index].reset();
    }
  }

  static std::string Bytes(std::size_t size) {
    std::string bytes(size, '\0');
    for (std::size_t index = 0; index < size; ++index) {
      bytes[index] = static_cast<char>(index % 251);
    }
    return bytes;
  }

  static void Write(const fs::path& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(file.good());
  }

  static std::string Read(const fs::path& path) {
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>()};
  }

  void Put(const std::string& bytes) {
    Write(root_ / "input", bytes);
    ASSERT_TRUE(client_->Put(root_ / "input", "sample").ok());
  }

  fs::path root_;
  MgmService mgm_;
  std::unique_ptr<grpc::Server> mgm_server_;
  std::vector<std::unique_ptr<FstService>> stores_;
  std::vector<std::unique_ptr<FaultyFst>> services_;
  std::vector<std::unique_ptr<grpc::Server>> servers_;
  std::unique_ptr<ReplicatedClient> client_;
};

TEST_F(ReplicationTest, BoundarySizesRoundTripAndRecordThreeReplicasPerChunk) {
  for (const auto size :
       {std::size_t{0}, std::size_t{17}, kChunkSize, 2 * kChunkSize + 7}) {
    const std::string id = "sample";
    const auto bytes = Bytes(size);
    Put(bytes);
    v1::FileMetadata info;
    ASSERT_TRUE(client_->Stat(id, info).ok());
    EXPECT_TRUE(info.complete());
    EXPECT_EQ(info.size_bytes(), size);
    EXPECT_EQ(info.chunk_count(), ChunkCount(size));
    EXPECT_EQ(info.chunks_size(), info.chunk_count());
    for (const auto& chunk : info.chunks()) {
      ASSERT_EQ(chunk.replicas_size(), 3);
      std::set<std::string> ids;
      std::set<std::string> endpoints;
      for (const auto& node : chunk.replicas()) {
        ids.insert(node.node_id());
        endpoints.insert(node.endpoint());
        EXPECT_GT(node.capacity_bytes(), 0);
      }
      EXPECT_EQ(ids.size(), 3);
      EXPECT_EQ(endpoints.size(), 3);
    }
    ASSERT_TRUE(client_->Get(id, root_ / "output").ok());
    EXPECT_EQ(Read(root_ / "output"), bytes);
    EXPECT_EQ(client_->Put(root_ / "input", id).error_code(),
              StatusCode::ALREADY_EXISTS);
    ASSERT_TRUE(client_->Remove(id).ok());
    EXPECT_EQ(client_->Stat(id, info).error_code(), StatusCode::NOT_FOUND);
  }
}

class OneFailedFstTest : public ReplicationTest,
                         public testing::WithParamInterface<std::size_t> {};

TEST_P(OneFailedFstTest, ReadsAfterAnyOneServerStops) {
  const auto bytes = Bytes(2 * kChunkSize + 7);
  Put(bytes);
  Stop(GetParam());
  ASSERT_TRUE(client_->Get("sample", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes);
}

INSTANTIATE_TEST_SUITE_P(EveryReplica, OneFailedFstTest,
                         testing::Values(std::size_t{0}, std::size_t{1},
                                         std::size_t{2}));

TEST_F(ReplicationTest, FallsBackWhenStoredChunkFailsFstVerification) {
  const auto bytes = Bytes(2 * kChunkSize + 7);
  Put(bytes);
  const auto path = NodeDir(1) / "sample" / "1.chunk";
  auto damaged = Read(path);
  ASSERT_FALSE(damaged.empty());
  damaged.front() ^= 1;
  Write(path, damaged);
  ASSERT_TRUE(client_->Get("sample", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes);
}

TEST_F(ReplicationTest, RejectsSelfConsistentButWrongResponseUsingMgmChecksum) {
  const auto bytes = Bytes(kChunkSize + 7);
  Put(bytes);
  services_[0]->lie_on_read.store(true);
  ASSERT_TRUE(client_->Get("sample", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes);
}

TEST_F(ReplicationTest, AllCorruptReplicasFailWithoutReplacingExistingOutput) {
  Put(Bytes(29));
  for (const auto& service : services_) {
    service->lie_on_read.store(true);
  }
  Write(root_ / "output", "preserve this");
  const auto status = client_->Get("sample", root_ / "output");
  EXPECT_EQ(status.error_code(), StatusCode::DATA_LOSS);
  EXPECT_NE(status.error_message().find("CHECKSUM_MISMATCH"),
            std::string::npos);
  EXPECT_EQ(Read(root_ / "output"), "preserve this");
  EXPECT_FALSE(fs::exists(root_ / "output.download"));
}

TEST_F(ReplicationTest, AllUnavailableReplicasFailWithoutPublishingOutput) {
  Put(Bytes(29));
  for (std::size_t index = 0; index < servers_.size(); ++index) {
    Stop(index);
  }
  EXPECT_EQ(client_->Get("sample", root_ / "output").error_code(),
            StatusCode::UNAVAILABLE);
  EXPECT_FALSE(fs::exists(root_ / "output"));
}

TEST_F(ReplicationTest, FailedSecondChunkRecordsOnlySuccessfulReplicaWrites) {
  services_[1]->reject_second_chunk.store(true);
  Write(root_ / "input", Bytes(2 * kChunkSize + 7));
  const auto status = client_->Put(root_ / "input", "partial");
  EXPECT_EQ(status.error_code(), StatusCode::INTERNAL);
  EXPECT_NE(status.error_message().find("committed_replicas=2/3"),
            std::string::npos);
  v1::FileMetadata info;
  ASSERT_TRUE(client_->Stat("partial", info).ok());
  EXPECT_FALSE(info.complete());
  ASSERT_EQ(info.chunks_size(), 2);
  EXPECT_EQ(info.chunks(0).replicas_size(), 3);
  ASSERT_EQ(info.chunks(1).replicas_size(), 2);
  EXPECT_EQ(info.chunks(1).replicas(0).node_id(), "fst1");
  EXPECT_EQ(info.chunks(1).replicas(1).node_id(), "fst3");
  EXPECT_EQ(client_->Get("partial", root_ / "output").error_code(),
            StatusCode::FAILED_PRECONDITION);
  EXPECT_FALSE(fs::exists(root_ / "output"));
  EXPECT_TRUE(fs::exists(NodeDir(0) / "partial" / "1.chunk"));
  EXPECT_FALSE(fs::exists(NodeDir(1) / "partial" / "1.chunk"));
  EXPECT_TRUE(fs::exists(NodeDir(2) / "partial" / "1.chunk"));
  EXPECT_TRUE(client_->Remove("partial").ok());
}

TEST_F(ReplicationTest, FailedDeleteRetainsMetadataForRetry) {
  Put(Bytes(29));
  Stop(1);
  EXPECT_EQ(client_->Remove("sample").error_code(), StatusCode::UNAVAILABLE);
  v1::FileMetadata info;
  EXPECT_TRUE(client_->Stat("sample", info).ok());
}

class ErasureTest : public ReplicationTest {
 protected:
  std::size_t NodeCount() const override { return 6; }
};

TEST_F(ErasureTest, RoundTripsBoundariesAndNeverReadsParityOnTheHealthyPath) {
  for (const auto size : {std::size_t{0}, std::size_t{1}, std::size_t{17},
                          kChunkSize, 4 * kChunkSize, 4 * kChunkSize + 7}) {
    const auto bytes = Bytes(size);
    Write(root_ / "input", bytes);
    ASSERT_TRUE(client_->Put(root_ / "input", "rs", v1::RS42).ok());
    v1::FileMetadata info;
    ASSERT_TRUE(client_->Stat("rs", info).ok());
    EXPECT_TRUE(info.complete());
    EXPECT_EQ(info.layout(), v1::RS42);
    EXPECT_EQ(info.chunk_count(), LayoutChunkCount(v1::RS42, size));
    for (const auto& chunk : info.chunks()) {
      EXPECT_EQ(chunk.replicas_size(), 1);
      EXPECT_EQ(chunk.stripe_index(), chunk.chunk().index() % 6);
      EXPECT_EQ(chunk.parity(), chunk.stripe_index() >= 4);
    }
    ASSERT_TRUE(client_->Get("rs", root_ / "output").ok());
    EXPECT_EQ(Read(root_ / "output"), bytes);
    EXPECT_EQ(services_[4]->reads.load(), 0);
    EXPECT_EQ(services_[5]->reads.load(), 0);
    ASSERT_TRUE(client_->Remove("rs").ok());
  }
}

TEST_F(ErasureTest, ReconstructsTwoLossesAndPreservesOutputAfterThree) {
  const auto bytes = Bytes(4 * kChunkSize + 7);
  Write(root_ / "input", bytes);
  ASSERT_TRUE(client_->Put(root_ / "input", "rs", v1::RS42).ok());
  Stop(0);
  Stop(1);
  ASSERT_TRUE(client_->Get("rs", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes);
  Stop(2);
  Write(root_ / "output", "preserve this");
  const auto status = client_->Get("rs", root_ / "output");
  EXPECT_EQ(status.error_code(), StatusCode::UNAVAILABLE);
  EXPECT_NE(status.error_message().find("four verified shards"), std::string::npos);
  EXPECT_EQ(Read(root_ / "output"), "preserve this");
}

TEST_F(ErasureTest, RejectsSelfConsistentCorruptionBeforeReconstruction) {
  const auto bytes = Bytes(1234);
  Write(root_ / "input", bytes);
  ASSERT_TRUE(client_->Put(root_ / "input", "rs", v1::RS42).ok());
  Stop(0);
  services_[1]->lie_on_read.store(true);
  ASSERT_TRUE(client_->Get("rs", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), bytes);
  services_[2]->lie_on_read.store(true);
  EXPECT_EQ(client_->Get("rs", root_ / "output").error_code(), StatusCode::UNAVAILABLE);
  EXPECT_EQ(Read(root_ / "output"), bytes);
}

TEST_F(ErasureTest, FailedShardWriteCannotPublishAnIncompleteRsFile) {
  Write(root_ / "input", Bytes(29));
  services_[1]->reject_second_chunk.store(true);
  EXPECT_EQ(client_->Put(root_ / "input", "rs", v1::RS42).error_code(), StatusCode::INTERNAL);
  v1::FileMetadata info;
  ASSERT_TRUE(client_->Stat("rs", info).ok());
  EXPECT_FALSE(info.complete());
  EXPECT_EQ(client_->Get("rs", root_ / "output").error_code(), StatusCode::FAILED_PRECONDITION);
  ASSERT_TRUE(client_->Remove("rs").ok());
}

}  // namespace
}  // namespace objectstore
