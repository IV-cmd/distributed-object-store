#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <string>

#include "client/client.h"
#include "common/chunk.h"
#include "fst/service.h"

namespace objectstore {
namespace {

namespace fs = std::filesystem;
using grpc::StatusCode;

void Deadline(grpc::ClientContext& context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));
}

std::string Read(const fs::path& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}

void Write(const fs::path& path, const std::string& bytes) {
  std::ofstream output(path, std::ios::binary);
  output.exceptions(std::ios::failbit | std::ios::badbit);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  output.close();
}

class StorageTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::temp_directory_path() /
            ("objectstore-test-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    ASSERT_TRUE(fs::create_directory(root_));
    Start();
  }

  void Start() {
    service_ = std::make_unique<FstService>(root_ / "data");
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    const auto channel =
        grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                            grpc::InsecureChannelCredentials());
    stub_ = v1::FstService::NewStub(channel);
    client_ = std::make_unique<ObjectClient>(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    fs::remove_all(root_);
  }

  grpc::Status Begin(std::uint64_t size, const std::string& id = "object") {
    v1::BeginObjectRequest request;
    request.set_object_id(id);
    request.set_size_bytes(size);
    google::protobuf::Empty response;
    grpc::ClientContext context;
    Deadline(context);
    return stub_->BeginObject(&context, request, &response);
  }

  grpc::Status Finish() {
    v1::ObjectRequest request;
    request.set_object_id("object");
    google::protobuf::Empty response;
    grpc::ClientContext context;
    Deadline(context);
    return stub_->FinishObject(&context, request, &response);
  }

  grpc::Status Info(v1::ObjectInfo& response) {
    v1::ObjectRequest request;
    request.set_object_id("object");
    grpc::ClientContext context;
    Deadline(context);
    return stub_->GetObjectInfo(&context, request, &response);
  }

  grpc::Status PutChunk(const std::string& bytes, std::uint32_t index = 0) {
    v1::WriteChunkRequest request;
    request.mutable_chunk()->set_object_id("object");
    request.mutable_chunk()->set_index(index);
    request.set_data(bytes);
    request.set_checksum(Checksum(bytes));
    google::protobuf::Empty response;
    grpc::ClientContext context;
    Deadline(context);
    return stub_->WriteChunk(&context, request, &response);
  }

  grpc::Status GetChunk(v1::ReadChunkResponse& response) {
    v1::ReadChunkRequest request;
    request.mutable_chunk()->set_object_id("object");
    grpc::ClientContext context;
    Deadline(context);
    return stub_->ReadChunk(&context, request, &response);
  }

  void Store(const std::string& bytes = "abc") {
    ASSERT_TRUE(Begin(bytes.size()).ok());
    ASSERT_TRUE(PutChunk(bytes).ok());
    ASSERT_TRUE(Finish().ok());
  }

  fs::path root_;
  std::unique_ptr<FstService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<v1::FstService::Stub> stub_;
  std::unique_ptr<ObjectClient> client_;
};

TEST_F(StorageTest, ClientRoundTripsBoundarySizesAndRemovesObjects) {
  for (const auto size : {std::size_t{0}, std::size_t{17}, kChunkSize,
                          kChunkSize + 31, 2 * kChunkSize + 5}) {
    SCOPED_TRACE(size);
    std::string bytes(size, '\0');
    for (std::size_t i = 0; i < size; ++i) {
      bytes[i] = static_cast<char>(i % 251);
    }
    const auto id = "size-" + std::to_string(size);
    Write(root_ / "input", bytes);
    ASSERT_TRUE(client_->Put(root_ / "input", id).ok());
    ASSERT_TRUE(client_->Get(id, root_ / "output").ok());
    EXPECT_EQ(Read(root_ / "output"), bytes);
    EXPECT_TRUE(fs::exists(root_ / "data" / id / "manifest"));
    EXPECT_FALSE(fs::exists(root_ / "data" / id / "pending"));
    for (std::uint64_t i = 0; i < ChunkCount(size); ++i) {
      EXPECT_TRUE(
          fs::exists(root_ / "data" / id / (std::to_string(i) + ".chunk")));
      EXPECT_EQ(Read(root_ / "data" / id / (std::to_string(i) + ".crc")).size(),
                17);
    }
    ASSERT_TRUE(client_->Remove(id).ok());
    EXPECT_FALSE(fs::exists(root_ / "data" / id));
    EXPECT_EQ(client_->Get(id, root_ / "output").error_code(),
              StatusCode::NOT_FOUND);
  }
}

TEST_F(StorageTest, MissingObjectReturnsNotFound) {
  v1::ObjectInfo info;
  v1::ReadChunkResponse chunk;
  EXPECT_EQ(Info(info).error_code(), StatusCode::NOT_FOUND);
  EXPECT_EQ(GetChunk(chunk).error_code(), StatusCode::NOT_FOUND);
  EXPECT_EQ(client_->Remove("object").error_code(), StatusCode::NOT_FOUND);
}

TEST_F(StorageTest, IncompleteUploadIsNotReadableOrPublishable) {
  ASSERT_TRUE(Begin(3).ok());
  v1::ObjectInfo info;
  v1::ReadChunkResponse chunk;
  EXPECT_EQ(Info(info).error_code(), StatusCode::FAILED_PRECONDITION);
  EXPECT_EQ(GetChunk(chunk).error_code(), StatusCode::FAILED_PRECONDITION);
  EXPECT_EQ(Finish().error_code(), StatusCode::NOT_FOUND);
  ASSERT_TRUE(PutChunk("abc").ok());
  ASSERT_TRUE(Finish().ok());
  ASSERT_TRUE(Info(info).ok());
  EXPECT_EQ(info.size_bytes(), 3);
  EXPECT_EQ(info.chunk_count(), 1);
}

TEST_F(StorageTest, RejectsWriteChecksumMismatchWithoutStoringData) {
  ASSERT_TRUE(Begin(3).ok());
  v1::WriteChunkRequest request;
  request.mutable_chunk()->set_object_id("object");
  request.set_data("abc");
  request.set_checksum(Checksum("abc") ^ 1);
  google::protobuf::Empty response;
  grpc::ClientContext context;
  Deadline(context);
  const auto status = stub_->WriteChunk(&context, request, &response);
  EXPECT_EQ(status.error_code(), StatusCode::DATA_LOSS);
  EXPECT_NE(status.error_message().find("CHECKSUM_MISMATCH"),
            std::string::npos);
  EXPECT_FALSE(fs::exists(root_ / "data/object/0.chunk"));
  EXPECT_FALSE(fs::exists(root_ / "data/object/0.crc"));
  EXPECT_TRUE(PutChunk("abc").ok());
}

TEST_F(StorageTest, RejectsWrongChunkSizesAndIndices) {
  ASSERT_TRUE(Begin(3).ok());
  EXPECT_EQ(PutChunk("ab").error_code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(PutChunk("abc", 1).error_code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(PutChunk(std::string(2 * kChunkSize, 'x')).error_code(),
            StatusCode::INVALID_ARGUMENT);
}

TEST_F(StorageTest, ObjectIdsAreWriteOnceUntilDeleted) {
  ASSERT_TRUE(Begin(3).ok());
  EXPECT_EQ(Begin(3).error_code(), StatusCode::ALREADY_EXISTS);
  ASSERT_TRUE(PutChunk("abc").ok());
  EXPECT_EQ(PutChunk("abc").error_code(), StatusCode::ALREADY_EXISTS);
  ASSERT_TRUE(Finish().ok());
  EXPECT_EQ(PutChunk("abc").error_code(), StatusCode::ALREADY_EXISTS);
  ASSERT_TRUE(client_->Remove("object").ok());
  EXPECT_TRUE(Begin(0).ok());
}

TEST_F(StorageTest, DetectsFlippedStoredByteAndPreservesExistingDownload) {
  Store();
  const auto chunk_path = root_ / "data/object/0.chunk";
  std::fstream disk(chunk_path,
                    std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(disk.is_open());
  char byte = 0;
  disk.get(byte);
  byte ^= 1;
  disk.seekp(0);
  disk.put(byte);
  disk.close();
  v1::ReadChunkResponse response;
  const auto status = GetChunk(response);
  EXPECT_EQ(status.error_code(), StatusCode::DATA_LOSS);
  EXPECT_NE(status.error_message().find("CHECKSUM_MISMATCH"),
            std::string::npos);
  EXPECT_TRUE(response.data().empty());
  Write(root_ / "output", "keep me");
  EXPECT_EQ(client_->Get("object", root_ / "output").error_code(),
            StatusCode::DATA_LOSS);
  EXPECT_EQ(Read(root_ / "output"), "keep me");
  EXPECT_FALSE(fs::exists(root_ / "output.download"));
}

TEST_F(StorageTest, RejectsCorruptChecksumSidecar) {
  Store();
  Write(root_ / "data/object/0.crc", "not-a-checksum\n");
  v1::ReadChunkResponse response;
  const auto status = GetChunk(response);
  EXPECT_EQ(status.error_code(), StatusCode::DATA_LOSS);
  EXPECT_NE(status.error_message().find("CHECKSUM_MISMATCH"),
            std::string::npos);
}

TEST_F(StorageTest, FinishVerifiesDataBeforePublishingManifest) {
  ASSERT_TRUE(Begin(3).ok());
  ASSERT_TRUE(PutChunk("abc").ok());
  Write(root_ / "data/object/0.chunk", "bad");
  EXPECT_EQ(Finish().error_code(), StatusCode::DATA_LOSS);
  EXPECT_FALSE(fs::exists(root_ / "data/object/manifest"));
  EXPECT_TRUE(client_->Remove("object").ok());
}

TEST_F(StorageTest, DeleteChunkRemovesDataAndChecksum) {
  Store();
  v1::DeleteChunkRequest request;
  request.mutable_chunk()->set_object_id("object");
  google::protobuf::Empty response;
  grpc::ClientContext context;
  Deadline(context);
  EXPECT_TRUE(stub_->DeleteChunk(&context, request, &response).ok());
  EXPECT_FALSE(fs::exists(root_ / "data/object/0.chunk"));
  EXPECT_FALSE(fs::exists(root_ / "data/object/0.crc"));
  v1::ReadChunkResponse chunk;
  EXPECT_EQ(GetChunk(chunk).error_code(), StatusCode::NOT_FOUND);
}

TEST_F(StorageTest, RejectsUnsafeIdsAndUnrepresentableSizes) {
  for (const std::string id :
       {"", "..", "../outside", "a/b", "/absolute", "a.b"}) {
    EXPECT_EQ(Begin(0, id).error_code(), StatusCode::INVALID_ARGUMENT);
  }
  EXPECT_EQ(Begin(0, std::string(129, 'x')).error_code(),
            StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(Begin(std::numeric_limits<std::uint64_t>::max()).error_code(),
            StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(fs::exists(root_ / "outside"));
}

TEST_F(StorageTest, PersistsAcrossServiceRestart) {
  Store();
  server_->Shutdown();
  server_->Wait();
  server_.reset();
  service_.reset();
  Start();
  ASSERT_TRUE(client_->Get("object", root_ / "output").ok());
  EXPECT_EQ(Read(root_ / "output"), "abc");
}

TEST_F(StorageTest, ConcurrentBeginReservesIdExactlyOnce) {
  auto first = std::async(std::launch::async, [&] { return Begin(0); });
  const auto second = Begin(0);
  const auto initial = first.get();
  EXPECT_NE(initial.ok(), second.ok());
  EXPECT_EQ(initial.ok() ? second.error_code() : initial.error_code(),
            StatusCode::ALREADY_EXISTS);
}

TEST_F(StorageTest, FilesystemFailuresReturnInternal) {
  fs::remove_all(root_ / "data");
  Write(root_ / "data", "not a directory");
  EXPECT_EQ(Begin(0).error_code(), StatusCode::INTERNAL);
}

class BadChecksumService final : public v1::FstService::Service {
 public:
  grpc::Status GetObjectInfo(grpc::ServerContext*, const v1::ObjectRequest*,
                             v1::ObjectInfo* response) override {
    response->set_size_bytes(3);
    response->set_chunk_count(1);
    return grpc::Status::OK;
  }
  grpc::Status ReadChunk(grpc::ServerContext*, const v1::ReadChunkRequest*,
                         v1::ReadChunkResponse* response) override {
    response->set_data("abc");
    response->set_checksum(Checksum("abc") ^ 1);
    return grpc::Status::OK;
  }
};

TEST_F(StorageTest, ClientIndependentlyRejectsBadReadChecksum) {
  BadChecksumService service;
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ObjectClient client(grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                          grpc::InsecureChannelCredentials()));
  const auto status = client.Get("object", root_ / "output");
  EXPECT_EQ(status.error_code(), StatusCode::DATA_LOSS);
  EXPECT_NE(status.error_message().find("CHECKSUM_MISMATCH"),
            std::string::npos);
  EXPECT_FALSE(fs::exists(root_ / "output"));
  EXPECT_FALSE(fs::exists(root_ / "output.download"));
  server->Shutdown();
  server->Wait();
}

}  // namespace
}  // namespace objectstore
