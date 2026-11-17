#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/chunk.h"
#include "mgm/service.h"

namespace objectstore {
namespace {

using grpc::StatusCode;

class MetadataTest : public testing::Test {
 protected:
  void SetUp() override {
    for (const std::string id : {"c", "a", "b"}) {
      v1::RegisterNodeRequest request;
      request.mutable_node()->set_node_id(id);
      request.mutable_node()->set_endpoint(id + ":50052");
      request.mutable_node()->set_capacity_bytes(1ULL << 30);
      v1::RegisterNodeResponse response;
      ASSERT_TRUE(service_.RegisterNode(nullptr, &request, &response).ok());
    }
    v1::PutRequest request;
    request.set_object_id("file");
    request.set_name("original.bin");
    request.set_size_bytes(kChunkSize + 7);
    v1::PutResponse response;
    ASSERT_TRUE(service_.Put(nullptr, &request, &response).ok());
  }

  grpc::Status Allocate(v1::ReplicaPlan& plan, std::uint32_t index = 0) {
    v1::ChunkId request;
    request.set_object_id("file");
    request.set_index(index);
    return service_.AllocateChunk(nullptr, &request, &plan);
  }

  grpc::Status Commit(const std::vector<std::string>& nodes,
                      std::uint32_t index = 0) {
    v1::CommitChunkRequest request;
    request.mutable_chunk()->set_object_id("file");
    request.mutable_chunk()->set_index(index);
    request.set_checksum(42);
    for (const auto& id : nodes) {
      request.add_node_ids(id);
    }
    google::protobuf::Empty response;
    return service_.CommitChunk(nullptr, &request, &response);
  }

  grpc::Status Stat(v1::FileMetadata& metadata) {
    v1::ObjectRequest request;
    request.set_object_id("file");
    return service_.StatFile(nullptr, &request, &metadata);
  }

  grpc::Status Complete() {
    v1::ObjectRequest request;
    request.set_object_id("file");
    google::protobuf::Empty response;
    return service_.CompleteFile(nullptr, &request, &response);
  }

  MgmService service_;
};

TEST_F(MetadataTest, AllocateCommitLocateAndStatRoundTrip) {
  v1::ReplicaPlan plan;
  ASSERT_TRUE(Allocate(plan).ok());
  ASSERT_EQ(plan.nodes_size(), 3);
  ASSERT_TRUE(Commit({"c", "a", "b"}).ok());
  v1::ChunkId request;
  request.set_object_id("file");
  v1::ChunkMetadata chunk;
  ASSERT_TRUE(service_.LocateChunk(nullptr, &request, &chunk).ok());
  EXPECT_TRUE(chunk.committed());
  EXPECT_EQ(chunk.checksum(), 42);
  ASSERT_EQ(chunk.replicas_size(), 3);
  EXPECT_EQ(chunk.replicas(0).node_id(), "a");
  v1::FileMetadata metadata;
  ASSERT_TRUE(Stat(metadata).ok());
  EXPECT_EQ(metadata.name(), "original.bin");
  EXPECT_EQ(metadata.size_bytes(), kChunkSize + 7);
  EXPECT_EQ(metadata.chunk_count(), 2);
  EXPECT_FALSE(metadata.complete());
  EXPECT_EQ(metadata.chunks_size(), 1);
}

TEST_F(MetadataTest, PreservesHonestPartialReplicaCommit) {
  v1::ReplicaPlan plan;
  ASSERT_TRUE(Allocate(plan).ok());
  ASSERT_TRUE(Commit({"a", "c"}).ok());
  v1::FileMetadata metadata;
  ASSERT_TRUE(Stat(metadata).ok());
  ASSERT_EQ(metadata.chunks_size(), 1);
  EXPECT_EQ(metadata.chunks(0).replicas_size(), 2);
  EXPECT_FALSE(metadata.complete());
  EXPECT_EQ(Complete().error_code(), StatusCode::FAILED_PRECONDITION);
}

TEST_F(MetadataTest, RejectsDuplicateOrUnallocatedReplicaClaims) {
  EXPECT_EQ(Commit({"a"}).error_code(), StatusCode::FAILED_PRECONDITION);
  v1::ReplicaPlan plan;
  ASSERT_TRUE(Allocate(plan).ok());
  EXPECT_EQ(Commit({"a", "a"}).error_code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(Commit({"other"}).error_code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(Complete().error_code(), StatusCode::FAILED_PRECONDITION);
}

TEST_F(MetadataTest, CompletesOnlyAfterAllChunksHaveThreeReplicas) {
  for (std::uint32_t i = 0; i < 2; ++i) {
    v1::ReplicaPlan plan;
    ASSERT_TRUE(Allocate(plan, i).ok());
    ASSERT_TRUE(Commit({"a", "b", "c"}, i).ok());
  }
  ASSERT_TRUE(Complete().ok());
  v1::FileMetadata metadata;
  ASSERT_TRUE(Stat(metadata).ok());
  EXPECT_TRUE(metadata.complete());
  v1::ReplicaPlan plan;
  EXPECT_EQ(Allocate(plan).error_code(), StatusCode::FAILED_PRECONDITION);
  v1::GetRequest request;
  request.set_object_id("file");
  v1::GetResponse response;
  ASSERT_TRUE(service_.Get(nullptr, &request, &response).ok());
  EXPECT_EQ(response.plan().chunks_size(), 6);
}

TEST_F(MetadataTest, FileTargetsStayStableAcrossChunks) {
  v1::ReplicaPlan first;
  v1::ReplicaPlan second;
  ASSERT_TRUE(Allocate(first).ok());
  ASSERT_TRUE(Allocate(second, 1).ok());
  EXPECT_EQ(first.SerializeAsString(), second.SerializeAsString());
  EXPECT_EQ(Allocate(second, 2).error_code(), StatusCode::INVALID_ARGUMENT);
}

TEST_F(MetadataTest, ListsFilesAndDeletesIncompleteMetadata) {
  google::protobuf::Empty empty;
  v1::FileList list;
  ASSERT_TRUE(service_.ListFiles(nullptr, &empty, &list).ok());
  ASSERT_EQ(list.files_size(), 1);
  EXPECT_EQ(list.files(0).object_id(), "file");
  v1::ObjectRequest request;
  request.set_object_id("file");
  ASSERT_TRUE(service_.DeleteFile(nullptr, &request, &empty).ok());
  v1::FileMetadata metadata;
  EXPECT_EQ(Stat(metadata).error_code(), StatusCode::NOT_FOUND);
}

TEST_F(MetadataTest, FreshMgmHasAnEmptyNamespace) {
  MgmService restarted;
  google::protobuf::Empty request;
  v1::FileList response;
  ASSERT_TRUE(restarted.ListFiles(nullptr, &request, &response).ok());
  EXPECT_TRUE(response.files().empty());
}

TEST(MetadataPlacementTest, TooFewRegisteredNodesReturnsExplicitError) {
  MgmService service;
  v1::PutRequest put;
  put.set_object_id("file");
  put.set_size_bytes(1);
  v1::PutResponse response;
  ASSERT_TRUE(service.Put(nullptr, &put, &response).ok());
  v1::ChunkId chunk;
  chunk.set_object_id("file");
  v1::ReplicaPlan plan;
  EXPECT_EQ(service.AllocateChunk(nullptr, &chunk, &plan).error_code(),
            StatusCode::RESOURCE_EXHAUSTED);
}

}  // namespace
}  // namespace objectstore
