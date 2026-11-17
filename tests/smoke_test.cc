#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "mgm/service.h"
#include "objectstore.grpc.pb.h"

namespace objectstore {
namespace {

TEST(ProtoTest, PlacementPlanRoundTrip) {
  v1::PlacementPlan original;
  auto* location = original.add_chunks();
  location->mutable_chunk()->set_object_id("example");
  location->mutable_chunk()->set_index(2);
  location->mutable_node()->set_node_id("fst-1");
  location->mutable_node()->set_endpoint("fst-1:50052");
  std::string bytes;
  ASSERT_TRUE(original.SerializeToString(&bytes));
  v1::PlacementPlan decoded;
  ASSERT_TRUE(decoded.ParseFromString(bytes));
  ASSERT_EQ(decoded.chunks_size(), 1);
  EXPECT_EQ(decoded.chunks(0).chunk().object_id(), "example");
  EXPECT_EQ(decoded.chunks(0).chunk().index(), 2);
  EXPECT_EQ(decoded.chunks(0).node().node_id(), "fst-1");
  EXPECT_EQ(decoded.chunks(0).node().endpoint(), "fst-1:50052");
}

class MgmRpcTest : public testing::Test {
 protected:
  void SetUp() override {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    stub_ = v1::MgmService::NewStub(
        grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                            grpc::InsecureChannelCredentials()));
    request_.mutable_node()->set_node_id("fst-1");
    request_.mutable_node()->set_endpoint("127.0.0.1:50052");
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
  }

  grpc::Status Register(v1::RegisterNodeResponse& response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(2));
    return stub_->RegisterNode(&context, request_, &response);
  }

  MgmService service_;
  v1::RegisterNodeRequest request_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<v1::MgmService::Stub> stub_;
};

TEST_F(MgmRpcTest, AcceptsRegistration) {
  v1::RegisterNodeResponse response;
  EXPECT_TRUE(Register(response).ok());
  EXPECT_TRUE(response.accepted());
}

TEST_F(MgmRpcTest, RejectsMissingNodeId) {
  request_.mutable_node()->clear_node_id();
  v1::RegisterNodeResponse response;
  EXPECT_EQ(Register(response).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(response.accepted());
}

TEST_F(MgmRpcTest, RejectsMissingEndpoint) {
  request_.mutable_node()->clear_endpoint();
  v1::RegisterNodeResponse response;
  EXPECT_EQ(Register(response).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(response.accepted());
}

TEST_F(MgmRpcTest, MissingGetReturnsNotFound) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(2));
  v1::GetRequest request;
  request.set_object_id("example");
  v1::GetResponse response;
  EXPECT_EQ(stub_->Get(&context, request, &response).error_code(),
            grpc::StatusCode::NOT_FOUND);
}

TEST_F(MgmRpcTest, PutReservesMetadata) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(2));
  v1::PutRequest request;
  request.set_object_id("example");
  request.set_size_bytes(42);
  v1::PutResponse response;
  EXPECT_TRUE(stub_->Put(&context, request, &response).ok());
}

TEST_F(MgmRpcTest, HeartbeatUpdatesNodeCountersAndRejectsUnknownNode) {
  v1::HeartbeatRequest request;
  request.set_node_id("fst-1");
  request.set_endpoint("127.0.0.1:50052");
  request.set_chunk_count(7);
  request.set_free_bytes(4096);
  v1::HeartbeatResponse response;
  grpc::ClientContext missing;
  EXPECT_EQ(stub_->Heartbeat(&missing, request, &response).error_code(),
            grpc::StatusCode::NOT_FOUND);
  v1::RegisterNodeResponse registered;
  ASSERT_TRUE(Register(registered).ok());
  grpc::ClientContext context;
  EXPECT_TRUE(stub_->Heartbeat(&context, request, &response).ok());
  google::protobuf::Empty empty;
  v1::NodeList nodes;
  grpc::ClientContext listing;
  ASSERT_TRUE(stub_->ListNodes(&listing, empty, &nodes).ok());
  ASSERT_EQ(nodes.nodes_size(), 1);
  EXPECT_EQ(nodes.nodes(0).state(), v1::UP);
  EXPECT_EQ(nodes.nodes(0).chunk_count(), 7);
  EXPECT_EQ(nodes.nodes(0).free_bytes(), 4096);
}

}  // namespace
}  // namespace objectstore
