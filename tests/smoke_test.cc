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
    stub_ = v1::MgmService::NewStub(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
  }

  grpc::Status Register(const std::string& id, const std::string& endpoint,
                        v1::RegisterNodeResponse& response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(2));
    v1::RegisterNodeRequest request;
    request.mutable_node()->set_node_id(id);
    request.mutable_node()->set_endpoint(endpoint);
    return stub_->RegisterNode(&context, request, &response);
  }

  MgmService service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<v1::MgmService::Stub> stub_;
};

TEST_F(MgmRpcTest, AcceptsRegistration) {
  v1::RegisterNodeResponse response;
  EXPECT_TRUE(Register("fst-1", "127.0.0.1:50052", response).ok());
  EXPECT_TRUE(response.accepted());
}

TEST_F(MgmRpcTest, RejectsMissingNodeId) {
  v1::RegisterNodeResponse response;
  EXPECT_EQ(Register("", "127.0.0.1:50052", response).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(response.accepted());
}

TEST_F(MgmRpcTest, RejectsMissingEndpoint) {
  v1::RegisterNodeResponse response;
  EXPECT_EQ(Register("fst-1", "", response).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(response.accepted());
}

TEST_F(MgmRpcTest, GetIsUnimplemented) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  v1::GetRequest request;
  request.set_object_id("example");
  v1::GetResponse response;
  EXPECT_EQ(stub_->Get(&context, request, &response).error_code(),
            grpc::StatusCode::UNIMPLEMENTED);
}

TEST_F(MgmRpcTest, PutIsUnimplemented) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  v1::PutRequest request;
  request.set_object_id("example");
  request.set_size_bytes(42);
  v1::PutResponse response;
  EXPECT_EQ(stub_->Put(&context, request, &response).error_code(),
            grpc::StatusCode::UNIMPLEMENTED);
}

}  // namespace
}  // namespace objectstore
