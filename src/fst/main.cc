#include <grpcpp/grpcpp.h>

#include <chrono>
#include <iostream>

#include "objectstore.grpc.pb.h"

int main(int argc, char* argv[]) {
  if (argc != 5) {
    std::cerr << "Usage: fst <listen_address> <mgm_address> <node_id> "
                 "<advertised_endpoint>\n";
    return 2;
  }
  objectstore::v1::FstService::Service service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(argv[1], grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "Failed to start FST on " << argv[1] << '\n';
    return 1;
  }
  std::cout << "FST listening on " << argv[1] << '\n' << std::flush;

  auto stub = objectstore::v1::MgmService::NewStub(
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()));
  objectstore::v1::RegisterNodeRequest request;
  request.mutable_node()->set_node_id(argv[3]);
  request.mutable_node()->set_endpoint(argv[4]);
  objectstore::v1::RegisterNodeResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(15));
  context.set_wait_for_ready(true);
  const auto status = stub->RegisterNode(&context, request, &response);
  if (!status.ok() || !response.accepted()) {
    std::cerr << "RegisterNode failed: code=" << status.error_code()
              << " message=" << status.error_message() << '\n';
    server->Shutdown();
    return 1;
  }
  std::cout << "RegisterNode succeeded: node_id=" << argv[3]
            << " endpoint=" << argv[4] << '\n' << std::flush;
  server->Wait();
}
