#include <grpcpp/grpcpp.h>

#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>

#include "fst/service.h"

int main(int argc, char* argv[]) {
  if (argc > 6 || argc == 4 || argc == 5) {
    std::cerr << "Usage: fst [listen_address] [data_dir] [mgm_address node_id endpoint]\n";
    return 2;
  }
  try {
    const std::string address = argc >= 2 ? argv[1] : "127.0.0.1:50052";
    const std::string data_dir = argc >= 3 ? argv[2] : "data";
    objectstore::FstService service(data_dir);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "Failed to start FST on " << address << '\n';
      return 1;
    }
    std::cout << "FST listening on " << address << '\n' << std::flush;
    if (argc == 6) {
      auto stub = objectstore::v1::MgmService::NewStub(
          grpc::CreateChannel(argv[3], grpc::InsecureChannelCredentials()));
      objectstore::v1::RegisterNodeRequest request;
      request.mutable_node()->set_node_id(argv[4]);
      request.mutable_node()->set_endpoint(argv[5]);
      request.mutable_node()->set_capacity_bytes(
          std::filesystem::space(data_dir).available);
      objectstore::v1::RegisterNodeResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(15));
      context.set_wait_for_ready(true);
      const auto status = stub->RegisterNode(&context, request, &response);
      if (!status.ok() || !response.accepted()) {
        std::cerr << "RegisterNode failed: " << status.error_message() << '\n';
        server->Shutdown();
        return 1;
      }
      std::cout << "RegisterNode succeeded: node_id=" << argv[4] << '\n'
                << std::flush;
    }
    server->Wait();
  } catch (const std::exception& error) {
    std::cerr << "FST: " << error.what() << '\n';
    return 1;
  }
}
