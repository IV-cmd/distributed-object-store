#include <grpcpp/grpcpp.h>

#include <iostream>
#include <string>

#include "mgm/service.h"

int main(int argc, char* argv[]) {
  if (argc > 2) {
    std::cerr << "Usage: mgm [listen_address]\n";
    return 2;
  }
  const std::string address = argc == 2 ? argv[1] : "127.0.0.1:50051";
  objectstore::MgmService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "Failed to start MGM on " << address << '\n';
    return 1;
  }
  std::cout << "MGM listening on " << address << '\n' << std::flush;
  server->Wait();
}
