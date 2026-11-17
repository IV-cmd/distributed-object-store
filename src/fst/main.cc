#include <grpcpp/grpcpp.h>

#include <exception>
#include <iostream>
#include <string>

#include "fst/service.h"

int main(int argc, char* argv[]) {
  if (argc > 3) {
    std::cerr << "Usage: fst [listen_address] [data_dir]\n";
    return 2;
  }
  try {
    const std::string address = argc >= 2 ? argv[1] : "127.0.0.1:50052";
    objectstore::FstService service(argc == 3 ? argv[2] : "data");
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "Failed to start FST on " << address << '\n';
      return 1;
    }
    std::cout << "FST listening on " << address << '\n' << std::flush;
    server->Wait();
  } catch (const std::exception& error) {
    std::cerr << "FST: " << error.what() << '\n';
    return 1;
  }
}
