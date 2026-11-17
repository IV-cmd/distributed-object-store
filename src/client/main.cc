#include <grpcpp/grpcpp.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "client/client.h"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: client put <file> <object_id> | get <object_id> "
                 "<out_file> | rm <object_id>\n"
                 "FST_ADDRESS defaults to 127.0.0.1:50052\n";
    return 2;
  }
  const std::string command = argv[1];
  if (!((command == "put" || command == "get") && argc == 4) &&
      !(command == "rm" && argc == 3)) {
    std::cerr << "Invalid command or argument count\n";
    return 2;
  }
  try {
    const char* configured = std::getenv("FST_ADDRESS");
    const std::string address = configured ? configured : "127.0.0.1:50052";
    objectstore::ObjectClient client(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    grpc::Status status;
    if (command == "put") {
      status = client.Put(argv[2], argv[3]);
    } else if (command == "get") {
      status = client.Get(argv[2], argv[3]);
    } else {
      status = client.Remove(argv[2]);
    }
    if (!status.ok()) {
      std::cerr << "RPC status " << status.error_code() << ": "
                << status.error_message() << '\n';
      return 1;
    }
    std::cout << command << " OK\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "client: " << error.what() << '\n';
    return 1;
  }
}
