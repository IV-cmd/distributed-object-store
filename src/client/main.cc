#include <grpcpp/grpcpp.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "client/client.h"

namespace {

void Print(const objectstore::v1::FileMetadata& info) {
  std::cout << "object_id=" << info.object_id() << " name=" << info.name()
            << " size=" << info.size_bytes() << " chunks=" << info.chunk_count()
            << " complete=" << std::boolalpha << info.complete() << '\n';
  for (const auto& chunk : info.chunks()) {
    std::cout << "chunk=" << chunk.chunk().index()
              << " checksum=" << chunk.checksum()
              << " replicas=" << chunk.replicas_size() << " nodes=";
    for (int index = 0; index < chunk.replicas_size(); ++index) {
      if (index != 0) {
        std::cout << ',';
      }
      std::cout << chunk.replicas(index).node_id();
    }
    std::cout << '\n';
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  const std::string command = argc >= 2 ? argv[1] : "";
  if (!((command == "put" || command == "get") && argc == 4) &&
      !((command == "rm" || command == "stat") && argc == 3) &&
      !((command == "ls" || command == "nodes") && argc == 2)) {
    std::cerr << "Usage: client put <file> <object_id> | get <object_id> "
                 "<out_file> | rm <object_id> | stat <object_id> | ls | nodes\n"
                 "MGM_ADDRESS defaults to 127.0.0.1:50051\n";
    return 2;
  }
  try {
    const char* configured = std::getenv("MGM_ADDRESS");
    const std::string address = configured ? configured : "127.0.0.1:50051";
    objectstore::ReplicatedClient client(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    grpc::Status status;
    if (command == "put") {
      status = client.Put(argv[2], argv[3]);
    } else if (command == "get") {
      status = client.Get(argv[2], argv[3]);
    } else if (command == "rm") {
      status = client.Remove(argv[2]);
    } else if (command == "stat") {
      objectstore::v1::FileMetadata info;
      status = client.Stat(argv[2], info);
      if (status.ok()) {
        Print(info);
      }
    } else if (command == "nodes") {
      objectstore::v1::NodeList nodes;
      status = client.Nodes(nodes);
      if (status.ok()) {
        for (const auto& node : nodes.nodes()) {
          std::cout << "node_id=" << node.node_id()
                    << " state=" << objectstore::v1::NodeState_Name(node.state())
                    << " chunks=" << node.chunk_count()
                    << " free_bytes=" << node.free_bytes() << '\n';
        }
      }
    } else {
      objectstore::v1::FileList files;
      status = client.List(files);
      if (status.ok()) {
        for (const auto& info : files.files()) {
          Print(info);
        }
      }
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
