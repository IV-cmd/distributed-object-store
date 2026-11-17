#include <grpcpp/grpcpp.h>

#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

#include "objectstore.grpc.pb.h"

int main(int argc, char* argv[]) {
  if (argc < 4 || !((std::string_view(argv[2]) == "get" && argc == 4) ||
                    (std::string_view(argv[2]) == "put" && argc == 5))) {
    std::cerr << "Usage: client <mgm_address> get <object_id>\n"
                 "       client <mgm_address> put <object_id> <size_bytes>\n";
    return 2;
  }
  std::uint64_t size_bytes = 0;
  if (argc == 5) {
    const std::string_view size_arg(argv[4]);
    const auto [end, error] = std::from_chars(
        size_arg.data(), size_arg.data() + size_arg.size(), size_bytes);
    if (error != std::errc{} || end != size_arg.data() + size_arg.size()) {
      std::cerr << "size_bytes must be an unsigned 64-bit integer\n";
      return 2;
    }
  }
  auto stub = objectstore::v1::MgmService::NewStub(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));
  context.set_wait_for_ready(true);
  grpc::Status status;
  if (std::string_view(argv[2]) == "get") {
    objectstore::v1::GetRequest request;
    request.set_object_id(argv[3]);
    objectstore::v1::GetResponse response;
    status = stub->Get(&context, request, &response);
    if (status.ok()) {
      std::cout << response.DebugString();
    }
  } else {
    objectstore::v1::PutRequest request;
    request.set_object_id(argv[3]);
    request.set_size_bytes(size_bytes);
    objectstore::v1::PutResponse response;
    status = stub->Put(&context, request, &response);
    if (status.ok()) {
      std::cout << response.DebugString();
    }
  }
  std::cout << "RPC status " << status.error_code() << ": "
            << status.error_message() << '\n';
  if (status.ok()) {
    return 0;
  }
  return status.error_code() == grpc::StatusCode::UNIMPLEMENTED ? 3 : 1;
}
