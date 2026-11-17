find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(proto_file "${CMAKE_CURRENT_SOURCE_DIR}/objectstore.proto")
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${generated_dir}")

add_custom_command(
  OUTPUT
    "${generated_dir}/objectstore.pb.cc"
    "${generated_dir}/objectstore.pb.h"
    "${generated_dir}/objectstore.grpc.pb.cc"
    "${generated_dir}/objectstore.grpc.pb.h"
  COMMAND protobuf::protoc
  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
    --proto_path=${Protobuf_INCLUDE_DIRS}
    --cpp_out=${generated_dir}
    --grpc_out=${generated_dir}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    "${proto_file}"
  DEPENDS "${proto_file}" protobuf::protoc gRPC::grpc_cpp_plugin
  VERBATIM
)

add_library(objectstore_proto
  "${generated_dir}/objectstore.pb.cc"
  "${generated_dir}/objectstore.grpc.pb.cc"
)
target_include_directories(objectstore_proto SYSTEM PUBLIC "${generated_dir}")
target_link_libraries(objectstore_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
