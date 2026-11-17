#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "objectstore.grpc.pb.h"

namespace objectstore {

grpc::Status FetchStripe(
    const google::protobuf::RepeatedPtrField<v1::ChunkMetadata>& chunks,
    int begin, std::uint64_t object_size, std::string& data,
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::time_point::max());

}  // namespace objectstore
