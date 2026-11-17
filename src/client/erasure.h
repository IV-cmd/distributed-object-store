#pragma once

#include <istream>
#include <ostream>

#include "objectstore.grpc.pb.h"

namespace objectstore {

grpc::Status PutErasure(v1::MgmService::Stub& mgm, std::istream& input,
                         const std::string& object_id, std::uint64_t size);
grpc::Status ReadErasure(const v1::FileMetadata& info, std::ostream& output);

}  // namespace objectstore
