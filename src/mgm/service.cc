#include "mgm/service.h"

#include <iostream>
#include <syncstream>

namespace objectstore {

grpc::Status MgmService::RegisterNode(grpc::ServerContext*,
                                      const v1::RegisterNodeRequest* request,
                                      v1::RegisterNodeResponse* response) {
  const auto& node = request->node();
  if (node.node_id().empty() || node.endpoint().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "node_id and endpoint are required"};
  }
  response->set_accepted(true);
  std::osyncstream(std::cout)
      << "RegisterNode accepted: node_id=" << node.node_id()
      << " endpoint=" << node.endpoint() << '\n'
      << std::flush;
  return grpc::Status::OK;
}

grpc::Status MgmService::Get(grpc::ServerContext*, const v1::GetRequest*,
                             v1::GetResponse*) {
  return {grpc::StatusCode::UNIMPLEMENTED, "Get is not implemented yet"};
}

grpc::Status MgmService::Put(grpc::ServerContext*, const v1::PutRequest*,
                             v1::PutResponse*) {
  return {grpc::StatusCode::UNIMPLEMENTED, "Put is not implemented yet"};
}

}  // namespace objectstore
