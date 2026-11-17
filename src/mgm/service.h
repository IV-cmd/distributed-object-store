#pragma once

#include "objectstore.grpc.pb.h"

namespace objectstore {

class MgmService final : public v1::MgmService::Service {
 public:
  grpc::Status RegisterNode(grpc::ServerContext*,
                            const v1::RegisterNodeRequest* request,
                            v1::RegisterNodeResponse* response) override;
  grpc::Status Get(grpc::ServerContext*, const v1::GetRequest*,
                   v1::GetResponse*) override;
  grpc::Status Put(grpc::ServerContext*, const v1::PutRequest*,
                   v1::PutResponse*) override;
};

}  // namespace objectstore
