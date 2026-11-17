#pragma once

#include <filesystem>
#include <functional>
#include <mutex>

#include "objectstore.grpc.pb.h"

namespace objectstore {

class FstService final : public v1::FstService::Service {
 public:
  explicit FstService(std::filesystem::path data_dir);

  grpc::Status BeginObject(grpc::ServerContext*, const v1::BeginObjectRequest*,
                           google::protobuf::Empty*) override;
  grpc::Status FinishObject(grpc::ServerContext*, const v1::ObjectRequest*,
                            google::protobuf::Empty*) override;
  grpc::Status GetObjectInfo(grpc::ServerContext*, const v1::ObjectRequest*,
                             v1::ObjectInfo*) override;
  grpc::Status DeleteObject(grpc::ServerContext*, const v1::ObjectRequest*,
                            google::protobuf::Empty*) override;
  grpc::Status WriteChunk(grpc::ServerContext*, const v1::WriteChunkRequest*,
                          google::protobuf::Empty*) override;
  grpc::Status ReadChunk(grpc::ServerContext*, const v1::ReadChunkRequest*,
                         v1::ReadChunkResponse*) override;
  grpc::Status DeleteChunk(grpc::ServerContext*, const v1::DeleteChunkRequest*,
                           google::protobuf::Empty*) override;

  void FillHeartbeat(v1::HeartbeatRequest& request);

 private:
  grpc::Status Run(const std::function<void()>& operation);
  std::filesystem::path ObjectDir(const std::string& object_id) const;

  std::filesystem::path data_dir_;
  std::mutex mutex_;
};

}  // namespace objectstore
