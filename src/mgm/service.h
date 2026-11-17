#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "objectstore.grpc.pb.h"

namespace objectstore {

inline constexpr std::size_t kReplicationFactor = 3;

class MgmService final : public v1::MgmService::Service {
 public:
  grpc::Status RegisterNode(grpc::ServerContext*,
                            const v1::RegisterNodeRequest*,
                            v1::RegisterNodeResponse*) override;
  grpc::Status Put(grpc::ServerContext*, const v1::PutRequest*,
                   v1::PutResponse*) override;
  grpc::Status AllocateChunk(grpc::ServerContext*, const v1::ChunkId*,
                             v1::ReplicaPlan*) override;
  grpc::Status Get(grpc::ServerContext*, const v1::GetRequest*,
                   v1::GetResponse*) override;
  grpc::Status CommitChunk(grpc::ServerContext*, const v1::CommitChunkRequest*,
                           google::protobuf::Empty*) override;
  grpc::Status LocateChunk(grpc::ServerContext*, const v1::ChunkId*,
                           v1::ChunkMetadata*) override;
  grpc::Status StatFile(grpc::ServerContext*, const v1::ObjectRequest*,
                        v1::FileMetadata*) override;
  grpc::Status ListFiles(grpc::ServerContext*, const google::protobuf::Empty*,
                         v1::FileList*) override;
  grpc::Status CompleteFile(grpc::ServerContext*, const v1::ObjectRequest*,
                            google::protobuf::Empty*) override;
  grpc::Status DeleteFile(grpc::ServerContext*, const v1::ObjectRequest*,
                          google::protobuf::Empty*) override;

 private:
  struct File {
    v1::FileMetadata info;
    std::map<std::uint32_t, v1::ChunkMetadata> chunks;
  };

  static v1::FileMetadata Snapshot(const File& file);

  std::mutex mutex_;
  std::map<std::string, v1::NodeInfo> nodes_;
  std::map<std::string, File> files_;
};

}  // namespace objectstore
