#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "objectstore.grpc.pb.h"
#include "mgm/recovery.h"

namespace objectstore {

inline constexpr std::size_t kReplicationFactor = 3;

class MgmService final : public v1::MgmService::Service {
 public:
  explicit MgmService(std::chrono::milliseconds timeout =
                          std::chrono::milliseconds(6000)) : timeout_(timeout) {}
  void Maintain(HealthClock::time_point now = HealthClock::now());

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

  grpc::Status ReportChunkStatus(grpc::ServerContext*,
                                  const v1::ReportChunkStatusRequest*,
                                  google::protobuf::Empty*) override;
  grpc::Status Heartbeat(grpc::ServerContext*, const v1::HeartbeatRequest*,
                         v1::HeartbeatResponse*) override;
  grpc::Status ListNodes(grpc::ServerContext*, const google::protobuf::Empty*,
                         v1::NodeList*) override;

 private:
  struct Node {
    v1::NodeInfo info;
    std::chrono::steady_clock::time_point last_heartbeat;
  };

  struct File {
    v1::FileMetadata info;
    std::map<std::uint32_t, v1::ChunkMetadata> chunks;
  };

  v1::FileMetadata Snapshot(const File& file) const;
  v1::ChunkMetadata LiveChunk(const v1::ChunkMetadata& chunk) const;

  std::chrono::milliseconds timeout_;
  std::uint64_t next_generation_ = 0;
  std::size_t repair_cursor_ = 0;
  std::mutex maintenance_mutex_;

  std::mutex mutex_;
  std::map<std::string, Node> nodes_;
  std::map<std::string, File> files_;
};

}  // namespace objectstore
