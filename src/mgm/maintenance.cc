#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <vector>

#include "common/log.h"
#include "mgm/service.h"

namespace objectstore {

void MgmService::Maintain(HealthClock::time_point now) {
  const std::lock_guard serial(maintenance_mutex_);
  std::vector<v1::FileMetadata> files;
  std::vector<v1::NodeInfo> nodes;
  {
    const std::lock_guard lock(mutex_);
    std::vector<NodeWatch> watches;
    watches.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
      watches.push_back({node.info, node.last_heartbeat});
    }
    for (const auto& id : ExpiredNodes(watches, now, timeout_)) {
      auto& node = nodes_.at(id);
      node.info.set_state(v1::DOWN);
      node.info.set_incarnation(node.info.incarnation() + 1);
      std::size_t affected = 0;
      for (const auto& [file_id, file] : files_) {
        for (const auto& [index, chunk] : file.chunks) {
          for (const auto& holder : chunk.replicas()) {
            if (holder.node_id() == id) {
              ++affected;
            }
          }
        }
      }
      Log("node_down",
          "node=" + id + " affected_chunks=" + std::to_string(affected));
    }
    files.reserve(files_.size());
    for (const auto& [id, file] : files_) {
      files.push_back(Snapshot(file));
    }
    nodes.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
      nodes.push_back(node.info);
    }
  }
  const auto tasks = DecideRepairs(files, nodes);
  if (tasks.empty()) {
    return;
  }
  const auto& task = tasks[repair_cursor_++ % tasks.size()];
  const auto& id = task.chunk.chunk();
  for (const auto& source : task.sources) {
    Log("repair_started",
        "object=" + id.object_id() + " chunk=" + std::to_string(id.index()) +
            " source=" + source.node_id() + " target=" + task.target.node_id());
    auto stub = v1::FstService::NewStub(grpc::CreateChannel(
        source.endpoint(), grpc::InsecureChannelCredentials()));
    v1::CopyChunkRequest request;
    *request.mutable_chunk() = id;
    *request.mutable_target() = task.target;
    request.set_checksum(task.chunk.checksum());
    request.set_size_bytes(task.size_bytes);
    for (const auto& shard : task.stripe) {
      *request.add_stripe() = shard;
    }
    google::protobuf::Empty empty;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    const auto status = stub->CopyChunk(&context, request, &empty);
    const std::lock_guard lock(mutex_);
    const auto file = files_.find(id.object_id());
    if (file == files_.end() ||
        file->second.info.generation() != task.generation) {
      Log("repair_discarded",
          "object=" + id.object_id() + " reason=stale_file");
      return;
    }
    auto& chunk = file->second.chunks.at(id.index());
    if (!status.ok()) {
      Log("repair_failed", "object=" + id.object_id() +
                               " chunk=" + std::to_string(id.index()) +
                               " source=" + source.node_id() +
                               " code=" + std::to_string(status.error_code()));
      if (task.stripe.empty() &&
          (status.error_code() == grpc::StatusCode::DATA_LOSS ||
           status.error_code() == grpc::StatusCode::NOT_FOUND) &&
          status.error_message().starts_with("SOURCE_CORRUPT:") &&
          nodes_.at(source.node_id()).info.incarnation() ==
              source.incarnation()) {
        const auto& invalid = chunk.invalid_replicas();
        if (std::find(invalid.begin(), invalid.end(), source.node_id()) ==
            invalid.end()) {
          chunk.add_invalid_replicas(source.node_id());
        }
        Log("replica_quarantined",
            "object=" + id.object_id() + " node=" + source.node_id());
      }
      continue;
    }
    const auto target = nodes_.find(task.target.node_id());
    if (target == nodes_.end() || target->second.info.state() != v1::UP ||
        target->second.info.incarnation() != task.target.incarnation()) {
      Log("repair_discarded",
          "object=" + id.object_id() + " reason=stale_target");
      return;
    }
    *chunk.add_replicas() = target->second.info;
    auto* targets = file->second.info.mutable_targets();
    if (std::none_of(targets->begin(), targets->end(), [&](const auto& node) {
          return node.node_id() == task.target.node_id();
        })) {
      *targets->Add() = target->second.info;
    }
    Log("repair_completed",
        "object=" + id.object_id() + " chunk=" + std::to_string(id.index()) +
            " target=" + task.target.node_id() +
            " replicas=" + std::to_string(LiveChunk(chunk).replicas_size()));
    return;
  }
}

}  // namespace objectstore
