#include "mgm/service.h"

#include <map>
#include <set>
#include <utility>

#include "common/log.h"

namespace objectstore {

grpc::Status MgmService::RegisterNode(grpc::ServerContext*,
                                      const v1::RegisterNodeRequest* request,
                                      v1::RegisterNodeResponse* response) {
  const auto& node = request->node();
  if (node.node_id().empty() || node.node_id().size() > 128 ||
      node.node_id().find_first_not_of(
          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") !=
          std::string::npos || node.endpoint().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "valid node ID and endpoint required"};
  }
  const std::lock_guard lock(mutex_);
  for (const auto& [id, existing] : nodes_) {
    if ((id == node.node_id()) != (existing.info.endpoint() == node.endpoint())) {
      return {grpc::StatusCode::ALREADY_EXISTS,
              "node IDs and endpoints must be unique and stable"};
    }
  }
  const auto [entry, inserted] = nodes_.try_emplace(node.node_id());
  auto& record = entry->second;
  const auto incarnation = record.info.incarnation() + 1;
  record.info = node;
  record.info.set_incarnation(incarnation);
  record.info.set_state(inserted ? v1::UP : v1::REJOINING);
  record.last_heartbeat = std::chrono::steady_clock::now();
  response->set_accepted(true);
  Log(inserted ? "node_registered" : "node_rejoining", "node=" + node.node_id() + " endpoint=" + node.endpoint());
  return grpc::Status::OK;
}

grpc::Status MgmService::Heartbeat(grpc::ServerContext*,
                                   const v1::HeartbeatRequest* request,
                                   v1::HeartbeatResponse* response) {
  const std::lock_guard lock(mutex_);
  const auto node = nodes_.find(request->node_id());
  if (node == nodes_.end()) {
    return {grpc::StatusCode::NOT_FOUND, "node must register first"};
  }
  auto& record = node->second;
  if (request->endpoint() != record.info.endpoint()) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "heartbeat endpoint changed"};
  }
  if (record.info.state() == v1::DOWN) {
    record.info.set_state(v1::REJOINING);
    record.info.set_incarnation(record.info.incarnation() + 1);
    Log("node_rejoining", "node=" + request->node_id());
  }
  response->set_report_inventory(record.info.state() == v1::REJOINING);
  response->set_incarnation(record.info.incarnation());
  record.last_heartbeat = std::chrono::steady_clock::now();
  record.info.set_chunk_count(request->chunk_count());
  record.info.set_free_bytes(request->free_bytes());
  return grpc::Status::OK;
}

grpc::Status MgmService::ListNodes(grpc::ServerContext*,
                                   const google::protobuf::Empty*,
                                   v1::NodeList* response) {
  const std::lock_guard lock(mutex_);
  for (const auto& [id, node] : nodes_) {
    *response->add_nodes() = node.info;
  }
  return grpc::Status::OK;
}

grpc::Status MgmService::ReportChunkStatus(
    grpc::ServerContext*, const v1::ReportChunkStatusRequest* request,
    google::protobuf::Empty*) {
  const std::lock_guard lock(mutex_);
  const auto node = nodes_.find(request->node_id());
  if (node == nodes_.end()) {
    return {grpc::StatusCode::NOT_FOUND, "node must register first"};
  }
  auto& record = node->second;
  if (record.info.state() != v1::REJOINING ||
      record.info.incarnation() != request->incarnation() ||
      record.info.endpoint() != request->endpoint()) {
    return {grpc::StatusCode::FAILED_PRECONDITION, "inventory request is stale"};
  }
  using Key = std::pair<std::string, std::uint32_t>;
  std::map<Key, const v1::ChunkStatus*> reported;
  for (const auto& chunk : request->chunks()) {
    if (!reported.emplace(Key{chunk.chunk().object_id(), chunk.chunk().index()},
                          &chunk).second) {
      return {grpc::StatusCode::INVALID_ARGUMENT, "duplicate inventory entry"};
    }
  }
  std::set<Key> expected;
  std::size_t verified = 0;
  std::size_t missing = 0;
  std::size_t corrupt = 0;
  for (auto& [id, file] : files_) {
    if (!file.info.complete()) {
      continue;
    }
    for (auto& [index, chunk] : file.chunks) {
      auto holders = chunk.replicas();
      chunk.clear_replicas();
      for (const auto& holder : holders) {
        if (holder.node_id() != request->node_id()) {
          *chunk.add_replicas() = holder;
          continue;
        }
        const Key key{id, index};
        expected.insert(key);
        const auto found = reported.find(key);
        const bool absent = found == reported.end() ||
                            found->second->state() == v1::ChunkStatus::MISSING;
        const bool valid = !absent &&
            found->second->state() == v1::ChunkStatus::HEALTHY &&
            found->second->checksum() == chunk.checksum() &&
            found->second->size_bytes() == file.info.size_bytes();
        auto invalid = chunk.invalid_replicas();
        chunk.clear_invalid_replicas();
        for (const auto& rejected : invalid) {
          if (rejected != request->node_id()) {
            chunk.add_invalid_replicas(rejected);
          }
        }
        if (absent) {
          ++missing;
        } else {
          *chunk.add_replicas() = holder;
          if (valid) {
            ++verified;
          } else {
            ++corrupt;
            chunk.add_invalid_replicas(request->node_id());
          }
        }
      }
    }
  }
  std::size_t unexpected = 0;
  for (const auto& [key, chunk] : reported) {
    if (!expected.contains(key)) {
      ++unexpected;
      Log("unexpected_chunk", "node=" + request->node_id() + " object=" +
                                  key.first + " chunk=" + std::to_string(key.second));
    }
  }
  record.info.set_state(v1::UP);
  record.last_heartbeat = HealthClock::now();
  Log("node_up", "node=" + request->node_id() + " verified=" +
        std::to_string(verified) + " missing=" + std::to_string(missing) +
        " corrupt=" + std::to_string(corrupt) + " unexpected=" +
        std::to_string(unexpected));
  return grpc::Status::OK;
}

}  // namespace objectstore
