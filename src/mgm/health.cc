#include "mgm/service.h"

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

}  // namespace objectstore
