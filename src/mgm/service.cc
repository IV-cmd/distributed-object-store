#include "mgm/service.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <set>
#include <syncstream>
#include <utility>
#include <vector>

#include "common/chunk.h"
#include "common/layout.h"
#include "mgm/placement.h"

namespace objectstore {
namespace {

using grpc::StatusCode;

bool ValidId(const std::string& id) {
  return !id.empty() && id.size() <= 128 &&
         id.find_first_not_of(
             "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
             "-") == std::string::npos;
}

}  // namespace

v1::ChunkMetadata MgmService::LiveChunk(const v1::ChunkMetadata& chunk) const {
  auto result = chunk;
  result.clear_replicas();
  result.clear_known_replicas();
  const std::set<std::string> invalid(chunk.invalid_replicas().begin(),
                                      chunk.invalid_replicas().end());
  for (const auto& holder : chunk.replicas()) {
    const auto node = nodes_.find(holder.node_id());
    if (node == nodes_.end()) {
      continue;
    }
    *result.add_known_replicas() = node->second.info;
    if (node->second.info.state() == v1::UP &&
        !invalid.contains(holder.node_id())) {
      *result.add_replicas() = node->second.info;
    }
  }
  return result;
}

v1::FileMetadata MgmService::Snapshot(const File& file) const {
  auto info = file.info;
  for (const auto& [index, chunk] : file.chunks) {
    *info.add_chunks() = LiveChunk(chunk);
  }
  return info;
}

grpc::Status MgmService::Put(grpc::ServerContext*,
                             const v1::PutRequest* request, v1::PutResponse*) {
  const auto count = LayoutChunkCount(request->layout(), request->size_bytes());
  if (!v1::Layout_IsValid(request->layout()) ||
      !ValidId(request->object_id()) ||
      count > std::numeric_limits<std::uint32_t>::max()) {
    return {StatusCode::INVALID_ARGUMENT, "invalid object ID or size"};
  }
  const std::lock_guard lock(mutex_);
  auto [it, inserted] = files_.try_emplace(request->object_id());
  if (!inserted) {
    return {StatusCode::ALREADY_EXISTS, "object ID already exists"};
  }
  auto& info = it->second.info;
  info.set_object_id(request->object_id());
  info.set_generation(++next_generation_);
  info.set_name(request->name().empty() ? request->object_id()
                                        : request->name());
  info.set_size_bytes(request->size_bytes());
  info.set_layout(request->layout());
  info.set_chunk_count(static_cast<std::uint32_t>(count));
  return grpc::Status::OK;
}

grpc::Status MgmService::AllocateChunk(grpc::ServerContext*,
                                       const v1::ChunkId* request,
                                       v1::ReplicaPlan* response) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  auto& record = file->second;
  if (request->index() >= record.info.chunk_count()) {
    return {StatusCode::INVALID_ARGUMENT, "chunk index out of range"};
  }
  if (record.info.complete()) {
    return {StatusCode::FAILED_PRECONDITION, "completed files are write-once"};
  }
  if (record.info.targets().empty()) {
    std::vector<v1::NodeInfo> available;
    available.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
      available.push_back(node.info);
    }
    const std::size_t required =
        record.info.layout() == v1::RS42 ? 6 : kReplicationFactor;
    const auto selected = SelectNodes(std::move(available), required);
    if (selected.size() != required) {
      return {StatusCode::RESOURCE_EXHAUSTED,
              "layout requires more distinct healthy FSTs"};
    }
    for (const auto& node : selected) {
      *record.info.add_targets() = node;
    }
  }
  auto [chunk, inserted] = record.chunks.try_emplace(request->index());
  if (inserted) {
    *chunk->second.mutable_chunk() = *request;
    if (record.info.layout() == v1::RS42) {
      chunk->second.set_stripe_index(request->index() % 6);
      chunk->second.set_parity(request->index() % 6 >= 4);
    }
  }
  if (record.info.layout() == v1::RS42) {
    *response->add_nodes() = record.info.targets(request->index() % 6);
  } else {
    *response->mutable_nodes() = record.info.targets();
  }
  return grpc::Status::OK;
}

grpc::Status MgmService::Get(grpc::ServerContext*,
                             const v1::GetRequest* request,
                             v1::GetResponse* response) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  if (!file->second.info.complete()) {
    return {StatusCode::FAILED_PRECONDITION, "file upload is incomplete"};
  }
  for (const auto& [index, chunk] : file->second.chunks) {
    const auto live = LiveChunk(chunk);
    for (const auto& node : live.replicas()) {
      auto* location = response->mutable_plan()->add_chunks();
      *location->mutable_chunk() = chunk.chunk();
      *location->mutable_node() = node;
    }
  }
  return grpc::Status::OK;
}

grpc::Status MgmService::CommitChunk(grpc::ServerContext*,
                                     const v1::CommitChunkRequest* request,
                                     google::protobuf::Empty*) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->chunk().object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  auto& record = file->second;
  const auto chunk = record.chunks.find(request->chunk().index());
  if (record.info.complete() || chunk == record.chunks.end()) {
    return {StatusCode::FAILED_PRECONDITION, "chunk must be allocated first"};
  }
  if (record.info.layout() == v1::RS42 &&
      (request->node_ids_size() > 1 ||
       (request->node_ids_size() == 1 &&
        request->node_ids(0) !=
            record.info.targets(request->chunk().index() % 6).node_id()))) {
    return {StatusCode::INVALID_ARGUMENT,
            "RS shard must use its assigned target"};
  }
  std::set<std::string> ids;
  for (const auto& id : request->node_ids()) {
    const auto& targets = record.info.targets();
    if (!ids.insert(id).second ||
        std::none_of(targets.begin(), targets.end(),
                     [&](const auto& node) { return node.node_id() == id; })) {
      return {StatusCode::INVALID_ARGUMENT,
              "replicas must be distinct targets"};
    }
  }
  auto& metadata = chunk->second;
  if (metadata.committed()) {
    return {StatusCode::ALREADY_EXISTS, "chunk already committed"};
  }
  metadata.set_checksum(request->checksum());
  metadata.set_committed(true);
  for (const auto& target : record.info.targets()) {
    if (ids.contains(target.node_id())) {
      *metadata.add_replicas() = target;
    }
  }
  return grpc::Status::OK;
}

grpc::Status MgmService::LocateChunk(grpc::ServerContext*,
                                     const v1::ChunkId* request,
                                     v1::ChunkMetadata* response) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  if (request->index() >= file->second.info.chunk_count()) {
    return {StatusCode::INVALID_ARGUMENT, "chunk index out of range"};
  }
  const auto chunk = file->second.chunks.find(request->index());
  if (chunk == file->second.chunks.end() || !chunk->second.committed()) {
    return {StatusCode::FAILED_PRECONDITION, "chunk is not committed"};
  }
  *response = LiveChunk(chunk->second);
  return grpc::Status::OK;
}

grpc::Status MgmService::StatFile(grpc::ServerContext*,
                                  const v1::ObjectRequest* request,
                                  v1::FileMetadata* response) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  *response = Snapshot(file->second);
  return grpc::Status::OK;
}

grpc::Status MgmService::ListFiles(grpc::ServerContext*,
                                   const google::protobuf::Empty*,
                                   v1::FileList* response) {
  const std::lock_guard lock(mutex_);
  for (const auto& [id, file] : files_) {
    *response->add_files() = Snapshot(file);
  }
  return grpc::Status::OK;
}

grpc::Status MgmService::CompleteFile(grpc::ServerContext*,
                                      const v1::ObjectRequest* request,
                                      google::protobuf::Empty*) {
  const std::lock_guard lock(mutex_);
  const auto file = files_.find(request->object_id());
  if (file == files_.end()) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  auto& record = file->second;
  const std::size_t copies =
      record.info.layout() == v1::RS42 ? 1 : kReplicationFactor;
  if (record.chunks.size() != record.info.chunk_count() ||
      std::any_of(record.chunks.begin(), record.chunks.end(),
                  [this, copies](const auto& item) {
                    return !item.second.committed() ||
                           LiveChunk(item.second).replicas_size() != copies;
                  })) {
    return {StatusCode::FAILED_PRECONDITION, "file is not fully replicated"};
  }
  record.info.set_complete(true);
  return grpc::Status::OK;
}

grpc::Status MgmService::DeleteFile(grpc::ServerContext*,
                                    const v1::ObjectRequest* request,
                                    google::protobuf::Empty*) {
  const std::lock_guard lock(mutex_);
  if (files_.erase(request->object_id()) == 0) {
    return {StatusCode::NOT_FOUND, "file not found"};
  }
  return grpc::Status::OK;
}

}  // namespace objectstore
