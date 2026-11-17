#include "mgm/service.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <set>
#include <syncstream>

#include "common/chunk.h"

namespace objectstore {
namespace {

using grpc::StatusCode;

bool ValidId(const std::string& id) {
  return !id.empty() && id.size() <= 128 &&
         id.find_first_not_of(
             "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") ==
             std::string::npos;
}

}  // namespace

v1::FileMetadata MgmService::Snapshot(const File& file) {
  auto info = file.info;
  for (const auto& [index, chunk] : file.chunks) {
    *info.add_chunks() = chunk;
  }
  return info;
}

grpc::Status MgmService::RegisterNode(grpc::ServerContext*,
                                      const v1::RegisterNodeRequest* request,
                                      v1::RegisterNodeResponse* response) {
  const auto& node = request->node();
  if (!ValidId(node.node_id()) || node.endpoint().empty()) {
    return {StatusCode::INVALID_ARGUMENT, "valid node ID and endpoint required"};
  }
  const std::lock_guard lock(mutex_);
  for (const auto& [id, existing] : nodes_) {
    if ((id == node.node_id()) != (existing.endpoint() == node.endpoint())) {
      return {StatusCode::ALREADY_EXISTS,
              "node IDs and endpoints must be unique and stable"};
    }
  }
  nodes_[node.node_id()] = node;
  response->set_accepted(true);
  std::osyncstream(std::cout)
      << "RegisterNode accepted: node_id=" << node.node_id()
      << " endpoint=" << node.endpoint() << " capacity=" << node.capacity_bytes()
      << '\n' << std::flush;
  return grpc::Status::OK;
}

grpc::Status MgmService::Put(grpc::ServerContext*, const v1::PutRequest* request,
                             v1::PutResponse*) {
  const auto count = ChunkCount(request->size_bytes());
  if (!ValidId(request->object_id()) ||
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
  info.set_name(request->name());
  info.set_size_bytes(request->size_bytes());
  info.set_chunk_count(static_cast<std::uint32_t>(count));
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
  std::set<std::string> ids;
  for (const auto& id : request->node_ids()) {
    const auto& targets = record.info.targets();
    if (!ids.insert(id).second ||
        std::none_of(targets.begin(), targets.end(),
                     [&](const auto& node) { return node.node_id() == id; })) {
      return {StatusCode::INVALID_ARGUMENT, "replicas must be distinct targets"};
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
  *response = chunk->second;
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
  if (record.chunks.size() != record.info.chunk_count() ||
      std::any_of(record.chunks.begin(), record.chunks.end(), [](const auto& item) {
        return !item.second.committed() ||
               item.second.replicas_size() != kReplicationFactor;
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
