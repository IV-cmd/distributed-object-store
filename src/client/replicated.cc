#include "client/client.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <fstream>
#include <set>
#include <stdexcept>
#include <vector>

#include "common/chunk.h"

namespace objectstore {
namespace {

void Deadline(grpc::ClientContext& context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));
}

struct Replica {
  v1::NodeInfo node;
  std::unique_ptr<v1::FstService::Stub> stub;
  grpc::Status status;
};

std::vector<Replica> StartReplicas(const v1::ReplicaPlan& plan,
                                  const v1::BeginObjectRequest& begin) {
  if (plan.nodes_size() != 3) {
    throw std::runtime_error("MGM did not allocate three replicas");
  }
  std::vector<Replica> replicas;
  std::set<std::string> ids;
  std::set<std::string> endpoints;
  for (const auto& node : plan.nodes()) {
    if (!ids.insert(node.node_id()).second ||
        !endpoints.insert(node.endpoint()).second) {
      throw std::runtime_error("MGM returned duplicate replica targets");
    }
    auto stub = v1::FstService::NewStub(
        grpc::CreateChannel(node.endpoint(), grpc::InsecureChannelCredentials()));
    google::protobuf::Empty response;
    grpc::ClientContext context;
    Deadline(context);
    const auto status = stub->BeginObject(&context, begin, &response);
    replicas.push_back({node, std::move(stub), status});
  }
  return replicas;
}

grpc::Status ReplicateChunk(v1::MgmService::Stub& mgm,
                             const v1::WriteChunkRequest& request,
                             std::vector<Replica>& replicas) {
  v1::CommitChunkRequest commit;
  *commit.mutable_chunk() = request.chunk();
  commit.set_checksum(request.checksum());
  grpc::Status failure;
  google::protobuf::Empty empty;
  for (auto& replica : replicas) {
    if (replica.status.ok()) {
      grpc::ClientContext context;
      Deadline(context);
      replica.status = replica.stub->WriteChunk(&context, request, &empty);
    }
    if (replica.status.ok()) {
      commit.add_node_ids(replica.node.node_id());
    } else {
      failure = replica.status;
    }
  }
  grpc::ClientContext context;
  Deadline(context);
  const auto status = mgm.CommitChunk(&context, commit, &empty);
  if (!status.ok()) {
    return {status.error_code(),
            "replica writes attempted but metadata commit failed: " +
                status.error_message()};
  }
  if (!failure.ok()) {
    return {failure.error_code(),
            "partial write: chunk=" + std::to_string(request.chunk().index()) +
                " committed_replicas=" + std::to_string(commit.node_ids_size()) +
                "/3; " + failure.error_message()};
  }
  return grpc::Status::OK;
}

grpc::Status FinishReplicas(const v1::ObjectRequest& request,
                             std::vector<Replica>& replicas) {
  grpc::Status failure;
  for (auto& replica : replicas) {
    google::protobuf::Empty response;
    grpc::ClientContext context;
    Deadline(context);
    const auto status = replica.stub->FinishObject(&context, request, &response);
    if (!status.ok()) {
      failure = {status.error_code(),
                 "FST finish failed; MGM file remains incomplete: " +
                     status.error_message()};
    }
  }
  return failure;
}

}  // namespace

ReplicatedClient::ReplicatedClient(const std::shared_ptr<grpc::Channel>& channel)
    : stub_(v1::MgmService::NewStub(channel)) {}

grpc::Status ReplicatedClient::Put(const std::filesystem::path& file,
                                    const std::string& object_id) {
  std::ifstream input(file, std::ios::binary);
  if (!input) {
    throw std::runtime_error("cannot open input file");
  }
  const auto size = std::filesystem::file_size(file);
  v1::PutRequest create;
  create.set_object_id(object_id);
  create.set_name(file.filename().string());
  create.set_size_bytes(size);
  v1::PutResponse response;
  grpc::ClientContext create_context;
  Deadline(create_context);
  auto status = stub_->Put(&create_context, create, &response);
  if (!status.ok()) {
    return status;
  }
  std::vector<Replica> replicas;
  std::string bytes;
  std::uint64_t sent = 0;
  std::uint32_t index = 0;
  while (ReadNextChunk(input, bytes)) {
    if (bytes.size() > size - sent) {
      throw std::runtime_error("input grew during upload; remove partial object");
    }
    v1::ChunkId chunk;
    chunk.set_object_id(object_id);
    chunk.set_index(index++);
    v1::ReplicaPlan plan;
    grpc::ClientContext allocate_context;
    Deadline(allocate_context);
    status = stub_->AllocateChunk(&allocate_context, chunk, &plan);
    if (!status.ok()) {
      return status;
    }
    if (replicas.empty()) {
      v1::BeginObjectRequest begin;
      begin.set_object_id(object_id);
      begin.set_size_bytes(size);
      replicas = StartReplicas(plan, begin);
    }
    v1::WriteChunkRequest write;
    *write.mutable_chunk() = chunk;
    write.set_data(bytes);
    write.set_checksum(Checksum(bytes));
    status = ReplicateChunk(*stub_, write, replicas);
    if (!status.ok()) {
      return status;
    }
    sent += bytes.size();
  }
  if (sent != size) {
    throw std::runtime_error("input shrank during upload; remove partial object");
  }
  v1::ObjectRequest finish;
  finish.set_object_id(object_id);
  status = FinishReplicas(finish, replicas);
  if (!status.ok()) {
    return status;
  }
  google::protobuf::Empty empty;
  grpc::ClientContext context;
  Deadline(context);
  return stub_->CompleteFile(&context, finish, &empty);
}

grpc::Status ReplicatedClient::Stat(const std::string& object_id,
                                     v1::FileMetadata& metadata) {
  v1::ObjectRequest request;
  request.set_object_id(object_id);
  grpc::ClientContext context;
  Deadline(context);
  return stub_->StatFile(&context, request, &metadata);
}

grpc::Status ReplicatedClient::List(v1::FileList& files) {
  google::protobuf::Empty request;
  grpc::ClientContext context;
  Deadline(context);
  return stub_->ListFiles(&context, request, &files);
}

}  // namespace objectstore
