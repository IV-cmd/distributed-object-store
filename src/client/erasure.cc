#include "client/erasure.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <set>

#include "common/chunk.h"
#include "common/rs_codec.h"
#include "common/rs_transfer.h"

namespace objectstore {
namespace {

void Deadline(grpc::ClientContext& context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));
}

}  // namespace

grpc::Status PutErasure(v1::MgmService::Stub& mgm, std::istream& input,
                        const std::string& object_id, std::uint64_t size) {
  std::set<std::string> initialized;
  std::uint64_t sent = 0;
  std::uint32_t index = 0;
  while (sent < size) {
    const auto length = std::min<std::uint64_t>(4 * kChunkSize, size - sent);
    std::string stripe(length, '\0');
    input.read(stripe.data(), static_cast<std::streamsize>(length));
    if (input.gcount() != static_cast<std::streamsize>(length)) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "input changed during RS upload"};
    }
    const auto shards = RsEncode(stripe);
    std::set<std::string> nodes, endpoints;
    for (const auto& bytes : shards) {
      v1::ChunkId id;
      id.set_object_id(object_id);
      id.set_index(index++);
      v1::ReplicaPlan plan;
      grpc::ClientContext allocation;
      Deadline(allocation);
      auto status = mgm.AllocateChunk(&allocation, id, &plan);
      if (!status.ok()) {
        return status;
      }
      if (plan.nodes_size() != 1 ||
          !nodes.insert(plan.nodes(0).node_id()).second ||
          !endpoints.insert(plan.nodes(0).endpoint()).second) {
        return {grpc::StatusCode::DATA_LOSS,
                "RS requires six distinct placement targets"};
      }
      const auto& node = plan.nodes(0);
      auto fst = v1::FstService::NewStub(grpc::CreateChannel(
          node.endpoint(), grpc::InsecureChannelCredentials()));
      google::protobuf::Empty empty;
      if (initialized.insert(node.node_id()).second) {
        v1::BeginObjectRequest begin;
        begin.set_object_id(object_id);
        begin.set_size_bytes(size);
        begin.set_layout(v1::RS42);
        grpc::ClientContext context;
        Deadline(context);
        status = fst->BeginObject(&context, begin, &empty);
      }
      v1::WriteChunkRequest write;
      *write.mutable_chunk() = id;
      write.set_data(bytes);
      write.set_checksum(Checksum(bytes));
      if (status.ok()) {
        grpc::ClientContext context;
        Deadline(context);
        status = fst->WriteChunk(&context, write, &empty);
      }
      v1::CommitChunkRequest commit;
      *commit.mutable_chunk() = id;
      commit.set_checksum(write.checksum());
      if (status.ok()) {
        commit.add_node_ids(node.node_id());
      }
      grpc::ClientContext context;
      Deadline(context);
      const auto recorded = mgm.CommitChunk(&context, commit, &empty);
      if (!recorded.ok()) {
        return recorded;
      }
      if (!status.ok()) {
        return {status.error_code(),
                "RS shard write failed; file remains incomplete: " +
                    status.error_message()};
      }
    }
    sent += length;
  }
  if (input.peek() != std::char_traits<char>::eof()) {
    return {grpc::StatusCode::FAILED_PRECONDITION,
            "input grew during RS upload"};
  }
  v1::ObjectRequest finish;
  finish.set_object_id(object_id);
  google::protobuf::Empty empty;
  grpc::ClientContext context;
  Deadline(context);
  return mgm.CompleteFile(&context, finish, &empty);
}

grpc::Status ReadErasure(const v1::FileMetadata& info, std::ostream& output) {
  if (info.chunks_size() != static_cast<int>(info.chunk_count())) {
    return {grpc::StatusCode::DATA_LOSS, "incomplete RS file metadata"};
  }
  for (int begin = 0; begin < info.chunks_size(); begin += 6) {
    const auto& id = info.chunks(begin).chunk();
    if (id.object_id() != info.object_id() ||
        id.index() != static_cast<std::uint32_t>(begin)) {
      return {grpc::StatusCode::DATA_LOSS,
              "RS stripe disagrees with file metadata"};
    }
    std::string data;
    const auto status =
        FetchStripe(info.chunks(), begin, info.size_bytes(), data);
    if (!status.ok()) {
      return status;
    }
    output.write(data.data(), static_cast<std::streamsize>(data.size()));
  }
  return grpc::Status::OK;
}

}  // namespace objectstore
