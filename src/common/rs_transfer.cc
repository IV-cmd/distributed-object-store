#include "common/rs_transfer.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/chunk.h"
#include "common/layout.h"
#include "common/rs_codec.h"

namespace objectstore {

grpc::Status FetchStripe(
    const google::protobuf::RepeatedPtrField<v1::ChunkMetadata>& chunks,
    int begin, std::uint64_t object_size, std::string& data,
    std::chrono::system_clock::time_point deadline) {
  using grpc::StatusCode;
  if (begin < 0 || chunks.size() - begin < 6) {
    return {StatusCode::DATA_LOSS, "incomplete RS stripe metadata"};
  }
  const auto& first = chunks.Get(begin).chunk();
  if (first.index() % 6 != 0 ||
      first.index() >= LayoutChunkCount(v1::RS42, object_size)) {
    return {StatusCode::DATA_LOSS, "invalid RS stripe index"};
  }
  const auto size = StripeSize(object_size, first.index());
  const auto width = size / 4 + (size % 4 != 0);
  for (int i = 0; i < 6; ++i) {
    const auto& chunk = chunks.Get(begin + i);
    if (!chunk.committed() || chunk.chunk().object_id() != first.object_id() ||
        chunk.chunk().index() != first.index() + i ||
        chunk.stripe_index() != static_cast<unsigned>(i) ||
        chunk.parity() != (i >= 4)) {
      return {StatusCode::DATA_LOSS, "inconsistent RS stripe metadata"};
    }
  }
  RsShards buffers;
  std::vector<RsShard> available;
  for (int i = 0; i < 6 && available.size() < 4; ++i) {
    const auto& chunk = chunks.Get(begin + i);
    for (const auto& node : chunk.replicas()) {
      auto stub = v1::FstService::NewStub(grpc::CreateChannel(
          node.endpoint(), grpc::InsecureChannelCredentials()));
      v1::ReadChunkRequest request;
      *request.mutable_chunk() = chunk.chunk();
      v1::ReadChunkResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::min(deadline, std::chrono::system_clock::now() +
                                                  std::chrono::seconds(5)));
      auto status = stub->ReadChunk(&context, request, &response);
      if (status.ok() && (response.data().size() != width ||
                          response.checksum() != chunk.checksum() ||
                          !VerifyChecksum(response.data(), chunk.checksum()))) {
        status = {StatusCode::DATA_LOSS, "RS shard checksum mismatch"};
      }
      if (status.ok()) {
        buffers[i] = std::move(*response.mutable_data());
        available.push_back({static_cast<std::size_t>(i), buffers[i]});
        break;
      }
      std::clog << "RS shard rejected index=" << chunk.chunk().index()
                << " node=" << node.node_id() << " code=" << status.error_code()
                << '\n';
    }
  }
  if (available.size() < 4) {
    return {StatusCode::UNAVAILABLE,
            "RS42 requires four verified shards; received " +
                std::to_string(available.size())};
  }
  if (available[3].index == 3) {
    data = buffers[0] + buffers[1] + buffers[2] + buffers[3];
    data.resize(size);
    return grpc::Status::OK;
  }
  try {
    data = RsDecode(available, size);
    const auto recovered = RsEncode(data);
    for (int i = 0; i < 6; ++i) {
      if (!VerifyChecksum(recovered[i], chunks.Get(begin + i).checksum())) {
        return {StatusCode::DATA_LOSS,
                "reconstructed RS checksum disagrees with MGM"};
      }
    }
  } catch (const std::invalid_argument& error) {
    return {StatusCode::DATA_LOSS, error.what()};
  }
  std::clog << "RS reconstructed stripe=" << first.index() / 6 << '\n';
  return grpc::Status::OK;
}

}  // namespace objectstore
