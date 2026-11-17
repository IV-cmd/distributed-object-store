#include "client/client.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <stdexcept>
#include <system_error>

#include "client/download.h"
#include "common/chunk.h"

namespace objectstore {
namespace {

namespace fs = std::filesystem;

void SetDeadline(grpc::ClientContext& context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(30));
}


}  // namespace

ObjectClient::ObjectClient(const std::shared_ptr<grpc::Channel>& channel)
    : stub_(v1::FstService::NewStub(channel)) {}

grpc::Status ObjectClient::Put(const fs::path& file,
                               const std::string& object_id) {
  std::ifstream input(file, std::ios::binary);
  if (!input) {
    throw std::runtime_error("cannot open input file");
  }
  const auto size = fs::file_size(file);
  v1::BeginObjectRequest begin;
  begin.set_object_id(object_id);
  begin.set_size_bytes(size);
  google::protobuf::Empty empty;
  grpc::ClientContext begin_context;
  SetDeadline(begin_context);
  auto status = stub_->BeginObject(&begin_context, begin, &empty);
  if (!status.ok()) {
    return status;
  }
  std::string bytes;
  std::uint64_t sent = 0;
  std::uint32_t index = 0;
  while (ReadNextChunk(input, bytes)) {
    if (bytes.size() > size - sent) {
      throw std::runtime_error(
          "input file grew during upload; remove partial object");
    }
    v1::WriteChunkRequest request;
    request.mutable_chunk()->set_object_id(object_id);
    request.mutable_chunk()->set_index(index++);
    request.set_data(bytes);
    request.set_checksum(Checksum(bytes));
    grpc::ClientContext context;
    SetDeadline(context);
    status = stub_->WriteChunk(&context, request, &empty);
    if (!status.ok()) {
      return status;
    }
    sent += bytes.size();
  }
  if (sent != size) {
    throw std::runtime_error(
        "input file shrank during upload; remove partial object");
  }
  v1::ObjectRequest finish;
  finish.set_object_id(object_id);
  grpc::ClientContext context;
  SetDeadline(context);
  return stub_->FinishObject(&context, finish, &empty);
}

grpc::Status ObjectClient::Get(const std::string& object_id,
                               const fs::path& file) {
  v1::ObjectRequest request;
  request.set_object_id(object_id);
  v1::ObjectInfo info;
  grpc::ClientContext info_context;
  SetDeadline(info_context);
  auto status = stub_->GetObjectInfo(&info_context, request, &info);
  if (!status.ok()) {
    return status;
  }
  if (info.chunk_count() != ChunkCount(info.size_bytes())) {
    return {grpc::StatusCode::DATA_LOSS, "invalid object metadata"};
  }
  const Download download(file);
  std::ofstream output;
  output.exceptions(std::ios::failbit | std::ios::badbit);
  output.open(download.File(), std::ios::binary | std::ios::trunc);
  auto remaining = info.size_bytes();
  for (std::uint32_t i = 0; i < info.chunk_count(); ++i) {
    v1::ReadChunkRequest chunk;
    chunk.mutable_chunk()->set_object_id(object_id);
    chunk.mutable_chunk()->set_index(i);
    v1::ReadChunkResponse response;
    grpc::ClientContext context;
    SetDeadline(context);
    status = stub_->ReadChunk(&context, chunk, &response);
    if (!status.ok()) {
      return status;
    }
    if (response.data().size() !=
            std::min<std::uint64_t>(kChunkSize, remaining) ||
        !VerifyChecksum(response.data(), response.checksum())) {
      return {grpc::StatusCode::DATA_LOSS,
              "CHECKSUM_MISMATCH: rejected read response"};
    }
    output.write(response.data().data(),
                 static_cast<std::streamsize>(response.data().size()));
    remaining -= response.data().size();
  }
  output.close();
  fs::rename(download.File(), file);
  return grpc::Status::OK;
}

grpc::Status ObjectClient::Remove(const std::string& object_id) {
  v1::ObjectRequest request;
  request.set_object_id(object_id);
  google::protobuf::Empty response;
  grpc::ClientContext context;
  SetDeadline(context);
  return stub_->DeleteObject(&context, request, &response);
}

}  // namespace objectstore
