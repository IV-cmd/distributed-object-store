#include "fst/service.h"

#include <grpcpp/grpcpp.h>

#include <charconv>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "common/chunk.h"

namespace objectstore {
namespace {

namespace fs = std::filesystem;
using grpc::StatusCode;

struct RpcError {
  grpc::Status status;
};

void Require(bool condition, StatusCode code, const char* message) {
  if (!condition) {
    throw RpcError{{code, message}};
  }
}

std::string ReadFile(const fs::path& path, std::size_t limit) {
  Require(fs::exists(path), StatusCode::NOT_FOUND, "file not found");
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  input.exceptions(std::ios::failbit | std::ios::badbit);
  const auto length = input.tellg();
  Require(length >= 0 && static_cast<std::uint64_t>(length) <= limit,
          StatusCode::DATA_LOSS, "invalid stored file length");
  std::string bytes(static_cast<std::size_t>(length), '\0');
  input.seekg(0);
  input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  return bytes;
}

void WriteFile(const fs::path& path, std::string_view bytes) {
  std::ofstream output;
  output.exceptions(std::ios::failbit | std::ios::badbit);
  output.open(path, std::ios::binary | std::ios::trunc);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  output.close();
}

v1::ObjectInfo LoadInfo(const fs::path& dir, const char* state = "manifest") {
  Require(fs::exists(dir), StatusCode::NOT_FOUND, "object not found");
  const auto path = dir / state;
  Require(fs::exists(path), StatusCode::FAILED_PRECONDITION,
          "object is not in the required upload state");
  std::istringstream input(ReadFile(path, 64));
  std::uint64_t size = 0;
  std::uint64_t count = 0;
  Require(static_cast<bool>(input >> size >> count), StatusCode::DATA_LOSS,
          "invalid manifest");
  input >> std::ws;
  Require(input.eof() && count == ChunkCount(size) &&
              count <= std::numeric_limits<std::uint32_t>::max(),
          StatusCode::DATA_LOSS, "invalid manifest");
  v1::ObjectInfo info;
  info.set_size_bytes(size);
  info.set_chunk_count(static_cast<std::uint32_t>(count));
  return info;
}

std::uint64_t ExpectedSize(const v1::ObjectInfo& info, std::uint32_t index) {
  Require(index < info.chunk_count(), StatusCode::INVALID_ARGUMENT,
          "chunk index out of range");
  return index == info.chunk_count() - 1
             ? info.size_bytes() -
                   static_cast<std::uint64_t>(index) * kChunkSize
             : kChunkSize;
}

fs::path ChunkPath(const fs::path& dir, std::uint32_t index) {
  return dir / (std::to_string(index) + ".chunk");
}

fs::path ChecksumPath(const fs::path& dir, std::uint32_t index) {
  return dir / (std::to_string(index) + ".crc");
}

std::string ReadVerified(const fs::path& dir, const v1::ChunkId& id,
                         const v1::ObjectInfo& info) {
  const auto expected_size = ExpectedSize(info, id.index());
  const auto bytes = ReadFile(ChunkPath(dir, id.index()), kChunkSize);
  const auto sidecar = ReadFile(ChecksumPath(dir, id.index()), 32);
  std::uint64_t expected = 0;
  const auto result = std::from_chars(
      sidecar.data(), sidecar.data() + sidecar.size(), expected, 16);
  Require(sidecar.size() == 17 && sidecar.back() == '\n' &&
              result.ec == std::errc{} && result.ptr == sidecar.data() + 16 &&
              bytes.size() == expected_size && VerifyChecksum(bytes, expected),
          StatusCode::DATA_LOSS, "CHECKSUM_MISMATCH: stored chunk is corrupt");
  return bytes;
}

}  // namespace

FstService::FstService(fs::path data_dir) : data_dir_(std::move(data_dir)) {
  fs::create_directories(data_dir_);
  data_dir_ = fs::canonical(data_dir_);
}

grpc::Status FstService::Run(const std::function<void()>& operation) {
  const std::lock_guard lock(mutex_);
  try {
    operation();
    return grpc::Status::OK;
  } catch (const RpcError& error) {
    return error.status;
  } catch (const fs::filesystem_error&) {
    return {StatusCode::INTERNAL, "filesystem operation failed"};
  } catch (const std::ios_base::failure&) {
    return {StatusCode::INTERNAL, "file I/O failed"};
  }
}

fs::path FstService::ObjectDir(const std::string& object_id) const {
  Require(
      !object_id.empty() && object_id.size() <= 128 &&
          object_id.find_first_not_of(
              "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
              "0123456789_-") == std::string::npos,
      StatusCode::INVALID_ARGUMENT,
      "object_id must be 1-128 ASCII letters, digits, underscores or hyphens");
  return data_dir_ / object_id;
}

grpc::Status FstService::BeginObject(grpc::ServerContext*,
                                     const v1::BeginObjectRequest* request,
                                     google::protobuf::Empty*) {
  return Run([&] {
    const auto dir = ObjectDir(request->object_id());
    const auto count = ChunkCount(request->size_bytes());
    Require(count <= std::numeric_limits<std::uint32_t>::max(),
            StatusCode::INVALID_ARGUMENT, "object exceeds chunk-index limit");
    Require(fs::create_directory(dir), StatusCode::ALREADY_EXISTS,
            "object ID already exists; delete it before reuse");
    WriteFile(dir / "pending", std::to_string(request->size_bytes()) + " " +
                                   std::to_string(count) + "\n");
  });
}

grpc::Status FstService::FinishObject(grpc::ServerContext*,
                                      const v1::ObjectRequest* request,
                                      google::protobuf::Empty*) {
  return Run([&] {
    const auto dir = ObjectDir(request->object_id());
    const auto info = LoadInfo(dir, "pending");
    v1::ChunkId id;
    id.set_object_id(request->object_id());
    for (std::uint32_t i = 0; i < info.chunk_count(); ++i) {
      id.set_index(i);
      ReadVerified(dir, id, info);
    }
    fs::rename(dir / "pending", dir / "manifest");
  });
}

grpc::Status FstService::GetObjectInfo(grpc::ServerContext*,
                                       const v1::ObjectRequest* request,
                                       v1::ObjectInfo* response) {
  return Run([&] { *response = LoadInfo(ObjectDir(request->object_id())); });
}

grpc::Status FstService::DeleteObject(grpc::ServerContext*,
                                      const v1::ObjectRequest* request,
                                      google::protobuf::Empty*) {
  return Run([&] {
    Require(fs::remove_all(ObjectDir(request->object_id())) != 0,
            StatusCode::NOT_FOUND, "object not found");
  });
}

grpc::Status FstService::WriteChunk(grpc::ServerContext*,
                                    const v1::WriteChunkRequest* request,
                                    google::protobuf::Empty*) {
  return Run([&] {
    const auto& id = request->chunk();
    const auto dir = ObjectDir(id.object_id());
    Require(!fs::exists(dir / "manifest"), StatusCode::ALREADY_EXISTS,
            "completed objects are write-once");
    const auto info = LoadInfo(dir, "pending");
    Require(request->data().size() == ExpectedSize(info, id.index()),
            StatusCode::INVALID_ARGUMENT, "chunk length does not match object");
    Require(VerifyChecksum(request->data(), request->checksum()),
            StatusCode::DATA_LOSS, "CHECKSUM_MISMATCH: rejected write");
    const auto chunk = ChunkPath(dir, id.index());
    const auto crc = ChecksumPath(dir, id.index());
    Require(!(fs::exists(chunk) && fs::exists(crc)), StatusCode::ALREADY_EXISTS,
            "chunk already exists");
    std::ostringstream digest;
    digest << std::hex << std::setfill('0') << std::setw(16)
           << Checksum(request->data()) << '\n';
    WriteFile(dir / "chunk.tmp", request->data());
    WriteFile(dir / "crc.tmp", digest.str());
    fs::rename(dir / "chunk.tmp", chunk);
    fs::rename(dir / "crc.tmp", crc);
  });
}

grpc::Status FstService::ReadChunk(grpc::ServerContext*,
                                   const v1::ReadChunkRequest* request,
                                   v1::ReadChunkResponse* response) {
  return Run([&] {
    const auto& id = request->chunk();
    const auto dir = ObjectDir(id.object_id());
    const auto bytes = ReadVerified(dir, id, LoadInfo(dir, fs::exists(dir / "manifest")
                                                   ? "manifest" : "repair"));
    response->set_data(bytes);
    response->set_checksum(Checksum(bytes));
  });
}

grpc::Status FstService::DeleteChunk(grpc::ServerContext*,
                                     const v1::DeleteChunkRequest* request,
                                     google::protobuf::Empty*) {
  return Run([&] {
    const auto& id = request->chunk();
    const auto dir = ObjectDir(id.object_id());
    const auto info = LoadInfo(dir, fs::exists(dir / "manifest") ? "manifest"
                         : fs::exists(dir / "repair") ? "repair" : "pending");
    ExpectedSize(info, id.index());
    const bool removed = fs::remove(ChunkPath(dir, id.index()));
    const bool crc_removed = fs::remove(ChecksumPath(dir, id.index()));
    Require(removed || crc_removed, StatusCode::NOT_FOUND, "chunk not found");
  });
}

grpc::Status FstService::InstallChunk(grpc::ServerContext*,
                                        const v1::InstallChunkRequest* request,
                                        google::protobuf::Empty*) {
  return Run([&] {
    const auto& write = request->write();
    const auto& id = write.chunk();
    const auto dir = ObjectDir(id.object_id());
    Require(!fs::exists(dir / "pending"), StatusCode::FAILED_PRECONDITION,
            "repair cannot modify an unfinished upload");
    v1::ObjectInfo info;
    info.set_size_bytes(request->size_bytes());
    const auto count = ChunkCount(request->size_bytes());
    Require(count <= std::numeric_limits<std::uint32_t>::max(),
            StatusCode::INVALID_ARGUMENT, "object exceeds chunk-index limit");
    info.set_chunk_count(static_cast<std::uint32_t>(count));
    Require(write.data().size() == ExpectedSize(info, id.index()),
            StatusCode::INVALID_ARGUMENT, "invalid repair chunk length");
    Require(VerifyChecksum(write.data(), write.checksum()), StatusCode::DATA_LOSS,
            "CHECKSUM_MISMATCH: rejected repair bytes");
    if (fs::exists(dir / "manifest") || fs::exists(dir / "repair")) {
      const auto existing = LoadInfo(dir, fs::exists(dir / "manifest")
                                             ? "manifest" : "repair");
      Require(existing.size_bytes() == info.size_bytes(),
              StatusCode::ALREADY_EXISTS, "repair object has a different size");
    } else {
      fs::create_directories(dir);
      WriteFile(dir / "repair", std::to_string(info.size_bytes()) + " " +
                                    std::to_string(info.chunk_count()) + "\n");
    }
    if (fs::exists(ChunkPath(dir, id.index())) &&
        fs::exists(ChecksumPath(dir, id.index()))) {
      Require(ReadVerified(dir, id, info) == write.data(),
              StatusCode::ALREADY_EXISTS, "repair would replace existing data");
      return;
    }
    std::ostringstream digest;
    digest << std::hex << std::setfill('0') << std::setw(16) << write.checksum()
           << '\n';
    WriteFile(dir / "chunk.tmp", write.data());
    WriteFile(dir / "crc.tmp", digest.str());
    fs::rename(dir / "chunk.tmp", ChunkPath(dir, id.index()));
    fs::rename(dir / "crc.tmp", ChecksumPath(dir, id.index()));
    Require(ReadVerified(dir, id, info) == write.data(), StatusCode::DATA_LOSS,
            "CHECKSUM_MISMATCH: repair readback failed");
  });
}

grpc::Status FstService::CopyChunk(grpc::ServerContext*,
                                   const v1::CopyChunkRequest* request,
                                   google::protobuf::Empty*) {
  v1::InstallChunkRequest install;
  const auto status = Run([&] {
    const auto& id = request->chunk();
    const auto dir = ObjectDir(id.object_id());
    const auto info = LoadInfo(dir, fs::exists(dir / "manifest")
                                       ? "manifest" : "repair");
    Require(info.size_bytes() == request->size_bytes(), StatusCode::DATA_LOSS,
            "source object size disagrees with MGM");
    const auto bytes = ReadVerified(dir, id, info);
    Require(VerifyChecksum(bytes, request->checksum()), StatusCode::DATA_LOSS,
            "source checksum disagrees with MGM");
    install.set_size_bytes(info.size_bytes());
    auto* write = install.mutable_write();
    *write->mutable_chunk() = id;
    write->set_data(bytes);
    write->set_checksum(request->checksum());
  });
  if (!status.ok()) {
    return {status.error_code(), "SOURCE_CORRUPT: " + status.error_message()};
  }
  if (request->target().endpoint().empty()) {
    return {StatusCode::INVALID_ARGUMENT, "copy target is required"};
  }
  auto target = v1::FstService::NewStub(grpc::CreateChannel(
      request->target().endpoint(), grpc::InsecureChannelCredentials()));
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
  google::protobuf::Empty empty;
  return target->InstallChunk(&context, install, &empty);
}

v1::ReportChunkStatusRequest FstService::Inventory() {
  const std::lock_guard lock(mutex_);
  v1::ReportChunkStatusRequest report;
  for (const auto& object : fs::directory_iterator(data_dir_)) {
    if (!object.is_directory()) {
      continue;
    }
    for (const auto& entry : fs::directory_iterator(object.path())) {
      if (!entry.is_regular_file() || entry.path().extension() != ".chunk") {
        continue;
      }
      const auto stem = entry.path().stem().string();
      std::uint32_t index = 0;
      const auto parsed = std::from_chars(stem.data(), stem.data() + stem.size(), index);
      if (parsed.ec != std::errc{} || parsed.ptr != stem.data() + stem.size()) {
        continue;
      }
      auto* chunk = report.add_chunks();
      chunk->mutable_chunk()->set_object_id(object.path().filename().string());
      chunk->mutable_chunk()->set_index(index);
      try {
        const auto info = LoadInfo(object.path(), fs::exists(object.path() / "manifest")
                                                    ? "manifest" : "repair");
        const auto bytes = ReadVerified(object.path(), chunk->chunk(), info);
        chunk->set_state(v1::ChunkStatus::HEALTHY);
        chunk->set_checksum(Checksum(bytes));
        chunk->set_size_bytes(info.size_bytes());
      } catch (const RpcError&) {
        chunk->set_state(v1::ChunkStatus::CORRUPT);
      } catch (const fs::filesystem_error&) {
        chunk->set_state(v1::ChunkStatus::CORRUPT);
      } catch (const std::ios_base::failure&) {
        chunk->set_state(v1::ChunkStatus::CORRUPT);
      }
    }
  }
  return report;
}

void FstService::FillHeartbeat(v1::HeartbeatRequest& request) {
  const std::lock_guard lock(mutex_);
  std::uint64_t count = 0;
  for (const auto& object : fs::directory_iterator(data_dir_)) {
    if (!object.is_directory()) {
      continue;
    }
    for (const auto& entry : fs::directory_iterator(object.path())) {
      if (entry.is_regular_file() && entry.path().extension() == ".chunk") {
        ++count;
      }
    }
  }
  request.set_chunk_count(count);
  request.set_free_bytes(fs::space(data_dir_).available);
}

}  // namespace objectstore
