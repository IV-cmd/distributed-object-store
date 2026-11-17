#include "common/chunk.h"

#include <xxhash.h>

namespace objectstore {

std::uint64_t Checksum(std::string_view bytes) {
  return XXH64(bytes.data(), bytes.size(), 0);
}

bool VerifyChecksum(std::string_view bytes, std::uint64_t expected) {
  return Checksum(bytes) == expected;
}

bool ReadNextChunk(std::istream& input, std::string& chunk) {
  chunk.resize(kChunkSize);
  input.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
  if (input.bad() || (input.fail() && !input.eof())) {
    throw std::ios_base::failure("failed to read input");
  }
  chunk.resize(static_cast<std::size_t>(input.gcount()));
  return !chunk.empty();
}

std::uint64_t ChunkCount(std::uint64_t size_bytes) {
  return size_bytes / kChunkSize + (size_bytes % kChunkSize != 0);
}

}  // namespace objectstore
