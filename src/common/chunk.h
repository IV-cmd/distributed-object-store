#pragma once

#include <cstddef>
#include <cstdint>
#include <istream>
#include <string>
#include <string_view>

namespace objectstore {

inline constexpr std::size_t kChunkSize = std::size_t{1024} * 1024;

std::uint64_t Checksum(std::string_view bytes);
bool VerifyChecksum(std::string_view bytes, std::uint64_t expected);
bool ReadNextChunk(std::istream& input, std::string& chunk);
std::uint64_t ChunkCount(std::uint64_t size_bytes);

}  // namespace objectstore
