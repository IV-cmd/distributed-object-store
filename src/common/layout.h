#pragma once

#include <algorithm>
#include <cstdint>

#include "common/chunk.h"
#include "objectstore.pb.h"

namespace objectstore {

inline std::uint64_t LayoutChunkCount(v1::Layout layout, std::uint64_t size) {
  if (layout == v1::REPLICA3) {
    return ChunkCount(size);
  }
  const auto stripe = 4 * kChunkSize;
  return (size / stripe + (size % stripe != 0)) * 6;
}

inline std::uint64_t StripeSize(std::uint64_t size, std::uint32_t index) {
  const auto start = static_cast<std::uint64_t>(index / 6) * 4 * kChunkSize;
  return std::min<std::uint64_t>(4 * kChunkSize, size - start);
}

inline std::uint64_t LayoutChunkSize(v1::Layout layout, std::uint64_t size,
                                     std::uint32_t index) {
  if (layout == v1::REPLICA3) {
    return std::min<std::uint64_t>(kChunkSize,
                                   size - index * std::uint64_t{kChunkSize});
  }
  const auto bytes = StripeSize(size, index);
  return bytes / 4 + (bytes % 4 != 0);
}

}  // namespace objectstore
