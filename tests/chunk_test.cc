#include "common/chunk.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <sstream>
#include <string>

namespace objectstore {
namespace {

TEST(ChecksumTest, KnownEmptyDigest) {
  EXPECT_EQ(Checksum(""), UINT64_C(0xef46db3751d8e999));
}

TEST(ChecksumTest, DetectsChangedByte) {
  std::string bytes("a\0b", 3);
  const auto expected = Checksum(bytes);
  EXPECT_TRUE(VerifyChecksum(bytes, expected));
  bytes[1] ^= 1;
  EXPECT_FALSE(VerifyChecksum(bytes, expected));
}

TEST(ChunkTest, SplitAndReassembleBoundaries) {
  for (const auto size : {std::size_t{0}, std::size_t{17}, kChunkSize,
                          kChunkSize + 17, 2 * kChunkSize}) {
    SCOPED_TRACE(size);
    std::string original(size, '\0');
    for (std::size_t i = 0; i < size; ++i) {
      original[i] = static_cast<char>(i % 251);
    }
    std::istringstream input(original);
    std::string chunk;
    std::string reassembled;
    std::uint64_t count = 0;
    while (ReadNextChunk(input, chunk)) {
      EXPECT_LE(chunk.size(), kChunkSize);
      reassembled.append(chunk);
      ++count;
    }
    EXPECT_EQ(reassembled, original);
    EXPECT_EQ(count, ChunkCount(size));
  }
}

TEST(ChunkTest, ReportsInputFailure) {
  std::istringstream input;
  input.setstate(std::ios::badbit);
  std::string chunk;
  EXPECT_THROW(ReadNextChunk(input, chunk), std::ios_base::failure);
}

}  // namespace
}  // namespace objectstore
