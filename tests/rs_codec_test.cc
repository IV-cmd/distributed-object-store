#include "common/rs_codec.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/chunk.h"

namespace objectstore {
namespace {

std::string BinaryData(std::size_t size) {
  std::string data(size, '\0');
  for (std::size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(i * 13);
  }
  return data;
}

TEST(GaloisField, ZeroIdentityReductionAndAllInverses) {
  EXPECT_EQ(GfMultiply(128, 2), 0x1d);
  for (unsigned value = 1; value < 256; ++value) {
    const auto byte = static_cast<std::uint8_t>(value);
    EXPECT_EQ(GfMultiply(byte, 0), 0);
    EXPECT_EQ(GfMultiply(byte, 1), byte);
    EXPECT_EQ(GfMultiply(byte, GfInverse(byte)), 1);
  }
  EXPECT_THROW(GfInverse(0), std::invalid_argument);
}

TEST(GaloisField, MultiplicationDistributesOverXor) {
  for (unsigned i = 0; i < 256; ++i) {
    const auto a = static_cast<std::uint8_t>(i);
    EXPECT_EQ(GfMultiply(a, 37 ^ 201), GfMultiply(a, 37) ^ GfMultiply(a, 201));
  }
}

TEST(RsCodec, SystematicRoundTripHandlesEmptyTinyAndUnalignedInputs) {
  for (const auto size : {0, 1, 2, 3, 4, 5, 17, 4096, 4097}) {
    const auto data = BinaryData(size);
    const auto shards = RsEncode(data);
    const std::array<RsShard, 4> available{{{0, shards[0]}, {1, shards[1]},
                                          {2, shards[2]}, {3, shards[3]}}};
    EXPECT_EQ(RsDecode(available, data.size()), data);
    std::string systematic;
    for (std::size_t i = 0; i < 4; ++i) {
      systematic += shards[i];
    }
    systematic.resize(data.size());
    EXPECT_EQ(systematic, data);
  }
}

std::vector<std::pair<int, int>> LossPairs() {
  std::vector<std::pair<int, int>> pairs;
  for (int first = 0; first < 6; ++first) {
    for (int second = first + 1; second < 6; ++second) {
      pairs.emplace_back(first, second);
    }
  }
  return pairs;
}

class RsLossTest : public testing::TestWithParam<std::pair<int, int>> {};

TEST_P(RsLossTest, RecoversEveryTwoShardLossIncludingParity) {
  const auto data = BinaryData(4097);
  const auto shards = RsEncode(data);
  const auto [first, second] = GetParam();
  std::vector<RsShard> available;
  for (int i = 0; i < 6; ++i) {
    if (i != first && i != second) {
      available.push_back({static_cast<std::size_t>(i), shards[i]});
    }
  }
  std::reverse(available.begin(), available.end());
  EXPECT_EQ(RsDecode(available, data.size()), data);
}

INSTANTIATE_TEST_SUITE_P(TwoLosses, RsLossTest, testing::ValuesIn(LossPairs()));

TEST(RsCodec, RejectsThreeLossesDuplicateIndicesAndMalformedShards) {
  const auto shards = RsEncode("binary payload");
  std::vector<RsShard> available{{0, shards[0]}, {1, shards[1]}, {2, shards[2]}};
  EXPECT_THROW(RsDecode(available, 14), std::invalid_argument);
  available.push_back({0, shards[0]});
  EXPECT_THROW(RsDecode(available, 14), std::invalid_argument);
  available.back() = {6, shards[3]};
  EXPECT_THROW(RsDecode(available, 14), std::invalid_argument);
  available.back() = {3, "wrong length"};
  EXPECT_THROW(RsDecode(available, 14), std::invalid_argument);
}

TEST(RsCodec, CallerRejectsCorruptBytesBeforeDecoding) {
  const auto data = BinaryData(4097);
  auto shards = RsEncode(data);
  std::array<std::uint64_t, 6> expected{};
  for (std::size_t i = 0; i < 6; ++i) {
    expected[i] = Checksum(shards[i]);
  }
  shards[1][0] ^= 1;
  std::vector<RsShard> available;
  for (std::size_t i = 0; i < 6; ++i) {
    if (VerifyChecksum(shards[i], expected[i])) {
      available.push_back({i, shards[i]});
    }
  }
  ASSERT_EQ(available.size(), 5);
  EXPECT_EQ(RsDecode(available, data.size()), data);
}

}  // namespace
}  // namespace objectstore
