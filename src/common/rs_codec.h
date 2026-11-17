#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace objectstore {

using RsShards = std::array<std::string, 6>;

struct RsShard {
  std::size_t index;
  std::string_view bytes;
};

std::uint8_t GfMultiply(std::uint8_t left, std::uint8_t right);
std::uint8_t GfInverse(std::uint8_t value);
RsShards RsEncode(std::string_view data);
std::string RsDecode(std::span<const RsShard> available, std::size_t size);

}  // namespace objectstore
