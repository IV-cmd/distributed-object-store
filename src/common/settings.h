#pragma once

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace objectstore {

inline std::chrono::milliseconds EnvMilliseconds(const char* name,
                                                  int default_value) {
  const char* configured = std::getenv(name);
  if (configured == nullptr) {
    return std::chrono::milliseconds(default_value);
  }
  const std::string_view text(configured);
  int value = 0;
  const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
  if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
      value <= 0) {
    throw std::runtime_error(std::string(name) + " must be positive milliseconds");
  }
  return std::chrono::milliseconds(value);
}

}  // namespace objectstore
