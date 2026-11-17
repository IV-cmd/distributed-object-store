#pragma once

#include <chrono>
#include <iostream>
#include <string_view>
#include <syncstream>

namespace objectstore {

inline void Log(std::string_view event, std::string_view fields) {
  const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  std::osyncstream(std::cout) << "timestamp_ms=" << timestamp.count()
                              << " event=" << event << ' ' << fields << '\n'
                              << std::flush;
}

}  // namespace objectstore
