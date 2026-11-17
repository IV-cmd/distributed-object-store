#pragma once

#include <filesystem>
#include <stdexcept>
#include <system_error>

namespace objectstore {

class Download {
 public:
  explicit Download(const std::filesystem::path& output)
      : dir_(output.string() + ".download") {
    if (!std::filesystem::create_directory(dir_)) {
      throw std::runtime_error("temporary download directory already exists");
    }
  }
  ~Download() {
    std::error_code ignored;
    std::filesystem::remove_all(dir_, ignored);
  }
  Download(const Download&) = delete;
  Download& operator=(const Download&) = delete;

  std::filesystem::path File() const { return dir_ / "data"; }

 private:
  std::filesystem::path dir_;
};

}  // namespace objectstore
