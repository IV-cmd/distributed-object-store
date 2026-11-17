#include "common/rs_codec.h"

#include <algorithm>
#include <stdexcept>

namespace objectstore {
namespace {

using Row = std::array<std::uint8_t, 4>;
using Matrix = std::array<Row, 4>;

struct Field {
  std::array<std::uint8_t, 512> powers{};
  std::array<std::uint8_t, 256> logs{};

  Field() {
    // GF(256), primitive element 2, reduction polynomial x^8+x^4+x^3+x^2+1.
    unsigned value = 1;
    for (unsigned i = 0; i < 255; ++i) {
      powers[i] = static_cast<std::uint8_t>(value);
      logs[value] = static_cast<std::uint8_t>(i);
      value <<= 1;
      if (value & 256) {
        value ^= 0x11d;
      }
    }
    for (unsigned i = 255; i < powers.size(); ++i) {
      powers[i] = powers[i - 255];
    }
  }

  std::uint8_t Multiply(std::uint8_t left, std::uint8_t right) const {
    return left == 0 || right == 0 ? 0 : powers[logs[left] + logs[right]];
  }

  std::uint8_t Inverse(std::uint8_t value) const {
    if (value == 0) {
      throw std::invalid_argument("zero has no GF(256) inverse");
    }
    return powers[255 - logs[value]];
  }

  Row Generator(std::size_t index) const {
    Row row{};
    if (index < 4) {
      row[index] = 1;
    } else {
      // Systematic Cauchy generator [I; C], C[i,j]=1/(i XOR (j+2)).
      // Disjoint x={0,1}, y={2,3,4,5} make every square minor invertible.
      for (std::size_t j = 0; j < 4; ++j) {
        row[j] = Inverse(static_cast<std::uint8_t>((index - 4) ^ (j + 2)));
      }
    }
    return row;
  }
};

Matrix Invert(Matrix matrix, const Field& field) {
  Matrix inverse{};
  for (std::size_t i = 0; i < 4; ++i) {
    inverse[i][i] = 1;
  }
  for (std::size_t column = 0; column < 4; ++column) {
    std::size_t pivot = column;
    while (pivot < 4 && matrix[pivot][column] == 0) {
      ++pivot;
    }
    if (pivot == 4) {
      throw std::invalid_argument("singular RS generator rows");
    }
    std::swap(matrix[pivot], matrix[column]);
    std::swap(inverse[pivot], inverse[column]);
    const auto scale = field.Inverse(matrix[column][column]);
    for (std::size_t j = 0; j < 4; ++j) {
      matrix[column][j] = field.Multiply(matrix[column][j], scale);
      inverse[column][j] = field.Multiply(inverse[column][j], scale);
    }
    for (std::size_t row = 0; row < 4; ++row) {
      if (row == column) {
        continue;
      }
      const auto factor = matrix[row][column];
      for (std::size_t j = 0; j < 4; ++j) {
        matrix[row][j] ^= field.Multiply(factor, matrix[column][j]);
        inverse[row][j] ^= field.Multiply(factor, inverse[column][j]);
      }
    }
  }
  return inverse;
}

}  // namespace

std::uint8_t GfMultiply(std::uint8_t left, std::uint8_t right) {
  return Field{}.Multiply(left, right);
}

std::uint8_t GfInverse(std::uint8_t value) { return Field{}.Inverse(value); }

RsShards RsEncode(std::string_view data) {
  const Field field;
  const auto width = data.size() / 4 + (data.size() % 4 != 0);
  RsShards shards;
  for (auto& shard : shards) {
    shard.resize(width, '\0');
  }
  for (std::size_t index = 0; index < 4; ++index) {
    const auto start = index * width;
    if (start < data.size()) {
      std::copy_n(data.begin() + start, std::min(width, data.size() - start),
                  shards[index].begin());
    }
  }
  for (std::size_t index = 4; index < 6; ++index) {
    const auto row = field.Generator(index);
    for (std::size_t offset = 0; offset < width; ++offset) {
      std::uint8_t value = 0;
      for (std::size_t j = 0; j < 4; ++j) {
        value ^= field.Multiply(row[j],
                                static_cast<std::uint8_t>(shards[j][offset]));
      }
      shards[index][offset] = static_cast<char>(value);
    }
  }
  return shards;
}

std::string RsDecode(std::span<const RsShard> available, std::size_t size) {
  if (available.size() < 4 || available.size() > 6) {
    throw std::invalid_argument("RS42 requires at least four distinct shards");
  }
  const auto width = size / 4 + (size % 4 != 0);
  std::array<bool, 6> seen{};
  for (const auto& shard : available) {
    if (shard.index >= 6 || seen[shard.index] || shard.bytes.size() != width) {
      throw std::invalid_argument(
          "invalid RS shard index, length, or duplicate");
    }
    seen[shard.index] = true;
  }
  const Field field;
  Matrix rows;
  for (std::size_t i = 0; i < 4; ++i) {
    rows[i] = field.Generator(available[i].index);
  }
  const auto inverse = Invert(rows, field);
  std::string data(size, '\0');
  for (std::size_t row = 0; row < 4; ++row) {
    for (std::size_t offset = 0; offset < width && row * width + offset < size;
         ++offset) {
      std::uint8_t value = 0;
      for (std::size_t j = 0; j < 4; ++j) {
        value ^= field.Multiply(
            inverse[row][j],
            static_cast<std::uint8_t>(available[j].bytes[offset]));
      }
      data[row * width + offset] = static_cast<char>(value);
    }
  }
  return data;
}

}  // namespace objectstore
