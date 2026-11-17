#pragma once

#include <cstddef>
#include <vector>

#include "objectstore.pb.h"

namespace objectstore {

std::vector<v1::NodeInfo> SelectNodes(std::vector<v1::NodeInfo> available,
                                      std::size_t replication_factor);

}  // namespace objectstore
