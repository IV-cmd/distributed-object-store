#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "objectstore.pb.h"

namespace objectstore {

using HealthClock = std::chrono::steady_clock;

struct NodeWatch {
  v1::NodeInfo info;
  HealthClock::time_point last_heartbeat;
};

struct RepairTask {
  v1::ChunkMetadata chunk;
  std::uint64_t size_bytes;
  std::uint64_t generation;
  std::vector<v1::NodeInfo> sources;
  v1::NodeInfo target;
};

std::vector<std::string> ExpiredNodes(const std::vector<NodeWatch>& nodes,
                                      HealthClock::time_point now,
                                      std::chrono::milliseconds timeout);
std::vector<RepairTask> DecideRepairs(const std::vector<v1::FileMetadata>& files,
                                      const std::vector<v1::NodeInfo>& nodes);

}  // namespace objectstore
