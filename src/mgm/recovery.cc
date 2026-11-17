#include "mgm/recovery.h"

#include <algorithm>
#include <set>
#include <utility>

#include "mgm/placement.h"

namespace objectstore {

std::vector<std::string> ExpiredNodes(const std::vector<NodeWatch>& nodes,
                                      HealthClock::time_point now,
                                      std::chrono::milliseconds timeout) {
  std::vector<std::string> expired;
  for (const auto& node : nodes) {
    if (node.info.state() != v1::DOWN && now - node.last_heartbeat >= timeout) {
      expired.push_back(node.info.node_id());
    }
  }
  return expired;
}

std::vector<RepairTask> DecideRepairs(
    const std::vector<v1::FileMetadata>& files,
    const std::vector<v1::NodeInfo>& nodes) {
  std::vector<RepairTask> tasks;
  for (const auto& file : files) {
    if (!file.complete()) {
      continue;
    }
    for (const auto& chunk : file.chunks()) {
      if (!chunk.committed()) {
        continue;
      }
      const auto& known = chunk.known_replicas().empty()
                              ? chunk.replicas()
                              : chunk.known_replicas();
      std::set<std::string> holders;
      std::set<std::string> invalid(chunk.invalid_replicas().begin(),
                                    chunk.invalid_replicas().end());
      std::vector<v1::NodeInfo> sources;
      for (const auto& replica : known) {
        holders.insert(replica.node_id());
        const auto live =
            std::find_if(nodes.begin(), nodes.end(), [&](const auto& node) {
              return node.node_id() == replica.node_id() &&
                     node.state() == v1::UP;
            });
        if (live != nodes.end() && !invalid.contains(replica.node_id())) {
          sources.push_back(*live);
        }
      }
      if (sources.empty() || sources.size() >= 3) {
        continue;
      }
      std::vector<v1::NodeInfo> available;
      available.reserve(nodes.size());
      for (const auto& node : nodes) {
        if (!holders.contains(node.node_id())) {
          available.push_back(node);
        }
      }
      auto targets = SelectNodes(std::move(available), 1);
      if (!targets.empty()) {
        tasks.push_back({chunk, file.size_bytes(), file.generation(),
                         std::move(sources), targets.front()});
      }
    }
  }
  return tasks;
}

}  // namespace objectstore
