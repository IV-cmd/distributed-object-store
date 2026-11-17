#include "mgm/recovery.h"

#include <algorithm>
#include <set>
#include <utility>

#include "mgm/placement.h"

namespace objectstore {

namespace {

std::vector<v1::NodeInfo> Sources(const v1::ChunkMetadata& chunk,
                                  const std::vector<v1::NodeInfo>& nodes) {
  const auto& known = chunk.known_replicas().empty() ? chunk.replicas() : chunk.known_replicas();
  const std::set<std::string> invalid(chunk.invalid_replicas().begin(), chunk.invalid_replicas().end());
  std::vector<v1::NodeInfo> sources;
  for (const auto& holder : known) {
    const auto live = std::find_if(nodes.begin(), nodes.end(), [&](const auto& node) {
      return node.node_id() == holder.node_id() && node.state() == v1::UP;
    });
    if (live != nodes.end() && !invalid.contains(holder.node_id())) {
      sources.push_back(*live);
    }
  }
  return sources;
}

void AddHolders(const v1::ChunkMetadata& chunk, std::set<std::string>& holders) {
  const auto& known = chunk.known_replicas().empty() ? chunk.replicas() : chunk.known_replicas();
  for (const auto& holder : known) {
    holders.insert(holder.node_id());
  }
}

}  // namespace

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
      auto sources = Sources(chunk, nodes);
      std::set<std::string> holders;
      AddHolders(chunk, holders);
      std::vector<v1::ChunkMetadata> stripe;
      if (file.layout() == v1::RS42) {
        if (!sources.empty()) {
          continue;
        }
        std::set<std::uint32_t> indices;
        unsigned available = 0;
        for (const auto& peer : file.chunks()) {
          if (peer.chunk().index() / 6 != chunk.chunk().index() / 6) {
            continue;
          }
          AddHolders(peer, holders);
          auto live = peer;
          live.clear_replicas();
          const auto peers = Sources(peer, nodes);
          for (const auto& node : peers) {
            *live.add_replicas() = node;
          }
          if (peer.committed() && !peers.empty()) {
            ++available;
            sources.push_back(peers.front());
          }
          indices.insert(peer.chunk().index());
          stripe.push_back(std::move(live));
        }
        if (stripe.size() != 6 || indices.size() != 6 || available < 4) {
          continue;
        }
        std::sort(stripe.begin(), stripe.end(), [](const auto& left, const auto& right) {
          return left.chunk().index() < right.chunk().index();
        });
      } else if (sources.empty() || sources.size() >= 3) {
        continue;
      }
      std::vector<v1::NodeInfo> available;
      for (const auto& node : nodes) {
        if (!holders.contains(node.node_id())) {
          available.push_back(node);
        }
      }
      const auto targets = SelectNodes(std::move(available), 1);
      if (!targets.empty()) {
        tasks.push_back({chunk, file.size_bytes(), file.generation(),
                         std::move(sources), targets.front(), std::move(stripe)});
      }
    }
  }
  return tasks;
}

}  // namespace objectstore
