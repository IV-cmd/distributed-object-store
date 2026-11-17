#include "mgm/placement.h"

#include <algorithm>
#include <set>
#include <string>
#include <tuple>

namespace objectstore {

std::vector<v1::NodeInfo> SelectNodes(std::vector<v1::NodeInfo> available,
                                      std::size_t replication_factor) {
  std::sort(available.begin(), available.end(),
            [](const auto& a, const auto& b) {
              return std::tie(a.node_id(), a.endpoint()) <
                     std::tie(b.node_id(), b.endpoint());
            });
  std::vector<v1::NodeInfo> selected;
  std::set<std::string> ids;
  std::set<std::string> endpoints;
  for (const auto& node : available) {
    if (selected.size() == replication_factor) {
      break;
    }
    if (node.node_id().empty() || node.endpoint().empty() ||
        ids.contains(node.node_id()) || endpoints.contains(node.endpoint())) {
      continue;
    }
    ids.insert(node.node_id());
    endpoints.insert(node.endpoint());
    selected.push_back(node);
  }
  return selected;
}

}  // namespace objectstore
