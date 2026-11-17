#include "mgm/service.h"

#include <vector>

#include "common/log.h"

namespace objectstore {

void MgmService::Maintain(HealthClock::time_point now) {
  const std::lock_guard serial(maintenance_mutex_);
  const std::lock_guard lock(mutex_);
  std::vector<NodeWatch> watches;
  watches.reserve(nodes_.size());
  for (const auto& [id, node] : nodes_) {
    watches.push_back({node.info, node.last_heartbeat});
  }
  for (const auto& id : ExpiredNodes(watches, now, timeout_)) {
    auto& node = nodes_.at(id);
    node.info.set_state(v1::DOWN);
    node.info.set_incarnation(node.info.incarnation() + 1);
    std::size_t affected = 0;
    for (const auto& [file_id, file] : files_) {
      for (const auto& [index, chunk] : file.chunks) {
        for (const auto& holder : chunk.replicas()) {
          if (holder.node_id() == id) {
            ++affected;
          }
        }
      }
    }
    Log("node_down", "node=" + id + " affected_chunks=" +
                          std::to_string(affected));
  }
}

}  // namespace objectstore
