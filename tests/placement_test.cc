#include "mgm/placement.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace objectstore {
namespace {

v1::NodeInfo Node(const std::string& id) {
  v1::NodeInfo node;
  node.set_node_id(id);
  node.set_endpoint(id + ":50052");
  return node;
}

TEST(PlacementTest, ReturnsThreeDistinctNodesDeterministically) {
  std::vector<v1::NodeInfo> available{Node("c"), Node("a"), Node("d"), Node("b")};
  const auto selected = SelectNodes(available, 3);
  ASSERT_EQ(selected.size(), 3);
  EXPECT_EQ(selected[0].node_id(), "a");
  EXPECT_EQ(selected[1].node_id(), "b");
  EXPECT_EQ(selected[2].node_id(), "c");
  std::reverse(available.begin(), available.end());
  const auto again = SelectNodes(available, 3);
  for (std::size_t i = 0; i < selected.size(); ++i) {
    EXPECT_EQ(selected[i].SerializeAsString(), again[i].SerializeAsString());
  }
}

TEST(PlacementTest, ReturnsOnlyAvailableNodesWhenTooFewExist) {
  EXPECT_TRUE(SelectNodes({}, 3).empty());
  EXPECT_EQ(SelectNodes({Node("a"), Node("b")}, 3).size(), 2);
  EXPECT_TRUE(SelectNodes({Node("a")}, 0).empty());
}

TEST(PlacementTest, NeverCountsDuplicateIdsOrEndpointsAsReplicas) {
  auto alias = Node("alias");
  alias.set_endpoint("a:50052");
  const auto selected = SelectNodes({Node("a"), Node("a"), alias, Node("b")}, 3);
  EXPECT_EQ(selected.size(), 2);
  std::set<std::string> ids;
  std::set<std::string> endpoints;
  for (const auto& node : selected) {
    EXPECT_TRUE(ids.insert(node.node_id()).second);
    EXPECT_TRUE(endpoints.insert(node.endpoint()).second);
  }
}

}  // namespace
}  // namespace objectstore
