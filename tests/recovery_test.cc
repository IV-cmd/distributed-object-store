#include "mgm/recovery.h"

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

namespace objectstore {
namespace {

v1::NodeInfo Node(const std::string& id, v1::NodeState state = v1::UP) {
  v1::NodeInfo node;
  node.set_node_id(id);
  node.set_endpoint(id + ":50052");
  node.set_state(state);
  return node;
}

v1::FileMetadata File() {
  v1::FileMetadata file;
  file.set_object_id("sample");
  file.set_complete(true);
  file.set_size_bytes(7);
  file.set_generation(12);
  auto* chunk = file.add_chunks();
  chunk->mutable_chunk()->set_object_id("sample");
  chunk->set_committed(true);
  chunk->set_checksum(42);
  for (const auto* id : {"a", "b", "c"}) {
    *chunk->add_replicas() = Node(id);
  }
  return file;
}

TEST(RecoveryTest, TimeoutBoundaryIsDeterministicAndDownIsNotRepeated) {
  const auto start = HealthClock::time_point{};
  const std::vector<NodeWatch> nodes{{Node("a"), start},
                                     {Node("b", v1::DOWN), start}};
  EXPECT_TRUE(ExpiredNodes(nodes, start + std::chrono::milliseconds(5999),
                           std::chrono::milliseconds(6000))
                  .empty());
  EXPECT_EQ(ExpiredNodes(nodes, start + std::chrono::milliseconds(6000),
                         std::chrono::milliseconds(6000)),
            std::vector<std::string>{"a"});
}

TEST(RecoveryTest, ReplacesDownHolderWithDistinctUpSpare) {
  const auto tasks =
      DecideRepairs({File()}, {Node("a"), Node("b", v1::DOWN), Node("c"),
                               Node("d"), Node("e", v1::DOWN)});
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_EQ(tasks[0].target.node_id(), "d");
  ASSERT_EQ(tasks[0].sources.size(), 2);
  EXPECT_EQ(tasks[0].sources[0].node_id(), "a");
  EXPECT_EQ(tasks[0].sources[1].node_id(), "c");
  EXPECT_EQ(tasks[0].generation, 12);
}

TEST(RecoveryTest, DoesNotInventTargetsWhenOnlyTwoHealthyHoldersExist) {
  EXPECT_TRUE(
      DecideRepairs({File()}, {Node("a"), Node("b", v1::DOWN), Node("c")})
          .empty());
}

TEST(RecoveryTest, DoesNotCopyFromDownOrUnverifiedRejoiningNodes) {
  EXPECT_TRUE(
      DecideRepairs({File()}, {Node("a", v1::DOWN), Node("b", v1::REJOINING),
                               Node("c", v1::DOWN), Node("d")})
          .empty());
}

TEST(RecoveryTest, SkipsCompleteReplicationAndUnfinishedUploads) {
  EXPECT_TRUE(
      DecideRepairs({File()}, {Node("a"), Node("b"), Node("c"), Node("d")})
          .empty());
  auto file = File();
  file.set_complete(false);
  EXPECT_TRUE(DecideRepairs({file}, {Node("a"), Node("b", v1::DOWN), Node("c"),
                                     Node("d")})
                  .empty());
}

TEST(RecoveryTest, ExcludesQuarantinedCopiesFromSources) {
  auto file = File();
  file.mutable_chunks(0)->add_invalid_replicas("a");
  const auto tasks = DecideRepairs(
      {file}, {Node("a"), Node("b", v1::DOWN), Node("c"), Node("d")});
  ASSERT_EQ(tasks.size(), 1);
  ASSERT_EQ(tasks[0].sources.size(), 1);
  EXPECT_EQ(tasks[0].sources[0].node_id(), "c");
  EXPECT_EQ(tasks[0].target.node_id(), "d");
}

TEST(RecoveryTest, ReconstructsRsFromFourShardsOntoDistinctSpareNodes) {
  auto file = File();
  file.set_layout(v1::RS42);
  file.clear_chunks();
  std::vector<v1::NodeInfo> nodes;
  for (const std::string id : {"a", "b", "c", "d", "e", "f", "g", "h"}) {
    nodes.push_back(Node(id, id == "a" || id == "b" ? v1::DOWN : v1::UP));
  }
  for (unsigned index = 0; index < 6; ++index) {
    auto* chunk = file.add_chunks();
    chunk->mutable_chunk()->set_object_id("sample");
    chunk->mutable_chunk()->set_index(index);
    chunk->set_stripe_index(index);
    chunk->set_parity(index >= 4);
    chunk->set_committed(true);
    *chunk->add_replicas() = nodes[index];
  }
  const auto tasks = DecideRepairs({file}, nodes);
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_EQ(tasks[0].target.node_id(), "g");
  EXPECT_EQ(tasks[0].stripe.size(), 6);
  EXPECT_EQ(tasks[0].sources.size(), 4);
  *file.mutable_chunks(0)->add_replicas() = nodes[6];
  const auto remaining = DecideRepairs({file}, nodes);
  ASSERT_EQ(remaining.size(), 1);
  EXPECT_EQ(remaining[0].target.node_id(), "h");
  nodes[2].set_state(v1::DOWN);
  file.mutable_chunks(0)->mutable_replicas()->RemoveLast();
  EXPECT_TRUE(DecideRepairs({file}, nodes).empty());
}

}  // namespace
}  // namespace objectstore
