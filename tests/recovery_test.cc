#include <gtest/gtest.h>

#include <chrono>
#include <vector>

#include "mgm/recovery.h"

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
                           std::chrono::milliseconds(6000)).empty());
  EXPECT_EQ(ExpiredNodes(nodes, start + std::chrono::milliseconds(6000),
                         std::chrono::milliseconds(6000)),
            std::vector<std::string>{"a"});
}

TEST(RecoveryTest, ReplacesDownHolderWithDistinctUpSpare) {
  const auto tasks = DecideRepairs({File()},
      {Node("a"), Node("b", v1::DOWN), Node("c"), Node("d"),
       Node("e", v1::DOWN)});
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_EQ(tasks[0].target.node_id(), "d");
  ASSERT_EQ(tasks[0].sources.size(), 2);
  EXPECT_EQ(tasks[0].sources[0].node_id(), "a");
  EXPECT_EQ(tasks[0].sources[1].node_id(), "c");
  EXPECT_EQ(tasks[0].generation, 12);
}

TEST(RecoveryTest, DoesNotInventTargetsWhenOnlyTwoHealthyHoldersExist) {
  EXPECT_TRUE(DecideRepairs({File()},
      {Node("a"), Node("b", v1::DOWN), Node("c")}).empty());
}

TEST(RecoveryTest, DoesNotCopyFromDownOrUnverifiedRejoiningNodes) {
  EXPECT_TRUE(DecideRepairs({File()},
      {Node("a", v1::DOWN), Node("b", v1::REJOINING),
       Node("c", v1::DOWN), Node("d")}).empty());
}

TEST(RecoveryTest, SkipsCompleteReplicationAndUnfinishedUploads) {
  EXPECT_TRUE(DecideRepairs({File()},
      {Node("a"), Node("b"), Node("c"), Node("d")}).empty());
  auto file = File();
  file.set_complete(false);
  EXPECT_TRUE(DecideRepairs({file},
      {Node("a"), Node("b", v1::DOWN), Node("c"), Node("d")}).empty());
}

TEST(RecoveryTest, ExcludesQuarantinedCopiesFromSources) {
  auto file = File();
  file.mutable_chunks(0)->add_invalid_replicas("a");
  const auto tasks = DecideRepairs({file},
      {Node("a"), Node("b", v1::DOWN), Node("c"), Node("d")});
  ASSERT_EQ(tasks.size(), 1);
  ASSERT_EQ(tasks[0].sources.size(), 1);
  EXPECT_EQ(tasks[0].sources[0].node_id(), "c");
  EXPECT_EQ(tasks[0].target.node_id(), "d");
}

}  // namespace
}  // namespace objectstore
