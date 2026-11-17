import unittest

from cluster import fields, replicas_ready


class MetadataAssertions(unittest.TestCase):
    def chunk(self, index=0, nodes="fst1,fst3,fst4"):
        return fields(f"chunk={index} checksum=123 replicas=3 nodes={nodes}")

    def test_accepts_three_distinct_copies(self):
        self.assertTrue(replicas_ready([self.chunk()], 1, exclude=("fst2",)))

    def test_rejects_duplicate_holders(self):
        self.assertFalse(replicas_ready([self.chunk(nodes="fst1,fst1,fst4")], 1))

    def test_rejects_old_failed_holder(self):
        self.assertFalse(replicas_ready([self.chunk(nodes="fst1,fst2,fst3")], 1,
                                        exclude=("fst2",)))

    def test_rejects_missing_chunks(self):
        self.assertFalse(replicas_ready([], 1))

    def test_rejects_repeated_chunk_indices(self):
        self.assertFalse(replicas_ready([self.chunk(), self.chunk()], 2))

    def test_parses_structured_log_prefix(self):
        self.assertEqual(fields("mgm | timestamp_ms=123 event=node_down node=fst2"),
                         {"timestamp_ms": "123", "event": "node_down", "node": "fst2"})
