import unittest

from cluster import fields, replicas_ready, rs_ready


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


class BenchmarkUsage(unittest.TestCase):
    def test_rejects_non_transfer_benchmark(self):
        from cluster import ROOT, run
        binary = ROOT / "build/src/client/client"
        if not binary.exists():
            self.skipTest("Build the C++ client to check its usage contract")
        result = run([str(binary), "bench", "nodes"], check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("Usage:", result.stderr)


class RsAssertions(unittest.TestCase):
    def chunks(self, missing=()):
        return [fields(f"chunk={i} replicas={0 if i in missing else 1} "
                       f"nodes={'' if i in missing else 'fst' + str(i + 1)} "
                       f"stripe_index={i} kind={'parity' if i >= 4 else 'data'}")
                for i in range(6)]

    def test_accepts_six_distinct_fragments(self):
        self.assertTrue(rs_ready(self.chunks(), 6))

    def test_accepts_two_missing_but_not_three(self):
        self.assertTrue(rs_ready(self.chunks((0, 1)), 6, missing=(0, 1)))
        self.assertFalse(rs_ready(self.chunks((0, 1, 2)), 6, missing=(0, 1, 2)))

    def test_rejects_colocated_fragments_and_missing_metadata(self):
        chunks = self.chunks()
        chunks[1]["nodes"] = "fst1"
        self.assertFalse(rs_ready(chunks, 6))
        self.assertFalse(rs_ready([], 6))

    def test_rejects_wrong_parity_and_failed_holders(self):
        chunks = self.chunks()
        chunks[4]["kind"] = "data"
        self.assertFalse(rs_ready(chunks, 6))
        self.assertFalse(rs_ready(self.chunks(), 6, exclude=("fst2",)))
