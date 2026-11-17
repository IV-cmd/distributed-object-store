#!/usr/bin/env python3
"""Prove degraded reads separately from repair so healing cannot hide a loss."""

from itertools import combinations
import time

from cluster import Cluster


def verified_read(cluster, source, object_id, output):
    result = cluster.client("get", object_id, output)
    cluster.compare(source, output)
    return "RS reconstructed stripe=" in result.stderr


def main():
    pairs = []
    with Cluster("erasure", nodes=6) as cluster:
        source = "/tmp/rs-input-unaligned"
        cluster.compose("exec", "-T", "mgm", "sh", "-c",
                        f"head -c 5242887 /dev/urandom > {source}")
        cluster.client("put", source, "rs-pairs", "rs42")
        initial = cluster.wait_rs("rs-pairs", 12)
        for begin in range(0, len(initial), 6):
            if len({row["checksum"] for row in initial[begin:begin + 6]}) != 6:
                raise AssertionError("proof requires six distinct fragment hashes")
        assert not verified_read(cluster, source, "rs-pairs", "/tmp/rs-output")
        for first, second in combinations(range(1, 7), 2):
            victims = (f"fst{first}", f"fst{second}")
            cluster.compose("stop", "-t", "1", *victims)
            for node in victims:
                cluster.wait_node(node, "DOWN")
            cluster.wait_rs("rs-pairs", 12, missing=(first - 1, second - 1))
            decoded = verified_read(cluster, source, "rs-pairs", "/tmp/rs-output")
            if decoded != (first <= 4):
                raise RuntimeError("Wrong RS fast-path/reconstruction behavior")
            pairs.append({"lost_nodes": victims, "byte_equal": True, "decoded": decoded})
            cluster.compose("start", *victims)
            for node in victims:
                cluster.wait_node(node, "UP")
            cluster.wait_rs("rs-pairs", 12)
            print(f"PASS RS pair {victims}: byte-equal read; decoded={decoded}", flush=True)
        cluster.client("rm", "rs-pairs")

        cluster.client("put", source, "rs-corrupt", "rs42")
        cluster.compose("exec", "-T", "fst2", "sh", "-c",
                        "printf '\\377' | dd of=/var/lib/objectstore/rs-corrupt/1.chunk "
                        "bs=1 count=1 conv=notrunc status=none")
        assert verified_read(cluster, source, "rs-corrupt", "/tmp/rs-output")
        cluster.client("rm", "rs-corrupt")
        print("PASS RS corrupt fragment: checksum rejection and byte-equal reconstruction", flush=True)

        cluster.client("put", source, "rs-repair", "rs42")
        cluster.compose("up", "--no-deps", "-d", "fst7", "fst8")
        for node in ("fst7", "fst8"):
            cluster.wait_node(node, "UP")
            assert cluster.compose("exec", "-T", node, "id", "-u").stdout.strip() != "0"
        cluster.client("stat", "rs-repair")
        cluster.compose("stop", "-t", "1", "fst1", "fst5")
        for node in ("fst1", "fst5"):
            cluster.wait_node(node, "DOWN")
        cluster.wait_rs("rs-repair", 12, exclude=("fst1", "fst5"))
        verified_read(cluster, source, "rs-repair", "/tmp/rs-output")
        chunks = cluster.chunks("rs-repair")
        assert {"fst7", "fst8"}.issubset({row["nodes"] for row in chunks})
        repairs = [event for event in cluster.events()
                   if event.get("event") == "repair_completed" and event.get("object") == "rs-repair"]
        assert {int(event["chunk"]) for event in repairs} == {0, 4, 6, 10}
        cluster.save("erasure.json", {"two_loss_pairs": pairs, "corrupt_read": "passed",
                                      "repaired_chunks": chunks, "repair_events": repairs})
        print("PASS RS repair: six distinct live holders per stripe; fst1/fst5 remain DOWN", flush=True)

    with Cluster("erasure-three-loss", nodes=6) as cluster:
        source = cluster.payload(5)
        cluster.client("put", source, "rs-loss", "rs42")
        cluster.compose("stop", "-t", "1", "fst1", "fst2", "fst3")
        for node in ("fst1", "fst2", "fst3"):
            cluster.wait_node(node, "DOWN")
        cluster.compose("exec", "-T", "mgm", "sh", "-c", "printf keep > /tmp/lost-output")
        started = time.monotonic()
        result = cluster.client("get", "rs-loss", "/tmp/lost-output", check=False, timeout=15)
        elapsed = time.monotonic() - started
        assert result.returncode != 0 and "requires four verified shards" in result.stderr
        assert cluster.compose("exec", "-T", "mgm", "cat", "/tmp/lost-output").stdout == "keep"
        cluster.save("three-loss.json", {"stderr": result.stderr, "elapsed_seconds": elapsed,
                                         "chunks": cluster.chunks("rs-loss"), "output_preserved": True})
        print(f"PASS RS three-loss failure ({elapsed:.3f}s): {result.stderr.strip()}", flush=True)


if __name__ == "__main__":
    main()
