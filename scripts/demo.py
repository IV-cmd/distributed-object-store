#!/usr/bin/env python3
"""Write, lose one FST, repair onto the spare, and verify the download."""

import time

from cluster import Cluster


def main():
    with Cluster("demo") as cluster:
        started = time.monotonic()
        source = cluster.payload(3)
        cluster.client("put", source, "demo")
        cluster.wait_replicas("demo", 3)
        print("Uploaded 3 MiB with three copies; stopping fst2", flush=True)
        cluster.compose("stop", "-t", "1", "fst2")
        cluster.wait_node("fst2", "DOWN")
        cluster.wait_replicas("demo", 3, exclude=("fst2",))
        print(cluster.client("stat", "demo").stdout, end="", flush=True)
        cluster.client("get", "demo", "/tmp/demo-output")
        cluster.compare(source, "/tmp/demo-output")
        seconds = time.monotonic() - started
        cluster.save("demo.json", {"setup_seconds": cluster.setup_seconds,
                                  "sequence_seconds": seconds,
                                  "object_bytes": 3 * 1024 * 1024,
                                  "byte_equal": True})
        print(f"PASS demo: verified repair and download; sequence={seconds:.3f}s "
              f"(image build/startup={cluster.setup_seconds:.3f}s separately)", flush=True)


if __name__ == "__main__":
    main()
