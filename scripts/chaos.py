#!/usr/bin/env python3
"""Independent fault scenarios; any failed assertion makes the command fail."""

import json
import statistics
import threading
import time

from cluster import Cluster, RESULTS


def node_stop():
    with Cluster("chaos/node-stop") as cluster:
        source = cluster.payload(3)
        cluster.client("put", source, "node-stop")
        cluster.wait_replicas("node-stop", 3)
        done, first_read = threading.Event(), threading.Event()
        samples, errors = [], []

        def reader():
            while not done.is_set():
                try:
                    samples.append(cluster.read_seconds("node-stop", source))
                    first_read.set()
                except Exception as error:
                    errors.append(str(error))
                    first_read.set()
                    return
                done.wait(0.1)

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            if not first_read.wait(30) or errors:
                raise RuntimeError(f"Initial sampled read failed: {errors}")
            kill_ms = time.time_ns() // 1_000_000
            cluster.compose("stop", "-t", "1", "fst2")
            cluster.wait_node("fst2", "DOWN")
            cluster.wait_replicas("node-stop", 3, exclude=("fst2",))
            cluster.compose("start", "fst2")
            cluster.wait_node("fst2", "UP")
            cluster.wait_replicas("node-stop", 3, count=4)
        finally:
            done.set()
            thread.join()
        if errors:
            raise RuntimeError(f"Read failed during recovery: {errors}")
        events = cluster.events()
        if not any(event.get("event") == "node_up" and event.get("node") == "fst2"
                   and event.get("verified") == "3" for event in events):
            raise RuntimeError("Returning node was not inventory-verified")
        cluster.save("result.json", {"kill_requested_ms": kill_ms,
                                    "sampled_reads": len(samples), "events": events})
        return f"{len(samples)} byte-equal sampled reads; RF=3 on spare; verified rejoin"


def disk_corruption():
    with Cluster("chaos/disk-corruption") as cluster:
        source = cluster.payload(3)
        cluster.client("put", source, "corrupt")
        cluster.wait_replicas("corrupt", 3)
        cluster.compose("exec", "-T", "fst2", "sh", "-c",
                        "printf '\\001' | dd of=/var/lib/objectstore/corrupt/1.chunk "
                        "bs=1 count=1 conv=notrunc status=none")
        result = cluster.client("get", "corrupt", "/tmp/corrupt-output")
        cluster.compare(source, "/tmp/corrupt-output")
        rejection = "replica rejected node_id=fst2 chunk=1 code=15"
        if rejection not in result.stderr:
            raise RuntimeError("No DATA_LOSS rejection from the deliberately corrupted copy")
        cluster.save("result.json", {"rejection": result.stderr.strip(),
                                    "byte_equal": True, "proactive_repair": False})
        return "corrupt replica rejected; healthy fallback byte-equal (no scrub/repair claim)"


def network_delay():
    with Cluster("chaos/network-delay", faults=True) as cluster:
        source = cluster.payload(3)
        cluster.client("put", source, "delayed")
        cluster.wait_replicas("delayed", 3)

        def readings():
            return [cluster.read_seconds("delayed", source) for _ in range(3)]

        healthy = readings()
        tc = ["exec", "-T", "-u", "0", "fst2", "tc", "qdisc"]
        try:
            cluster.compose(*tc, "add", "dev", "eth0", "root", "netem", "delay", "100ms")
        except RuntimeError as error:
            raise RuntimeError("tc/netem unavailable; no latency result substituted. "
                               "Use a Linux Docker host with netem and NET_ADMIN. " + str(error)) from error
        try:
            rule = cluster.compose(*tc, "show", "dev", "eth0").stdout
            if "netem" not in rule or "delay 100ms" not in rule:
                raise RuntimeError(f"Delay rule not installed: {rule}")
            delayed = readings()
            cluster.wait_node("fst2", "UP")
        finally:
            cluster.compose(*tc, "del", "dev", "eth0", "root")
        if "netem" in cluster.compose(*tc, "show", "dev", "eth0").stdout:
            raise RuntimeError("Delay rule was not removed")
        recovered = readings()
        medians = [statistics.median(values) for values in (healthy, delayed, recovered)]
        if not (medians[1] > medians[0] and medians[2] < medians[1]):
            raise RuntimeError(f"Expected latency rise and recovery not observed: {medians}")
        cluster.save("result.json", {"netem_egress_ms": 100, "rule": rule.strip(),
                                    "healthy_seconds": healthy, "delayed_seconds": delayed,
                                    "recovered_seconds": recovered,
                                    "timing": "CLI wall time, including docker exec; not a WAN model"})
        return (f"CLI medians healthy/delayed/restored="
                f"{medians[0]*1000:.1f}/{medians[1]*1000:.1f}/{medians[2]*1000:.1f} ms; "
                "100 ms egress netem removed; every download byte-equal")


def main():
    results = []
    for name, scenario in [("node-stop", node_stop), ("disk-corruption", disk_corruption),
                           ("network-delay", network_delay)]:
        print(f"TEST {name}", flush=True)
        try:
            detail = scenario()
            passed = True
        except Exception as error:
            detail, passed = str(error), False
        results.append({"scenario": name, "passed": passed, "detail": detail})
        print(f"{'PASS' if passed else 'FAIL'} {name}: {detail}", flush=True)
    (RESULTS / "chaos/results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
