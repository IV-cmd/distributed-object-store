#!/usr/bin/env python3
"""Measure the real client path; save raw samples and a reproducible summary."""

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import statistics
import time

from cluster import Cluster, MIB, run

TRIALS = 3


def environment(cluster):
    cpu = next((line.split(":", 1)[1].strip()
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), "unknown")
    docker = run(["docker", "info", "--format",
                  '{"version":"{{.ServerVersion}}","storage_driver":"{{.Driver}}",'
                  '"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}']).stdout
    repository, run_id = os.getenv("GITHUB_REPOSITORY"), os.getenv("GITHUB_RUN_ID")
    return {"measured_at_utc": datetime.now(timezone.utc).isoformat(),
            "revision": run(["git", "rev-parse", "HEAD"]).stdout.strip(),
            "actions_url": (f"https://github.com/{repository}/actions/runs/{run_id}"
                            if repository and run_id else None),
            "runner_os": os.getenv("ImageOS", platform.system()),
            "kernel": platform.release(), "cpu_model": cpu,
            "docker": json.loads(docker),
            "compose": run(["docker", "compose", "version", "--short"]).stdout.strip(),
            "heartbeat_interval_ms": int(cluster.env.get("HEARTBEAT_INTERVAL_MS", "2000")),
            "heartbeat_timeout_ms": int(cluster.env.get("HEARTBEAT_TIMEOUT_MS", "6000")),
            "repair_interval_ms": int(cluster.env.get("REPAIR_INTERVAL_MS", "500"))}


def measure(cluster, samples, phase, operation, *args):
    row = json.loads(cluster.client("bench", operation, *args).stdout)
    if (row["operation"] != operation or row["bytes"] <= 0
            or not math.isfinite(row["seconds"]) or row["seconds"] <= 0):
        raise RuntimeError(f"Invalid client measurement: {row}")
    row["phase"] = phase
    samples.append(row)
    return row


def summary(result):
    samples = result["samples"]
    lines = ["## Measured benchmarks", "", "| Object | RF=3 write MB/s | Healthy read MB/s |",
             "|---|---:|---:|"]
    for mib in (1, 16, 64):
        rates = [statistics.median(row["MB_per_s"] for row in samples
                                   if row["phase"] == "throughput"
                                   and row["operation"] == operation and row["bytes"] == mib * MIB)
                 for operation in ("put", "get")]
        lines.append(f"| {mib} MiB | {rates[0]:.2f} | {rates[1]:.2f} |")
    lines += ["", "| 16 MiB reads | Median latency |", "|---|---:|"]
    for phase, label in [("latency-healthy", "Three healthy holders"),
                         ("latency-degraded", "One holder DOWN; RF=2; spare not started")]:
        milliseconds = statistics.median(row["seconds"] for row in samples
                                         if row["phase"] == phase) * 1000
        lines.append(f"| {label} | {milliseconds:.2f} ms |")
    rebuild = result["rebuild"]
    lines += ["", "| 64 MiB rebuild | Time |", "|---|---:|",
              f"| Kill request to DOWN | {rebuild['kill_to_down_s']:.3f} s |",
              f"| DOWN to RF=3 | {rebuild['down_to_rf3_s']:.3f} s |",
              f"| Kill request to RF=3 | {rebuild['kill_to_rf3_s']:.3f} s |"]
    return "\n".join(lines) + "\n"


def main():
    samples = []
    with Cluster("bench", nodes=3) as cluster:
        host = environment(cluster)
        for mib in (1, 16, 64):
            source = cluster.payload(mib)
            for trial in range(TRIALS):
                object_id = f"throughput-{mib}-{trial}"
                for operation, args in [("put", (source, object_id)),
                                         ("get", (object_id, "/tmp/throughput-output"))]:
                    row = measure(cluster, samples, "throughput", operation, *args)
                    if row["bytes"] != mib * MIB:
                        raise RuntimeError(f"Wrong sample size: {row}")
                cluster.compare(source, "/tmp/throughput-output")
                cluster.client("rm", object_id)
            print(f"Measured {mib} MiB put/get ({TRIALS} trials)", flush=True)

        source = cluster.payload(16)
        cluster.client("put", source, "latency")
        cluster.read_seconds("latency", source)  # Unmeasured warm-up.
        for _ in range(TRIALS):
            measure(cluster, samples, "latency-healthy", "get", "latency", "/tmp/latency-output")
            cluster.compare(source, "/tmp/latency-output")
        cluster.compose("stop", "-t", "1", "fst2")
        cluster.wait_node("fst2", "DOWN")
        cluster.wait_replicas("latency", 16, count=2, exclude=("fst2",))
        degraded_nodes = cluster.client("nodes").stdout
        for _ in range(TRIALS):
            measure(cluster, samples, "latency-degraded", "get", "latency", "/tmp/latency-output")
            cluster.compare(source, "/tmp/latency-output")
        cluster.compose("start", "fst2")
        cluster.wait_node("fst2", "UP")
        cluster.wait_replicas("latency", 16)
        cluster.client("rm", "latency")
        if cluster.client("ls").stdout.strip() != "ls OK":
            raise RuntimeError("Unexpected data would contaminate the rebuild volume")

        cluster.compose("up", "-d", "fst4")
        cluster.wait_node("fst4", "UP")
        source = "/tmp/input-64MiB"
        cluster.client("put", source, "rebuild")
        cluster.wait_replicas("rebuild", 64)
        if not all("fst2" in chunk["nodes"].split(",") for chunk in cluster.chunks("rebuild")):
            raise RuntimeError("Rebuild victim does not hold every measured chunk")
        kill_ms = time.time_ns() // 1_000_000
        cluster.compose("stop", "-t", "1", "fst2")
        cluster.wait_node("fst2", "DOWN")
        cluster.wait_replicas("rebuild", 64, exclude=("fst2",))
        cluster.client("get", "rebuild", "/tmp/rebuilt-output")
        cluster.compare(source, "/tmp/rebuilt-output")
        events = cluster.events()
        down = next(int(event["timestamp_ms"]) for event in events
                    if event.get("event") == "node_down" and event.get("node") == "fst2"
                    and int(event["timestamp_ms"]) >= kill_ms)
        repaired = [event for event in events if event.get("event") == "repair_completed"
                    and event.get("object") == "rebuild" and event.get("target") == "fst4"]
        if len(repaired) != 64 or {int(event["chunk"]) for event in repaired} != set(range(64)):
            raise RuntimeError("Missing distinct per-chunk repair completion evidence")
        restored = max(int(event["timestamp_ms"]) for event in repaired)
        result = {"environment": host, "trials": TRIALS,
                  "method": "Single client, sequential RF=3 writes, warm OS cache, no fsync; "
                            "client steady-clock Put/Get timer includes file I/O and RPCs, "
                            "excludes Docker/process startup; decimal MB/s, binary MiB sizes. "
                            "Degraded latency is post-DOWN with no spare started, not timeout cost. "
                            "Rebuild uses same-host millisecond MGM log timestamps.",
                  "samples": samples, "degraded_node_states": degraded_nodes,
                  "rebuild": {"object_bytes": 64 * MIB, "chunks": 64,
                              "kill_requested_ms": kill_ms, "down_ms": down,
                              "rf3_ms": restored, "kill_to_down_s": (down - kill_ms) / 1000,
                              "down_to_rf3_s": (restored - down) / 1000,
                              "kill_to_rf3_s": (restored - kill_ms) / 1000},
                  "events": events}
        cluster.save("benchmark.json", result)
        (cluster.directory / "summary.md").write_text(summary(result))
        print(summary(result), flush=True)
        print("PASS bench: all samples verified; known-volume rebuild completed", flush=True)


if __name__ == "__main__":
    main()
