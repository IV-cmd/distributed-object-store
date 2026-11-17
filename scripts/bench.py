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


def measure(cluster, samples, phase, operation, *args, layout="replica3"):
    response = cluster.client("bench", operation, *args)
    row = json.loads(response.stdout)
    row["layout"] = layout
    row["reconstructed"] = "RS reconstructed stripe=" in response.stderr
    if layout == "rs42" and operation == "get":
        if row["reconstructed"] != (phase == "latency-degraded"):
            raise RuntimeError("Unexpected RS read path during measurement")
    if (row["operation"] != operation or row["bytes"] <= 0
            or not math.isfinite(row["seconds"]) or row["seconds"] <= 0):
        raise RuntimeError(f"Invalid client measurement: {row}")
    row["phase"] = phase
    samples.append(row)
    return row


def summary(result):
    samples = result["samples"]
    lines = ["## Measured benchmarks", "",
             "| Object | replica3 write MB/s | rs42 write MB/s | replica3 read MB/s | rs42 read MB/s |",
             "|---|---:|---:|---:|---:|"]
    for mib in (1, 16, 64):
        rates = [statistics.median(row["MB_per_s"] for row in samples
                                   if row["phase"] == "throughput" and row["layout"] == layout
                                   and row["operation"] == operation and row["bytes"] == mib * MIB)
                 for operation in ("put", "get") for layout in ("replica3", "rs42")]
        lines.append(f"| {mib} MiB | " + " | ".join(f"{rate:.2f}" for rate in rates) + " |")
    lines += ["", "| 16 MiB read | Healthy | One holder DOWN; no spare |", "|---|---:|---:|"]
    for layout in ("replica3", "rs42"):
        milliseconds = [statistics.median(row["seconds"] for row in samples
                                          if row["layout"] == layout and row["phase"] == phase) * 1000
                        for phase in ("latency-healthy", "latency-degraded")]
        lines.append(f"| {layout} | {milliseconds[0]:.2f} ms | {milliseconds[1]:.2f} ms |")
    lines += ["", "| Object | replica3 payload/file | rs42 payload/file |", "|---|---:|---:|"]
    for mib in (1, 16, 64):
        ratios = [next(row["payload_bytes"] / row["object_bytes"] for row in result["storage"]
                       if row["layout"] == layout and row["object_bytes"] == mib * MIB)
                  for layout in ("replica3", "rs42")]
        lines.append(f"| {mib} MiB | {ratios[0]:.3f}x | {ratios[1]:.3f}x |")
    rebuild = result["rebuild"]
    lines += ["", "| replica3 64 MiB rebuild | Time |", "|---|---:|",
              f"| Kill request to DOWN | {rebuild['kill_to_down_s']:.3f} s |",
              f"| DOWN to RF=3 | {rebuild['down_to_rf3_s']:.3f} s |",
              f"| Kill request to RF=3 | {rebuild['kill_to_rf3_s']:.3f} s |"]
    return "\n".join(lines) + "\n"


def throughput(cluster, samples, storage, layout):
    for mib in (1, 16, 64):
        source = cluster.payload(mib)
        for trial in range(TRIALS):
            object_id = f"throughput-{mib}-{trial}"
            for operation, args in [("put", (source, object_id, layout)),
                                     ("get", (object_id, "/tmp/throughput-output"))]:
                row = measure(cluster, samples, "throughput", operation, *args, layout=layout)
                if row["bytes"] != mib * MIB:
                    raise RuntimeError(f"Wrong sample size: {row}")
            cluster.compare(source, "/tmp/throughput-output")
            if trial == 0:
                payload = total = 0
                for node in cluster.nodes:
                    files = cluster.compose("exec", "-T", node, "find",
                                            "/var/lib/objectstore/" + object_id,
                                            "-type", "f", "-printf", "%f %s\n").stdout
                    for line in files.splitlines():
                        name, length = line.rsplit(" ", 1)
                        total += int(length)
                        if name.endswith(".chunk"):
                            payload += int(length)
                storage.append({"layout": layout, "object_bytes": mib * MIB,
                                "payload_bytes": payload, "marker_and_checksum_bytes": total - payload})
            cluster.client("rm", object_id)
        print(f"Measured {layout} {mib} MiB put/get ({TRIALS} trials)", flush=True)


def main():
    samples, storage = [], []
    with Cluster("bench", nodes=3) as cluster:
        host = environment(cluster)
        directory = cluster.directory
        throughput(cluster, samples, storage, "replica3")

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

        cluster.compose("up", "--no-deps", "-d", "fst4")
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
                  "samples": samples, "degraded_node_states": {"replica3": degraded_nodes},
                  "rebuild": {"object_bytes": 64 * MIB, "chunks": 64,
                              "kill_requested_ms": kill_ms, "down_ms": down,
                              "rf3_ms": restored, "kill_to_down_s": (down - kill_ms) / 1000,
                              "down_to_rf3_s": (restored - down) / 1000,
                              "kill_to_rf3_s": (restored - kill_ms) / 1000},
                  "events": events}

    with Cluster("bench-rs", nodes=6) as cluster:
        throughput(cluster, samples, storage, "rs42")
        source = cluster.payload(16)
        cluster.client("put", source, "latency", "rs42")
        cluster.wait_rs("latency", 24)
        cluster.read_seconds("latency", source)
        for _ in range(TRIALS):
            measure(cluster, samples, "latency-healthy", "get", "latency", "/tmp/latency-output", layout="rs42")
            cluster.compare(source, "/tmp/latency-output")
        cluster.compose("stop", "-t", "1", "fst2")
        cluster.wait_node("fst2", "DOWN")
        cluster.wait_rs("latency", 24, missing=(1,))
        result["degraded_node_states"]["rs42"] = cluster.client("nodes").stdout
        for _ in range(TRIALS):
            measure(cluster, samples, "latency-degraded", "get", "latency", "/tmp/latency-output", layout="rs42")
            cluster.compare(source, "/tmp/latency-output")

    result["method"] = ("Single client; sequential replica3 or rs42 writes; 4 MiB RS stripes, "
                        "at most 1 MiB per fragment; warm OS cache, no fsync. "
                        "Client steady-clock Put/Get timer includes codec, file I/O and RPCs, "
                        "excludes Docker/process startup; decimal MB/s, binary MiB sizes. "
                        "Three initial replica3 holders or six rs42 holders on one host. "
                        "Degraded latency is one holder post-DOWN with no spare, not timeout cost; "
                        "RS reconstruction asserted in every degraded sample. "
                        "Storage sums actual .chunk file lengths; marker/checksum byte lengths "
                        "recorded separately; filesystem block allocation is not measured. "
                        "Synthetic repeating bytes 0..255; replica3 measured before rs42. "
                        "Rebuild is replica3 only, using same-host millisecond MGM log timestamps.")
    result["storage"] = storage
    (directory / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n")
    (directory / "summary.md").write_text(summary(result))
    print(summary(result), flush=True)
    print("PASS bench: both layouts, verified reconstruction, measured storage and RF=3 rebuild", flush=True)


if __name__ == "__main__":
    main()
