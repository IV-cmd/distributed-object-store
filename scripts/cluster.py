"""Disposable Compose stacks shared by the proof commands (Python standard library)."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / ".proof-results"
MIB = 1024 * 1024


def fields(line):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def replicas_ready(chunks, count_chunks, replicas=3, exclude=()):
    return (len(chunks) == count_chunks
            and {int(chunk["chunk"]) for chunk in chunks} == set(range(count_chunks))
            and all(int(chunk["replicas"]) == replicas
                    and len(set(chunk.get("nodes", "").split(","))) == replicas
                    and not set(exclude).intersection(chunk.get("nodes", "").split(","))
                    for chunk in chunks))


def rs_ready(chunks, count_chunks, missing=(), exclude=()):
    if (count_chunks % 6 or len(chunks) != count_chunks or len(set(missing)) > 2
            or {int(row["chunk"]) for row in chunks} != set(range(count_chunks))):
        return False
    holders = {}
    for row in chunks:
        index = int(row["chunk"])
        nodes = [node for node in row.get("nodes", "").split(",") if node]
        expected = 0 if index % 6 in missing else 1
        group = holders.setdefault(index // 6, set())
        if (int(row["replicas"]) != expected or len(nodes) != expected
                or int(row["stripe_index"]) != index % 6
                or row["kind"] != ("parity" if index % 6 >= 4 else "data")
                or set(nodes).intersection(group) or set(nodes).intersection(exclude)):
            return False
        group.update(nodes)
    return True


def run(args, *, env=None, check=True, timeout=60):
    result = subprocess.run(args, env=env, capture_output=True, text=True,
                            timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{' '.join(map(str, args))}:\n{result.stdout}{result.stderr}")
    return result


class Cluster:
    def __init__(self, name, *, nodes=4, faults=False):
        self.directory = RESULTS / name
        self.directory.mkdir(parents=True, exist_ok=True)
        self.env = dict(os.environ, MGM_PORT="0")
        self.command = ["docker", "compose", "-p", "proof-" + uuid.uuid4().hex[:10],
                        "-f", str(ROOT / "docker/docker-compose.yml")]
        if faults:
            self.command += ["-f", str(ROOT / "docker/chaos.override.yml")]
        self.nodes = [f"fst{i}" for i in range(1, nodes + 1)]

    def compose(self, *args, **kwargs):
        return run(self.command + list(args), env=self.env, **kwargs)

    def client(self, *args, **kwargs):
        return self.compose("exec", "-T", "mgm", "env", "MGM_ADDRESS=mgm:50051",
                            "client", *args, **kwargs)

    def __enter__(self):
        started = time.monotonic()
        try:
            self.compose("up", "--build", "-d", "mgm", *self.nodes, timeout=600)
            for node in self.nodes:
                self.wait_node(node, "UP")
            for service in ["mgm", *self.nodes]:
                uid = self.compose("exec", "-T", service, "id", "-u").stdout.strip()
                if uid == "0":
                    raise RuntimeError(f"{service} unexpectedly runs as root")
            self.setup_seconds = time.monotonic() - started
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        try:
            logs = self.compose("logs", "--no-color", check=False).stdout
            (self.directory / "containers.log").write_text(logs)
        finally:
            self.compose("down", "--volumes", "--remove-orphans", timeout=120)

    def __exit__(self, *exception):
        self.close()

    def wait(self, description, predicate, timeout=120):
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except RuntimeError as error:
                last = str(error)
            time.sleep(0.2)
        raise RuntimeError(f"Timed out waiting for {description}: {last}")

    def wait_node(self, node, state):
        return self.wait(f"{node} {state}", lambda: any(
            fields(line).get("node_id") == node and fields(line).get("state") == state
            for line in self.client("nodes").stdout.splitlines()))

    def chunks(self, object_id):
        return [fields(line) for line in self.client("stat", object_id).stdout.splitlines()
                if line.startswith("chunk=")]

    def wait_replicas(self, object_id, mib, count=3, exclude=()):
        self.wait(f"{object_id} RF={count}, excluding {exclude}",
                  lambda: replicas_ready(self.chunks(object_id), mib, count, exclude))

    def wait_rs(self, object_id, count_chunks, missing=(), exclude=()):
        self.wait(f"{object_id} RS layout, missing={missing}",
                  lambda: rs_ready(self.chunks(object_id), count_chunks, missing, exclude))

    def payload(self, mib):
        destination = f"/tmp/input-{mib}MiB"
        with tempfile.TemporaryDirectory(prefix="objectstore-proof-") as temporary:
            source = Path(temporary) / "input"
            with source.open("wb") as output:
                for _ in range(mib):
                    output.write(bytes(range(256)) * 4096)
            self.compose("cp", str(source), "mgm:" + destination)
        return destination

    def compare(self, source, output):
        self.compose("exec", "-T", "mgm", "cmp", source, output)

    def read_seconds(self, object_id, source, output="/tmp/read-output"):
        started = time.perf_counter()
        self.client("get", object_id, output)
        seconds = time.perf_counter() - started
        self.compare(source, output)
        return seconds

    def events(self):
        return [fields(line) for line in self.compose("logs", "--no-color", "mgm").stdout.splitlines()
                if "timestamp_ms=" in line]

    def save(self, name, value):
        (self.directory / name).write_text(json.dumps(value, indent=2) + "\n")
