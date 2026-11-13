"""Store failover drill: queue traffic on fakellm while a Valkey primary fails over.

Starts a throwaway primary and replica in Docker (ports 6391 / 6392) and a TCP proxy on 6390
that plays the store's DNS name, then sends 10 calls a second for 30 s and at 8 s:

- `failover`: FAILOVER (the primary becomes a replica); the name moves to the new primary.
- `slowdns`: FAILOVER, but the name keeps pointing at the old primary for 6 s (READONLY).
- `restart`: every connection drops and the node refuses new ones for 6 s.

Pass mark: `failover` and `restart` lose no call. In `slowdns` only calls already in flight
when the store stopped taking writes may fail (`deadline_exceeded`), at most 5 %.

    python scripts/failover.py                  # all three
    python scripts/failover.py slowdns          # one
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from taskiq_llm_queue import CallFailed, LLMQueue  # noqa: E402  # needs the sys.path entry

NODES = {"a": ("llmqfo-a", 6391), "b": ("llmqfo-b", 6392)}
NETWORK = "llmqfo"
IMAGE = "valkey/valkey:8.1-alpine"
PROXY_PORT = 6390
RUN_SECS = 30
EVENT_AT_SECS = 8
OUTAGE_SECS = 6
REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "hi"}]}],
    "inferenceConfig": {"maxTokens": 20},
}


def docker(*args: str, check: bool = True) -> str:
    """Run one docker command; its stdout."""
    proc = subprocess.run(["docker", *args], capture_output=True, text=True, check=check)
    return proc.stdout.strip()


def role(node: str) -> str:
    """`master` or `slave`."""
    return docker("exec", NODES[node][0], "valkey-cli", "role").splitlines()[0]


def nodes_up() -> None:
    """A primary (a) and its replica (b), replication link up."""
    nodes_down()
    docker("network", "create", NETWORK)
    name_a, port_a = NODES["a"]
    name_b, port_b = NODES["b"]
    docker("run", "-d", "--name", name_a, "--network", NETWORK, "-p", f"{port_a}:6379", IMAGE)
    docker(
        "run", "-d", "--name", name_b, "--network", NETWORK, "-p", f"{port_b}:6379", IMAGE,
        "valkey-server", "--replicaof", name_a, "6379",
    )  # fmt: skip
    wait_link()


def wait_link() -> None:
    """Until the replica's link to the primary is up (20 s at most)."""
    for _ in range(40):
        replica = "b" if role("a") == "master" else "a"
        info = docker("exec", NODES[replica][0], "valkey-cli", "info", "replication", check=False)
        if "master_link_status:up" in info:
            return
        time.sleep(0.5)
    raise SystemExit("replication link did not come up")


def nodes_down() -> None:
    """Remove the drill's containers and network."""
    for name, _ in NODES.values():
        docker("rm", "-f", name, check=False)
    docker("network", "rm", NETWORK, check=False)


class Proxy:
    """The store's DNS name: new connections go to `target`; old ones stay where they are."""

    def __init__(self) -> None:
        """Start on node a, taking connections."""
        self.target = "a"
        self.refuse = False
        self.open: list[asyncio.StreamWriter] = []

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        """Forward one client connection to the current target."""
        if self.refuse:
            client_w.close()
            return
        try:
            node_r, node_w = await asyncio.open_connection("127.0.0.1", NODES[self.target][1])
        except OSError:
            client_w.close()
            return
        self.open += [client_w, node_w]
        await asyncio.gather(self._pipe(client_r, node_w), self._pipe(node_r, client_w))

    def drop_all(self) -> None:
        """Close every open connection (a node restart)."""
        for writer in self.open:
            writer.close()
        self.open.clear()


def environment(prefix: str) -> dict[str, str]:
    """`LLMQ_*` for the drill: the proxy as store, fakellm, quotas far above the load."""
    doc = json.loads((ROOT / "samples" / "registry.json").read_text(encoding="utf-8"))
    for model in doc["models"].values():
        model["rpm"], model["tpm"] = 100_000, 100_000_000
    return {
        "LLMQ_STORE_URL": f"redis://127.0.0.1:{PROXY_PORT}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
        "LLMQ_REGISTRY": json.dumps(doc),
        "LLMQ_METRICS_SINK": "none",
        "LLMQ_BEDROCK_ENDPOINT_URL": "http://127.0.0.1:8080",
    }


async def drill(scenario: str) -> bool:
    """Run one scenario; print per-second results; True when it meets the pass mark."""
    if role("a") != "master":  # the previous scenario failed over: swap back
        docker("exec", NODES["b"][0], "valkey-cli", "FAILOVER")
        await asyncio.sleep(2)
    await asyncio.to_thread(wait_link)
    proxy = Proxy()
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", PROXY_PORT)
    llmq = LLMQueue.from_env(environment(f"llmqfo:{uuid.uuid4().hex[:6]}"))
    await llmq.start()
    started = time.monotonic()
    ok: collections.Counter[int] = collections.Counter()
    failed: collections.Counter[int] = collections.Counter()
    errors: collections.Counter[str] = collections.Counter()
    latencies: list[float] = []

    async def one() -> None:
        second = int(time.monotonic() - started)
        sent = time.monotonic()
        try:
            await asyncio.wait_for(llmq.converse("gpt20b", REQUEST), 150)
            ok[second] += 1
            latencies.append(time.monotonic() - sent)
        except CallFailed as exc:
            failed[second] += 1
            errors[exc.kind] += 1
        except Exception as exc:  # the drill reports every kind of failure
            failed[second] += 1
            errors[type(exc).__name__] += 1

    async def traffic() -> None:
        calls = []
        while time.monotonic() - started < RUN_SECS:
            calls.append(asyncio.create_task(one()))
            await asyncio.sleep(0.1)
        await asyncio.gather(*calls)

    async def event() -> None:
        await asyncio.sleep(EVENT_AT_SECS)
        at = f"[{time.monotonic() - started:.1f}s]"
        if scenario == "restart":
            proxy.refuse = True
            proxy.drop_all()
            print(f"  {at} node down: every connection dropped")
            await asyncio.sleep(OUTAGE_SECS)
            proxy.refuse = False
            print(f"  [{time.monotonic() - started:.1f}s] node back")
            return
        # In a thread: a blocking call here stalled the drill's own callers mid-traffic.
        await asyncio.to_thread(docker, "exec", NODES["a"][0], "valkey-cli", "FAILOVER")
        print(f"  {at} FAILOVER: a is now a replica")
        if scenario == "slowdns":
            await asyncio.sleep(OUTAGE_SECS)
        proxy.target = "b"
        print(f"  [{time.monotonic() - started:.1f}s] the name now points at b")

    try:
        await asyncio.gather(traffic(), event())
        health = await llmq.health()
    finally:
        await llmq.stop(drain_secs=1)
        server.close()
    total_ok, total_failed = sum(ok.values()), sum(failed.values())
    latencies.sort()
    timing = (
        f"p50 {latencies[len(latencies) // 2]:.2f}s, max {latencies[-1]:.1f}s"
        if latencies
        else "no call succeeded"
    )
    print("  per second ok/failed:", " ".join(f"{s}:{ok[s]}/{failed[s]}" for s in sorted(ok)))
    print(
        f"  {total_ok} ok, {total_failed} failed {dict(errors) or ''}; {timing}; "
        f"stores after: {health['stores']}"
    )
    allowed = 0 if scenario != "slowdns" else (total_ok + total_failed) // 20
    passed = total_failed <= allowed and health["stores"] == {"state": "ok", "queue": "ok"}
    print(f"{'PASS' if passed else 'FAIL'}  {scenario}")
    return passed


def main() -> int:
    """Run the chosen scenarios on fresh containers; exit 1 if any fails."""
    chosen = sys.argv[1:] or ["failover", "slowdns", "restart"]
    os.environ.update({"AWS_ACCESS_KEY_ID": "aws-main", "AWS_SECRET_ACCESS_KEY": "fake"})
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    nodes_up()
    try:
        results = []
        for scenario in chosen:
            print(f"[{scenario}]")
            results.append(asyncio.run(drill(scenario)))
    finally:
        nodes_down()
    print("ALL PASS" if all(results) else "FAILED")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
