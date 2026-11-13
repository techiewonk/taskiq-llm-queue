"""The fleet `scripts/gates.py` drives: fakellm, worker / caller processes, store measurements.

Workers are separate Python processes (`python scripts/gates_fleet.py worker`), like pods: each
builds `LLMQueue.from_env()` with `LLMQ_MODE=worker`, counts its metrics into the store, and
stops cleanly on a `stop` line (or dies at once on `kill()`). Callers can be processes too
(`caller` mode, for the kill-switch gate) or queues in the driver itself.
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

PY = sys.executable
STORE = "redis://:localdev@127.0.0.1:6379/0"
FAKE_PORT = int(os.environ.get("LLMQ_FAKE_PORT", "8099"))  # one per parallel run
CHAT_ID = "openai.gpt-oss-20b-1:0"
EMBED_ID = "cohere.embed-english-v3"
TITAN_ID = "amazon.titan-embed-text-v2:0"
REGIONS = (
    "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1",
    "eu-west-2", "eu-central-1", "ap-south-1", "ap-southeast-1", "ap-northeast-1",
)  # fmt: skip


def registry(
    models: dict[str, dict[str, Any]],
    *,
    accounts: int = 3,
    regions: int = 3,
    defaults: dict[str, Any] | None = None,
    routing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A registry: every model on `accounts` x `regions` deployments (all confirmed)."""
    names = [f"aws-{i}" for i in range(accounts)]
    used = list(REGIONS[:regions])
    doc: dict[str, Any] = {
        "registry_version": 1,
        "defaults": {"timeout_secs": 120, "max_wait_secs": 120, **(defaults or {})},
        "residency": {"allowed_regions": used},
        "accounts": {n: {"kind": "aws"} for n in names},
        "models": {},
    }
    if routing:
        doc["routing"] = routing
    for alias, spec in models.items():
        entry = {"type": "chat", "provider": "bedrock", "model_id": CHAT_ID, **spec}
        entry["deployments"] = {n: {"confirmed": used} for n in names}
        doc["models"][alias] = entry
    return doc


def environment(prefix: str, doc: dict[str, Any], **extra: str) -> dict[str, str]:
    """A process environment for one gate run (fake AWS keys, the gate's fakellm)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LLMQ_", "AWS_"))}
    env.update(
        {
            "LLMQ_STORE_URL": STORE,
            "LLMQ_KEY_PREFIX": prefix,
            "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
            "LLMQ_REGISTRY": json.dumps(doc),
            "LLMQ_METRICS_SINK": "none",
            "LLMQ_BEDROCK_ENDPOINT_URL": f"http://127.0.0.1:{FAKE_PORT}",
            "AWS_ACCESS_KEY_ID": "gates",
            "AWS_SECRET_ACCESS_KEY": "fake",
            "AWS_DEFAULT_REGION": "us-east-1",
            "PYTHONIOENCODING": "utf-8",
            **extra,
        }
    )
    return env


LOG_FORMAT = "%(asctime)s %(process)d %(levelname)s %(name)s: %(message)s"


def log_dir() -> Path:
    """Where drill processes write their logs, made on first use.

    `LLMQ_DRILL_LOG_DIR` (check.py sets it per step) or `reports/drill-<time>` (git-ignored).
    """
    raw = os.environ.get("LLMQ_DRILL_LOG_DIR")
    if not raw:
        raw = str(ROOT / "reports" / f"drill-{time.strftime('%Y%m%d-%H%M%S')}")
        os.environ["LLMQ_DRILL_LOG_DIR"] = raw  # one folder for the whole run
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def log_file(kind: str) -> Any:
    """A new, numbered log file for one drill process (`worker-3.log`, `fakellm-1.log`)."""
    folder = log_dir()
    n = 1 + sum(1 for _ in folder.glob(f"{kind}-*.log"))
    return (folder / f"{kind}-{n}.log").open("ab")


def setup_logging() -> None:
    """Library warnings with timestamps on stderr (the drill's own log file)."""
    import logging

    level = os.environ.get("DRILL_LOG_LEVEL", "WARNING").upper()  # INFO: every call
    logging.basicConfig(level=level, format=LOG_FORMAT, force=True)


def new_prefix() -> str:
    """A fresh key prefix (deleted by `cleanup`)."""
    return f"llmqgate:{uuid.uuid4().hex[:6]}"


class FakeLLM:
    """A fakellm process on FAKE_PORT with a gate's own scenario (latency, limits)."""

    def __init__(self, scenario: dict[str, Any]) -> None:
        """Write the scenario; `start()` runs it."""
        self.file = Path(tempfile.mkdtemp()) / "scenario.json"
        self.file.write_text(json.dumps({"seed": 7, "strict_models": False, **scenario}))
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        """Start and wait until it answers."""
        env = dict(os.environ, PYTHONPATH=str(ROOT / "tools" / "fakellm"))
        cmd = [PY, "-m", "fakellm", "--host", "127.0.0.1", "--port", str(FAKE_PORT)]
        log = log_file("fakellm")
        self.proc = subprocess.Popen(
            [*cmd, "--scenario", str(self.file)], env=env, stdout=log, stderr=log
        )
        log.close()  # the process keeps its own handle
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", FAKE_PORT), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("fakellm did not start")

    def stop(self) -> None:
        """Stop the process."""
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait(10)


class Worker:
    """One worker process (a pod)."""

    def __init__(self, env: dict[str, str]) -> None:
        """Start it (`LLMQ_MODE=worker`); `ready()` waits for its start."""
        log = log_file("worker")  # its warnings and errors, with timestamps
        self.proc = subprocess.Popen(
            [PY, str(Path(__file__)), "worker"],
            env={**env, "LLMQ_MODE": "worker"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
        )
        log.close()

    async def ready(self) -> None:
        """Until the worker printed `ready`."""
        assert self.proc.stdout is not None
        line = await asyncio.to_thread(self.proc.stdout.readline)
        if not line.startswith(b"ready"):
            raise RuntimeError(f"worker did not start: {line!r}")

    async def stop(self) -> None:
        """Graceful: pause, drain, stop."""
        if self.proc.poll() is None and self.proc.stdin is not None:
            self.proc.stdin.write(b"stop\n")
            self.proc.stdin.flush()
            await asyncio.to_thread(self.proc.wait, 60)

    def kill(self) -> None:
        """At once, no cleanup (a crashed pod)."""
        self.proc.kill()
        self.proc.wait(10)


async def workers(env: dict[str, str], count: int) -> list[Worker]:
    """`count` started worker processes."""
    fleet = [Worker(env) for _ in range(count)]
    await asyncio.gather(*(w.ready() for w in fleet))
    return fleet


async def stop_all(fleet: list[Worker]) -> None:
    """Stop every worker still alive."""
    await asyncio.gather(*(w.stop() for w in fleet if w.proc.poll() is None))


class Store:
    """Measurements on the local Valkey: commands, script latency, memory, gate metrics."""

    def __init__(self) -> None:
        """A client on the stack's Valkey."""
        import redis

        self.client = redis.Redis.from_url(STORE)

    def commands(self) -> int:
        """Commands processed so far."""
        return int(self.client.info("stats")["total_commands_processed"])

    def script_calls(self) -> int:
        """EVALSHA calls so far (every admission, receive, finish ... is one)."""
        stats = self.client.info("commandstats")
        return int(stats.get("cmdstat_evalsha", {}).get("calls", 0))

    def reset_latency(self) -> None:
        """Clear the per-command latency histograms."""
        self.client.config_resetstat()

    def script_p99_ms(self) -> float:
        """p99 server time of EVALSHA since the last reset (INFO latencystats, exact)."""
        stats = self.client.info("latencystats").get("latency_percentiles_usec_evalsha", {})
        return float(stats.get("p99", 0.0)) / 1000.0

    def memory_mb(self) -> float:
        """Used memory."""
        return int(self.client.info("memory")["used_memory"]) / 1e6

    def metrics(self, prefix: str) -> dict[str, float]:
        """What the workers counted (`MetricsToStore`)."""
        raw = self.client.hgetall(f"{prefix}:gates:metrics")
        return {(k.decode() if isinstance(k, bytes) else k): float(v) for k, v in raw.items()}

    def cleanup(self, prefix: str) -> None:
        """Delete every key and queue of a run."""
        queue_prefix = prefix.replace(":", "-") + "-"
        for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
            keys = list(self.client.scan_iter(match=pattern, count=1000))
            for i in range(0, len(keys), 500):
                self.client.delete(*keys[i : i + 500])


class MetricsToStore:
    """A metrics sink that adds counts into one store hash every second (per worker process)."""

    def __init__(self, prefix: str) -> None:
        """Counts kept in memory until flushed."""
        self.key = f"{prefix}:gates:metrics"
        self.counts: collections.Counter[str] = collections.Counter()  # float values

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """A count, by name and reason / outcome."""
        tag = labels.get("reason") or labels.get("outcome") or ""
        key = f"{name}:{tag}" if tag else name
        self.counts[key] = self.counts[key] + int(value)

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Not needed by the gates."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Not needed by the gates."""

    async def flush(self, client: Any) -> None:
        """Add and clear what was counted."""
        counts, self.counts = self.counts, collections.Counter()
        if counts:
            async with client.pipeline(transaction=False) as pipe:
                for name, value in counts.items():
                    pipe.hincrbyfloat(self.key, name, value)
                await pipe.execute()


async def _worker_main() -> None:
    """A worker process: start, count metrics into the store, stop on `stop`."""
    import redis.asyncio

    from taskiq_llm_queue import LLMQueue

    setup_logging()
    sink = MetricsToStore(os.environ["LLMQ_KEY_PREFIX"])
    llmq = LLMQueue.from_env(metrics=sink)
    await llmq.start()
    client = redis.asyncio.Redis.from_url(STORE)

    async def flusher() -> None:
        while True:
            await asyncio.sleep(1)
            await sink.flush(client)

    task = asyncio.create_task(flusher())
    print("ready", flush=True)
    await asyncio.to_thread(sys.stdin.readline)  # "stop", or EOF when the driver exits
    llmq.pause_receiving()
    await llmq.stop(drain_secs=30)
    task.cancel()
    await sink.flush(client)
    await client.aclose()


async def _caller_main(calls: int, model: str) -> None:
    """A caller process: run `calls` calls at once, print the outcome counts as JSON."""
    from taskiq_llm_queue import CallFailed, LLMQueue

    llmq = LLMQueue.from_env()
    await llmq.start()
    print("ready", flush=True)
    await asyncio.to_thread(sys.stdin.readline)  # "go"
    outcomes: collections.Counter[str] = collections.Counter()
    request = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}

    async def one() -> None:
        try:
            await llmq.converse(model, request)
            outcomes["ok"] += 1
        except CallFailed as exc:
            outcomes[exc.kind] += 1

    await asyncio.gather(*(one() for _ in range(calls)))
    print(json.dumps(outcomes), flush=True)
    await llmq.stop(drain_secs=1)


if __name__ == "__main__":
    if sys.argv[1] == "worker":
        asyncio.run(_worker_main())
    elif sys.argv[1] == "caller":
        asyncio.run(_caller_main(int(sys.argv[2]), sys.argv[3]))
