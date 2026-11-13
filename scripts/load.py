"""Load drill on fakellm: many concurrent calls over three workers, and a budget under pressure.

Gate `throughput`: 2,000 calls, 200 at a time, three worker processes' engines sharing one
store; every call must succeed, each worker must serve some, and nothing may stay held.
Gate `rate_limit`: 300 calls at once against a 600 RPM model (smooth window, 10% burst);
in the first 10 s at most burst + 10 s of rate may be served (60 + 100), the rest are refused
as `rate_limited` (or, for at most 1 %, `deadline_exceeded`: a call granted just before its
`max_wait_secs` whose attempt the shared fakellm's slow tail cut, p95 2.5 s), no call outlives
its deadline, and nothing stays held.

    python scripts/load.py                 # both gates
    python scripts/load.py rate_limit      # one
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from taskiq_llm_queue import CallFailed, LLMQueue  # noqa: E402  # needs the sys.path entry

STORE_URL = "redis://:localdev@127.0.0.1:6379/0"
REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "hi"}]}],
    "inferenceConfig": {"maxTokens": 20},
}


def environment(prefix: str, rpm: int) -> dict[str, str]:
    """`LLMQ_*` of one drill: the samples' registry with `gpt20b` at `rpm`."""
    doc = json.loads((ROOT / "samples" / "registry.json").read_text(encoding="utf-8"))
    for model in doc["models"].values():
        model["rpm"], model["tpm"] = rpm, 100_000_000
        model["worker_slots"] = 100
    return {
        "LLMQ_STORE_URL": STORE_URL,
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
        "LLMQ_REGISTRY": json.dumps(doc),
        "LLMQ_METRICS_SINK": "none",
        "LLMQ_BEDROCK_ENDPOINT_URL": "http://127.0.0.1:8080",
    }


async def workers(env: dict[str, str], count: int) -> list[LLMQueue]:
    """`count` started queues on one store, as separate pods would be."""
    queues = [LLMQueue.from_env(env) for _ in range(count)]
    for queue in queues:
        await queue.start()
    return queues


async def still_held(queues: list[LLMQueue], secs: float = 3.0) -> int:
    """Held calls left once the last replies are acknowledged (a reply is published first)."""
    for _ in range(int(secs / 0.1)):
        held = sum(len(q.engine._held) for q in queues)
        if not held:
            return 0
        await asyncio.sleep(0.1)
    return sum(len(q.engine._held) for q in queues)


async def cleanup(prefix: str) -> None:
    """Delete the drill's keys and queues."""
    import redis.asyncio

    client = redis.asyncio.Redis.from_url(STORE_URL)
    queue_prefix = prefix.replace(":", "-") + "-"
    for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
        keys = [k async for k in client.scan_iter(match=pattern, count=500)]
        if keys:
            await client.delete(*keys)
    await client.aclose()


async def throughput(calls: int = 2000, concurrency: int = 200) -> bool:
    """Every call succeeds across three workers; no call stays held."""
    prefix = f"llmqload:{uuid.uuid4().hex[:6]}"
    queues = await workers(environment(prefix, rpm=1_000_000), 3)
    gate = asyncio.Semaphore(concurrency)
    served: collections.Counter[str] = collections.Counter()
    errors: collections.Counter[str] = collections.Counter()

    async def one(i: int) -> None:
        async with gate:
            try:
                reply = await queues[i % 3].converse("gpt20b", REQUEST, timeout_secs=60)
                served[reply.meta.worker] += 1
            except CallFailed as exc:
                errors[exc.kind] += 1

    started = time.monotonic()
    try:
        await asyncio.gather(*(one(i) for i in range(calls)))
        secs = time.monotonic() - started
        held = await still_held(queues)
    finally:
        for queue in queues:
            await queue.stop(drain_secs=1)
        await cleanup(prefix)
    ok = sum(served.values())
    print(f"  {ok}/{calls} ok in {secs:.1f}s ({ok / secs:.0f}/s); errors {dict(errors) or 0}")
    print(f"  served per worker: {sorted(served.values())}; still held: {held}")
    passed = ok == calls and len(served) == 3 and held == 0
    print(f"{'PASS' if passed else 'FAIL'}  throughput")
    return passed


async def rate_limit(calls: int = 300, rpm: int = 600) -> bool:
    """A burst over the budget: served within the bucket's bound, the rest `rate_limited`."""
    prefix = f"llmqload:{uuid.uuid4().hex[:6]}"
    queues = await workers(environment(prefix, rpm=rpm), 3)
    served_at: list[float] = []
    kinds: collections.Counter[str] = collections.Counter()
    late: list[float] = []
    started = time.monotonic()

    async def one(i: int) -> None:
        sent = time.monotonic()
        try:
            await queues[i % 3].converse("gpt20b", REQUEST, timeout_secs=10, max_wait_secs=5)
            served_at.append(time.monotonic() - started)
        except CallFailed as exc:
            kinds[exc.kind] += 1
        if time.monotonic() - sent > 10 + 6:  # deadline + caller margin + slack
            late.append(time.monotonic() - sent)

    try:
        await asyncio.gather(*(one(i) for i in range(calls)))
        held = await still_held(queues)
    finally:
        for queue in queues:
            await queue.stop(drain_secs=1)
        await cleanup(prefix)
    early = sum(1 for t in served_at if t <= 10)
    bound = int(rpm * 0.1) + int(rpm / 60 * 10) + 5  # burst + 10 s of rate + clock slack
    print(
        f"  served {len(served_at)} ({early} in the first 10 s, bound {bound}); "
        f"refused {dict(kinds) or 0}; past deadline {len(late)}; still held {held}"
    )
    passed = (
        early <= bound
        and set(kinds) <= {"rate_limited", "deadline_exceeded"}
        and kinds.get("deadline_exceeded", 0) <= calls // 100
        and not late
        and held == 0
        and len(served_at) + sum(kinds.values()) == calls
    )
    print(f"{'PASS' if passed else 'FAIL'}  rate_limit")
    return passed


def main() -> int:
    """Run the chosen gates; exit 1 if any fails."""
    os.environ.update({"AWS_ACCESS_KEY_ID": "aws-main", "AWS_SECRET_ACCESS_KEY": "fake"})
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    gates: dict[str, Any] = {"throughput": throughput, "rate_limit": rate_limit}
    chosen = sys.argv[1:] or list(gates)
    results = []
    for name in chosen:
        print(f"[{name}]")
        results.append(asyncio.run(gates[name]()))
    print("ALL PASS" if all(results) else "FAILED")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
