"""V7 numbers for the Valkey queue backend (docs/VALKEY_QUEUE_PLAN.md §6.1, §7).

Against the persistent scenario Valkey (`valkey-scn-persist`, :16380):

1. memory per queued message for a 10K backlog of a typical envelope (~3 KB body);
2. send and receive+delete throughput with `appendfsync always` vs `everysec`;
3. messages lost when Valkey is killed (`docker compose stop -t 0`) right after a burst of
   sends, for each fsync mode.

Run: .venv/Scripts/python tools/valkey_queue_bench.py   (needs the `scn` compose profile up)
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
import uuid
from pathlib import Path

import redis.asyncio as redis_asyncio

from taskiq_llm_queue.broker.valkey_queue import ValkeyQueueClient, queue_url

HOST, PORT, PASSWORD = "localhost", 16380, "localdev"
SERVICE = "valkey-scn-persist"
COMPOSE = ["docker", "compose", "-f", str(Path(__file__).parents[1] / "docker" / "compose.yaml")]
BODY = json.dumps({"payload": "x" * 3000})  # a typical LLM envelope is 2-20 KB


def _compose(*args: str) -> None:
    subprocess.run([*COMPOSE, "--profile", "scn", *args], check=True, capture_output=True)


def _redis() -> redis_asyncio.Redis:
    return redis_asyncio.Redis(host=HOST, port=PORT, password=PASSWORD)


async def _client(prefix: str) -> ValkeyQueueClient:
    client = ValkeyQueueClient(_redis(), prefix=prefix, owns_redis=True)
    await client.create_queue(QueueName="bench", Attributes={"VisibilityTimeout": "60"})
    return client


async def _cleanup(prefix: str) -> None:
    r = _redis()
    keys = [k async for k in r.scan_iter(match=f"*{prefix}*", count=1000)]
    if keys:
        await r.delete(*keys)
    await r.aclose()


async def _send(client: ValkeyQueueClient, n: int, concurrency: int = 32) -> float:
    url = queue_url("bench")
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int) -> None:
        async with sem:
            await client.send_message(QueueUrl=url, MessageBody=BODY)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    return n / (time.perf_counter() - t0)


async def _drain(client: ValkeyQueueClient, n: int, concurrency: int = 8) -> float:
    url = queue_url("bench")
    done = 0

    async def worker() -> None:
        nonlocal done
        while done < n:
            msgs = (await client.receive_message(QueueUrl=url, MaxNumberOfMessages=10)).get(
                "Messages", []
            )
            if not msgs:
                return
            entries = [
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)
            ]
            await client.delete_message_batch(QueueUrl=url, Entries=entries)
            done += len(msgs)

    t0 = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return done / (time.perf_counter() - t0)


async def memory_per_message(n: int = 10_000) -> dict[str, float]:
    """Bytes of Valkey memory per queued message for an `n`-message backlog."""
    prefix = f"bench:{uuid.uuid4().hex[:8]}"
    client = await _client(prefix)
    r = _redis()
    before = int((await r.info("memory"))["used_memory"])
    await _send(client, n)
    after = int((await r.info("memory"))["used_memory"])
    await r.aclose()
    await client.close()
    await _cleanup(prefix)
    per = (after - before) / n
    return {
        "messages": n,
        "body_bytes": len(BODY),
        "bytes_per_message": round(per),
        "overhead_bytes": round(per - len(BODY)),
    }


async def throughput(mode: str, n: int = 5_000) -> dict[str, object]:
    """Sends and receive+delete per second under one `appendfsync` mode."""
    r = _redis()
    await r.config_set("appendfsync", mode)
    await r.aclose()
    prefix = f"bench:{uuid.uuid4().hex[:8]}"
    client = await _client(prefix)
    sends = await _send(client, n)
    drains = await _drain(client, n)
    await client.close()
    await _cleanup(prefix)
    return {"appendfsync": mode, "sends_per_s": round(sends), "receive_delete_per_s": round(drains)}


async def loss_on_kill(mode: str, n: int = 2_000) -> dict[str, object]:
    """Messages left after a hard kill right after `n` sends, under one `appendfsync` mode."""
    r = _redis()
    await r.config_set("appendfsync", mode)
    await r.aclose()
    prefix = f"bench:{uuid.uuid4().hex[:8]}"
    client = await _client(prefix)
    await _send(client, n)
    await client.close()
    await asyncio.to_thread(_compose, "stop", "-t", "0", SERVICE)
    await asyncio.to_thread(_compose, "start", SERVICE)
    for _ in range(60):  # wait for the AOF to load
        try:
            r = _redis()
            await r.ping()
            break
        except Exception:
            await asyncio.sleep(0.5)
    client = ValkeyQueueClient(_redis(), prefix=prefix, owns_redis=True)
    depth = await client.depth("bench")
    await client.close()
    await _cleanup(prefix)
    kept = int(depth["visible"] + depth["in_flight"])
    return {"appendfsync": mode, "sent": n, "kept_after_kill": kept, "lost": n - kept}


async def main() -> None:
    """Run the three measurements and print them as JSON."""
    results: dict[str, object] = {"memory": await memory_per_message()}
    results["throughput"] = [await throughput("always"), await throughput("everysec")]
    results["kill"] = [await loss_on_kill("always"), await loss_on_kill("everysec")]
    r = _redis()
    await r.config_set("appendfsync", "always")  # back to the compose default
    await r.aclose()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
