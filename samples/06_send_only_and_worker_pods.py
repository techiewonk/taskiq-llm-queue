"""Send-only and work-only processes: who sends a call and who runs it can differ.

`start(mode=...)` picks the role of a process:

- `"send"`: makes calls and waits for results; never takes work from SQS (CLIs, scripts,
  a web tier that should not execute LLM calls).
- `"work"`: only runs queued calls (a dedicated worker deployment).
- `"both"`: the default for app pods, which send and execute.

With no argument this script runs one sender and two workers in one process (three
`LLMQueue` instances) and prints which worker served each call. You can also run the roles
as separate processes, e.g. in two terminals:

    .venv/Scripts/python samples/06_send_only_and_worker_pods.py work   # Ctrl+C to stop
    .venv/Scripts/python samples/06_send_only_and_worker_pods.py send

Run: .venv/Scripts/python samples/06_send_only_and_worker_pods.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections import Counter

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue, QueueConfig

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


async def send(config: QueueConfig, calls: int = 12) -> None:
    """Send `calls` converse calls from a send-only instance and count the serving workers."""
    sender = LLMQueue(config, pod_id="sender")
    await sender.start(mode="send")
    try:
        results = await asyncio.gather(
            *(sender.converse("gpt20b", HELLO, timeout=60) for _ in range(calls))
        )
    finally:
        await sender.stop()
    served = Counter(r.meta.worker for r in results)
    print(f"sender 'sender' got {len(results)} results; served by: {dict(served)}")


async def work(config: QueueConfig, name: str) -> None:
    """Run a work-only instance until cancelled (Ctrl+C)."""
    worker = LLMQueue(config, pod_id=name)
    await worker.start(mode="work")
    print(f"worker {name!r} receiving; Ctrl+C to stop")
    try:
        await asyncio.Event().wait()
    finally:
        await worker.stop()  # releases unstarted messages for other workers


async def demo(config: QueueConfig) -> None:
    """One sender and two workers in one process."""
    workers = [asyncio.create_task(work(config, f"worker-{n}")) for n in (1, 2)]
    await asyncio.sleep(1)  # let the workers start polling
    try:
        await send(config)
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def main(role: str) -> None:
    """Dispatch on the role given on the command line."""
    config = local_config()
    await ensure_queues(config)
    if role == "send":
        await send(config)
    elif role == "work":
        await work(config, "worker")
    else:
        await demo(config)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "demo"))
