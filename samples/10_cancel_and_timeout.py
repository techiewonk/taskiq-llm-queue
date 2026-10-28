"""Cancelling calls and bounding how long a caller waits.

- `cancel(request_id)` sets a shared cancel flag. A queued call that no worker has started
  yet is dropped unrun when a worker receives it; the waiting caller gets `LLMQueueError`.
  Give calls your own `request_id=` so another task (or pod) can cancel them.
- `timeout=` (seconds) bounds the caller's wait; past it the caller gets `LLMQueueTimeout`
  and the call is flagged cancelled so no worker spends tokens on it later.
- `deadline=` is the same as an absolute epoch time (e.g. an upstream request deadline).
- Cancelling the awaiting asyncio task also flags the call cancelled.

Part 1 sends from a send-only instance (nothing is working yet), cancels, then starts a
worker. Parts 2-4 use a 0.5 s budget against `gpt120b`, which takes ~2.5 s in fakellm.

Run: .venv/Scripts/python samples/10_cancel_and_timeout.py
"""

from __future__ import annotations

import asyncio
import time

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue, LLMQueueError, LLMQueueTimeout

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


async def cancel_before_start(sender: LLMQueue, worker: LLMQueue) -> None:
    """Part 1: cancel a queued call before any worker picks it up."""
    request_id = f"cancel-demo-{time.time_ns()}"
    call = asyncio.create_task(sender.converse("gpt20b", HELLO, request_id=request_id))
    await asyncio.sleep(0.5)  # the call is now waiting in SQS
    await sender.cancel(request_id)
    await worker.start(mode="work")  # the worker sees the flag and drops it unrun
    try:
        await asyncio.wait_for(call, 30)
    except LLMQueueError as exc:  # cancellation has no dedicated subclass
        print("1. cancel(request_id)   ->", type(exc).__name__, exc)


async def timeouts(llmq: LLMQueue) -> None:
    """Parts 2-4: per-call timeout, absolute deadline, and asyncio cancellation."""
    t0 = time.monotonic()
    try:
        await llmq.converse("gpt120b", HELLO, timeout=0.5)
    except LLMQueueTimeout as exc:
        secs = time.monotonic() - t0
        print(f"2. timeout=0.5          -> LLMQueueTimeout after {secs:.1f}s: {exc}")

    t0 = time.monotonic()
    try:
        await llmq.converse("gpt120b", HELLO, deadline=time.time() + 0.5)
    except LLMQueueTimeout:
        print(f"3. deadline=now+0.5     -> LLMQueueTimeout after {time.monotonic() - t0:.1f}s")

    task = asyncio.create_task(llmq.converse("gpt120b", HELLO))
    await asyncio.sleep(0.5)
    task.cancel()  # e.g. the HTTP client disconnected
    try:
        await task
    except asyncio.CancelledError:
        print("4. task.cancel()        -> CancelledError; the queued call is flagged cancelled")


async def main() -> None:
    """Run the four parts with one sender and one worker instance."""
    config = local_config()
    await ensure_queues(config)
    sender = LLMQueue(config, pod_id="sender")
    worker = LLMQueue(config, pod_id="worker")
    await sender.start(mode="send")
    try:
        await cancel_before_start(sender, worker)
        await timeouts(sender)
    finally:
        await sender.stop()
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
