"""Lanes: live (`rt`) calls overtake a backlog of background (`bg`) calls.

Each model has an `-rt` queue that workers poll first and a base queue for `bg` work, and `bg`
may use at most 80 % of the learned limit. The lane comes from (highest wins):

1. the call's `lane=` argument,
2. the surrounding `request_context(lane=...)` (set once per request, e.g. in middleware),
3. the default, `bg`.

This sample shrinks the worker to 4 slots for `gpt20b`, floods 24 `bg` calls, then sends 4
`rt` calls and prints the average queue wait per lane: `rt` should wait far less.

Run: .venv/Scripts/python samples/04_lanes_and_request_context.py
"""

from __future__ import annotations

import asyncio
from statistics import mean

from _local import ensure_queues, local_config

from taskiq_llm_queue import ConverseResult, LLMQueue, current_context, request_context

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


async def background_call(llmq: LLMQueue) -> ConverseResult:
    """A call with no lane set anywhere: lands in `bg`."""
    return await llmq.converse("gpt20b", HELLO)


async def live_call(llmq: LLMQueue, n: int) -> ConverseResult:
    """A user-facing call: the lane comes from the request context, like an API handler."""
    with request_context(lane="rt", session_id=f"session-{n}", user_id="u-1"):
        return await llmq.converse("gpt20b", HELLO)


async def main() -> None:
    """Flood `bg`, then send `rt`, and compare queue waits."""
    # 4 worker slots for gpt20b so a backlog builds up (default is much larger).
    config = local_config(worker_slots={"gpt20b": 4})
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        print("default context :", current_context())
        with request_context(lane="rt", session_id="s-1"), request_context(lane="bg"):
            # Nesting changes only what you pass: the session is inherited.
            print("nested context  :", current_context())

        bg = [asyncio.create_task(background_call(llmq)) for _ in range(24)]
        await asyncio.sleep(0.5)  # let the bg backlog reach SQS first
        rt = [asyncio.create_task(live_call(llmq, n)) for n in range(4)]
        # An explicit lane= argument beats the context.
        with request_context(lane="rt"):
            explicit = await llmq.converse("gpt20b", HELLO, lane="bg")

        rt_results = await asyncio.gather(*rt)
        bg_results = await asyncio.gather(*bg)
        rt_wait = mean(r.meta.queue_wait_ms for r in rt_results)
        bg_wait = mean(r.meta.queue_wait_ms for r in bg_results)
        print(f"rt : {len(rt_results):2d} calls, mean queue_wait_ms = {rt_wait:.0f}")
        print(f"bg : {len(bg_results):2d} calls, mean queue_wait_ms = {bg_wait:.0f}")
        print(f"explicit lane=bg inside an rt context waited {explicit.meta.queue_wait_ms} ms")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
