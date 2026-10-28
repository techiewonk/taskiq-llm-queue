"""Fallback when the provider throttles: same model on another target, then another model.

The worker tries a fallback chain, retrying each step with backoff before moving on:

1. `gpt120b@primary` (region ap-south-1)
2. `gpt120b@apac` (the cross-region inference profile), the next target of the same model
3. `gpt20b`, the next *model*, only when the call's policy is `fallback="chain"`

This sample uses the fakellm admin API to make targets throttle (`force_error`), then shows
`meta.served_model`, `meta.served_target`, `meta.fallback_used` and `meta.attempts`. The
fakellm scenario is saved first and restored at the end, even on error.

Run: .venv/Scripts/python samples/05_fallback_on_throttle.py
"""

from __future__ import annotations

import asyncio

from _local import (
    GPT120B_APAC,
    GPT120B_PRIMARY,
    ensure_queues,
    local_config,
    patch_model,
    restore_scenario,
    save_scenario,
)

from taskiq_llm_queue import LLMQueue, ProviderError, request_context
from taskiq_llm_queue.envelope import FallbackPolicy

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
THROTTLE = {"force_error": "ThrottlingException"}


async def show(llmq: LLMQueue, label: str, policy: FallbackPolicy) -> None:
    """Make one rt call with `policy` and print how it was served (or the error)."""
    try:
        with request_context(lane="rt"):
            result = await llmq.converse("gpt120b", HELLO, fallback=policy)
    except ProviderError as exc:
        print(f"{label:34s} -> {type(exc).__name__}(retryable={exc.retryable}): {exc}")
        return
    m = result.meta
    print(
        f"{label:34s} -> served {m.served_model}@{m.served_target} "
        f"fallback_used={m.fallback_used} attempts={m.attempts}"
    )


async def main() -> None:
    """Healthy, then primary throttled, then both gpt120b targets throttled."""
    config = local_config()
    await ensure_queues(config)
    scenario = await save_scenario()
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        await show(llmq, "healthy", "same_model")

        await patch_model(GPT120B_PRIMARY, THROTTLE)  # the region now always throttles
        await show(llmq, "primary throttled, same_model", "same_model")

        await patch_model(GPT120B_APAC, THROTTLE)  # the APAC profile too
        await show(llmq, "both targets throttled, chain", "chain")
        await show(llmq, "both targets throttled, same_model", "same_model")
    finally:
        await llmq.stop()
        await restore_scenario(scenario)  # undo the patches for the other samples


if __name__ == "__main__":
    asyncio.run(main())
