"""Retry curve, error details on failed calls, and how long a live (rt) call waits for a slot.

1. Prints the retry waits the curve gives (base x multiplier^n, window x spread, capped).
2. Makes gpt20b always throttle and shows the failed call's `exc.meta`: attempts, how long it
   waited in the queue, and the last target tried, plus how long the retries took.
3. With a bucket of 1 and ~4 s replies, sends two rt calls at once. With
   `rt_capacity_wait_secs` 1 the second gives up with `no_capacity`; with 10 it waits for the
   slot and succeeds. Both values are set live, per model.

Live overrides and the fakellm scenario are reset at the end, even on error.

Run: .venv/Scripts/python samples/15_retries_errors_and_rt_wait.py
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time

from _local import (
    GPT20B_PRIMARY,
    ensure_queues,
    local_config,
    patch_model,
    restore_scenario,
    save_scenario,
)

from taskiq_llm_queue import LLMQueue, ProviderError, request_context
from taskiq_llm_queue.config import create_redis
from taskiq_llm_queue.core import backoff_secs
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.model_settings import Backoff, model_settings_store

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


def show_curve() -> None:
    """Print the retry waits of the default curve and of a gentler per-model one."""
    rng = random.Random(0)
    for label, curve in (
        ("default", Backoff()),
        ("x3, cap 10", Backoff(multiplier=3, max_secs=10)),
    ):
        windows = []
        for n in range(4):
            draws = [backoff_secs(n, curve, rng, None) for _ in range(500)]
            windows.append(f"{min(draws):.1f}-{max(draws):.1f}s")
        print(f"    {label:11s} retry 1..4: {', '.join(windows)}")


async def one_rt_call(llmq: LLMQueue, tag: str) -> str:
    """One rt converse on gpt20b; returns a one-line outcome."""
    t0 = time.monotonic()
    try:
        with request_context(lane="rt"):
            result = await llmq.converse("gpt20b", HELLO, fallback="same_model")
    except ProviderError as exc:
        meta = exc.meta
        waited = f"queue wait {meta.queue_wait_ms} ms" if meta else ""
        return f"{tag}: failed after {time.monotonic() - t0:.1f}s ({exc}) {waited}"
    return f"{tag}: ok after {time.monotonic() - t0:.1f}s, attempts={result.meta.attempts}"


async def main() -> None:
    """Retry curve, a throttled call's meta, then rt waiting 1 s vs 10 s for a slot."""
    config = local_config(cfg_reload_secs=1.0)  # sample only: fast live reload
    await ensure_queues(config)
    scenario = await save_scenario()
    redis = create_redis(config, client_name="sample-15")
    store = model_settings_store(redis, KeySpace(config.key_prefix))
    await store.reset(actor="sample-15")
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        print("1. Retry waits (production base 1 s; samples use LLMQ_RETRY_BASE_SECS=0.2):")
        show_curve()

        await patch_model(GPT20B_PRIMARY, {"force_error": "ThrottlingException"})
        t0 = time.monotonic()
        try:
            with request_context(lane="rt"):
                await llmq.converse("gpt20b", HELLO, fallback="same_model")
        except ProviderError as exc:
            m = exc.meta
            assert m is not None
            print(
                f"2. Always throttled: {exc} after {time.monotonic() - t0:.1f}s; meta: "
                f"attempts={m.attempts} last={m.served_model}@{m.served_target} "
                f"queue_wait={m.queue_wait_ms} ms retryable={exc.retryable}"
            )
        await restore_scenario(scenario)

        await patch_model(GPT20B_PRIMARY, {"latency_ms": {"p50": 4000, "p95": 4200}})
        for wait in (1, 10):
            await store.set(
                {"models": {"gpt20b": {"max_concurrency": 1, "rt_capacity_wait_secs": wait}}},
                actor="sample-15",
            )  # = llm-queue settings set --model gpt20b max_concurrency=1 rt_capacity_wait_secs=N
            await asyncio.sleep(config.cfg_reload_secs + 0.5)
            outcomes = await asyncio.gather(
                one_rt_call(llmq, "call A"), one_rt_call(llmq, "call B")
            )
            print(f"3. bucket 1, rt_capacity_wait_secs={wait}:")
            for line in sorted(outcomes):
                print(f"    {line}")
    finally:
        await llmq.stop()
        with contextlib.suppress(Exception):
            await store.reset(actor="sample-15")
        await redis.aclose()
        await restore_scenario(scenario)


if __name__ == "__main__":
    asyncio.run(main())
