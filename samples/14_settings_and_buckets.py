"""Settings per provider and per model: bucket size, live changes and a provider-wide bucket.

Each setting resolves through six layers, highest first: live model, live provider (Valkey,
`llm-queue settings`), env model, env provider (`LLMQ_SETTINGS`), env global (`LLMQ_*`) and
the built-in default. This sample:

1. sets env-layer values in code (`settings_default`, the same JSON as `LLMQ_SETTINGS`) and
   prints a few settings with their source;
2. runs 12 parallel gpt20b calls with `max_concurrency` 3 and shows the provider never sees
   more than 3 at once;
3. lowers the bucket to 1 *live* (as `llm-queue settings set --model gpt20b max_concurrency=1`
   would) and shows the next run peaks at 1, without a restart;
4. gives the `bedrock` provider one bucket of 2 shared by gpt20b and gpt120b, and shows the
   two models together never exceed 2.

fakellm replies are slowed to ~0.6 s so the overlap is visible. The live overrides and the
fakellm scenario are reset at the end, even on error.

Run: .venv/Scripts/python samples/14_settings_and_buckets.py
"""

from __future__ import annotations

import asyncio
import contextlib

from _local import (
    GPT20B_PRIMARY,
    GPT120B_PRIMARY,
    ensure_queues,
    local_config,
    patch_model,
    restore_scenario,
    save_scenario,
)

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.config import ModelSettings, create_redis
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.model_settings import model_settings_store
from taskiq_llm_queue.reliability import AdaptiveLimiter

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
SLOW = {"latency_ms": {"p50": 600, "p95": 700}}
SHOWN = ("max_concurrency", "attempt_timeout_secs", "timeout_secs.rt", "retry_multiplier")


def show_settings(llmq: LLMQueue, alias: str) -> None:
    """Print a few effective settings of `alias` with the layer they come from."""
    described = llmq.settings.describe(alias)
    for name in SHOWN:
        row = described[name]
        print(f"    {alias}.{name:22s} = {row['value']!s:6s} ({row['source']})")


async def peak_inflight(
    limiter: AdaptiveLimiter, keys: list[tuple[str, str]], stop: asyncio.Event
) -> int:
    """Highest sum of live leases over `keys` ((model, target) pairs), sampled every 20 ms."""
    peak = 0
    while not stop.is_set():
        states = [await limiter.state(model, target) for model, target in keys]
        peak = max(peak, sum(s.inflight for s in states))
        await asyncio.sleep(0.02)
    return peak


async def burst(
    llmq: LLMQueue, limiter: AdaptiveLimiter, calls: dict[str, int], watch: list[tuple[str, str]]
) -> int:
    """Run `calls` (alias -> count) in parallel on the bg lane; return the peak in flight."""
    stop = asyncio.Event()
    sampler = asyncio.create_task(peak_inflight(limiter, watch, stop))
    with request_context(lane="bg"):
        await asyncio.gather(
            *(llmq.converse(alias, HELLO) for alias, n in calls.items() for _ in range(n))
        )
    stop.set()
    return await sampler


async def main() -> None:
    """Env-layer settings, a bucket of 3, a live change to 1, then a provider bucket of 2."""
    config = local_config(
        cfg_reload_secs=1.0,  # sample only: pick up live changes fast (default 30 s)
        limiter={"bg_share": 1.0},  # let bg use the whole bucket (default 0.8 keeps 20 % for rt)
        settings_default={  # the same JSON as LLMQ_SETTINGS in .env / Secrets Manager
            "providers": {"bedrock": {"attempt_timeout_secs": 120}},
            "models": {
                "gpt20b": {"max_concurrency": 3, "timeout_secs": {"rt": 60}},
                "gpt120b": {"max_concurrency": 3},
            },
        },
    )
    await ensure_queues(config)
    scenario = await save_scenario()
    redis = create_redis(config, client_name="sample-14")
    keys = KeySpace(config.key_prefix)
    store = model_settings_store(redis, keys)
    limiter = AdaptiveLimiter(redis, keys)
    for target in (GPT20B_PRIMARY, GPT120B_PRIMARY):
        await patch_model(target, SLOW)
    await store.reset(actor="sample-14")  # start from env values only
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        print("1. Effective settings (env layers from settings_default):")
        show_settings(llmq, "gpt20b")

        peak = await burst(llmq, limiter, {"gpt20b": 12}, [("gpt20b", "primary")])
        print(f"2. 12 parallel gpt20b calls, bucket 3   -> peak in flight {peak}")

        await store.set(
            ModelSettings().with_values("models", "gpt20b", {"max_concurrency": 1}),
            actor="sample-14",
        )  # what `llm-queue settings set --model gpt20b max_concurrency=1` does
        await asyncio.sleep(config.cfg_reload_secs + 0.5)  # every pod reloads
        print("3. After a live change (no restart):")
        show_settings(llmq, "gpt20b")
        peak = await burst(llmq, limiter, {"gpt20b": 4}, [("gpt20b", "primary")])
        print(f"   4 parallel gpt20b calls, live bucket 1 -> peak in flight {peak}")

        await store.set(
            {"providers": {"bedrock": {"shared_max_concurrency": 2}}}, actor="sample-14"
        )  # one bucket for every bedrock model; drops the gpt20b override above
        await asyncio.sleep(config.cfg_reload_secs + 0.5)
        peak = await burst(llmq, limiter, {"gpt20b": 6, "gpt120b": 6}, [("@bedrock", "shared")])
        print(f"4. 6 gpt20b + 6 gpt120b, provider bucket 2 -> combined peak in flight {peak}")
    finally:
        await llmq.stop()
        with contextlib.suppress(Exception):
            await store.reset(actor="sample-14")
        await redis.aclose()
        await restore_scenario(scenario)


if __name__ == "__main__":
    asyncio.run(main())
