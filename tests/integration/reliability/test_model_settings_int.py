"""Live per-model / per-provider settings on the real Valkey: shared provider cap, live caps.

Calls run through `core.execute_job` with the real `AdaptiveLimiter` (direct mode, no claim)
and a slow fake provider that records its peak concurrency.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any

import pytest
import redis.asyncio

from taskiq_llm_queue import Envelope, ErrorInfo, ErrorKind, LLMReply, QueueConfig, Target
from taskiq_llm_queue.client import LLMQueue
from taskiq_llm_queue.config import Lane, ModelSettings
from taskiq_llm_queue.core import CoreDeps, execute_job
from taskiq_llm_queue.failover import FailoverState
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.lifecycle import limiter_settings
from taskiq_llm_queue.model_settings import SettingsResolver, model_settings_store
from taskiq_llm_queue.reliability import AdaptiveLimiter, CircuitBreaker

pytestmark = pytest.mark.integration

MODELS: dict[str, Any] = {
    "a": {"provider": "openai", "model_id": "model-a", "max_concurrency": 10},
    "b": {"provider": "openai", "model_id": "model-b", "max_concurrency": 10},
}


class SlowProvider:
    """Sleeps per call and records how many calls ran at once across every alias."""

    name = "slow"

    def __init__(self, secs: float = 0.05) -> None:
        self.secs = secs
        self.running = 0
        self.peak = 0

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            await asyncio.sleep(self.secs)
        finally:
            self.running -= 1
        return {"ok": True}

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(ErrorKind.RETRYABLE, type(exc).__name__)


def envelope(i: int, model: str, lane: str = "bg") -> Envelope:
    now = time.time()
    return Envelope.model_validate(
        {
            "request_id": f"r-{i}",
            "op": "chat",
            "model": model,
            "lane": lane,
            "params": {"messages": []},
            "enqueued_at": now,
            "deadline_at": now + 60,
        }
    )


async def run_all(deps: CoreDeps, n: int) -> list[LLMReply]:
    jobs = [execute_job(envelope(i, "ab"[i % 2]), deps, claim=False, store=False) for i in range(n)]
    results = await asyncio.gather(*jobs)
    return [r.reply for r in results if r.reply is not None]


async def test_shared_provider_cap_bounds_all_models_of_the_provider(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Two models (cap 10 each) of one provider with a shared cap of 3: never more than 3 calls
    run at once across both, every call succeeds and no slot is left behind."""
    config = QueueConfig(models=MODELS)
    live = ModelSettings.model_validate({"providers": {"openai": {"shared_max_concurrency": 3}}})
    resolver = SettingsResolver(config, live)
    limiter = AdaptiveLimiter(valkey, keys, limiter_settings(config, resolver))
    provider = SlowProvider()
    deps = CoreDeps(
        config=config,
        keys=keys,
        providers=lambda alias: provider,
        limiter=limiter,
        settings=lambda: resolver,
    )
    replies = await run_all(deps, 40)
    assert len(replies) == 40
    assert all(r.ok for r in replies)
    assert provider.peak == 3
    shared = await limiter.state("@openai", "shared")
    assert (shared.limit, shared.inflight) == (3, 0)
    assert (await limiter.state("a", "primary")).inflight == 0


async def test_live_cap_lowers_a_learned_limit_on_the_next_acquire(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """A stored limit of 10 is clamped to a live `max_concurrency` of 2 on the next acquire, and
    the provider never sees more than 2 rt calls of that model at once."""
    config = QueueConfig(models=MODELS)
    queue = LLMQueue(config)
    limiter = AdaptiveLimiter(valkey, keys, limiter_settings(config))
    breaker = CircuitBreaker(valkey, keys)
    lease = await limiter.try_acquire("a", "primary", Lane.RT)
    assert lease is not None
    await limiter.release(lease, throttled=False)
    assert (await limiter.state("a", "primary")).limit == 10
    store = model_settings_store(valkey, keys)
    await store.set({"models": {"a": {"max_concurrency": 2}}}, actor="test")
    queue._apply_settings(await store.load(), limiter, breaker)
    assert limiter.settings.max_for("a") == 2
    provider = SlowProvider()
    deps = CoreDeps(
        config=config,
        keys=keys,
        providers=lambda alias: provider,
        limiter=limiter,
        settings=lambda: queue.settings,
    )
    jobs = [execute_job(envelope(i, "a", "rt"), deps, claim=False, store=False) for i in range(12)]
    replies = [r.reply for r in await asyncio.gather(*jobs)]
    assert all(r is not None and r.ok for r in replies)
    assert provider.peak == 2
    assert limiter.settings.share_for(Lane.BG, "a") * 2 < 2  # bg/bulk get bg_share of it: 1
    assert (await limiter.state("a", "primary")).limit == 2
    assert isinstance(queue.failover, FailoverState)
    assert queue.failover.local_cap("a", "primary") <= 2  # Valkey-down caps follow too
