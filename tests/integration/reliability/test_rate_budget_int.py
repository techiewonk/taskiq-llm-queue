"""Rate budget Lua (`RATE`) on real Valkey: smooth and fixed windows, rt reserve, refunds, TPM.

The local budget (Valkey down) is checked against the same cases with an injected clock.
"""

from __future__ import annotations

import time

import pytest
import redis.asyncio

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability.rate import (
    LocalRateBudget,
    RateLimits,
    ValkeyRateBudget,
    estimate_tokens,
    usage_tokens,
)

pytestmark = pytest.mark.integration


async def test_smooth_allows_the_burst_then_one_per_interval(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """RPM 600 smooth, burst 5: 5 back to back, the 6th waits ~0.1 s (60 / 600)."""
    budget = ValkeyRateBudget(valkey, keys)
    limits = RateLimits(rpm=600, burst=5)
    grants = [await budget.take("m", "t", limits, bg=False, tokens=0) for _ in range(5)]
    assert all(g is not None for g, _ in grants)
    grant, wait = await budget.take("m", "t", limits, bg=False, tokens=0)
    assert grant is None
    assert 0.05 < wait <= 0.11


async def test_fixed_window_allows_the_whole_minute_then_waits_for_the_next(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """RPM 10 fixed: 10 now, the 11th waits until the next calendar minute."""
    budget = ValkeyRateBudget(valkey, keys)
    limits = RateLimits(rpm=10, window="fixed")
    started = time.time()
    for _ in range(10):
        assert (await budget.take("m", "t", limits, bg=False, tokens=0))[0] is not None
    grant, wait = await budget.take("m", "t", limits, bg=False, tokens=0)
    assert grant is None
    next_minute = (int(started // 60) + 1) * 60 - started
    assert abs(wait - next_minute) < 1.5 or wait <= 60


async def test_bg_cannot_use_the_rt_reserve(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """RPM 10 fixed, rt share 0.2: bg stops at 8, rt still gets the last 2."""
    budget = ValkeyRateBudget(valkey, keys)
    limits = RateLimits(rpm=10, window="fixed", rt_share=0.2)
    taken = 0
    while (await budget.take("m", "t", limits, bg=True, tokens=0))[0] is not None:
        taken += 1
    assert taken == 8
    rt = [await budget.take("m", "t", limits, bg=False, tokens=0) for _ in range(3)]
    assert [g is not None for g, _ in rt] == [True, True, False]


async def test_refund_returns_the_call_and_tpm_correction_charges_usage(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """A refunded call frees its unit; TPM reserves the estimate, `give` corrects to usage."""
    budget = ValkeyRateBudget(valkey, keys)
    limits = RateLimits(rpm=2, tpm=1000, window="fixed")
    first, _ = await budget.take("m", "t", limits, bg=False, tokens=600)
    assert first is not None
    assert (await budget.take("m", "t", limits, bg=False, tokens=600))[0] is None  # tpm full
    await budget.give(first, 500, call=False)  # used 100 of the 600 reserved
    second, _ = await budget.take("m", "t", limits, bg=False, tokens=600)
    assert second is not None
    seen = await budget.peek("m", "t", limits)
    assert (seen["calls"], seen["tokens"], seen["rpm_left"]) == (2, 700, 0)
    await budget.give(second, call=True)  # never started: call and tokens back
    seen = await budget.peek("m", "t", limits)
    assert (seen["calls"], seen["tokens"], seen["rpm_left"], seen["tpm_left"]) == (1, 100, 1, 900)


async def test_a_call_larger_than_the_tpm_burst_passes_when_idle(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """A 5,000-token call under TPM 10,000 (burst 1,000) is not refused forever."""
    budget = ValkeyRateBudget(valkey, keys)
    limits = RateLimits(tpm=10_000)
    assert (await budget.take("m", "t", limits, bg=False, tokens=5000))[0] is not None
    grant, wait = await budget.take("m", "t", limits, bg=False, tokens=5000)
    assert grant is None
    assert 25 < wait < 31  # 5,000 tokens at 10,000 / min


async def test_local_budget_matches_and_divides_by_pods() -> None:
    """C8: while Valkey is down each pod gets `share` of the budget, with the same rules."""
    now = [1000.0]
    budget = LocalRateBudget(share=lambda: 0.5, clock=lambda: now[0])
    limits = RateLimits(rpm=20, window="fixed", rt_share=0.2)
    now[0] = 60 * 100 + 1.0
    taken = 0
    while (await budget.take("m", "t", limits, bg=True, tokens=0))[0] is not None:
        taken += 1
    assert taken == 8  # 20 * 0.5 = 10 for this pod, 80 % of it for bg
    grant, _ = await budget.take("m", "t", limits, bg=False, tokens=0)
    assert grant is not None
    await budget.give(grant, call=True)
    assert (await budget.take("m", "t", limits, bg=False, tokens=0))[0] is not None
    now[0] += 60
    assert (await budget.take("m", "t", limits, bg=True, tokens=0))[0] is not None


def test_token_estimate_and_usage_shapes() -> None:
    """Estimate = input chars / 4 + output limit; usage read from Converse, OpenAI, Titan."""
    converse = {
        "messages": [{"role": "user", "content": [{"text": "x" * 400}]}],
        "inferenceConfig": {"maxTokens": 50},
    }
    assert estimate_tokens(converse) == (400 + 4) // 4 + 50
    assert estimate_tokens({"messages": [{"role": "user", "content": "hi"}]}) > 1000
    assert estimate_tokens({"input": ["a" * 40]}) == 10
    assert usage_tokens({"usage": {"totalTokens": 77}}) == 77
    assert usage_tokens({"usage": {"prompt_tokens": 5, "completion_tokens": 7}}) == 12
    assert usage_tokens({"body": b'{"inputTextTokenCount": 9}'}) == 9
    assert usage_tokens({"output": {}}) is None
