"""R01-R12: RPM / TPM rate budgets (docs/RATE_LIMIT_PLAN.md) on fakellm.

Gates count provider starts per calendar minute from fakellm's call log (`t_start`), with a
tolerance of 2 for a token taken just before a minute boundary whose call lands just after it.
Budgets are small (RPM 20-60) so each scenario runs in one to three minutes.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections import Counter
from typing import Any

from tools.scenarios import api, stack
from tools.scenarios.common import calls, one
from tools.scenarios.env import GPT20B_ID, GPT20B_PRIMARY, GPT120B_APAC, GPT120B_ID
from tools.scenarios.harness import Ctx, scenario
from tools.scenarios.metrics import percentile

_EDGE = 2  # a token taken at :59.9 whose provider call starts at :00.1


async def _starts(ctx: Ctx, tag: str = "") -> list[tuple[float, str]]:
    """(t_start, model key) of every provider call of this run's `rid(..., tag)` ids."""
    log = await ctx.fake.calls(ctx.rid_prefix(tag))
    return [(float(c[0]), str(c[3])) for entries in log.values() for c in entries]


def _per_minute(starts: list[tuple[float, str]], match: str = "") -> dict[int, int]:
    return dict(sorted(Counter(math.floor(t / 60) for t, m in starts if match in m).items()))


async def _wait_for_minute_start(min_secs_left: float = 50) -> None:
    """Start a fixed-window run early in a minute, so its two windows are easy to read."""
    left = 60 - time.time() % 60
    if left < min_secs_left:
        await asyncio.sleep(left + 0.2)


def _settings(**models: Any) -> dict[str, Any]:
    providers = models.pop("providers", {})
    return {"settings_default": {"models": models, "providers": providers}}


@scenario("R01", "RPM fixed window: 120 queued, RPM 50, three pods share it", tags=("slow",))
async def r01(ctx: Ctx) -> None:
    """R01 gate.

    With R04: at most 50 starts per calendar minute across three workers; all 120 done.
    """
    cfg = ctx.config(**_settings(gpt20b={"rpm": 50, "rate_window": "fixed"}))
    await ctx.workers(3, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start()
    t0 = time.monotonic()
    replies = await calls(ctx, q, 120, lane="bg", timeout_secs=300)
    took = round(time.monotonic() - t0, 1)
    minutes = _per_minute(await _starts(ctx))
    kinds = Counter(r.error_kind for r in replies)
    ctx.metric("starts_per_minute", list(minutes.values()))
    ctx.metric("done_s", took)
    ctx.metric("outcomes", dict(kinds))
    ctx.expect(kinds.get("ok", 0) == 120, f"outcomes {dict(kinds)}")
    ctx.expect(max(minutes.values()) <= 50 + _EDGE, f"a minute had {max(minutes.values())}")
    ctx.expect(len(minutes) >= 3, f"120 at 50/min finished in {len(minutes)} minutes")
    await ctx.final_checks()


@scenario("R02", "RPM smooth: burst then an even pace; concurrency still caps", tags=("slow",))
async def r02(ctx: Ctx) -> None:
    """R02 gate.

    RPM 120 smooth (burst 10) and `max_concurrency` 3 with 1 s calls: never more than 3 in
    flight, at most burst + 2 per second in any 10 s window, 60 calls in >= 25 s.
    """
    await ctx.fake.patch(
        GPT20B_PRIMARY,
        {"latency_ms": {"p50": 1000, "p95": 1000}},
    )
    cfg = ctx.config(**_settings(gpt20b={"rpm": 120, "rate_burst": 10, "max_concurrency": 3}))
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    t0 = time.monotonic()
    replies = await calls(ctx, q, 60, lane="bg", timeout_secs=300)
    took = time.monotonic() - t0
    starts = sorted(t for t, _ in await _starts(ctx))
    window = max(sum(1 for u in starts if t <= u < t + 10) for t in starts)
    stats = await stack.leak_report(ctx.settings)
    ctx.metric("done_s", round(took, 1))
    ctx.metric("max_starts_in_10s", window)
    ctx.expect(all(r.ok for r in replies), "not every call succeeded")
    ctx.expect(window <= 10 + 20 + _EDGE, f"{window} starts in 10 s (burst 10 + 2/s)")
    ctx.expect(took >= 25, f"60 calls at 120/min with burst 10 took {took:.1f}s")
    ctx.expect(stats["live_leases"] == 0, f"leases left: {stats}")
    await ctx.final_checks()


@scenario("R03", "rt keeps its reserve while a bg backlog uses the budget", tags=("slow",))
async def r03(ctx: Ctx) -> None:
    """R03 gate.

    RPM 60 smooth, rt share 0.2: with 150 bg queued, an rt call every 4 s starts within 2
    s (p95) and bg never exceeds 48/min (80 %).
    """
    cfg = ctx.config(**_settings(gpt20b={"rpm": 60, "rate_burst": 4, "rt_rate_share": 0.2}))
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    backlog = asyncio.create_task(calls(ctx, q, 150, lane="bg", tag="bg", timeout_secs=400))
    await asyncio.sleep(5)
    waits = []
    for i in range(15):
        t0 = time.monotonic()
        reply = await one(ctx, q, i, tag="rt", lane="rt", timeout_secs=30)
        if reply.ok:
            waits.append(time.monotonic() - t0)
        await asyncio.sleep(4)
    backlog.cancel()
    bg_minutes = _per_minute(await _starts(ctx, "bg"))
    full = list(bg_minutes.values())[1:-1] or list(bg_minutes.values())
    ctx.metric("rt_ok", len(waits))
    ctx.metric("rt_p95_s", round(percentile(waits, 95), 2) if waits else None)
    ctx.metric("bg_starts_per_minute", list(bg_minutes.values()))
    ctx.expect(len(waits) == 15, f"rt calls ok: {len(waits)}/15")
    ctx.expect(bool(waits) and percentile(waits, 95) <= 2.0, "rt waited too long for a token")
    ctx.expect(max(full) <= 48 + _EDGE, f"bg used the rt reserve: {bg_minutes}")
    for i in range(150):
        await q.cancel(ctx.rid(i, "bg"))


@scenario("R05", "TPM budget: tokens per minute, corrected with usage", tags=("slow",))
async def r05(ctx: Ctx) -> None:
    """R05 gate.

    TPM 30,000 smooth; each call reserves ~260 tokens (max_tokens 250) and uses ~20 (fakellm
    replies are short). On reservations alone bg (80 % = 24,000/min) could start ~92 calls a
    minute, so 200 calls would need over two minutes; corrected with `usage` they finish in
    well under 90 s, and the tokens really used per minute stay under the TPM.
    """
    cfg = ctx.config(**_settings(gpt20b={"tpm": 30_000}))
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    t0 = time.monotonic()
    replies = await asyncio.gather(
        *(
            api.converse(
                q,
                "gpt20b",
                api.converse_request(ctx.text(ctx.rid(i)), max_tokens=250),
                lane="bg",
                request_id=ctx.rid(i),
                timeout_secs=300,
            )
            for i in range(200)
        )
    )
    took = time.monotonic() - t0
    used = sum(int(r.response["usage"]["totalTokens"]) for r in replies if r.ok)
    kinds = Counter(r.error_kind for r in replies)
    ctx.metric("done_s", round(took, 1))
    ctx.metric("tokens_used", used)
    ctx.metric("outcomes", dict(kinds))
    ctx.expect(kinds.get("ok", 0) == 200, f"outcomes {dict(kinds)}")
    ctx.expect(took < 90, f"200 calls took {took:.0f}s: the usage correction did not free tokens")
    ctx.expect(used / max(took / 60, 1) <= 30_000, f"{used} tokens in {took:.0f}s")
    code, out = await api.cli(cfg, "status", "--model", "gpt20b")
    counted = json.loads(out)[0]["targets"][0].get("tokens") if code == 0 else None
    ctx.metric("budget_tokens_this_minute", counted)
    # Charged usage (~2,800), not reservations (~52,000); lower if the minute turned mid-run.
    ctx.expect(counted is not None and 0 < counted <= used + 500, f"budget counted {counted}")


@scenario("R06", "Provider shared RPM over two models", tags=("slow",))
async def r06(ctx: Ctx) -> None:
    """R06 gate.

    gpt20b and gpt120b at RPM 40 each, bedrock shared RPM 50 (fixed): per minute each
    model <= 40 and both together <= 50.
    """
    cfg = ctx.config(
        **_settings(
            gpt20b={"rpm": 40, "rate_window": "fixed"},
            gpt120b={"rpm": 40, "rate_window": "fixed"},
            providers={"bedrock": {"shared_rpm": 50}},
        )
    )
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start()
    replies = await asyncio.gather(
        calls(ctx, q, 50, model="gpt20b", tag="a", timeout_secs=300),
        calls(ctx, q, 50, model="gpt120b", tag="b", timeout_secs=300),
    )
    starts = await _starts(ctx, "a") + await _starts(ctx, "b")
    both = _per_minute(starts)
    small = _per_minute(starts, GPT20B_ID)
    big = _per_minute(starts, GPT120B_ID)
    ctx.metric("both_per_minute", list(both.values()))
    ctx.metric("gpt20b_per_minute", list(small.values()))
    ctx.metric("gpt120b_per_minute", list(big.values()))
    ctx.expect(all(r.ok for group in replies for r in group), "not every call succeeded")
    ctx.expect(max(both.values()) <= 50 + _EDGE, f"provider budget exceeded: {both}")
    ctx.expect(max(small.values()) <= 40 + _EDGE, f"gpt20b exceeded 40: {small}")
    ctx.expect(max(big.values()) <= 40 + _EDGE, f"gpt120b exceeded 40: {big}")


@scenario("R07", "Deadline shorter than the wait for budget: rate_limited, not a timeout")
async def r07(ctx: Ctx) -> None:
    """R07 gate.

    the budget is spent until the next minute; an rt call with 10 s gets `rate_limited`
    with `retry_after_secs` before its deadline, never an LLMQueueTimeout.
    """
    cfg = ctx.config(**_settings(gpt20b={"rpm": 1, "rate_window": "fixed"}))
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start(30)
    first = await one(ctx, q, 0, lane="rt", timeout_secs=10)
    t0 = time.monotonic()
    second = await one(ctx, q, 1, lane="rt", timeout_secs=10)
    took = time.monotonic() - t0
    error = second.error
    ctx.metric("second_error", repr(error)[:120])
    ctx.metric("answered_after_s", round(took, 1))
    ctx.expect(first.ok, f"first call: {first.error!r}")
    ctx.expect("rate_limited" in str(error), f"want rate_limited, got {error!r}")
    retry_after = getattr(error, "retry_after_secs", None)
    ctx.expect(retry_after is not None and retry_after > 0, f"retry_after {retry_after}")
    ctx.expect(took < 10, f"answered after {took:.1f}s, past the deadline")
    await ctx.final_checks()


@scenario("R08", "Change RPM live mid-run", tags=("slow",))
async def r08(ctx: Ctx) -> None:
    """R08 gate.

    RPM 60 fixed, then `settings set rpm=15` live: the first full minute after the reload
    starts at most 15 calls.
    """
    cfg = ctx.config(cfg_reload_secs=5.0, **_settings(gpt20b={"rpm": 60, "rate_window": "fixed"}))
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start()
    task = asyncio.create_task(calls(ctx, q, 100, lane="bg", timeout_secs=400))
    await asyncio.sleep(10)
    code, out = await api.cli(cfg, "settings", "set", "--model", "gpt20b", "rpm=15")
    ctx.expect(code == 0, f"settings set failed: {out}")
    changed_minute = math.floor((time.time() + 10) / 60)  # after one reload
    await asyncio.sleep(max(0.0, (changed_minute + 1) * 60 - time.time() + 2))
    minutes = _per_minute(await _starts(ctx))
    task.cancel()
    for i in range(100):
        await q.cancel(ctx.rid(i))
    after = [n for m, n in minutes.items() if m > changed_minute]
    ctx.metric("starts_per_minute", list(minutes.values()))
    ctx.expect(bool(after) and max(after) <= 15 + _EDGE, f"after the live change: {minutes}")


@scenario("R09", "Valkey down: each pod keeps its share of the budget", chaos=True, tags=("slow",))
async def r09(ctx: Ctx) -> None:
    """R09 gate.

    Valkey stopped, RPM 20 fixed: the sender runs calls direct under its local budget
    (20 / live pods) and nothing fails; at most 20 start in the minute.
    """
    cfg = ctx.config(**_settings(gpt20b={"rpm": 20, "rate_window": "fixed"}))
    q = await ctx.queue(cfg)
    await _wait_for_minute_start()
    await stack.stop_service(ctx.settings.valkey_service)
    try:
        replies = await calls(ctx, q, 25, lane="bg", timeout_secs=100)
    finally:
        await stack.start_service(ctx.settings.valkey_service)
    minutes = _per_minute(await _starts(ctx))
    kinds = Counter(r.error_kind for r in replies)
    ctx.metric("outcomes", dict(kinds))
    ctx.metric("starts_per_minute", list(minutes.values()))
    ctx.expect(max(minutes.values()) <= 20 + _EDGE, f"local budget exceeded: {minutes}")
    ctx.expect(kinds.get("internal_error", 0) == 0, f"outcomes {dict(kinds)}")


@scenario("R11", "Budget used on the primary: rt falls back, bg waits")
async def r11(ctx: Ctx) -> None:
    """R11 gate.

    gpt120b RPM 5 per target (fixed): 8 rt calls use primary then apac; 8 bg calls stay
    on primary (the rest wait for the next minute, never apac).
    """
    cfg = ctx.config(**_settings(gpt120b={"rpm": 5, "rate_window": "fixed"}))
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start(40)
    rt = [await one(ctx, q, i, tag="rt", lane="rt", model="gpt120b") for i in range(8)]
    served = Counter(r.meta.served_target if r.ok else r.error_kind for r in rt)
    bg = asyncio.create_task(calls(ctx, q, 8, model="gpt120b", tag="bg", timeout_secs=200))
    await asyncio.sleep(15)
    bg_targets = Counter(m for _, m in await _starts(ctx, "bg"))
    ctx.metric("rt_served", dict(served))
    ctx.metric("bg_started_by_target", dict(bg_targets))
    ctx.expect(served.get("primary", 0) == 5, f"rt on primary: {dict(served)}")
    ctx.expect(served.get("apac", 0) == 3, f"rt fell back: {dict(served)}")
    ctx.expect(not any(GPT120B_APAC in m for m in bg_targets), f"bg used apac: {bg_targets}")
    bg.cancel()
    for i in range(8):
        await q.cancel(ctx.rid(i, "bg"))


@scenario("R12", "llm-queue status matches the call log")
async def r12(ctx: Ctx) -> None:
    """R12 gate.

    after 12 starts in this minute (RPM 30 fixed), `status` shows started 12, rpm_left 18,
    working 0 and an empty queue.
    """
    cfg = ctx.config(**_settings(gpt20b={"rpm": 30, "rate_window": "fixed"}))
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    await _wait_for_minute_start(30)
    replies = await calls(ctx, q, 12, lane="bg", timeout_secs=60)
    await asyncio.sleep(1)
    code, out = await api.cli(cfg, "status", "--model", "gpt20b")
    ctx.expect(code == 0, f"status failed: {out}")
    (row,) = json.loads(out)
    (target,) = row["targets"]
    ctx.metric("status", target)
    ctx.expect(all(r.ok for r in replies), "not every call succeeded")
    ctx.expect(target["started"] == 12, f"started {target['started']} (log: 12)")
    ctx.expect(target["rpm_left"] == 18, f"rpm_left {target['rpm_left']}")
    ctx.expect(target["working"] == 0, f"working {target['working']}")
    ctx.expect(row["queues"]["default"]["waiting"] == 0, f"queues {row['queues']}")
