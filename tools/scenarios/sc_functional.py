"""Functional scenarios, part 2.

OpenAI round trip, priority, limiter, pause gate, fallback, live config, breaker, large params,
canary, sync callers, jobs, custom provider.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from typing import Any

import aiohttp

from tools.scenarios import api, stack
from tools.scenarios.common import (
    Stream,
    breaker_state,
    calls,
    hgetall,
    limiter_state,
    redis_call,
    wait_until,
)
from tools.scenarios.env import GPT20B_PRIMARY, GPT120B_APAC, GPT120B_PRIMARY, TITAN_PRIMARY
from tools.scenarios.harness import Ctx, scenario
from tools.scenarios.metrics import percentile


def _strip_openai(body: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in body.items() if k not in ("created", "id", "system_fingerprint")}


@scenario("S02", "Chat round trip (OpenAI-compatible)", aws_safe="n/a")
async def s02(ctx: Ctx) -> None:
    """Queued `chat` on llama equals a plain POST to the OpenAI-compatible server."""
    await ctx.workers(1)
    q = await ctx.queue()
    request = {"messages": [{"role": "user", "content": ctx.text(ctx.rid(1), "hi there")}]}
    reply = await api.chat(q, "llama", request, lane="rt", request_id=ctx.rid(1))
    ctx.expect(reply.ok, f"queued chat failed: {reply.error!r}")
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            f"{ctx.settings.fakellm_url}/v1/chat/completions",
            json={"model": "llama-3.1-70b", **request},
        ) as resp,
    ):
        direct = await resp.json()
    got, want = _strip_openai(reply.response), _strip_openai(direct)
    ctx.expect(got["choices"] == want["choices"], f"choices differ: {got} vs {want}")
    ctx.expect(got["usage"] == want["usage"], f"usage differs: {got} vs {want}")
    ctx.metric("model", got.get("model"))
    await ctx.final_checks()


@scenario("S06", "Priority: rt stays fast behind a bg backlog")
async def s06(ctx: Ctx) -> None:
    """3K bg calls queued first, then rt at 5/s for 20 s: rt wait p95 <= 2 s, bg progresses."""
    n_bg = int(os.environ.get("SCN_S06_BG", "3000"))
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 300, "p95": 500}})
    cfg = ctx.config(worker_slots={"gpt20b": 40}, limiter={"initial": {"gpt20b": 40}})
    q = await ctx.queue(cfg)
    bg = asyncio.create_task(calls(ctx, q, n_bg, tag="bg", lane="bg", timeout_secs=900))
    await asyncio.sleep(3)  # the backlog sits in SQS before any worker runs
    await ctx.workers(1, cfg)
    stream = Stream(ctx, q, "gpt20b", "rt", 5.0, "rt").start()
    await asyncio.sleep(20)
    rt = await stream.stop()
    done_bg = len(await ctx.fake.calls(ctx.rid_prefix("bg")))
    waits = [r.meta.queue_wait_ms / 1000 for _, _, r in rt if r.ok]
    p95 = percentile(waits, 95)
    ctx.metric("rt_calls", len(rt))
    ctx.metric("rt_wait_p95_s", round(p95, 3))
    ctx.metric("bg_done_during_rt", done_bg)
    bg.cancel()
    await asyncio.gather(bg, return_exceptions=True)
    ctx.expect(len(waits) == len(rt), f"rt failures: {[r.error for _, _, r in rt if not r.ok][:3]}")
    ctx.expect(p95 <= 2.0, f"rt queue wait p95 {p95:.2f}s > 2s")
    ctx.expect(done_bg > 0, "bg made no progress while rt was served")


async def _closed_loop(
    ctx: Ctx, q: Any, model: str, concurrency: int, secs: float, tag: str
) -> list[api.Reply]:
    """`concurrency` callers each sending back-to-back bg calls for `secs`."""
    stop_at = time.monotonic() + secs
    out: list[api.Reply] = []
    counter = iter(range(10**9))

    async def caller() -> None:
        while time.monotonic() < stop_at:
            rid = ctx.rid(next(counter), tag)
            out.append(
                await api.converse(
                    q,
                    model,
                    api.converse_request(ctx.text(rid)),
                    lane="bg",
                    request_id=rid,
                    timeout_secs=120,
                )
            )

    await asyncio.gather(*(caller() for _ in range(concurrency)))
    return out


async def _sample_limit(
    ctx: Ctx, model: str, target: str, secs: float, every: float = 1.0
) -> list[tuple[float, int]]:
    out: list[tuple[float, int]] = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < secs:
        state = await limiter_state(ctx, model, target)
        out.append((round(time.monotonic() - t0, 1), int(state.get("limit", 0) or 0)))
        await asyncio.sleep(every)
    return out


@scenario(
    "S07",
    "Limit learning (capacity 50, offered 200)",
    tags=("slow",),
    xfail="the learned limit saw-tooths 44-64 around a real capacity of 50 instead of settling "
    "in 40-55: it keeps growing to ~1.3x capacity although in-flight is capped at 50 (DS12 "
    "'grow only when inflight >= 0.9 x limit' does not hold); throttle rate after 60 s is "
    "~0.2 % and no call fails",
)
async def s07(ctx: Ctx) -> None:
    """Limiter settles at 40-55 within 60 s; throttle rate < 2 % afterwards; nothing fails."""
    await ctx.fake.patch(
        GPT20B_PRIMARY, {"max_concurrency": 50, "latency_ms": {"p50": 400, "p95": 700}}
    )
    cfg = ctx.config(worker_slots={"gpt20b": 100})
    await ctx.workers(3, cfg)
    q = await ctx.queue(cfg)
    sampler = asyncio.create_task(_sample_limit(ctx, "gpt20b", "primary", 90))
    load = asyncio.create_task(_closed_loop(ctx, q, "gpt20b", 200, 90, "l"))
    await asyncio.sleep(60)
    before = (await ctx.fake.stats()).get(GPT20B_PRIMARY, {})
    replies = await load
    after = (await ctx.fake.stats()).get(GPT20B_PRIMARY, {})
    samples = await sampler
    late = [lim for t, lim in samples if t >= 60]
    calls_after = after.get("calls", 0) - before.get("calls", 0)
    thr_after = after.get("throttled", 0) - before.get("throttled", 0)
    rate = thr_after / calls_after if calls_after else 0.0
    failed = [r for r in replies if not r.ok]
    ctx.metric("limit_after_60s", f"{min(late, default=0)}-{max(late, default=0)}")
    ctx.metric("throttle_rate_after_60s", round(rate, 4))
    ctx.metric("calls", len(replies))
    ctx.metric("failed", len(failed))
    ctx.metric("max_concurrent_seen", after.get("max_concurrent_seen"))
    (ctx.dir / "limit_samples.txt").write_text(str(samples), encoding="utf-8")
    ctx.expect(not failed, f"{len(failed)} calls failed: {failed[0].error if failed else ''}")
    ctx.expect(bool(late) and min(late) >= 40 and max(late) <= 55, f"limit after 60 s: {late}")
    ctx.expect(rate < 0.02, f"throttle rate after settling {rate:.3%}")
    await ctx.final_checks(cfg)


@scenario("S08", "Fast start (initial 40, capacity 50)", tags=("slow",))
async def s08(ctx: Ctx) -> None:
    """With the stored limit wiped and initial 40, the limit reaches 45+ within 20 s."""
    await ctx.fake.patch(
        GPT20B_PRIMARY, {"max_concurrency": 50, "latency_ms": {"p50": 400, "p95": 700}}
    )
    cfg = ctx.config(worker_slots={"gpt20b": 100}, limiter={"initial": {"gpt20b": 40}})
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    sampler = asyncio.create_task(_sample_limit(ctx, "gpt20b", "primary", 25, every=0.5))
    load = asyncio.create_task(_closed_loop(ctx, q, "gpt20b", 120, 25, "f"))
    samples = await sampler
    replies = await load
    first = next((t for t, lim in samples if lim >= 45), None)
    ctx.metric("reached_45_at_s", first)
    ctx.metric("limits", [lim for _, lim in samples][::4])
    ctx.metric("failed", sum(1 for r in replies if not r.ok))
    ctx.expect(first is not None and first <= 20, f"limit did not reach 45 in 20 s: {samples}")


@scenario("S09", "Pause gate: throttled model stops receiving, others unaffected")
async def s09(ctx: Ctx) -> None:
    """gpt120b throttled 100 % for 15 s: its receives pause, gpt20b rate unchanged, then drains."""
    await ctx.workers(2)
    q = await ctx.queue()
    s120 = Stream(ctx, q, "gpt120b", "bg", 10.0, "a").start()
    s20 = Stream(ctx, q, "gpt20b", "bg", 10.0, "b").start()
    await asyncio.sleep(5)
    base = await ctx.fake.stats()
    for key in (GPT120B_PRIMARY, GPT120B_APAC):
        await ctx.fake.patch(key, {"throttle_rate": 1.0})
    t_pause = time.time()
    await asyncio.sleep(15)
    mid = await ctx.fake.stats()
    for key in (GPT120B_PRIMARY, GPT120B_APAC):
        await ctx.fake.patch(key, {"throttle_rate": 0.0})
    t_resume = time.time()
    await asyncio.sleep(5)
    r120 = await s120.stop()
    r20 = await s20.stop()
    keys120 = (GPT120B_PRIMARY, GPT120B_APAC)
    calls120 = sum(
        mid.get(k, {}).get("calls", 0) - base.get(k, {}).get("calls", 0) for k in keys120
    )
    rate_before = sum(1 for _, e, r in r20 if r.ok and e < t_pause) / max(1.0, t_pause - r20[0][0])
    rate_during = sum(1 for _, e, r in r20 if r.ok and t_pause <= e < t_resume) / 15
    ctx.metric("gpt120b_attempts_during_pause", calls120)
    ctx.metric("gpt20b_rps_before_during", (round(rate_before, 1), round(rate_during, 1)))
    ctx.metric("gpt120b_ok", sum(1 for _, _, r in r120 if r.ok))
    ctx.metric("gpt120b_errors", dict(Counter(r.error_kind for _, _, r in r120 if not r.ok)))
    # Unpaused, 10 calls/s for 15 s x 3 attempts x 2 targets would be ~900 provider attempts.
    ctx.expect(calls120 < 150, f"{calls120} gpt120b attempts while paused (receiving not paused)")
    ctx.expect(
        rate_during >= 0.8 * rate_before, f"gpt20b slowed: {rate_before:.1f} -> {rate_during:.1f}/s"
    )
    # Calls already running when the storm began may exhaust their retries; calls queued
    # during the pause must wait it out (not received) and succeed after the resume.
    queued = [r for s, _, r in r120 if s >= t_pause + 3]
    in_flight = [r for s, _, r in r120 if s < t_pause + 3]
    ctx.metric("failed_in_flight_at_pause", sum(1 for r in in_flight if not r.ok))
    ctx.expect(all(r.ok for r in queued), "gpt120b calls queued during the pause failed")
    await ctx.final_checks()


@scenario("S10", "Fallback: profile then model", aws_safe="low-volume")
async def s10(ctx: Ctx) -> None:
    """Region throttled -> APAC profile serves (queue keeps draining); both throttled -> chain."""
    await ctx.workers(1)
    q = await ctx.queue()
    await ctx.fake.patch(GPT120B_PRIMARY, {"force_error": "ThrottlingException"})

    async def call(n: int | str, lane: str, policy: str) -> api.Reply:
        rid = ctx.rid(n)
        req = api.converse_request(ctx.text(rid))
        return await api.converse(q, "gpt120b", req, lane=lane, fallback=policy, request_id=rid)

    r1 = await call(1, "rt", "same_model")
    ctx.expect(
        r1.ok and r1.meta.served_target == "apac", f"region throttled: {r1.meta} {r1.error!r}"
    )
    bg = await asyncio.gather(*(call(f"q{i}", "bg", "same_model") for i in range(30)))
    ctx.expect(all(r.ok for r in bg), "gpt120b bg queue did not drain via apac")
    await ctx.fake.patch(GPT120B_APAC, {"force_error": "ThrottlingException"})
    r2 = await call(2, "rt", "chain")
    ctx.expect(
        r2.ok and r2.meta.served_model == "gpt20b" and r2.meta.fallback_used,
        f"chain: {r2.meta} {r2.error!r}",
    )
    r3 = await call(3, "rt", "same_model")
    ctx.expect(not r3.ok and isinstance(r3.error, api.ProviderError), f"same_model: {r3.error!r}")
    ctx.expect(bool(getattr(r3.error, "retryable", False)), f"not retryable: {r3.error!r}")
    ctx.metric("served", [f"{r.meta.served_model}@{r.meta.served_target}" for r in (r1, r2)])
    ctx.metric("chain_attempts", r2.meta.attempts)
    await ctx.final_checks()


@scenario("S11", "Live fallback change", aws_safe="low-volume")
async def s11(ctx: Ctx) -> None:
    """`fallback set` reaches every worker within 30 s; invalid JSON keeps the last good chain."""
    cfg = ctx.config(cfg_reload_secs=5.0, retry_attempts=1)  # one attempt per step: fast
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    for key in (GPT120B_PRIMARY, GPT120B_APAC):
        await ctx.fake.patch(key, {"force_error": "ThrottlingException"})
    counter = iter(range(10**6))

    async def served() -> str:
        rid = ctx.rid(next(counter))
        r = await api.converse(
            q,
            "gpt120b",
            api.converse_request(ctx.text(rid)),
            lane="rt",
            fallback="chain",
            request_id=rid,
        )
        return r.meta.served_model if r.ok else "error"

    ctx.expect(await served() == "gpt20b", "default chain did not serve gpt20b")
    code, out = await api.fallback_set(cfg, "gpt120b", [])
    ctx.expect(code == 0, f"fallback set failed: {out}")
    t0 = time.monotonic()
    while time.monotonic() - t0 < 30 and await served() != "error":  # noqa: ASYNC110
        await asyncio.sleep(1)
    took = round(time.monotonic() - t0, 1)
    ctx.metric("new_chain_effective_s", took)
    ctx.expect(took < 30, "new (empty) chain not used within 30 s")
    results = [await served() for _ in range(6)]
    ctx.expect(set(results) == {"error"}, f"a worker still used the old chain: {results}")
    await api.fallback_set(cfg, "gpt120b", ["gpt20b"])
    await asyncio.sleep(cfg.cfg_reload_secs * 2 + 1)
    key = api.keys(ctx.settings.prefix).fallback
    await redis_call(ctx, lambda r: r.set(key, b"{not json"))
    await asyncio.sleep(cfg.cfg_reload_secs * 2 + 1)
    results = [await served() for _ in range(6)]
    ctx.metric("after_invalid_json", dict(Counter(results)))
    ctx.expect(set(results) == {"gpt20b"}, f"invalid JSON broke the chain: {results}")
    code, out = await api.cli(cfg, "fallback", "set", "--model", "gpt120b", "--chain", "gpt120b")
    ctx.metric("cli_rejects_self_chain", code != 0)
    ctx.expect(code != 0, f"CLI accepted a self-referencing chain: {out}")


async def _breaker_is(ctx: Ctx, state: str) -> bool:
    return await breaker_state(ctx, "gpt20b", "primary") == state


@scenario("S12", "Circuit breaker opens, half-opens, closes")
async def s12(ctx: Ctx) -> None:
    """5xx 100 %: open within 30 s, (almost) no calls while open, closed once healthy."""
    await ctx.workers(1)
    q = await ctx.queue()
    await ctx.fake.patch(GPT20B_PRIMARY, {"error_rate_5xx": 1.0})
    stream = Stream(ctx, q, "gpt20b", "rt", 2.0, "s").start()
    opened = await wait_until(lambda: _breaker_is(ctx, "open"), timeout_secs=30)
    ctx.metric("opened_after_s", opened)
    ctx.expect(opened is not None, "breaker did not open within 30 s")
    c0 = (await ctx.fake.stats()).get(GPT20B_PRIMARY, {}).get("calls", 0)
    await asyncio.sleep(8)
    c1 = (await ctx.fake.stats()).get(GPT20B_PRIMARY, {}).get("calls", 0)
    ctx.metric("calls_while_open_8s", c1 - c0)
    await ctx.fake.patch(GPT20B_PRIMARY, {"error_rate_5xx": 0.0})
    closed = await wait_until(lambda: _breaker_is(ctx, "closed"), timeout_secs=40)
    ctx.metric("closed_after_heal_s", closed)
    records = await stream.stop()
    ctx.metric("outcomes", dict(Counter(r.error_kind for _, _, r in records)))
    ctx.expect(c1 - c0 <= 2, f"{c1 - c0} calls reached the target while open")
    ctx.expect(closed is not None, "breaker not closed within 40 s of healing")
    await ctx.final_checks()


@scenario("S18", "Large params by reference (300 KB prompt)", aws_safe="low-volume")
async def s18(ctx: Ctx) -> None:
    """300 KB prompt: sent as params_ref (TTL ~state_ttl), result OK, ref deleted afterwards."""
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["hang:3"])
    big = ctx.text(rid, "x" * 300_000)
    task = asyncio.create_task(
        api.converse(q, "gpt20b", api.converse_request(big), lane="bg", request_id=rid)
    )
    pkey = api.keys(ctx.settings.prefix).params(rid)
    await asyncio.sleep(1.5)
    ttl = await redis_call(ctx, lambda r: r.ttl(pkey))
    reply = await task
    ctx.metric("params_ttl_s", ttl)
    ctx.expect(reply.ok, f"call failed: {reply.error!r}")
    ctx.expect(1790 <= ttl <= 1800, f"params key TTL {ttl} (want 1790-1800)")
    left = await redis_call(ctx, lambda r: r.exists(pkey))
    ctx.expect(left == 0, "params_ref key not deleted after the result was stored")
    await ctx.final_checks()


@scenario("S19", "Canary routing: 10 % sqs, sticky, kill switch", aws_safe="low-volume")
async def s19(ctx: Ctx) -> None:
    """`routing set gpt20b rt 10`: ~10 % of sessions on sqs, sticky; kill on -> 100 % direct."""
    from taskiq_llm_queue import request_context

    cfg = ctx.config(routing_enabled=True, cfg_reload_secs=2.0)
    await ctx.workers(1, cfg)
    code, out = await api.routing(cfg, "set", "--model", "gpt20b", "--lane", "rt", "--pct", "10")
    ctx.expect(code == 0, f"routing set failed: {out}")
    q = await ctx.queue(cfg)
    await asyncio.sleep(5)

    async def path_for(session: str, i: int) -> str:
        rid = ctx.rid(i)
        with request_context(lane="rt", session_id=session):
            r = await api.converse(q, "gpt20b", api.converse_request(ctx.text(rid)), request_id=rid)
        return "direct" if r.meta.worker.startswith("direct") else "sqs"

    paths = {s: await path_for(f"sess-{s}", s) for s in range(200)}
    share = sum(1 for p in paths.values() if p == "sqs") / len(paths)
    again = [await path_for(f"sess-{s}", 1000 + s) for s in range(40)]
    sticky = all(again[s] == paths[s] for s in range(40))
    ctx.metric("sqs_share", round(share, 3))
    ctx.metric("sticky", sticky)
    ctx.expect(0.04 <= share <= 0.18, f"sqs share {share:.2%} not ~10 %")
    ctx.expect(sticky, "routing not sticky per session")
    await api.routing(cfg, "kill", "on")
    t0 = time.monotonic()
    i = 2000
    took = None
    while time.monotonic() - t0 < 30:
        sample = [await path_for(f"sess-{s}", i + s) for s in range(30)]
        i += 30
        if all(p == "direct" for p in sample):
            took = round(time.monotonic() - t0, 1)
            break
    ctx.metric("kill_switch_s", took)
    ctx.expect(took is not None, "kill switch did not route 100 % direct within 30 s")


@scenario("S22", "Sync callers: thread, shared limiter, loop not blocked", aws_safe="low-volume")
async def s22(ctx: Ctx) -> None:
    """converse_sync / embed_sync from threads run direct, count sync_fallback, loop lag low."""
    q = await ctx.queue(mode="send")
    lag: list[float] = []
    stop = asyncio.Event()

    async def ticker() -> None:
        while not stop.is_set():
            t0 = time.perf_counter()
            await asyncio.sleep(0.01)
            lag.append(time.perf_counter() - t0 - 0.01)

    tick = asyncio.create_task(ticker())
    req = [api.converse_request(ctx.text(ctx.rid(i))) for i in range(10)]
    replies = await asyncio.gather(
        *(asyncio.to_thread(api.converse_sync, q, "gpt20b", r) for r in req),
        asyncio.to_thread(api.embed_sync, q, "titan", [ctx.text(ctx.rid("e"))]),
    )
    stop.set()
    await tick
    ctx.metric("sync_fallback_count", q.sync_fallback_count)
    ctx.metric("max_loop_lag_ms", round(max(lag) * 1000, 1))
    ctx.metric("workers", sorted({r.meta.worker for r in replies[:10]}))
    ctx.expect(
        all(r.ok for r in replies), f"sync failures: {[r.error for r in replies if not r.ok]}"
    )
    ctx.expect(q.sync_fallback_count >= 11, f"sync_fallback_count {q.sync_fallback_count}")
    ctx.expect(max(lag) < 0.1, f"event loop lag {max(lag) * 1000:.0f} ms")
    leaks = await stack.leak_report(ctx.settings)
    ctx.expect(leaks["live_leases"] == 0, f"sync path leaked leases: {leaks}")


@scenario("S23", "Jobs: ingest job embeds 5K texts on bulk", tags=("slow",), aws_safe="low-volume")
async def s23(ctx: Ctx) -> None:
    """A job on jobs-ingest embeds 5K texts on the bulk lane; claim ends `done`."""
    n = int(os.environ.get("SCN_S23_TEXTS", "5000"))
    await ctx.workers(1)
    done = asyncio.Event()
    vectors: list[int] = []
    holder: list[Any] = []

    async def ingest(payload: dict[str, Any]) -> None:
        texts = [ctx.text(ctx.rid(i), "chunk") for i in range(int(payload["n"]))]
        reply = await api.embed(holder[0], "titan", texts, lane="bulk")
        if not reply.ok:
            raise RuntimeError(str(reply.error))
        vectors.append(len(reply.response))
        done.set()

    q = api.new_queue(ctx.base, f"{ctx.sid.lower()}-jobs")
    q.register_job("ingest", ingest)
    holder.append(q)
    await q.start(mode="both")
    ctx._queues.append(q)
    job_id = ctx.rid("job")
    await api.send_job(q, "ingest", {"n": n}, job_id=job_id)
    await asyncio.wait_for(done.wait(), 900)
    await asyncio.sleep(2)
    claim = await hgetall(ctx, api.keys(ctx.settings.prefix).job(job_id))
    st = (await ctx.fake.stats()).get(TITAN_PRIMARY, {})
    ctx.metric("vectors", vectors[0] if vectors else 0)
    ctx.metric("job_state", claim.get("state"))
    ctx.metric("max_concurrent_seen", st.get("max_concurrent_seen"))
    ctx.expect(vectors == [n], f"vectors {vectors}")
    ctx.expect(claim.get("state") == "done", f"job claim {claim}")


class _ToyThrottle(Exception):
    """Raised by the toy provider to simulate a throttle."""


class _ToyProvider:
    """Custom provider: throttles calls whose params contain 'throttle-me', else answers."""

    name = "toy"

    def __init__(self) -> None:
        self.calls = 0

    async def call(self, op: str, params: Any, target: Any) -> dict[str, Any]:
        self.calls += 1
        if "throttle-me" in str(params):
            raise _ToyThrottle()
        return {
            "output": {"message": {"role": "assistant", "content": [{"text": "toy"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
        }

    def classify(self, exc: BaseException) -> Any:
        return api.error_kind_of("throttle" if isinstance(exc, _ToyThrottle) else "fatal")

    async def aclose(self) -> None:
        return None


@scenario("S25", "Custom provider: routed, limited, fallback-able", aws_safe="n/a")
async def s25(ctx: Ctx) -> None:
    """A toy provider serves via its own queue; throttles reach the limiter; chain falls back."""
    from taskiq_llm_queue import ModelConfig, Target

    toy = _ToyProvider()
    models = dict(ctx.base.models)
    models["toy"] = ModelConfig(
        provider="toy",
        model_id="toy-1",
        targets={"primary": Target()},
        queues={"rt": ctx.settings.queue("toy-rt"), "default": ctx.settings.queue("toy")},
    )
    cfg = ctx.config(
        models=models,
        allowed_providers=frozenset({"bedrock", "openai", "toy"}),
        fallback_default={"chains": {"gpt120b": ["gpt20b"], "toy": ["gpt20b"]}},
    )
    await ctx.ensure_queues(cfg)
    factories = {"toy": api.custom_provider_factory(toy)}
    await ctx.queue(cfg, mode="work", pod_id="toy-worker", provider_factories=factories)
    q = await ctx.queue(cfg, provider_factories=factories)
    r1 = await api.converse(
        q, "toy", api.converse_request(ctx.text(ctx.rid(1))), lane="rt", request_id=ctx.rid(1)
    )
    ctx.expect(
        r1.ok and r1.meta.served_model == "toy" and r1.meta.worker == "toy-worker",
        f"toy call: {r1.meta} {r1.error!r}",
    )
    r2 = await api.converse(
        q,
        "toy",
        api.converse_request(ctx.text(ctx.rid(2), "throttle-me")),
        lane="rt",
        fallback="chain",
        request_id=ctx.rid(2),
    )
    ctx.expect(r2.ok and r2.meta.served_model == "gpt20b", f"toy fallback: {r2.meta} {r2.error!r}")
    state = await limiter_state(ctx, "toy", "primary")
    ctx.metric("toy_calls", toy.calls)
    ctx.metric("toy_limiter", {k: state.get(k) for k in ("limit", "ramp", "last_decrease")})
    ctx.expect(
        state.get("ramp") == "0" or float(state.get("last_decrease", 0) or 0) > 0,
        f"limiter did not see the toy throttle: {state}",
    )
    await ctx.final_checks(cfg)


@scenario(
    "S04",
    "LangChain/LangGraph drop-in",
    skip="adapter API is being reworked "
    "(QueuedChatBedrockConverse model_alias=); covered by tests/integration until it lands",
)
async def s04(ctx: Ctx) -> None:
    """Placeholder."""


@scenario(
    "S24",
    "Broker parity (upstream examples)",
    skip="covered by tests/integration broker "
    "suite + capability check; S3 offload needs MinIO which the stack does not run",
)
async def s24(ctx: Ctx) -> None:
    """Placeholder."""


@scenario(
    "S33",
    "Broker call counts",
    skip="needs an SQS call-counting proxy in front of "
    "ElasticMQ (not built yet); A6 batching is covered by broker unit tests",
)
async def s33(ctx: Ctx) -> None:
    """Placeholder."""
