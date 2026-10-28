"""Failure-path scenarios: expiry, cancellation, retries, timeouts, fatal errors, regions, DLQ.

Per-request faults are scripted in the fakellm probe (`ctx.fake.script(rid, [...])`), so each
scenario controls exactly which attempt fails and can count provider calls per request.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import Counter
from typing import Any

import orjson

from tools.scenarios import api, stack
from tools.scenarios.common import breaker_state, calls, one, redis_call, wait_until
from tools.scenarios.env import GPT20B_PRIMARY, GPT120B_APAC, GPT120B_PRIMARY
from tools.scenarios.harness import Ctx, scenario


def _attempt_gaps(log: list[list[Any]]) -> list[float]:
    """Seconds between the end of one provider call and the start of the next."""
    rows = sorted(log, key=lambda c: c[0])
    return [round(b[0] - a[1], 3) for a, b in itertools.pairwise(rows)]


@scenario("S16", "Expiry: rt deadline passes while workers are down", aws_safe="yes")
async def s16(ctx: Ctx) -> None:
    """Rt calls with a 2 s budget, no worker for 5 s: callers time out, messages deleted unrun."""
    q = await ctx.queue()
    t0 = time.monotonic()
    replies = await calls(ctx, q, 20, lane="rt", timeout_secs=2)
    waited = time.monotonic() - t0
    await asyncio.sleep(3)
    await ctx.workers(1)
    await asyncio.sleep(4)
    log = await ctx.fake.calls(ctx.rid_prefix())
    ctx.metric("caller_wait_s", round(waited, 2))
    ctx.metric("outcomes", dict(Counter(r.error_kind for r in replies)))
    ctx.metric("provider_calls", sum(len(v) for v in log.values()))
    ctx.expect(
        all(r.error_kind == "timeout" for r in replies), "callers did not get LLMQueueTimeout"
    )
    ctx.expect(not log, f"expired calls reached the provider: {list(log)[:3]}")
    await ctx.final_checks()


@scenario("S41", "Deadline expiry while queued (rt, bg, bulk)", aws_safe="yes")
async def s41(ctx: Ctx) -> None:
    """Every lane queued past its deadline with no worker: deleted unrun, never called."""
    q = await ctx.queue()
    rt = calls(ctx, q, 10, lane="rt", tag="r", timeout_secs=2)
    bg = calls(ctx, q, 10, lane="bg", tag="b", timeout_secs=3)
    texts = [ctx.text(ctx.rid(i, "e")) for i in range(5)]
    bulk = api.embed(q, "titan", texts, lane="bulk", timeout_secs=3)
    r_rt, r_bg, r_bulk = await asyncio.gather(rt, bg, bulk)
    await asyncio.sleep(2)
    await ctx.workers(1)
    await asyncio.sleep(5)
    log = await ctx.fake.calls(ctx.rid_prefix(""))
    kinds = Counter(r.error_kind for r in [*r_rt, *r_bg, r_bulk])
    ctx.metric("outcomes", dict(kinds))
    ctx.metric("provider_calls", sum(len(v) for v in log.values()))
    ctx.expect(set(kinds) == {"timeout"}, f"outcomes {dict(kinds)}")
    ctx.expect(not log, f"expired work reached the provider: {sorted(log)[:5]}")
    await ctx.final_checks()


@scenario("S17", "Cancellation of queued calls", aws_safe="yes")
async def s17(ctx: Ctx) -> None:
    """50 queued calls cancelled before any worker runs: none executed, callers get cancelled."""
    q = await ctx.queue()
    tasks = [asyncio.create_task(one(ctx, q, i)) for i in range(50)]
    await asyncio.sleep(1.5)
    for i in range(50):
        await q.cancel(ctx.rid(i))
    await ctx.workers(1)
    replies = await asyncio.gather(*tasks)
    await asyncio.sleep(2)
    log = await ctx.fake.calls(ctx.rid_prefix())
    kinds = Counter(r.error_kind for r in replies)
    ctx.metric("outcomes", dict(kinds))
    ctx.metric("provider_calls", sum(len(v) for v in log.values()))
    ctx.expect(kinds.get("cancelled", 0) == 50, f"outcomes {dict(kinds)}")
    ctx.expect(not log, f"cancelled calls executed: {len(log)}")
    leaks = await stack.leak_report(ctx.settings)
    ctx.expect(leaks["results_unread"] == 0, f"results left behind: {leaks}")
    await ctx.final_checks()


@scenario("S40", "Caller timeout shorter than processing", aws_safe="yes")
async def s40(ctx: Ctx) -> None:
    """Timeout 1 s vs a 3 s provider call.

    LLMQueueTimeout on time, provider called at most once, the late result is discarded (no
    unread result kept past its TTL), no lease/claim leak.
    """
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["hang:3"])
    t0 = time.monotonic()
    reply = await one(ctx, q, 1, lane="rt", timeout_secs=1.0)
    took = time.monotonic() - t0
    await asyncio.sleep(4)
    log = (await ctx.fake.calls(rid)).get(rid, [])
    rkey = api.keys(ctx.settings.prefix).result(rid)
    ttl = await redis_call(ctx, lambda r: r.ttl(rkey))
    ctx.metric("caller_wait_s", round(took, 3))
    ctx.metric("provider_calls", len(log))
    ctx.metric("late_result_ttl_s", ttl)
    ctx.expect(reply.error_kind == "timeout", f"got {reply.error!r}")
    ctx.expect(abs(took - 1.0) <= 0.2, f"timeout after {took:.3f}s (want 1.0 +- 0.2)")
    ctx.expect(len(log) <= 1, f"provider called {len(log)} times")
    ctx.expect(ttl <= ctx.base.result_ttl_secs, f"late result kept with TTL {ttl}")
    await ctx.final_checks()


@scenario("S42", "Retry on retryable 5xx and on 429 + Retry-After", aws_safe="no")
async def s42(ctx: Ctx) -> None:
    """503, 503, OK -> attempts=3 with backoff; OpenAI 429 Retry-After 1 s -> wait >= 1 s."""
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["ServiceUnavailableException", "ServiceUnavailableException"])
    r1 = await one(ctx, q, 1, lane="rt")
    log1 = (await ctx.fake.calls(rid))[rid]
    gaps = _attempt_gaps(log1)
    ctx.metric("bedrock_attempts", r1.meta.attempts)
    ctx.metric("bedrock_backoff_gaps_s", gaps)
    ctx.expect(r1.ok and r1.meta.attempts == 3, f"{r1.meta} {r1.error!r}")
    ctx.expect(len(log1) == 3, f"provider saw {len(log1)} calls (SDK retries must be off)")
    base = ctx.base.retry_base_secs
    ctx.expect(gaps[0] >= base * 0.9 and gaps[1] >= 2 * base * 0.9, f"no backoff: {gaps}")
    rid2 = ctx.rid(2)
    await ctx.fake.script(rid2, ["ThrottlingException"])
    request = {"messages": [{"role": "user", "content": ctx.text(rid2)}]}
    r2 = await api.chat(q, "llama", request, lane="rt", request_id=rid2)
    log2 = (await ctx.fake.calls(rid2))[rid2]
    gaps2 = _attempt_gaps(log2)
    ctx.metric("openai_calls", len(log2))
    ctx.metric("openai_retry_after_gap_s", gaps2)
    ctx.expect(r2.ok, f"openai call failed: {r2.error!r}")
    ctx.expect(len(log2) == 2, f"openai provider saw {len(log2)} calls (SDK retries must be off)")
    ctx.expect(gaps2[0] >= 0.95, f"backoff {gaps2[0]}s < Retry-After 1 s")
    await ctx.final_checks()


@scenario("S30", "SDK retries off + Retry-After honoured", aws_safe="no")
async def s30(ctx: Ctx) -> None:
    """One throttle per call (Bedrock and OpenAI): provider calls == attempts; limiter saw it."""
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["ThrottlingException"])
    r1 = await one(ctx, q, 1, lane="rt")
    log = (await ctx.fake.calls(rid))[rid]
    ctx.metric("bedrock_calls_vs_attempts", (len(log), r1.meta.attempts))
    ctx.expect(r1.ok and len(log) == r1.meta.attempts == 2, f"{len(log)} calls, {r1.meta}")
    from tools.scenarios.common import limiter_state

    state = await limiter_state(ctx, "gpt20b", "primary")
    ctx.metric("limiter_after_throttle", {k: state.get(k) for k in ("ramp", "last_decrease")})
    ctx.expect(state.get("ramp") == "0", f"limiter did not see the throttle: {state}")
    rid2 = ctx.rid(2)
    await ctx.fake.script(rid2, ["ThrottlingException"])
    r2 = await api.chat(
        q, "llama", {"messages": [{"role": "user", "content": ctx.text(rid2)}]}, request_id=rid2
    )
    log2 = (await ctx.fake.calls(rid2))[rid2]
    ctx.metric("openai_calls", len(log2))
    ctx.expect(r2.ok and len(log2) == 2, f"openai: {len(log2)} calls, {r2.error!r}")
    ctx.expect(_attempt_gaps(log2)[0] >= 0.95, f"Retry-After ignored: {_attempt_gaps(log2)}")


@scenario("S43", "No retry on fatal 4xx (ValidationException)", aws_safe="no")
async def s43(ctx: Ctx) -> None:
    """400 ValidationException: returned at once, attempts=1, no fallback, breaker unaffected."""
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["ValidationException"], repeat=True)
    t0 = time.monotonic()
    r = await api.converse(
        q,
        "gpt120b",
        api.converse_request(ctx.text(rid)),
        lane="rt",
        fallback="chain",
        request_id=rid,
    )
    took = time.monotonic() - t0
    log = (await ctx.fake.calls(rid))[rid]
    ctx.metric("provider_calls", len(log))
    ctx.metric("secs", round(took, 3))
    ctx.metric("error", str(r.error)[:80])
    ctx.expect(
        r.error_kind == "provider" and not getattr(r.error, "retryable", True), f"{r.error!r}"
    )
    ctx.expect(len(log) == 1, f"fatal error retried: {len(log)} calls ({[c[3] for c in log]})")
    ctx.expect(await breaker_state(ctx, "gpt120b", "primary") == "closed", "breaker tripped")
    from tools.scenarios.common import hgetall

    cb = await hgetall(ctx, api.keys(ctx.settings.prefix).breaker("gpt120b", "primary"))
    ctx.expect(int(cb.get("fail_count", 0) or 0) == 0, f"fatal counted as breaker failure: {cb}")
    await ctx.final_checks()


@scenario("S45", "Credentials error (ExpiredToken / AccessDenied)", aws_safe="no")
async def s45(ctx: Ctx) -> None:
    """ExpiredTokenException 403 and AccessDenied: fatal, not retried, breaker untouched."""
    await ctx.workers(1)
    q = await ctx.queue()
    out = {}
    for n, action in ((1, "err:ExpiredTokenException:403"), (2, "AccessDeniedException")):
        rid = ctx.rid(n)
        await ctx.fake.script(rid, [action], repeat=True)
        r = await one(ctx, q, n, lane="rt")
        log = (await ctx.fake.calls(rid))[rid]
        out[action.split(":")[1] if ":" in action else action] = (len(log), str(r.error)[:60])
        ctx.expect(r.error_kind == "provider", f"{action}: {r.error!r}")
        ctx.expect(not getattr(r.error, "retryable", True), f"{action} marked retryable")
        ctx.expect(len(log) == 1, f"{action} retried ({len(log)} calls)")
        ctx.expect(
            "ExpiredToken" in str(r.error) or "AccessDenied" in str(r.error),
            f"error does not name the cause: {r.error}",
        )
    ctx.metric("calls_and_error", out)
    ctx.expect(await breaker_state(ctx, "gpt20b", "primary") == "closed", "breaker tripped")
    await ctx.final_checks()


@scenario(
    "S38",
    "Model not available in region -> profile",
    xfail="core.run_call returns on any FATAL error without trying the next fallback step, so "
    "a per-region ResourceNotFound/Validation never reaches the APAC profile",
    aws_safe="no",
)
async def s38(ctx: Ctx) -> None:
    """Region answers ResourceNotFound: not retried there, served by the APAC profile."""
    await ctx.workers(1)
    q = await ctx.queue()
    await ctx.fake.patch(GPT120B_PRIMARY, {"force_error": "ResourceNotFoundException"})
    rid = ctx.rid(1)
    r = await api.converse(
        q, "gpt120b", api.converse_request(ctx.text(rid)), lane="rt", request_id=rid
    )
    log = (await ctx.fake.calls(rid)).get(rid, [])
    primary_calls = sum(1 for c in log if c[3].startswith("openai.gpt-oss-120b"))
    ctx.metric("primary_calls", primary_calls)
    ctx.metric("served", f"{r.meta.served_model}@{r.meta.served_target}" if r.ok else str(r.error))
    ctx.expect(await breaker_state(ctx, "gpt120b", "primary") == "closed", "breaker tripped")
    ctx.expect(primary_calls == 1, f"region called {primary_calls} times")
    ctx.expect(r.ok and r.meta.served_target == "apac", f"not served by apac: {r.error!r}")


@scenario(
    "S39",
    "Provider hang -> per-attempt timeout -> next target",
    xfail="no per-attempt timeout: run_call waits on one attempt until the call deadline "
    "(asyncio.wait_for(deadline - now)); Bedrock read_timeout is 300 s and not configurable",
    aws_safe="no",
)
async def s39(ctx: Ctx) -> None:
    """Region hangs 60 s; call budget 15 s: attempt aborted, served by apac within the deadline."""
    await ctx.workers(1)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["hang:60"], model="openai.gpt-oss-120b-1:0@")
    t0 = time.monotonic()
    r = await api.converse(
        q,
        "gpt120b",
        api.converse_request(ctx.text(rid)),
        lane="rt",
        timeout_secs=15,
        request_id=rid,
    )
    took = time.monotonic() - t0
    ctx.metric("secs", round(took, 2))
    ctx.metric("outcome", f"{r.meta.served_target}" if r.ok else r.error_kind)
    ctx.expect(took <= 15.5, f"took {took:.1f}s > deadline")
    ctx.expect(r.ok and r.meta.served_target == "apac", f"not retried on apac: {r.error!r}")


@scenario("S44", "Retries exhausted on every step", aws_safe="no")
async def s44(ctx: Ctx) -> None:
    """All targets throttled: chain and same_model end with a retryable ProviderError.

    Both end before the deadline; messages are acked once (no DLQ, nothing left in flight).
    """
    await ctx.workers(1)
    q = await ctx.queue()
    for key in (GPT120B_PRIMARY, GPT120B_APAC, GPT20B_PRIMARY):
        await ctx.fake.patch(key, {"force_error": "ThrottlingException"})
    out = {}
    for n, policy in ((1, "chain"), (2, "same_model")):
        rid = ctx.rid(n)
        t0 = time.monotonic()
        r = await api.converse(
            q,
            "gpt120b",
            api.converse_request(ctx.text(rid)),
            lane="bg",
            fallback=policy,
            timeout_secs=60,
            request_id=rid,
        )
        took = time.monotonic() - t0
        log = (await ctx.fake.calls(rid)).get(rid, [])
        out[policy] = {"secs": round(took, 1), "calls": len(log), "error": str(r.error)[:50]}
        ctx.expect(r.error_kind == "provider", f"{policy}: {r.error!r}")
        ctx.expect(bool(getattr(r.error, "retryable", False)), f"{policy}: not retryable")
        ctx.expect(took < 60, f"{policy}: ran to the deadline")
    ctx.metric("result", out)
    await ctx.final_checks()


@scenario("S46", "Cancel during retry backoff", aws_safe="no")
async def s46(ctx: Ctx) -> None:
    """Cancel while the worker sleeps between retries: no further provider calls."""
    cfg = ctx.config(retry_base_secs=3.0)
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["ServiceUnavailableException"], repeat=True)
    task = asyncio.create_task(one(ctx, q, 1, lane="bg", timeout_secs=60))
    first = await wait_until(lambda: _has_calls(ctx, rid), timeout_secs=20, every=0.1)
    ctx.expect(first is not None, "first attempt never happened")
    await asyncio.sleep(0.5)
    await q.cancel(rid)
    reply = await task
    await asyncio.sleep(8)
    log = (await ctx.fake.calls(rid))[rid]
    ctx.metric("provider_calls", len(log))
    ctx.metric("outcome", reply.error_kind)
    ctx.expect(reply.error_kind == "cancelled", f"caller got {reply.error!r}")
    ctx.expect(len(log) == 1, f"{len(log)} provider calls after cancelling in backoff")
    await ctx.final_checks(cfg)


async def _has_calls(ctx: Ctx, rid: str) -> bool:
    return bool((await ctx.fake.calls(rid)).get(rid))


@scenario("S36", "Region down -> profile -> back", aws_safe="no")
async def s36(ctx: Ctx) -> None:
    """Region 5xx 100 %: calls served by APAC; after recovery traffic returns to the region."""
    from tools.scenarios.common import Stream

    await ctx.workers(1)
    q = await ctx.queue()
    await ctx.fake.patch(GPT120B_PRIMARY, {"error_rate_5xx": 1.0})
    stream = Stream(ctx, q, "gpt120b", "rt", 4.0, "s").start()
    await asyncio.sleep(20)
    t_heal = time.time()
    await ctx.fake.patch(GPT120B_PRIMARY, {"error_rate_5xx": 0.0})
    await asyncio.sleep(35)
    records = await stream.stop()
    down = [r for s, _, r in records if s < t_heal - 2]
    after = [(s, r) for s, _, r in records if s >= t_heal]
    back = next((s - t_heal for s, r in after if r.ok and r.meta.served_target == "primary"), None)
    tail = [r for s, r in after if s >= t_heal + 25]
    ctx.metric("down_served", dict(Counter(r.meta.served_target or r.error_kind for r in down)))
    ctx.metric("back_on_primary_after_s", round(back, 1) if back is not None else None)
    ctx.metric("last_10s", dict(Counter(r.meta.served_target or r.error_kind for r in tail)))
    ctx.expect(
        all(r.ok for r in down),
        f"failures while region down: {Counter(r.error_kind for r in down)}",
    )
    ctx.expect(back is not None and back <= 25, f"not back on primary within 25 s ({back})")
    ctx.expect(all(r.meta.served_target == "primary" for r in tail), "traffic not back on primary")
    await ctx.final_checks()


@scenario("S37", "Live target change (skip_targets) mid-run", aws_safe="low-volume")
async def s37(ctx: Ctx) -> None:
    """Region throttled, apac serving; then `skip_targets: apac` is set live.

    Every worker chains to gpt20b within 30 s, with no in-flight loss.
    """
    from tools.scenarios.common import Stream

    cfg = ctx.config(cfg_reload_secs=5.0)
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    await ctx.fake.patch(GPT120B_PRIMARY, {"force_error": "ThrottlingException"})
    stream = Stream(ctx, q, "gpt120b", "rt", 4.0, "s", {"fallback": "chain"}).start()
    await asyncio.sleep(5)
    path = ctx.dir / "fallback.json"
    path.write_bytes(
        orjson.dumps({"chains": {"gpt120b": ["gpt20b"]}, "skip_targets": {"gpt120b": ["apac"]}})
    )
    code, out = await api.cli(cfg, "fallback", "set", "--file", str(path))
    ctx.expect(code == 0, f"fallback set --file failed: {out}")
    t_set = time.time()
    await asyncio.sleep(20)
    records = await stream.stop()
    after = [(s, r) for s, _, r in records if s >= t_set]
    switched = next((s - t_set for s, r in after if r.ok and r.meta.served_model == "gpt20b"), None)
    late = [r for s, r in after if s >= t_set + 2 * cfg.cfg_reload_secs + 1]
    ctx.metric("switched_after_s", round(switched, 1) if switched is not None else None)
    ctx.metric(
        "outcomes", dict(Counter(r.meta.served_target or r.error_kind for _, _, r in records))
    )
    ctx.expect(all(r.ok for _, _, r in records), "calls failed or were lost during the change")
    ctx.expect(switched is not None and switched < 30, "new targets not used within 30 s")
    ctx.expect(all(r.meta.served_model == "gpt20b" for r in late), "a worker kept using apac")
    await ctx.final_checks(cfg)


@scenario("S51", "Embedding batch partial failure", aws_safe="no")
async def s51(ctx: Ctx) -> None:
    """Cohere 192 texts = 2 batches; batch 1 fails once (503): only it is retried, order kept."""
    from fakellm.synth import embedding

    await ctx.workers(1)
    q = await ctx.queue()
    texts = [ctx.text(ctx.rid(i), f"t{i}") for i in range(192)]
    await ctx.fake.script(ctx.rid(0), ["ServiceUnavailableException"])
    r = await api.embed(q, "cohere", texts, lane="bg", input_type="search_document")
    log = await ctx.fake.calls(ctx.rid_prefix())
    per_batch = {rid: len(v) for rid, v in log.items()}
    ctx.metric("calls_per_batch", per_batch)
    ctx.expect(r.ok, f"embed failed: {r.error!r}")
    ctx.expect(per_batch.get(ctx.rid(0)) == 2, f"failed batch calls: {per_batch}")
    ctx.expect(per_batch.get(ctx.rid(96)) == 1, f"healthy batch retried: {per_batch}")
    dims = len(r.response[0])
    bad = [i for i, v in enumerate(r.response) if abs(v[0] - embedding(texts[i], dims)[0]) > 1e-5]
    ctx.expect(not bad, f"vectors out of order at {bad[:5]}")
    await ctx.final_checks()


@scenario("S26", "DLQ + redrive for a poison message", tags=("slow",), aws_safe="low-volume")
async def s26(ctx: Ctx) -> None:
    """Unparseable message on a queue with maxReceiveCount 3 lands in the DLQ.

    Redrive moves it back; healthy traffic next to it is unaffected.
    """
    queue = ctx.settings.queue("gpt20b")
    await stack.ensure_queues(ctx.settings, [queue], max_receive=3)
    ctx.on_cleanup(lambda: stack.ensure_queues(ctx.settings, [queue], max_receive=10))
    await ctx.workers(1)
    await stack.send_raw(ctx.settings, queue, "{this is not a taskiq message")
    q = await ctx.queue()
    ok = await calls(ctx, q, 5)
    ctx.expect(all(r.ok for r in ok), "healthy calls failed next to the poison message")

    async def in_dlq() -> bool:
        counts = await stack.queue_counts(ctx.settings, [queue])
        return counts[f"{queue}-dlq"]["visible"] >= 1

    took = await wait_until(in_dlq, timeout_secs=150, every=2)
    ctx.metric("dlq_after_s", took)
    ctx.expect(took is not None, "poison message never reached the DLQ")
    await ctx.stop_all()
    moved = await stack.redrive(ctx.settings, queue)
    counts = await stack.queue_counts(ctx.settings, [queue])
    ctx.metric("redriven", moved)
    ctx.expect(moved == 1 and counts[queue]["visible"] == 1, f"redrive: {moved} {counts}")
    await stack.purge(ctx.settings, [queue])


@scenario("S28", "Malformed input is released, never silently dropped", aws_safe="low-volume")
async def s28(ctx: Ctx) -> None:
    """Bad JSON, unknown task, envelope v=2: released (not acked).

    Invalid live config and invalid config are rejected with typed errors.
    """
    queue = ctx.settings.queue("gpt20b")
    q = await ctx.queue()
    sent = asyncio.create_task(one(ctx, q, "tmpl", timeout_secs=30))
    await asyncio.sleep(1.5)
    template = (await stack.peek(ctx.settings, queue))[0]["Body"]
    await stack.purge(ctx.settings, [queue])
    sent.cancel()
    msg = orjson.loads(template)
    unknown = {**msg, "task_name": "someone_else.task", "task_id": "unknown-1"}
    newer = orjson.loads(template)
    newer["task_id"] = "v2-1"
    newer["args"][0]["v"] = 2
    newer["args"][0]["request_id"] = "v2-1"
    await stack.send_raw(ctx.settings, queue, "{bad json")
    await stack.send_raw(ctx.settings, queue, orjson.dumps(unknown).decode())
    await stack.send_raw(ctx.settings, queue, orjson.dumps(newer).decode())
    await ctx.workers(1)
    await asyncio.sleep(6)
    counts = (await stack.queue_counts(ctx.settings, [queue]))[queue]
    ctx.metric("still_in_queue", counts)
    ctx.expect(counts["visible"] + counts["inflight"] == 3, f"malformed messages acked: {counts}")
    path = ctx.dir / "bad.json"
    path.write_text('{"chains": {"gpt120b": "not-a-list"', encoding="utf-8")
    code, out = await api.cli(ctx.base, "fallback", "set", "--file", str(path))
    ctx.expect(code != 0, f"invalid fallback JSON accepted: {out}")
    try:
        ctx.config(models={"Bad@Alias": ctx.base.models["gpt20b"]})
    except Exception as exc:  # pydantic ValidationError (a ValueError)
        ctx.metric("bad_config_error", type(exc).__name__)
    else:
        ctx.expect(False, "invalid model alias accepted")
    unknown_alias = await api.converse(q, "nope", api.converse_request("x"))
    ctx.expect(unknown_alias.error_kind == "config", f"unknown alias: {unknown_alias.error!r}")
    await stack.purge(ctx.settings, [queue])


@scenario("S29", "No secrets or content in messages and logs", aws_safe="yes")
async def s29(ctx: Ctx) -> None:
    """Messages and INFO worker logs carry no credentials, prompt text or raw session ids."""
    from taskiq_llm_queue import request_context

    secret_prompt = "TOPSECRETPROMPT-" + ctx.run
    q = await ctx.queue()
    queue = ctx.settings.queue("gpt20b-rt")
    with request_context(lane="rt", session_id="sess-RAW-123", user_id="user-RAW-456"):
        pending = asyncio.create_task(
            api.converse(
                q,
                "gpt20b",
                api.converse_request(ctx.text(ctx.rid(1), secret_prompt)),
                request_id=ctx.rid(1),
            )
        )
    await asyncio.sleep(1.5)
    peeked = await stack.peek(ctx.settings, queue)
    bodies = [m["Body"] + orjson.dumps(m.get("MessageAttributes", {})).decode() for m in peeked]
    await ctx.workers(1, log_level="INFO")
    reply = await pending
    await ctx.stop_all()
    logs = "".join(p.read_text(encoding="utf-8", errors="replace") for p in ctx.dir.glob("w*.log"))
    banned = [
        "AKIA",
        "aws_secret",
        "Authorization",
        "api_key",
        "sk-local",
        "localdev",
        secret_prompt,
    ]
    in_msgs = [b for b in banned if any(b in body for body in bodies) and b != secret_prompt]
    in_logs = [b for b in banned if b in logs]
    labels = [json_labels(m["Body"]) for m in peeked]
    raw_ids = [x for x in ("sess-RAW-123", "user-RAW-456") if any(x in lb for lb in labels)]
    ctx.metric("messages_checked", len(bodies))
    ctx.metric("log_bytes", len(logs))
    ctx.expect(reply.ok, f"call failed: {reply.error!r}")
    ctx.expect(bool(bodies), "no message captured")
    ctx.expect(not in_msgs, f"secrets in SQS messages: {in_msgs}")
    ctx.expect(not in_logs, f"secrets or prompt text in logs: {in_logs}")
    ctx.expect(not raw_ids, f"raw ids in task labels: {raw_ids}")


def json_labels(body: str) -> str:
    """The `labels` part of a taskiq message body as text ('' when unparsable)."""
    try:
        data = orjson.loads(body)
    except ValueError:
        return ""
    return orjson.dumps(data.get("labels", {}) if isinstance(data, dict) else {}).decode()
