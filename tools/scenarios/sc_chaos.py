"""Chaos scenarios: killed / stopped / paused workers, SQS and Valkey outages, scaling, deploys.

Infrastructure chaos stops the harness's own `valkey-scn` / `sqs-scn` services only; `isolate()`
brings them back before the next scenario even when one fails half way.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter

from tools.scenarios import api, stack
from tools.scenarios.common import Stream, calls, one, redis_call, wait_until
from tools.scenarios.env import GPT20B_PRIMARY
from tools.scenarios.harness import Ctx, scenario


def _ok_once(ctx: Ctx, replies: list[api.Reply], ok200: dict[str, int], rids: list[str]) -> None:
    """Every caller got an ok reply; report provider successes per request."""
    failed = [r.error for r in replies if not r.ok]
    ctx.expect(not failed, f"{len(failed)} calls failed: {failed[:3]}")
    missing = [rid for rid in rids if ok200.get(rid, 0) == 0]
    ctx.expect(not missing, f"ok replies without a provider success: {missing[:5]}")


@scenario("S13", "Worker killed mid-task", chaos=True, tags=("slow",))
async def s13(ctx: Ctx) -> None:
    """A worker is hard-killed during 5 s calls: every call gets one result, reclaimed <= ~75 s."""
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 5000, "p95": 5000}})
    # Short lease TTL so the dead pod's limiter leases expire (DS11) within the check window.
    cfg = ctx.config(limiter={"lease_ttl_secs": 20.0})
    w1, w2 = await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    task = asyncio.create_task(calls(ctx, q, 40, lane="bg", timeout_secs=200))
    await asyncio.sleep(2.5)
    w1.kill()
    t_kill = time.monotonic()
    replies = await task
    took = time.monotonic() - t_kill
    ok200 = stack.ok_calls(await ctx.fake.calls(ctx.rid_prefix()))
    rids = [ctx.rid(i) for i in range(40)]
    served = Counter(r.meta.worker for r in replies)
    ctx.metric("served", dict(served))
    ctx.metric("all_done_after_kill_s", round(took, 1))
    ctx.metric("re_executed", sum(1 for n in ok200.values() if n > 1))
    _ok_once(ctx, replies, ok200, rids)
    ctx.expect(took <= 75, f"reclaim took {took:.0f}s (> visibility 60 s + slack)")
    await ctx.final_checks(cfg, lease_wait_secs=30)
    del w2


@scenario("S14", "Duplicate delivery (visibility 2 s, 5 s task)", aws_safe="no")
async def s14(ctx: Ctx) -> None:
    """Visibility shorter than the task: the claim keeps the copy from running twice."""
    dup = ctx.settings.queue("dup-test")
    await stack.ensure_queues(ctx.settings, [dup], visibility_secs=2)
    models = dict(ctx.base.models)
    models["gpt20b"] = models["gpt20b"].model_copy(update={"queues": {"default": dup}})
    cfg = ctx.config(models=models)
    await ctx.workers(2, cfg)
    q = await ctx.queue(cfg)
    rids = [ctx.rid(i) for i in range(5)]
    for rid in rids:
        await ctx.fake.script(rid, ["hang:5"])
    replies = await calls(ctx, q, 5, lane="bg")
    log = await ctx.fake.calls(ctx.rid_prefix())
    ctx.metric("provider_calls", {rid[-1]: len(v) for rid, v in log.items()})
    ctx.expect(all(r.ok for r in replies), f"failures: {[r.error for r in replies if not r.ok]}")
    ctx.expect(all(len(log.get(rid, [])) == 1 for rid in rids), f"ran twice: {log}")
    await ctx.final_checks(cfg)
    await stack.purge(ctx.settings, [dup])


@scenario("S15", "Graceful shutdown releases unstarted work at once", chaos=True)
async def s15(ctx: Ctx) -> None:
    """Stopping a worker releases its buffered, unstarted messages: others pick them up < 5 s."""
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 1500, "p95": 1500}})
    cfg = ctx.config(worker_slots={"gpt20b": 10})
    w1, _w2 = await ctx.workers(2, cfg, drain_secs=20)
    q = await ctx.queue(cfg)
    task = asyncio.create_task(calls(ctx, q, 120, lane="bg", timeout_secs=200))
    await asyncio.sleep(3)
    t_stop = time.monotonic()
    stop_secs = await w1.stop(60)
    replies = await task
    total = time.monotonic() - t_stop
    ctx.metric("stop_secs", round(stop_secs, 1))
    ctx.metric("done_after_stop_s", round(total, 1))
    ctx.metric("served", dict(Counter(r.meta.worker for r in replies)))
    max_wait = max(r.meta.queue_wait_ms for r in replies) / 1000
    ctx.metric("max_queue_wait_s", round(max_wait, 1))
    ctx.expect(all(r.ok for r in replies), "calls failed across the shutdown")
    # 120 calls / (10 slots x 1.5 s) ~ 18 s on one worker; a stuck message would add 60 s.
    ctx.expect(total < 45, f"work finished {total:.0f}s after the stop (messages not released)")
    await ctx.final_checks(cfg)


@scenario(
    "S20",
    "SQS down -> direct, then back to sqs",
    chaos=True,
    aws_safe="no",
)
async def s20(ctx: Ctx) -> None:
    """SQS stopped: the first call fails over to direct within its budget, later ones go direct.

    Back to sqs once SQS returns and the failover window closes. The send is bounded by
    `sqs_send_timeout_secs` (it hung ~50-70 s on a dead pooled connection before).
    """
    cfg = ctx.config(sqs_failover_secs=15.0)
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    r0 = await one(ctx, q, 0)
    ctx.expect(r0.ok and not r0.meta.worker.startswith("direct"), f"baseline: {r0.meta}")
    await stack.stop_service(ctx.settings.sqs_service)
    t0 = time.monotonic()
    during = [await one(ctx, q, i, timeout_secs=30) for i in range(1, 6)]
    ctx.metric("five_calls_s", round(time.monotonic() - t0, 1))  # 3 send timeouts, then direct
    ctx.metric("during", [r.meta.worker.split(":")[0] if r.ok else r.error_kind for r in during])
    ctx.expect(
        all(r.ok and r.meta.worker.startswith("direct") for r in during),
        f"not served direct while SQS down: {[r.error for r in during]}",
    )
    await stack.start_service(ctx.settings.sqs_service)
    await ctx.ensure_queues()  # ElasticMQ keeps queues in memory; real SQS keeps them
    t1 = time.monotonic()
    back = None
    i = 10
    while time.monotonic() - t1 < 90:
        r = await one(ctx, q, i, timeout_secs=30)
        i += 1
        if r.ok and not r.meta.worker.startswith("direct"):
            back = round(time.monotonic() - t1, 1)
            break
        await asyncio.sleep(1)
    ctx.metric("back_to_sqs_after_s", back)
    ctx.expect(back is not None, "never returned to sqs after recovery")


@scenario(
    "S21",
    "Valkey down -> direct with local cap, no crash",
    chaos=True,
    aws_safe="no",
)
async def s21(ctx: Ctx) -> None:
    """Valkey stopped: calls still answered (direct, local cap); normal again after it returns.

    The send-only pod learns of the outage from its result dispatcher; calls already queued
    run direct after `valkey_wait_failover_secs` instead of waiting to their deadline.
    """
    await ctx.workers(1)
    q = await ctx.queue()
    ctx.expect((await one(ctx, q, 0)).ok, "baseline failed")
    await stack.stop_service(ctx.settings.valkey_service)
    t0 = time.monotonic()
    during = await asyncio.gather(*(one(ctx, q, i, timeout_secs=40) for i in range(1, 21)))
    ctx.metric("during_s", round(time.monotonic() - t0, 1))
    ctx.metric(
        "during",
        dict(Counter((r.meta.worker.split(":")[0] if r.ok else r.error_kind) for r in during)),
    )
    await stack.start_service(ctx.settings.valkey_service)

    async def healthy() -> bool:
        r = await one(ctx, q, f"h{time.monotonic_ns()}", timeout_secs=20)
        return r.ok and not r.meta.worker.startswith("direct")

    back = await wait_until(healthy, timeout_secs=90, every=1)
    ctx.metric("back_to_sqs_after_s", back)
    ctx.expect(
        all(r.ok for r in during),
        f"calls failed while Valkey was down: "
        f"{[str(r.error)[:60] for r in during if not r.ok][:3]}",
    )
    ctx.expect(back is not None, "not back to normal 90 s after Valkey returned")


@scenario("S47", "Manual pause/resume of a worker", aws_safe="low-volume")
async def s47(ctx: Ctx) -> None:
    """`pause_receiving` stops a pod taking work (backlog waits); resume drains it."""
    (w1,) = await ctx.workers(1)
    q = await ctx.queue()
    w1.command("pause")
    await asyncio.sleep(2)
    task = asyncio.create_task(calls(ctx, q, 30, lane="bg", timeout_secs=120))
    await asyncio.sleep(5)
    during = len(await ctx.fake.calls(ctx.rid_prefix()))
    counts = await stack.queue_counts(ctx.settings, [ctx.settings.queue("gpt20b")])
    w1.command("resume")
    t0 = time.monotonic()
    replies = await task
    ctx.metric("calls_while_paused", during)
    ctx.metric("backlog_while_paused", counts[ctx.settings.queue("gpt20b")]["visible"])
    ctx.metric("drained_after_resume_s", round(time.monotonic() - t0, 1))
    ctx.expect(during == 0, f"{during} calls ran while paused")
    ctx.expect(all(r.ok for r in replies), "calls failed after resume")
    await ctx.final_checks()


@scenario(
    "S48", "Remove: queue purge, alias removed from worker config, kill switch", aws_safe="no"
)
async def s48(ctx: Ctx) -> None:
    """Cohere removed from the worker's config while its messages are queued.

    They must be released (not lost) and run once the config is restored.
    """
    models = {k: v for k, v in ctx.base.models.items() if k != "cohere"}
    no_cohere = ctx.config(models=models)
    await ctx.workers(1, no_cohere, names=["old"])
    q = await ctx.queue()
    task = asyncio.create_task(
        api.embed(q, "cohere", [ctx.text(ctx.rid(1))], lane="bg", timeout_secs=90)
    )
    await asyncio.sleep(6)
    early = task.done()
    await ctx.workers(1, ctx.base, names=["new"])
    reply = await task
    ctx.metric("answered_by_old_worker", early)
    ctx.metric("outcome", reply.error_kind if not reply.ok else "ok")
    ctx.expect(not early, f"old worker answered a model it does not know: {task.result().error!r}")
    ctx.expect(reply.ok, f"call lost after the config was restored: {reply.error!r}")


@scenario("S49", "Long call > visibility timeout", tags=("slow",), aws_safe="low-volume")
async def s49(ctx: Ctx) -> None:
    """A 90 s provider call on a 60 s-visibility queue runs exactly once (heartbeat extends)."""
    await ctx.workers(2)
    q = await ctx.queue()
    rid = ctx.rid(1)
    await ctx.fake.script(rid, ["hang:90"])
    reply = await one(ctx, q, 1, lane="bg", timeout_secs=200)
    log = (await ctx.fake.calls(rid))[rid]
    ctx.metric("provider_calls", len(log))
    ctx.expect(reply.ok, f"failed: {reply.error!r}")
    ctx.expect(len(log) == 1, f"executed {len(log)} times")
    await ctx.final_checks()


@scenario("S50", "Valkey blip mid-call", chaos=True, aws_safe="no")
async def s50(ctx: Ctx) -> None:
    """Valkey restarted during 2 s calls: every caller gets an answer, no internal_error."""
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 2000, "p95": 2000}})
    await ctx.workers(2)
    q = await ctx.queue()
    task = asyncio.create_task(calls(ctx, q, 40, lane="bg", timeout_secs=150))
    await asyncio.sleep(1.0)
    await stack.stop_service(ctx.settings.valkey_service)
    await asyncio.sleep(2)
    await stack.start_service(ctx.settings.valkey_service)
    replies = await task
    ok200 = stack.ok_calls(await ctx.fake.calls(ctx.rid_prefix()))
    kinds = Counter(r.error_kind for r in replies)
    internal = [r for r in replies if "internal_error" in str(r.error)]
    ctx.metric("outcomes", dict(kinds))
    ctx.metric("duplicate_provider_success", sum(1 for n in ok200.values() if n > 1))
    ctx.expect(not internal, f"internal_error replies: {len(internal)}")
    ctx.expect(kinds.get("ok", 0) == len(replies), f"outcomes {dict(kinds)}")


@scenario("S52", "Scale out / in under load", chaos=True, tags=("slow",), aws_safe="no")
async def s52(ctx: Ctx) -> None:
    """1 -> 3 -> 2 workers during a 20/s stream: live_pods follows, nothing lost."""
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 300, "p95": 600}})
    await ctx.workers(1)
    q = await ctx.queue()
    pods_key = f"{ctx.settings.prefix}:pods"

    async def live() -> int:
        return int(await redis_call(ctx, lambda r: r.zcount(pods_key, f"({time.time()}", "+inf")))

    stream = Stream(ctx, q, "gpt20b", "bg", 20.0, "s").start()
    await asyncio.sleep(10)
    pods = [await live()]
    added = await ctx.workers(2)
    await asyncio.sleep(25)
    pods.append(await live())
    await added[-1].stop()
    await asyncio.sleep(40)
    pods.append(await live())
    records = await stream.stop()
    ctx.metric("live_pods", pods)
    ctx.metric("calls", len(records))
    ctx.metric("served", dict(Counter(r.meta.worker for _, _, r in records)))
    ctx.expect(all(r.ok for _, _, r in records), "calls failed or were lost while scaling")
    ctx.expect(pods[1] >= pods[0] + 2 and pods[2] == pods[1] - 1, f"live_pods {pods}")
    await ctx.final_checks()


@scenario("S32", "Rolling deploy under load", chaos=True, tags=("slow",), aws_safe="no")
async def s32(ctx: Ctx) -> None:
    """Each of 3 workers replaced one by one during a 30/s mixed stream.

    Pass: no DLQ, < 1 % rt timeouts, nothing lost.
    """
    await ctx.fake.patch(GPT20B_PRIMARY, {"latency_ms": {"p50": 400, "p95": 900}})
    workers = await ctx.workers(3, drain_secs=20)
    q = await ctx.queue()
    rt = Stream(ctx, q, "gpt20b", "rt", 20.0, "r").start()
    bg = Stream(ctx, q, "gpt120b", "bg", 10.0, "b").start()
    await asyncio.sleep(10)
    for i, old in enumerate(list(workers)):
        await old.stop(60)
        await ctx.workers(1, names=[f"new{i + 1}"])
        await asyncio.sleep(5)
    await asyncio.sleep(10)
    r_rt = await rt.stop()
    r_bg = await bg.stop()
    rt_timeouts = sum(1 for _, _, r in r_rt if r.error_kind == "timeout")
    ctx.metric("rt_calls", len(r_rt))
    ctx.metric("rt_timeout_ratio", round(rt_timeouts / max(1, len(r_rt)), 4))
    ctx.metric("outcomes", dict(Counter(r.error_kind for _, _, r in [*r_rt, *r_bg])))
    ctx.expect(rt_timeouts / max(1, len(r_rt)) < 0.01, f"{rt_timeouts} rt timeouts")
    ctx.expect(all(r.ok for _, _, r in [*r_rt, *r_bg]), "calls failed during the deploy")
    await ctx.final_checks()


del os
