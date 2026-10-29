"""Load scenarios: S35 10K burst, S35b storm + worker kill, S06 priority, S27 load, S31, S34.

The sender is a separate process (`tools.scenarios.loadgen`) so its peak RSS is measured alone;
exactly-once is proven by joining the sender's replies with the fakellm probe's call log.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
from collections import Counter
from typing import Any

import orjson

from tools.scenarios import api, stack
from tools.scenarios.env import ROOT
from tools.scenarios.harness import Ctx, scenario
from tools.scenarios.loadgen import DEFAULT_MIX

THROTTLES = "llmq_throttles_total"
FALLBACKS = "llmq_fallback_total"


async def run_loadgen(
    ctx: Ctx,
    spec: dict[str, Any],
    config: Any,
    *,
    timeout_secs: float,
    during: Any = None,
) -> dict[str, Any]:
    """Run the load generator as a child process; `during(elapsed, done)` is polled each 0.5 s."""
    spec_path = ctx.dir / "loadgen.spec.json"
    cfg_path = ctx.dir / "loadgen.config.json"
    out_path = ctx.dir / "loadgen.out.json"
    progress = ctx.dir / "loadgen.progress"
    spec_path.write_bytes(orjson.dumps(spec))
    cfg_path.write_bytes(api.config_to_json(config))
    log = (ctx.dir / "loadgen.log").open("ab")
    proc = subprocess.Popen(  # noqa: ASYNC220  # a child process, polled below without blocking
        [
            ctx.settings.python,
            "-m",
            "tools.scenarios.loadgen",
            "--spec",
            str(spec_path),
            "--config",
            str(cfg_path),
            "--out",
            str(out_path),
            "--progress",
            str(progress),
        ],
        cwd=ROOT,
        env=ctx.settings.worker_env(),
        stdout=log,
        stderr=log,
    )
    log.close()
    t0 = time.monotonic()
    try:
        while proc.poll() is None:
            elapsed = time.monotonic() - t0
            if elapsed > timeout_secs:
                raise TimeoutError(f"load generator still running after {timeout_secs}s")
            if during is not None:
                done = 0
                if progress.exists():
                    try:
                        done = int(progress.read_text(encoding="utf-8").split()[0])
                    except (OSError, ValueError, IndexError):
                        done = 0
                await during(elapsed, done)
            await asyncio.sleep(0.5)
    finally:
        if proc.poll() is None:
            proc.kill()
    ctx.expect(proc.returncode == 0 and out_path.exists(), f"loadgen exited {proc.returncode}")
    result: dict[str, Any] = orjson.loads(out_path.read_bytes())
    return result


class Timeline:
    """Once a second: limit, live leases and pause per model target, and queue depths.

    Written as JSON lines to `timeline.jsonl` in the scenario dir, so a failing burst shows
    which model's limit collapsed and when (S35b investigation, STATUS.md I4).
    """

    def __init__(self, ctx: Ctx, config: Any, procs: list[Any] | None = None) -> None:
        """Remember the model targets, queues and worker processes; nothing is read yet."""
        self.ctx = ctx
        self.procs = procs or []
        self.cpu_last: dict[str, tuple[float, float]] = {}  # name -> (wall, cpu_s)
        self.keys = api.keys(ctx.settings.prefix)
        self.targets = [(alias, t) for alias, m in config.models.items() for t in m.targets]
        self.queues = sorted({q for m in config.models.values() for q in m.queues.values()})
        self.path = ctx.dir / "timeline.jsonl"
        self.next_at = 0.0

    async def sample(self, elapsed: float) -> None:
        """Append one row when a second has passed since the last one."""
        if elapsed < self.next_at:
            return
        self.next_at = elapsed + 1.0
        r = stack.redis_client(self.ctx.settings)
        now = time.time()
        row: dict[str, Any] = {"t": round(elapsed, 1), "models": {}, "queues": {}}
        try:
            for alias, target in self.targets:
                state = await r.hgetall(self.keys.limiter(alias, target))
                live = await r.zcount(self.keys.leases(alias, target), now, "+inf")
                paused = float(state.get(b"paused_until", 0) or 0)
                row["models"][f"{alias}@{target}"] = {
                    "limit": int(state.get(b"limit", 0) or 0),
                    "inflight": int(live),
                    "paused": round(max(0.0, paused - now), 2),
                }
        finally:
            await r.aclose()
        counts = await stack.queue_counts(self.ctx.settings, self.queues)
        row["queues"] = {q: counts.get(q, {}).get("visible", 0) for q in self.queues}
        row["cpu"] = await self._cpu()
        with self.path.open("ab") as f:
            f.write(orjson.dumps(row) + b"\n")

    async def _cpu(self) -> dict[str, float]:
        """CPU seconds per wall second since the last sample, per worker, fakellm and sender."""
        now = time.monotonic()
        totals: dict[str, float] = {}
        for p in self.procs:
            cpu_s = p.stats().get("cpu_s")
            if cpu_s is not None and p.alive:
                totals[p.name] = float(cpu_s)
        with contextlib.suppress(Exception):
            totals["fakellm"] = float((await self.ctx.fake._req("GET", "/scn/cpu"))["cpu_s"])
        progress = self.ctx.dir / "loadgen.progress"
        with contextlib.suppress(OSError, ValueError, IndexError):
            totals["sender"] = float(progress.read_text(encoding="utf-8").split()[3])
        out: dict[str, float] = {}
        for name, cpu_s in totals.items():
            last = self.cpu_last.get(name)
            if last is not None and now > last[0]:
                out[name] = round((cpu_s - last[1]) / (now - last[0]), 2)
            self.cpu_last[name] = (now, cpu_s)
        return out


def _worker_counter(stats: dict[str, Any], name: str) -> float:
    counters = (stats.get("metrics") or {}).get("counters") or {}
    return float(sum(v for k, v in counters.items() if k.split("{", 1)[0] == name))


async def burst(
    ctx: Ctx,
    *,
    fake_scenario: str,
    total: int,
    workers: int = 3,
    kill: tuple[str, float] | None = None,
    min_ok_ratio: float = 1.0,
    timeout_secs: float = 1200.0,
) -> None:
    """Shared body of S35/S35b: burst from one sender, N workers, exactly-once accounting."""
    await ctx.fake.load(fake_scenario)
    config = ctx.config(timeout_secs={"rt": 180.0, "bg": 600.0, "bulk": 900.0})
    procs = await ctx.workers(workers, config)
    prefix = ctx.rid_prefix()
    spec = {"rid_prefix": prefix, "total": total, "mix": DEFAULT_MIX}
    ops_before = await stack.commandstats(ctx.settings)
    killed: dict[str, Any] = {}
    timeline = Timeline(ctx, config, procs)

    async def during(elapsed: float, done: int) -> None:
        await timeline.sample(elapsed)
        if kill and not killed and elapsed >= kill[1]:
            victim = next(p for p in procs if p.name == kill[0])
            killed["stats"] = victim.stats()
            killed["at_s"] = round(elapsed, 1)
            killed["done_before"] = done
            victim.kill()

    t0 = time.monotonic()
    out = await run_loadgen(ctx, spec, config, timeout_secs=timeout_secs, during=during)
    wall = time.monotonic() - t0
    ops_after = await stack.commandstats(ctx.settings)
    s = out["summary"]
    rows = out["rows"]
    log = await ctx.fake.calls(prefix)
    ok200 = stack.ok_calls(log)
    stats = await ctx.fake.stats()
    # Let live workers consume released copies (acked as done) before they stop; then stop
    # them so their final stats include peak RSS and counters.
    names = api.all_queue_names(config)
    leftover = await stack.settle_queues(ctx.settings, names, timeout_secs=120)
    ctx.metric(
        "settle_left", sum(c["visible"] + c["inflight"] for q, c in leftover.items() if q in names)
    )
    for p in procs:
        if p.alive:
            await p.stop()
    wstats = {p.name: p.stats() for p in procs}

    replies = Counter(r["rid"] for r in rows)
    lost = total - len(replies)
    double_replies = sum(1 for c in replies.values() if c > 1)
    ok_rids = {r["rid"] for r in rows if r["ok"]}
    ghosts = [rid for rid in ok_rids if ok200.get(rid, 0) == 0]
    dups = {rid: n for rid, n in ok200.items() if n > 1}
    delta = stack.ops_delta(ops_before, ops_after)
    ops_total = sum(delta.values())

    ctx.metric("requests", total)
    ctx.metric("ok", s["ok"])
    ctx.metric("errors", s["errors"])
    ctx.metric("lost", lost)
    ctx.metric("duplicate_provider_success", len(dups))
    ctx.metric("throughput_rps", round(total / s["wall_s"], 1))
    ctx.metric("wall_s", s["wall_s"])
    ctx.metric("retries", s["retries"])
    ctx.metric("fallbacks", s["fallbacks"])
    ctx.metric("throttles_fakellm", stack.stat_sum(stats, "throttled"))
    ctx.metric("throttles_workers", sum(_worker_counter(w, THROTTLES) for w in wstats.values()))
    ctx.metric("provider_calls", stack.stat_sum(stats, "calls"))
    ctx.metric("valkey_ops_per_s", round(ops_total / wall, 1))
    ctx.metric("valkey_top_cmds", dict(list(delta.items())[:8]))
    ctx.metric(
        "peak_rss_mb",
        {
            "sender": out["sender_peak_rss_mb"],
            **{n: w.get("peak_rss_mb") for n, w in wstats.items()},
        },
    )
    ctx.metric("served_by_worker", s["workers"])
    ctx.metric("served_targets", s["served"])
    ctx.metric(
        "lanes",
        {
            lane: {
                "n": v["n"],
                "ok": v["ok"],
                "queue_wait_s": {k: v["queue_wait_s"].get(k) for k in ("p50", "p95", "p99")},
                "e2e_s": {k: v["e2e_s"].get(k) for k in ("p50", "p95", "p99")},
            }
            for lane, v in s["lanes"].items()
        },
    )
    if s["error_samples"]:
        ctx.metric("error_samples", s["error_samples"])
    if kill:
        ctx.metric(
            "killed",
            {
                "worker": kill[0],
                "at_s": killed.get("at_s"),
                "done_before": killed.get("done_before"),
            },
        )
    (ctx.dir / "burst.metrics.json").write_bytes(orjson.dumps(ctx.metrics))

    ctx.expect(lost == 0, f"{lost} requests got no reply")
    ctx.expect(double_replies == 0, f"{double_replies} requests got two replies")
    ctx.expect(not ghosts, f"{len(ghosts)} ok replies without a provider success: {ghosts[:5]}")
    if kill:
        # A hard kill mid-call can repeat only calls that were in flight on the dead pod.
        slots = sum(config.worker_slots.get(a, 50) for a in ("gpt20b", "gpt120b", "titan"))
        ctx.expect(len(dups) <= slots, f"{len(dups)} duplicate provider successes > {slots}")
        await ctx.final_checks(config, settle_secs=120.0)
    else:
        ctx.expect(not dups, f"{len(dups)} requests ran twice at the provider: {list(dups)[:5]}")
        await ctx.final_checks(config)
    ok_ratio = s["ok"] / total
    ctx.expect(ok_ratio >= min_ok_ratio, f"ok ratio {ok_ratio:.4f} < {min_ok_ratio}: {s['errors']}")


@scenario(
    "S35",
    "10K burst: 1 sender, 3 workers, realistic latency, light throttle",
    tags=("slow",),
)
async def s35(ctx: Ctx) -> None:
    """10,000 requests (60 % rt, 30 % bg, 10 % bulk embed); exactly one reply each."""
    total = int(os.environ.get("SCN_BURST_TOTAL", "10000"))
    await burst(ctx, fake_scenario="scn_burst", total=total)


@scenario(
    "S35b",
    "10K burst under a 30 % throttle storm, one worker killed mid-run",
    tags=("slow",),
    chaos=True,
)
async def s35b(ctx: Ctx) -> None:
    """Same mix with throttle_rate 0.3 everywhere; w2 is hard-killed 10 s in."""
    total = int(os.environ.get("SCN_BURST_TOTAL", "10000"))
    await burst(
        ctx,
        fake_scenario="scn_storm",
        total=total,
        kill=("w2", float(os.environ.get("SCN_KILL_AFTER_S", "10"))),
        min_ok_ratio=float(os.environ.get("SCN_STORM_MIN_OK", "0.95")),
    )


@scenario("S35c", "10K burst, capacity throttling only (max_concurrency)", tags=("slow",))
async def s35c(ctx: Ctx) -> None:
    """S35 with 429s only above each target's capacity: what the limiter is built to learn."""
    total = int(os.environ.get("SCN_BURST_TOTAL", "10000"))
    await burst(ctx, fake_scenario="scn_burst_cap", total=total)


@scenario("S27", "Load: paced lane mix (default 30K/min for 15 min)", tags=("slow",))
async def s27(ctx: Ctx) -> None:
    """Paced load from one sender: nothing lost, rt queue wait p95 <= 2 s, throttles reported.

    Rate/duration/workers: SCN_LOAD_RATE_PER_MIN, SCN_LOAD_MINUTES, SCN_LOAD_WORKERS.
    Informational locally (one laptop runs sender, workers and fakellm); a gate in dev AWS.
    """
    rate = float(os.environ.get("SCN_LOAD_RATE_PER_MIN", "30000")) / 60
    minutes = float(os.environ.get("SCN_LOAD_MINUTES", "15"))
    n_workers = int(os.environ.get("SCN_LOAD_WORKERS", "3"))
    await ctx.fake.load("scn_burst_cap")
    config = ctx.config(timeout_secs={"rt": 90.0, "bg": 600.0, "bulk": 900.0})
    await ctx.workers(n_workers, config)
    total = int(rate * minutes * 60)
    spec = {
        "rid_prefix": ctx.rid_prefix(),
        "total": total,
        "rate_per_sec": rate,
        "mix": DEFAULT_MIX,
    }
    ops_before = await stack.commandstats(ctx.settings)
    t0 = time.monotonic()
    out = await run_loadgen(ctx, spec, config, timeout_secs=minutes * 60 + 1200)
    wall = time.monotonic() - t0
    delta = stack.ops_delta(ops_before, await stack.commandstats(ctx.settings))
    s = out["summary"]
    lost = total - len({r["rid"] for r in out["rows"]})
    rt_p95 = s["lanes"].get("rt", {}).get("queue_wait_s", {}).get("p95", 0.0)
    stats = await ctx.fake.stats()
    ctx.metric("offered_rps", round(rate, 1))
    ctx.metric("achieved_rps", s["throughput_rps"])
    ctx.metric("ok", s["ok"])
    ctx.metric("errors", s["errors"])
    ctx.metric("lost", lost)
    ctx.metric("rt_queue_wait_p95_s", rt_p95)
    ctx.metric("throttles_fakellm", stack.stat_sum(stats, "throttled"))
    ctx.metric("valkey_ops_per_s", round(sum(delta.values()) / wall, 1))
    ctx.metric("sender_peak_rss_mb", out["sender_peak_rss_mb"])
    ctx.expect(lost == 0, f"{lost} requests lost")
    ctx.expect(s["ok"] == total, f"errors: {s['errors']} {s['error_samples']}")
    ctx.expect(rt_p95 <= 2.0, f"rt queue wait p95 {rt_p95}s > 2 s")
    await ctx.final_checks(config, settle_secs=60)


@scenario("S31", "1K concurrent waiters on one pod")
async def s31(ctx: Ctx) -> None:
    """1,000 callers awaiting 3 s calls on one pod: batched result reads, loop lag < 100 ms."""
    from tools.scenarios.common import calls

    await ctx.fake.patch(
        "openai.gpt-oss-20b-1:0@ap-south-1", {"latency_ms": {"p50": 3000, "p95": 3000}}
    )
    cfg = ctx.config(worker_slots={"gpt20b": 400}, limiter={"initial": {"gpt20b": 400}})
    await ctx.workers(3, cfg)
    q = await ctx.queue(cfg)
    lag: list[float] = []
    stop = asyncio.Event()

    async def ticker() -> None:
        while not stop.is_set():
            t = time.perf_counter()
            await asyncio.sleep(0.02)
            lag.append(time.perf_counter() - t - 0.02)

    tick = asyncio.create_task(ticker())
    before = await stack.commandstats(ctx.settings)
    t0 = time.monotonic()
    pending = asyncio.create_task(calls(ctx, q, 1000, lane="bg", timeout_secs=300))
    await asyncio.sleep(2.5)  # all 1,000 sends are out; from here the pod only waits
    submit_lag = max(lag, default=0.0)
    lag.clear()
    replies = await pending
    wall = time.monotonic() - t0
    after = await stack.commandstats(ctx.settings)
    stop.set()
    await tick
    delta = stack.ops_delta(before, after)
    mget_rate = delta.get("mget", 0) / wall
    ctx.metric("wall_s", round(wall, 1))
    ctx.metric("ok", sum(1 for r in replies if r.ok))
    ctx.metric("valkey_ops_per_s", round(sum(delta.values()) / wall, 1))
    ctx.metric("mget_per_s", round(mget_rate, 1))
    ctx.metric("max_loop_lag_ms_waiting", round(max(lag) * 1000, 1))
    ctx.metric("max_loop_lag_ms_submit_burst", round(submit_lag * 1000, 1))
    ctx.metric("top_cmds", dict(list(delta.items())[:6]))
    ctx.expect(all(r.ok for r in replies), "calls failed")
    # Batched = one sweep per bg interval (100 ms here) plus at most one pub/sub-hinted take
    # per finished call, never one read per waiter (1K waiters x 10/s = 10K reads/s). A fixed
    # 40 MGET/s cap measured completion timing instead: 64ea8fe (passed at 32.5/s on 10-29)
    # read 45/s on the same stack later; 653 MGETs = 133 sweeps + 520 hinted takes (2026-10-29).
    mget_budget = wall / 0.1 + len(replies)
    ctx.metric("mget_vs_budget", (delta.get("mget", 0), round(mget_budget)))
    ctx.expect(
        delta.get("mget", 0) <= mget_budget,
        f"{mget_rate:.0f} MGET/s over sweeps + one per result: result reads are not batched",
    )
    ctx.expect(max(lag) < 0.1, f"event-loop lag {max(lag) * 1000:.0f} ms")
    await ctx.final_checks(cfg)


@scenario("S34", "Bulk isolation: bg waits stay low behind a bulk backlog", tags=("slow",))
async def s34(ctx: Ctx) -> None:
    """20K bulk titan embeddings queued, then bg embeddings at 2/s: bg wait p95 <= 60 s."""
    n_bulk = int(os.environ.get("SCN_S34_BULK", "20000"))
    await ctx.fake.patch(
        "amazon.titan-embed-text-v2:0@ap-south-1", {"latency_ms": {"p50": 100, "p95": 200}}
    )
    cfg = ctx.config(worker_slots={"titan": 50})
    q = await ctx.queue(cfg)

    async def embed_one(i: int, lane: str, tag: str) -> api.Reply:
        return await api.embed(
            q, "titan", [ctx.text(ctx.rid(i, tag))], lane=lane, timeout_secs=1800
        )

    sem = asyncio.Semaphore(2000)

    async def bulk_one(i: int) -> api.Reply:
        async with sem:
            return await embed_one(i, "bulk", "k")

    async def all_bulk() -> list[api.Reply]:
        return list(await asyncio.gather(*(bulk_one(i) for i in range(n_bulk))))

    bulk = asyncio.create_task(all_bulk())
    await asyncio.sleep(10)
    await ctx.workers(2, cfg)
    bg: list[api.Reply] = []
    waits: list[float] = []  # end-to-end: embed results carry no meta on the 0.x API
    for i in range(60):
        t0 = time.monotonic()
        reply = await embed_one(i, "bg", "g")
        bg.append(reply)
        if reply.ok:
            waits.append(time.monotonic() - t0)
        await asyncio.sleep(0.5)
    bulk_done_during = len(await ctx.fake.calls(ctx.rid_prefix("k")))
    p95 = sorted(waits)[int(0.95 * len(waits)) - 1] if waits else 0.0
    bulk.cancel()
    await asyncio.gather(bulk, return_exceptions=True)
    ctx.metric("bg_ok", len(waits))
    ctx.metric("bg_e2e_p95_s", round(p95, 2))
    ctx.metric("bulk_done_during_bg", bulk_done_during)
    ctx.expect(len(waits) == len(bg), "bg embeddings failed")
    ctx.expect(p95 <= 60, f"bg end-to-end p95 {p95:.1f}s > 60 s")
