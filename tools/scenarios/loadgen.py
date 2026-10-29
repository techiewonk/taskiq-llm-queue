"""Pure-asyncio load generator: one sender process, a lane/model mix, per-request records.

Used as a child process by S35/S35b (burst) and by `python -m tools.scenarios load` (S27-style
paced load for N minutes). Every request carries `rid=<id>` so the fakellm probe can count
provider calls per request. Output: one JSON file with per-request rows and summaries.

Run: `python -m tools.scenarios.loadgen --spec spec.json --out result.json`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

from tools.scenarios import api
from tools.scenarios.env import Settings, ensure_aws_env
from tools.scenarios.metrics import RecordingSink, peak_rss_mb, summary

# (weight, kind, model, lane): 60 % rt converse, 30 % bg converse, 10 % bulk embeddings.
DEFAULT_MIX: list[tuple[float, str, str, str]] = [
    (0.30, "converse", "gpt20b", "rt"),
    (0.30, "converse", "gpt120b", "rt"),
    (0.15, "converse", "gpt20b", "bg"),
    (0.15, "converse", "gpt120b", "bg"),
    (0.10, "embed", "titan", "bulk"),
]


@dataclass(slots=True)
class Spec:
    """What to send: count (or rate x duration), mix, id prefix, timeouts, outstanding cap."""

    rid_prefix: str
    total: int = 10_000
    rate_per_sec: float = 0.0  # 0 = burst: everything at once
    max_outstanding: int = 10_000
    timeout_secs: dict[str, float] = field(default_factory=dict)  # per lane; default = config
    fallback: str = "same_model"
    seed: int = 7
    mix: list[tuple[float, str, str, str]] = field(default_factory=lambda: list(DEFAULT_MIX))


def plan(spec: Spec) -> list[tuple[str, str, str, str]]:
    """(rid, kind, model, lane) for every request, shuffled deterministically."""
    rng = random.Random(spec.seed)
    weights = [m[0] for m in spec.mix]
    rows: list[tuple[str, str, str, str]] = []
    for i in range(spec.total):
        _, kind, model, lane = rng.choices(spec.mix, weights)[0]
        rows.append((f"{spec.rid_prefix}{i}", kind, model, lane))
    return rows


async def one(
    q: Any, rid: str, kind: str, model: str, lane: str, spec: Spec, t_origin: float
) -> dict[str, Any]:
    """Send one request and return its record."""
    text = f"rid={rid} summarise item {rid[-6:]}"
    timeout = spec.timeout_secs.get(lane)
    t0 = time.time()
    if kind == "embed":
        reply = await api.embed(q, model, [text], lane=lane, timeout_secs=timeout, request_id=rid)
    else:
        reply = await api.converse(
            q,
            model,
            api.converse_request(text),
            lane=lane,
            fallback=spec.fallback,
            timeout_secs=timeout,
            request_id=rid,
        )
    t1 = time.time()
    m = reply.meta
    return {
        "rid": rid,
        "kind": kind,
        "model": model,
        "lane": lane,
        "ok": reply.ok,
        "error": reply.error_kind,
        "error_text": None if reply.ok else str(reply.error)[:160],
        "sent_at": round(t0 - t_origin, 4),
        "e2e_s": round(t1 - t0, 4),
        "queue_wait_ms": m.queue_wait_ms,
        "provider_ms": m.provider_ms,
        "attempts": m.attempts,
        "fallback_used": m.fallback_used,
        "served": f"{m.served_model}@{m.served_target}",
        "worker": m.worker,
    }


def summarise(rows: list[dict[str, Any]], wall_s: float) -> dict[str, Any]:
    """Throughput, outcome counts and per-lane latency percentiles."""
    by_lane: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_lane[row["lane"]].append(row)
    lanes = {}
    for lane, items in sorted(by_lane.items()):
        ok = [r for r in items if r["ok"]]
        lanes[lane] = {
            "n": len(items),
            "ok": len(ok),
            "errors": dict(Counter(r["error"] for r in items if not r["ok"])),
            "queue_wait_s": summary(r["queue_wait_ms"] / 1000 for r in ok),
            "e2e_s": summary(r["e2e_s"] for r in items),
        }
    ok_rows = [r for r in rows if r["ok"]]
    return {
        "requests": len(rows),
        "ok": len(ok_rows),
        "wall_s": round(wall_s, 2),
        "throughput_rps": round(len(rows) / wall_s, 1) if wall_s else 0.0,
        "errors": dict(Counter(r["error"] for r in rows if not r["ok"])),
        "error_samples": sorted({r["error_text"] for r in rows if not r["ok"]})[:5],
        "retries": sum(max(0, r["attempts"] - 1) for r in ok_rows),
        "fallbacks": sum(1 for r in ok_rows if r["fallback_used"]),
        "served": dict(Counter(r["served"] for r in ok_rows)),
        "workers": dict(Counter(r["worker"] for r in ok_rows)),
        "lanes": lanes,
    }


async def run(spec: Spec, config_path: Path | None, out: Path, progress: Path | None) -> None:
    """Send the whole plan from one `send`-mode `LLMQueue` and write the result file."""
    ensure_aws_env()
    settings = Settings()
    if config_path is not None:
        config = api.config_from_json(config_path.read_bytes())  # noqa: ASYNC240  # small local file, startup/teardown only
    else:
        config = api.build_config(settings)
    sink = RecordingSink()
    q = api.new_queue(config, f"sender-{spec.rid_prefix.rstrip('-')}", metrics=sink)
    await q.start(mode="send")
    rows: list[dict[str, Any]] = []
    gate = asyncio.Semaphore(spec.max_outstanding)
    done = 0

    async def guarded(item: tuple[str, str, str, str]) -> None:
        nonlocal done
        async with gate:
            rows.append(await one(q, *item, spec, t_origin))
        done += 1

    t_origin = time.time()
    tasks: list[asyncio.Task[None]] = []
    reporter = asyncio.create_task(_report_progress(progress, lambda: done, spec.total))
    try:
        for i, item in enumerate(plan(spec)):
            if spec.rate_per_sec > 0:
                due = t_origin + i / spec.rate_per_sec
                if (delay := due - time.time()) > 0:
                    await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(guarded(item)))
        await asyncio.gather(*tasks)
    finally:
        reporter.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reporter
        wall = time.time() - t_origin
        await q.stop(drain_secs=2)
    result = {
        "summary": summarise(rows, wall),
        "rows": rows,
        "sender_peak_rss_mb": peak_rss_mb(),
        "sender_metrics": sink.snapshot()["counters"],
    }
    out.write_bytes(orjson.dumps(result))  # noqa: ASYNC240  # small local file, startup/teardown only


async def _report_progress(path: Path | None, done: Any, total: int) -> None:
    if path is None:
        return
    while True:
        line = f"{done()} {total} {time.time()} {time.process_time():.3f}"
        path.write_text(line, encoding="utf-8")  # noqa: ASYNC240  # small local file
        await asyncio.sleep(0.5)


async def standalone(
    settings: Settings, *, rate_per_min: float, minutes: float, workers: int
) -> dict[str, Any]:
    """`python -m tools.scenarios load`: paced load against N local workers (S27-style)."""
    import os

    from tools.scenarios import harness

    os.environ["SCN_LOAD_RATE_PER_MIN"] = str(rate_per_min)
    os.environ["SCN_LOAD_MINUTES"] = str(minutes)
    os.environ["SCN_LOAD_WORKERS"] = str(workers)
    specs = harness.select(["S27"], None)
    results = await harness.run_many(specs, settings, harness.default_run_dir())
    r = results[0]
    return {
        "status": r.status,
        "message": r.message,
        "metrics": r.metrics,
        "lost": r.metrics.get("lost", 0),
    }


def main(argv: list[str] | None = None) -> None:
    """Parse flags and run."""
    parser = argparse.ArgumentParser(prog="loadgen")
    parser.add_argument("--spec", required=True)
    parser.add_argument("--config")
    parser.add_argument("--out", required=True)
    parser.add_argument("--progress")
    args = parser.parse_args(argv)
    raw = orjson.loads(Path(args.spec).read_bytes())
    raw["mix"] = [tuple(m) for m in raw.get("mix", DEFAULT_MIX)]
    spec = Spec(**raw)
    asyncio.run(
        run(
            spec,
            Path(args.config) if args.config else None,
            Path(args.out),
            Path(args.progress) if args.progress else None,
        )
    )


if __name__ == "__main__":
    main()
