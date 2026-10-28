"""Scenario registry, per-scenario context (isolation, workers, senders, checks) and runner.

A scenario is an async function taking a `Ctx`, registered with `@scenario(id, title, ...)`.
It raises `ScenarioFailed` (via `ctx.expect`) on a failed criterion and records numbers with
`ctx.metric`. `xfail=` marks a known library bug: the assertion stays, a failure is "xfail".
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from taskiq_llm_queue import LLMQueue, QueueConfig
from tools.scenarios import api, stack
from tools.scenarios.env import Settings, ensure_aws_env
from tools.scenarios.metrics import RecordingSink
from tools.scenarios.procs import FakellmProc, WorkerProc

ScenarioFn = Callable[["Ctx"], Awaitable[None]]
BASE_FAKELLM = "scn_base"


class ScenarioFailed(AssertionError):
    """A pass criterion did not hold."""


class ScenarioSkipped(Exception):
    """The scenario cannot run here (e.g. chaos disabled on a shared stack)."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """One registered scenario."""

    id: str
    title: str
    fn: ScenarioFn
    tags: tuple[str, ...]
    xfail: str | None = None
    skip: str | None = None
    chaos: bool = False
    aws_safe: str = "no"  # "yes", "low-volume" or "no" for real AWS runs


REGISTRY: dict[str, Scenario] = {}


def scenario(
    sid: str,
    title: str,
    *,
    tags: Sequence[str] = ("fast",),
    xfail: str | None = None,
    skip: str | None = None,
    chaos: bool = False,
    aws_safe: str = "no",
) -> Callable[[ScenarioFn], ScenarioFn]:
    """Register a scenario function under `sid`."""

    def wrap(fn: ScenarioFn) -> ScenarioFn:
        REGISTRY[sid] = Scenario(sid, title, fn, tuple(tags), xfail, skip, chaos, aws_safe)
        return fn

    return wrap


def _sort_key(sid: str) -> tuple[int, str]:
    digits = "".join(ch for ch in sid if ch.isdigit())
    return int(digits or 0), sid


def select(ids: Sequence[str], tag: str | None) -> list[Scenario]:
    """Scenarios by id (case-blind) or tag (`fast`, `slow`, `all`), in id order."""
    load_all()
    if ids:
        by_upper = {k.upper(): k for k in REGISTRY}
        wanted = {i.upper() for i in ids}
        unknown = wanted - set(by_upper)
        if unknown:
            raise SystemExit(f"unknown scenario ids: {sorted(unknown)}")
        chosen = [REGISTRY[by_upper[i]] for i in wanted]
    else:
        tag = tag or "fast"
        chosen = [s for s in REGISTRY.values() if tag == "all" or tag in s.tags]
    return sorted(chosen, key=lambda s: _sort_key(s.id))


def load_all() -> None:
    """Import every scenario module (registration is a side effect of import)."""
    from tools.scenarios import (  # noqa: F401  # imported for registration
        sc_basic,
        sc_chaos,
        sc_failures,
        sc_functional,
        sc_load,
    )


@dataclass(slots=True)
class Result:
    """Outcome of one scenario."""

    id: str
    title: str
    status: str  # pass, fail, xfail, xpass, skip, error
    secs: float
    metrics: dict[str, Any] = field(default_factory=dict)
    message: str = ""


class Ctx:
    """Everything one scenario needs; cleans up its workers and queues afterwards."""

    def __init__(self, settings: Settings, run_dir: Path, sid: str) -> None:
        """Bind to the harness settings and this scenario's output directory."""
        self.settings = settings
        self.dir = run_dir / sid
        self.dir.mkdir(parents=True, exist_ok=True)
        self.sid = sid
        self.run = uuid.uuid4().hex[:6]
        self.fake = stack.FakeLLM(settings.fakellm_url)
        self.metrics: dict[str, Any] = {}
        self._workers: list[WorkerProc] = []
        self._queues: list[LLMQueue] = []
        self._cleanups: list[Callable[[], Awaitable[None]]] = []
        self.base = api.build_config(settings)

    # ------------------------------------------------------------- helpers

    def rid(self, n: int | str, tag: str = "") -> str:
        """Unique request id for this scenario run, also used as the fakellm marker."""
        return f"{self.sid.lower()}{tag}-{self.run}-{n}"

    def rid_prefix(self, tag: str = "") -> str:
        """Prefix shared by every `rid(..., tag)` of this run."""
        return f"{self.sid.lower()}{tag}-{self.run}-"

    @staticmethod
    def text(rid: str, words: str = "hello") -> str:
        """Prompt text carrying the probe marker."""
        return f"rid={rid} {words}"

    def metric(self, name: str, value: Any) -> None:
        """Record a number for the report."""
        self.metrics[name] = value

    def expect(self, condition: bool, message: str) -> None:
        """Fail the scenario with `message` unless `condition` holds."""
        if not condition:
            raise ScenarioFailed(message)

    def config(self, **overrides: Any) -> QueueConfig:
        """Harness config with overrides."""
        return api.build_config(self.settings, **overrides)

    def on_cleanup(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Run `fn` after the scenario (even when it fails)."""
        self._cleanups.append(fn)

    async def workers(
        self,
        n: int,
        config: QueueConfig | None = None,
        *,
        names: Iterable[str] | None = None,
        log_level: str = "WARNING",
        drain_secs: float | None = None,
    ) -> list[WorkerProc]:
        """Start `n` worker processes and wait until each is receiving."""
        cfg = config or self.base
        wanted = (
            list(names)
            if names is not None
            else [f"w{len(self._workers) + i + 1}" for i in range(n)]
        )
        procs = [
            WorkerProc(
                self.settings, cfg, name, self.dir, log_level=log_level, drain_secs=drain_secs
            )
            for name in wanted
        ]
        for proc in procs:
            proc.start()
            self._workers.append(proc)
        await asyncio.gather(*(p.wait_ready() for p in procs))
        return procs

    async def queue(
        self,
        config: QueueConfig | None = None,
        *,
        mode: str = "send",
        pod_id: str | None = None,
        metrics: RecordingSink | None = None,
        provider_factories: Any = None,
    ) -> LLMQueue:
        """Start an in-process `LLMQueue` (`send` by default); stopped after the scenario."""
        q = api.new_queue(
            config or self.base,
            pod_id or f"{self.sid.lower()}-{mode}-{len(self._queues)}",
            metrics=metrics,
            provider_factories=provider_factories,
        )
        await q.start(mode=mode)  # type: ignore[arg-type]
        self._queues.append(q)
        return q

    async def ensure_queues(self, config: QueueConfig | None = None, **kw: Any) -> list[str]:
        """Create the config's queues (with DLQs) on the harness SQS."""
        names = api.all_queue_names(config or self.base)
        await stack.ensure_queues(self.settings, names, **kw)
        return names

    async def final_checks(
        self,
        config: QueueConfig | None = None,
        *,
        settle_secs: float = 20.0,
        allow_running: int = 0,
        lease_wait_secs: float = 0.0,
    ) -> None:
        """No DLQ messages, nothing left visible/in flight, no live leases or running claims."""
        names = api.all_queue_names(config or self.base)
        counts = await stack.settle_queues(self.settings, names, timeout_secs=settle_secs)
        dlq = stack.dlq_total(counts)
        stuck = sum(c["visible"] + c["inflight"] for q, c in counts.items() if q in names)
        leaks = await stack.leak_report(self.settings)
        waited = 0.0
        while leaks["live_leases"] and waited < lease_wait_secs:  # a killed pod's leases expire
            await asyncio.sleep(1.0)
            waited += 1.0
            leaks = await stack.leak_report(self.settings)
        self.metric("dlq", dlq)
        self.metric("left_in_queues", stuck)
        self.metric("live_leases", leaks["live_leases"])
        self.metric("running_claims", leaks["running_claims"])
        self.expect(dlq == 0, f"{dlq} messages in DLQs: {counts}")
        self.expect(stuck == 0, f"{stuck} messages still visible/in flight: {counts}")
        self.expect(leaks["live_leases"] == 0, f"leaked limiter leases: {leaks}")
        self.expect(leaks["running_claims"] <= allow_running, f"claims left running: {leaks}")

    async def stop_all(self) -> None:
        """Stop senders and workers (graceful, then hard)."""
        for q in self._queues:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(q.stop(drain_secs=2), 30)
        self._queues.clear()
        await asyncio.gather(*(w.stop(30) for w in self._workers), return_exceptions=True)

    async def close(self) -> None:
        """Cleanup callbacks, then every queue and worker."""
        for fn in reversed(self._cleanups):
            with contextlib.suppress(Exception):
                await fn()
        await self.stop_all()


async def services_up(settings: Settings) -> bool:
    """True when Valkey answers PING and SQS answers ListQueues."""
    r = stack.redis_client(settings)
    try:
        await asyncio.wait_for(r.ping(), 3)
        await asyncio.wait_for(stack.queue_counts(settings, [settings.queue("gpt20b")]), 5)
    except Exception:
        return False
    finally:
        with contextlib.suppress(Exception):
            await r.aclose()
    return True


async def isolate(settings: Settings) -> None:
    """Before every scenario: services up, queues purged, prefix flushed, fakellm reset."""
    if settings.chaos and not await services_up(settings):
        for service in (settings.valkey_service, settings.sqs_service):
            await asyncio.to_thread(
                stack.compose, "--profile", "scn", "unpause", service, check=False
            )
            await stack.start_service(service)
    base = api.build_config(settings)
    names = api.all_queue_names(base)
    await stack.ensure_queues(settings, names)
    await stack.purge(settings, names)
    await stack.flush_prefix(settings)
    fake = stack.FakeLLM(settings.fakellm_url)
    await fake.load(BASE_FAKELLM)


async def run_one(spec: Scenario, settings: Settings, run_dir: Path) -> Result:
    """Isolate, run and classify one scenario."""
    t0 = time.monotonic()
    if spec.skip:
        return Result(spec.id, spec.title, "skip", 0.0, message=spec.skip)
    if spec.chaos and not settings.chaos:
        return Result(spec.id, spec.title, "skip", 0.0, message="chaos disabled (SCN_CHAOS=0)")
    ctx = Ctx(settings, run_dir, spec.id)
    status, message = "pass", ""
    try:
        await isolate(settings)
        await spec.fn(ctx)
    except ScenarioSkipped as exc:
        status, message = "skip", str(exc)
    except ScenarioFailed as exc:
        status, message = "fail", str(exc)
    except Exception as exc:
        status = "error"
        message = f"{type(exc).__name__}: {exc}"
        (ctx.dir / "traceback.txt").write_text(traceback.format_exc(), encoding="utf-8")
    finally:
        await ctx.close()
    from tools.scenarios.common import log_events

    events = log_events(sorted(ctx.dir.glob("w*.log")))
    if events:
        ctx.metrics["worker_log_events"] = events
    if spec.xfail:
        if status in ("fail", "error"):
            status, message = "xfail", f"{spec.xfail} | observed: {message}"
        elif status == "pass":
            status, message = "xpass", f"expected bug not seen: {spec.xfail}"
    return Result(
        spec.id, spec.title, status, round(time.monotonic() - t0, 1), ctx.metrics, message
    )


async def run_many(
    specs: Sequence[Scenario], settings: Settings, run_dir: Path, *, echo: bool = True
) -> list[Result]:
    """Run scenarios in order with a managed fakellm probe; prints a line per scenario."""
    ensure_aws_env()
    run_dir.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240  # small local file, startup/teardown only
    fake_proc: FakellmProc | None = None
    if settings.manage_fakellm and not await stack.FakeLLM(settings.fakellm_url).health():
        fake_proc = FakellmProc(settings, run_dir)
        fake_proc.start()
        for _ in range(100):
            if await stack.FakeLLM(settings.fakellm_url).health():
                break
            await asyncio.sleep(0.2)
    results: list[Result] = []
    try:
        for spec in specs:
            if echo:
                print(f"--> {spec.id} {spec.title}", flush=True)
            result = await run_one(spec, settings, run_dir)
            results.append(result)
            if echo:
                print(f"    {result.status.upper()} {result.secs}s {short(result)}", flush=True)
    finally:
        if fake_proc is not None:
            fake_proc.stop()
    return results


def short(result: Result, width: int = 110) -> str:
    """One-line metric summary (or the failure message)."""
    if result.status in ("fail", "error", "xfail", "skip", "xpass"):
        text = result.message
    else:
        text = ", ".join(f"{k}={v}" for k, v in result.metrics.items() if not isinstance(v, dict))
    text = text.replace("\n", " ")
    return text if len(text) <= width else text[: width - 3] + "..."


def table(results: Sequence[Result]) -> str:
    """Fixed-width pass/fail table."""
    lines = [f"{'ID':6s} {'STATUS':6s} {'SECS':>7s}  TITLE / KEY METRICS", "-" * 120]
    for r in results:
        lines.append(f"{r.id:6s} {r.status.upper():6s} {r.secs:7.1f}  {r.title}")
        lines.append(f"{'':22s}{short(r)}")
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    lines.append("-" * 120)
    lines.append("  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return "\n".join(lines)


def default_run_dir() -> Path:
    """`$SCN_RUN_DIR` or `<system temp>/llmq-scenario-runs/<timestamp>` (logs, stats files)."""
    import os
    import tempfile

    base = os.environ.get("SCN_RUN_DIR") or str(Path(tempfile.gettempdir()) / "llmq-scenario-runs")
    return Path(base) / time.strftime("%Y%m%d-%H%M%S")
