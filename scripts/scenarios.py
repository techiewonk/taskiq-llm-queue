"""Scenarios: one fault (or a chosen pair) at a time, measured by calls, each with a pass rule.

Every scenario runs three phases of calls against worker processes and fakellm, never real
Bedrock: `before` (healthy), `during` (the fault is on, and calls keep coming until it ends),
`after` (the fault is gone and the system has settled). A phase is a number of calls, not a
time, so a scenario takes as long as its fault: 10-40 s with the fast profile.

Each run is one scenario x one load (`under` 50 % of the budget, `at` 100 %, `burst` 300 %).
Common pass rule: `before` and `after` calls all succeed (a burst may refuse calls on time,
as `rate_limited` or `deadline_exceeded`, in any phase); `during` fails only with the kinds
the scenario allows; no answer arrives after its deadline; nothing is left in progress at the
end. Some scenarios add checks (the provider never called again, one run for two callers ...).

    python scripts/scenarios.py --list
    python scripts/scenarios.py S4 P1            # these, under every load
    python scripts/scenarios.py S4 --load at     # one load
    python scripts/scenarios.py --group store    # store worker provider registry caller pairs
    python scripts/scenarios.py all              # every scenario and pair, every load
    python scripts/scenarios.py S4 --profile prod  # production timers (slower)
    python scripts/scenarios.py all --jobs 4     # 4 runs at a time (default; 1: one by one)

In parallel, each run is its own process with its own fakellm and store-proxy ports and its
own log (`reports/scenarios-<time>/<id>-<load>.log`, worker logs beside it). Runs that change
something shared run alone, after the others: S9 (the store's memory limit) and the Docker
failover drills (fixed ports).

Profiles: `fast` (default) shortens the timers so faults resolve in seconds (lease 3 s,
visibility 2 s, caller TTL 5 s, deadline 10 s, attempt 3 s, wait 8 s); `prod` keeps the
production values. Each run's worker and fakellm logs go to `reports/` (see gates_fleet).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import json
import os
import random
import subprocess
import sys
import time
import urllib.request
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import failover  # needs the sys.path entry
from gates import Gate, Result, Run, chat, registry
from gates_fleet import CHAT_ID, FAKE_PORT, PY, log_dir, setup_logging

from taskiq_llm_queue import CallFailed, LLMQueue, StoreUnavailable

PROXY_PORT = int(os.environ.get("LLMQ_SCENARIO_PROXY_PORT", "6389"))  # one per parallel run
ROOT = Path(__file__).resolve().parent.parent
REGIONS = ("us-east-1", "us-east-2")  # gates_fleet.registry's first two
RPM = 600  # per deployment: 2 deployments, 20 chat calls a second of budget
PHASE_CALLS = {"before": 30, "during": 60, "after": 30}
LOADS = {"under": 0.5, "at": 1.0, "burst": 3.0}


@dataclasses.dataclass(frozen=True)
class Profile:
    """Timers for one profile: process settings (env) and registry defaults."""

    env: dict[str, str]
    defaults: dict[str, Any]
    fault_secs: float  # how long a timed fault stays on
    deadline_secs: float  # timeout_secs, for the "no answer after its deadline" check


PROFILES = {
    "fast": Profile(
        env={
            "LLMQ_LEASE_SECS": "3",
            "LLMQ_CALLER_TTL_SECS": "10",  # 5 s: one stray caller_gone at 300 % (busy driver?)
            "LLMQ_STATUS_TTL_SECS": "30",
            "LLMQ_CFG_RELOAD_SECS": "1",
            "LLMQ_DRAIN_SECS": "5",
        },
        defaults={
            "timeout_secs": 10,
            "attempt_timeout_secs": 3,
            "max_wait_secs": 8,
            "queue": {"visibility_secs": 2},
            # Recovery in seconds, not minutes: an open breaker or a disabled deployment is
            # tried again soon after the fault ends (prod: 15 s and 15 min).
            "breaker_cooldown_secs": 3,
            "health": {"probe_after_secs": 5, "disable_window_secs": 30},
        },
        fault_secs=5,
        deadline_secs=10,
    ),
    "prod": Profile(
        env={},
        defaults={"timeout_secs": 30, "attempt_timeout_secs": 10, "max_wait_secs": 20},
        fault_secs=20,
        deadline_secs=30,
    ),
}
CALLER_MARGIN_SECS = 5.0  # EngineSettings.caller_margin_secs


# ------------------------------------------------------------------------------ the store


class FaultProxy(failover.Proxy):  # type: ignore[misc]  # failover is a script module
    """The store as the processes see it, in front of the stack's Valkey.

    It can refuse new connections (`refuse`), drop open ones (`drop_all`), swallow traffic
    without closing anything (`freeze`: a half-open connection) or add latency (`delay_secs`).
    """

    def __init__(self) -> None:
        """Forwarding normally."""
        super().__init__()
        self.freeze = False
        self.delay_secs = 0.0

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                if self.freeze:
                    continue  # swallowed: the sender waits for a reply that never comes
                if self.delay_secs:
                    await asyncio.sleep(self.delay_secs)
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(self, client_r: Any, client_w: Any) -> None:
        """Forward one connection to the stack's Valkey (or refuse it)."""
        if self.refuse:
            client_w.close()
            return
        try:
            node_r, node_w = await asyncio.open_connection("127.0.0.1", 6379)
        except OSError:
            client_w.close()
            return
        self.open += [client_w, node_w]
        await asyncio.gather(self._pipe(client_r, node_w), self._pipe(node_r, client_w))


async def serve(proxy: FaultProxy) -> None:
    """Keep the proxy listening, re-opening it if asyncio closed it.

    On Windows a failed accept (WinError 64: a client gave up mid-accept while the store was
    blocked) makes asyncio close the listening socket for good, and the store then looked
    down forever (scenario X1, a harness fault).
    """
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", PROXY_PORT)
    try:
        while True:
            await asyncio.sleep(0.2)
            if all(sock.fileno() == -1 for sock in server.sockets):
                server.close()
                server = await asyncio.start_server(proxy.handle, "127.0.0.1", PROXY_PORT)
    finally:
        server.close()


# ------------------------------------------------------------------------------ one run


@dataclasses.dataclass
class Ctx:
    """What a scenario's fault can touch: the processes, the store proxy, fakellm, callers."""

    gate: Gate
    proxy: FaultProxy
    queues: list[LLMQueue]
    profile: Profile
    run: Run
    notes: list[str] = dataclasses.field(default_factory=list)
    checks: dict[str, bool] = dataclasses.field(default_factory=dict)

    def fake(self, method: str, path: str, body: Any = None) -> Any:
        """Call fakellm's admin API (blocking: run it in a thread)."""
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{FAKE_PORT}{path}", data=data, method=method
        )
        request.add_header("content-type", "application/json")
        with urllib.request.urlopen(request, timeout=10) as reply:
            return json.loads(reply.read() or b"null")

    async def provider(self, region: str, **behaviour: Any) -> None:
        """Change the fake provider's behaviour in one region (`{}` keys reset it)."""
        await asyncio.to_thread(self.fake, "PATCH", f"/admin/models/{CHAT_ID}@{region}", behaviour)

    async def provider_calls(self, region: str) -> int:
        """Calls fakellm received in one region so far."""
        stats = await asyncio.to_thread(self.fake, "GET", "/admin/stats")
        entry = stats.get(f"{CHAT_ID}@{region}") or {}
        return int(entry.get("calls", entry.get("requests", 0)) or 0)

    async def edit(self, values: dict[str, Any], *, model: str | None = "chat") -> None:
        """A live registry edit."""
        # During a store outage the edit fails with StoreUnavailable; an operator would retry.
        for _ in range(60):
            try:
                await self.queues[0].registry.set(values, model=model, actor="scenario")
                return
            except StoreUnavailable:
                await asyncio.sleep(0.5)
        raise RuntimeError("the registry edit never reached the store")


@dataclasses.dataclass(frozen=True)
class Scenario:
    """One fault: what it does, which failures `during` may show, and its own checks."""

    sid: str
    group: str
    title: str
    rule: str
    fault: Callable[[Ctx], Awaitable[None]]  # turns the fault on, waits, turns it off
    allowed: frozenset[str] = frozenset()  # failure kinds `during` may show
    settle_secs: float = 0.0  # after the fault, before `after` starts
    extra: Callable[[Ctx], Awaitable[None]] | None = None  # scenario checks, after `after`


async def phase(
    ctx: Ctx, name: str, rate: float, calls: int, until: asyncio.Event | None
) -> list[Result]:
    """Send `calls` calls at `rate` a second (and more while `until` is not set)."""
    tasks: list[asyncio.Task[Result]] = []
    t0 = time.monotonic()
    i = 0
    while i < calls or (until is not None and not until.is_set()):
        llmq = ctx.queues[i % len(ctx.queues)]
        tasks.append(asyncio.create_task(ctx.run.call(llmq, "chat", chat(), priority=i % 5 == 0)))
        i += 1
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
    return list(await asyncio.gather(*tasks))


async def run_one(scenario: Scenario, load: str, profile_name: str) -> bool:
    """Run one scenario under one load; print its numbers and verdict."""
    profile = PROFILES[profile_name]
    doc = registry(
        {"chat": {"rpm": RPM, "worker_slots": 50}},
        accounts=1,
        regions=len(REGIONS),
        defaults=profile.defaults,
    )
    proxy = FaultProxy()
    serving = asyncio.create_task(serve(proxy))
    scenario_doc = {
        "defaults": {"latency_ms": {"p50": 150, "p95": 250}},
        "models": {f"{CHAT_ID}@{r}": {} for r in REGIONS},
    }
    rate = LOADS[load] * RPM / 60 * len(REGIONS)
    started = time.monotonic()
    phases: dict[str, list[Result]] = {}
    try:
        store = f"redis://:localdev@127.0.0.1:{PROXY_PORT}/0"
        async with Gate(doc, scenario_doc, LLMQ_STORE_URL=store, **profile.env) as gate:
            await gate.workers(3)
            queues = await gate.callers(2)
            ctx = Ctx(gate, proxy, queues, profile, Run())
            phases["before"] = await phase(ctx, "before", rate, PHASE_CALLS["before"], None)
            ended = asyncio.Event()

            async def fault() -> None:
                try:
                    await scenario.fault(ctx)
                finally:
                    ended.set()

            during = asyncio.create_task(phase(ctx, "during", rate, PHASE_CALLS["during"], ended))
            await fault()
            phases["during"] = await during
            await asyncio.sleep(scenario.settle_secs)
            phases["after"] = await phase(ctx, "after", rate, PHASE_CALLS["after"], None)
            if scenario.extra is not None:
                await scenario.extra(ctx)
            # Nothing left in progress: a killed worker's lease ends and the sweep clears it.
            key = f"{gate.prefix}:{{m:chat}}:inprog"
            for _ in range(60):
                inprog = gate.store.client.zcard(key)
                if not inprog:
                    break
                await asyncio.sleep(0.5)
    finally:
        serving.cancel()
    secs = time.monotonic() - started
    # Three times the budget: refusing on time (no budget, or no time left) is the right answer.
    allow_everywhere = {"rate_limited", "deadline_exceeded"} if load == "burst" else set()
    bad = {
        name: collections.Counter(
            r.kind
            for r in results
            if not r.ok
            and r.kind not in allow_everywhere
            and not (name == "during" and r.kind in scenario.allowed)
        )
        for name, results in phases.items()
    }
    limit = profile.deadline_secs + CALLER_MARGIN_SECS + 1
    late = [r for results in phases.values() for r in results if r.total_secs > limit]
    checks = {
        "before: all served": not bad["before"],
        f"during: failures only {sorted(scenario.allowed | allow_everywhere) or 'none'}": not bad[
            "during"
        ],
        "after: all served": not bad["after"],
        "no answer after its deadline": not late,
        "nothing in progress at the end": not inprog,
        **ctx.checks,
    }
    counts = {
        name: f"{sum(r.ok for r in results)}/{len(results)} ok"
        + (
            f" {dict(collections.Counter(r.kind for r in results if not r.ok))}"
            if any(not r.ok for r in results)
            else ""
        )
        for name, results in phases.items()
    }
    passed = all(checks.values())
    print(
        f"[{scenario.sid} {load}] {scenario.title} ({secs:.0f}s): "
        + "; ".join(f"{k} {v}" for k, v in counts.items()),
        flush=True,
    )
    for note in ctx.notes:
        print(f"    {note}")
    for name, ok in checks.items():
        if not ok:
            print(
                f"    FAIL {name}"
                + (f" {dict(bad[name.split(':')[0]])}" if name.split(":")[0] in bad else "")
            )
    if late:
        print(f"    late: {[round(r.total_secs, 1) for r in late[:5]]}")
    for name, results in phases.items():  # when each unexpected failure was sent, and its time
        shown = [r for r in results if not r.ok and r.kind in bad[name]][:5]
        for r in shown:
            print(f"    {name}: {r.kind} sent at {r.sent:.1f}s, answered after {r.total_secs:.1f}s")
    print(f"{'PASS' if passed else 'FAIL'}  {scenario.sid} {load}", flush=True)
    return passed


# ------------------------------------------------------------------------------ faults


async def _hold(ctx: Ctx) -> None:
    await asyncio.sleep(ctx.profile.fault_secs)


def _live(ctx: Ctx) -> list[Any]:
    return [w for w in ctx.gate.fleet if w.proc.poll() is None]


async def worker_kill(ctx: Ctx) -> None:
    """Kill -9 one worker mid-traffic; start a replacement."""
    await asyncio.sleep(1)
    random.choice(_live(ctx)).kill()
    await ctx.gate.workers(1)
    await _hold(ctx)


async def worker_restart(ctx: Ctx) -> None:
    """Graceful: a new worker first, then the old one drains and stops."""
    await asyncio.sleep(1)
    victim = random.choice(_live(ctx))
    await ctx.gate.workers(1)
    await victim.stop()


async def workers_all_down(ctx: Ctx) -> None:
    """Every worker killed; new ones only after the fault time."""
    await asyncio.sleep(1)
    for worker in _live(ctx):
        worker.kill()
    await _hold(ctx)
    await ctx.gate.workers(3)


async def store_block(ctx: Ctx) -> None:
    """Every store connection dropped, new ones refused."""
    ctx.proxy.refuse = True
    ctx.proxy.drop_all()
    await _hold(ctx)
    ctx.proxy.refuse = False


async def store_half_open(ctx: Ctx) -> None:
    """Connections stay open but nothing comes back (a blackholed node)."""
    ctx.proxy.freeze = True
    await _hold(ctx)
    ctx.proxy.freeze = False


async def store_slow(ctx: Ctx) -> None:
    """Every store packet 50 ms late."""
    ctx.proxy.delay_secs = 0.05
    await _hold(ctx)
    ctx.proxy.delay_secs = 0.0


async def store_reset(ctx: Ctx) -> None:
    """Every key of this run deleted (a flushed or replaced store)."""
    await asyncio.sleep(1)
    await asyncio.to_thread(ctx.gate.store.cleanup, ctx.gate.prefix)
    await _hold(ctx)


async def store_oom(ctx: Ctx) -> None:
    """The store refuses writes (maxmemory reached, noeviction)."""
    client = ctx.gate.store.client
    before = client.config_get("maxmemory")["maxmemory"]
    policy = client.config_get("maxmemory-policy")["maxmemory-policy"]
    used = int(client.info("memory")["used_memory"])
    client.config_set("maxmemory-policy", "noeviction")
    client.config_set("maxmemory", str(max(1, used - 1_000_000)))
    try:
        await _hold(ctx)
    finally:
        client.config_set("maxmemory", before)
        client.config_set("maxmemory-policy", policy)


def _provider_fault(**behaviour: Any) -> Callable[[Ctx], Awaitable[None]]:
    async def fault(ctx: Ctx) -> None:
        await ctx.provider(REGIONS[0], **behaviour)
        await _hold(ctx)
        reset = dict.fromkeys(behaviour)
        await ctx.provider(REGIONS[0], **reset)

    return fault


async def provider_both_down(ctx: Ctx) -> None:
    """Every region of the model failing (5xx)."""
    for region in REGIONS:
        await ctx.provider(region, force_error="InternalServerException")
    await _hold(ctx)
    for region in REGIONS:
        await ctx.provider(region, force_error=None)


async def rpm_cut(ctx: Ctx) -> None:
    """The budget cut to a quarter, live, then put back."""
    await ctx.edit({"rpm": RPM // 4})
    await _hold(ctx)
    await ctx.edit({"rpm": RPM})


async def deployment_off(ctx: Ctx) -> None:
    """One region disabled live, then enabled."""
    path = f"deployments.aws-0.overrides.{REGIONS[0]}.enabled"
    await ctx.edit({path: False})
    await _hold(ctx)
    await ctx.edit({path: True})


async def kill_switch(ctx: Ctx) -> None:
    """The kill switch: every call runs direct (no queue), same budgets."""
    await ctx.edit({"routing.kill_switch": True}, model=None)
    await _hold(ctx)
    await ctx.edit({"routing.kill_switch": False}, model=None)


async def bad_edit(ctx: Ctx) -> None:
    """An invalid live edit is refused; the version in use does not change."""
    version = ctx.queues[0].registry.current.registry.version
    try:
        await ctx.edit({"rpm": -5})
        ctx.checks["bad edit refused"] = False
    except Exception:  # the refusal
        ctx.checks["bad edit refused"] = True
    await asyncio.sleep(1)
    ctx.checks["version unchanged"] = ctx.queues[0].registry.current.registry.version == version
    await _hold(ctx)


async def caller_cancel(ctx: Ctx) -> None:
    """Half of 40 slow calls cancelled by their caller; the rest served."""
    await ctx.provider(REGIONS[0], latency_ms={"p50": 2000, "p95": 2000})
    await ctx.provider(REGIONS[1], latency_ms={"p50": 2000, "p95": 2000})
    tasks = [asyncio.create_task(ctx.queues[0].converse("chat", chat())) for _ in range(40)]
    await asyncio.sleep(0.5)
    for task in tasks[:20]:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Cancelling some must not cancel the others: each kept call is served, or (under a
    # burst, competing with three times the budget) refused on time, never `cancelled`.
    on_time = {"rate_limited", "deadline_exceeded"}
    kept_ok = all(
        not isinstance(r, BaseException) or (isinstance(r, CallFailed) and r.kind in on_time)
        for r in results[20:]
    )
    ctx.checks["uncancelled calls served or refused on time"] = kept_ok
    for region in REGIONS:
        await ctx.provider(region, latency_ms=None)


async def caller_dies(ctx: Ctx) -> None:
    """A caller process with 100 calls waiting is killed; its calls must not stay held."""
    proc = subprocess.Popen(  # noqa: ASYNC220  # one process, started once
        [PY, str(Path(__file__).parent / "gates_fleet.py"), "caller", "100", "chat"],
        env={**ctx.gate.env, "LLMQ_MODE": "caller"},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    await asyncio.to_thread(proc.stdout.readline)
    proc.stdin.write(b"go\n")
    proc.stdin.flush()
    await asyncio.sleep(1)
    proc.kill()
    await _hold(ctx)


async def duplicate_id(ctx: Ctx) -> None:
    """Two callers, one request id, at once: one provider call, the same reply for both."""
    before = sum([await ctx.provider_calls(r) for r in REGIONS])
    rid = f"dup-{random.randrange(10**9)}"
    a, b = await asyncio.gather(
        ctx.queues[0].converse("chat", chat(), request_id=rid),
        ctx.queues[1].converse("chat", chat(), request_id=rid),
        return_exceptions=True,
    )
    after = sum([await ctx.provider_calls(r) for r in REGIONS])
    same = (
        not isinstance(a, BaseException)
        and not isinstance(b, BaseException)
        and a.response == b.response
    )
    ctx.checks["both callers got the same reply"] = same
    ctx.notes.append(f"provider calls for the shared id: {after - before} (other traffic included)")
    await _hold(ctx)


def pair(
    first: Callable[[Ctx], Awaitable[None]], second: Callable[[Ctx], Awaitable[None]]
) -> Callable[[Ctx], Awaitable[None]]:
    """Two faults at once."""

    async def both(ctx: Ctx) -> None:
        await asyncio.gather(first(ctx), second(ctx))

    return both


def _s(
    sid: str,
    group: str,
    title: str,
    rule: str,
    fault: Callable[[Ctx], Awaitable[None]],
    *allowed: str,
    settle: float = 0.0,
    extra: Any = None,
) -> Scenario:
    return Scenario(sid, group, title, rule, fault, frozenset(allowed), settle, extra)


ON_TIME = ("deadline_exceeded", "queue_unavailable", "rate_limited")
SCENARIOS = [
    _s(
        "S1",
        "worker",
        "worker killed (kill -9)",
        "its calls finish elsewhere; nothing held",
        worker_kill,
    ),
    _s("S2", "worker", "worker restarted (graceful)", "nothing lost", worker_restart),
    _s(
        "S3",
        "worker",
        "every worker down, then back",
        "calls wait, then served or refused on time",
        workers_all_down,
        *ON_TIME,
        settle=2,
    ),
    _s(
        "S4",
        "store",
        "store blocked",
        "refused on time while down; served after",
        store_block,
        *ON_TIME,
        settle=2,
    ),
    _s(
        "S5",
        "store",
        "store half-open (no replies)",
        "fails within the read timeout; nothing stuck",
        store_half_open,
        *ON_TIME,
        settle=17,
    ),
    _s(
        "S8",
        "store",
        "store reset (keys gone)",
        "running calls re-registered; nothing stuck",
        store_reset,
        *ON_TIME,
        settle=3,
    ),
    _s(
        "S9",
        "store",
        "store out of memory",
        "refused as queue_unavailable; no crash",
        store_oom,
        *ON_TIME,
        settle=2,
    ),
    _s("S10", "store", "store slow (+50 ms a packet)", "all served", store_slow),
    _s(
        "P1",
        "provider",
        "region throttled (429)",
        "other region serves",
        _provider_fault(force_error="ThrottlingException"),
    ),
    _s(
        "P2",
        "provider",
        "region failing (5xx)",
        "other region serves; breaker opens and recovers",
        _provider_fault(force_error="InternalServerException"),
    ),
    _s(
        "P3", "provider", "region hangs", "attempt timeout, next region", _provider_fault(hang=True)
    ),
    _s(
        "P4",
        "provider",
        "model not in region",
        "target error: other region serves",
        _provider_fault(force_error="ResourceNotFoundException"),
    ),
    _s(
        "P5",
        "provider",
        "bad credentials in a region",
        "other region serves",
        _provider_fault(force_error="UnrecognizedClientException"),
    ),
    _s(
        "P7",
        "provider",
        "every region failing (5xx)",
        "refused on time; served after",
        provider_both_down,
        "provider_error",
        "model_unavailable",
        "deadline_exceeded",
        "rate_limited",
        "retry_budget_exhausted",  # every region failing: the budget stops a retry storm
        # Breaker cooldown 3 s, health probe 5 s; a probe made while the fault was still on
        # backs off to 10 s, so recovery takes up to ~15 s (fast profile).
        settle=16,
    ),
    _s(
        "R1",
        "registry",
        "rpm cut to a quarter, live",
        "over-budget calls rate_limited",
        rpm_cut,
        "rate_limited",
        "deadline_exceeded",
    ),
    _s("R2", "registry", "region disabled, live", "other region serves", deployment_off),
    _s("R3", "registry", "kill switch on", "direct path, same budgets", kill_switch),
    _s("R4", "registry", "bad live edit", "refused; version unchanged", bad_edit),
    _s("C1", "caller", "callers cancel", "cancelled stop; others served", caller_cancel),
    _s("C2", "caller", "caller process dies", "its calls dropped; nothing held", caller_dies),
    _s("C3", "caller", "duplicate request id", "one run; same reply for both", duplicate_id),
    _s(
        "X1",
        "pairs",
        "store blocked + worker killed",
        "as S4",
        pair(store_block, worker_kill),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X2",
        "pairs",
        "store blocked + rpm cut",
        "as S4 and R1",
        pair(store_block, rpm_cut),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X3",
        "pairs",
        "worker killed + region throttled",
        "as S1 and P1",
        pair(worker_kill, _provider_fault(force_error="ThrottlingException")),
    ),
    _s(
        "X4",
        "pairs",
        "store slow + region failing",
        "as S10 and P2",
        pair(store_slow, _provider_fault(force_error="InternalServerException")),
    ),
    _s(
        "X5",
        "pairs",
        "store half-open + worker killed",
        "as S5 and S1",
        pair(store_half_open, worker_kill),
        *ON_TIME,
        settle=17,
    ),
    _s(
        "X6",
        "pairs",
        "store reset + worker killed",
        "as S8 and S1",
        pair(store_reset, worker_kill),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X7",
        "pairs",
        "rpm cut + region throttled",
        "as R1 and P1",
        pair(rpm_cut, _provider_fault(force_error="ThrottlingException")),
        "rate_limited",
        "deadline_exceeded",
    ),
    _s(
        "X8",
        "pairs",
        "region disabled + other region hangs",
        "refused on time; served after",
        pair(deployment_off, _provider_fault(hang=True)),
        *ON_TIME,
        "model_unavailable",
        "provider_error",
        settle=3,
    ),
]
BY_ID = {s.sid: s for s in SCENARIOS}
ALONE = {"S9"}  # changes the shared store's memory limit: never next to another run
# Store failover on two Docker nodes: run by failover.py's drill (its own pass marks).
FAILOVER = {"S6": "failover", "S7": "slowdns", "S11": "restart"}


async def parallel(units: list[tuple[str, str]], profile: str, jobs: int) -> dict[str, bool]:
    """Run each (scenario, load) as its own process, `jobs` at a time, on its own ports."""
    folder = ROOT / "reports" / f"scenarios-{time.strftime('%Y%m%d-%H%M%S')}"
    folder.mkdir(parents=True, exist_ok=True)
    print(f"  logs: {folder}", flush=True)
    slots: asyncio.Queue[int] = asyncio.Queue()
    for slot in range(jobs):
        slots.put_nowait(slot)
    results: dict[str, bool] = {}

    async def one(sid: str, load: str) -> None:
        slot = await slots.get()
        name = f"{sid}-{load}"
        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "LLMQ_FAKE_PORT": str(8100 + slot),
            "LLMQ_SCENARIO_PROXY_PORT": str(6400 + slot),
            "LLMQ_DRILL_LOG_DIR": str(folder / f"{name}-processes"),
        }
        cmd = [PY, __file__, sid, "--load", load, "--profile", profile, "--jobs", "1"]
        try:
            with (folder / f"{name}.log").open("w", encoding="utf-8") as log:
                proc = await asyncio.create_subprocess_exec(*cmd, env=env, stdout=log, stderr=log)
                await proc.wait()
            results[f"{sid} {load}"] = proc.returncode == 0
            text = (folder / f"{name}.log").read_text(encoding="utf-8", errors="replace")
            lines = [
                line
                for line in text.splitlines()
                if line.startswith(("[", "PASS", "FAIL", "    "))
                and not line.startswith("    logs")
            ]
            print("\n".join(lines), flush=True)
        finally:
            slots.put_nowait(slot)

    await asyncio.gather(*(one(sid, load) for sid, load in units))
    return results


def main() -> int:
    """Run the chosen scenarios under the chosen loads; exit 1 if any fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("ids", nargs="*", help="scenario ids (S4 P1 ...) or all")
    parser.add_argument("--group", choices=sorted({s.group for s in SCENARIOS} | {"failover"}))
    parser.add_argument("--load", choices=sorted(LOADS), action="append")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="fast")
    parser.add_argument("--jobs", type=int, default=4, help="runs at a time (1: one by one)")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        for s in SCENARIOS:
            print(f"{s.sid:4} {s.group:9} {s.title}: {s.rule}")
        for sid, name in FAILOVER.items():
            print(f"{sid:4} failover  Docker failover drill `{name}` (failover.py)")
        return 0
    setup_logging()
    ids = list(BY_ID) + list(FAILOVER) if args.ids == ["all"] else list(args.ids)
    if args.group == "failover":
        ids += list(FAILOVER)
    elif args.group:
        ids += [s.sid for s in SCENARIOS if s.group == args.group]
    unknown = [i for i in ids if i not in BY_ID and i not in FAILOVER]
    if unknown or not ids:
        parser.error(f"unknown or no scenario: {unknown}; see --list")
    ids = list(dict.fromkeys(ids))
    loads = args.load or list(LOADS)
    started = time.monotonic()
    results: dict[str, bool] = {}
    shared = [(sid, load) for sid in ids if sid in BY_ID and sid not in ALONE for load in loads]
    alone = [(sid, load) for sid in ids if sid in ALONE for load in loads]
    if args.jobs > 1 and len(shared) > 1:
        results.update(asyncio.run(parallel(shared, args.profile, args.jobs)))
    else:
        alone = shared + alone
    if alone:
        print(f"  logs of worker processes: {log_dir()}", flush=True)
    for sid, load in alone:
        results[f"{sid} {load}"] = asyncio.run(run_one(BY_ID[sid], load, args.profile))
    for sid in [i for i in ids if i in FAILOVER]:

        async def drill(name: str = FAILOVER[sid]) -> bool:
            return bool(await failover.drill(name))

        failover.nodes_up()
        try:
            results[sid] = asyncio.run(drill())
        finally:
            failover.nodes_down()
    failed = [k for k, ok in results.items() if not ok]
    print(
        f"{len(results) - len(failed)}/{len(results)} passed in {time.monotonic() - started:.0f}s"
    )
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
