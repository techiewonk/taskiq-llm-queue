"""The plan's load and chaos gates (§11: B17, B19, B22, B23, B33, B34, B56, B91-B97) on fakellm.

Workers are real processes (pods) against one store, driven from here. Rates and durations are
scaled to run on one machine in minutes; each gate prints its numbers and the plan's pass mark
it applies. Nothing calls a real LLM.

    python scripts/gates.py                # every gate (about 20 minutes)
    python scripts/gates.py B17 B95        # some
    python scripts/gates.py --quick        # all but the slow calibration gates (about 14 minutes)
    python scripts/gates.py --list

`--quick` leaves out B17 (fixed windows over whole minutes), B91 (20 models x 30 deployments),
and B92 (3 minutes idle); run every gate before a release.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import itertools
import json
import random
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gates_fleet import (  # needs the sys.path entry
    EMBED_ID,
    PY,
    TITAN_ID,
    FakeLLM,
    Store,
    Worker,
    environment,
    log_dir,
    new_prefix,
    registry,
    setup_logging,
    stop_all,
    workers,
)

from taskiq_llm_queue import CallFailed, LLMQueue

FAST = {"defaults": {"latency_ms": {"p50": 150, "p95": 250}}}


def chat(text: str = "hi", *, tools: bool = False) -> dict[str, Any]:
    """A Converse request."""
    request: dict[str, Any] = {
        "messages": [{"role": "user", "content": [{"text": text}]}],
        "inferenceConfig": {"maxTokens": 50},
    }
    if tools:
        spec = {"type": "object", "properties": {"symbol": {"type": "string"}}}
        request["toolConfig"] = {
            "tools": [{"toolSpec": {"name": "price", "inputSchema": {"json": spec}}}]
        }
    return request


@dataclass
class Result:
    """One call's outcome."""

    sent: float
    ok: bool
    kind: str = ""
    wait_secs: float = 0.0
    total_secs: float = 0.0
    deployment: str = ""
    model: str = ""
    worker: str = ""
    seen_secs: float = 0.0  # until a worker first received it
    priority: bool = False
    order: int = 0

    @property
    def started(self) -> float:
        """When its provider call began (the reply's arrival minus the provider's time)."""
        return self.sent + self.wait_secs


@dataclass
class Run:
    """The calls of one gate."""

    t0: float = field(default_factory=time.monotonic)
    results: list[Result] = field(default_factory=list)
    messages: dict[str, str] = field(default_factory=dict)  # first message per failure kind

    async def call(
        self, llmq: LLMQueue, model: str, request: dict[str, Any], *, order: int = 0, **kw: Any
    ) -> Result:
        """One call, timed and recorded."""
        sent = time.monotonic() - self.t0
        try:
            reply = await llmq.converse(model, request, **kw)
            done = time.monotonic() - self.t0
            # Wait = until the provider call began (granted and started), not until a worker
            # first saw it (`meta.queue_wait_ms`), which hides the time spent held.
            began = done - reply.meta.provider_ms / 1000
            result = Result(
                sent,
                True,
                wait_secs=max(0.0, began - sent),
                total_secs=done - sent,
                deployment=reply.meta.deployment,
                model=reply.meta.served_model,
                worker=reply.meta.worker,
                seen_secs=reply.meta.queue_wait_ms / 1000,
                priority=bool(kw.get("priority")),
                order=order,
            )
        except CallFailed as exc:
            done = time.monotonic() - self.t0
            result = Result(sent, False, exc.kind, total_secs=done - sent, order=order)
            self.messages.setdefault(exc.kind, str(exc)[:160])
        self.results.append(result)
        return result

    def kinds(self) -> dict[str, Any]:
        """Failures by kind, with the first message of each."""
        counts = collections.Counter(r.kind for r in self.results if not r.ok)
        return {k: f"{n} ({self.messages.get(k, '')})" for k, n in counts.items()}


def p95(values: list[float]) -> float:
    """95th percentile (0 for no values)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


async def callers(env: dict[str, str], count: int = 2) -> list[LLMQueue]:
    """Caller-only queues in this process."""
    queues = []
    for _ in range(count):
        queue = LLMQueue.from_env({**env, "LLMQ_MODE": "caller"})
        await queue.start()
        queues.append(queue)
    return queues


async def paced(rate: float, secs: float, fn: Callable[[int], Coroutine[Any, Any, Any]]) -> None:
    """Start `fn(i)` `rate` times a second for `secs`, then wait for all of them."""
    tasks = []
    t0 = time.monotonic()
    i = 0
    while time.monotonic() - t0 < secs:
        tasks.append(asyncio.create_task(fn(i)))
        i += 1
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
    await asyncio.gather(*tasks)


class Gate:
    """One gate's setup and teardown (prefix, fakellm, workers, callers)."""

    def __init__(self, doc: dict[str, Any], scenario: dict[str, Any] | None = None, **env: str):
        """Prepare; `async with` starts fakellm, callers and the workers added later."""
        self.prefix = new_prefix()
        self.env = environment(self.prefix, doc, **env)
        self.fake = FakeLLM(scenario or FAST)
        self.store = Store()
        self.fleet: list[Worker] = []
        self.queues: list[LLMQueue] = []

    async def __aenter__(self) -> Gate:
        await asyncio.to_thread(self.fake.start)
        return self

    async def workers(self, count: int) -> list[Worker]:
        """Start `count` more worker processes."""
        more: list[Worker] = await workers(self.env, count)
        self.fleet += more
        return more

    async def callers(self, count: int = 2) -> list[LLMQueue]:
        """Caller queues in this process."""
        more = await callers(self.env, count)
        self.queues += more
        return more

    async def __aexit__(self, *exc: object) -> None:
        for queue in self.queues:
            with contextlib.suppress(Exception):
                await queue.stop(drain_secs=1)
        await stop_all(self.fleet)
        self.fake.stop()
        self.store.cleanup(self.prefix)


def verdict(name: str, checks: dict[str, bool], lines: list[str]) -> bool:
    """Print a gate's numbers and checks; True when all pass."""
    for line in lines:
        print(f"  {line}")
    for check, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {check}")
    passed = all(checks.values())
    print(f"{'PASS' if passed else 'FAIL'}  {name}")
    return passed


# ------------------------------------------------------------------------------------------ gates


async def b17() -> bool:
    """10K mixed calls on 3 accounts x 3 regions (scaled: ~5,000 over 2.5 min).

    Pass: no deployment over its RPM in any minute; served >= 0.9 x the quota in the full
    minute; priority wait p95 <= 2 s.
    """
    rpm = 200  # per deployment, fixed minute: 9 x 200 = 1,800 a minute
    doc = registry(
        {
            "chat": {"rpm": rpm, "rate_window": "fixed", "balance": "headroom"},
            "titan": {"type": "embedding", "model_id": TITAN_ID, "rpm": 100_000},
        }
    )
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        sizes = [500, 2000, 8000, 20000]

        async def one(i: int) -> None:
            llmq = queues[i % 2]
            if random.random() < 0.4:
                await run.call(llmq, "titan", {"body": {"inputText": "text " * 50}})
                return
            text = "x" * (4 * random.choice(sizes))
            await run.call(
                llmq,
                "chat",
                chat(text, tools=random.random() < 0.2),
                priority=random.random() < 0.3,
                order=i,
            )

        used: dict[tuple[int, str], int] = {}  # (minute, deployment) -> the store's own count
        stop = asyncio.Event()

        async def sample() -> None:
            while not stop.is_set():
                status = await queues[0].status("chat")
                minute = int(time.time() // 60)
                for dep in status["deployments"]:
                    key = (minute, dep["name"])
                    used[key] = max(used.get(key, 0), dep["rpm"]["used"])
                await asyncio.sleep(0.25)

        sampler = asyncio.create_task(sample())
        # 95 % of the quota offered (28.5 chat/s of 30/s): at 100 % the priority calls above
        # the 20 % reserve wait for the next fixed minute whenever normal calls spend theirs first.
        await paced(47.5, 150, one)
        stop.set()
        await sampler
    chats = [r for r in run.results if r.model == "chat"]
    minutes = sorted({m for m, _ in used})
    full = minutes[1] if len(minutes) > 2 else minutes[0]  # a minute the run covered whole
    served_full = sum(n for (m, _), n in used.items() if m == full)
    chat_served = sum(1 for r in chats if r.ok)
    priority_wait = p95([r.wait_secs for r in run.results if r.ok and r.priority])
    over = max(used.values(), default=0)
    lines = [
        f"{len(run.results)} calls ({chat_served} chat served); failures {run.kinds() or 0}",
        f"served in the full minute: {served_full} of {9 * rpm}; max per deployment-minute "
        f"{over} (limit {rpm}); priority wait p95 {priority_wait:.2f}s",
    ]
    return verdict(
        "B17",
        {
            "no deployment over its RPM in a minute": over <= rpm,
            "throughput >= 0.9 x quota": served_full >= 0.9 * 9 * rpm,
            "priority wait p95 <= 2 s": priority_wait <= 2.0,
        },
        lines,
    )


async def b19() -> bool:
    """Burst of 2,000 in 10 s.

    Pass: admission refusals per call <= 0.05; store commands per
    call recorded (0.2.1 had 6 state-store calls per worker call).
    """
    doc = registry({"chat": {"rpm": 1_000_000, "worker_slots": 100}}, accounts=1, regions=1)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        before_cmd, before_scr = gate.store.commands(), gate.store.script_calls()
        await paced(200, 10, lambda i: run.call(queues[i % 2], "chat", chat()))
        await asyncio.sleep(2)
        cmds = gate.store.commands() - before_cmd
        scripts = gate.store.script_calls() - before_scr
        metrics = gate.store.metrics(gate.prefix)
    refusals = sum(v for k, v in metrics.items() if k.startswith("llmq_admission_refusals"))
    calls = len(run.results)
    lines = [
        f"{calls} calls, {sum(r.ok for r in run.results)} ok; failures {run.kinds() or 0}",
        f"admission refusals {refusals:.0f} ({refusals / calls:.3f} per call); "
        f"store scripts per call {scripts / calls:.1f} "
        f"(commands incl. inside them {cmds / calls:.0f})",
    ]
    return verdict(
        "B19",
        {
            "every call served": all(r.ok for r in run.results),
            "refusals per call <= 0.05": refusals / calls <= 0.05,
        },
        lines,
    )


async def _shares(processes: int) -> tuple[dict[str, float], float, float]:
    """Deployment shares and store commands per call for one load on `processes` workers."""
    doc = registry({"chat": {"rpm": 1_000_000, "balance": "round_robin"}})
    async with Gate(doc) as gate:
        await gate.workers(processes)
        queues = await gate.callers(2)
        await asyncio.sleep(3)
        idle_t0, idle0 = time.monotonic(), gate.store.commands()
        await asyncio.sleep(5)
        idle_rate = (gate.store.commands() - idle0) / (time.monotonic() - idle_t0)
        run = Run()
        t0, c0 = time.monotonic(), gate.store.commands()
        await paced(100, 15, lambda i: run.call(queues[i % 2], "chat", chat()))
        secs, cmds = time.monotonic() - t0, gate.store.commands() - c0
        probes = gate.store.client.keys(f"{gate.prefix}:lock:*")
    served = collections.Counter(r.deployment for r in run.results if r.ok)
    total = sum(served.values())
    shares = {d: n / total for d, n in served.items()}
    per_call = (cmds - idle_rate * secs) / max(1, len(run.results))
    return shares, per_call, len(probes) + 0.0


async def b22() -> bool:
    """The same load on 2 and on 8 worker processes (plan: 20).

    Pass: deployment shares and
    store commands per call (idle polling subtracted) equal within 5 %.
    """
    two, two_cmds, _ = await _shares(2)
    eight, eight_cmds, _ = await _shares(8)
    worst = max(abs(two.get(d, 0) - eight.get(d, 0)) / max(two.get(d, 1e-9), 1e-9) for d in two)
    cmd_diff = abs(two_cmds - eight_cmds) / two_cmds
    lines = [
        f"shares on 2: {sorted(round(v, 3) for v in two.values())}",
        f"shares on 8: {sorted(round(v, 3) for v in eight.values())}",
        f"store commands per call: {two_cmds:.1f} on 2, {eight_cmds:.1f} on 8",
    ]
    return verdict(
        "B22",
        {
            "shares equal within 5 %": worst <= 0.05,
            "store commands per call equal within 5 %": cmd_diff <= 0.05,
        },
        lines,
    )


async def b56_b93() -> bool:
    """Backlog: 2,400 normal calls waiting on rpm 1,200 (plan: 5,000 on 500 / 4,000), 8 processes.

    Pass: starts per minute >= 0.95 x the normal-class quota; after warm-up, start
    order = send order (1 s of cross-process timing allowed); priority calls start within 2 s;
    store calls per second the same with 2,000 waiting as with a few hundred.
    """
    rpm = 1200
    doc = registry(
        {"chat": {"rpm": rpm, "worker_slots": 50}},
        accounts=1,
        regions=1,
        defaults={"timeout_secs": 300, "timeout_max_secs": 300, "max_wait_secs": 300},
    )
    async with Gate(doc) as gate:
        await gate.workers(8)
        queues = await gate.callers(2)
        run = Run()
        samples: list[tuple[int, float]] = []  # (calls not yet started, store scripts/s)
        done = asyncio.Event()

        async def sample() -> None:
            while not done.is_set():
                s0, t0 = gate.store.script_calls(), time.monotonic()
                await asyncio.sleep(10)
                rate = (gate.store.script_calls() - s0) / (time.monotonic() - t0)
                samples.append((2400 - sum(1 for r in run.results if r.ok), rate))

        tasks = []
        for i in range(2400):  # 5 ms apart: a well-defined send order
            tasks.append(asyncio.create_task(run.call(queues[i % 2], "chat", chat(), order=i)))
            await asyncio.sleep(0.005)
        await asyncio.sleep(3)
        sampler = asyncio.create_task(sample())  # after the sends: only the waiting is measured
        await asyncio.sleep(17)
        prio = [
            asyncio.create_task(run.call(queues[i % 2], "chat", chat(), priority=True))
            for i in range(20)
        ]
        await asyncio.gather(*prio)
        await asyncio.gather(*tasks)
        done.set()
        await sampler
    normal = sorted((r for r in run.results if r.ok and not r.priority), key=lambda r: r.order)
    starts = sorted(r.started for r in normal)
    window = [s for s in starts if starts[0] + 10 <= s < starts[0] + 70]
    quota = int(rpm * 0.8)  # normal calls may use 1 - priority_reserve (plan E79: 80 %)
    steady = [r for r in normal if r.sent >= 15]  # after the first burst and cold pools
    behind = [a.started - b.started for a, b in itertools.pairwise(steady)]
    late = sum(1 for d in behind if d > 1.0)
    prio_wait = max((r.wait_secs for r in run.results if r.priority and r.ok), default=99)
    big = [rate for left, rate in samples if left > 1500]
    small = [rate for left, rate in samples if 100 < left < 600]
    load_big = statistics.mean(big) if big else 0.0
    load_small = statistics.mean(small) if small else 0.0
    lines = [
        f"{len(normal)} normal served; failures {run.kinds() or 0}",
        f"starts in a full minute {len(window)} (normal-class quota {quota} of {rpm})",
        f"after warm-up, older-after-newer starts: worst {max(behind, default=0):.2f}s, "
        f"{late} over 1 s; priority wait max {prio_wait:.2f}s",
        f"store scripts/s: {load_big:.0f} with > 1,500 waiting, {load_small:.0f} with 100-600",
    ]
    return verdict(
        "B56/B93",
        {
            "starts per minute >= 0.95 x quota": len(window) >= 0.95 * quota,
            "start order = send order": late == 0,
            "priority calls start within 2 s": prio_wait <= 2.0,
            "store calls do not grow with the backlog": bool(big and small)
            and load_big <= 1.25 * load_small,
            "every call served": all(r.ok for r in run.results),
        },
        lines,
    )


async def b94() -> bool:
    """The pod holding waiters killed with others waiting, both window modes.

    Pass: the next
    waiter is granted within 2 s (the dead pod's waiters do not block the line).
    """
    ok = True
    for window, rpm in (("smooth", 60), ("fixed", 30)):
        doc = registry({"chat": {"rpm": rpm, "rate_window": window}}, accounts=1, regions=1)
        async with Gate(doc) as gate:
            fleet = await gate.workers(4)
            queues = await gate.callers(2)
            run = Run()
            tasks = [
                asyncio.create_task(run.call(queues[i % 2], "chat", chat(), order=i))
                for i in range(60)
            ]
            await asyncio.sleep(8)
            killed_at = time.monotonic() - run.t0
            fleet[0].kill()
            await asyncio.gather(*tasks)
        grants = sorted(r.started for r in run.results if r.ok and r.started > killed_at)
        interval = 60 / rpm if window == "smooth" else 0.5
        if window == "fixed":  # the next grants come at the next minute
            clock = time.time() - time.monotonic()  # fixed windows turn on wall-clock minutes
            minute = (int((clock + run.t0 + killed_at) // 60) + 1) * 60 - clock - run.t0
            first = min((g for g in grants if g >= minute), default=1e9) - minute
            gap = first
        else:
            gap = max((b - a for a, b in itertools.pairwise(grants)), default=0)
        ok &= verdict(
            f"B94 ({window})",
            {f"next waiter granted within 2 s (+{interval:.1f} s of rate)": gap <= 2 + interval,
             "every call served": all(r.ok for r in run.results)},
            [f"{sum(r.ok for r in run.results)}/60 served; failures {run.kinds() or 0}; "
             f"worst gap after the kill {gap:.2f}s"],
        )  # fmt: skip
    return ok


async def b95() -> bool:
    """State store blocked 45 s with 500 calls waiting.

    Pass: none dropped as `caller_gone`;
    all served.
    """
    import failover

    class StoreProxy(failover.Proxy):  # type: ignore[misc]  # failover is a script module
        """The drill's proxy, in front of the stack's Valkey (not the drill's nodes)."""

        async def handle(self, client_r: Any, client_w: Any) -> None:
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

    doc = registry({"chat": {"rpm": 600}}, accounts=1, regions=1)
    proxy = StoreProxy()
    proxy_port = 6388
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", proxy_port)
    try:
        async with Gate(doc, LLMQ_STORE_URL=f"redis://:localdev@127.0.0.1:{proxy_port}/0") as gate:
            await gate.workers(4)
            queues = await gate.callers(2)
            run = Run()
            tasks = [
                asyncio.create_task(
                    run.call(queues[i % 2], "chat", chat(), timeout_secs=300, max_wait_secs=300)
                )
                for i in range(500)
            ]
            await asyncio.sleep(5)
            proxy.refuse = True
            proxy.drop_all()
            await asyncio.sleep(45)
            proxy.refuse = False
            await asyncio.gather(*tasks)
    finally:
        server.close()
    kinds = run.kinds()
    return verdict(
        "B95",
        {
            "0 calls dropped as caller_gone": "cancelled" not in kinds,
            "all served": all(r.ok for r in run.results),
        },
        [f"{sum(r.ok for r in run.results)}/500 served; failures {kinds or 0}"],
    )


async def b96() -> bool:
    """Kill switch on, 2,000 direct calls waiting in 4 caller processes (plan: 5,000 on 20).

    Pass: admission calls <= 20 per process per second.
    """
    doc = registry({"chat": {"rpm": 600}}, accounts=1, regions=1, routing={"kill_switch": True})
    async with Gate(doc) as gate:
        procs = [
            subprocess.Popen(  # noqa: ASYNC220  # started once, before the measurement
                [PY, str(Path(__file__).parent / "gates_fleet.py"), "caller", "500", "chat"],
                env={**gate.env, "LLMQ_MODE": "caller"},
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            for _ in range(4)
        ]
        for proc in procs:
            assert proc.stdout is not None
            await asyncio.to_thread(proc.stdout.readline)
        for proc in procs:
            assert proc.stdin is not None
            proc.stdin.write(b"go\n")
            proc.stdin.flush()
        await asyncio.sleep(10)
        s0, t0 = gate.store.script_calls(), time.monotonic()
        await asyncio.sleep(20)
        rate = (gate.store.script_calls() - s0) / (time.monotonic() - t0) / len(procs)
        for proc in procs:
            proc.kill()
    return verdict(
        "B96",
        {"store scripts <= 20 per process per second": rate <= 20},
        [f"store scripts per caller process per second while 2,000 direct calls wait: {rate:.1f}"],
    )


async def b91() -> bool:
    """20 models, 10 regions x 3 accounts.

    Pass: script p99 <= 2 ms server time; status of
    everything <= 500 KB and <= 200 ms.
    """
    models = {f"m{i:02d}": {"rpm": 100_000, "model_id": f"openai.m{i}-1:0"} for i in range(20)}
    doc = registry(models, accounts=3, regions=10)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        await asyncio.sleep(2)
        gate.store.reset_latency()
        run = Run()
        await paced(100, 15, lambda i: run.call(queues[i % 2], f"m{i % 20:02d}", chat()))
        p99 = gate.store.script_p99_ms()
        times = []
        for _ in range(6):
            t0 = time.monotonic()
            status = await queues[0].status()
            times.append((time.monotonic() - t0) * 1000)
        cold_ms, status_ms = times[0], statistics.median(times[1:])
        size_kb = len(json.dumps(status, default=str)) / 1024
    return verdict(
        "B91",
        {
            "every call served": all(r.ok for r in run.results),
            "script p99 <= 2 ms": p99 <= 2.0,
            "status <= 500 KB": size_kb <= 500,
            "status <= 200 ms": status_ms <= 200,
        },
        [f"{len(run.results)} calls over 20 models x 30 deployments; script p99 {p99:.2f} ms "
         f"(exact); status {size_kb:.0f} KB in {status_ms:.0f} ms (first call {cold_ms:.0f} ms)"],
    )  # fmt: skip


async def b92() -> bool:
    """Idle (plan: 30 min; here 3 min), then 3,000 calls in 60 s across 3 regions.

    Pass: 0
    connect errors; first-call overhead <= 500 ms; priority wait p95 <= 2 s.
    """
    doc = registry({"chat": {"rpm": 100_000}}, accounts=1, regions=3)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        await asyncio.sleep(180)
        run = Run()
        first = await run.call(queues[0], "chat", chat(), priority=True)
        await paced(50, 60, lambda i: run.call(queues[i % 2], "chat", chat(), priority=i % 3 == 0))
    overhead = first.total_secs - 0.25  # minus fakellm's p95 latency
    wait = p95([r.wait_secs for r in run.results if r.ok and r.priority])
    return verdict(
        "B92",
        {
            "0 errors": all(r.ok for r in run.results),
            "first-call overhead <= 500 ms": overhead <= 0.5,
            "priority wait p95 <= 2 s": wait <= 2.0,
        },
        [f"{len(run.results)} calls; first call {first.total_secs * 1000:.0f} ms; "
         f"priority wait p95 {wait:.2f}s; failures {run.kinds() or 0}"],
    )  # fmt: skip


async def b97() -> bool:
    """Bulk embedding on cohere (96 texts per call) at its full rpm.

    Pass: store memory stays
    under its budget (here 64 MB above the start) for the whole run.
    """
    doc = registry(
        {"cohere": {"type": "embedding", "model_id": EMBED_ID, "rpm": 600,
                    "embed_batch_size": 96}},
        accounts=1, regions=1,
    )  # fmt: skip
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        base = gate.store.memory_mb()
        peak = base
        done = asyncio.Event()

        async def sample() -> None:
            nonlocal peak
            while not done.is_set():
                peak = max(peak, gate.store.memory_mb())
                await asyncio.sleep(0.5)

        sampler = asyncio.create_task(sample())
        texts = ["a sentence about markets and earnings " * 8] * (96 * 300)
        failures: collections.Counter[str] = collections.Counter()
        try:
            await queues[0].embed("cohere", texts, input_type="search_document")
        except CallFailed as exc:
            failures[exc.kind] += 1
        done.set()
        await sampler
    return verdict(
        "B97",
        {"every batch served": not failures, "store memory <= +64 MB": peak - base <= 64},
        [f"300 calls of 96 texts; store memory {base:.0f} -> peak {peak:.0f} MB"],
    )


async def b33() -> bool:
    """Rolling restart of 4 workers under load (plan: 0.3.x -> 0.3.y at 10K / min).

    Here 3K / min and one version.

    Pass: 0 lost, 0 failed priority calls; registry version never goes
    back.
    """
    doc = registry({"chat": {"rpm": 1_000_000}}, accounts=1, regions=1)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        versions: list[int] = []

        async def restarts() -> None:
            for worker in list(gate.fleet):
                await asyncio.sleep(6)
                await gate.workers(1)
                await worker.stop()
                versions.append(int(queues[0].registry.current.registry.version))

        load = paced(50, 40, lambda i: run.call(queues[i % 2], "chat", chat(), priority=i % 4 == 0))
        await asyncio.gather(load, restarts())
    return verdict(
        "B33",
        {
            "0 lost": all(r.ok for r in run.results),
            "0 failed priority calls": all(r.ok for r in run.results if r.priority),
            "registry version never goes back": versions == sorted(versions),
        },
        [f"{len(run.results)} calls during 4 restarts; failures {run.kinds() or 0}"],
    )


def _ramp_call(run: Run, queues: list[LLMQueue], i: int) -> Coroutine[Any, Any, Result]:
    """One ramp call (a named function: a lambda's default arguments are untyped)."""
    return run.call(queues[i % 2], "chat", chat())


async def b34() -> bool:
    """Ramp until something saturates; queue overhead.

    Pass: the ceiling and store memory are
    recorded; with no backlog the queue path adds <= 20 ms at p95 over a direct call.
    """
    fixed = {"defaults": {"latency_ms": {"p50": 100, "p95": 100}}}
    doc = registry({"chat": {"rpm": 10_000_000, "worker_slots": 200}}, accounts=1, regions=1)
    async with Gate(doc, fixed) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        warm = Run()
        await paced(10, 5, lambda i: warm.call(queues[i % 2], "chat", chat()))
        for _ in range(20):
            await warm.call(queues[0], "chat", chat(), force_direct=True)
        queued, direct = Run(), Run()  # alternated, both warm: same conditions for each

        async def pair(i: int) -> None:
            await queued.call(queues[i % 2], "chat", chat())
            await direct.call(queues[i % 2], "chat", chat(), force_direct=True)

        await paced(5, 30, pair)
        ceiling = 0.0
        for rate in (50, 100, 200, 400, 800):
            ramp = Run()
            await paced(rate, 8, functools.partial(_ramp_call, ramp, queues))
            ok = [r for r in ramp.results if r.ok]
            if len(ok) < len(ramp.results) or p95([r.wait_secs for r in ok]) > 1.0:
                break
            ceiling = len(ok) / 8
        memory = gate.store.memory_mb()
    added = p95([r.total_secs for r in queued.results]) - p95(
        [r.total_secs for r in direct.results]
    )
    return verdict(
        "B34",
        {"queue path adds <= 20 ms at p95": added <= 0.020},
        [f"queued p95 {p95([r.total_secs for r in queued.results]) * 1000:.0f} ms, direct p95 "
         f"{p95([r.total_secs for r in direct.results]) * 1000:.0f} ms (+{added * 1000:.0f} ms)",
         f"ceiling on this machine: {ceiling:.0f} calls/s with queue wait p95 <= 1 s "
         f"(4 workers); store memory {memory:.0f} MB"],
    )  # fmt: skip


async def b23() -> bool:
    """Queue store primary killed mid-run, `queue_wait_replicas` off and on.

    Pass: lost sends
    and duplicates recorded; with it on, losses not higher than with it off.
    """
    import failover

    losses: dict[str, int] = {}
    for wait in (0, 1):
        failover.nodes_up()
        proxy = failover.Proxy()
        server = await asyncio.start_server(proxy.handle, "127.0.0.1", failover.PROXY_PORT)
        doc = registry({"chat": {"rpm": 1_000_000}}, accounts=1, regions=1)
        try:
            async with Gate(
                doc,
                LLMQ_QUEUE_STORE_URL=f"redis://127.0.0.1:{failover.PROXY_PORT}/0",
                LLMQ_QUEUE_WAIT_REPLICAS=str(wait),
                LLMQ_QUEUE_WAIT_MS="200",
                LLMQ_QUEUE_FAILOVER_SECS="3",
            ) as gate:
                await gate.workers(2)
                queues = await gate.callers(2)
                run = Run()

                async def chaos() -> None:
                    await asyncio.sleep(5)
                    # In a thread: a blocking docker call stalled this driver's own callers
                    # (their misses counted as lost sends under load).
                    await asyncio.to_thread(failover.docker, "kill", failover.NODES["a"][0])
                    await asyncio.to_thread(
                        failover.docker,
                        "exec",
                        failover.NODES["b"][0],
                        "valkey-cli",
                        "REPLICAOF",
                        "NO",
                        "ONE",
                    )
                    proxy.target = "b"  # noqa: B023  # awaited within this iteration

                load = paced(
                    100,
                    15,
                    lambda i: run.call(  # noqa: B023  # awaited within this iteration
                        queues[i % 2],  # noqa: B023
                        "chat",
                        chat(),
                        timeout_secs=20,
                    ),
                )
                await asyncio.gather(load, chaos())
        finally:
            server.close()
            failover.nodes_down()
        lost = sum(1 for r in run.results if not r.ok and r.kind == "deadline_exceeded")
        losses["on" if wait else "off"] = lost
        print(f"  queue_wait_replicas={wait}: {len(run.results)} calls, lost {lost}, "
              f"failures {run.kinds() or 0}")  # fmt: skip
    return verdict(
        "B23",
        {"losses with the wait on <= with it off": losses["on"] <= losses["off"]},
        [f"lost sends: off {losses['off']}, on {losses['on']}"],
    )


GATES: dict[str, Callable[[], Coroutine[Any, Any, bool]]] = {
    "B17": b17,
    "B19": b19,
    "B22": b22,
    "B23": b23,
    "B33": b33,
    "B34": b34,
    "B56": b56_b93,
    "B91": b91,
    "B92": b92,
    "B94": b94,
    "B95": b95,
    "B96": b96,
    "B97": b97,
}


SLOW_GATES = frozenset({"B17", "B91", "B92"})  # left out by --quick


def main() -> int:
    """Run the chosen gates; exit 1 if any fails."""
    setup_logging()
    print(f"  logs of worker processes: {log_dir()}", flush=True)
    args = sys.argv[1:]
    if args == ["--list"]:
        for name, fn in GATES.items():
            print(f"{name:4} {(fn.__doc__ or '').splitlines()[0]}")
        return 0
    if args == ["--quick"]:
        args = [name for name in GATES if name not in SLOW_GATES]
    chosen = args or list(GATES)
    results: dict[str, bool] = {}
    for name in chosen:
        print(f"[{name}]", flush=True)
        started = time.monotonic()
        results[name] = asyncio.run(GATES[name]())
        print(f"  ({time.monotonic() - started:.0f}s)", flush=True)
    failed = [n for n, ok in results.items() if not ok]
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
