"""Building and draining the worker side: brokers per model, receivers, heartbeat (§6, §7.4).

`build_brokers` creates one `HybridSQSBroker` per distinct queue set (rt priority 0, default 1,
bulk 2) with limiter-aware gates and receive budgets (DS4, DS13). `Workers` runs taskiq
`Receiver`s in-process and stops them in the ordered drain of §7.4.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from redis.exceptions import RedisError
from taskiq.acks import AcknowledgeType
from taskiq.receiver import Receiver

from taskiq_llm_queue import metrics_names as mn
from taskiq_llm_queue.broker.broker import HybridSQSBroker
from taskiq_llm_queue.broker.events import BrokerHooks
from taskiq_llm_queue.broker.queue import SQSQueue
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.reliability.breaker import BreakerSettings
from taskiq_llm_queue.reliability.claims import ClaimSettings
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter, LimiterSettings
from taskiq_llm_queue.routing_hooks import QueueHooks
from taskiq_llm_queue.tasks import WorkerRuntime, job_filter, register_tasks

if TYPE_CHECKING:
    from taskiq import AsyncResultBackend

    from taskiq_llm_queue.config import ModelConfig, QueueConfig
    from taskiq_llm_queue.metrics import MetricsSink
    from taskiq_llm_queue.protocols import ReceiveGate
    from taskiq_llm_queue.routing_hooks import FailoverPolicy

logger = logging.getLogger(__name__)

DEFAULT_WORKER_SLOTS = 50
DEFAULT_JOB_SLOTS = 4
JOBS_BROKER = "jobs"
# Queue key -> (priority, lanes it serves); rt first (A1), bulk last.
QUEUE_PRIORITY: Mapping[str, int] = {"rt": 0, "default": 1, "bulk": 2}
_QUEUE_LANE: Mapping[str, Lane] = {"rt": Lane.RT, "default": Lane.BG, "bulk": Lane.BULK}
_FLUSH_SECS = 10.0


class MetricsHooks(QueueHooks, BrokerHooks):
    """Client, worker and broker events reported to a `MetricsSink` (names: `metrics_names`).

    The default `hooks` of `LLMQueue`; labels are only those in `metrics_names.LABELS`.
    """

    def __init__(self, sink: MetricsSink) -> None:
        """Report to `sink`."""
        self.sink = sink

    def on_call(self, model: str, lane: str, path: str, ok: bool, secs: float) -> None:
        """Count a finished client call by outcome."""
        labels = {"model": model, "lane": lane, "path": path, "kind": "ok" if ok else "error"}
        self.sink.incr(mn.CALLS_TOTAL, 1.0, labels)

    def on_attempt(self, model: str, target: str, kind: str, secs: float) -> None:
        """Provider latency per attempt; throttles counted separately."""
        self.sink.observe(mn.PROVIDER_SECONDS, secs, {"model": model, "target": target})
        if kind == "throttle":
            self.sink.incr(mn.THROTTLES_TOTAL, 1.0, {"model": model, "target": target})

    def on_fallback(self, model: str, step: str) -> None:
        """Count calls served by a fallback step."""
        self.sink.incr(mn.FALLBACK_TOTAL, 1.0, {"model": model, "step": step})

    def on_queue_wait_ms(self, model: str, lane: str, wait_ms: int) -> None:
        """Time from enqueue to execution start."""
        self.sink.observe(mn.QUEUE_WAIT_SECONDS, wait_ms / 1000, {"model": model, "lane": lane})

    def on_sync_fallback(self, model: str) -> None:
        """Count sync calls that ran direct."""
        self.sink.incr(mn.SYNC_FALLBACK_TOTAL, 1.0, {"model": model})

    def on_release(self, queue: str, count: int, reason: str) -> None:
        """Count messages released because this pod didn't know the task (DS9)."""
        if reason.startswith("unknown_task") or reason == "unsupported_envelope":
            self.sink.incr(mn.RELEASED_UNKNOWN_TOTAL, float(count), {"queue": queue})


def limiter_settings(config: QueueConfig) -> LimiterSettings:
    """`LimiterSettings` from `QueueConfig.limiter`; per-model starts from `limiter_initial`."""
    tuning = config.limiter
    initial = {alias: m.limiter_initial for alias, m in config.models.items()}
    initial.update(tuning.initial)
    return LimiterSettings(
        initial=initial,
        min_limit=tuning.min_limit,
        max_limit=tuning.max_limit,
        ramp_window=tuning.ramp_window,
        increase_every=tuning.increase_every,
        decrease_factor=tuning.decrease_factor,
        pause_secs=tuning.pause_secs,
        bg_share=tuning.bg_share,
        utilisation=tuning.utilisation,
        ramp_cap=tuning.ramp_cap,
        lease_ttl_secs=tuning.lease_ttl_secs,
    )


def breaker_settings(config: QueueConfig) -> BreakerSettings:
    """`BreakerSettings` from `QueueConfig.breaker`."""
    b = config.breaker
    return BreakerSettings(b.threshold, b.window_secs, b.cooldown_secs)


def claim_settings(config: QueueConfig) -> ClaimSettings:
    """`ClaimSettings` from the config: state TTL, worst-case execution, `max_failures`."""
    return ClaimSettings(
        state_ttl_secs=config.state_ttl_secs,
        exec_max_secs=round(max(config.timeout_secs.values())),
        max_failures=config.max_failures,
    )


def queue_for_lane(model: ModelConfig, lane: Lane) -> str | None:
    """Queue name or URL a call in `lane` goes to: rt → `rt`, bulk → `bulk`, else `default`.

    Falls back to whatever the model has (default, then rt), so a model with one queue works.
    """
    order = {Lane.RT: ("rt", "default", "bulk"), Lane.BG: ("default", "rt", "bulk")}
    for key in order.get(lane, ("bulk", "default", "rt")):
        if key in model.queues:
            return model.queues[key]
    return None


def queue_name(value: str) -> str:
    """Routing name of a queue given as a name or URL (its last path segment)."""
    return value.rstrip("/").rsplit("/", 1)[-1]


class ChainGate:
    """`ReceiveGate` open while any target of a model has headroom and Valkey is up (DS13, DS18).

    Wraps the limiter's per-target pause gates; while the failover policy says Valkey is down
    it stays closed, so workers stop taking tasks they could not claim.
    """

    def __init__(
        self, gates: list[ReceiveGate], redis_down: Callable[[], bool], *, retry_secs: float = 1.0
    ) -> None:
        """Keep the target gates and the Valkey-down probe."""
        self._gates = gates
        self._redis_down = redis_down
        self._retry_secs = retry_secs

    def is_open(self) -> bool:
        """True when Valkey is up and at least one target is not paused."""
        return not self._redis_down() and any(g.is_open() for g in self._gates)

    async def wait_open(self) -> None:
        """Return once Valkey is up and one target gate opened."""
        while True:
            if self._redis_down():
                await asyncio.sleep(self._retry_secs)
                continue
            waiters = [asyncio.ensure_future(g.wait_open()) for g in self._gates]
            try:
                await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for waiter in waiters:
                    waiter.cancel()
            if self.is_open():
                return


def receive_budget(
    alias: str,
    model: ModelConfig,
    limiter: AdaptiveLimiter,
    lanes: Mapping[str, Lane],
    failover: FailoverPolicy | None,
) -> Callable[[SQSQueue], Any]:
    """DS4 budget for one model's queues: the best limiter headroom across its targets.

    Returns 0 while Valkey is down or unreachable, so nothing is received that can't run.
    """

    async def budget(queue: SQSQueue) -> int:
        if failover is not None and failover.redis_down():
            return 0
        share = limiter.settings.share_for(lanes.get(queue.name, Lane.BG))
        best = 0
        try:
            for target in model.targets:
                state = await limiter.state(alias, target)
                best = max(best, state.headroom(share))
        except RedisError as exc:
            if failover is not None:
                failover.report_redis_failure(exc)
            return 0
        return best

    return budget


@dataclass(slots=True)
class BrokerSet:
    """The brokers of one `LLMQueue`: per model alias (shared when queues are equal) and jobs."""

    by_alias: dict[str, HybridSQSBroker] = field(default_factory=dict)
    jobs: HybridSQSBroker | None = None

    def all(self) -> list[HybridSQSBroker]:
        """Every distinct broker, models first."""
        seen: list[HybridSQSBroker] = []
        for broker in [*self.by_alias.values(), self.jobs]:
            if broker is not None and all(broker is not b for b in seen):
                seen.append(broker)
        return seen

    def slots(self, broker: HybridSQSBroker) -> int:
        """`max_async_tasks` for a broker's receiver (its scheduler capacity)."""
        return broker.scheduler.max_inflight


def _scheduler_split(slots: int, queues: int) -> tuple[int, int]:
    """(`reserve_for_priority0`, `min_slots_for_lower`) for a multi-queue broker."""
    if queues < 2:
        return 0, 0
    return slots // 5, min(2, slots // 4)


def build_brokers(
    config: QueueConfig,
    runtime: WorkerRuntime,
    limiter: AdaptiveLimiter,
    result_backend: AsyncResultBackend[Any],
    *,
    failover: FailoverPolicy | None = None,
    hooks: BrokerHooks | None = None,
) -> BrokerSet:
    """One broker per distinct model queue set, plus a jobs broker when `job_queues` is set.

    Every broker gets the three stable tasks, metadata labels (receipt handle for release),
    the shared result backend and `runtime.on_lost`. Nothing connects until `startup()`.
    """
    brokers = BrokerSet()
    by_queues: dict[tuple[str, ...], HybridSQSBroker] = {}
    redis_down = failover.redis_down if failover is not None else (lambda: False)
    common: dict[str, Any] = {
        "endpoint_url": config.sqs_endpoint_url,
        "region_name": config.aws_region,
        "expose_message_metadata": True,
        "hooks": hooks,
        "on_lost": runtime.on_lost,
        "release_running_on_shutdown": True,
    }
    for alias, model in config.models.items():
        if not model.queues:
            continue
        key = tuple(sorted(model.queues.values()))
        if (shared := by_queues.get(key)) is not None:
            brokers.by_alias[alias] = shared
            continue
        gate = ChainGate([limiter.gate(alias, t) for t in model.targets], redis_down)
        lanes = {queue_name(v): _QUEUE_LANE[k] for k, v in model.queues.items()}
        queues = [
            SQSQueue(
                name=queue_name(value),
                url=value if "://" in value else None,
                priority=QUEUE_PRIORITY[kind],
                wait_time_secs=config.sqs_wait_time_secs,
                gate=gate,
            )
            for kind, value in sorted(model.queues.items(), key=lambda kv: QUEUE_PRIORITY[kv[0]])
        ]
        slots = config.worker_slots.get(alias, DEFAULT_WORKER_SLOTS)
        reserve, lower = _scheduler_split(slots, len(queues))
        broker = HybridSQSBroker(
            queues,
            default_queue=queue_name(queue_for_lane(model, Lane.BG) or queues[0].name),
            max_inflight=slots,
            reserve_for_priority0=reserve,
            min_slots_for_lower=lower,
            receive_budget=receive_budget(alias, model, limiter, lanes, failover),
            **common,
        ).with_result_backend(result_backend)
        register_tasks(broker, runtime)
        by_queues[key] = brokers.by_alias[alias] = broker
    job_queues = sorted(set(config.job_queues.values()))
    if job_queues:
        jobs = HybridSQSBroker(
            [
                SQSQueue(
                    name=queue_name(v),
                    url=v if "://" in v else None,
                    wait_time_secs=config.sqs_wait_time_secs,
                )
                for v in job_queues
            ],
            max_inflight=config.worker_slots.get(JOBS_BROKER, DEFAULT_JOB_SLOTS),
            message_filter=job_filter(runtime),
            **common,
        ).with_result_backend(result_backend)
        register_tasks(jobs, runtime)
        brokers.jobs = jobs
    return brokers


class Workers:
    """In-process taskiq receivers for every broker, the claim/lease heartbeat and the drain.

    `start` launches one `Receiver.listen` per broker (`ack_type=WHEN_SAVED`, `max_async_tasks`
    = the broker's slots). `drain` is steps 1-4 of §7.4; closing the brokers is the caller's.
    """

    def __init__(self, brokers: BrokerSet, runtime: WorkerRuntime) -> None:
        """Keep the brokers and runtime; nothing starts here."""
        self._brokers = brokers
        self._runtime = runtime
        self._finish = asyncio.Event()
        self._receivers: list[Receiver] = []
        self._listeners: list[asyncio.Task[None]] = []
        self._heartbeat: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start receivers and the heartbeat; brokers must already be started."""
        self._finish = asyncio.Event()
        for broker in self._brokers.all():
            receiver = Receiver(
                broker,
                max_async_tasks=self._brokers.slots(broker),
                ack_type=AcknowledgeType.WHEN_SAVED,
                run_startup=False,
                propagate_exceptions=False,
            )
            self._receivers.append(receiver)
            self._listeners.append(
                asyncio.create_task(receiver.listen(self._finish), name="llmq-receiver")
            )
        self._heartbeat = asyncio.create_task(self._beat(), name="llmq-claim-heartbeat")

    async def pause(self) -> None:
        """§7.4 step 1: stop receiving and release buffered, unstarted messages."""
        await asyncio.gather(*(b.pause_receiving() for b in self._brokers.all()))

    def resume(self) -> None:
        """Undo `pause`."""
        for broker in self._brokers.all():
            broker.resume_receiving()

    async def drain(self, drain_secs: float) -> None:
        """§7.4 steps 1-4: pause, let running tasks finish, then hand leftovers back."""
        await self.pause()
        for receiver in self._receivers:
            receiver.wait_tasks_timeout = drain_secs
        self._finish.set()
        if self._listeners:
            _, pending = await asyncio.wait(self._listeners, timeout=drain_secs + _FLUSH_SECS)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._listeners, return_exceptions=True)
        leftovers = self._runtime.stop_running("drain")
        if leftovers:
            logger.info("drain: handing back %d running tasks", len(leftovers))
            await asyncio.wait(leftovers, timeout=_FLUSH_SECS)
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat
        self._listeners.clear()
        self._receivers.clear()

    async def _beat(self) -> None:
        """Refresh claims of running tasks and renew in-flight leases (DS3, DS6, DS11)."""
        deps = self._runtime.deps
        claims = deps.claims
        interval = claims.settings.heartbeat_interval_secs if claims is not None else 10.0
        while True:
            await asyncio.sleep(interval)
            try:
                ids = list(self._runtime.running)
                if claims is not None and ids:
                    lost = await claims.heartbeat_owned(ids, worker=deps.worker)
                    if lost:
                        logger.warning("claims lost for %d running tasks", len(lost))
                        self._runtime.stop_running("lost", lost)
                if isinstance(deps.limiter, AdaptiveLimiter) and deps.leases:
                    await deps.limiter.renew(list(deps.leases))
            except Exception as exc:  # keep beating; a blip must not stop the loop
                logger.warning("claim heartbeat failed: %s", type(exc).__name__)
