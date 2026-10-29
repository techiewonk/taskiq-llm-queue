"""Building and draining the worker side: brokers per model, receivers, heartbeat (§6, §7.4).

`build_brokers` creates one `HybridSQSBroker` per distinct queue set (rt priority 0, default 1,
bulk 2) with limiter-aware gates and receive budgets (DS4, DS13). `Workers` runs taskiq
`Receiver`s in-process and stops them in the ordered drain of §7.4. `PodHeartbeat` keeps every
claim and lease this pod holds alive, in every mode (DS3, DS6, DS11, DS17).
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from redis.exceptions import RedisError
from taskiq.acks import AcknowledgeType
from taskiq.receiver import Receiver

from taskiq_llm_queue import metrics_names as mn
from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.broker.broker import HybridSQSBroker
from taskiq_llm_queue.broker.events import BrokerHooks
from taskiq_llm_queue.broker.queue import SQSQueue
from taskiq_llm_queue.broker.serializer import WireSerializer
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.model_settings import SHARED_PREFIX, SHARED_TARGET, SettingsResolver
from taskiq_llm_queue.reliability.breaker import BreakerSettings
from taskiq_llm_queue.reliability.claims import ClaimSettings
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter, LimiterSettings
from taskiq_llm_queue.routing_hooks import QueueHooks
from taskiq_llm_queue.tasks import WorkerRuntime, job_filter, register_tasks

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from redis.asyncio import Redis
    from taskiq import AsyncResultBackend

    from taskiq_llm_queue.config import ModelConfig, QueueConfig
    from taskiq_llm_queue.metrics import MetricsSink
    from taskiq_llm_queue.protocols import Lease, ReceiveGate
    from taskiq_llm_queue.reliability.claims import RedisClaimStore
    from taskiq_llm_queue.routing_hooks import FailoverPolicy

logger = get_logger(__name__)

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


def limiter_settings(
    config: QueueConfig, resolver: SettingsResolver | None = None
) -> LimiterSettings:
    """`LimiterSettings` from `QueueConfig.limiter` and the effective per-model settings.

    Per alias: start (`limiter_initial`), cap (`max_concurrency`) and `bg_share` from the
    resolver's six layers (README "Settings"); each provider's shared cap becomes a fixed
    limit of the `@<provider>` limiter alias (start = cap).
    """
    resolver = resolver or SettingsResolver(config)
    tuning = config.limiter
    aliases = list(config.models)
    shared = resolver.shared_caps()
    initial = {a: resolver.value(a, "limiter_initial") for a in aliases}
    return LimiterSettings(
        initial={**initial, **shared},
        max_by_model={**resolver.model_caps(), **shared},
        bg_share_by_model={a: resolver.value(a, "bg_share") for a in aliases},
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


def model_caps(config: QueueConfig, resolver: SettingsResolver | None = None) -> dict[str, int]:
    """Hard per-alias concurrency caps (effective `max_concurrency`).

    Feeds the shared limiter and the per-pod local caps used while Valkey is down.
    """
    return (resolver or SettingsResolver(config)).model_caps()


def breaker_settings(config: QueueConfig) -> BreakerSettings:
    """`BreakerSettings` from `QueueConfig.breaker`."""
    b = config.breaker
    return BreakerSettings(b.threshold, b.window_secs, b.cooldown_secs)


def breaker_by_model(resolver: SettingsResolver) -> dict[str, BreakerSettings]:
    """Per-alias breaker settings whose threshold or cooldown differ from the global ones."""
    base = breaker_settings(resolver.config)
    out: dict[str, BreakerSettings] = {}
    for alias in resolver.config.models:
        threshold = resolver.value(alias, "breaker_threshold")
        cooldown = resolver.value(alias, "breaker_cooldown_secs")
        if (threshold, cooldown) != (base.threshold, base.cooldown_secs):
            out[alias] = BreakerSettings(threshold, base.window_secs, cooldown)
    return out


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

    Headroom is net of this pod's callers already waiting for a slot of that target, so a full
    bucket (e.g. `max_concurrency`, or the provider's shared cap) leaves the backlog in SQS.
    Returns 0 while Valkey is down or unreachable, so nothing is received that can't run.
    """

    async def budget(queue: SQSQueue) -> int:
        if failover is not None and failover.redis_down():
            return 0
        share = limiter.settings.share_for(lanes.get(queue.name, Lane.BG), alias)
        shared = f"{SHARED_PREFIX}{model.provider}"
        has_shared = shared in (limiter.settings.max_by_model or {})
        try:  # every target in one pipelined round trip, on each poll
            states = await limiter.states(alias, model.targets)
            pool = await limiter.states(shared, [SHARED_TARGET]) if has_shared else []
        except RedisError as exc:
            if failover is not None:
                failover.report_redis_failure(exc)
            return 0
        best = max(
            (
                state.headroom(share) - limiter.waiting(alias, target)
                for target, state in zip(model.targets, states, strict=True)
            ),
            default=0,
        )
        if pool:  # the provider's shared cap bounds every model of that provider
            best = min(best, pool[0].headroom() - limiter.waiting(shared, SHARED_TARGET))
        return max(0, best)

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
    session: AioSession | None = None,
) -> BrokerSet:
    """One broker per distinct model queue set, plus a jobs broker when `job_queues` is set.

    Every broker gets the three stable tasks, metadata labels (receipt handle for release),
    the shared result backend, `runtime.on_lost` and the orjson `WireSerializer` (reads
    stdlib-JSON messages from older pods and vice versa). `session` is the pod's one shared
    aiobotocore session (credential chain resolved once). Nothing connects until `startup()`.
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
        "session": session,
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
        broker.with_serializer(WireSerializer())
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
        jobs.with_serializer(WireSerializer())
        register_tasks(jobs, runtime)
        brokers.jobs = jobs
    return brokers


class Workers:
    """In-process taskiq receivers for every broker and their drain.

    `start` launches one `Receiver.listen` per broker (`ack_type=WHEN_SAVED`, `max_async_tasks`
    = the broker's slots). `drain` is steps 1-4 of §7.4; claims are kept alive by `PodHeartbeat`.
    """

    def __init__(self, brokers: BrokerSet, runtime: WorkerRuntime) -> None:
        """Keep the brokers and runtime; nothing starts here."""
        self._brokers = brokers
        self._runtime = runtime
        self._finish = asyncio.Event()
        self._receivers: list[Receiver] = []
        self._listeners: list[asyncio.Task[None]] = []

    def start(self) -> None:
        """Start receivers; brokers must already be started."""
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
            logger.info("drain_hand_back", running=len(leftovers))
            await asyncio.wait(leftovers, timeout=_FLUSH_SECS)
        self._listeners.clear()
        self._receivers.clear()


@dataclass(frozen=True, slots=True)
class ClaimGroup:
    """Claims held under one worker identity: `ids()` lists them, `on_lost` hears lost ones.

    A pod has two: its worker tasks (`pod_id`) and its DS17 direct calls (`direct:<pod_id>`).
    """

    worker: str
    ids: Callable[[], Sequence[str]]
    on_lost: Callable[[list[str]], None]


class PodRegistry:
    """Live pods of one key prefix: a sorted set (`KeySpace.pods`) scored by expiry.

    Each heartbeat re-adds this pod and drops expired ones; the count is the `live_pods` that
    splits the last known limit into a per-pod local cap while Valkey is down (DS18).
    """

    def __init__(
        self,
        client: Redis,
        key: str,
        pod_id: str,
        *,
        ttl_secs: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Keep the client, the set's key, this pod's id and how long an entry lives."""
        self._client = client
        self._key = key
        self._pod = pod_id
        self._ttl = ttl_secs
        self._clock = clock

    async def beat(self) -> int:
        """Mark this pod alive and return how many pods are."""
        now = self._clock()
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.zadd(self._key, {self._pod: now + self._ttl})
            pipe.zremrangebyscore(self._key, "-inf", now)
            pipe.zcard(self._key)
            pipe.expire(self._key, math.ceil(2 * self._ttl))
            _, _, count, _ = await pipe.execute()
        return int(count)

    async def leave(self) -> None:
        """Remove this pod (clean shutdown); expiry covers a crash."""
        await self._client.zrem(self._key, self._pod)


class PodHeartbeat:
    """One loop per pod, in every mode: claims, leases and the live-pod count (DS3, DS6, DS11).

    Covers worker tasks and DS17 direct claims, and renews every lease of the worker, direct
    and sync paths (one shared set). A Valkey blip is logged; the loop never stops.
    """

    def __init__(
        self,
        *,
        claims: RedisClaimStore | None,
        limiter: AdaptiveLimiter | None,
        leases: set[Lease],
        groups: Sequence[ClaimGroup],
        interval_secs: float,
        pods: PodRegistry | None = None,
        on_live_pods: Callable[[int], None] | None = None,
    ) -> None:
        """Keep what to refresh; nothing starts until `run()`."""
        self._claims = claims
        self._limiter = limiter
        self._leases = leases
        self._groups = list(groups)
        self.interval_secs = interval_secs
        self._pods = pods
        self._on_live_pods = on_live_pods

    async def run(self) -> None:
        """Beat every `interval_secs` until cancelled."""
        while True:
            await asyncio.sleep(self.interval_secs)
            await self.beat_once()

    async def beat_once(self) -> None:
        """Refresh claims, renew leases and the pod registry once; errors are logged."""
        for group in self._groups:
            await self._beat_claims(group)
        leases = self._lease_snapshot()
        if self._limiter is not None and leases:
            try:
                await self._limiter.renew(leases)
            except Exception as exc:
                logger.warning(
                    "lease_renew_failed", leases=len(leases), error_type=type(exc).__name__
                )
        if self._pods is not None:
            try:
                live = await self._pods.beat()
            except Exception as exc:
                logger.warning("pod_registry_failed", error_type=type(exc).__name__)
            else:
                if self._on_live_pods is not None:
                    safe_call(self._on_live_pods, live)

    async def _beat_claims(self, group: ClaimGroup) -> None:
        ids = list(dict.fromkeys(group.ids()))
        if self._claims is None or not ids:
            return
        try:
            lost = await self._claims.heartbeat_owned(ids, worker=group.worker)
        except Exception as exc:  # keep beating; a blip must not stop the loop
            logger.warning("claim_heartbeat_failed", error_type=type(exc).__name__)
            return
        if lost:
            logger.warning("claims_lost", worker=group.worker, running=len(lost))
            safe_call(group.on_lost, lost)

    def _lease_snapshot(self) -> list[Lease]:
        """Copy the shared lease set; sync callers add to it from their own thread."""
        for _ in range(3):
            try:
                return list(self._leases)
            except RuntimeError:  # changed size during iteration
                continue
        return []


def safe_call(callback: Callable[..., object], *args: object) -> None:
    """Run a heartbeat callback, logging (never raising) whatever it throws."""
    try:
        callback(*args)
    except Exception:
        logger.exception("heartbeat_callback_failed")
