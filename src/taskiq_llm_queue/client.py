"""`LLMQueue`: the public client. Sends calls through SQS (or runs them `direct`) and works them.

One instance per process. `start(mode)` connects Valkey and SQS, starts the result dispatcher
(senders) and the in-process taskiq receivers (workers); `stop()` drains in the order of §7.4.
Calls build an envelope from `request_context`, route it, and await the reply (§6.1).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import threading
import time
import uuid
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import orjson
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.connection import Connection, SSLConnection
from redis.exceptions import RedisError
from taskiq.exceptions import SendTaskError

from taskiq_llm_queue import metrics_names as mn
from taskiq_llm_queue.broker.errors import SQSSendError
from taskiq_llm_queue.broker.events import BrokerHooks
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.context import current_context
from taskiq_llm_queue.core import CoreDeps, execute_job
from taskiq_llm_queue.envelope import (
    TASK_CALL,
    TASK_EMBED,
    TASK_JOB,
    CallMeta,
    Envelope,
    LLMReply,
    task_labels,
)
from taskiq_llm_queue.errors import (
    ConfigError,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
)
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.lifecycle import (
    BrokerSet,
    MetricsHooks,
    Workers,
    breaker_settings,
    build_brokers,
    claim_settings,
    limiter_settings,
    queue_for_lane,
    queue_name,
)
from taskiq_llm_queue.metrics import MetricsSink, NullMetrics
from taskiq_llm_queue.protocols import ClaimDecision
from taskiq_llm_queue.providers.embeddings import (
    batches,
    embedding_body,
    max_batch_size,
    parse_embeddings,
)
from taskiq_llm_queue.providers.registry import ProviderRegistry
from taskiq_llm_queue.reliability.breaker import CircuitBreaker
from taskiq_llm_queue.reliability.claims import RedisClaimStore
from taskiq_llm_queue.reliability.config_store import LiveConfigStore, fallback_store
from taskiq_llm_queue.reliability.fallback import FallbackConfig
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter, SyncAdaptiveLimiter
from taskiq_llm_queue.reliability.local_limiter import LocalLimiter
from taskiq_llm_queue.results.backend import LLMQResultBackend
from taskiq_llm_queue.results.dispatcher import ResultDispatcher
from taskiq_llm_queue.routing import CanaryRouter, RoutingConfig, routing_store
from taskiq_llm_queue.routing_hooks import CallPath, QueueHooks, safe_hook
from taskiq_llm_queue.tasks import JOB_VERSION, JobFunction, WorkerResultBackend, WorkerRuntime

if TYPE_CHECKING:
    import redis

    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.envelope import FallbackPolicy, Op
    from taskiq_llm_queue.protocols import Lease, Limiter
    from taskiq_llm_queue.providers.registry import ProviderFactory
    from taskiq_llm_queue.routing_hooks import FailoverPolicy, PathRouter

logger = logging.getLogger(__name__)

Mode = Literal["send", "work", "both"]
# HTTP status the OpenAI transport reports for an error reply, by `ErrorKind` value.
_STATUS_BY_KIND: Mapping[str, int] = {"throttle": 429, "retryable": 503, "fatal": 400}
_T = TypeVar("_T")  # PEP 695 needs 3.12; the package supports 3.11


@dataclass(frozen=True, slots=True)
class ConverseResult:
    """`converse` result: the exact Bedrock Converse dict and how the call was served."""

    response: dict[str, Any]
    meta: CallMeta


@dataclass(slots=True)
class _Started:
    """Everything `start()` creates; dropped as a whole by `stop()`."""

    mode: Mode
    redis: Redis
    owns_redis: bool
    results: LLMQResultBackend
    limiter: AdaptiveLimiter
    claims: RedisClaimStore
    breaker: CircuitBreaker
    fallback: LiveConfigStore[FallbackConfig]
    dispatcher: ResultDispatcher
    brokers: BrokerSet
    runtime: WorkerRuntime
    workers: Workers | None
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)


class LLMQueue:
    """LLM request client with its own durable queue; also the in-process worker (README).

    `router` picks `sqs` or `direct` per call; without one, `routing_enabled` builds a
    `CanaryRouter` on the live routing config, else `sqs` when the model has queues. `failover`
    defaults to a `FailoverState` from the config. `hooks` defaults to `MetricsHooks(metrics)`
    (if it is also a `BrokerHooks`, the brokers report to it too). `provider_factories` and
    `redis` are injection points for tests and custom wiring.
    """

    def __init__(
        self,
        config: QueueConfig,
        *,
        pod_id: str | None = None,
        router: PathRouter | None = None,
        failover: FailoverPolicy | None = None,
        hooks: QueueHooks | None = None,
        metrics: MetricsSink | None = None,
        provider_factories: Mapping[str, ProviderFactory] | None = None,
        redis: Redis | None = None,
    ) -> None:
        """Validate providers (boot-time `ConfigError`); no I/O until `start()`."""
        self.config = config
        self.pod_id = pod_id or f"pod-{uuid.uuid4().hex[:8]}"
        self.metrics: MetricsSink = metrics or NullMetrics()
        self.router = router
        self._own_router: CanaryRouter | None = None
        self.failover: FailoverPolicy = failover or FailoverState(
            FailoverSettings(
                sqs_failover_secs=config.sqs_failover_secs,
                local_cap_fallback=config.local_limit_cap,
            ),
            probe=self._probe_redis,
            metrics=self.metrics,
        )
        self.hooks = hooks or MetricsHooks(self.metrics)
        self.keys = KeySpace(config.key_prefix)
        self._factories = dict(provider_factories or {})
        self.registry = ProviderRegistry(config, factories=self._factories)
        self._injected_redis = redis
        self._state: _Started | None = None
        self._direct_tasks: dict[str, asyncio.Task[Any]] = {}
        self._local_limiter = _LocalLimiters(self.failover, config)
        self._jobs: dict[str, JobFunction] = {}
        self._sync: _SyncRunner | None = None
        self._sync_lock = threading.Lock()
        self.sync_fallback_count = 0

    # ------------------------------------------------------------------ lifecycle

    @property
    def started(self) -> bool:
        """True between `start()` and `stop()`."""
        return self._state is not None

    async def __aenter__(self) -> LLMQueue:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def _probe_redis(self) -> object:
        """Valkey health probe for the default `FailoverState`."""
        state = self._state
        if state is None:
            raise QueueUnavailable("not started")
        return await state.redis.ping()

    def _redis_ok(self) -> None:
        """Tell the failover policy Valkey answered (ends Valkey-down mode early)."""
        report = getattr(self.failover, "report_redis_ok", None)
        if callable(report):
            safe_hook(report)

    async def _watch_limits(self, limiter: AdaptiveLimiter) -> None:
        """Feed learned limits to the failover local cap and the `llmq_limit` gauge."""
        note = getattr(self.failover, "note_limit", None)
        while True:
            await asyncio.sleep(self.config.cfg_reload_secs)
            for (model, target), limit in list(limiter.last_limits.items()):
                if callable(note):
                    safe_hook(note, model, target, limit)
                labels = {"model": model, "target": target}
                safe_hook(self.metrics.gauge, mn.LIMIT, float(limit), labels)

    async def start(self, mode: Mode = "both") -> None:
        """Connect and start: `send` (dispatcher), `work` (receivers) or `both`; idempotent."""
        if self._state is not None:
            return
        if mode not in ("send", "work", "both"):
            raise ConfigError(f"mode must be send, work or both, got {mode!r}")
        config = self.config
        owns = self._injected_redis is None
        client = self._injected_redis or _redis_client(config)
        results = LLMQResultBackend.from_config(config, redis=client)
        limiter = AdaptiveLimiter(client, self.keys, limiter_settings(config))
        claims = RedisClaimStore(client, self.keys, claim_settings(config))
        breaker = CircuitBreaker(client, self.keys, breaker_settings(config))
        default_chains = FallbackConfig.model_validate(config.fallback_default)
        fallback = fallback_store(client, self.keys, default_chains)
        dispatcher = ResultDispatcher(
            results,
            redis=client,
            channel=self.keys.done_channel(self.pod_id),
            intervals_ms=config.result_dispatch_ms,
        )
        deps = CoreDeps(
            config=config,
            keys=self.keys,
            providers=self.registry.for_model,
            limiter=limiter,
            claims=claims,
            breaker=breaker,
            results=results,
            redis=client,
            fallback=lambda: fallback.current,
            worker=self.pod_id,
            hooks=self.hooks,
        )
        runtime = WorkerRuntime(
            deps, jobs=self._jobs, defer_secs=round(2 * claims.settings.heartbeat_interval_secs)
        )
        broker_hooks = self.hooks if isinstance(self.hooks, BrokerHooks) else None
        brokers = build_brokers(
            config,
            runtime,
            limiter,
            WorkerResultBackend(results),
            failover=self.failover,
            hooks=broker_hooks,
        )
        started: list[Any] = []
        try:
            for broker in brokers.all():
                await broker.startup()
                started.append(broker)
            await fallback.load()
        except BaseException:
            for broker in started:
                with contextlib.suppress(Exception):
                    await broker.shutdown()
            if owns:
                await client.aclose()
            raise
        workers = Workers(brokers, runtime) if mode in ("work", "both") else None
        state = _Started(
            mode,
            client,
            owns,
            results,
            limiter,
            claims,
            breaker,
            fallback,
            dispatcher,
            brokers,
            runtime,
            workers,
        )
        state.tasks.append(
            asyncio.create_task(fallback.run_reload(config.cfg_reload_secs), name="llmq-fallback")
        )
        state.tasks.append(asyncio.create_task(self._watch_limits(limiter), name="llmq-limits"))
        if self.router is None and config.routing_enabled:
            default = RoutingConfig.model_validate(config.routing_default)
            self._own_router = CanaryRouter(
                routing_store(client, self.keys, default),
                metrics=self.metrics,
                reload_secs=config.cfg_reload_secs,
            )
            await self._own_router.reload()
            self._own_router.start()
        if mode in ("send", "both"):
            await dispatcher.start()
        if workers is not None:
            workers.start()
        self._state = state
        _set_default_queue(self)

    async def stop(self, drain_secs: float | None = None) -> None:
        """Ordered shutdown (§7.4): pause → drain running tasks → hand back leftovers → close.

        `drain_secs` defaults to `config.drain_secs`. Waiting callers on this pod are failed
        with `QueueUnavailable`. Idempotent.
        """
        state, self._state = self._state, None
        if state is None:
            return
        _set_default_queue(None, only=self)
        drain = self.config.drain_secs if drain_secs is None else drain_secs
        if state.workers is not None:
            await state.workers.drain(drain)
        else:
            await asyncio.gather(*(b.pause_receiving() for b in state.brokers.all()))
        router, self._own_router = self._own_router, None
        if router is not None:
            await router.aclose()
        close_failover = getattr(self.failover, "aclose", None)
        if callable(close_failover):
            with contextlib.suppress(Exception):
                await close_failover()
        for task in state.tasks:
            task.cancel()
        await asyncio.gather(*state.tasks, return_exceptions=True)
        await state.dispatcher.stop()
        for broker in state.brokers.all():
            try:
                await broker.shutdown()
            except Exception as exc:
                logger.warning("broker shutdown failed: %s", type(exc).__name__)
        await self.registry.aclose()
        if state.owns_redis:
            with contextlib.suppress(Exception):
                await state.redis.aclose()
                await state.redis.connection_pool.disconnect()
        runner, self._sync = self._sync, None
        if runner is not None:
            runner.close()

    async def pause_receiving(self) -> None:
        """For `/drain`: stop taking new messages and release unstarted ones (§7.4 step 1)."""
        if self._state is not None:
            await asyncio.gather(*(b.pause_receiving() for b in self._state.brokers.all()))

    def resume_receiving(self) -> None:
        """Undo `pause_receiving`."""
        if self._state is not None:
            for broker in self._state.brokers.all():
                broker.resume_receiving()

    # ------------------------------------------------------------------ client API

    async def converse(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API
        deadline: float | None = None,
        request_id: str | None = None,
    ) -> ConverseResult:
        """Bedrock Converse through the queue; `response` is the exact Converse dict."""
        reply = await self._call(
            "converse", model, request, lane, fallback, timeout, deadline, request_id
        )
        return ConverseResult(reply.response or {}, reply.meta)

    async def invoke_model(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API
        deadline: float | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Bedrock InvokeModel; `request` is the model body (or full kwargs with `body`).

        Returns `{"body": bytes, "contentType": str}`.
        """
        params = request if "body" in request else {"body": request}
        reply = await self._call(
            "invoke_model", model, params, lane, fallback, timeout, deadline, request_id
        )
        return decode_invoke(reply.response or {})

    async def chat(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API
        deadline: float | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """OpenAI-compatible chat completion; returns the completion body as a dict."""
        reply = await self._call(
            "chat", model, request, lane, fallback, timeout, deadline, request_id
        )
        return reply.response or {}

    async def embed(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API
        deadline: float | None = None,
    ) -> list[list[float]]:
        """Embed `texts` as one task per provider batch (Titan 1, Cohere 96, OpenAI 2048).

        Embeddings never switch model; vectors come back in input order.
        """
        if not texts:
            return []
        plan = self._embed_plan(model, texts, input_type)
        replies = await asyncio.gather(
            *(
                self._call(
                    op, model, params, lane, "same_model", timeout, deadline, None, embed=True
                )
                for op, params in plan
            )
        )
        return [
            v for (op, _), reply in zip(plan, replies, strict=True) for v in _vectors(op, reply)
        ]

    async def cancel(self, request_id: str) -> None:
        """Cancel a call: set the shared cancel flag (DS14) and stop a local `direct` run."""
        task = self._direct_tasks.get(request_id)
        if task is not None and not task.done():
            task.cancel()
        if self._state is not None:
            await self._state.claims.cancel(request_id)

    # ------------------------------------------------------------------ jobs

    def register_job(self, name: str, function: JobFunction) -> None:
        """Allow-list an app job for the `taskiq_llm_queue.job` task on this pod."""
        self._jobs[name] = function

    async def send_job(
        self,
        name: str,
        payload: dict[str, Any],
        *,
        job_id: str | None = None,
        deadline_secs: float | None = None,
    ) -> str:
        """Send one job to `config.job_queues[name]`; returns its job id (not awaited)."""
        state = self._require_started()
        queue = self.config.job_queues.get(name)
        if queue is None or state.brokers.jobs is None:
            raise ConfigError(f"no job queue configured for {name!r}")
        job_id = job_id or f"job-{uuid.uuid4().hex}"
        body: dict[str, Any] = {
            "v": JOB_VERSION,
            "request_id": job_id,
            "job": name,
            "payload": payload,
            "deadline_at": time.time() + (deadline_secs or self.config.state_ttl_secs),
        }
        task = state.brokers.jobs.find_task(TASK_JOB)
        assert task is not None
        await task.kicker().with_task_id(job_id).with_labels(queue_name=queue_name(queue)).kiq(body)
        return job_id

    # ------------------------------------------------------------------ sync

    def converse_sync(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> ConverseResult:
        """Blocking `converse`: runs `direct` under the shared limiter (sync fallback)."""
        reply = self._run_sync(model, "converse", request, lane, fallback, timeout, deadline)
        return ConverseResult(reply.response or {}, reply.meta)

    def chat_sync(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """Blocking `chat`: runs `direct` under the shared limiter (sync fallback)."""
        reply = self._run_sync(model, "chat", request, lane, fallback, timeout, deadline)
        return reply.response or {}

    def embed_sync(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> list[list[float]]:
        """Blocking `embed`: one `direct` call per provider batch under the shared limiter."""
        if not texts:
            return []
        out: list[list[float]] = []
        for op, params in self._embed_plan(model, texts, input_type):
            reply = self._run_sync(model, op, params, lane, "same_model", timeout, deadline, True)
            out.extend(_vectors(op, reply))
        return out

    # ------------------------------------------------------------------ internals

    def _require_started(self) -> _Started:
        if self._state is None:
            raise QueueUnavailable("LLMQueue is not started; call start() first")
        return self._state

    def _embed_plan(
        self, model: str, texts: list[str], input_type: str | None
    ) -> list[tuple[Op, dict[str, Any]]]:
        """(op, params) per provider batch for `texts`."""
        _, cfg, _ = self.config.resolve(model)
        size = max_batch_size(cfg.provider, cfg.model_id)
        plan: list[tuple[Op, dict[str, Any]]] = []
        for batch in batches(texts, size):
            if cfg.provider == "bedrock":
                options = (
                    {"input_type": input_type}
                    if input_type and "cohere" in cfg.model_id.lower()
                    else {}
                )
                plan.append(
                    ("invoke_model", {"body": embedding_body(cfg.model_id, batch, **options)})
                )
            else:
                plan.append(("embeddings", {"input": batch}))
        return plan

    def _envelope(
        self,
        op: Op,
        model: str,
        params: dict[str, Any],
        lane: Lane | str | None,
        fallback: FallbackPolicy | None,
        timeout: float | None,
        deadline: float | None,
        request_id: str | None,
        *,
        embed: bool = False,
    ) -> Envelope:
        """Build the envelope from `request_context` (lane, session, user) and lane timeouts."""
        self.config.resolve(model)  # unknown alias / target → ConfigError before anything runs
        ctx = current_context()
        call_lane = Lane(lane) if lane is not None else ctx.lane
        waits = self.config.embed_timeout_secs if embed else self.config.timeout_secs
        now = time.time()
        deadline_at = now + (timeout if timeout is not None else waits[call_lane])
        if deadline is not None:
            deadline_at = min(deadline_at, deadline)
        return Envelope(
            request_id=request_id or f"llm-{uuid.uuid4().hex}",
            session_id=ctx.session_id,
            user_id=ctx.user_id,
            reply_to=self.pod_id,
            lane=call_lane,
            op=op,
            fallback=fallback or "same_model",
            model=model,
            params=params,
            enqueued_at=now,
            deadline_at=max(deadline_at, now),
        )

    def _route(self, alias: str, envelope: Envelope) -> CallPath:
        """`direct` while failed over or Valkey is down, else the router, else sqs-if-queued."""
        state = self._state
        if state is None or alias not in state.brokers.by_alias:
            return "direct"
        failover = self.failover
        if failover.redis_down() or failover.path_override(alias):
            return "direct"
        router = self.router or self._own_router
        if router is not None:
            return router.route(alias, envelope.lane, envelope.session_id, envelope.user_id)
        return "sqs"

    async def _call(
        self,
        op: Op,
        model: str,
        params: dict[str, Any],
        lane: Lane | str | None,
        fallback: FallbackPolicy | None,
        timeout: float | None,  # noqa: ASYNC109  # timeout is a queue wait, not an asyncio timeout
        deadline: float | None,
        request_id: str | None,
        *,
        embed: bool = False,
    ) -> LLMReply:
        """Build, route and run one call; raises the mapped error for an `ok=False` reply."""
        envelope = self._envelope(
            op, model, params, lane, fallback, timeout, deadline, request_id, embed=embed
        )
        alias = model.partition("@")[0]
        path = self._route(alias, envelope)
        t0 = time.monotonic()
        ok = False
        try:
            if path == "sqs":
                reply = await self._via_sqs(alias, envelope, embed=embed)
            else:
                reply = await self._direct(envelope)
            result = _unwrap(reply)
            ok = True
            return result
        finally:
            secs = time.monotonic() - t0
            safe_hook(self.hooks.on_call, alias, envelope.lane.value, path, ok, secs)

    async def _via_sqs(self, alias: str, envelope: Envelope, *, embed: bool) -> LLMReply:
        """Kiq to the model's lane queue and await the reply (§6.1); failover to `direct`."""
        state = self._require_started()
        broker = state.brokers.by_alias[alias]
        queue = queue_for_lane(self.config.models[alias], envelope.lane)
        assert queue is not None
        sent = envelope
        try:
            sent = await self._maybe_by_reference(state, envelope)
        except RedisError as exc:
            self._report_redis(exc)
            return await self._direct(envelope)
        labels = {
            **task_labels(sent, hash_ids=self.config.hash_ids),
            "queue_name": queue_name(queue),
            "expiry": repr(sent.deadline_at),
        }
        task = broker.find_task(TASK_EMBED if embed else TASK_CALL)
        assert task is not None
        try:
            await (
                task.kicker()
                .with_task_id(sent.request_id)
                .with_labels(**labels)
                .kiq(sent.model_dump(mode="json"))
            )
        except (
            SendTaskError,
            SQSSendError,
            QueueUnavailable,
            OSError,
        ) as exc:  # taskiq wraps kick errors
            logger.warning("SQS send for %s failed (%s); running direct", alias, type(exc).__name__)
            self.failover.report_sqs_failure(alias, exc)
            return await self._direct(envelope, claim=True)
        return await self._wait(state, envelope)

    async def _wait(self, state: _Started, envelope: Envelope) -> LLMReply:
        """Await the dispatcher; on timeout or caller cancellation set the cancel flag."""
        remaining = max(0.0, envelope.deadline_at - time.time())
        try:
            reply = await state.dispatcher.wait(envelope.request_id, remaining, envelope.lane)
        except LLMQueueTimeout:
            await self._flag_cancel(state, envelope.request_id)
            raise
        except asyncio.CancelledError:
            await asyncio.shield(self._flag_cancel(state, envelope.request_id))
            raise
        self._redis_ok()  # the reply came through Valkey
        return reply

    async def _flag_cancel(self, state: _Started, request_id: str) -> None:
        with contextlib.suppress(Exception):
            await state.claims.cancel(request_id)

    async def _maybe_by_reference(self, state: _Started, envelope: Envelope) -> Envelope:
        """Store params over `inline_max_bytes` in Valkey and send `params_ref` instead (D48)."""
        size = len(envelope.model_dump_json().encode())
        if size <= self.config.inline_max_bytes:
            return envelope
        key = self.keys.params(envelope.request_id)
        await state.redis.set(key, orjson.dumps(envelope.params), ex=self.config.state_ttl_secs)
        return envelope.model_copy(update={"params": None, "params_ref": key})

    def _report_redis(self, exc: BaseException) -> None:
        self.failover.report_redis_failure(exc)

    def _direct_deps(self) -> CoreDeps:
        """Deps for an in-process call: shared limiter + breaker, or the local cap (DS18)."""
        state = self._require_started()
        if self.failover.redis_down():
            return CoreDeps(
                config=self.config,
                keys=self.keys,
                providers=self.registry.for_model,
                limiter=self._local_limiter,
                worker=f"direct:{self.pod_id}",
                hooks=self.hooks,
            )
        return CoreDeps(
            config=self.config,
            keys=self.keys,
            providers=self.registry.for_model,
            limiter=state.limiter,
            claims=state.claims,
            breaker=state.breaker,
            redis=state.redis,
            fallback=lambda: state.fallback.current,
            worker=f"direct:{self.pod_id}",
            hooks=self.hooks,
        )

    async def _direct(self, envelope: Envelope, *, claim: bool = False) -> LLMReply:
        """Run in-process; with `claim` (DS17) a queued copy that already runs is awaited."""
        deps = self._direct_deps()
        task = asyncio.current_task()
        if task is not None:
            self._direct_tasks[envelope.request_id] = task
        try:
            result = await execute_job(
                envelope, deps, claim=claim and deps.claims is not None, store=False
            )
        except RedisError as exc:
            self._report_redis(exc)
            deps = self._direct_deps()
            result = await execute_job(envelope, deps, claim=False, store=False)
        finally:
            self._direct_tasks.pop(envelope.request_id, None)
        if result.reply is not None:
            return result.reply
        if result.decision is ClaimDecision.ACK_CANCELLED:
            raise LLMQueueError(f"call {envelope.request_id} was cancelled")
        state = self._require_started()
        return await self._wait(state, envelope)  # the queued copy runs elsewhere

    def _run_sync(
        self,
        model: str,
        op: Op,
        params: dict[str, Any],
        lane: Lane | str | None,
        fallback: FallbackPolicy | None,
        timeout: float | None,
        deadline: float | None,
        embed: bool = False,
    ) -> LLMReply:
        """Run one call `direct` on the private sync loop; counts `llmq_sync_fallback_total`."""
        if self.config.strict_async:
            raise LLMQueueError(f"sync {op} call refused: strict_async is set")
        envelope = self._envelope(
            op, model, params, lane, fallback, timeout, deadline, None, embed=embed
        )
        self.sync_fallback_count += 1
        safe_hook(self.hooks.on_sync_fallback, model.partition("@")[0])
        with self._sync_lock:
            if self._sync is None:
                self._sync = _SyncRunner(self)
            runner = self._sync
        reply = runner.run(runner.execute(envelope), envelope.deadline_at - time.time() + 5)
        return _unwrap(reply)


class _LocalLimiters:
    """`Limiter` for Valkey-down mode (DS18): the failover policy's per-target local caps.

    Uses `FailoverState.local_limiter(model, target)` when the policy has it, else one
    `LocalLimiter` capped at `local_limit_cap`.
    """

    def __init__(self, failover: FailoverPolicy, config: QueueConfig) -> None:
        self._failover = failover
        self._fallback = LocalLimiter(
            config.local_limit_cap,
            bg_share=config.limiter.bg_share,
            pause_secs=config.limiter.pause_secs,
        )

    def _for(self, model: str, target: str) -> LocalLimiter:
        factory = getattr(self._failover, "local_limiter", None)
        limiter = factory(model, target) if callable(factory) else None
        return limiter if isinstance(limiter, LocalLimiter) else self._fallback

    async def acquire(
        self, model: str, target: str, lane: Lane, timeout_secs: float
    ) -> Lease | None:
        return await self._for(model, target).acquire(model, target, lane, timeout_secs)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        await self._for(lease.model, lease.target).release(lease, throttled=throttled)

    def gate(self, model: str, target: str) -> Any:
        return self._for(model, target).gate(model, target)


class _SyncLimiter:
    """`Limiter` over `SyncAdaptiveLimiter`, run in threads so the private loop never blocks."""

    def __init__(self, inner: SyncAdaptiveLimiter) -> None:
        self._inner = inner

    async def acquire(
        self, model: str, target: str, lane: Lane, timeout_secs: float
    ) -> Lease | None:
        return await asyncio.to_thread(self._inner.acquire, model, target, lane, timeout_secs)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        await asyncio.to_thread(self._inner.release, lease, throttled=throttled)

    def gate(self, model: str, target: str) -> Any:
        raise NotImplementedError("sync limiter has no receive gate")


class _SyncRunner:
    """A daemon thread with its own event loop and providers for sync callers (R22).

    Providers bind their HTTP clients to a loop, so sync calls get their own registry; the
    limiter is the shared Valkey one through a blocking client (or the local cap, DS18).
    """

    def __init__(self, owner: LLMQueue) -> None:
        self._owner = owner
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="llmq-sync", daemon=True
        )
        self._thread.start()
        self._registry = ProviderRegistry(owner.config, factories=owner._factories)
        self._redis: redis.Redis | None = None

    def run(self, coro: Coroutine[Any, Any, _T], timeout_secs: float) -> _T:
        """Run `coro` on the private loop and block for its result."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(max(1.0, timeout_secs))
        except TimeoutError as exc:
            future.cancel()
            raise LLMQueueTimeout("sync call timed out") from exc

    def _limiter(self) -> Limiter:
        owner = self._owner
        if owner.failover.redis_down():
            return owner._local_limiter
        if self._redis is None:
            import redis as sync_redis

            cfg = owner.config
            self._redis = sync_redis.Redis(
                host=cfg.redis_host,
                port=cfg.redis_port,
                password=cfg.redis_password,
                ssl=cfg.redis_ssl,
            )
        return _SyncLimiter(
            SyncAdaptiveLimiter(self._redis, owner.keys, limiter_settings(owner.config))
        )

    async def execute(self, envelope: Envelope) -> LLMReply:
        owner = self._owner
        deps = CoreDeps(
            config=owner.config,
            keys=owner.keys,
            providers=self._registry.for_model,
            limiter=self._limiter(),
            worker=f"sync:{owner.pod_id}",
            hooks=owner.hooks,
        )
        try:
            result = await execute_job(envelope, deps, claim=False, store=False)
        except RedisError as exc:
            owner._report_redis(exc)
            deps.limiter = owner._local_limiter
            result = await execute_job(envelope, deps, claim=False, store=False)
        assert result.reply is not None
        return result.reply

    def close(self) -> None:
        """Close providers, stop the loop and the thread."""
        with contextlib.suppress(Exception):
            asyncio.run_coroutine_threadsafe(self._registry.aclose(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        if self._redis is not None:
            self._redis.close()


def decode_invoke(response: Mapping[str, Any]) -> dict[str, Any]:
    """`{"body_b64", "contentType"}` from a worker → `{"body": bytes, "contentType": str}`."""
    return {
        "body": base64.b64decode(response.get("body_b64", "")),
        "contentType": str(response.get("contentType", "")),
    }


def _vectors(op: Op, reply: LLMReply) -> list[list[float]]:
    """Vectors from one embed reply: Bedrock InvokeModel body or OpenAI embeddings `data`."""
    response = reply.response or {}
    if op == "invoke_model":
        return parse_embeddings(orjson.loads(decode_invoke(response)["body"]))
    data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
    return [list(item["embedding"]) for item in data]


def _unwrap(reply: LLMReply) -> LLMReply:
    """Return an ok reply; map an error reply to `LLMQueueTimeout` / `ProviderError`."""
    if reply.ok:
        return reply
    error = reply.error or {}
    code = str(error.get("type", "error"))
    message = str(error.get("message", code))
    if reply.cancelled or code == "cancelled":
        raise LLMQueueError(f"call {reply.request_id} was cancelled")
    if code == "deadline_exceeded":
        raise LLMQueueTimeout(f"call {reply.request_id} ran past its deadline")
    retry_after = error.get("retry_after_secs")
    exc = ProviderError(
        f"{code}: {message}",
        retryable=bool(error.get("retryable", False)),
        retry_after_secs=float(retry_after) if retry_after is not None else None,
    )
    exc.status_code = _STATUS_BY_KIND.get(str(error.get("kind", "")), 502)  # type: ignore[attr-defined]  # optional attribute read by http_transport
    raise exc


def _redis_client(config: QueueConfig) -> Redis:
    """The package's own bounded pool (no `decode_responses`), separate from the app's."""
    pool = BlockingConnectionPool(
        connection_class=SSLConnection if config.redis_ssl else Connection,
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        max_connections=config.redis_max_connections,
        timeout=5.0,
    )
    return Redis(connection_pool=pool)


def _set_default_queue(llmq: LLMQueue | None, *, only: LLMQueue | None = None) -> None:
    """Register (or clear) the adapters' default queue; clearing only if it is still `only`."""
    from taskiq_llm_queue.langchain._client import get_default_queue, set_default_queue

    if only is not None:
        try:
            if get_default_queue() is not only:
                return
        except LLMQueueError:
            return
    set_default_queue(llmq)
