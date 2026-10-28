"""`LLMQueue`: the public client. Sends calls through SQS (or runs them `direct`) and works them.

One instance per process. `start(mode)` connects Valkey and SQS, starts the result dispatcher
(senders) and the in-process taskiq receivers (workers); `stop()` drains in the order of §7.4.
Calls build an envelope from `request_context`, route it, and await the reply (§6.1).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import orjson
from redis.asyncio import Redis
from redis.exceptions import RedisError
from taskiq.exceptions import SendTaskError

from taskiq_llm_queue import metrics_names as mn
from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.broker.errors import ErrorFamily, SQSSendError
from taskiq_llm_queue.broker.events import BrokerHooks
from taskiq_llm_queue.config import Lane, create_redis, create_sync_redis
from taskiq_llm_queue.context import current_context
from taskiq_llm_queue.core import CoreDeps, InfraError, JobResult, execute_job
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
    LLMQueueCancelled,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
)
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.lifecycle import (
    BrokerSet,
    ClaimGroup,
    MetricsHooks,
    PodHeartbeat,
    PodRegistry,
    Workers,
    breaker_settings,
    build_brokers,
    claim_settings,
    limiter_settings,
    queue_for_lane,
    queue_name,
)
from taskiq_llm_queue.metrics import MetricsSink, NullMetrics
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState
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
    from aiobotocore.session import AioSession

    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.envelope import FallbackPolicy, Op
    from taskiq_llm_queue.protocols import Lease, Limiter
    from taskiq_llm_queue.providers.registry import ProviderFactory
    from taskiq_llm_queue.routing_hooks import FailoverPolicy, PathRouter

logger = get_logger(__name__)

Mode = Literal["send", "work", "both"]
# HTTP status the OpenAI transport reports for an error reply, by `ErrorKind` value.
_STATUS_BY_KIND: Mapping[str, int] = {"throttle": 429, "retryable": 503, "fatal": 400}
_T = TypeVar("_T")  # PEP 695 needs 3.12; the package supports 3.11


@dataclass(frozen=True, slots=True)
class ConverseResult:
    """`converse` result: the exact Bedrock Converse dict and how the call was served."""

    response: dict[str, Any]
    meta: CallMeta


@dataclass(frozen=True, slots=True)
class ChatResult:
    """`chat` result: the OpenAI-compatible chat completion body and how it was served."""

    response: dict[str, Any]
    meta: CallMeta


@dataclass(frozen=True, slots=True)
class InvokeResult:
    """`invoke_model` result: the raw model body, its content type and how it was served.

    `response` gives the InvokeModel-style `{"body": bytes, "contentType": str}`; `json()`
    parses a JSON body.
    """

    body: bytes
    content_type: str
    meta: CallMeta

    @property
    def response(self) -> dict[str, Any]:
        """`{"body": bytes, "contentType": str}`, as boto3's InvokeModel reply has them."""
        return {"body": self.body, "contentType": self.content_type}

    def json(self) -> Any:
        """The body parsed as JSON (embedding and most text models reply in JSON)."""
        return orjson.loads(self.body)


@dataclass(frozen=True, slots=True)
class EmbedResult:
    """`embed` result: one vector per input text, in input order, and how they were served.

    It also behaves as the list of vectors (`len`, iteration, indexing). With several provider
    batches, `meta` merges them: first batch's model/target, summed attempts, slowest waits.
    """

    vectors: list[list[float]]
    meta: CallMeta

    def __len__(self) -> int:
        return len(self.vectors)

    def __iter__(self) -> Iterator[list[float]]:
        return iter(self.vectors)

    def __getitem__(self, index: int) -> list[float]:
        return self.vectors[index]


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
    pods: PodRegistry | None = None
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)


class LLMQueue:
    """LLM request client with its own durable queue; also the in-process worker (README).

    `router` picks `sqs` or `direct` per call; without one, `routing_enabled` builds a
    `CanaryRouter` on the live routing config, else `sqs` when the model has queues. `failover`
    defaults to a `FailoverState` from the config. `hooks` defaults to `MetricsHooks(metrics)`
    (if it is also a `BrokerHooks`, the brokers report to it too). `provider_factories`,
    `redis` and `aws_session` are injection points for tests and custom wiring.
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
        aws_session: AioSession | None = None,
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
        self._injected_session = aws_session
        self.aws_session: AioSession | None = aws_session
        self._state: _Started | None = None
        self._direct_tasks: dict[str, asyncio.Task[Any]] = {}
        self._embed_groups: dict[str, list[str]] = {}  # embed request id -> batch request ids
        # DS17 claims held as `direct:<pod>` (request id -> holders) and every in-flight lease
        # of this pod (worker, direct and sync paths): both kept alive by the pod heartbeat.
        self._direct_claims: dict[str, int] = {}
        self._leases: set[Lease] = set()
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
        client = self._injected_redis or create_redis(config, client_name=self._redis_name())
        # One AWS session (credential chain, IRSA refresh) for every provider of this pod.
        self.aws_session = self._injected_session or _new_aws_session()
        self.registry.aws_session = self.aws_session
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
            leases=self._leases,
        )
        runtime = WorkerRuntime(
            deps,
            jobs=self._jobs,
            defer_secs=round(2 * claims.settings.heartbeat_interval_secs),
            failover=self.failover,
        )
        broker_hooks = self.hooks if isinstance(self.hooks, BrokerHooks) else None
        brokers = build_brokers(
            config,
            runtime,
            limiter,
            WorkerResultBackend(results),
            failover=self.failover,
            hooks=broker_hooks,
            session=self.aws_session,
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
        interval = claims.settings.heartbeat_interval_secs
        pods = PodRegistry(client, self.keys.pods, self.pod_id, ttl_secs=3 * interval)
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
            pods,
        )
        heartbeat = self._pod_heartbeat(state, interval)
        state.tasks.append(
            asyncio.create_task(fallback.run_reload(config.cfg_reload_secs), name="llmq-fallback")
        )
        state.tasks.append(asyncio.create_task(heartbeat.run(), name="llmq-pod-heartbeat"))
        state.tasks.append(asyncio.create_task(self._watch_limits(limiter), name="llmq-limits"))
        if self.router is None and config.routing_enabled:
            default = RoutingConfig.model_validate(config.routing_default)
            self._own_router = CanaryRouter(
                routing_store(client, self.keys, default),
                metrics=self.metrics,
                reload_secs=config.cfg_reload_secs,
                salt=config.id_hash_key,
            )
            await self._own_router.reload()
            self._own_router.start()
        if mode in ("send", "both"):
            await dispatcher.start()
        if workers is not None:
            workers.start()
        self._state = state
        _set_default_queue(self)

    def _pod_heartbeat(self, state: _Started, interval: float) -> PodHeartbeat:
        """One heartbeat for every mode: worker claims, DS17 direct claims, all leases (DS11)."""
        runtime = state.runtime

        def worker_lost(lost: list[str]) -> None:
            runtime.stop_running("claim_lost", lost)

        def direct_lost(lost: list[str]) -> None:
            logger.warning("direct_claims_lost", count=len(lost))

        set_live = getattr(self.failover, "set_live_pods", None)
        return PodHeartbeat(
            claims=state.claims,
            limiter=state.limiter,
            leases=self._leases,
            groups=[
                ClaimGroup(self.pod_id, runtime.running_ids, worker_lost),
                ClaimGroup(f"direct:{self.pod_id}", lambda: list(self._direct_claims), direct_lost),
            ],
            interval_secs=interval,
            pods=state.pods,
            on_live_pods=set_live if callable(set_live) else None,
        )

    async def stop(self, drain_secs: float | None = None) -> None:
        """Ordered shutdown (§7.4): pause → drain running tasks → hand back leftovers → close.

        `drain_secs` defaults to `config.drain_secs`. Waiting callers on this pod are failed
        with `QueueUnavailable`. Also closes the sync runner (even if `start()` never ran, e.g.
        when only `*_sync` calls were made). Idempotent.
        """
        state, self._state = self._state, None
        if state is None:
            await asyncio.to_thread(self.close_sync)
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
                logger.warning("broker_shutdown_failed", error_type=type(exc).__name__)
        await self.registry.aclose()
        if state.pods is not None:
            with contextlib.suppress(Exception):
                await state.pods.leave()
        if state.owns_redis:
            with contextlib.suppress(Exception):
                await state.redis.aclose()
                await state.redis.connection_pool.disconnect()
        await asyncio.to_thread(self.close_sync)

    def close_sync(self) -> None:
        """Close the sync runner (thread, loop, providers, blocking Valkey client); idempotent.

        For sync-only apps that never call `start()`/`stop()`; a later sync call starts a new one.
        """
        with self._sync_lock:
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
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> ConverseResult:
        """Bedrock Converse through the queue; `response` is the exact Converse dict.

        `timeout_secs` (relative) defaults to the lane timeout; `deadline_at` (epoch seconds)
        caps it. Raises `LLMQueueTimeout`, `LLMQueueCancelled` or `ProviderError`.
        """
        reply = await self._call(
            "converse", model, request, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return ConverseResult(reply.response or {}, reply.meta)

    async def invoke_model(
        self,
        model: str,
        body: Mapping[str, Any] | list[Any] | str | bytes,
        *,
        content_type: str = "application/json",
        accept: str = "application/json",
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> InvokeResult:
        """Bedrock InvokeModel with the model-native `body` (JSON value, or UTF-8 text/bytes).

        Returns the raw reply body (`InvokeResult.body`, `.json()`) and how it was served.
        """
        params = {"body": _invoke_body(body), "contentType": content_type, "accept": accept}
        reply = await self._call(
            "invoke_model", model, params, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return _invoke_result(reply)

    async def chat(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> ChatResult:
        """OpenAI-compatible chat completion; `response` is the completion body as a dict."""
        reply = await self._call(
            "chat", model, request, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return ChatResult(reply.response or {}, reply.meta)

    async def embed(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> EmbedResult:
        """Embed `texts` as one task per provider batch (Titan 1, Cohere 96, OpenAI 2048).

        Embeddings never switch model; vectors come back in input order. With several batches
        each task gets `<request_id>-<n>`; `cancel(request_id)` cancels all of them.
        """
        if not texts:
            return EmbedResult([], CallMeta())
        plan = self._embed_plan(model, texts, input_type)
        rid = request_id or _new_request_id()
        ids = _batch_ids(rid, len(plan))
        parent = rid if len(plan) > 1 else None
        self._embed_groups[rid] = ids
        try:
            replies = await asyncio.gather(
                *(
                    self._call(
                        op,
                        model,
                        params,
                        lane,
                        "same_model",
                        timeout_secs,
                        deadline_at,
                        batch_id,
                        embed=True,
                        parent_request_id=parent,
                    )
                    for (op, params), batch_id in zip(plan, ids, strict=True)
                )
            )
        finally:
            self._embed_groups.pop(rid, None)
        return _embed_result(plan, replies)

    async def cancel(self, request_id: str) -> None:
        """Cancel a call, or every batch of an `embed`; its caller gets `LLMQueueCancelled`.

        Sets the shared cancel flag (DS14) and stops a local `direct` run.
        """
        for rid in self._embed_groups.get(request_id) or [request_id]:
            task = self._direct_tasks.get(rid)
            if task is not None and not task.done():
                task.cancel()
            if self._state is not None:
                await self._state.claims.cancel(rid)

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
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
    ) -> str:
        """Send one job to `config.job_queues[name]`; returns its job id (not awaited).

        The job must finish within `timeout_secs` (default `state_ttl_secs`), capped by
        `deadline_at` (epoch seconds).
        """
        state = self._require_started()
        queue = self.config.job_queues.get(name)
        if queue is None or state.brokers.jobs is None:
            raise ConfigError(f"no job queue configured for {name!r}")
        job_id = job_id or f"job-{uuid.uuid4().hex}"
        now = time.time()
        deadline = now + (timeout_secs if timeout_secs is not None else self.config.state_ttl_secs)
        if deadline_at is not None:
            deadline = min(deadline, deadline_at)
        body: dict[str, Any] = {
            "v": JOB_VERSION,
            "request_id": job_id,
            "job": name,
            "payload": payload,
            "deadline_at": max(deadline, now),
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
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> ConverseResult:
        """Blocking `converse`: runs `direct` under the shared limiter (sync fallback)."""
        reply = self._run_sync(
            model, "converse", request, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return ConverseResult(reply.response or {}, reply.meta)

    def chat_sync(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> ChatResult:
        """Blocking `chat`: runs `direct` under the shared limiter (sync fallback)."""
        reply = self._run_sync(
            model, "chat", request, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return ChatResult(reply.response or {}, reply.meta)

    def invoke_model_sync(
        self,
        model: str,
        body: Mapping[str, Any] | list[Any] | str | bytes,
        *,
        content_type: str = "application/json",
        accept: str = "application/json",
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> InvokeResult:
        """Blocking `invoke_model`: runs `direct` under the shared limiter (sync fallback)."""
        params = {"body": _invoke_body(body), "contentType": content_type, "accept": accept}
        reply = self._run_sync(
            model, "invoke_model", params, lane, fallback, timeout_secs, deadline_at, request_id
        )
        return _invoke_result(reply)

    def embed_sync(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout_secs: float | None = None,
        deadline_at: float | None = None,
        request_id: str | None = None,
    ) -> EmbedResult:
        """Blocking `embed`: one `direct` call per provider batch under the shared limiter."""
        if not texts:
            return EmbedResult([], CallMeta())
        plan = self._embed_plan(model, texts, input_type)
        ids = _batch_ids(request_id or _new_request_id(), len(plan))
        replies = [
            self._run_sync(
                model, op, params, lane, "same_model", timeout_secs, deadline_at, rid, embed=True
            )
            for (op, params), rid in zip(plan, ids, strict=True)
        ]
        return _embed_result(plan, replies)

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
        timeout_secs: float | None,
        deadline_at: float | None,
        request_id: str | None,
        *,
        embed: bool = False,
        parent_request_id: str | None = None,
    ) -> Envelope:
        """Build the envelope from `request_context` (lane, session, user) and lane timeouts."""
        self.config.resolve(model)  # unknown alias / target → ConfigError before anything runs
        ctx = current_context()
        call_lane = Lane(lane) if lane is not None else ctx.lane
        waits = self.config.embed_timeout_secs if embed else self.config.timeout_secs
        now = time.time()
        deadline = now + (timeout_secs if timeout_secs is not None else waits[call_lane])
        if deadline_at is not None:
            deadline = min(deadline, deadline_at)
        return Envelope(
            request_id=request_id or _new_request_id(),
            parent_request_id=parent_request_id,
            session_id=ctx.session_id,
            user_id=ctx.user_id,
            reply_to=self.pod_id,
            lane=call_lane,
            op=op,
            fallback=fallback or "same_model",
            model=model,
            params=params,
            enqueued_at=now,
            deadline_at=max(deadline, now),
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
        timeout_secs: float | None,
        deadline_at: float | None,
        request_id: str | None,
        *,
        embed: bool = False,
        parent_request_id: str | None = None,
    ) -> LLMReply:
        """Build, route and run one call; raises the mapped error for an `ok=False` reply."""
        envelope = self._envelope(
            op,
            model,
            params,
            lane,
            fallback,
            timeout_secs,
            deadline_at,
            request_id,
            embed=embed,
            parent_request_id=parent_request_id,
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
            **task_labels(sent, hash_ids=self.config.hash_ids, salt=self.config.id_hash_key),
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
        except asyncio.CancelledError:  # a batched send may still go out: flag it (DS14)
            await asyncio.shield(self._flag_cancel(state, envelope.request_id))
            raise
        except (
            SendTaskError,
            SQSSendError,
            QueueUnavailable,
            OSError,
        ) as exc:  # taskiq wraps kick errors
            rejected = _deterministic_send_error(exc)
            if rejected is not None:  # the same message would fail again: no failover
                logger.warning(
                    "sqs_send_rejected",
                    request_id=envelope.request_id,
                    model=alias,
                    error_type=type(rejected).__name__,
                )
                if isinstance(rejected, LLMQueueError):
                    raise rejected from exc
                raise LLMQueueError(f"send rejected: {type(rejected).__name__}") from exc
            logger.warning(
                "sqs_send_failed",
                request_id=envelope.request_id,
                model=alias,
                lane=envelope.lane.value,
                error_type=type(exc).__name__,
            )
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
            leases=self._leases,
        )

    async def _direct(self, envelope: Envelope, *, claim: bool = False) -> LLMReply:
        """Run in-process; with `claim` (DS17) a queued copy that already runs is awaited.

        The DS17 claim is heartbeated by the pod heartbeat while the call runs. If Valkey fails
        before any provider attempt the call runs again under the local cap; after one started,
        the caller gets `QueueUnavailable` (never a second provider call).
        """
        deps = self._direct_deps()
        rid = envelope.request_id
        held = claim and deps.claims is not None
        task = asyncio.current_task()
        if task is not None:
            self._direct_tasks[rid] = task
        if held:
            self._direct_claims[rid] = self._direct_claims.get(rid, 0) + 1
        try:
            try:
                result = await execute_job(envelope, deps, claim=held, store=False)
            except InfraError as exc:
                self._report_redis(exc.__cause__ or exc)
                if exc.attempted:
                    raise QueueUnavailable(f"Valkey failed during call {rid}") from exc
                result = await self._direct_again(envelope)
                if held:
                    await self._finish_direct_claim(rid, result.reply)
        finally:
            self._direct_tasks.pop(rid, None)
            if held:
                self._drop_direct_claim(rid)
        if result.reply is not None:
            return result.reply
        if result.decision is ClaimDecision.ACK_CANCELLED:
            raise LLMQueueCancelled(f"call {envelope.request_id} was cancelled")
        state = self._require_started()
        return await self._wait(state, envelope)  # the queued copy runs elsewhere

    async def _direct_again(self, envelope: Envelope) -> JobResult:
        """Second, unclaimed run after Valkey failed before any attempt (local cap, DS18)."""
        try:
            return await execute_job(envelope, self._direct_deps(), claim=False, store=False)
        except InfraError as exc:
            self._report_redis(exc.__cause__ or exc)
            raise QueueUnavailable(f"Valkey failed during call {envelope.request_id}") from exc

    async def _finish_direct_claim(self, request_id: str, reply: LLMReply | None) -> None:
        """Best effort: mark a DS17 claim terminal after the local re-run answered the caller."""
        state = self._state
        if state is None or reply is None:
            return
        final = ClaimState.CANCELLED if reply.cancelled else ClaimState.DONE
        try:
            await state.claims.complete(request_id, final, worker=f"direct:{self.pod_id}")
        except Exception as exc:
            logger.warning(
                "claim_complete_failed", request_id=request_id, error_type=type(exc).__name__
            )

    def _drop_direct_claim(self, request_id: str) -> None:
        left = self._direct_claims.get(request_id, 0) - 1
        if left > 0:
            self._direct_claims[request_id] = left
        else:
            self._direct_claims.pop(request_id, None)

    def _run_sync(
        self,
        model: str,
        op: Op,
        params: dict[str, Any],
        lane: Lane | str | None,
        fallback: FallbackPolicy | None,
        timeout_secs: float | None,
        deadline_at: float | None,
        request_id: str | None,
        *,
        embed: bool = False,
    ) -> LLMReply:
        """Run one call `direct` on the private sync loop; counts `llmq_sync_fallback_total`."""
        if self.config.strict_async:
            raise LLMQueueError(f"sync {op} call refused: strict_async is set")
        envelope = self._envelope(
            op, model, params, lane, fallback, timeout_secs, deadline_at, request_id, embed=embed
        )
        self.sync_fallback_count += 1
        safe_hook(self.hooks.on_sync_fallback, model.partition("@")[0])
        with self._sync_lock:
            if self._sync is None:
                self._sync = _SyncRunner(self)
            runner = self._sync
        reply = runner.run(runner.execute(envelope), envelope.deadline_at - time.time() + 5)
        return _unwrap(reply)

    def _redis_name(self) -> str | None:
        """`CLIENT SETNAME` value: the configured name plus this pod's id."""
        base = self.config.redis_client_name
        return f"{base}:{self.pod_id}" if base else None


class _LocalLimiters:
    """`Limiter` for Valkey-down mode (DS18): the failover policy's per-target local caps.

    Uses `FailoverState.local_limiter(model, target)` when the policy has it, else one
    `LocalLimiter` capped at `local_limit_cap`. A lease goes back to the limiter that issued it,
    even after the policy rebuilt its limiters.
    """

    def __init__(self, failover: FailoverPolicy, config: QueueConfig) -> None:
        self._failover = failover
        self._fallback = LocalLimiter(
            config.local_limit_cap,
            bg_share=config.limiter.bg_share,
            pause_secs=config.limiter.pause_secs,
        )
        self._issued: dict[str, LocalLimiter] = {}

    def _for(self, model: str, target: str) -> LocalLimiter:
        factory = getattr(self._failover, "local_limiter", None)
        limiter = factory(model, target) if callable(factory) else None
        return limiter if isinstance(limiter, LocalLimiter) else self._fallback

    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        limiter = self._for(model, target)
        lease = await limiter.acquire(model, target, lane, timeout_secs, should_abort=should_abort)
        if lease is not None:
            self._issued[lease.token] = limiter
        return lease

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        limiter = self._issued.pop(lease.token, None) or self._for(lease.model, lease.target)
        await limiter.release(lease, throttled=throttled)

    def gate(self, model: str, target: str) -> Any:
        return self._for(model, target).gate(model, target)


class _SyncLimiter:
    """`Limiter` over `SyncAdaptiveLimiter`, run in threads so the private loop never blocks."""

    def __init__(self, inner: SyncAdaptiveLimiter) -> None:
        self._inner = inner
        self._orphans: set[asyncio.Future[None]] = set()

    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        """Take a slot in a thread; a lease granted after the caller gave up is released."""
        abort = _sync_abort(should_abort, asyncio.get_running_loop())
        future = asyncio.ensure_future(
            asyncio.to_thread(
                self._inner.acquire, model, target, lane, timeout_secs, should_abort=abort
            )
        )
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:  # sync timeout: the thread may still win a slot
            future.add_done_callback(self._release_orphan)
            raise

    def _release_orphan(self, future: asyncio.Future[Lease | None]) -> None:
        if future.cancelled() or future.exception() is not None:
            return
        lease = future.result()
        if lease is not None:
            self._orphans.add(asyncio.ensure_future(self.release(lease, throttled=False)))
            for done in [f for f in self._orphans if f.done()]:
                self._orphans.discard(done)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        await asyncio.to_thread(self._inner.release, lease, throttled=throttled)

    def gate(self, model: str, target: str) -> Any:
        raise NotImplementedError("sync limiter has no receive gate")


def _sync_abort(
    should_abort: Callable[[], Awaitable[bool]] | None, loop: asyncio.AbstractEventLoop
) -> Callable[[], bool] | None:
    """Run an async `should_abort` on `loop` from the limiter's thread (False on any error)."""
    if should_abort is None:
        return None

    async def call() -> bool:
        return await should_abort()

    def check() -> bool:
        try:
            return asyncio.run_coroutine_threadsafe(call(), loop).result(5)
        except Exception:
            return False

    return check


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
            self._redis = create_sync_redis(owner.config, client_name=owner._redis_name())
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
            leases=owner._leases,  # renewed by the pod heartbeat (DS11)
        )
        try:
            result = await execute_job(envelope, deps, claim=False, store=False)
        except InfraError as exc:
            owner._report_redis(exc.__cause__ or exc)
            if exc.attempted:  # never a second provider call
                raise QueueUnavailable(f"Valkey failed during call {envelope.request_id}") from exc
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


def _new_request_id() -> str:
    return f"llm-{uuid.uuid4().hex}"


def _new_aws_session() -> AioSession:
    """A fresh aiobotocore session (credential chain resolved once, shared by its clients)."""
    from aiobotocore.session import get_session

    return get_session()


def _batch_ids(request_id: str, count: int) -> list[str]:
    """Request id per embed batch: the id itself for one batch, else `<id>-<n>`."""
    return [request_id] if count == 1 else [f"{request_id}-{i}" for i in range(count)]


def _invoke_body(body: Mapping[str, Any] | list[Any] | str | bytes) -> Any:
    """Envelope-safe InvokeModel body: JSON values as-is, bytes as UTF-8 text."""
    if isinstance(body, bytes):
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invoke_model body bytes must be UTF-8 text") from exc
    if isinstance(body, Mapping):
        return dict(body)
    return body


def _invoke_result(reply: LLMReply) -> InvokeResult:
    raw = decode_invoke(reply.response or {})
    return InvokeResult(raw["body"], raw["contentType"], reply.meta)


def _merge_meta(metas: Sequence[CallMeta]) -> CallMeta:
    """One `CallMeta` for several batches: first model/target, summed attempts, slowest waits."""
    if not metas:
        return CallMeta()
    first = metas[0]
    return CallMeta(
        served_model=first.served_model,
        served_target=first.served_target,
        fallback_used=any(m.fallback_used for m in metas),
        queue_wait_ms=max(m.queue_wait_ms for m in metas),
        provider_ms=max(m.provider_ms for m in metas),
        attempts=sum(m.attempts for m in metas),
        worker=first.worker,
    )


def _embed_result(plan: Sequence[tuple[Op, Any]], replies: Sequence[LLMReply]) -> EmbedResult:
    vectors = [v for (op, _), reply in zip(plan, replies, strict=True) for v in _vectors(op, reply)]
    return EmbedResult(vectors, _merge_meta([r.meta for r in replies]))


def _vectors(op: Op, reply: LLMReply) -> list[list[float]]:
    """Vectors from one embed reply: Bedrock InvokeModel body or OpenAI embeddings `data`."""
    response = reply.response or {}
    if op == "invoke_model":
        return parse_embeddings(orjson.loads(decode_invoke(response)["body"]))
    data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
    return [list(item["embedding"]) for item in data]


def _deterministic_send_error(exc: BaseException) -> BaseException | None:
    """The cause of a send that would fail again (bad label, too large, config), else None.

    Only infrastructure failures (SQS down, throttled, timeouts) open the SQS failover window.
    """
    seen: BaseException | None = exc
    for _ in range(5):
        if seen is None:
            return None
        if isinstance(seen, SQSSendError):
            return seen if seen.family is ErrorFamily.SIZE_LIMIT else None
        if isinstance(seen, ValueError | TypeError | ConfigError):
            return seen
        seen = seen.__cause__
    return None


def _unwrap(reply: LLMReply) -> LLMReply:
    """Return an ok reply; map an error reply to `LLMQueueTimeout` / `ProviderError`."""
    if reply.ok:
        return reply
    error = reply.error or {}
    code = str(error.get("type", "error"))
    message = str(error.get("message", code))
    if reply.cancelled or code == "cancelled":
        raise LLMQueueCancelled(f"call {reply.request_id} was cancelled")
    if code == "deadline_exceeded":
        raise LLMQueueTimeout(f"call {reply.request_id} ran past its deadline")
    retry_after = error.get("retry_after_secs")
    exc = ProviderError(
        _error_text(code, message),
        retryable=bool(error.get("retryable", False)),
        retry_after_secs=float(retry_after) if retry_after is not None else None,
    )
    exc.status_code = _STATUS_BY_KIND.get(str(error.get("kind", "")), 502)  # type: ignore[attr-defined]  # optional attribute read by http_transport
    raise exc


def _error_text(code: str, message: str) -> str:
    """`code: message` without repeating the code (`ThrottlingException: ThrottlingException`)."""
    parts: list[str] = []
    for part in [code, *message.split(": ")]:
        if part and part not in parts:
            parts.append(part)
    return ": ".join(parts) or "error"


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
