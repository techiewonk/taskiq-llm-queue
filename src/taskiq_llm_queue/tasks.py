"""The taskiq tasks every worker registers: `taskiq_llm_queue.call`, `.embed` and `.job`.

Names are stable for all of 0.x (STABILITY.md). LLM tasks run `core.execute_job`, which stores
the reply itself; the task returns None and `WorkerResultBackend` skips taskiq's own write, so
a result is written exactly once (DS8). Jobs run app-registered functions from an allow-list.
No exception leaves a task body: taskiq would ack (delete) the message, so every infrastructure
failure releases the message for redelivery instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from redis.exceptions import RedisError
from taskiq import AsyncResultBackend, Context, TaskiqDepends, TaskiqResult

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.broker.broker import (
    SQS_MESSAGE_ID_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
)
from taskiq_llm_queue.core import InfraError, execute_job
from taskiq_llm_queue.envelope import (
    TASK_CALL,
    TASK_EMBED,
    TASK_JOB,
    LLMReply,
    parse_envelope,
)
from taskiq_llm_queue.errors import QueueUnavailable, UnsupportedEnvelopeError
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState

if TYPE_CHECKING:
    from taskiq_llm_queue.broker.broker import HybridSQSBroker
    from taskiq_llm_queue.broker.delivery import Delivery
    from taskiq_llm_queue.core import CoreDeps
    from taskiq_llm_queue.results.backend import LLMQResultBackend
    from taskiq_llm_queue.routing_hooks import FailoverPolicy

logger = get_logger(__name__)

JOB_VERSION = 1
JobFunction = Callable[[dict[str, Any]], Awaitable[Any]]
_UNSUPPORTED_RELEASE_SECS = 30
# DS5: an exhausted job is released quickly until `maxReceiveCount` moves it to the DLQ.
_DLQ_RELEASE_SECS = 1
_CONTEXT: Any = TaskiqDepends()  # taskiq injects the message Context into the task functions

# `_Running.stop_reason` values set by the broker heartbeat and the pod heartbeat.
LOST = "lost"  # SQS receipt handle invalid (DS6): the message is no longer ours
CLAIM_LOST = "claim_lost"  # another pod owns the claim now: never delete the message


class WorkerResultBackend(AsyncResultBackend[Any]):
    """Result backend given to the brokers: skips taskiq's write for tasks that stored their own.

    LLM tasks return None after `execute_job` stored the reply; a task that raised unexpectedly
    gets an `ok=False` reply so its waiter is released. Cancellations write nothing.
    """

    def __init__(self, inner: LLMQResultBackend) -> None:
        """Wrap the shared JSON backend (its user count tracks every broker)."""
        super().__init__()
        self.inner = inner

    async def startup(self) -> None:
        """Register one user of the shared backend."""
        await self.inner.startup()

    async def shutdown(self) -> None:
        """Drop one user of the shared backend."""
        await self.inner.shutdown()

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        """Store only unexpected task errors, never over a reply the task already stored."""
        error = result.error
        if not result.is_err or isinstance(error, asyncio.CancelledError):
            return
        with contextlib.suppress(Exception):
            if await self.inner.is_result_ready(task_id):
                return
        reply = LLMReply(
            request_id=task_id,
            ok=False,
            error={"type": "task_error", "message": type(error).__name__, "retryable": True},
        )
        await self.inner.put(reply, ttl_secs=self.inner.result_ttl_secs)

    async def is_result_ready(self, task_id: str) -> bool:
        """Delegate to the shared backend."""
        return await self.inner.is_result_ready(task_id)

    async def get_result(self, task_id: str, with_logs: bool = False) -> TaskiqResult[Any]:
        """Delegate to the shared backend."""
        return await self.inner.get_result(task_id, with_logs)


@dataclass(slots=True, eq=False)
class _Running:
    """One claimed task this pod runs: its asyncio task, broker and SQS receipt handle."""

    request_id: str
    task: asyncio.Task[Any]
    broker: HybridSQSBroker
    receipt_handle: str | None
    message_id: str | None
    stop_reason: str | None = None


@dataclass(slots=True)
class WorkerRuntime:
    """Per-`LLMQueue` worker state shared by every registered task.

    Tracks claimed, running deliveries (one entry each, so two deliveries of one request never
    clobber each other) for drain, the pod heartbeat and the broker's `on_lost` (DS6).
    """

    deps: CoreDeps
    jobs: dict[str, JobFunction] = field(default_factory=dict)
    running: dict[tuple[str, int], _Running] = field(default_factory=dict)
    defer_secs: int = 20
    failover: FailoverPolicy | None = None

    def register(
        self, request_id: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> _Running:
        """Record the current asyncio task as running `request_id` (call only after RUN)."""
        task = asyncio.current_task()
        assert task is not None
        entry = _Running(
            request_id,
            task,
            broker,
            _label(labels, SQS_RECEIPT_HANDLE_LABEL),
            _label(labels, SQS_MESSAGE_ID_LABEL),
        )
        self.running[(request_id, id(entry))] = entry
        return entry

    def unregister(self, entry: _Running) -> None:
        """Drop exactly this delivery's entry."""
        self.running.pop((entry.request_id, id(entry)), None)

    def running_ids(self) -> list[str]:
        """Distinct request ids of running tasks (for the claim heartbeat)."""
        return list(dict.fromkeys(entry.request_id for entry in self.running.values()))

    def on_lost(self, delivery: Delivery) -> None:
        """Broker heartbeat lost the message (DS6): cancel its task; it writes no result."""
        for entry in list(self.running.values()):
            if entry.message_id == delivery.message_id and not entry.task.done():
                entry.stop_reason = LOST
                entry.task.cancel()

    def stop_running(self, reason: str, request_ids: Sequence[str] | None = None) -> list[Any]:
        """Cancel running tasks (all, or the given ids); returns the tasks to await."""
        chosen = [
            entry
            for entry in list(self.running.values())
            if request_ids is None or entry.request_id in request_ids
        ]
        for entry in chosen:
            if not entry.task.done():
                entry.stop_reason = entry.stop_reason or reason
                entry.task.cancel()
        return [entry.task for entry in chosen]

    async def run_llm(self, raw: Any, labels: Mapping[str, Any], broker: HybridSQSBroker) -> None:
        """Body of the `call` / `embed` tasks: parse, execute, release instead of losing work."""
        try:
            envelope = parse_envelope(raw)
        except UnsupportedEnvelopeError:
            await _release(broker, labels, _UNSUPPORTED_RELEASE_SECS)
            return
        except ValueError as exc:
            logger.error("poison_task_dropped", error_type=type(exc).__name__)
            await self._reply_poison(raw)
            return
        rid = envelope.request_id
        owned: list[_Running] = []

        def on_run() -> None:
            owned.append(self.register(rid, broker, labels))

        try:
            result = await execute_job(envelope, self.deps, on_run=on_run)
            if result.decision is ClaimDecision.DEFER:
                await _release(broker, labels, self.defer_secs)  # DS2: never delete
            elif result.requeue_secs is not None:
                await _release(broker, labels, result.requeue_secs)  # DS4: no capacity
        except InfraError as exc:
            await self._infra_release(rid, exc, broker, labels, owned=bool(owned))
        except QueueUnavailable:  # DS8: reply cached on this pod, claim left running
            logger.warning(
                "result_not_stored", request_id=rid, model=envelope.model, lane=envelope.lane.value
            )
            await _release(broker, labels, self.defer_secs)
        except asyncio.CancelledError:
            entry = owned[0] if owned else None
            await asyncio.shield(self._after_cancel(rid, entry, broker, labels))
            raise
        except Exception as exc:  # a bug: release (maxReceiveCount bounds it) rather than ack
            logger.exception(
                "task_failed_unexpectedly", request_id=rid, error_type=type(exc).__name__
            )
            await _release(broker, labels, self.defer_secs)
        finally:
            if owned:
                self.unregister(owned[0])

    async def _infra_release(
        self,
        request_id: str,
        exc: BaseException,
        broker: HybridSQSBroker,
        labels: Mapping[str, Any],
        *,
        owned: bool,
    ) -> None:
        """Valkey failed: report it, hand our claim back if we hold it, release the message."""
        cause = exc.__cause__ or exc
        self._report_redis(cause)
        logger.warning(
            "valkey_error_task_released",
            request_id=request_id,
            error_type=type(cause).__name__,
            attempted=getattr(exc, "attempted", False),
        )
        claims = self.deps.claims
        if owned and claims is not None:
            with contextlib.suppress(Exception):
                await claims.release(request_id, worker=self.deps.worker)
        await _release(broker, labels, self.defer_secs)

    def _report_redis(self, exc: BaseException) -> None:
        if self.failover is not None and isinstance(exc, RedisError):
            try:
                self.failover.report_redis_failure(exc)
            except Exception:  # the policy must never break the worker
                logger.exception("failover_report_failed")

    async def _after_cancel(
        self,
        request_id: str,
        entry: _Running | None,
        broker: HybridSQSBroker,
        labels: Mapping[str, Any],
    ) -> None:
        """Finish a cancelled delivery without deleting a message another pod may still need."""
        claims = self.deps.claims
        reason = entry.stop_reason if entry is not None else None
        if entry is None:  # cancelled before the claim said RUN: the claim may not be ours
            await _release(broker, labels, self.defer_secs)
        elif reason == LOST:  # DS6: SQS gave the message away; free our claim for the new owner
            if claims is not None:
                with contextlib.suppress(Exception):
                    await claims.release(request_id, worker=self.deps.worker)
        elif reason == CLAIM_LOST:  # another pod runs it now: keep the message, never ack it
            logger.warning("claim_lost_task_stopped", request_id=request_id)
            await _release(broker, labels, self.defer_secs)
        else:  # drain leftover (§7.4): claim `released`, message visible now
            await self._hand_back(request_id, broker, labels)

    async def _hand_back(
        self, request_id: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> None:
        """Drain leftover: claim `released` and message visible now, so another pod starts it."""
        claims = self.deps.claims
        with contextlib.suppress(Exception):
            if claims is not None:
                await claims.release(request_id, worker=self.deps.worker)
        await _release(broker, labels, 0)

    async def _reply_poison(self, raw: Any) -> None:
        """Release a waiter on a malformed envelope that still names its request id."""
        rid = raw.get("request_id") if isinstance(raw, dict) else None
        if isinstance(rid, str) and self.deps.results is not None:
            reply = LLMReply(
                request_id=rid,
                ok=False,
                error={"type": "invalid_envelope", "message": "malformed", "retryable": False},
            )
            with contextlib.suppress(Exception):
                await self.deps.results.put(reply, ttl_secs=self.deps.config.result_ttl_secs)

    async def run_job(self, raw: Any, labels: Mapping[str, Any], broker: HybridSQSBroker) -> None:
        """Body of the `job` task: run an allow-listed app job once (claimed) by name.

        A failing job is released with backoff; after `max_failures` it keeps being released
        until SQS `maxReceiveCount` moves it to the DLQ (DS5). Valkey errors release it too.
        """
        if not isinstance(raw, dict) or not isinstance(raw.get("request_id"), str):
            logger.error("poison_job_dropped")
            return
        job_id: str = raw["request_id"]
        name = str(raw.get("job"))
        function = self.jobs.get(name)
        if function is None:
            await _release(broker, labels, _UNSUPPORTED_RELEASE_SECS)
            return
        claims = self.deps.claims
        deadline = float(raw.get("deadline_at") or time.time() + 86400)
        try:
            if claims is not None:
                decision = await claims.claim(job_id, deadline_at=deadline, worker=self.deps.worker)
                if decision is ClaimDecision.DEFER:
                    await _release(broker, labels, self.defer_secs)
                    return
                if decision is not ClaimDecision.RUN:
                    return
                if await self._failures_exhausted(job_id):
                    await self._to_dlq(job_id, name, broker, labels)
                    return
        except RedisError as exc:
            await self._infra_release(job_id, exc, broker, labels, owned=False)
            return
        entry = self.register(job_id, broker, labels)
        try:
            await function(dict(raw.get("payload") or {}))
        except asyncio.CancelledError:
            await asyncio.shield(self._after_cancel(job_id, entry, broker, labels))
            raise
        except Exception:
            logger.exception("job_failed", job=name, request_id=job_id)
            await self._job_failed(job_id, name, broker, labels)
            return
        finally:
            self.unregister(entry)
        if claims is not None:  # the job ran; a failed CAS is logged, never re-runs it here
            try:
                await claims.complete(job_id, ClaimState.DONE, worker=self.deps.worker)
            except Exception as exc:
                logger.warning(
                    "claim_complete_failed", request_id=job_id, error_type=type(exc).__name__
                )

    async def _job_failed(
        self, job_id: str, name: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> None:
        """Count a real failure (DS5) and release the message with backoff, or toward the DLQ."""
        claims = self.deps.claims
        if claims is None:
            await _release(broker, labels, _UNSUPPORTED_RELEASE_SECS)
            return
        try:
            failures = await claims.record_failure(job_id)
        except RedisError as exc:
            await self._infra_release(job_id, exc, broker, labels, owned=True)
            return
        if failures.exhausted:
            await self._to_dlq(job_id, name, broker, labels)
            return
        with contextlib.suppress(Exception):
            await claims.release(job_id, worker=self.deps.worker)
        await _release(broker, labels, min(900, 30 * 2**failures.count))

    async def _failures_exhausted(self, job_id: str) -> bool:
        """True when the claim record already counts `max_failures` real failures (DS5)."""
        redis = self.deps.redis
        if redis is None:
            return False
        raw = await redis.hget(self.deps.keys.job(job_id), "failures")
        return raw is not None and int(raw) >= self.deps.config.max_failures

    async def _to_dlq(
        self, job_id: str, name: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> None:
        """DS5: never ack an exhausted job; release it until `maxReceiveCount` dead-letters it."""
        logger.error("job_failures_exhausted", job=name, request_id=job_id)
        claims = self.deps.claims
        if claims is not None:
            with contextlib.suppress(Exception):
                await claims.release(job_id, worker=self.deps.worker)
        await _release(broker, labels, _DLQ_RELEASE_SECS)


def _label(labels: Mapping[str, Any], name: str) -> str | None:
    value = labels.get(name)
    return None if value is None else str(value)


async def _release(broker: HybridSQSBroker, labels: Mapping[str, Any], secs: int) -> None:
    """Make the current message visible again after `secs` instead of acking it."""
    handle = _label(labels, SQS_RECEIPT_HANDLE_LABEL)
    if handle is not None:
        await broker.release(handle, visibility_secs=secs, reason="task")


def job_filter(runtime: WorkerRuntime) -> Callable[[Any], bool]:
    """Broker message filter: job messages whose name this pod doesn't know are released."""

    def accept(message: Any) -> bool:
        if message.task_name != TASK_JOB:
            return True
        args = [*message.args, *message.kwargs.values()]
        return any(isinstance(a, dict) and a.get("job") in runtime.jobs for a in args)

    return accept


def register_tasks(broker: HybridSQSBroker, runtime: WorkerRuntime) -> None:
    """Register the three stable task names on `broker`, bound to `runtime`."""

    async def call(envelope: dict[str, Any], context: Context = _CONTEXT) -> None:
        await runtime.run_llm(envelope, context.message.labels, broker)

    async def embed(envelope: dict[str, Any], context: Context = _CONTEXT) -> None:
        await runtime.run_llm(envelope, context.message.labels, broker)

    async def job(payload: dict[str, Any], context: Context = _CONTEXT) -> None:
        await runtime.run_job(payload, context.message.labels, broker)

    broker.register_task(call, task_name=TASK_CALL)
    broker.register_task(embed, task_name=TASK_EMBED)
    broker.register_task(job, task_name=TASK_JOB)
