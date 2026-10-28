"""The taskiq tasks every worker registers: `taskiq_llm_queue.call`, `.embed` and `.job`.

Names are stable for all of 0.x (STABILITY.md). LLM tasks run `core.execute_job`, which stores
the reply itself; the task returns None and `WorkerResultBackend` skips taskiq's own write, so
a result is written exactly once (DS8). Jobs run app-registered functions from an allow-list.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from taskiq import AsyncResultBackend, Context, TaskiqDepends, TaskiqResult

from taskiq_llm_queue.broker.broker import (
    SQS_MESSAGE_ID_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
)
from taskiq_llm_queue.core import execute_job
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

logger = structlog.get_logger(__name__)

JOB_VERSION = 1
JobFunction = Callable[[dict[str, Any]], Awaitable[Any]]
_UNSUPPORTED_RELEASE_SECS = 30
_CONTEXT: Any = TaskiqDepends()  # taskiq injects the message Context into the task functions


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
        """Store only unexpected task errors; normal results were stored by the task."""
        error = result.error
        if not result.is_err or isinstance(error, asyncio.CancelledError):
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


@dataclass(slots=True)
class _Running:
    """One task this pod runs: its asyncio task, broker and SQS receipt handle."""

    task: asyncio.Task[Any]
    broker: HybridSQSBroker
    receipt_handle: str | None
    message_id: str | None
    stop_reason: str | None = None


@dataclass(slots=True)
class WorkerRuntime:
    """Per-`LLMQueue` worker state shared by every registered task.

    Tracks running tasks so `stop()` can cancel leftovers (drain) and the broker's `on_lost`
    can stop a task whose message was lost (DS6). `jobs` is the job allow-list.
    """

    deps: CoreDeps
    jobs: dict[str, JobFunction] = field(default_factory=dict)
    running: dict[str, _Running] = field(default_factory=dict)
    defer_secs: int = 20

    def register(
        self, request_id: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> _Running:
        """Record the current asyncio task as running `request_id`."""
        task = asyncio.current_task()
        assert task is not None
        entry = _Running(
            task,
            broker,
            _label(labels, SQS_RECEIPT_HANDLE_LABEL),
            _label(labels, SQS_MESSAGE_ID_LABEL),
        )
        self.running[request_id] = entry
        return entry

    def on_lost(self, delivery: Delivery) -> None:
        """Broker heartbeat lost the message (DS6): cancel its task; it writes no result."""
        for entry in self.running.values():
            if entry.message_id == delivery.message_id and not entry.task.done():
                entry.stop_reason = "lost"
                entry.task.cancel()

    def stop_running(self, reason: str, request_ids: Sequence[str] | None = None) -> list[Any]:
        """Cancel running tasks (all, or the given ids); returns the tasks to await."""
        chosen = [
            entry
            for rid, entry in self.running.items()
            if request_ids is None or rid in request_ids
        ]
        for entry in chosen:
            if not entry.task.done():
                entry.stop_reason = entry.stop_reason or reason
                entry.task.cancel()
        return [entry.task for entry in chosen]

    async def run_llm(self, raw: Any, labels: Mapping[str, Any], broker: HybridSQSBroker) -> None:
        """Body of the `call` / `embed` tasks: parse, execute, release on defer or drain."""
        try:
            envelope = parse_envelope(raw)
        except UnsupportedEnvelopeError:
            await _release(broker, labels, _UNSUPPORTED_RELEASE_SECS)
            return
        except ValueError as exc:
            logger.error("poison_task_dropped", error_type=type(exc).__name__)
            await self._reply_poison(raw)
            return
        entry = self.register(envelope.request_id, broker, labels)
        try:
            result = await execute_job(envelope, self.deps)
            if result.decision is ClaimDecision.DEFER:
                await _release(broker, labels, self.defer_secs)  # DS2: never delete
        except QueueUnavailable:
            logger.warning(
                "result_not_stored",
                request_id=envelope.request_id,
                model=envelope.model,
                lane=envelope.lane.value,
            )
            await _release(broker, labels, 0)
        except asyncio.CancelledError:
            if entry.stop_reason != "lost":
                await asyncio.shield(self._hand_back(envelope.request_id, broker, labels))
            raise
        finally:
            self.running.pop(envelope.request_id, None)

    async def _hand_back(
        self, request_id: str, broker: HybridSQSBroker, labels: Mapping[str, Any]
    ) -> None:
        """Drain leftover: claim `released` and message visible now, so another pod starts it."""
        claims = self.deps.claims
        with contextlib.suppress(Exception):
            if claims is not None:
                await claims.release(request_id)
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

        A failing job is released with backoff until `max_failures`, then dropped with an
        error log (DS5). Unknown names never reach here: the broker filter releases them.
        """
        if not isinstance(raw, dict) or not isinstance(raw.get("request_id"), str):
            logger.error("poison_job_dropped")
            return
        job_id: str = raw["request_id"]
        function = self.jobs.get(str(raw.get("job")))
        if function is None:
            await _release(broker, labels, _UNSUPPORTED_RELEASE_SECS)
            return
        claims = self.deps.claims
        deadline = float(raw.get("deadline_at") or time.time() + 86400)
        if claims is not None:
            decision = await claims.claim(job_id, deadline_at=deadline, worker=self.deps.worker)
            if decision is ClaimDecision.DEFER:
                await _release(broker, labels, self.defer_secs)
                return
            if decision is not ClaimDecision.RUN:
                return
        entry = self.register(job_id, broker, labels)
        try:
            await function(dict(raw.get("payload") or {}))
        except asyncio.CancelledError:
            if entry.stop_reason != "lost":
                await asyncio.shield(self._hand_back(job_id, broker, labels))
            raise
        except Exception:
            logger.exception("job_failed", job=str(raw.get("job")), request_id=job_id)
            if claims is not None:
                failures = await claims.record_failure(job_id)
                if not failures.exhausted:
                    await claims.release(job_id)
                    await _release(broker, labels, min(900, 30 * 2**failures.count))
                    return
        finally:
            self.running.pop(job_id, None)
        if claims is not None:
            await claims.complete(job_id, ClaimState.DONE)


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
