"""Interfaces between the layers: providers, limiter, claims, results and receive gates.

Each workstream implements one of these and codes against the others, so pieces can be built
and tested in parallel with in-memory fakes before the real implementations land.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from taskiq_llm_queue.config import Lane, Target
    from taskiq_llm_queue.envelope import LLMReply, Op
    from taskiq_llm_queue.errors import ErrorInfo


class ClaimState(StrEnum):
    """Lifecycle of one task's claim record in Redis."""

    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    RELEASED = "released"


class ClaimDecision(StrEnum):
    """What a worker does with a received message after trying to claim it."""

    RUN = "run"
    DEFER = "defer"  # another pod is running it: extend visibility, never delete
    ACK_DONE = "ack_done"
    ACK_CANCELLED = "ack_cancelled"
    ACK_EXPIRED = "ack_expired"


@dataclass(frozen=True, slots=True)
class Lease:
    """One in-flight slot held for one provider attempt; released in `finally`."""

    model: str
    target: str
    token: str
    expires_at: float


@runtime_checkable
class Provider(Protocol):
    """An LLM backend: performs one attempt and classifies its failures."""

    name: str

    async def call(self, op: Op, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        """Run one attempt and return the provider-native response body."""
        ...

    def classify(self, exc: BaseException) -> ErrorInfo:
        """Map a provider exception to throttle / retryable / fatal."""
        ...


@runtime_checkable
class ReceiveGate(Protocol):
    """Lets the broker stop receiving for a queue, e.g. while its model is throttled."""

    def is_open(self) -> bool:
        """True when the broker may receive now."""
        ...

    async def wait_open(self) -> None:
        """Return once receiving is allowed again."""
        ...


@runtime_checkable
class Limiter(Protocol):
    """Cluster-wide adaptive concurrency limit per model target, learned from throttles."""

    async def acquire(
        self, model: str, target: str, lane: Lane, timeout_secs: float
    ) -> Lease | None:
        """Take a slot for one attempt, or None if none frees up within the timeout."""
        ...

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        """Return the slot and feed the outcome into the limit (grow or shrink)."""
        ...

    def gate(self, model: str, target: str) -> ReceiveGate:
        """Gate that closes while this model target is paused after a throttle."""
        ...


@runtime_checkable
class ClaimStore(Protocol):
    """Exactly-once execution on top of at-least-once delivery."""

    async def claim(self, request_id: str, *, deadline_at: float, worker: str) -> ClaimDecision:
        """Atomically decide whether this worker runs the task (Lua CAS)."""
        ...

    async def heartbeat(self, request_ids: Sequence[str], *, worker: str) -> None:
        """Refresh heartbeats and TTLs for running tasks."""
        ...

    async def complete(self, request_id: str, state: ClaimState) -> None:
        """Mark a task done, cancelled or expired after its result is stored."""
        ...

    async def release(self, request_id: str) -> None:
        """Hand a running task back so another pod can start it at once."""
        ...


@runtime_checkable
class ResultStore(Protocol):
    """Stores task results and hands them to waiting callers in batches."""

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        """Store one result (JSON) with a TTL."""
        ...

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        """Fetch and delete whichever of these results are ready."""
        ...
