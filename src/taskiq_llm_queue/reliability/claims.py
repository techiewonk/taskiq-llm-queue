"""Exactly-once execution on at-least-once SQS delivery: the claim record per task (DS1-DS3).

`llmq:<env>:job:<id>` holds `state`, `worker`, `heartbeat_at`, `deadline_at` and `failures`;
every transition is one Lua CAS using Valkey `TIME`. Used by the claim middleware before a task
runs, by the heartbeat loop, and by `direct` failover (DS17: claim as `direct:<pod>`).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import structlog

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState
from taskiq_llm_queue.reliability import _lua

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.config import QueueConfig

logger = structlog.get_logger(__name__)

_TERMINAL = frozenset({ClaimState.DONE, ClaimState.CANCELLED, ClaimState.EXPIRED})


class ClaimOutcome(StrEnum):
    """Result of an owner-checked `complete_owned` / `release_owned` call.

    `NOT_OWNER` means another worker holds the claim now, so nothing was written.
    """

    APPLIED = "applied"
    NOT_RUNNING = "not_running"  # release only: the record was not running
    NOT_OWNER = "not_owner"


@dataclass(frozen=True, slots=True)
class ClaimSettings:
    """Claim timing: TTLs, worst-case execution and heartbeat staleness (DS3, DS5).

    A heartbeat older than `stale_after_beats x heartbeat_interval_secs` is stale and the task
    is reclaimed; at least 3 beats, so one missed or late beat never hands a live task over.
    """

    state_ttl_secs: int = 1800
    exec_max_secs: int = 300
    heartbeat_interval_secs: float = 10.0
    max_failures: int = 3
    stale_after_beats: float = 3.0

    def __post_init__(self) -> None:
        """Reject a staleness window that a single missed heartbeat could exceed."""
        if self.stale_after_beats < 3:
            raise ValueError("stale_after_beats must be >= 3 heartbeat intervals")

    @property
    def stale_after_secs(self) -> float:
        """Heartbeat age after which a running claim may be taken over."""
        return self.stale_after_beats * self.heartbeat_interval_secs

    @classmethod
    def from_config(cls, config: QueueConfig, **overrides: Any) -> ClaimSettings:
        """Settings whose state TTL comes from `QueueConfig.state_ttl_secs`."""
        return cls(state_ttl_secs=config.state_ttl_secs, **overrides)


@dataclass(frozen=True, slots=True)
class FailureCount:
    """Real failures counted for a task and whether it should now go to the DLQ (DS5)."""

    count: int
    exhausted: bool


class RedisClaimStore:
    """`ClaimStore` on Valkey: claim, heartbeat, complete, release, failures and cancel flag.

    A running task with a fresh heartbeat is never acked by another pod (DEFER, DS2); terminal
    records outlive the message so duplicates are acked without a second provider call.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: KeySpace,
        settings: ClaimSettings | None = None,
    ) -> None:
        """Register the Lua scripts on `client`; no I/O happens until the first call."""
        self._client = client
        self._keys = keys
        self.settings = settings or ClaimSettings()
        self._claim = client.register_script(_lua.CLAIM)
        self._heartbeat = client.register_script(_lua.CLAIM_HEARTBEAT)
        self._complete = client.register_script(_lua.CLAIM_COMPLETE)
        self._release = client.register_script(_lua.CLAIM_RELEASE)

    async def claim(self, request_id: str, *, deadline_at: float, worker: str) -> ClaimDecision:
        """Atomically decide whether this worker runs the task (Lua CAS)."""
        s = self.settings
        decision = await self._claim(
            [self._keys.job(request_id), self._keys.cancel(request_id)],
            [
                repr(float(deadline_at)),
                worker,
                s.state_ttl_secs,
                s.exec_max_secs,
                s.stale_after_secs,
            ],
        )
        return ClaimDecision(_lua.as_text(decision))

    async def heartbeat_owned(self, request_ids: Sequence[str], *, worker: str) -> list[str]:
        """Refresh heartbeat and TTL of tasks this worker runs; return the ids it lost (DS6)."""
        if not request_ids:
            return []
        s = self.settings
        lost = await self._heartbeat(
            [self._keys.job(rid) for rid in request_ids],
            [worker, s.state_ttl_secs, s.exec_max_secs],
        )
        return [request_ids[int(i) - 1] for i in lost]

    async def heartbeat(self, request_ids: Sequence[str], *, worker: str) -> None:
        """Refresh heartbeats and TTLs for running tasks."""
        await self.heartbeat_owned(request_ids, worker=worker)

    async def complete(
        self, request_id: str, state: ClaimState, *, worker: str | None = None
    ) -> None:
        """Mark a task done, cancelled or expired after its result is stored (DS8).

        With `worker`, only the current owner may complete it (see `complete_owned`).
        """
        await self.complete_owned(request_id, state, worker=worker)

    async def complete_owned(
        self, request_id: str, state: ClaimState, *, worker: str | None
    ) -> ClaimOutcome:
        """Owner-checked `complete`: NOT_OWNER when another worker holds the claim now.

        A record without an owner (released, or missing) is written; `worker=None` skips the check.
        """
        if state not in _TERMINAL:
            raise ValueError(f"complete() needs a terminal state, got {state!r}")
        reply = await self._complete(
            [self._keys.job(request_id)], [state.value, self.settings.state_ttl_secs, worker or ""]
        )
        if int(reply) < 0:
            logger.warning("claim_complete_not_owner", request_id=request_id, worker=worker)
            return ClaimOutcome.NOT_OWNER
        return ClaimOutcome.APPLIED

    async def release(self, request_id: str, *, worker: str | None = None) -> None:
        """Hand a running task back so another pod can start it at once.

        With `worker`, only the current owner may release it (see `release_owned`).
        """
        await self.release_owned(request_id, worker=worker)

    async def release_owned(self, request_id: str, *, worker: str | None) -> ClaimOutcome:
        """Owner-checked `release`: APPLIED, NOT_RUNNING, or NOT_OWNER (nothing written).

        `worker=None` releases any running record (the old, unchecked behaviour).
        """
        reply = int(await self._release([self._keys.job(request_id)], [worker or ""]))
        if reply < 0:
            logger.warning("claim_release_not_owner", request_id=request_id, worker=worker)
            return ClaimOutcome.NOT_OWNER
        return ClaimOutcome.APPLIED if reply else ClaimOutcome.NOT_RUNNING

    async def state(self, request_id: str) -> ClaimState | None:
        """Current claim state, or None when there is no record."""
        raw = await self._client.hget(self._keys.job(request_id), "state")
        return None if raw is None else ClaimState(_lua.as_text(raw))

    async def record_failure(self, request_id: str) -> FailureCount:
        """Count one real failure (not a throttle or a release) against `max_failures` (DS5)."""
        key = self._keys.job(request_id)
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.hincrby(key, "failures", 1)
            pipe.expire(key, self.settings.state_ttl_secs, nx=True)  # never leave a key without TTL
            raw_count, _ = await pipe.execute()
        count = int(raw_count)
        return FailureCount(count, count >= self.settings.max_failures)

    async def cancel(self, request_id: str) -> None:
        """Set the cancel flag that workers check before every attempt and sleep (DS14)."""
        await self._client.set(self._keys.cancel(request_id), b"1", ex=self.settings.state_ttl_secs)

    async def is_cancelled(self, request_id: str) -> bool:
        """True when the caller cancelled or timed out on this task."""
        return bool(await self._client.exists(self._keys.cancel(request_id)))
