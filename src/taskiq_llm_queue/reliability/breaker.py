"""Circuit breaker per (model, target) in Valkey.

Opens after `threshold` counted failures within `window_secs` (callers decide what counts, e.g.
5xx but not throttles, which the limiter handles); half-open lets `half_open_probes` calls through
and closes on a probe's success. While open, the worker goes to the next fallback step.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import _lua

if TYPE_CHECKING:
    import redis.asyncio


class BreakerState(StrEnum):
    """Breaker position: closed (normal), open (skip target), half-open (probing)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class BreakerSettings:
    """Breaker thresholds; defaults (5 failures in 30 s, half-open after 15 s, one probe).

    `half_open_probes` probes may run at once; a probe that never reports frees after cooldown.
    """

    threshold: int = 5
    window_secs: float = 30.0
    cooldown_secs: float = 15.0
    half_open_probes: int = 1

    def __post_init__(self) -> None:
        """Reject impossible settings at construction (boot time)."""
        if self.threshold < 1 or self.half_open_probes < 1:
            raise ValueError("threshold and half_open_probes must be >= 1")


class CircuitBreaker:
    """Shared breaker for every (model, target); all transitions are one Lua call each."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: KeySpace,
        settings: BreakerSettings | None = None,
    ) -> None:
        """Register the Lua script on `client`; no I/O happens until the first call."""
        self._keys = keys
        self.settings = settings or BreakerSettings()
        self._script = client.register_script(_lua.BREAKER)

    async def _run(self, op: str, model: str, target: str) -> tuple[BreakerState, bool]:
        s = self.settings
        state, allowed = await self._script(
            [self._keys.breaker(model, target)],
            [op, s.threshold, s.window_secs, s.cooldown_secs, s.half_open_probes],
        )
        return BreakerState(_lua.as_text(state)), bool(int(allowed))

    async def allow(self, model: str, target: str) -> bool:
        """True if a call may go to this target now (takes the half-open probe slot)."""
        return (await self._run("allow", model, target))[1]

    async def record(self, model: str, target: str, *, ok: bool) -> BreakerState:
        """Feed one outcome: `ok=False` counts a failure, a success closes a half-open breaker.

        Call it only for outcomes that count; use `record_neutral` for the rest (e.g. throttles).
        """
        return (await self._run("success" if ok else "failure", model, target))[0]

    async def record_neutral(self, model: str, target: str) -> BreakerState:
        """Report an attempt that counts neither way; frees its half-open probe slot, if any."""
        return (await self._run("neutral", model, target))[0]

    async def state(self, model: str, target: str) -> BreakerState:
        """Current position without taking the probe slot."""
        return (await self._run("peek", model, target))[0]
