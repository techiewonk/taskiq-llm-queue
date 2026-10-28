"""Circuit breaker per (model, target) in Valkey (D79, §7.4).

Opens after `threshold` throttles/5xx within `window_secs`, lets one probe through every
`cooldown_secs` while half-open, and closes on the probe's success. While open, the worker goes
to the next fallback step instead of calling the target.
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
    """Breaker position: closed (normal), open (skip target), half-open (one probe)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class BreakerSettings:
    """Breaker thresholds; defaults follow §7.4 (5 failures in 30 s, half-open after 15 s)."""

    threshold: int = 5
    window_secs: float = 30.0
    cooldown_secs: float = 15.0


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
            [self._keys.breaker(model, target)], [op, s.threshold, s.window_secs, s.cooldown_secs]
        )
        return BreakerState(_lua.as_text(state)), bool(int(allowed))

    async def allow(self, model: str, target: str) -> bool:
        """True if a call may go to this target now (takes the half-open probe slot)."""
        return (await self._run("allow", model, target))[1]

    async def record(self, model: str, target: str, *, ok: bool) -> BreakerState:
        """Feed one outcome: a throttle/5xx counts as failure, a success closes a half-open one."""
        return (await self._run("success" if ok else "failure", model, target))[0]

    async def state(self, model: str, target: str) -> BreakerState:
        """Current position without taking the probe slot."""
        return (await self._run("peek", model, target))[0]
