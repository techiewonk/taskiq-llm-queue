"""In-process per-pod limiter used while Valkey is down (DS18).

Each pod caps its own concurrency at `local_cap(...)` so all pods together stay under the last
shared limit. Thread-safe, so the same instance serves async workers and sync `direct` callers.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.protocols import Lease
from taskiq_llm_queue.reliability.limiter import PauseGate


class LocalLimiter:
    """`Limiter` with a fixed per-pod cap per (model, target), lane share and throttle pause.

    No learning: the cap is set by the caller from `local_cap`, lowered per alias by
    `model_caps` (`max_concurrency`) when one instance serves every model. A throttle storm (at
    least `min_throttles` throttles making up `pause_ratio` of a `window_secs` window) pauses the
    model target for `pause_secs`, closing its gate; a lone throttle does not (S35).
    """

    def __init__(
        self,
        cap: int,
        *,
        model_caps: Mapping[str, int] | None = None,
        bg_share: float = 0.8,
        pause_secs: float = 2.0,
        lease_ttl_secs: float = 60.0,
        poll_secs: float = 0.05,
        min_throttles: int = 2,
        pause_ratio: float = 0.5,
        window_secs: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        sync_sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Keep the cap and the injected clock and sleeps; nothing starts here."""
        if cap < 1:
            raise ValueError("cap must be >= 1")
        self.cap = cap
        self.model_caps = dict(model_caps or {})
        self._bg_share = bg_share
        self._pause_secs = pause_secs
        self._lease_ttl = lease_ttl_secs
        self._poll = poll_secs
        self._min_throttles = min_throttles
        self._pause_ratio = pause_ratio
        self._window = window_secs
        self._clock = clock
        self._sleep = sleep
        self._sync_sleep = sync_sleep
        self._lock = threading.Lock()
        self._held: dict[tuple[str, str], set[str]] = {}
        self._gates: dict[tuple[str, str], PauseGate] = {}
        self._outcomes: dict[tuple[str, str], list[float]] = {}  # [window start, ok, throttled]

    def inflight(self, model: str, target: str) -> int:
        """Slots currently held for a model target in this pod."""
        with self._lock:
            return len(self._held.get((model, target), ()))

    def try_acquire(self, model: str, target: str, lane: Lane) -> Lease | None:
        """One non-blocking attempt to take a slot (safe from any thread)."""
        if not self.gate(model, target).is_open():
            return None
        share = 1.0 if lane is Lane.RT else self._bg_share
        allowed = max(1, int(min(self.cap, self.model_caps.get(model, self.cap)) * share))
        with self._lock:
            held = self._held.setdefault((model, target), set())
            if len(held) >= allowed:
                return None
            token = uuid.uuid4().hex
            held.add(token)
        return Lease(model, target, token, self._clock() + self._lease_ttl)

    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        """Take a slot, polling until one frees up or `timeout_secs` passes (then None).

        `should_abort` is checked before every sleep; True gives up with None.
        """
        deadline = self._clock() + max(0.0, timeout_secs)
        while (lease := self.try_acquire(model, target, lane)) is None:
            remaining = deadline - self._clock()
            if remaining <= 0 or (should_abort is not None and await should_abort()):
                return None
            await self._sleep(min(self._poll, remaining))
        return lease

    def acquire_sync(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], bool] | None = None,
    ) -> Lease | None:
        """Blocking `acquire` for sync callers."""
        deadline = self._clock() + max(0.0, timeout_secs)
        while (lease := self.try_acquire(model, target, lane)) is None:
            remaining = deadline - self._clock()
            if remaining <= 0 or (should_abort is not None and should_abort()):
                return None
            self._sync_sleep(min(self._poll, remaining))
        return lease

    def release_sync(self, lease: Lease, *, throttled: bool) -> None:
        """Return the slot; a throttle storm pauses the model target for `pause_secs`."""
        key = (lease.model, lease.target)
        now = self._clock()
        with self._lock:
            self._held.get(key, set()).discard(lease.token)
            counts = self._outcomes.setdefault(key, [now, 0.0, 0.0])
            if now - counts[0] >= self._window:
                counts[:] = [now, 0.0, 0.0]
            counts[2 if throttled else 1] += 1
            storm = (
                throttled
                and counts[2] >= self._min_throttles
                and counts[2] >= self._pause_ratio * (counts[1] + counts[2])
            )
        if storm:
            self.gate(*key).close_for(self._pause_secs)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        """Async form of `release_sync` (the `Limiter` protocol)."""
        self.release_sync(lease, throttled=throttled)

    def gate(self, model: str, target: str) -> PauseGate:
        """Gate closed while this model target is paused after a throttle."""
        with self._lock:
            gate = self._gates.get((model, target))
            if gate is None:

                async def refresh() -> None:
                    return None

                gate = PauseGate(refresh, clock=self._clock, sleep=self._sleep)
                self._gates[(model, target)] = gate
            return gate
