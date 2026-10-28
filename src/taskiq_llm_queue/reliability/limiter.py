"""Cluster-wide adaptive concurrency limiter per (model, target), learned from throttles.

State lives in Valkey (`KeySpace.limiter` hash + `KeySpace.leases` ZSET); every decision is one
Lua call using Valkey `TIME` (DS10-DS12). `AdaptiveLimiter` serves asyncio workers,
`SyncAdaptiveLimiter` serves sync callers on the `direct` path with the same scripts.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from redis.exceptions import RedisError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import Lease
from taskiq_llm_queue.reliability import _lua

if TYPE_CHECKING:
    import redis
    import redis.asyncio

    from taskiq_llm_queue.config import QueueConfig

logger = get_logger(__name__)

_MAX_ORPHANS = 4096  # past this, leases left by failed calls just expire (lease_ttl_secs)


@dataclass(frozen=True, slots=True)
class LimiterSettings:
    """Tuning of the adaptive limiter (`LLMQ_AC_*`); defaults are recommended production starts.

    `initial` maps a model alias to its start limit (a stored learned limit always wins).
    DS12: growth needs `inflight >= utilisation x allowed` and stops at 2x the peak inflight,
    which decays with `peak_halflife_secs`; `ramp_cap` is an extra hard cap on ramp doubling.
    Cuts need clustered throttles (`loss_tolerance` of a `window_secs` window, see `_lua`).
    """

    initial: Mapping[str, int] | None = None
    default_initial: int = 20
    min_limit: int = 2
    max_limit: int = 2000
    ramp_window: int = 50
    increase_every: int = 20
    decrease_factor: float = 0.7
    pause_secs: float = 2.0
    bg_share: float = 0.8
    utilisation: float = 0.9
    ramp_cap: int | None = None
    lease_ttl_secs: float = 240.0
    poll_secs: float = 0.05
    max_poll_secs: float = 0.25
    peak_halflife_secs: float = 300.0
    # S35: a lone 429 among many successes is noise, not a capacity signal.
    loss_tolerance: float = 0.05
    min_throttles: int = 2
    pause_ratio: float = 0.5
    window_secs: float = 1.0
    # S07: hold the limit near `settle_ratio x` the last cut point and probe above it slowly.
    settle_ratio: float = 0.9
    probe_secs: float = 5.0

    def __post_init__(self) -> None:
        """Reject impossible settings at construction (boot time), never on a call path."""
        if not 1 <= self.min_limit <= self.max_limit:
            raise ValueError("need 1 <= min_limit <= max_limit")
        if not 0 < self.decrease_factor < 1 or not 0 < self.bg_share <= 1:
            raise ValueError("decrease_factor must be in (0, 1) and bg_share in (0, 1]")
        if self.ramp_window < 1 or self.increase_every < 1 or self.lease_ttl_secs <= 0:
            raise ValueError("ramp_window, increase_every and lease_ttl_secs must be positive")
        if not 0 < self.utilisation <= 1:
            raise ValueError("utilisation must be in (0, 1]")
        if self.poll_secs <= 0 or self.max_poll_secs < self.poll_secs:
            raise ValueError("need 0 < poll_secs <= max_poll_secs")
        if not 0 <= self.loss_tolerance <= 1 or not 0 <= self.pause_ratio <= 1:
            raise ValueError("loss_tolerance and pause_ratio must be in [0, 1]")
        if self.min_throttles < 1 or self.window_secs <= 0 or self.probe_secs < 0:
            raise ValueError("need min_throttles >= 1, window_secs > 0 and probe_secs >= 0")
        if not 0 < self.settle_ratio <= 1:
            raise ValueError("settle_ratio must be in (0, 1]")

    def retry_delay(self, attempt: int, paused_secs: float, rng: random.Random) -> float:
        """Sleep before the next acquire try: the pause remaining, else jittered backoff.

        Backoff doubles from `poll_secs` to `max_poll_secs`; jitter spreads waiting pods.
        """
        if paused_secs > 0:
            return paused_secs + rng.uniform(0, self.poll_secs)
        base = min(self.max_poll_secs, self.poll_secs * 2 ** min(attempt, 16))
        return rng.uniform(base / 2, base)

    def start_for(self, model: str) -> int:
        """Start limit for a model alias before anything was learned."""
        return (self.initial or {}).get(model, self.default_initial)

    def share_for(self, lane: Lane) -> float:
        """Fraction of the limit a lane may use: rt the full limit, bg/bulk `bg_share`."""
        return 1.0 if lane is Lane.RT else self.bg_share

    @classmethod
    def from_config(cls, config: QueueConfig, **overrides: Any) -> LimiterSettings:
        """Settings whose start limits come from each model's `limiter_initial`."""
        initial = {alias: m.limiter_initial for alias, m in config.models.items()}
        return cls(initial=initial, **overrides)


@dataclass(frozen=True, slots=True)
class LimiterState:
    """Snapshot of one model target: learned limit, live leases and remaining pause."""

    limit: int | None
    inflight: int
    paused_secs: float

    def headroom(self, share: float = 1.0) -> int:
        """Free slots for a lane share; 0 while paused, unknown limits count as one slot."""
        if self.paused_secs > 0:
            return 0
        if self.limit is None:
            return 1
        return max(0, max(1, int(self.limit * share)) - self.inflight)


class PauseGate:
    """`ReceiveGate` that is closed while its model target is paused after a throttle.

    `is_open` reads a local deadline updated by every limiter reply; `wait_open` re-reads Valkey
    so a pause set by another pod is seen. If Valkey is unreachable the gate stays closed (DS18).
    """

    def __init__(
        self,
        refresh: Callable[[], Awaitable[None]],
        *,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
        retry_secs: float = 1.0,
    ) -> None:
        """Keep the refresh callback and the injected monotonic clock and sleep."""
        self._refresh = refresh
        self._clock = clock
        self._sleep = sleep
        self._retry_secs = retry_secs
        self._closed_until = 0.0

    def close_for(self, secs: float) -> None:
        """Close the gate for `secs` from now (0 opens it)."""
        self._closed_until = self._clock() + max(0.0, secs)

    def is_open(self) -> bool:
        """True when the broker may receive now."""
        return self._clock() >= self._closed_until

    async def wait_open(self) -> None:
        """Return once Valkey confirms the model target is not paused."""
        while True:
            try:
                await self._refresh()
            except RedisError:
                logger.warning("limiter_gate_unreachable", retry_secs=self._retry_secs)
                self.close_for(self._retry_secs)
            if self.is_open():
                return
            await self._sleep(max(0.01, self._closed_until - self._clock()))


def _acquire_args(settings: LimiterSettings, model: str, lane: Lane, token: str) -> list[Any]:
    return [
        settings.start_for(model),
        settings.min_limit,
        settings.max_limit,
        settings.share_for(lane),
        settings.lease_ttl_secs,
        token,
    ]


def _release_args(
    settings: LimiterSettings, lease: Lease, throttled: bool, share: float = 1.0
) -> list[Any]:
    return [
        settings.start_for(lease.model),
        settings.min_limit,
        settings.max_limit,
        lease.token,
        1 if throttled else 0,
        settings.ramp_window,
        settings.increase_every,
        settings.decrease_factor,
        settings.pause_secs,
        min(settings.ramp_cap or settings.max_limit, settings.max_limit),
        settings.utilisation,
        settings.peak_halflife_secs,
        share,
        settings.loss_tolerance,
        settings.min_throttles,
        settings.pause_ratio,
        settings.window_secs,
        settings.settle_ratio,
        settings.probe_secs,
        settings.lease_ttl_secs,
    ]


def local_cap(last_known_limit: int | None, live_pods: int, fallback_cap: int) -> int:
    """Per-pod cap while Valkey is down: `last_known_limit / live_pods`, else the fallback.

    Keeps the sum over all pods under the shared limit (DS18); never below 1.
    """
    if last_known_limit is None or live_pods < 1:
        return max(1, fallback_cap)
    return max(1, last_known_limit // live_pods)


class AdaptiveLimiter:
    """asyncio `Limiter`: per (model, target) AIMD limit with ramp, pause and lane share.

    One instance per pod, built on the package's own `redis.asyncio` client (injected). Leases
    expire on their own (DS11), so a crashed pod's slots come back without a sweep.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: KeySpace,
        settings: LimiterSettings | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Register the Lua scripts on `client`; no I/O happens until the first call."""
        self._client = client
        self._keys = keys
        self.settings = settings or LimiterSettings()
        self._clock = clock
        self._sleep = sleep
        self._acquire = client.register_script(_lua.LIMITER_ACQUIRE)
        self._release = client.register_script(_lua.LIMITER_RELEASE)
        self._peek = client.register_script(_lua.LIMITER_PEEK)
        self._renew = client.register_script(_lua.LEASE_RENEW)
        self._gates: dict[tuple[str, str], PauseGate] = {}
        self._shares: dict[str, float] = {}  # lease token -> lane share, for DS12 at release
        self._orphans: set[tuple[str, str, str]] = set()  # (model, target, token) to drop
        self._rng = random.Random()
        self.last_limits: dict[tuple[str, str], int] = {}

    def _key_pair(self, model: str, target: str) -> list[str]:
        return [self._keys.limiter(model, target), self._keys.leases(model, target)]

    def _note(self, model: str, target: str, limit: int | None, paused_secs: float) -> None:
        if limit is not None:
            self.last_limits[(model, target)] = limit
        self.gate(model, target).close_for(paused_secs)

    async def _attempt(self, model: str, target: str, lane: Lane) -> tuple[Lease | None, float]:
        if self._orphans:
            await self._drop_orphans()
        token = uuid.uuid4().hex
        args = _acquire_args(self.settings, model, lane, token)
        try:
            granted, _inflight, limit, extra = await self._acquire(
                self._key_pair(model, target), args
            )
        except BaseException:
            # The script may have run and only the reply been lost (timeout, cancel): a lease
            # nobody holds would block a slot for `lease_ttl_secs`, so drop the token later.
            if len(self._orphans) < _MAX_ORPHANS:
                self._orphans.add((model, target, token))
            raise
        if int(granted):
            self._note(model, target, int(limit), 0.0)
            self._shares[token] = self.settings.share_for(lane)
            return Lease(model, target, token, float(_lua.as_text(extra))), 0.0
        paused = float(_lua.as_text(extra))
        self._note(model, target, int(limit), paused)
        return None, paused

    async def try_acquire(self, model: str, target: str, lane: Lane) -> Lease | None:
        """One non-blocking attempt to take a slot."""
        return (await self._attempt(model, target, lane))[0]

    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        """Take a slot, waiting until one frees up or `timeout_secs` passes (then None).

        Sleeps out a pause, else backs off with jitter; `should_abort` (e.g. the cancel flag)
        is checked before every sleep and returning True gives up with None.
        """
        deadline = self._clock() + max(0.0, timeout_secs)
        attempt = 0
        while True:
            lease, paused = await self._attempt(model, target, lane)
            if lease is not None:
                return lease
            remaining = deadline - self._clock()
            if remaining <= 0 or (should_abort is not None and await should_abort()):
                return None
            delay = self.settings.retry_delay(attempt, paused, self._rng)
            await self._sleep(min(delay, remaining))
            attempt += 1

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        """Return the slot and grow or shrink the limit, in one Lua call.

        If Valkey fails the error is raised and the lease is remembered; the next acquire or
        release on this pod drops it, so it does not hold a slot for `lease_ttl_secs`.
        """
        share = self._shares.pop(lease.token, 1.0)
        try:
            limit, _removed, paused = await self._release(
                self._key_pair(lease.model, lease.target),
                _release_args(self.settings, lease, throttled, share),
            )
        except BaseException:
            if len(self._orphans) < _MAX_ORPHANS:
                self._orphans.add((lease.model, lease.target, lease.token))
            raise
        self._note(lease.model, lease.target, int(limit), float(_lua.as_text(paused)))
        if self._orphans:
            await self._drop_orphans()

    async def _drop_orphans(self) -> None:
        """Remove leases whose release (or acquire reply) failed; kept for a later try on error.

        Bug 4 (S35 pool exhaustion): a failed release left its lease live for 240 s.
        """
        orphans = list(self._orphans)
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                for model, target, token in orphans:
                    pipe.zrem(self._keys.leases(model, target), token)
                await pipe.execute()
        except RedisError as exc:
            logger.warning(
                "limiter_orphans_kept", leases=len(orphans), error_type=type(exc).__name__
            )
            return
        self._orphans.difference_update(orphans)

    async def renew(self, leases: Iterable[Lease]) -> list[Lease]:
        """Extend still-held leases by `lease_ttl_secs` (called by the heartbeat, DS11)."""
        by_target: dict[tuple[str, str], list[Lease]] = {}
        for lease in leases:
            by_target.setdefault((lease.model, lease.target), []).append(lease)
        groups = list(by_target.items())
        replies = await _lua.run_many(  # every target in one round trip
            self._client,
            [
                (
                    self._renew,
                    [self._keys.leases(model, target)],
                    [self.settings.lease_ttl_secs, *(lease.token for lease in group)],
                )
                for (model, target), group in groups
            ],
        )
        renewed: list[Lease] = []
        for ((model, target), group), (expires, _count) in zip(groups, replies, strict=True):
            renewed += [
                Lease(model, target, ls.token, float(_lua.as_text(expires))) for ls in group
            ]
        return renewed

    async def state(self, model: str, target: str) -> LimiterState:
        """Read the limit, live leases and pause without changing anything."""
        reply = await self._peek(self._key_pair(model, target), [])
        return self._peeked(model, target, reply)

    async def states(self, model: str, targets: Iterable[str]) -> list[LimiterState]:
        """`state` of several targets of one model in one pipelined round trip."""
        names = list(targets)
        replies = await _lua.run_many(
            self._client, [(self._peek, self._key_pair(model, t), []) for t in names]
        )
        return [self._peeked(model, t, r) for t, r in zip(names, replies, strict=True)]

    def _peeked(self, model: str, target: str, reply: Any) -> LimiterState:
        limit, inflight, paused = reply
        state = LimiterState(
            None if int(limit) < 0 else int(limit), int(inflight), float(_lua.as_text(paused))
        )
        self._note(model, target, state.limit, state.paused_secs)
        return state

    def gate(self, model: str, target: str) -> PauseGate:
        """Gate that closes while this model target is paused after a throttle."""
        gate = self._gates.get((model, target))
        if gate is None:

            async def refresh() -> None:
                await self.state(model, target)  # updates this gate via _note

            gate = PauseGate(refresh, clock=self._clock, sleep=self._sleep)
            self._gates[(model, target)] = gate
        return gate


class SyncAdaptiveLimiter:
    """Blocking twin of `AdaptiveLimiter` for sync callers (same Lua, `redis.Redis` client).

    Used by sync `direct` calls so they share the cluster-wide limit with queued work (DS18).
    """

    def __init__(
        self,
        client: redis.Redis,
        keys: KeySpace,
        settings: LimiterSettings | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Register the Lua scripts on `client`; no I/O happens until the first call."""
        self._keys = keys
        self.settings = settings or LimiterSettings()
        self._clock = clock
        self._sleep = sleep
        self._acquire = client.register_script(_lua.LIMITER_ACQUIRE)
        self._release = client.register_script(_lua.LIMITER_RELEASE)
        self._shares: dict[str, float] = {}
        self._rng = random.Random()

    def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], bool] | None = None,
    ) -> Lease | None:
        """Take a slot, waiting until one frees up or `timeout_secs` passes (then None).

        Same pause-aware jittered wait as `AdaptiveLimiter.acquire`, with a sync `should_abort`.
        """
        keys = [self._keys.limiter(model, target), self._keys.leases(model, target)]
        deadline = self._clock() + max(0.0, timeout_secs)
        attempt = 0
        while True:
            token = uuid.uuid4().hex
            granted, _, _, extra = self._acquire(
                keys, _acquire_args(self.settings, model, lane, token)
            )
            if int(granted):
                self._shares[token] = self.settings.share_for(lane)
                return Lease(model, target, token, float(_lua.as_text(extra)))
            remaining = deadline - self._clock()
            if remaining <= 0 or (should_abort is not None and should_abort()):
                return None
            delay = self.settings.retry_delay(attempt, float(_lua.as_text(extra)), self._rng)
            self._sleep(min(delay, remaining))
            attempt += 1

    def release(self, lease: Lease, *, throttled: bool) -> None:
        """Return the slot and grow or shrink the limit, in one Lua call."""
        self._release(
            [
                self._keys.limiter(lease.model, lease.target),
                self._keys.leases(lease.model, lease.target),
            ],
            _release_args(self.settings, lease, throttled, self._shares.pop(lease.token, 1.0)),
        )
