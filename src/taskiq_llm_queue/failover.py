"""SQS and Valkey failover state for the sender side (D73, D74, DS17, DS18).

A failed SQS send puts that model on `direct` for `sqs_failover_secs`; a Valkey failure puts the
whole pod on `direct` with a local per-pod cap until a health probe (or a backoff window) says
Valkey is back. `LLMQueue` calls the `FailoverPolicy` methods; nothing here blocks or raises.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import structlog

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.metrics_names import FAILOVER_ACTIVE
from taskiq_llm_queue.reliability.limiter import local_cap
from taskiq_llm_queue.reliability.local_limiter import LocalLimiter

if TYPE_CHECKING:
    from taskiq_llm_queue.metrics import MetricsSink

logger = structlog.get_logger(__name__)
Probe = Callable[[], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class FailoverSettings:
    """Failover timings and the local cap (`LLMQ_SQS_FAILOVER_S`, `LLMQ_LOCAL_LIMIT_CAP`).

    `live_pods` < 1 means unknown: the local cap is then `local_cap_fallback`, never
    `last_known_limit` per pod, so an unknown pod count cannot overshoot the provider limit.
    """

    sqs_failover_secs: float = 300.0
    redis_retry_min_secs: float = 1.0
    redis_retry_max_secs: float = 30.0
    local_cap_fallback: int = 20
    live_pods: int = 0

    def __post_init__(self) -> None:
        """Reject impossible values at construction (boot time)."""
        if self.sqs_failover_secs <= 0 or self.local_cap_fallback < 1:
            raise ConfigError("sqs_failover_secs must be > 0 and local_cap_fallback >= 1")
        if not 0 < self.redis_retry_min_secs <= self.redis_retry_max_secs:
            raise ConfigError("need 0 < redis_retry_min_secs <= redis_retry_max_secs")

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None, *, live_pods: int = 0
    ) -> FailoverSettings:
        """Read `LLMQ_SQS_FAILOVER_S` and `LLMQ_LOCAL_LIMIT_CAP`; bad values raise `ConfigError`."""
        env = os.environ if environ is None else environ
        values: dict[str, str] = {}
        errors: list[str] = []
        for name, conv in (("LLMQ_SQS_FAILOVER_S", float), ("LLMQ_LOCAL_LIMIT_CAP", int)):
            raw = env.get(name)
            if raw is None:
                continue
            try:
                conv(raw)
            except ValueError as exc:
                errors.append(f"{name}: {exc}")
            values[name] = raw
        if errors:
            raise ConfigError("; ".join(errors))
        return cls(
            sqs_failover_secs=float(values.get("LLMQ_SQS_FAILOVER_S", 300.0)),
            local_cap_fallback=int(values.get("LLMQ_LOCAL_LIMIT_CAP", 20)),
            live_pods=live_pods,
        )


class FailoverState:
    """`FailoverPolicy`: per-model SQS failover windows and pod-wide Valkey-down mode.

    With a `probe` (e.g. `client.ping`) Valkey-down lasts until a probe succeeds, retried with
    exponential backoff on the running loop. Without one, or from a thread with no loop, the
    mode lapses after the backoff window so the next call tries Valkey again.
    """

    def __init__(
        self,
        settings: FailoverSettings | None = None,
        *,
        probe: Probe | None = None,
        metrics: MetricsSink | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Keep settings, the optional Valkey probe, metrics sink and injected clock/sleep."""
        self.settings = settings or FailoverSettings()
        self._probe = probe
        self._metrics = metrics
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._sqs_until: dict[str, float] = {}
        self._redis_down = False
        self._redis_retry_at = 0.0
        self._redis_backoff = self.settings.redis_retry_min_secs
        self._probe_task: asyncio.Task[None] | None = None
        self._limits: dict[tuple[str, str], int] = {}
        self._local: dict[tuple[str, str], LocalLimiter] = {}

    # ------------------------------------------------------------------ SQS

    def path_override(self, model: str) -> Literal["direct"] | None:
        """`direct` while Valkey is down or this model's SQS failover window is open, else None."""
        if self.redis_down() or self.sqs_failover_active(model):
            return "direct"
        return None

    def report_sqs_failure(self, model: str, exc: BaseException) -> None:
        """Open (or extend) the model's SQS failover window after a failed or ambiguous send."""
        alias = model.partition("@")[0]
        with self._lock:
            entering = alias not in self._sqs_until
            self._sqs_until[alias] = self._clock() + self.settings.sqs_failover_secs
        if entering:
            logger.error(
                "sqs_failover_active",
                model=alias,
                failover_secs=self.settings.sqs_failover_secs,
                error_type=type(exc).__name__,
            )
            self._gauge(1.0, {"model": alias, "kind": "sqs"})

    def sqs_failover_active(self, model: str) -> bool:
        """True while `model` (alias or `alias@target`) is inside its failover window."""
        alias = model.partition("@")[0]
        with self._lock:
            until = self._sqs_until.get(alias)
            if until is None:
                return False
            if self._clock() < until:
                return True
            del self._sqs_until[alias]
        logger.warning("sqs_failover_ended", model=alias)
        self._gauge(0.0, {"model": alias, "kind": "sqs"})
        return False

    # ------------------------------------------------------------------ Valkey

    def report_redis_failure(self, exc: BaseException) -> None:
        """Enter (or stay in) Valkey-down mode and start the health probe if one is configured."""
        with self._lock:
            entering = not self._redis_down
            if entering:
                self._redis_backoff = self.settings.redis_retry_min_secs
            else:
                self._redis_backoff = min(
                    self._redis_backoff * 2, self.settings.redis_retry_max_secs
                )
            self._redis_down = True
            self._redis_retry_at = self._clock() + self._redis_backoff
        if entering:
            logger.error("valkey_local_mode_active", error_type=type(exc).__name__)
            self._gauge(1.0, {"kind": "redis"})
        self._start_probe()

    def report_redis_ok(self) -> None:
        """Leave Valkey-down mode (a probe or a real call succeeded); drops local limiters."""
        with self._lock:
            if not self._redis_down:
                return
            self._redis_down = False
            self._redis_backoff = self.settings.redis_retry_min_secs
            self._local.clear()
        logger.warning("valkey_local_mode_ended")
        self._gauge(0.0, {"kind": "redis"})

    def redis_down(self) -> bool:
        """True while Valkey is considered down (probe running, or inside the backoff window)."""
        with self._lock:
            if not self._redis_down:
                return False
            probing = self._probe_task is not None and not self._probe_task.done()
            return probing or self._clock() < self._redis_retry_at

    async def probe_redis(self) -> bool:
        """Run the probe once; success leaves Valkey-down mode. False when no probe is set."""
        if self._probe is None:
            return False
        try:
            await self._probe()
        except Exception as exc:  # any failure of the probe means "still down"
            logger.debug("valkey_probe_failed", error_type=type(exc).__name__)
            return False
        self.report_redis_ok()
        return True

    async def _probe_loop(self) -> None:
        delay = self.settings.redis_retry_min_secs
        while self._redis_down:
            await self._sleep(delay)
            if await self.probe_redis():
                return
            delay = min(delay * 2, self.settings.redis_retry_max_secs)

    def _start_probe(self) -> None:
        if self._probe is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # sync caller in a thread: the backoff window covers it
            return
        with self._lock:
            if self._probe_task is None or self._probe_task.done():
                self._probe_task = loop.create_task(self._probe_loop(), name="llmq-valkey-probe")

    # ------------------------------------------------------------------ local cap

    def note_limit(self, model: str, target: str, limit: int | None) -> None:
        """Remember the last shared limit seen for a model target (feeds the local cap)."""
        if limit is not None:
            with self._lock:
                self._limits[(model, target)] = limit

    def local_cap(self, model: str, target: str) -> int:
        """Per-pod cap while Valkey is down: last known limit / live pods, else the fallback."""
        with self._lock:
            last = self._limits.get((model, target))
        s = self.settings
        return local_cap(last, s.live_pods, s.local_cap_fallback)

    def local_limiter(self, model: str, target: str) -> LocalLimiter:
        """The pod's `LocalLimiter` for a model target, built once per Valkey outage."""
        with self._lock:
            limiter = self._local.get((model, target))
        if limiter is None:
            limiter = LocalLimiter(self.local_cap(model, target), sleep=self._sleep)
            with self._lock:
                limiter = self._local.setdefault((model, target), limiter)
        return limiter

    async def aclose(self) -> None:
        """Stop the probe task; safe to call twice."""
        task, self._probe_task = self._probe_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def _gauge(self, value: float, labels: dict[str, str]) -> None:
        if self._metrics is not None:
            try:
                self._metrics.gauge(FAILOVER_ACTIVE, value, labels)
            except Exception:  # metrics must never break failover
                logger.exception("failover_metric_failed")
