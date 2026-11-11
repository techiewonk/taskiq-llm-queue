"""Engine that decides each call's fate and keeps the per-key counters.

Both API fronts call `Engine.call(key)`: it applies force_error, rpm, max_concurrency,
throttle_rate, latency, timeout_rate and error_rate_5xx in that order, using a seeded
RNG per key so a run with the same seed and request order is reproducible.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field

from fakellm.errors import THROTTLE_ERRORS
from fakellm.scenario import Behaviour, Scenario

_Z95 = 1.6448536269514722  # standard normal 95th percentile


@dataclass(slots=True)
class KeyStats:
    """Counters for one model x target key (served by /admin/stats and /metrics)."""

    calls: int = 0
    ok: int = 0
    concurrent: int = 0
    max_concurrent_seen: int = 0
    throttled: int = 0
    errors: int = 0
    tokens: int = 0
    window: deque[float] = field(default_factory=deque, repr=False)
    token_window: deque[list[float]] = field(default_factory=deque, repr=False)

    def public(self) -> dict[str, int]:
        """Counters without the private rpm / tpm windows."""
        data = asdict(self)
        del data["window"]
        del data["token_window"]
        return data


@dataclass(frozen=True, slots=True)
class Outcome:
    """Result of `Engine.call`: an error name (and retry hint) or a success latency."""

    behaviour: Behaviour
    error: str | None = None
    latency_ms: int = 0
    reservation: list[float] | None = None


class Engine:
    """Holds the scenario, seed, phase clock, RNGs and counters for one fakellm app."""

    def __init__(
        self,
        scenario: Scenario,
        *,
        seed: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Create an engine; `seed` overrides the scenario's, clock/sleep are injectable."""
        self.scenario = scenario
        self.seed_override = seed
        self._clock = clock
        self._sleep = sleep
        self.reset()

    @property
    def seed(self) -> int:
        """Effective seed: explicit override, else the scenario's, else 0."""
        if self.seed_override is not None:
            return self.seed_override
        return self.scenario.seed if self.scenario.seed is not None else 0

    def reset(self) -> None:
        """Clear counters, restart the phase clock and reseed every per-key RNG."""
        self.stats: dict[str, KeyStats] = {}
        self._rngs: dict[str, random.Random] = {}
        self._started = self._clock()

    def replace_scenario(self, scenario: Scenario) -> None:
        """Swap the scenario (PUT /admin/scenario) and reset like POST /admin/reset."""
        self.scenario = scenario
        self.reset()

    @property
    def elapsed_s(self) -> float:
        """Seconds since the last reset (the phase clock)."""
        return self._clock() - self._started

    def behaviour(self, key: str) -> Behaviour:
        """Effective behaviour of `key` now, with due phases applied."""
        return self.scenario.merged(key, self.elapsed_s)

    def resolve_bedrock_key(self, model_id: str, region: str, account: str | None = None) -> str:
        """Pick the scenario key: `<model>@<account>/<region>`, `<model>@<region>`, the model id.

        The account is the caller's access key id, so tests give each fake AWS account its own
        key id and quota. Profile ARNs also match on their last path segment (the profile id).
        """
        models = self.scenario.models
        ids = [model_id]
        if model_id.startswith("arn:") and "/" in model_id:
            ids.append(model_id.rsplit("/", 1)[1])
        candidates = []
        for one in ids:
            if account:
                candidates.append(f"{one}@{account}/{region}")
            candidates += [f"{one}@{region}", one]
        return next((c for c in candidates if c in models), f"{model_id}@{region}")

    def resolve_openai_key(self, model: str, account: str | None) -> str:
        """Scenario key of an OpenAI-compatible call: `<model>@<api key>`, else the model."""
        if account and f"{model}@{account}" in self.scenario.models:
            return f"{model}@{account}"
        return model

    def settle(self, key: str, outcome: Outcome, used_tokens: int) -> None:
        """Replace a call's TPM reservation by the tokens it really used (end of the call)."""
        stats = self._key_stats(key)
        stats.tokens += used_tokens
        if outcome.reservation is not None:
            outcome.reservation[1] = float(used_tokens)

    def _key_stats(self, key: str) -> KeyStats:
        return self.stats.setdefault(key, KeyStats())

    def record_error(self, key: str) -> None:
        """Count a request rejected before the engine ran (e.g. a malformed body)."""
        stats = self._key_stats(key)
        stats.calls += 1
        stats.errors += 1

    def _latency_ms(self, rng: random.Random, b: Behaviour) -> float:
        # Log-normal through p50 and p95, capped at 4 x p95 so one draw can't stall a test.
        if b.latency_p50_ms <= 0:
            return max(0.0, b.latency_p95_ms * rng.random())
        sigma = math.log(b.latency_p95_ms / b.latency_p50_ms) / _Z95
        return min(b.latency_p50_ms * math.exp(sigma * rng.gauss(0.0, 1.0)), 4 * b.latency_p95_ms)

    async def call(self, key: str, reserve_tokens: int = 0) -> Outcome:
        """Admit or reject one call for `key`, sleep its latency and roll late failures.

        `reserve_tokens` (input + max_tokens) is held against `tpm` until `settle`.
        """
        b = self.behaviour(key)
        stats = self._key_stats(key)
        stats.calls += 1
        rng = self._rngs.setdefault(key, random.Random(f"{self.seed}:{key}"))
        # Always draw the same number of values so one knob never shifts another's sequence.
        throttle_roll, timeout_roll, error_roll, which_5xx = (rng.random() for _ in range(4))
        latency_ms = self._latency_ms(rng, b)
        error: str | None = None
        if b.force_error is not None:
            error = b.force_error
        elif self.scenario.strict_models and key not in self.scenario.models:
            error = "ResourceNotFoundException"
        else:
            now = self._clock()
            while stats.window and now - stats.window[0] >= 60.0:
                stats.window.popleft()
            if (
                (b.rpm is not None and len(stats.window) >= b.rpm)
                or (b.max_concurrency is not None and stats.concurrent >= b.max_concurrency)
                or throttle_roll < b.throttle_rate
                or self._over_tpm(stats, b, now, reserve_tokens)
            ):
                error = "ThrottlingException"
            else:
                stats.window.append(now)
        if error is not None:
            if error in THROTTLE_ERRORS:
                stats.throttled += 1
            else:
                stats.errors += 1
            return Outcome(b, error=error)
        reservation: list[float] | None = None
        if b.tpm is not None:
            reservation = [self._clock(), float(reserve_tokens)]
            stats.token_window.append(reservation)
        stats.concurrent += 1
        stats.max_concurrent_seen = max(stats.max_concurrent_seen, stats.concurrent)
        try:
            if b.hang:
                await asyncio.Event().wait()  # never answers; the client gives up
            await self._sleep(latency_ms / 1000.0)
        finally:
            stats.concurrent -= 1
        if timeout_roll < b.timeout_rate:
            error = "ModelTimeoutException"
        elif error_roll < b.error_rate_5xx:
            error = "InternalServerException" if which_5xx < 0.5 else "ServiceUnavailableException"
        if error is not None:
            stats.errors += 1
            return Outcome(b, error=error)
        stats.ok += 1
        return Outcome(b, latency_ms=round(latency_ms), reservation=reservation)

    @staticmethod
    def _over_tpm(stats: KeyStats, b: Behaviour, now: float, reserve: int) -> bool:
        if b.tpm is None:
            return False
        while stats.token_window and now - stats.token_window[0][0] >= 60.0:
            stats.token_window.popleft()
        used = sum(entry[1] for entry in stats.token_window)
        return used + reserve > b.tpm

    def snapshot(self) -> dict[str, object]:
        """Stats body for GET /admin/stats."""
        return {
            "elapsed_s": round(self.elapsed_s, 3),
            "seed": self.seed,
            "models": {k: s.public() for k, s in sorted(self.stats.items())},
        }

    def prometheus(self) -> str:
        """Same counters in Prometheus text exposition format (GET /metrics)."""
        metrics = [
            ("calls", "counter", "Requests received"),
            ("ok", "counter", "Requests answered successfully"),
            ("throttled", "counter", "Requests throttled (429)"),
            ("errors", "counter", "Requests failed with a non-throttle error"),
            ("concurrent", "gauge", "Requests in flight"),
            ("max_concurrent_seen", "gauge", "Peak requests in flight since reset"),
        ]
        lines: list[str] = []
        for name, kind, help_text in metrics:
            full = f"fakellm_{name}_total" if kind == "counter" else f"fakellm_{name}"
            lines += [f"# HELP {full} {help_text}", f"# TYPE {full} {kind}"]
            for key, stats in sorted(self.stats.items()):
                label = key.replace("\\", "\\\\").replace('"', '\\"')
                lines.append(f'{full}{{key="{label}"}} {getattr(stats, name)}')
        lines += [
            "# HELP fakellm_elapsed_seconds Seconds since the last reset (phase clock)",
            "# TYPE fakellm_elapsed_seconds gauge",
            f"fakellm_elapsed_seconds {self.elapsed_s:.3f}",
        ]
        return "\n".join(lines) + "\n"
