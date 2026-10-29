"""Rate budgets (RPM / TPM) per model target and per provider, shared by every pod (F17).

A call starts only with a rate token and a concurrency slot (docs/RATE_LIMIT_PLAN.md). The
budget lives in Valkey (Lua `RATE`, one hash per scope); `LocalRateBudget` keeps the same rules
in process while Valkey is down, with the limits divided by the live pod count (C8).
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol

import orjson

from taskiq_llm_queue.reliability import _lua

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.keys import KeySpace

Window = Literal["smooth", "fixed"]
_STATE_TTL_SECS = 180  # an idle budget's state expires; a new one starts full
_BURST_SHARE = 0.1  # default `rate_burst`: 10 % of the per-minute limit
# TPM estimate when a request sets no output limit (corrected from `usage` after the call).
DEFAULT_OUTPUT_TOKENS = 1000


@dataclass(frozen=True, slots=True)
class RateLimits:
    """One scope's budget: per-minute `rpm` / `tpm` (None = off), window shape and rt reserve.

    `burst` is how many calls may start back to back in `smooth` mode (default 10 % of
    `rpm`); TPM uses the same fraction of `tpm`. bg/bulk may use `1 - rt_share` of each.
    """

    rpm: int | None = None
    tpm: int | None = None
    window: Window = "smooth"
    burst: int | None = None
    rt_share: float = 0.2

    @property
    def active(self) -> bool:
        """True when any budget is set."""
        return self.rpm is not None or self.tpm is not None

    def bursts(self) -> tuple[int, int]:
        """Burst allowance in calls (RPM) and tokens (TPM)."""
        rpm_burst = self.burst or max(1, round((self.rpm or 0) * _BURST_SHARE))
        fraction = rpm_burst / self.rpm if self.rpm else _BURST_SHARE
        return rpm_burst, max(1, round((self.tpm or 0) * fraction))

    def scaled(self, factor: float) -> RateLimits:
        """These limits times `factor` (a pod's share while Valkey is down), at least 1."""
        rpm = max(1, math.floor(self.rpm * factor)) if self.rpm else None
        tpm = max(1, math.floor(self.tpm * factor)) if self.tpm else None
        burst = max(1, math.floor(self.burst * factor)) if self.burst else None
        return RateLimits(rpm, tpm, self.window, burst, self.rt_share)


@dataclass(frozen=True, slots=True)
class RateGrant:
    """A taken token: what to give back if the call never starts, or to correct for TPM."""

    model: str
    target: str
    limits: RateLimits
    bg: bool
    tokens: int


class RateBudget(Protocol):
    """Takes and returns rate tokens; `take` gives `(grant, 0)` or `(None, wait_secs)`."""

    async def take(
        self, model: str, target: str, limits: RateLimits, *, bg: bool, tokens: int
    ) -> tuple[RateGrant | None, float]:
        """Take one call and `tokens`, all or nothing; else the seconds until there is room."""
        ...

    async def give(self, grant: RateGrant, tokens: int | None = None, *, call: bool) -> None:
        """Return the call (when it never started) and tokens (negative: charge more)."""
        ...


def _args(op: str, limits: RateLimits, *, bg: bool, tokens: int, calls: int = 0) -> list[Any]:
    rpm_burst, tpm_burst = limits.bursts()
    rpm_cost = 1 if op != "give" or calls else 0
    return [
        op,
        limits.window,
        1 if bg else 0,
        1 - limits.rt_share,
        _STATE_TTL_SECS,
        limits.rpm or 0,
        rpm_cost,
        rpm_burst,
        limits.tpm or 0,
        tokens,
        tpm_burst,
        calls,
    ]


class ValkeyRateBudget:
    """Cluster-wide budgets in Valkey: one `RATE` call per take, refund or correction."""

    def __init__(self, client: redis.asyncio.Redis, keys: KeySpace) -> None:
        """Register the script on `client`; no I/O until the first call."""
        self._keys = keys
        self._script = client.register_script(_lua.RATE)

    async def take(
        self, model: str, target: str, limits: RateLimits, *, bg: bool, tokens: int
    ) -> tuple[RateGrant | None, float]:
        """Take one call (and `tokens` for TPM) or say how long until there is room."""
        ok, wait = await self._script(
            [self._keys.rate(model, target)], _args("take", limits, bg=bg, tokens=tokens)
        )
        if int(ok):
            return RateGrant(model, target, limits, bg, tokens), 0.0
        return None, float(_lua.as_text(wait))

    async def give(self, grant: RateGrant, tokens: int | None = None, *, call: bool) -> None:
        """Return the call (`call`, when it never started) and `tokens` (negative: charge more)."""
        amount = grant.tokens if tokens is None else tokens
        args = _args("give", grant.limits, bg=grant.bg, tokens=amount, calls=1 if call else 0)
        await self._script([self._keys.rate(grant.model, grant.target)], args)

    async def peek(self, model: str, target: str, limits: RateLimits) -> dict[str, int]:
        """Calls and tokens started this minute, and units available now (status view)."""
        reply = await self._script(
            [self._keys.rate(model, target)], _args("peek", limits, bg=False, tokens=0)
        )
        calls, tokens, rpm_left, tpm_left = (int(v) for v in reply)
        return {"calls": calls, "tokens": tokens, "rpm_left": rpm_left, "tpm_left": tpm_left}


@dataclass(slots=True)
class _Bucket:
    value: float = 0.0  # smooth: theoretical arrival time; fixed: count this minute
    window: int = -1


@dataclass(slots=True)
class LocalRateBudget:
    """The same rules in process, for Valkey outages: limits times `share` (1 / live pods).

    Best effort by design (R09): pods do not see each other, so the total is right only while
    the pod count is right.
    """

    share: Callable[[], float] = lambda: 1.0
    clock: Callable[[], float] = time.time
    _buckets: dict[tuple[str, str, str], _Bucket] = field(default_factory=dict)

    def _check(
        self, key: tuple[str, str, str], window: Window, limit: float, burst: int, cost: int
    ) -> tuple[float, float]:
        now = self.clock()
        bucket = self._buckets.setdefault(key, _Bucket())
        if window == "fixed":
            minute = math.floor(now / 60)
            count = bucket.value if bucket.window == minute else 0.0
            if count + cost <= limit or (count == 0 and cost > limit):
                return 0.0, count + cost
            return (minute + 1) * 60 - now, count
        step = 60 / limit
        tat = max(bucket.value, now)
        new = tat + cost * step
        over = new - now - max(burst, cost) * step
        return (over, tat) if over > 0 else (0.0, new)

    def _scopes(
        self, model: str, target: str, limits: RateLimits, bg: bool, tokens: int
    ) -> list[tuple[tuple[str, str, str], float, int, int]]:
        rpm_burst, tpm_burst = limits.bursts()
        out = []
        for name, limit, burst, cost in (
            ("rpm", limits.rpm, rpm_burst, 1),
            ("tpm", limits.tpm, tpm_burst, tokens),
        ):
            if not limit:
                continue
            out.append(((model, target, name), float(limit), burst, cost))
            if bg:
                share = 1 - limits.rt_share
                bg_burst = max(1, math.floor(burst * share))
                out.append(((model, target, f"{name}_bg"), limit * share, bg_burst, cost))
        return out

    async def take(
        self, model: str, target: str, limits: RateLimits, *, bg: bool, tokens: int
    ) -> tuple[RateGrant | None, float]:
        """Take from this pod's share of the budget, all or nothing."""
        scaled = limits.scaled(self.share())
        checks = [
            (key, self._check(key, scaled.window, limit, burst, cost))
            for key, limit, burst, cost in self._scopes(model, target, scaled, bg, tokens)
        ]
        wait = max((w for _, (w, _) in checks), default=0.0)
        if wait > 0:
            return None, wait
        minute = math.floor(self.clock() / 60)
        for key, (_, value) in checks:
            self._buckets[key] = _Bucket(value, minute)
        return RateGrant(model, target, limits, bg, tokens), 0.0

    async def give(self, grant: RateGrant, tokens: int | None = None, *, call: bool) -> None:
        """Return the call and / or tokens to this pod's buckets."""
        scaled = grant.limits.scaled(self.share())
        amount = grant.tokens if tokens is None else tokens
        now = self.clock()
        for key, limit, _, cost in self._scopes(
            grant.model, grant.target, scaled, grant.bg, amount
        ):
            if key[2].startswith("rpm") and not call:
                continue
            bucket = self._buckets.get(key)
            if bucket is None:
                continue
            if scaled.window == "fixed":
                bucket.value = max(0.0, bucket.value - cost)
            elif cost < 0 or bucket.value > now:
                bucket.value = max(max(bucket.value, now) - cost * 60 / limit, now)


def _text_chars(value: Any) -> int:
    """Characters of every string inside `value` (keys excluded): the prompt size proxy."""
    if isinstance(value, str):
        return len(value)
    if isinstance(value, Mapping):
        return sum(_text_chars(v) for v in value.values())
    if isinstance(value, list | tuple):
        return sum(_text_chars(v) for v in value)
    return 0


def estimate_tokens(params: Mapping[str, Any]) -> int:
    """TPM reservation for one call: input characters / 4 plus the output limit.

    The output limit comes from `inferenceConfig.maxTokens` (Converse), `max_tokens` /
    `max_completion_tokens` (OpenAI-compatible), else `DEFAULT_OUTPUT_TOKENS`. Embedding
    calls have no output limit, so they reserve their input only.
    """
    inference = params.get("inferenceConfig")
    output = None
    if isinstance(inference, Mapping):
        output = inference.get("maxTokens")
    output = output or params.get("max_tokens") or params.get("max_completion_tokens")
    if output is None and not ("input" in params or "inputText" in params or "texts" in params):
        output = DEFAULT_OUTPUT_TOKENS
    return max(1, _text_chars(params) // 4 + int(output or 0))


def usage_tokens(response: Mapping[str, Any]) -> int | None:
    """Tokens a provider reported for one call, or None when the response has no usage."""
    usage = response.get("usage")
    if isinstance(usage, Mapping):
        for total in ("totalTokens", "total_tokens"):
            if isinstance(usage.get(total), int):
                return int(usage[total])
        parts = [usage.get(k) for k in ("inputTokens", "outputTokens", "prompt_tokens")]
        found = [p for p in parts if isinstance(p, int)]
        if found:
            return sum(found) + int(usage.get("completion_tokens") or 0)
    body = response.get("body")
    if isinstance(body, bytes | str):
        try:
            parsed = orjson.loads(body)
        except orjson.JSONDecodeError:
            return None
        if isinstance(parsed, Mapping) and isinstance(parsed.get("inputTextTokenCount"), int):
            return int(parsed["inputTextTokenCount"])
    if isinstance(response.get("inputTextTokenCount"), int):
        return int(response["inputTextTokenCount"])
    return None
