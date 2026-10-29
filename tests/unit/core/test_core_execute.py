"""Unit tests for `core.execute_job`: retry, fallback, limiter discipline, claims, results.

Lua only runs on a real Valkey, so these use in-memory fakes (each records its calls in order,
to assert DS8 / DS10 sequencing); the integration suite covers the real stores.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import fakeredis
import pytest
import redis.exceptions as redis_exceptions

from taskiq_llm_queue import (
    ClaimDecision,
    ClaimState,
    Envelope,
    ErrorInfo,
    ErrorKind,
    KeySpace,
    Lane,
    Lease,
    LLMReply,
    ModelConfig,
    QueueConfig,
    Target,
)
from taskiq_llm_queue.config import ModelSettings
from taskiq_llm_queue.core import (
    CoreDeps,
    InfraError,
    backoff_secs,
    execute_job,
)
from taskiq_llm_queue.errors import QueueUnavailable
from taskiq_llm_queue.model_settings import Backoff, SettingsResolver
from taskiq_llm_queue.reliability.breaker import BreakerState
from taskiq_llm_queue.reliability.claims import FailureCount
from taskiq_llm_queue.reliability.fallback import FallbackConfig


class FakeError(Exception):
    """A provider failure carrying its own classification."""

    def __init__(self, kind: ErrorKind, code: str = "boom") -> None:
        super().__init__(code)
        self.info = ErrorInfo(kind, code)


@dataclass
class FakeProvider:
    """Plays a script per target name: an exception instance or a response dict per call."""

    script: dict[str, list[Any]] = field(default_factory=dict)
    calls: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    name: str = "fake"

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        key = target.region or target.profile_id or "?"
        self.calls.append((op, key, dict(params)))
        queue = self.script.get(key, [])
        outcome = queue.pop(0) if queue else {"output": key}
        if isinstance(outcome, BaseException):
            raise outcome
        return dict(outcome)

    def classify(self, exc: BaseException) -> ErrorInfo:
        return exc.info if isinstance(exc, FakeError) else ErrorInfo(ErrorKind.FATAL, "unknown")


@dataclass
class FakeLimiter:
    """Grants slots unless a (model, target) is in `deny`; records acquire/release order."""

    deny: set[tuple[str, str]] = field(default_factory=set)
    events: list[tuple[str, str, str, bool]] = field(default_factory=list)
    held: int = 0
    waits: list[float] = field(default_factory=list)
    aborts: list[bool] = field(default_factory=list)
    on_acquire: Callable[[], Awaitable[None]] | None = None

    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        self.waits.append(timeout_secs)
        if should_abort is not None:
            self.aborts.append(await should_abort())
        if self.on_acquire is not None:
            await self.on_acquire()
        if (model, target) in self.deny:
            return None
        self.held += 1
        self.events.append(("acquire", model, target, False))
        return Lease(model, target, f"t{len(self.events)}", time.time() + 60)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        self.held -= 1
        self.events.append(("release", lease.model, lease.target, throttled))

    def gate(self, model: str, target: str) -> Any:
        raise NotImplementedError


@dataclass
class FakeClaims:
    """Claim store with a fixed decision and a cancel set; logs every call in `log`."""

    log: list[str] = field(default_factory=list)
    decision: ClaimDecision = ClaimDecision.RUN
    cancelled: set[str] = field(default_factory=set)

    async def claim(self, request_id: str, *, deadline_at: float, worker: str) -> ClaimDecision:
        self.log.append(f"claim:{worker}")
        return self.decision

    async def complete(
        self, request_id: str, state: ClaimState, *, worker: str | None = None
    ) -> None:
        self.log.append(f"complete:{state.value}")

    async def is_cancelled(self, request_id: str) -> bool:
        return request_id in self.cancelled

    async def release(self, request_id: str, *, worker: str | None = None) -> None:
        self.log.append("release")

    async def record_failure(self, request_id: str) -> FailureCount:
        return FailureCount(1, False)

    async def heartbeat(self, request_ids: Sequence[str], *, worker: str) -> None:
        return None


@dataclass
class FakeResults:
    """Result store that appends to a shared log so ordering with claims can be checked."""

    log: list[str]
    replies: dict[str, LLMReply] = field(default_factory=dict)

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        self.log.append("put")
        self.replies[reply.request_id] = reply

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        return {t: self.replies.pop(t) for t in task_ids if t in self.replies}


@dataclass
class FakeBreaker:
    """Breaker that blocks targets listed in `open_targets` and records outcomes.

    A failure on a target in `open_on_failure` opens its breaker (the state `record` returns).
    """

    open_targets: set[str] = field(default_factory=set)
    open_on_failure: set[str] = field(default_factory=set)
    records: list[tuple[str, bool | None]] = field(default_factory=list)

    def _state(self, target: str) -> BreakerState:
        return BreakerState.OPEN if target in self.open_targets else BreakerState.CLOSED

    async def allow(self, model: str, target: str) -> bool:
        return target not in self.open_targets

    async def record(self, model: str, target: str, *, ok: bool) -> BreakerState:
        self.records.append((target, ok))
        if not ok and target in self.open_on_failure:
            self.open_targets.add(target)
        return self._state(target)

    async def record_neutral(self, model: str, target: str) -> BreakerState:
        self.records.append((target, None))
        return self._state(target)


def config(**overrides: Any) -> QueueConfig:
    """Two Bedrock models with a primary and an APAC target; fast retries."""
    models = {
        "big": ModelConfig(
            provider="bedrock",
            model_id="big-id",
            targets={"primary": Target(region="primary"), "apac": Target(profile_id="apac")},
            queues={"rt": "q-big-rt", "default": "q-big"},
        ),
        "small": ModelConfig(
            provider="bedrock",
            model_id="small-id",
            targets={"primary": Target(region="small-primary")},
        ),
    }
    fields: dict[str, Any] = {
        "models": models,
        "retry_base_secs": 0.0,
        "fallback_default": {"chains": {"big": ["small"]}},
    }
    fields.update(overrides)
    return QueueConfig(**fields)


# ---------------------------------------------------------------- tests


def envelope(**overrides: Any) -> Envelope:
    now = time.time()
    fields: dict[str, Any] = {
        "request_id": "r-1",
        "op": "converse",
        "model": "big",
        "params": {"messages": [{"role": "user", "content": [{"text": "hi"}]}]},
        "enqueued_at": now - 0.25,
        "deadline_at": now + 30,
    }
    fields.update(overrides)
    return Envelope.model_validate(fields)


@dataclass
class World:
    provider: FakeProvider
    limiter: FakeLimiter
    claims: FakeClaims
    breaker: FakeBreaker
    deps: CoreDeps
    sleeps: list[float]


@pytest.fixture
def world() -> World:
    log: list[str] = []
    provider, limiter = FakeProvider(), FakeLimiter()
    claims, results, breaker = FakeClaims(log=log), FakeResults(log), FakeBreaker()
    sleeps: list[float] = []

    async def sleep(secs: float) -> None:
        sleeps.append(secs)

    cfg = config()
    deps = CoreDeps(
        config=cfg,
        keys=KeySpace("t"),
        providers=lambda alias: provider,
        limiter=limiter,
        claims=cast(Any, claims),
        breaker=cast(Any, breaker),
        results=results,
        fallback=lambda: FallbackConfig.model_validate(cfg.fallback_default),
        worker="pod-1",
        sleep=sleep,
    )
    return World(provider, limiter, claims, breaker, deps, sleeps)


async def test_success_sets_model_id_and_meta(world: World) -> None:
    """The step's model id is injected and meta says how the call was served."""
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert reply.response == {"output": "primary"}
    assert world.provider.calls[0][2]["modelId"] == "big-id"
    meta = reply.meta
    assert (meta.served_model, meta.served_target, meta.fallback_used) == ("big", "primary", False)
    assert (meta.attempts, meta.worker) == (1, "pod-1")
    assert meta.queue_wait_ms >= 200


async def test_result_written_before_claim_done(world: World) -> None:
    """DS8 ordering: claim, result put, then CAS done (taskiq acks only afterwards)."""
    await execute_job(envelope(), world.deps)
    assert world.claims.log == ["claim:pod-1", "put", "complete:done"]


async def test_throttles_retry_then_fall_back_to_other_target(world: World) -> None:
    """Six throttles on the primary, then the APAC target serves; slots never leak (DS10)."""
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle] * 6
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    meta = reply.meta
    assert (meta.served_target, meta.fallback_used, meta.attempts) == ("apac", True, 7)
    assert world.limiter.held == 0
    throttled = [e[3] for e in world.limiter.events if e[0] == "release"]
    assert throttled == [True] * 6 + [False]
    assert len(world.sleeps) == 5  # backoff between attempts, not after the last one
    assert ("primary", False) not in world.breaker.records  # throttles are the limiter's
    assert world.breaker.records.count(("primary", None)) == 6


async def test_throttled_step_gets_more_tries_than_errors(world: World) -> None:
    """Bug S35b: under 30 % random 429s a step gave up after 3 tries (2.7 % of calls failed).

    A throttled step is retried up to `throttle_retry_attempts`; plain errors keep 3 tries.
    """
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle] * 5
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert (reply.meta.served_target, reply.meta.attempts) == ("primary", 6)
    world.provider.script["primary"] = [FakeError(ErrorKind.RETRYABLE)] * 3
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert (reply.meta.served_target, reply.meta.attempts) == ("apac", 4)


async def test_release_happens_before_sleep(world: World) -> None:
    """DS10: the slot of a failed attempt is released before the backoff sleep."""
    order: list[str] = []
    world.provider.script["primary"] = [FakeError(ErrorKind.RETRYABLE)]
    original = world.limiter.release

    async def release(lease: Lease, *, throttled: bool) -> None:
        order.append("release")
        await original(lease, throttled=throttled)

    async def sleep(secs: float) -> None:
        order.append("sleep")

    world.limiter.release = release  # type: ignore[method-assign]
    world.deps.sleep = sleep
    await execute_job(envelope(), world.deps)
    assert order[:2] == ["release", "sleep"]


async def test_chain_policy_switches_model(world: World) -> None:
    """With `chain`, another model follows every target of the first; its id is used."""
    world.provider.script["primary"] = [FakeError(ErrorKind.RETRYABLE)] * 3
    world.provider.script["apac"] = [FakeError(ErrorKind.RETRYABLE)] * 3
    reply = (await execute_job(envelope(fallback="chain"), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert reply.meta.served_model == "small"
    assert world.provider.calls[-1][2]["modelId"] == "small-id"


async def test_fatal_error_is_not_retried(world: World) -> None:
    """Validation-type errors return at once with retryable=False."""
    world.provider.script["primary"] = [FakeError(ErrorKind.FATAL, "ValidationException")]
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert not reply.ok
    assert reply.error is not None
    assert (reply.error["type"], reply.error["retryable"]) == ("ValidationException", False)
    assert len(world.provider.calls) == 1


async def test_exhausted_steps_give_retryable_error(world: World) -> None:
    """Every step failing gives one retryable error reply, never an exception."""
    world.provider.script["primary"] = [FakeError(ErrorKind.THROTTLE, "Throttling")] * 6
    world.provider.script["apac"] = [FakeError(ErrorKind.THROTTLE, "Throttling")] * 6
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert not reply.ok
    assert reply.error is not None
    assert (reply.error["retryable"], reply.error["kind"]) == (True, "throttle")


async def test_no_capacity_moves_to_next_step(world: World) -> None:
    """DS13: no slot on the primary (rt wait over) moves the call to the next target."""
    world.limiter.deny.add(("big", "primary"))
    reply = (await execute_job(envelope(lane="rt"), world.deps)).reply
    assert reply is not None
    assert reply.meta.served_target == "apac"


async def test_open_breaker_skips_target(world: World) -> None:
    """An open breaker skips its target without calling the provider."""
    world.breaker.open_targets.add("primary")
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.meta.served_target == "apac"
    assert [c[1] for c in world.provider.calls] == ["apac"]


async def test_cancel_flag_stops_before_attempt(world: World) -> None:
    """DS14: a cancelled task writes a cancelled reply and completes as cancelled."""
    world.claims.cancelled.add("r-1")
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.cancelled
    assert not reply.ok
    assert world.claims.log[-1] == "complete:cancelled"
    assert world.provider.calls == []


async def test_deadline_passed_expires(world: World) -> None:
    """A task past its deadline is not called and completes as expired."""
    env = envelope()
    world.deps.clock = lambda: env.deadline_at + 1
    reply = (await execute_job(env, world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert reply.error["type"] == "deadline_exceeded"
    assert world.claims.log[-1] == "complete:expired"


@pytest.mark.parametrize(
    "decision", [ClaimDecision.DEFER, ClaimDecision.ACK_DONE, ClaimDecision.ACK_EXPIRED]
)
async def test_non_run_claims_never_call_provider(world: World, decision: ClaimDecision) -> None:
    """Duplicates (DS1-DS3) are acked or deferred without a second provider call."""
    world.claims.decision = decision
    result = await execute_job(envelope(), world.deps)
    assert (result.decision, result.reply) == (decision, None)
    assert world.provider.calls == []


async def test_direct_mode_skips_claim_and_store(world: World) -> None:
    """The direct path runs in-process: no claim, nothing written to the result store."""
    result = await execute_job(envelope(), world.deps, claim=False, store=False)
    assert result.reply is not None
    assert result.reply.ok
    assert world.claims.log == []


async def test_params_by_reference_are_loaded() -> None:
    """`params_ref` is read from Redis (fakeredis: plain GET, no Lua)."""
    redis = fakeredis.aioredis.FakeRedis()
    await redis.set("t:req:r-1", json.dumps({"messages": ["big"]}))
    provider = FakeProvider()
    deps = CoreDeps(
        config=config(),
        keys=KeySpace("t"),
        providers=lambda alias: provider,
        limiter=FakeLimiter(),
        redis=redis,
    )
    reply = (await execute_job(envelope(params=None, params_ref="t:req:r-1"), deps)).reply
    assert reply is not None
    assert reply.ok
    assert provider.calls[0][2]["messages"] == ["big"]
    missing = (await execute_job(envelope(params=None, params_ref="t:req:x"), deps)).reply
    assert missing is not None
    assert missing.error is not None
    assert missing.error["type"] == "params_missing"


def test_backoff_windows_cap_and_retry_after() -> None:
    """Default curve: 1-2.5 s, 4-10 s, 16-30 s, then the 30 s cap; throttles cap at 10 s.

    A provider Retry-After wins when larger, up to `max_secs`; a zero base never sleeps.
    """
    rng = random.Random(1)
    curve = Backoff()
    for attempt, (low, high) in enumerate([(1, 2.5), (4, 10), (16, 30), (30, 30)]):
        delays = [backoff_secs(attempt, curve, rng, None) for _ in range(200)]
        assert low <= min(delays)
        assert max(delays) <= high
    assert all(backoff_secs(2, curve, rng, None, throttled=True) == 10 for _ in range(20))
    assert backoff_secs(0, curve, rng, 7.0) == 7.0
    assert backoff_secs(0, curve, rng, 99.0) == 30.0
    assert backoff_secs(5, Backoff(base_secs=0), rng, None) == 0
    custom = Backoff(base_secs=0.5, multiplier=3, spread=2, max_secs=60)
    assert 4.5 <= backoff_secs(2, custom, rng, None) <= 9


# ---------------------------------------------------------------- reliability regressions


@dataclass
class FlakyResults:
    """Result store whose first `failures` puts raise (Valkey down while storing)."""

    log: list[str]
    failures: int = 0
    replies: dict[str, LLMReply] = field(default_factory=dict)

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        if self.failures:
            self.failures -= 1
            raise redis_exceptions.ConnectionError("down")
        self.log.append("put")
        self.replies[reply.request_id] = reply

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        return {}


async def test_claim_complete_failure_after_store_is_logged_only(world: World) -> None:
    """Bug #1: a RedisError in `claims.complete` after the reply was stored must not escape
    (taskiq would ack and overwrite the good reply with `task_error`)."""

    async def broken_complete(
        request_id: str, state: ClaimState, *, worker: str | None = None
    ) -> None:
        raise redis_exceptions.ConnectionError("down")

    world.claims.complete = broken_complete  # type: ignore[method-assign]
    result = await execute_job(envelope(), world.deps)
    assert result.reply is not None
    assert result.reply.ok
    assert world.claims.log == ["claim:pod-1", "put"]


async def test_breaker_opened_by_a_failure_stops_retries_on_that_target(world: World) -> None:
    """S12: a failure that opens the breaker ends the step; no retry hits the open target.

    In-flight calls used to keep retrying it (3 calls in 8 s while open, longer backoff).
    """
    world.breaker.open_on_failure.add("primary")
    world.provider.script["primary"] = [FakeError(ErrorKind.RETRYABLE, "ServiceUnavailable")] * 3
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.meta.served_target == "apac"
    assert [c[1] for c in world.provider.calls] == ["primary", "apac"]
    assert world.sleeps == []  # no backoff before giving up on the open target


async def test_throttles_do_not_trip_the_breaker(world: World) -> None:
    """Bug #5: throttles are neutral for the breaker; only 5xx/timeouts count as failures."""
    world.provider.script["primary"] = [
        FakeError(ErrorKind.THROTTLE, "ThrottlingException"),
        FakeError(ErrorKind.RETRYABLE, "ServiceUnavailable"),
    ]
    await execute_job(envelope(), world.deps)
    assert world.breaker.records == [("primary", None), ("primary", False), ("primary", True)]


async def test_all_circuits_open_waits_within_deadline(world: World) -> None:
    """Bug #5: with every step's breaker open, an rt call waits and retries before its
    deadline instead of failing at once with `circuit_open`."""
    world.breaker.open_targets.update({"primary", "apac"})

    async def sleep(secs: float) -> None:
        world.sleeps.append(secs)
        world.breaker.open_targets.clear()  # cooldown passed

    world.deps.sleep = sleep
    reply = (await execute_job(envelope(lane="rt"), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert world.sleeps == [1.0]


async def test_redis_error_on_acquire_raises_infra_error(world: World) -> None:
    """Bug #6: a Valkey blip in `limiter.acquire` is infrastructure (InfraError, nothing
    stored, claim not completed), not a stored non-retryable `internal_error`."""

    async def boom() -> None:
        raise redis_exceptions.ConnectionError("down")

    world.limiter.on_acquire = boom
    with pytest.raises(InfraError) as info:
        await execute_job(envelope(), world.deps)
    assert info.value.attempted is False
    assert world.claims.log == ["claim:pod-1"]


async def test_redis_error_after_an_attempt_reports_attempted(world: World) -> None:
    """Bug #3/#6: an InfraError after a provider attempt says so, so `direct` never re-runs."""
    world.provider.script["primary"] = [FakeError(ErrorKind.RETRYABLE)]
    calls = 0

    async def second_fails() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise redis_exceptions.ConnectionError("down")

    world.limiter.on_acquire = second_fails
    with pytest.raises(InfraError) as info:
        await execute_job(envelope(), world.deps)
    assert info.value.attempted is True
    assert world.limiter.held == 0


def _fake_clock(world: World) -> list[float]:
    """Drive `deps.clock` from `deps.sleep`, so waits are counted without real time."""
    now = [time.time()]

    async def sleep(secs: float) -> None:
        world.sleeps.append(secs)
        now[0] += secs

    world.deps.clock = lambda: now[0]
    world.deps.sleep = sleep
    return now


async def test_rt_call_walks_the_chain_again_before_no_capacity(world: World) -> None:
    """Bug S35: an rt call failed `no_capacity` after one 2 s slot wait in a burst ramp.

    rt walks the fallback chain again, so a slot that frees up on the second pass serves it.
    """
    targets = {("big", "primary"), ("big", "apac")}
    world.limiter.deny.update(targets)
    original = world.limiter.acquire

    async def freed_on_second_pass(*args: Any, **kwargs: Any) -> Lease | None:
        if len(world.limiter.waits) >= 2:
            world.limiter.deny.clear()
        return await original(*args, **kwargs)

    world.limiter.acquire = freed_on_second_pass  # type: ignore[method-assign]
    reply = (await execute_job(envelope(lane="rt"), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert reply.meta.served_target == "primary"


async def test_rt_waits_for_capacity_up_to_rt_capacity_wait_secs(world: World) -> None:
    """Bug S35b: 36 of 6,043 rt calls gave up after 3 passes with 33-157 s of deadline left.

    rt now keeps walking the chain for `rt_capacity_wait_secs` (30 s), 2 s per pass when the
    limiter refuses at once, then fails retryable `no_capacity` with the queue wait in meta.
    """
    _fake_clock(world)
    world.limiter.deny.update({("big", "primary"), ("big", "apac"), ("small", "primary")})
    reply = (
        await execute_job(envelope(lane="rt", deadline_at=time.time() + 300), world.deps)
    ).reply
    assert reply is not None
    assert reply.error is not None
    assert (reply.error["type"], reply.error["retryable"]) == ("no_capacity", True)
    assert sum(world.sleeps) == pytest.approx(30, abs=2)
    assert reply.meta.queue_wait_ms >= 200
    assert reply.meta.attempts == 0


async def test_rt_capacity_wait_is_per_model_and_bounded_by_deadline(world: World) -> None:
    """A live `rt_capacity_wait_secs` of 0 fails after one pass; the deadline caps a long wait."""
    _fake_clock(world)
    world.limiter.deny.update({("big", "primary"), ("big", "apac"), ("small", "primary")})
    _live(world, models={"big": {"rt_capacity_wait_secs": 0}})
    await execute_job(envelope(lane="rt", deadline_at=time.time() + 300), world.deps)
    assert world.sleeps == []
    _live(world, models={"big": {"rt_capacity_wait_secs": 600}})
    world.sleeps.clear()
    reply = (await execute_job(envelope(lane="rt", request_id="r-2"), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert 28 <= sum(world.sleeps) <= 30  # the envelope's 30 s deadline wins over 600 s


async def test_error_reply_meta_names_last_target_and_attempts(world: World) -> None:
    """Failed replies carry attempts, queue wait and the last target tried, like a success."""
    error = FakeError(ErrorKind.RETRYABLE, "ServiceUnavailable")
    world.provider.script = {
        "primary": [error] * 3,
        "apac": [error] * 3,
        "small-primary": [error] * 3,
    }
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert not reply.ok
    assert (reply.meta.served_model, reply.meta.served_target) == ("big", "apac")
    assert reply.meta.fallback_used
    assert reply.meta.attempts == 6  # 3 per target; `small` needs the chain policy
    assert reply.meta.queue_wait_ms >= 200


async def test_queued_bg_call_without_capacity_is_requeued(world: World) -> None:
    """Bug #7: a queued bg call waits a bounded time per step, then releases its claim and
    asks for a delayed release (DS4) instead of holding a worker slot until its deadline."""
    world.limiter.deny.update({("big", "primary"), ("big", "apac")})
    result = await execute_job(envelope(lane="bg", deadline_at=time.time() + 300), world.deps)
    assert result.reply is None
    assert result.requeue_secs is not None
    assert 60 <= result.requeue_secs <= 80
    assert world.limiter.waits == [15.0, 15.0]
    assert world.claims.log == ["claim:pod-1", "release"]
    assert world.limiter.aborts == [False, False]  # the cancel flag is checked while waiting


async def test_cancel_checked_right_after_acquire(world: World) -> None:
    """Bug #7 (DS14): a cancel that lands while waiting for a slot releases the slot and
    never calls the provider."""

    async def cancel_now() -> None:
        world.claims.cancelled.add("r-1")

    world.limiter.on_acquire = cancel_now
    reply = (await execute_job(envelope(lane="rt"), world.deps)).reply
    assert reply is not None
    assert reply.cancelled
    assert world.provider.calls == []
    assert world.limiter.held == 0


async def test_unstored_reply_is_cached_and_reused_on_redelivery(world: World) -> None:
    """Bug #15 (DS8): a reply whose write failed is cached on the pod; the redelivery stores
    it without a second provider call."""
    results = FlakyResults(world.claims.log, failures=1)
    world.deps.results = results
    world.deps.store_retry_secs = 0.0
    with pytest.raises(QueueUnavailable):
        await execute_job(envelope(), world.deps)
    assert len(world.deps.reply_cache) == 1
    assert "complete:done" not in world.claims.log  # claim left running
    result = await execute_job(envelope(), world.deps)
    assert result.reply is not None
    assert result.reply.ok
    assert len(world.provider.calls) == 1
    assert "r-1" in results.replies


class HangingProvider(FakeProvider):
    """Never answers within the test's deadline."""

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        await asyncio.sleep(10)
        return {}


class ProviderTimeout(FakeProvider):
    """Raises its own `TimeoutError` (e.g. a socket read timeout) well before the deadline."""

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        self.calls.append((op, "?", dict(params)))
        raise TimeoutError

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(ErrorKind.RETRYABLE, "timeout")


async def test_deadline_cut_attempt_is_expired_not_provider_error(world: World) -> None:
    """Item 7: our deadline cap firing means `deadline_exceeded` (caller sees a timeout)."""
    world.deps.providers = lambda alias: HangingProvider()
    now = time.time()
    reply = (await execute_job(envelope(enqueued_at=now, deadline_at=now + 0.2), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert reply.error["type"] == "deadline_exceeded"
    assert world.claims.log[-1] == "complete:expired"
    assert world.limiter.held == 0


async def test_provider_timeout_stays_retryable(world: World) -> None:
    """A provider's own TimeoutError is retried and reported, not mistaken for the deadline."""
    provider = ProviderTimeout()
    world.deps.providers = lambda alias: provider
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert (reply.error["type"], reply.error["retryable"]) == ("timeout", True)
    assert len(provider.calls) >= world.deps.config.retry_attempts  # retried, every step
    assert world.claims.log[-1] == "complete:done"


def test_attempt_message_does_not_repeat_the_code() -> None:
    """`ThrottlingException: ThrottlingException` became just the code; others keep the type."""
    from taskiq_llm_queue.core import _attempt_message

    class ThrottlingException(Exception):
        pass

    same = ErrorInfo(ErrorKind.THROTTLE, "ThrottlingException")
    assert _attempt_message(ThrottlingException(), same) == "ThrottlingException"
    other = ErrorInfo(ErrorKind.RETRYABLE, "timeout")
    assert _attempt_message(TimeoutError(), other) == "TimeoutError: timeout"
    assert _attempt_message(ValueError(), ErrorInfo(ErrorKind.FATAL)) == "ValueError: fatal"


# ---------------------------------------------------------------- S38: target-specific errors


async def test_target_error_falls_through_to_next_target(world: World) -> None:
    """Bug S38: ResourceNotFound in one region ended the call; now the APAC profile serves it.

    The region is called once (no retry there) and the breaker sees a neutral outcome.
    """
    missing = FakeError(ErrorKind.FATAL, "ResourceNotFoundException")
    world.provider.script["primary"] = [missing]
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert (reply.meta.served_target, reply.meta.attempts) == ("apac", 2)
    assert [c[1] for c in world.provider.calls] == ["primary", "apac"]
    assert world.breaker.records == [("primary", None), ("apac", True)]
    assert world.sleeps == []
    assert world.limiter.held == 0


@pytest.mark.parametrize("code", ["ResourceNotFoundException", "AccessDeniedException"])
async def test_target_error_on_every_target_is_fatal(world: World, code: str) -> None:
    """Bug S38: when every target refuses the model the reply is fatal, not retryable."""
    world.provider.script["primary"] = [FakeError(ErrorKind.FATAL, code)]
    world.provider.script["apac"] = [FakeError(ErrorKind.FATAL, code)]
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert (reply.error["type"], reply.error["retryable"], reply.error["kind"]) == (
        code,
        False,
        "fatal",
    )
    assert len(world.provider.calls) == 2


async def test_request_fatal_error_still_ends_the_call(world: World) -> None:
    """Bug S38 guard: a bad payload (ValidationException) never tries another target."""
    world.provider.script["primary"] = [FakeError(ErrorKind.FATAL, "ValidationException")]
    reply = (await execute_job(envelope(fallback="chain"), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert reply.error["retryable"] is False
    assert [c[1] for c in world.provider.calls] == ["primary"]


# ---------------------------------------------------------------- S39: per-attempt timeout


class HangOn(FakeProvider):
    """Hangs on the targets in `hang` (until cancelled); answers at once elsewhere."""

    def __init__(self, *hang: str) -> None:
        super().__init__()
        self.hang = set(hang)

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        key = target.region or target.profile_id or "?"
        if key in self.hang:
            self.calls.append((op, key, dict(params)))
            await asyncio.sleep(60)
        return await super().call(op, params, target)

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(ErrorKind.FATAL, type(exc).__name__)


async def test_hung_target_times_out_and_next_target_serves(world: World) -> None:
    """Bug S39: a hung region used the whole call budget; now the attempt is cut at
    `attempt_timeout_secs` and the next target serves, well inside the deadline."""
    provider = HangOn("primary")
    world.deps.providers = lambda alias: provider
    world.deps.config = config(attempt_timeout_secs=0.2)
    t0 = time.monotonic()
    reply = (await execute_job(envelope(), world.deps)).reply
    assert time.monotonic() - t0 < 2
    assert reply is not None
    assert reply.ok
    assert reply.meta.served_target == "apac"
    assert [c[1] for c in provider.calls] == ["primary", "apac"]
    assert world.breaker.records[0] == ("primary", False)  # a hang counts against the target
    assert world.limiter.held == 0


async def test_attempt_timeout_on_last_target_retries_then_is_retryable(world: World) -> None:
    """Bug S39: on the last step a timed-out attempt is retried in place; the reply is a
    retryable `attempt_timeout`, not a fatal error and not `deadline_exceeded`."""
    provider = HangOn("primary", "apac")
    world.deps.providers = lambda alias: provider
    world.deps.config = config(attempt_timeout_secs=0.05)
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert (reply.error["type"], reply.error["retryable"]) == ("attempt_timeout", True)
    assert [c[1] for c in provider.calls] == ["primary", "apac", "apac", "apac"]
    assert world.claims.log[-1] == "complete:done"


async def test_attempt_timeout_is_capped_by_the_deadline(world: World) -> None:
    """Bug S39 guard: an attempt timeout longer than the time left still ends as expired."""
    world.deps.providers = lambda alias: HangOn("primary")
    world.deps.config = config(attempt_timeout_secs=60)
    now = time.time()
    reply = (await execute_job(envelope(enqueued_at=now, deadline_at=now + 0.2), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert reply.error["type"] == "deadline_exceeded"


# ---------------------------------------------------------------- S48: model not configured


async def test_unknown_model_is_released_unclaimed(world: World) -> None:
    """Bug S48: a worker lacking the alias acked it with internal_error; now it hands the
    message back (with a delay) without claiming, so a configured pod can run it."""
    result = await execute_job(envelope(model="cohere"), world.deps)
    assert result.reply is None
    assert result.requeue_secs is not None
    assert result.requeue_secs > 0
    assert world.claims.log == []
    assert world.provider.calls == []


@pytest.mark.parametrize(
    ("deliveries", "secs_left"), [(8, 300.0), (1, 15.0)], ids=["budget", "deadline"]
)
async def test_unknown_model_answers_clearly_when_out_of_time(
    world: World, deliveries: int, secs_left: float
) -> None:
    """Bug S48: once the delivery budget or the deadline is spent the caller gets a clear
    `model_not_configured` error (claimed, stored once), never `internal_error`."""
    env = envelope(model="cohere", deadline_at=time.time() + secs_left)
    result = await execute_job(env, world.deps, deliveries=deliveries)
    assert result.requeue_secs is None
    assert result.reply is not None
    assert result.reply.error is not None
    assert (result.reply.error["type"], result.reply.error["retryable"]) == (
        "model_not_configured",
        False,
    )
    assert "cohere" in result.reply.error["message"]
    assert world.claims.log == ["claim:pod-1", "put", "complete:done"]


async def test_unknown_model_on_direct_path_answers_at_once(world: World) -> None:
    """Bug S48: without a claim store (direct path) there is nobody to hand back to."""
    result = await execute_job(envelope(model="cohere"), world.deps, claim=False, store=False)
    assert result.reply is not None
    assert result.reply.error is not None
    assert result.reply.error["type"] == "model_not_configured"


def test_attempt_and_provider_timeouts_read_from_env() -> None:
    """Bug S39: the new timeouts are settable as `LLMQ_*_SECS` and keep sane defaults."""
    cfg = QueueConfig.from_env(
        {
            "LLMQ_ATTEMPT_TIMEOUT_SECS": "20",
            "LLMQ_PROVIDER_READ_TIMEOUT_SECS": "25",
            "LLMQ_PROVIDER_CONNECT_TIMEOUT_SECS": "2",
        }
    )
    assert (
        cfg.attempt_timeout_secs,
        cfg.provider_read_timeout_secs,
        cfg.provider_connect_timeout_secs,
    ) == (20.0, 25.0, 2.0)
    default = QueueConfig()
    assert default.attempt_timeout_secs < default.provider_read_timeout_secs


# ---------------------------------------------------------------- live per-model settings


def _live(world: World, **settings: Any) -> None:
    resolver = SettingsResolver(world.deps.config, ModelSettings.model_validate(settings))
    world.deps.settings = lambda: resolver


async def test_live_attempt_timeout_override_applies_per_step(world: World) -> None:
    """A live `attempt_timeout_secs` of the alias cuts a hung target (config says 180 s)."""
    provider = HangOn("primary")
    world.deps.providers = lambda alias: provider
    _live(world, models={"big": {"attempt_timeout_secs": 0.2}})
    t0 = time.monotonic()
    reply = (await execute_job(envelope(), world.deps)).reply
    assert time.monotonic() - t0 < 2
    assert reply is not None
    assert reply.ok
    assert reply.meta.served_target == "apac"


async def test_live_model_throttle_retries_beat_provider_value(world: World) -> None:
    """Live model `throttle_retry_attempts` (8) wins over the live provider value (2)."""
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle] * 7
    _live(
        world,
        providers={"bedrock": {"throttle_retry_attempts": 2}},
        models={"big": {"throttle_retry_attempts": 8}},
    )
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert (reply.meta.served_target, reply.meta.attempts) == ("primary", 8)
    world.provider.script["primary"] = [throttle] * 7
    _live(world, providers={"bedrock": {"throttle_retry_attempts": 2}})
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert (reply.meta.served_target, reply.meta.attempts) == ("apac", 4)  # 3 = retry_attempts


async def test_throttle_sleep_is_capped_by_throttle_retry_max(world: World) -> None:
    """After a throttle one sleep is at most `throttle_retry_max_secs` (rt deadlines hold)."""
    world.deps.config = config(retry_base_secs=1.0, throttle_retry_max_secs=0.5)
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle] * 3
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert world.sleeps == [0.5, 0.5, 0.5]


async def test_shared_provider_slot_is_held_and_never_learns(world: World) -> None:
    """With a provider cap each attempt also holds `@bedrock/shared`; a throttle is fed only
    to the model's limit, and both slots are back after the call."""
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle]
    _live(world, providers={"bedrock": {"shared_max_concurrency": 4}})
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    assert world.limiter.held == 0
    assert world.limiter.events[:4] == [
        ("acquire", "big", "primary", False),
        ("acquire", "@bedrock", "shared", False),
        ("release", "big", "primary", True),
        ("release", "@bedrock", "shared", False),
    ]
    assert world.limiter.waits[1] <= world.limiter.waits[0]


async def test_full_shared_provider_slot_returns_the_model_slot(world: World) -> None:
    """No shared slot: the model slot is handed back and the step counts as no capacity."""
    world.limiter.deny = {("@bedrock", "shared")}
    _live(world, providers={"bedrock": {"shared_max_concurrency": 1}})
    reply = (await execute_job(envelope(lane=Lane.BG), world.deps)).reply
    assert reply is not None
    assert reply.error is not None
    assert reply.error["type"] == "no_capacity"
    assert world.limiter.held == 0
    assert world.provider.calls == []
