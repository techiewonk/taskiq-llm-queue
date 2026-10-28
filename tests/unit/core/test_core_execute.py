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
from taskiq_llm_queue.core import (
    MAX_BACKOFF_SECS,
    CoreDeps,
    InfraError,
    backoff_secs,
    execute_job,
)
from taskiq_llm_queue.errors import QueueUnavailable
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
    """Breaker that blocks targets listed in `open_targets` and records outcomes."""

    open_targets: set[str] = field(default_factory=set)
    records: list[tuple[str, bool | None]] = field(default_factory=list)

    async def allow(self, model: str, target: str) -> bool:
        return target not in self.open_targets

    async def record(self, model: str, target: str, *, ok: bool) -> None:
        self.records.append((target, ok))

    async def record_neutral(self, model: str, target: str) -> None:
        self.records.append((target, None))


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
    """Three throttles on the primary, then the APAC target serves; slots never leak (DS10)."""
    throttle = FakeError(ErrorKind.THROTTLE, "ThrottlingException")
    world.provider.script["primary"] = [throttle, throttle, throttle]
    reply = (await execute_job(envelope(), world.deps)).reply
    assert reply is not None
    assert reply.ok
    meta = reply.meta
    assert (meta.served_target, meta.fallback_used, meta.attempts) == ("apac", True, 4)
    assert world.limiter.held == 0
    throttled = [e[3] for e in world.limiter.events if e[0] == "release"]
    assert throttled == [True, True, True, False]
    assert len(world.sleeps) == 2  # backoff between attempts, not after the last one
    assert ("primary", False) not in world.breaker.records  # throttles are the limiter's
    assert world.breaker.records.count(("primary", None)) == 3


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
    world.provider.script["primary"] = [FakeError(ErrorKind.THROTTLE, "Throttling")] * 3
    world.provider.script["apac"] = [FakeError(ErrorKind.THROTTLE, "Throttling")] * 3
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


def test_backoff_grows_and_honours_retry_after() -> None:
    """Exponential with jitter, raised to Retry-After, capped at MAX_BACKOFF_SECS."""
    rng = random.Random(1)
    assert 1 <= backoff_secs(0, 1.0, rng, None) < 2
    assert 4 <= backoff_secs(2, 1.0, rng, None) < 5
    assert backoff_secs(0, 1.0, rng, 7.0) == 7.0
    assert backoff_secs(10, 1.0, rng, None) == MAX_BACKOFF_SECS


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
