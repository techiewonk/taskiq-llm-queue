"""Unit tests for `core.execute_job`: retry, fallback, limiter discipline, claims, results.

Lua only runs on a real Valkey, so these use in-memory fakes (each records its calls in order,
to assert DS8 / DS10 sequencing); the integration suite covers the real stores.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import fakeredis
import pytest

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
from taskiq_llm_queue.core import MAX_BACKOFF_SECS, CoreDeps, backoff_secs, execute_job
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

    async def acquire(
        self, model: str, target: str, lane: Lane, timeout_secs: float
    ) -> Lease | None:
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

    async def complete(self, request_id: str, state: ClaimState) -> None:
        self.log.append(f"complete:{state.value}")

    async def is_cancelled(self, request_id: str) -> bool:
        return request_id in self.cancelled

    async def release(self, request_id: str) -> None:
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
    records: list[tuple[str, bool]] = field(default_factory=list)

    async def allow(self, model: str, target: str) -> bool:
        return target not in self.open_targets

    async def record(self, model: str, target: str, *, ok: bool) -> None:
        self.records.append((target, ok))


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
    assert ("primary", False) in world.breaker.records


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
