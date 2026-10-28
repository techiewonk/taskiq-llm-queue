"""Regression tests for the worker task bodies, the pod heartbeat, `direct` and failover.

Each test names the reliability bug it pins down. Valkey-only pieces (Lua) are faked; the pod
registry runs on fakeredis (plain sorted-set commands).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import fakeredis
import pytest
import redis.exceptions as redis_exceptions
from taskiq.exceptions import SendTaskError

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
from taskiq_llm_queue.broker.broker import (
    SQS_MESSAGE_ID_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
    SQS_RECEIVE_COUNT_LABEL,
)
from taskiq_llm_queue.broker.errors import ErrorFamily, InvalidLabelError, SQSSendError
from taskiq_llm_queue.client import LLMQueue, _deterministic_send_error, _SyncLimiter
from taskiq_llm_queue.core import (
    UNKNOWN_MODEL_MAX_DELIVERIES,
    UNKNOWN_MODEL_RELEASE_SECS,
    CoreDeps,
    InfraError,
    JobResult,
)
from taskiq_llm_queue.errors import QueueUnavailable
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.lifecycle import ClaimGroup, PodHeartbeat, PodRegistry
from taskiq_llm_queue.reliability.claims import FailureCount
from taskiq_llm_queue.tasks import CLAIM_LOST, WorkerRuntime

LABELS = {SQS_RECEIPT_HANDLE_LABEL: "rh-1", SQS_MESSAGE_ID_LABEL: "m-1"}
MODELS = {
    "big": ModelConfig(
        provider="bedrock", model_id="big-id", targets={"primary": Target(region="r1")}
    )
}


@dataclass
class FakeBroker:
    """Records `release(handle, visibility_secs)`; nothing is ever deleted here."""

    releases: list[tuple[str, int]] = field(default_factory=list)

    async def release(self, target: str, *, visibility_secs: int = 0, reason: str = "") -> bool:
        self.releases.append((target, visibility_secs))
        return True


@dataclass
class Provider:
    """Answers every call; `hook` runs inside the call (to inspect or block it)."""

    hook: Callable[[], Awaitable[None]] | None = None
    calls: int = 0
    name: str = "fake"

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        self.calls += 1
        if self.hook is not None:
            await self.hook()
        return {"output": "ok"}

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(ErrorKind.FATAL, "x")


@dataclass
class Limiter:
    async def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], Awaitable[bool]] | None = None,
    ) -> Lease | None:
        return Lease(model, target, "tok", time.time() + 60)

    async def release(self, lease: Lease, *, throttled: bool) -> None:
        return None

    def gate(self, model: str, target: str) -> Any:
        raise NotImplementedError


@dataclass
class Claims:
    """Claim store with a scriptable `claim`; records every call in `log`."""

    log: list[str] = field(default_factory=list)
    on_claim: Callable[[], Awaitable[ClaimDecision]] | None = None
    failures: FailureCount = field(default_factory=lambda: FailureCount(1, False))

    async def claim(self, request_id: str, *, deadline_at: float, worker: str) -> ClaimDecision:
        self.log.append("claim")
        return await self.on_claim() if self.on_claim else ClaimDecision.RUN

    async def complete(
        self, request_id: str, state: ClaimState, *, worker: str | None = None
    ) -> None:
        self.log.append(f"complete:{state.value}")

    async def release(self, request_id: str, *, worker: str | None = None) -> None:
        self.log.append(f"release:{worker}")

    async def is_cancelled(self, request_id: str) -> bool:
        return False

    async def record_failure(self, request_id: str) -> FailureCount:
        self.log.append("failure")
        return self.failures

    async def heartbeat_owned(self, request_ids: Sequence[str], *, worker: str) -> list[str]:
        self.log.append(f"beat:{worker}:{','.join(request_ids)}")
        return []


@dataclass
class Results:
    replies: dict[str, LLMReply] = field(default_factory=dict)

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        self.replies[reply.request_id] = reply

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        return {}


class Failover:
    """Records Valkey failures reported by the worker."""

    def __init__(self) -> None:
        self.reported: list[BaseException] = []

    def path_override(self, model: str) -> None:
        return None

    def report_sqs_failure(self, model: str, exc: BaseException) -> None:
        return None

    def report_redis_failure(self, exc: BaseException) -> None:
        self.reported.append(exc)

    def redis_down(self) -> bool:
        return bool(self.reported)


@dataclass
class Rig:
    runtime: WorkerRuntime
    claims: Claims
    provider: Provider
    broker: FakeBroker
    failover: Failover


@pytest.fixture
def rig() -> Rig:
    claims, provider, failover = Claims(), Provider(), Failover()
    deps = CoreDeps(
        config=QueueConfig(models=MODELS, retry_base_secs=0.0),
        keys=KeySpace("t"),
        providers=lambda alias: provider,
        limiter=Limiter(),
        claims=cast(Any, claims),
        results=Results(),
        worker="pod-1",
    )
    runtime = WorkerRuntime(deps, defer_secs=20, failover=failover)
    return Rig(runtime, claims, provider, FakeBroker(), failover)


def raw_envelope(**overrides: Any) -> dict[str, Any]:
    now = time.time()
    fields: dict[str, Any] = {
        "request_id": "r-1",
        "op": "converse",
        "model": "big",
        "lane": "rt",
        "params": {"messages": []},
        "enqueued_at": now,
        "deadline_at": now + 30,
    }
    fields.update(overrides)
    return Envelope.model_validate(fields).model_dump(mode="json")


async def run_llm(rig: Rig, raw: dict[str, Any]) -> None:
    await rig.runtime.run_llm(raw, LABELS, cast(Any, rig.broker))


# ---------------------------------------------------------------- worker task bodies


async def test_valkey_error_on_claim_releases_message(rig: Rig) -> None:
    """Bug #1: a RedisError from `claims.claim` never reaches taskiq (which would ack and
    delete the message): the message is released with backoff and Valkey failure reported."""

    async def down() -> ClaimDecision:
        raise redis_exceptions.ConnectionError("down")

    rig.claims.on_claim = down
    await run_llm(rig, raw_envelope())
    assert rig.broker.releases == [("rh-1", 20)]
    assert len(rig.failover.reported) == 1
    assert rig.provider.calls == 0


async def test_unknown_model_is_released_not_acked(rig: Rig) -> None:
    """Bug S48: a worker whose config lacks the alias acked the message with internal_error;
    it now releases it (delayed, unclaimed) so a correctly configured pod can run it."""
    await run_llm(rig, raw_envelope(model="cohere", lane="bg", deadline_at=time.time() + 90))
    ((handle, secs),) = rig.broker.releases
    assert handle == "rh-1"
    assert UNKNOWN_MODEL_RELEASE_SECS <= secs <= 1.5 * UNKNOWN_MODEL_RELEASE_SECS
    assert rig.claims.log == []
    assert rig.runtime.deps.results is not None
    assert cast(Results, rig.runtime.deps.results).replies == {}


async def test_unknown_model_answers_after_the_delivery_budget(rig: Rig) -> None:
    """Bug S48: the SQS receive count reaches `execute_job`; at the budget the caller gets
    `model_not_configured` instead of the message cycling into the DLQ."""
    labels = {**LABELS, SQS_RECEIVE_COUNT_LABEL: str(UNKNOWN_MODEL_MAX_DELIVERIES)}
    raw = raw_envelope(model="cohere", lane="bg", deadline_at=time.time() + 90)
    await rig.runtime.run_llm(raw, labels, cast(Any, rig.broker))
    assert rig.broker.releases == []
    reply = cast(Results, rig.runtime.deps.results).replies["r-1"]
    assert reply.error is not None
    assert reply.error["type"] == "model_not_configured"


async def test_task_registered_only_after_claim_runs(rig: Rig) -> None:
    """Bug #8: a delivery is tracked as running only once its claim says RUN, so the claim
    heartbeat can't report it lost (and cancel it) while the claim is still being decided."""
    seen: list[int] = []

    async def claim() -> ClaimDecision:
        seen.append(len(rig.runtime.running))
        return ClaimDecision.RUN

    async def during_call() -> None:
        seen.append(len(rig.runtime.running))

    rig.claims.on_claim = claim
    rig.provider.hook = during_call
    await run_llm(rig, raw_envelope())
    assert seen == [0, 1]
    assert rig.runtime.running == {}


async def test_lost_claim_releases_message_instead_of_acking(rig: Rig) -> None:
    """Bug #8: a task stopped because another pod now owns its claim releases the message
    (visibility = defer window) rather than letting taskiq ack (delete) it."""
    started = asyncio.Event()

    async def block() -> None:
        started.set()
        await asyncio.sleep(30)

    rig.provider.hook = block
    task = asyncio.create_task(run_llm(rig, raw_envelope()))
    await started.wait()
    rig.runtime.stop_running(CLAIM_LOST, ["r-1"])
    await task  # ends quietly: no CancelledError, so taskiq logs no ERROR traceback
    assert rig.broker.releases == [("rh-1", 20)]
    assert not any(e.startswith("release") for e in rig.claims.log)  # not our claim any more
    assert cast(Results, rig.runtime.deps.results).replies == {}  # no result published


async def test_claim_loss_ends_quietly_but_shutdown_still_cancels(rig: Rig) -> None:
    """Bug: a genuine claim loss re-raised CancelledError, so taskiq printed an ERROR
    traceback. It now ends quietly, but a drain cancel or one on top of it still raises."""
    started = asyncio.Event()

    async def block() -> None:
        started.set()
        await asyncio.sleep(30)

    rig.provider.hook = block
    for stop in ("drain", "claim_lost_then_shutdown"):
        started.clear()
        task = asyncio.create_task(run_llm(rig, raw_envelope()))
        await started.wait()
        if stop == "drain":
            rig.runtime.stop_running("drain", ["r-1"])
        else:
            rig.runtime.stop_running(CLAIM_LOST, ["r-1"])
            task.cancel()  # pod shutdown arrives while the claim loss is being handled
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled(), stop


async def test_two_deliveries_of_one_request_keep_their_own_entries(rig: Rig) -> None:
    """Bug #9: two deliveries of one request on a pod no longer share `running[request_id]`;
    one finishing never drops the other's entry."""
    first = rig.runtime.register("r-1", cast(Any, rig.broker), LABELS)
    second = rig.runtime.register("r-1", cast(Any, rig.broker), LABELS)
    assert len(rig.runtime.running) == 2
    rig.runtime.unregister(first)
    assert list(rig.runtime.running.values()) == [second]
    assert rig.runtime.running_ids() == ["r-1"]


async def test_exhausted_job_is_released_toward_the_dlq(rig: Rig) -> None:
    """DS5: a job past `max_failures` is released (so `maxReceiveCount` dead-letters it),
    never acked and dropped."""

    async def failing(payload: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    rig.runtime.jobs["j"] = failing
    rig.claims.failures = FailureCount(3, True)
    raw = {"request_id": "job-1", "job": "j", "payload": {}, "deadline_at": time.time() + 60}
    await rig.runtime.run_job(raw, LABELS, cast(Any, rig.broker))
    assert rig.broker.releases == [("rh-1", 1)]
    assert "complete:done" not in rig.claims.log


async def test_job_failure_count_redis_error_releases(rig: Rig) -> None:
    """Bug #1: a RedisError from `record_failure` releases the job message (no ack)."""

    async def failing(payload: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    async def down(request_id: str) -> FailureCount:
        raise redis_exceptions.ConnectionError("down")

    rig.runtime.jobs["j"] = failing
    rig.claims.record_failure = down  # type: ignore[method-assign]
    raw = {"request_id": "job-1", "job": "j", "payload": {}, "deadline_at": time.time() + 60}
    await rig.runtime.run_job(raw, LABELS, cast(Any, rig.broker))
    assert rig.broker.releases == [("rh-1", 20)]
    assert rig.failover.reported


# ---------------------------------------------------------------- pod heartbeat


@dataclass
class RenewingLimiter:
    renewed: list[list[Lease]] = field(default_factory=list)

    async def renew(self, leases: list[Lease]) -> list[Lease]:
        self.renewed.append(leases)
        return leases


async def test_pod_heartbeat_covers_direct_claims_and_all_leases() -> None:
    """Bugs #2/#11: one pod-level heartbeat (every mode) refreshes DS17 direct claims as
    `direct:<pod>` and renews leases of the direct and sync paths, not only worker tasks."""
    claims, limiter = Claims(), RenewingLimiter()
    lease = Lease("big", "primary", "t1", time.time() + 240)
    redis = fakeredis.aioredis.FakeRedis()
    live: list[int] = []
    heartbeat = PodHeartbeat(
        claims=cast(Any, claims),
        limiter=cast(Any, limiter),
        leases={lease},
        groups=[
            ClaimGroup("pod-1", list, lambda lost: None),
            ClaimGroup("direct:pod-1", lambda: ["r-9"], lambda lost: None),
        ],
        interval_secs=10.0,
        pods=PodRegistry(redis, "t:pods", "pod-1", ttl_secs=30),
        on_live_pods=live.append,
    )
    await heartbeat.beat_once()
    assert claims.log == ["beat:direct:pod-1:r-9"]
    assert limiter.renewed == [[lease]]
    assert live == [1]


async def test_pod_registry_feeds_live_pods_into_local_cap() -> None:
    """Bug #14: `live_pods` comes from the pod registry, so the Valkey-down local cap is the
    last known limit split across live pods instead of always the fallback cap."""
    redis = fakeredis.aioredis.FakeRedis()
    now = [1000.0]
    a = PodRegistry(redis, "t:pods", "a", ttl_secs=30, clock=lambda: now[0])
    b = PodRegistry(redis, "t:pods", "b", ttl_secs=30, clock=lambda: now[0])
    await a.beat()
    assert await b.beat() == 2
    now[0] += 31
    assert await a.beat() == 1  # b stopped beating
    state = FailoverState(FailoverSettings(local_cap_fallback=3))
    state.note_limit("big", "primary", 100)
    assert state.local_cap("big", "primary") == 3
    state.set_live_pods(4)
    assert state.local_cap("big", "primary") == 25


async def test_valkey_flap_keeps_busy_local_limiter() -> None:
    """Bug #14: `report_redis_ok` no longer drops a local limiter with calls in flight; if
    Valkey fails again they are still counted against the cap."""
    state = FailoverState(FailoverSettings(local_cap_fallback=2))
    state.report_redis_failure(OSError())
    limiter = state.local_limiter("big", "primary")
    assert limiter.try_acquire("big", "primary", Lane.RT) is not None
    state.report_redis_ok()
    state.report_redis_failure(OSError())
    assert state.local_limiter("big", "primary") is limiter


# ---------------------------------------------------------------- client


def direct_queue(
    monkeypatch: pytest.MonkeyPatch, outcomes: list[Any]
) -> tuple[LLMQueue, list[int]]:
    """An LLMQueue whose `execute_job` plays `outcomes` (exceptions or JobResults)."""
    q = LLMQueue(QueueConfig(models=MODELS))
    deps = CoreDeps(config=q.config, keys=q.keys, providers=lambda a: Provider(), limiter=Limiter())
    monkeypatch.setattr(q, "_direct_deps", lambda: deps)
    calls: list[int] = []

    async def fake_execute(envelope: Envelope, deps: CoreDeps, **kwargs: Any) -> JobResult:
        calls.append(1)
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return cast(JobResult, outcome)

    monkeypatch.setattr("taskiq_llm_queue.client.execute_job", fake_execute)
    return q, calls


def infra(attempted: bool) -> InfraError:
    error = InfraError("r-1", attempted=attempted)
    error.__cause__ = redis_exceptions.ConnectionError("down")
    return error


async def test_direct_never_reruns_a_call_that_was_attempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bug #3: if Valkey fails after a provider attempt (e.g. in `claims.complete`), the
    direct path raises instead of running the call a second time."""
    q, calls = direct_queue(monkeypatch, [infra(attempted=True)])
    env = Envelope.model_validate(raw_envelope())
    with pytest.raises(QueueUnavailable):
        await q._direct(env, claim=True)
    assert calls == [1]
    assert q.failover.redis_down()


async def test_direct_falls_back_when_valkey_fails_before_any_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bug #3: a Valkey failure before any attempt (the DS17 claim) runs the call once more
    under the local cap."""
    ok = LLMReply(request_id="r-1", ok=True, response={"x": 1})
    q, calls = direct_queue(monkeypatch, [infra(attempted=False), JobResult(ClaimDecision.RUN, ok)])
    env = Envelope.model_validate(raw_envelope())
    assert await q._direct(env) is ok
    assert calls == [1, 1]


def test_only_transient_send_errors_fail_over() -> None:
    """Bug #20: deterministic send errors (bad label, too large) are not SQS outages and must
    raise to the caller instead of moving the model to `direct` for minutes."""

    def wrapped(cause: BaseException) -> SendTaskError:
        error = SendTaskError()
        error.__cause__ = cause
        return error

    bad_label = InvalidLabelError("delay")
    assert _deterministic_send_error(wrapped(bad_label)) is bad_label
    too_big = SQSSendError("big", family=ErrorFamily.SIZE_LIMIT)
    assert _deterministic_send_error(too_big) is too_big
    assert _deterministic_send_error(SQSSendError("x", family=ErrorFamily.TRANSIENT)) is None
    assert _deterministic_send_error(wrapped(OSError("reset"))) is None


@dataclass
class SlowSyncLimiter:
    """Blocking limiter whose acquire wins a slot only after the caller gave up."""

    released: list[Lease] = field(default_factory=list)

    def acquire(
        self,
        model: str,
        target: str,
        lane: Lane,
        timeout_secs: float,
        *,
        should_abort: Callable[[], bool] | None = None,
    ) -> Lease | None:
        time.sleep(0.2)
        return Lease(model, target, "late", time.time() + 60)

    def release(self, lease: Lease, *, throttled: bool) -> None:
        self.released.append(lease)


async def test_sync_lease_won_after_timeout_is_released() -> None:
    """Bug #11: a slot a sync call's thread wins after the call timed out is released, not
    leaked until its lease expires."""
    inner = SlowSyncLimiter()
    limiter = _SyncLimiter(cast(Any, inner))
    task = asyncio.create_task(limiter.acquire("big", "primary", Lane.RT, 5.0))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(50):
        if inner.released:
            break
        await asyncio.sleep(0.05)
    assert [lease.token for lease in inner.released] == ["late"]


def test_built_brokers_use_the_wire_serializer(rig: Rig) -> None:
    """Item 1: every worker/sender broker writes orjson bodies (readable by older pods)."""
    from taskiq_llm_queue.broker import DeliveryFormatter, WireSerializer
    from taskiq_llm_queue.lifecycle import build_brokers
    from taskiq_llm_queue.reliability import AdaptiveLimiter
    from taskiq_llm_queue.tasks import WorkerResultBackend

    models = {
        "big": ModelConfig(
            provider="bedrock",
            model_id="big-id",
            targets={"primary": Target(region="r1")},
            queues={"rt": "q-rt", "default": "q-bg"},
        )
    }
    config = QueueConfig(models=models, job_queues={"default": "q-jobs"})
    limiter = AdaptiveLimiter(fakeredis.FakeAsyncRedis(), KeySpace("t"))
    backend = WorkerResultBackend(cast(Any, Results()))
    brokers = build_brokers(config, rig.runtime, limiter, backend)
    assert len(brokers.all()) == 2
    for broker in brokers.all():
        assert isinstance(broker.serializer, WireSerializer)
        assert isinstance(broker.formatter, DeliveryFormatter)
