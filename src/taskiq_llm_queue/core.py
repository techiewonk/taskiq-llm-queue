"""Execution of one LLM call: claim, limiter, breaker, provider, retry, fallback, result.

`execute_job` is what a worker runs for every received envelope and what the `direct` path runs
in-process (without the claim and the stored result). Provider errors come back as
`LLMReply(ok=False, error=...)` (§8, §9.2); a Valkey failure raises `InfraError` so the caller
releases the message instead of acking it, and a failed result write raises `QueueUnavailable`.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import orjson
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.envelope import CallMeta, Envelope, LLMReply
from taskiq_llm_queue.errors import ErrorInfo, ErrorKind, QueueUnavailable
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState
from taskiq_llm_queue.reliability import _lua
from taskiq_llm_queue.reliability.claims import RedisClaimStore
from taskiq_llm_queue.reliability.fallback import FallbackConfig, resolve_fallback
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter
from taskiq_llm_queue.results.backend import LLMQResultBackend
from taskiq_llm_queue.routing_hooks import QueueHooks, safe_hook

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.envelope import Op
    from taskiq_llm_queue.keys import KeySpace
    from taskiq_llm_queue.protocols import Lease, Limiter, Provider, ResultStore
    from taskiq_llm_queue.reliability.breaker import CircuitBreaker
    from taskiq_llm_queue.reliability.fallback import FallbackStep

logger = get_logger(__name__)

# DS13: a live (rt) call that can't get a slot within 2 s moves to the next fallback step.
RT_ACQUIRE_SECS = 2.0
# Caps one backoff sleep; the deadline caps the total (DS10).
MAX_BACKOFF_SECS = 20.0
# DS4/DS10: a queued bg/bulk call waits this long per step for a slot, then the message is
# released with a delay instead of holding a worker slot until its deadline.
QUEUED_ACQUIRE_SECS = 15.0
# Longest a result write is retried before the message is released (DS8).
STORE_RETRY_SECS = 30.0
_RESULT_RETRY_SECS = 0.5
# Pause between passes when every fallback step is skipped by an open breaker.
_IDLE_RETRY_SECS = 1.0
_MIN_REQUEUE_SECS = 10
_MAX_REQUEUE_SECS = 900
_CACHE_ENTRIES = 1024


class InfraError(QueueUnavailable):
    """Valkey failed while running a call; the message must be released, never acked.

    `attempted` is True when a provider attempt had already started, so a caller must not
    simply run the call again elsewhere (the cause is the `RedisError`).
    """

    def __init__(self, request_id: str, *, attempted: bool) -> None:
        """Keep the request id and whether a provider attempt had started."""
        super().__init__(f"Valkey failed while running {request_id}")
        self.request_id = request_id
        self.attempted = attempted


class _Requeue(Exception):
    """Internal: no capacity for a queued bg/bulk call; release the message with a delay."""

    def __init__(self, secs: int) -> None:
        super().__init__(secs)
        self.secs = secs


@dataclass(slots=True)
class _Progress:
    """Provider attempts started so far for one call (tells `InfraError` what happened)."""

    attempts: int = 0


class ReplyCache:
    """Bounded per-pod cache of replies whose result write failed (DS8 "cached response").

    A redelivery of the same request to this pod stores the cached reply instead of calling
    the provider again. Other pods cannot see it, so a redelivery there calls once more.
    """

    def __init__(self, max_entries: int = _CACHE_ENTRIES) -> None:
        """Hold at most `max_entries` replies."""
        self._max = max_entries
        self._items: OrderedDict[str, tuple[float, LLMReply]] = OrderedDict()

    def __len__(self) -> int:
        return len(self._items)

    def put(self, reply: LLMReply, *, expires_at: float) -> None:
        """Keep `reply` until `expires_at` (the call's deadline); evicts the oldest when full."""
        self._items[reply.request_id] = (expires_at, reply)
        self._items.move_to_end(reply.request_id)
        while len(self._items) > self._max:
            self._items.popitem(last=False)

    def pop(self, request_id: str, now: float) -> LLMReply | None:
        """Take the cached reply for `request_id` if it has not expired."""
        item = self._items.pop(request_id, None)
        if item is None or item[0] <= now:
            return None
        return item[1]


class _Stopped(Exception):
    """Internal: the call was cancelled (DS14) or ran past its deadline."""

    def __init__(self, state: ClaimState) -> None:
        super().__init__(state.value)
        self.state = state


@dataclass(slots=True)
class CoreDeps:
    """Everything `execute_job` needs, injected so tests can pass fakes and clocks.

    `providers(alias)` returns the provider for a model alias; `fallback()` returns the live
    fallback config without I/O. `claims`, `breaker`, `results` and `redis` may be None on the
    `direct` path or while Valkey is down. `leases` holds this pod's in-flight leases for the
    pod heartbeat to renew (DS11); every path on a pod shares one set.
    """

    config: QueueConfig
    keys: KeySpace
    providers: Callable[[str], Provider]
    limiter: Limiter
    claims: RedisClaimStore | None = None
    breaker: CircuitBreaker | None = None
    results: ResultStore | None = None
    redis: Redis | None = None
    fallback: Callable[[], FallbackConfig] = FallbackConfig
    worker: str = ""
    hooks: QueueHooks = field(default_factory=QueueHooks)
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    rng: random.Random = field(default_factory=random.Random)
    leases: set[Lease] = field(default_factory=set)
    acquire_wait_secs: float = QUEUED_ACQUIRE_SECS
    store_retry_secs: float = STORE_RETRY_SECS
    reply_cache: ReplyCache = field(default_factory=ReplyCache)


@dataclass(frozen=True, slots=True)
class JobResult:
    """Outcome of `execute_job`: the claim decision and the reply (None unless it ran).

    `decision` other than RUN means the worker must not call the provider: DEFER releases the
    message with the heartbeat window, the ACK_* decisions ack it. `requeue_secs` set means no
    capacity: release the message with that delay (DS4); the claim was already released.
    """

    decision: ClaimDecision
    reply: LLMReply | None = None
    requeue_secs: int | None = None


def error_reply(
    envelope: Envelope,
    code: str,
    message: str,
    *,
    retryable: bool,
    kind: str = "",
    retry_after_secs: float | None = None,
    meta: CallMeta | None = None,
    cancelled: bool = False,
) -> LLMReply:
    """An `ok=False` reply in the §9.2 shape (`type`, `message`, `retryable`, ...)."""
    return LLMReply(
        request_id=envelope.request_id,
        ok=False,
        cancelled=cancelled,
        meta=meta or CallMeta(),
        error={
            "type": code,
            "message": message,
            "retryable": retryable,
            "kind": kind,
            "retry_after_secs": retry_after_secs,
        },
    )


def backoff_secs(attempt: int, base_secs: float, rng: random.Random, hint: float | None) -> float:
    """Sleep before retry `attempt` (0-based): `base * 2^attempt` + full jitter, or `Retry-After`.

    A provider's `Retry-After` hint wins when larger; the result is capped at 20 s.
    """
    delay = base_secs * float(2**attempt) + rng.uniform(0, base_secs)
    if hint is not None:
        delay = max(delay, hint)
    return min(delay, MAX_BACKOFF_SECS)


def params_for_step(op: Op, params: Mapping[str, Any], model_id: str) -> dict[str, Any]:
    """Provider params with the step's model id set (`modelId` for Bedrock ops, else `model`).

    Each fallback step may switch model, so the id always comes from that step's `ModelConfig`.
    """
    key = "modelId" if op in ("converse", "invoke_model") else "model"
    return {**params, key: model_id}


async def load_params(envelope: Envelope, deps: CoreDeps) -> dict[str, Any] | None:
    """Inline params, or the JSON stored at `params_ref` (None if it expired or Redis is off)."""
    if envelope.params is not None:
        return envelope.params
    if deps.redis is None or envelope.params_ref is None:
        return None
    raw = await deps.redis.get(envelope.params_ref)
    if raw is None:
        return None
    value = orjson.loads(raw)
    return value if isinstance(value, dict) else None


async def execute_job(
    envelope: Envelope,
    deps: CoreDeps,
    *,
    claim: bool = True,
    store: bool = True,
    on_run: Callable[[], None] | None = None,
) -> JobResult:
    """Run one envelope end to end (§6.1, §8): claim → call with retry/fallback → result.

    With `claim`, the Lua claim decides first (DS1-DS3) and the record is completed (best
    effort) after the result is stored (DS8). `on_run` is called once the claim says RUN. A
    Valkey failure raises `InfraError`; a `CancelledError` propagates with nothing written.
    """
    claims = deps.claims if claim else None
    if claims is not None:
        try:
            decision = await claims.claim(
                envelope.request_id, deadline_at=envelope.deadline_at, worker=deps.worker
            )
        except RedisError as exc:
            raise InfraError(envelope.request_id, attempted=False) from exc
        if decision is not ClaimDecision.RUN:
            if decision is ClaimDecision.ACK_CANCELLED and store:
                reply = error_reply(envelope, "cancelled", "cancelled", retryable=False)
                await _store(reply.model_copy(update={"cancelled": True}), envelope, deps)
            return JobResult(decision)
    if on_run is not None:
        on_run()
    state = ClaimState.DONE
    progress = _Progress()
    cached = deps.reply_cache.pop(envelope.request_id, deps.clock()) if store else None
    try:
        if cached is not None:  # DS8: an earlier delivery ran it but could not store the reply
            logger.info("cached_reply_reused", request_id=envelope.request_id)
            reply = cached
        elif (params := await load_params(envelope, deps)) is None:
            reply = error_reply(envelope, "params_missing", "params_ref not found", retryable=False)
        else:
            reply = await run_call(
                envelope,
                params,
                deps,
                check_cancel=claims is not None,
                requeue=claims is not None and store,
                progress=progress,
                # the claim script just checked the cancel flag (DS14): skip one EXISTS
                fresh_claim=isinstance(claims, RedisClaimStore),
            )
    except _Stopped as stop:
        state = stop.state
        cancelled = stop.state is ClaimState.CANCELLED
        code = "cancelled" if cancelled else "deadline_exceeded"
        reply = error_reply(envelope, code, code, retryable=False, cancelled=cancelled)
    except _Requeue as requeue:
        if claims is not None:
            await _quietly(
                claims.release(envelope.request_id, worker=deps.worker),
                "claim_release_failed",
                envelope,
            )
        return JobResult(ClaimDecision.RUN, requeue_secs=requeue.secs)
    except asyncio.CancelledError:
        raise
    except RedisError as exc:  # infrastructure, not the call: release for redelivery
        raise InfraError(envelope.request_id, attempted=progress.attempts > 0) from exc
    except Exception as exc:  # a bug must still release the waiter with a value
        logger.exception(
            "call_failed_unexpectedly",
            request_id=envelope.request_id,
            model=envelope.model,
            lane=envelope.lane.value,
        )
        reply = error_reply(envelope, "internal_error", type(exc).__name__, retryable=False)
    if store:
        try:
            await _finish(reply, envelope, deps, claims, state)
        except QueueUnavailable:
            deps.reply_cache.put(reply, expires_at=envelope.deadline_at)
            raise
    elif claims is not None:
        await _quietly(
            claims.complete(envelope.request_id, state, worker=deps.worker),
            "claim_complete_failed",
            envelope,
        )
    return JobResult(ClaimDecision.RUN, reply)


async def _finish(
    reply: LLMReply,
    envelope: Envelope,
    deps: CoreDeps,
    claims: RedisClaimStore | None,
    state: ClaimState,
) -> None:
    """DS8 tail: store the result, then complete the claim, then wake-up and params cleanup.

    One pipelined round trip when our stores share one non-cluster client (4 before); else
    the same steps one by one. Raises `QueueUnavailable` when the result cannot be stored.
    """
    results, redis = deps.results, deps.redis
    if (
        redis is not None
        and not isinstance(redis, RedisCluster)  # cluster nodes would run out of order
        and isinstance(results, LLMQResultBackend)
        and results.uses(redis)
        and (claims is None or (isinstance(claims, RedisClaimStore) and claims.client is redis))
        and await _finish_pipelined(reply, envelope, deps, results, claims, state)
    ):
        return
    await _store(reply, envelope, deps)
    if claims is not None:  # the reply is out; a failed CAS only lets a duplicate re-ack
        await _quietly(
            claims.complete(envelope.request_id, state, worker=deps.worker),
            "claim_complete_failed",
            envelope,
        )
    if deps.redis is not None:
        await _after_store(envelope, deps)


async def _finish_pipelined(
    reply: LLMReply,
    envelope: Envelope,
    deps: CoreDeps,
    results: LLMQResultBackend,
    claims: RedisClaimStore | None,
    state: ClaimState,
) -> bool:
    """SET result, claim CAS, PUBLISH, DEL params in one non-transactional pipeline (DS8).

    Valkey runs one connection's commands in order, so `done` is never written before the
    result, and a write it refuses (OOM, READONLY) is refused to the claim script too. A failed
    round trip is retried whole (each step is idempotent) until `_store`'s give-up time.
    Returns False when the `SET` alone was refused; the caller then goes step by step.
    """
    assert deps.redis is not None
    rid = envelope.request_id
    complete = claims.complete_call(rid, state, worker=deps.worker) if claims else None
    give_up_at = min(envelope.deadline_at, deps.clock() + deps.store_retry_secs)
    while True:
        try:
            async with deps.redis.pipeline(transaction=False) as pipe:
                results.queue_put(pipe, reply, ttl_secs=deps.config.result_ttl_secs)
                if complete is not None:
                    _lua.queue_script(pipe, *complete)
                if envelope.reply_to:
                    pipe.publish(deps.keys.done_channel(envelope.reply_to), rid)
                if envelope.params_ref:
                    pipe.delete(envelope.params_ref)
                replies = await pipe.execute(raise_on_error=False)
            break
        except Exception as exc:
            if deps.clock() + _RESULT_RETRY_SECS >= give_up_at:
                raise QueueUnavailable(f"cannot store result {rid}") from exc
            logger.warning("result_write_failed", request_id=rid, error_type=type(exc).__name__)
            await deps.sleep(_RESULT_RETRY_SECS)
    if isinstance(replies[0], Exception):
        logger.warning("result_write_failed", request_id=rid, error_type=type(replies[0]).__name__)
        return False
    if claims is not None and complete is not None:
        try:
            outcome = await _lua.settle(replies[1], *complete)
            claims.complete_outcome(outcome, rid, worker=deps.worker)
        except Exception as exc:  # the reply is out; a failed CAS only lets a duplicate re-ack
            logger.warning("claim_complete_failed", request_id=rid, error_type=type(exc).__name__)
    rest = replies[2:] if complete is not None else replies[1:]
    if failed := next((r for r in rest if isinstance(r, Exception)), None):
        # a lost wake-up only delays the result until the next poll
        logger.warning("result_cleanup_failed", request_id=rid, error_type=type(failed).__name__)
    return True


async def _quietly(call: Awaitable[Any], event: str, envelope: Envelope) -> None:
    """Await a best-effort claim update; a Valkey error is logged, never raised."""
    try:
        await call
    except Exception as exc:
        logger.warning(event, request_id=envelope.request_id, error_type=type(exc).__name__)


async def _store(reply: LLMReply, envelope: Envelope, deps: CoreDeps) -> None:
    """Write the reply, retrying until the deadline or `store_retry_secs` (DS8).

    Then raises `QueueUnavailable`; `execute_job` keeps the reply in the pod's `ReplyCache`.
    """
    if deps.results is None:
        return
    ttl = deps.config.result_ttl_secs
    give_up_at = min(envelope.deadline_at, deps.clock() + deps.store_retry_secs)
    while True:
        try:
            await deps.results.put(reply, ttl_secs=ttl)
            return
        except Exception as exc:
            if deps.clock() + _RESULT_RETRY_SECS >= give_up_at:
                raise QueueUnavailable(f"cannot store result {envelope.request_id}") from exc
            logger.warning(
                "result_write_failed",
                request_id=envelope.request_id,
                error_type=type(exc).__name__,
            )
            await deps.sleep(_RESULT_RETRY_SECS)


async def _after_store(envelope: Envelope, deps: CoreDeps) -> None:
    """Best effort: wake the sender's dispatcher and drop large params stored by reference."""
    assert deps.redis is not None
    try:
        if envelope.reply_to:
            await deps.redis.publish(deps.keys.done_channel(envelope.reply_to), envelope.request_id)
        if envelope.params_ref:
            await deps.redis.delete(envelope.params_ref)
    except Exception as exc:  # a lost wake-up only delays the result until the next poll
        logger.warning(
            "result_cleanup_failed",
            request_id=envelope.request_id,
            error_type=type(exc).__name__,
        )


async def _check_live(envelope: Envelope, deps: CoreDeps, check_cancel: bool) -> None:
    """Raise `_Stopped` when the call is cancelled (DS14) or past its deadline."""
    if deps.clock() >= envelope.deadline_at:
        raise _Stopped(ClaimState.EXPIRED)
    if (
        check_cancel
        and deps.claims is not None
        and await deps.claims.is_cancelled(envelope.request_id)
    ):
        raise _Stopped(ClaimState.CANCELLED)


async def _step_usable(step: FallbackStep, lane: Lane, deps: CoreDeps) -> bool:
    """Breaker allows the target and, for `require_headroom` steps, the limiter has room."""
    try:
        if step.require_headroom and isinstance(deps.limiter, AdaptiveLimiter):
            state = await deps.limiter.state(step.alias, step.target)
            if state.headroom(deps.limiter.settings.share_for(lane)) <= 0:
                return False
        if deps.breaker is not None:
            return await deps.breaker.allow(step.alias, step.target)
    except Exception as exc:  # Valkey blip: try the step rather than skip everything
        logger.warning(
            "step_check_failed",
            model=step.alias,
            target=step.target,
            lane=lane.value,
            error_type=type(exc).__name__,
        )
    return True


def _classify(provider: Provider, exc: BaseException) -> ErrorInfo:
    """Provider classification; our own per-attempt timeout counts as retryable."""
    if isinstance(exc, TimeoutError):
        return ErrorInfo(ErrorKind.RETRYABLE, "timeout")
    try:
        return provider.classify(exc)
    except Exception:  # a broken classifier must not turn into an unhandled error
        return ErrorInfo(ErrorKind.RETRYABLE, type(exc).__name__)


def _attempt_message(exc: BaseException, info: ErrorInfo) -> str:
    """`ExcType: code` for an error reply, or just the code when it is the type's name."""
    code = info.code or info.kind.value
    name = type(exc).__name__
    return code if code == name else f"{name}: {code}"


def _acquire_wait(envelope: Envelope, deps: CoreDeps, *, bounded: bool) -> float:
    """Slot wait for one attempt: rt 2 s (DS13); queued bg/bulk bounded (DS4); else the deadline."""
    remaining = envelope.deadline_at - deps.clock()
    if envelope.lane is Lane.RT:
        return min(RT_ACQUIRE_SECS, remaining)
    if bounded and remaining > 2 * deps.acquire_wait_secs:
        return deps.acquire_wait_secs
    return remaining


def _requeue_secs(envelope: Envelope, deps: CoreDeps) -> int | None:
    """Delay before a queued bg/bulk call with no capacity is redelivered; None near deadline.

    A quarter of the remaining time (10-900 s) keeps receives per message low (DS5).
    """
    remaining = envelope.deadline_at - deps.clock()
    if envelope.lane is Lane.RT or remaining <= 2 * deps.acquire_wait_secs:
        return None
    return int(min(_MAX_REQUEUE_SECS, max(_MIN_REQUEUE_SECS, remaining / 4)))


async def _take_slot(
    envelope: Envelope,
    step: FallbackStep,
    deps: CoreDeps,
    *,
    check_cancel: bool,
    bounded: bool,
    fresh_claim: bool = False,
) -> Lease | None:
    """Acquire one slot; cancel and deadline are checked before and right after (DS14).

    `fresh_claim`: the claim script checked the cancel flag just now, so skip the first check.
    """
    await _check_live(envelope, deps, check_cancel and not fresh_claim)
    wait = _acquire_wait(envelope, deps, bounded=bounded)
    abort = _cancel_probe(envelope, deps) if check_cancel else None
    lease = await deps.limiter.acquire(
        step.alias, step.target, envelope.lane, wait, should_abort=abort
    )
    if lease is None:
        return None
    try:
        await _check_live(envelope, deps, check_cancel)
    except BaseException:
        await _release(deps, lease, throttled=False)
        raise
    return lease


def _cancel_probe(envelope: Envelope, deps: CoreDeps) -> Callable[[], Awaitable[bool]] | None:
    """`should_abort` for a slot wait: the cancel flag (DS14); a Valkey blip reads as no."""
    claims = deps.claims
    if claims is None:
        return None

    async def cancelled() -> bool:
        try:
            return await claims.is_cancelled(envelope.request_id)
        except RedisError:
            return False

    return cancelled


async def run_call(
    envelope: Envelope,
    params: Mapping[str, Any],
    deps: CoreDeps,
    *,
    check_cancel: bool = False,
    requeue: bool = False,
    progress: _Progress | None = None,
    fresh_claim: bool = False,
) -> LLMReply:
    """Walk the fallback steps; each step gets `retry_attempts` attempts with backoff (§8).

    A limiter slot covers one attempt and is released in `finally` before any sleep (DS10).
    When no step could run: `requeue` raises `_Requeue` (bg/bulk), an all-open breaker waits
    and tries again within the deadline. Raises `_Stopped`; a Valkey error propagates.
    `fresh_claim` skips the cancel check before the first slot (the claim just made it).
    """
    config = deps.config
    steps = resolve_fallback(
        config, envelope.model, policy=envelope.fallback, op=envelope.op, live=deps.fallback()
    )
    started = deps.clock()
    queue_wait_ms = max(0, round((started - envelope.enqueued_at) * 1000))
    safe_hook(deps.hooks.on_queue_wait_ms, envelope.model, envelope.lane.value, queue_wait_ms)
    progress = progress or _Progress()
    last = ErrorInfo(ErrorKind.RETRYABLE, "no_capacity")
    last_message = "no fallback step had capacity"
    while True:
        attempts_before = progress.attempts
        circuit_only = True
        for index, step in enumerate(steps):
            if not await _step_usable(step, envelope.lane, deps):
                last, last_message = ErrorInfo(ErrorKind.RETRYABLE, "circuit_open"), "circuit open"
                continue
            circuit_only = False
            _, model_cfg, _ = config.resolve(f"{step.alias}@{step.target}")
            target = model_cfg.targets[step.target]
            provider = deps.providers(step.alias)
            call_params = params_for_step(envelope.op, params, model_cfg.model_id)
            for attempt in range(config.retry_attempts):
                lease = await _take_slot(
                    envelope,
                    step,
                    deps,
                    check_cancel=check_cancel,
                    bounded=requeue,
                    fresh_claim=fresh_claim,
                )
                fresh_claim = False
                if lease is None:
                    last, last_message = (
                        ErrorInfo(ErrorKind.RETRYABLE, "no_capacity"),
                        "no capacity",
                    )
                    break  # DS13: next step
                progress.attempts += 1
                deps.leases.add(lease)
                info: ErrorInfo | None = None
                # Deadline cap: no extra task, unlike wait_for, and `expired()` tells it apart
                # from a provider's own timeout.
                cap = asyncio.timeout(max(0.1, envelope.deadline_at - deps.clock()))
                t0 = time.monotonic()
                try:
                    async with cap:
                        response = await provider.call(envelope.op, call_params, target)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    info = _classify(provider, exc)
                    last, last_message = info, _attempt_message(exc, info)
                finally:
                    deps.leases.discard(lease)
                    await _release(
                        deps, lease, throttled=info is not None and info.kind is ErrorKind.THROTTLE
                    )
                secs = time.monotonic() - t0
                kind = info.kind if info else "ok"
                safe_hook(deps.hooks.on_attempt, step.alias, step.target, kind, secs)
                if info is None:
                    await _record(deps, step, ok=True)
                    if index:
                        safe_hook(
                            deps.hooks.on_fallback, envelope.model, f"{step.alias}@{step.target}"
                        )
                    meta = CallMeta(
                        served_model=step.alias,
                        served_target=step.target,
                        fallback_used=index > 0,
                        queue_wait_ms=queue_wait_ms,
                        provider_ms=round(secs * 1000),
                        attempts=progress.attempts,
                        worker=deps.worker,
                    )
                    return LLMReply(
                        request_id=envelope.request_id, ok=True, response=response, meta=meta
                    )
                if info.kind is ErrorKind.FATAL:
                    return error_reply(
                        envelope,
                        info.code or "fatal",
                        last_message,
                        retryable=False,
                        kind=info.kind.value,
                        meta=CallMeta(
                            served_model=step.alias,
                            served_target=step.target,
                            attempts=progress.attempts,
                            worker=deps.worker,
                        ),
                    )
                if info.kind is ErrorKind.RETRYABLE:
                    await _record(deps, step, ok=False)
                else:  # a throttle belongs to the limiter: frees a half-open probe, no count
                    await _record(deps, step, ok=None)
                if cap.expired():  # the call's deadline cut it off: expired, not a provider error
                    raise _Stopped(ClaimState.EXPIRED)
                if attempt + 1 < config.retry_attempts:
                    delay = backoff_secs(
                        attempt, config.retry_base_secs, deps.rng, info.retry_after_secs
                    )
                    if deps.clock() + delay >= envelope.deadline_at:
                        break
                    await _check_live(envelope, deps, check_cancel)
                    await deps.sleep(delay)
        if progress.attempts == attempts_before:  # no step could run in this pass
            if requeue and (secs_to_requeue := _requeue_secs(envelope, deps)) is not None:
                raise _Requeue(secs_to_requeue)
            if circuit_only and deps.clock() + _IDLE_RETRY_SECS < envelope.deadline_at:
                await _check_live(envelope, deps, check_cancel)
                await deps.sleep(_IDLE_RETRY_SECS)
                continue
        return error_reply(
            envelope,
            last.code or last.kind.value,
            last_message,
            retryable=True,
            kind=last.kind.value,
            retry_after_secs=last.retry_after_secs,
            meta=CallMeta(attempts=progress.attempts, worker=deps.worker),
        )


async def _release(deps: CoreDeps, lease: Lease, *, throttled: bool) -> None:
    """Return a slot; a Valkey error is logged (the lease expires on its own, DS11)."""
    try:
        await deps.limiter.release(lease, throttled=throttled)
    except Exception as exc:
        logger.warning(
            "limiter_release_failed",
            model=lease.model,
            target=lease.target,
            error_type=type(exc).__name__,
        )


async def _record(deps: CoreDeps, step: FallbackStep, *, ok: bool | None) -> None:
    """Feed one outcome into the breaker (best effort); `ok=None` is a neutral throttle.

    Only 5xx/timeouts count as failures: throttles are the limiter's job (AIMD, pause).
    """
    if deps.breaker is None:
        return
    try:
        if ok is None:
            await deps.breaker.record_neutral(step.alias, step.target)
        else:
            await deps.breaker.record(step.alias, step.target, ok=ok)
    except Exception as exc:
        logger.warning(
            "breaker_record_failed",
            model=step.alias,
            target=step.target,
            error_type=type(exc).__name__,
        )
