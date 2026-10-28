"""Execution of one LLM call: claim, limiter, breaker, provider, retry, fallback, result.

`execute_job` is what a worker runs for every received envelope and what the `direct` path runs
in-process (without the claim and the stored result). Errors never raise out of it: they come
back as `LLMReply(ok=False, error=...)`, so taskiq never logs them as task failures (§8, §9.2).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.envelope import CallMeta, Envelope, LLMReply
from taskiq_llm_queue.errors import ErrorInfo, ErrorKind, QueueUnavailable
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState
from taskiq_llm_queue.reliability.fallback import FallbackConfig, resolve_fallback
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter
from taskiq_llm_queue.routing_hooks import QueueHooks, safe_hook

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.envelope import Op
    from taskiq_llm_queue.keys import KeySpace
    from taskiq_llm_queue.protocols import Lease, Limiter, Provider, ResultStore
    from taskiq_llm_queue.reliability.breaker import CircuitBreaker
    from taskiq_llm_queue.reliability.claims import RedisClaimStore
    from taskiq_llm_queue.reliability.fallback import FallbackStep

logger = logging.getLogger(__name__)

# DS13: a live (rt) call that can't get a slot within 2 s moves to the next fallback step.
RT_ACQUIRE_SECS = 2.0
# Caps one backoff sleep; the deadline caps the total (DS10).
MAX_BACKOFF_SECS = 20.0
_RESULT_RETRY_SECS = 0.5


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
    heartbeat to renew (DS11).
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


@dataclass(frozen=True, slots=True)
class JobResult:
    """Outcome of `execute_job`: the claim decision and the reply (None unless it ran).

    `decision` other than RUN means the worker must not call the provider: DEFER releases the
    message with the heartbeat window, the ACK_* decisions ack it.
    """

    decision: ClaimDecision
    reply: LLMReply | None = None


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
    value = json.loads(raw)
    return value if isinstance(value, dict) else None


async def execute_job(
    envelope: Envelope, deps: CoreDeps, *, claim: bool = True, store: bool = True
) -> JobResult:
    """Run one envelope end to end (§6.1, §8): claim → call with retry/fallback → result.

    With `claim`, the Lua claim decides first (DS1-DS3) and the record is completed after the
    result is stored (DS8: result → `done` → ack by taskiq). With `store`, the reply is written
    to the result store and the sender's pod is woken. A `CancelledError` (drain, lost message)
    propagates with nothing written; the caller releases the claim and the message.
    """
    claims = deps.claims if claim else None
    if claims is not None:
        decision = await claims.claim(
            envelope.request_id, deadline_at=envelope.deadline_at, worker=deps.worker
        )
        if decision is not ClaimDecision.RUN:
            if decision is ClaimDecision.ACK_CANCELLED and store:
                reply = error_reply(envelope, "cancelled", "cancelled", retryable=False)
                await _store(reply.model_copy(update={"cancelled": True}), envelope, deps)
            return JobResult(decision)
    state = ClaimState.DONE
    try:
        params = await load_params(envelope, deps)
        if params is None:
            reply = error_reply(envelope, "params_missing", "params_ref not found", retryable=False)
        else:
            reply = await run_call(envelope, params, deps, check_cancel=claims is not None)
    except _Stopped as stop:
        state = stop.state
        cancelled = stop.state is ClaimState.CANCELLED
        code = "cancelled" if cancelled else "deadline_exceeded"
        reply = error_reply(envelope, code, code, retryable=False, cancelled=cancelled)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # a bug must still release the waiter with a value
        logger.exception("llmq call %s failed unexpectedly", envelope.request_id)
        reply = error_reply(envelope, "internal_error", type(exc).__name__, retryable=False)
    if store:
        await _store(reply, envelope, deps)
    if claims is not None:
        await claims.complete(envelope.request_id, state)
    if store and deps.redis is not None:
        await _after_store(envelope, deps)
    return JobResult(ClaimDecision.RUN, reply)


async def _store(reply: LLMReply, envelope: Envelope, deps: CoreDeps) -> None:
    """Write the reply, retrying until the deadline (DS8); then `QueueUnavailable`."""
    if deps.results is None:
        return
    ttl = deps.config.result_ttl_secs
    while True:
        try:
            await deps.results.put(reply, ttl_secs=ttl)
            return
        except Exception as exc:
            if deps.clock() + _RESULT_RETRY_SECS >= envelope.deadline_at:
                raise QueueUnavailable(f"cannot store result {envelope.request_id}") from exc
            logger.warning("result write failed (%s); retrying", type(exc).__name__)
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
        logger.warning("post-result cleanup failed: %s", type(exc).__name__)


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
        logger.warning("breaker/headroom check failed: %s", type(exc).__name__)
    return True


def _classify(provider: Provider, exc: BaseException) -> ErrorInfo:
    """Provider classification; our own per-attempt timeout counts as retryable."""
    if isinstance(exc, TimeoutError):
        return ErrorInfo(ErrorKind.RETRYABLE, "timeout")
    try:
        return provider.classify(exc)
    except Exception:  # a broken classifier must not turn into an unhandled error
        return ErrorInfo(ErrorKind.RETRYABLE, type(exc).__name__)


async def run_call(
    envelope: Envelope,
    params: Mapping[str, Any],
    deps: CoreDeps,
    *,
    check_cancel: bool = False,
) -> LLMReply:
    """Walk the fallback steps; each step gets `retry_attempts` attempts with backoff (§8).

    A limiter slot covers one attempt and is released in `finally` before any sleep (DS10).
    Fatal errors stop at once; exhausted steps give a retryable error reply. Raises `_Stopped`.
    """
    config = deps.config
    steps = resolve_fallback(
        config, envelope.model, policy=envelope.fallback, op=envelope.op, live=deps.fallback()
    )
    started = deps.clock()
    queue_wait_ms = max(0, round((started - envelope.enqueued_at) * 1000))
    safe_hook(deps.hooks.on_queue_wait_ms, envelope.model, envelope.lane.value, queue_wait_ms)
    attempts = 0
    last = ErrorInfo(ErrorKind.RETRYABLE, "no_capacity")
    last_message = "no fallback step had capacity"
    for index, step in enumerate(steps):
        if not await _step_usable(step, envelope.lane, deps):
            last, last_message = ErrorInfo(ErrorKind.RETRYABLE, "circuit_open"), "circuit open"
            continue
        _, model_cfg, _ = config.resolve(f"{step.alias}@{step.target}")
        target = model_cfg.targets[step.target]
        provider = deps.providers(step.alias)
        call_params = params_for_step(envelope.op, params, model_cfg.model_id)
        for attempt in range(config.retry_attempts):
            await _check_live(envelope, deps, check_cancel)
            remaining = envelope.deadline_at - deps.clock()
            wait = min(RT_ACQUIRE_SECS, remaining) if envelope.lane is Lane.RT else remaining
            lease = await deps.limiter.acquire(step.alias, step.target, envelope.lane, wait)
            if lease is None:
                last, last_message = ErrorInfo(ErrorKind.RETRYABLE, "no_capacity"), "no capacity"
                break  # DS13: next step
            attempts += 1
            deps.leases.add(lease)
            info: ErrorInfo | None = None
            t0 = time.monotonic()
            try:
                timeout = max(0.1, envelope.deadline_at - deps.clock())
                response = await asyncio.wait_for(
                    provider.call(envelope.op, call_params, target), timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                info = _classify(provider, exc)
                last, last_message = info, f"{type(exc).__name__}: {info.code or info.kind}"
            finally:
                deps.leases.discard(lease)
                await _release(
                    deps, lease, throttled=info is not None and info.kind is ErrorKind.THROTTLE
                )
            secs = time.monotonic() - t0
            key = f"{step.alias}@{step.target}"
            safe_hook(
                deps.hooks.on_attempt, step.alias, step.target, info.kind if info else "ok", secs
            )
            if info is None:
                await _record(deps, step, ok=True)
                if index:
                    safe_hook(deps.hooks.on_fallback, envelope.model, key)
                meta = CallMeta(
                    served_model=step.alias,
                    served_target=step.target,
                    fallback_used=index > 0,
                    queue_wait_ms=queue_wait_ms,
                    provider_ms=round(secs * 1000),
                    attempts=attempts,
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
                        attempts=attempts,
                        worker=deps.worker,
                    ),
                )
            await _record(deps, step, ok=False)
            if attempt + 1 < config.retry_attempts:
                delay = backoff_secs(
                    attempt, config.retry_base_secs, deps.rng, info.retry_after_secs
                )
                if deps.clock() + delay >= envelope.deadline_at:
                    break
                await _check_live(envelope, deps, check_cancel)
                await deps.sleep(delay)
    return error_reply(
        envelope,
        last.code or last.kind.value,
        last_message,
        retryable=True,
        kind=last.kind.value,
        retry_after_secs=last.retry_after_secs,
        meta=CallMeta(attempts=attempts, worker=deps.worker),
    )


async def _release(deps: CoreDeps, lease: Lease, *, throttled: bool) -> None:
    """Return a slot; a Valkey error is logged (the lease expires on its own, DS11)."""
    try:
        await deps.limiter.release(lease, throttled=throttled)
    except Exception as exc:
        logger.warning("limiter release failed: %s", type(exc).__name__)


async def _record(deps: CoreDeps, step: FallbackStep, *, ok: bool) -> None:
    """Feed one outcome into the breaker (best effort)."""
    if deps.breaker is None:
        return
    try:
        await deps.breaker.record(step.alias, step.target, ok=ok)
    except Exception as exc:
        logger.warning("breaker record failed: %s", type(exc).__name__)
