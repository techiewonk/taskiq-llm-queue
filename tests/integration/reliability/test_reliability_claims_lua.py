"""Claim CAS on real Valkey: every state transition, duplicates, stale reclaim, TTL rule."""

from __future__ import annotations

import asyncio
import time

import pytest
import redis.asyncio

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import ClaimDecision, ClaimState, ClaimStore
from taskiq_llm_queue.reliability import ClaimOutcome, ClaimSettings, RedisClaimStore

pytestmark = pytest.mark.integration

RID = "req-1"


def make(valkey: redis.asyncio.Redis, keys: KeySpace, **kw: float) -> RedisClaimStore:
    settings = ClaimSettings(
        state_ttl_secs=int(kw.get("state_ttl_secs", 60)),
        exec_max_secs=int(kw.get("exec_max_secs", 30)),
        heartbeat_interval_secs=kw.get("heartbeat_interval_secs", 5.0),
    )
    return RedisClaimStore(valkey, keys, settings)


def later(secs: float = 100.0) -> float:
    return time.time() + secs


async def server_now(valkey: redis.asyncio.Redis) -> float:
    """Valkey's clock: the Lua decides with `TIME`, and the container clock may skew."""
    secs, micros = await valkey.time()
    return secs + micros / 1e6


async def test_implements_protocol(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    assert isinstance(make(valkey, keys), ClaimStore)


async def test_missing_key_runs_and_sets_ttl(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """DS3: TTL = max(state_ttl, deadline - now + exec_max)."""
    store = make(valkey, keys)
    deadline = await server_now(valkey) + 1000
    assert await store.claim(RID, deadline_at=deadline, worker="a") is ClaimDecision.RUN
    record = await valkey.hgetall(keys.job(RID))
    assert record[b"state"] == b"running"
    assert record[b"worker"] == b"a"
    assert 1020 <= await valkey.ttl(keys.job(RID)) <= 1030
    assert await store.claim("short", deadline_at=later(5), worker="a") is ClaimDecision.RUN
    assert 55 <= await valkey.ttl(keys.job("short")) <= 60


async def test_missing_key_past_deadline_is_expired(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    store = make(valkey, keys)
    decision = await store.claim(RID, deadline_at=await server_now(valkey) - 1, worker="a")
    assert decision is ClaimDecision.ACK_EXPIRED
    assert await store.state(RID) is ClaimState.EXPIRED


async def test_duplicate_delivery_defers_and_keeps_record(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """DS2: running + fresh heartbeat -> DEFER (never delete), for other pods and the owner."""
    store = make(valkey, keys)
    assert await store.claim(RID, deadline_at=later(), worker="a") is ClaimDecision.RUN
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.DEFER
    assert await store.claim(RID, deadline_at=later(), worker="a") is ClaimDecision.DEFER
    assert await store.state(RID) is ClaimState.RUNNING
    assert await valkey.hget(keys.job(RID), "worker") == b"a"


@pytest.mark.parametrize(
    ("state", "decision"),
    [
        (ClaimState.DONE, ClaimDecision.ACK_DONE),
        (ClaimState.CANCELLED, ClaimDecision.ACK_CANCELLED),
        (ClaimState.EXPIRED, ClaimDecision.ACK_EXPIRED),
    ],
)
async def test_terminal_states_ack(
    valkey: redis.asyncio.Redis, keys: KeySpace, state: ClaimState, decision: ClaimDecision
) -> None:
    store = make(valkey, keys)
    await store.claim(RID, deadline_at=later(), worker="a")
    await store.complete(RID, state)
    assert await store.claim(RID, deadline_at=later(), worker="b") is decision
    assert await valkey.ttl(keys.job(RID)) >= 55


async def test_complete_rejects_non_terminal(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    with pytest.raises(ValueError, match="terminal"):
        await make(valkey, keys).complete(RID, ClaimState.RUNNING)


async def test_queued_and_released_run(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys)
    await valkey.hset(keys.job(RID), "state", "queued")
    assert await store.claim(RID, deadline_at=later(), worker="a") is ClaimDecision.RUN
    await store.release(RID)
    assert await store.state(RID) is ClaimState.RELEASED
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.RUN
    assert await valkey.hget(keys.job(RID), "worker") == b"b"


async def test_release_only_touches_running(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys)
    await store.claim(RID, deadline_at=later(), worker="a")
    await store.complete(RID, ClaimState.DONE)
    await store.release(RID)
    assert await store.state(RID) is ClaimState.DONE


async def test_released_past_deadline_expires(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys)
    await valkey.hset(keys.job(RID), "state", "released")
    decision = await store.claim(RID, deadline_at=await server_now(valkey) - 1, worker="a")
    assert decision is ClaimDecision.ACK_EXPIRED


async def test_cancel_flag_without_owner(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """DS14: a cancelled task is acked and recorded as cancelled."""
    store = make(valkey, keys)
    await store.cancel(RID)
    assert await store.is_cancelled(RID)
    assert await store.claim(RID, deadline_at=later(), worker="a") is ClaimDecision.ACK_CANCELLED
    assert await store.state(RID) is ClaimState.CANCELLED


async def test_cancel_flag_with_live_owner_keeps_running(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """The live owner writes the cancelled result itself, so its record stays running."""
    store = make(valkey, keys)
    await store.claim(RID, deadline_at=later(), worker="a")
    await store.cancel(RID)
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.ACK_CANCELLED
    assert await store.state(RID) is ClaimState.RUNNING


async def test_stale_heartbeat_is_reclaimed(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """A heartbeat older than 3 x interval means the owner died: another pod runs it."""
    store = make(valkey, keys, heartbeat_interval_secs=0.1)
    await store.claim(RID, deadline_at=later(), worker="a")
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.DEFER
    await asyncio.sleep(0.45)
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.RUN
    record = await valkey.hgetall(keys.job(RID))
    assert record[b"worker"] == b"b"
    assert record[b"reclaims"] == b"1"


async def test_stale_past_deadline_expires(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys, heartbeat_interval_secs=0.1)
    deadline = await server_now(valkey) + 0.2
    assert await store.claim(RID, deadline_at=deadline, worker="a") is ClaimDecision.RUN
    await asyncio.sleep(0.45)
    decision = await store.claim(RID, deadline_at=deadline, worker="b")
    assert decision is ClaimDecision.ACK_EXPIRED


async def test_heartbeat_keeps_claim_fresh(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys, heartbeat_interval_secs=0.15)
    deadline = await server_now(valkey) + 1000
    await store.claim(RID, deadline_at=deadline, worker="a")
    await valkey.expire(keys.job(RID), 10)
    for _ in range(3):
        await asyncio.sleep(0.15)
        await store.heartbeat([RID], worker="a")
    assert await store.claim(RID, deadline_at=deadline, worker="b") is ClaimDecision.DEFER
    assert await valkey.ttl(keys.job(RID)) >= 1020, "heartbeat refreshes the DS3 TTL"


async def test_heartbeat_reports_lost_claims(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = make(valkey, keys, heartbeat_interval_secs=0.1)
    await store.claim("mine", deadline_at=later(), worker="a")
    await store.claim("theirs", deadline_at=later(), worker="b")
    lost = await store.heartbeat_owned(["mine", "theirs", "gone"], worker="a")
    assert lost == ["theirs", "gone"]
    assert await store.heartbeat_owned([], worker="a") == []


async def test_failures_counter(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """DS5: real failures are counted on the claim; the third one exhausts the task."""
    store = make(valkey, keys)
    assert (await store.record_failure(RID)).count == 1
    assert not (await store.record_failure(RID)).exhausted
    third = await store.record_failure(RID)
    assert third.count == 3
    assert third.exhausted
    assert 0 < await valkey.ttl(keys.job(RID)) <= 60


async def test_concurrent_claims_pick_one_runner(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    store = make(valkey, keys)
    decisions = await asyncio.gather(
        *(store.claim(RID, deadline_at=later(), worker=f"w{i}") for i in range(20))
    )
    assert decisions.count(ClaimDecision.RUN) == 1
    assert decisions.count(ClaimDecision.DEFER) == 19


async def take_over(valkey: redis.asyncio.Redis, keys: KeySpace) -> RedisClaimStore:
    """`a` runs the task, misses its heartbeats, and `b` reclaims it."""
    store = make(valkey, keys, heartbeat_interval_secs=0.1)
    assert await store.claim(RID, deadline_at=later(), worker="a") is ClaimDecision.RUN
    await asyncio.sleep(0.45)
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.RUN
    return store


async def test_stale_owner_cannot_complete_new_owners_claim(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Finding 12: CLAIM_COMPLETE ignored `worker`, so a stale owner marked b's run done."""
    store = await take_over(valkey, keys)
    outcome = await store.complete_owned(RID, ClaimState.DONE, worker="a")
    assert outcome is ClaimOutcome.NOT_OWNER
    await store.complete(RID, ClaimState.CANCELLED, worker="a")  # protocol form: no-op too
    assert await store.state(RID) is ClaimState.RUNNING
    assert await valkey.hget(keys.job(RID), "worker") == b"b"
    assert await store.complete_owned(RID, ClaimState.DONE, worker="b") is ClaimOutcome.APPLIED
    assert await store.state(RID) is ClaimState.DONE


async def test_stale_owner_cannot_release_new_owners_claim(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Finding 12: CLAIM_RELEASE ignored `worker`, so a stale owner released b's running claim."""
    store = await take_over(valkey, keys)
    assert await store.release_owned(RID, worker="a") is ClaimOutcome.NOT_OWNER
    await store.release(RID, worker="a")
    assert await store.state(RID) is ClaimState.RUNNING
    assert await valkey.hget(keys.job(RID), "worker") == b"b"
    assert await store.release_owned(RID, worker="b") is ClaimOutcome.APPLIED
    assert await store.state(RID) is ClaimState.RELEASED
    assert await store.release_owned(RID, worker="b") is ClaimOutcome.NOT_RUNNING


async def test_complete_after_own_release_is_allowed(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """A record with no owner (released) can still be completed by the worker that ran it."""
    store = make(valkey, keys)
    await store.claim(RID, deadline_at=later(), worker="a")
    await store.release(RID, worker="a")
    assert await store.complete_owned(RID, ClaimState.DONE, worker="a") is ClaimOutcome.APPLIED
    assert await store.state(RID) is ClaimState.DONE


async def test_one_missed_heartbeat_is_not_stale(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Finding 12: staleness was 2 x interval, so one missed beat let another pod take over."""
    store = make(valkey, keys, heartbeat_interval_secs=0.1)
    await store.claim(RID, deadline_at=later(), worker="a")
    await asyncio.sleep(0.22)  # one beat missed, the next one late
    assert await store.claim(RID, deadline_at=later(), worker="b") is ClaimDecision.DEFER
