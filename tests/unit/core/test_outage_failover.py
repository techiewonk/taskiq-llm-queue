"""Sender-side failover during SQS and Valkey outages (scenarios S20 and S21).

`LLMQueue` runs on a hand-built fake `_Started` state: a broker whose send hangs, a dispatcher
that fails, claims that record cancel flags. `_direct` is replaced, so no provider runs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any, cast

import pytest

from taskiq_llm_queue import Envelope, Lane, LLMReply, ModelConfig, QueueConfig, Target
from taskiq_llm_queue.client import LLMQueue
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.results import ResultDispatcher
from taskiq_llm_queue.results.dispatcher import ResultsUnavailable

MODELS = {
    "big": ModelConfig(
        provider="bedrock",
        model_id="big-id",
        targets={"primary": Target(region="r1")},
        queues={"default": "q-big", "rt": "q-big-rt"},
    )
}


class HangingKicker:
    """taskiq kicker chain whose `kiq` never returns (send on a dead SQS connection)."""

    def with_task_id(self, task_id: str) -> HangingKicker:
        return self

    def with_labels(self, **labels: Any) -> HangingKicker:
        return self

    async def kiq(self, body: Any) -> None:
        await asyncio.sleep(3600)


class RecordingClaims:
    """Claim store that records cancel flags; `fail` makes `cancel` raise."""

    def __init__(self) -> None:
        self.cancelled: list[str] = []
        self.fail: set[str] = set()

    async def cancel(self, request_id: str) -> None:
        if request_id in self.fail:
            raise ConnectionError("down")
        self.cancelled.append(request_id)


class DownStore:
    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        raise ConnectionError("valkey down")

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        raise ConnectionError("valkey down")


class Pong:
    async def ping(self) -> bool:
        return True


def make_queue(**config: Any) -> tuple[LLMQueue, RecordingClaims, list[tuple[str, bool]]]:
    """A started-looking `LLMQueue` with a hanging send; `_direct` calls are recorded."""
    failover = FailoverState(FailoverSettings(redis_retry_min_secs=60, redis_retry_max_secs=60))
    q = LLMQueue(QueueConfig(models=MODELS, **config), failover=failover)
    claims = RecordingClaims()
    broker = SimpleNamespace(find_task=lambda name: SimpleNamespace(kicker=HangingKicker))
    q._state = cast(
        Any,
        SimpleNamespace(
            brokers=SimpleNamespace(by_alias={"big": broker}),
            claims=claims,
            redis=Pong(),
            dispatcher=None,
        ),
    )
    direct: list[tuple[str, bool]] = []

    async def fake_direct(envelope: Envelope, *, claim: bool = False) -> LLMReply:
        direct.append((envelope.request_id, claim))
        return LLMReply(request_id=envelope.request_id, ok=True, response={})

    q._direct = fake_direct  # type: ignore[method-assign]
    return q, claims, direct


def envelope(q: LLMQueue, timeout_secs: float) -> Envelope:
    return q._envelope("converse", "big", {"messages": []}, None, None, timeout_secs, None, "r-1")


async def test_hung_sqs_send_fails_over_to_direct_within_the_send_timeout() -> None:
    """S20 bug: a send to an unreachable SQS hung ~50-70 s (dead pooled connection, 60 s read
    timeout), past the call budget, so the caller got LLMQueueTimeout. The send is now bounded
    by `sqs_send_timeout_secs`, then the call runs direct (DS17 claim) and SQS is failed over."""
    q, _, direct = make_queue(sqs_send_timeout_secs=0.1)
    t0 = time.monotonic()
    reply = await q._via_sqs("big", envelope(q, 30.0), embed=False)
    assert reply.ok
    assert time.monotonic() - t0 < 1.0
    assert direct == [("r-1", True)]
    assert cast(FailoverState, q.failover).sqs_failover_active("big")
    assert q._route("big", envelope(q, 30.0)) == "direct"


async def test_send_gets_at_most_half_the_remaining_budget() -> None:
    """S20 bug: the bounded send also leaves half the call budget for the direct run, so a
    short call is not spent entirely on a hung send."""
    q, _, direct = make_queue(sqs_send_timeout_secs=30.0)
    t0 = time.monotonic()
    await q._via_sqs("big", envelope(q, 0.2), embed=False)
    assert time.monotonic() - t0 < 0.5
    assert direct == [("r-1", True)]


async def test_slow_send_behind_a_burst_keeps_waiting_while_sqs_progresses() -> None:
    """S35b bug: 10K sends queued behind one batching sender passed the 5 s send timeout, so the
    sender failed SQS over and ran ~1,000 calls direct in its own process (claims left running,
    pool errors). While other sends complete, a slow send waits on, up to the budget share.
    """
    q, _, direct = make_queue(sqs_send_timeout_secs=0.1)

    async def progressing() -> None:  # other sends completing every 50 ms
        for _ in range(10):
            q._sent_at = time.monotonic()
            await asyncio.sleep(0.05)

    async def slow_send() -> None:
        await asyncio.sleep(0.3)

    ticker = asyncio.create_task(progressing())
    await q._bounded_send("big", asyncio.ensure_future(slow_send()), time.time() + 30)
    await ticker
    assert direct == []
    assert q._sent_at > 0
    stalled = asyncio.ensure_future(asyncio.sleep(3600))
    with pytest.raises(TimeoutError):  # no send completed within the timeout: SQS stalled
        await q._bounded_send("big", stalled, time.time() + 30)
    await asyncio.sleep(0)  # let the cancellation land
    assert stalled.cancelled()


async def test_dispatcher_read_errors_switch_new_calls_to_direct() -> None:
    """S21 bug: a send-only pod never noticed Valkey was down because the dispatcher's read
    errors did not reach FailoverState; calls kept going to SQS. Wired as the client wires it,
    one failed poll now routes the next call direct."""
    q, _, _ = make_queue()
    assert q._route("big", envelope(q, 30.0)) == "sqs"
    dispatcher = ResultDispatcher(
        DownStore(),
        intervals_ms={Lane.BG: 10},
        on_error=q._report_redis,
        on_recovered=q._redis_ok,
        abandon_after_secs=60,
    )
    waiter = asyncio.create_task(dispatcher.wait("other", 5.0))
    await asyncio.sleep(0.05)
    assert q.failover.redis_down()
    assert q._route("big", envelope(q, 30.0)) == "direct"
    waiter.cancel()
    await dispatcher.stop()


async def test_queued_waiter_runs_direct_when_results_are_unavailable() -> None:
    """S21 bug: callers already queued when Valkey went down waited to their deadline. On
    `ResultsUnavailable` the call runs direct and its cancel flag is kept for the recovery
    probe, so the queued copy does not run again later."""
    q, claims, direct = make_queue()
    q.failover.report_redis_failure(ConnectionError("down"))

    class Abandoning:
        async def wait(self, task_id: str, timeout_secs: float, lane: Any) -> LLMReply:
            raise ResultsUnavailable("reads failing")

    state = cast(Any, q._state)
    state.dispatcher = Abandoning()
    reply = await q._wait(state, envelope(q, 30.0))
    assert reply.ok
    assert direct == [("r-1", False)]
    assert claims.cancelled == []
    assert q._pending_cancels == {"r-1"}


async def test_recovery_probe_sets_the_pending_cancel_flags() -> None:
    """S21 bug: cancel flags that could not be written during the outage are set by the first
    successful Valkey probe; a flag that still fails stays pending for the next one."""
    q, claims, _ = make_queue()
    q._pending_cancels = {"a", "b"}
    claims.fail = {"b"}
    assert await q._probe_redis() is True
    assert claims.cancelled == ["a"]
    assert q._pending_cancels == {"b"}


@pytest.mark.parametrize("down", [True, False])
async def test_flag_cancel_skips_valkey_while_it_is_down(down: bool) -> None:
    """S21 bug: a timed-out caller's cancel flag hit an unreachable Valkey (connect timeouts
    and retries), holding the caller past its deadline; while down it is queued instead."""
    q, claims, _ = make_queue()
    if down:
        q.failover.report_redis_failure(ConnectionError("down"))
    await q._flag_cancel(cast(Any, q._state), "r-1")
    assert claims.cancelled == ([] if down else ["r-1"])
    assert q._pending_cancels == ({"r-1"} if down else set())
