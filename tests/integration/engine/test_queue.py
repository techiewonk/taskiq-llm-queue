"""Stream queue on Valkey and Redis (plan step A5; scenarios B42, B43, B76, B80 queue parts).

Order by stream id, return in place without a failure, a lapsed hold counts one failure, dead
letters reported to the receiver, stale receipts refused, the waiting limit, redrive.
"""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio

from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.queue import DeadLetter, QueueFull, QueueSettings, StreamQueues

pytestmark = pytest.mark.integration
FAST = QueueSettings(visibility_secs=0.3, max_receives=3, max_waiting=0)


async def _filled(store: redis.asyncio.Redis, prefix: str, n: int) -> StreamQueues:
    queues = StreamQueues(store, Keys(prefix))
    for i in range(n):
        await queues.send("q", f"m{i}".encode(), FAST)
    return queues


async def test_order_and_ack(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 5)
    first, _ = await queues.receive("q", 2, FAST)
    second, _ = await queues.receive("q", 10, FAST)
    assert [m.body for m in first] == [b"m0", b"m1"]
    assert [m.body for m in second] == [b"m2", b"m3", b"m4"]
    assert await queues.depth("q") == (5, 5, 0)
    for message in first + second:
        assert await queues.ack(message)
    assert await queues.depth("q") == (0, 0, 0)
    assert first[0].position_ms > 0


async def test_return_keeps_place_and_no_failure(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 3)
    batch, _ = await queues.receive("q", 3, FAST)
    for message in batch:  # e.g. a held batch returned at shutdown
        await queues.return_(message)
    again, _ = await queues.receive("q", 3, FAST)
    assert [m.body for m in again] == [b"m0", b"m1", b"m2"]
    assert all(m.generation == 2 for m in again)
    for _ in range(10):  # returned many times: never dead
        for message in again:
            await queues.return_(message)
        again, dead = await queues.receive("q", 3, FAST)
        assert not dead
    assert len(again) == 3


async def test_returned_message_goes_before_newer_ones(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    queues = await _filled(store, prefix, 1)
    (held,), _ = await queues.receive("q", 1, FAST)
    await queues.send("q", b"later", FAST)
    await queues.return_(held)
    order, _ = await queues.receive("q", 2, FAST)
    assert [m.body for m in order] == [b"m0", b"later"]


async def test_delayed_return(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (held,), _ = await queues.receive("q", 1, FAST)
    await queues.return_(held, delay_secs=0.4)
    assert (await queues.receive("q", 1, FAST))[0] == []
    await asyncio.sleep(0.5)
    assert len((await queues.receive("q", 1, FAST))[0]) == 1


async def test_lapsed_hold_counts_until_dead(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    received, _ = await queues.receive("q", 1, FAST)
    assert len(received) == 1
    dead: list[DeadLetter] = []
    for _ in range(5):  # the worker dies every time: the hold lapses
        await asyncio.sleep(0.35)
        _, dead = await queues.receive("q", 1, FAST)
        if dead:
            break
    assert [d.body for d in dead] == [b"m0"]
    assert await queues.depth("q") == (0, 0, 1)
    letters = await queues.dead_letters("q")
    assert letters[0]["fails"] == 3
    assert await queues.redrive("q") == 1
    assert len((await queues.receive("q", 1, FAST))[0]) == 1


async def test_stale_receipt_refused(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (old,), _ = await queues.receive("q", 1, FAST)
    await asyncio.sleep(0.35)  # the hold lapses; another worker receives it
    (new,), _ = await queues.receive("q", 1, FAST)
    assert new.generation == old.generation + 1
    assert not await queues.ack(old)
    assert not await queues.extend(old, 10)
    assert not await queues.return_(old)
    assert await queues.extend(new, 10)
    assert await queues.ack(new)


async def test_failures_and_waiting_limit(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "retry"
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "retry"
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "dead"
    limited = QueueSettings(max_waiting=2)
    await queues.send("q", b"a", limited)
    await queues.send("q", b"b", limited)
    with pytest.raises(QueueFull) as info:
        await queues.send("q", b"c", limited)
    assert info.value.depth == 2


async def test_receive_wait_returns_when_a_message_arrives(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    queues = StreamQueues(store, Keys(prefix))

    async def later() -> None:
        await asyncio.sleep(0.2)
        await queues.send("q", b"x", FAST)

    task = asyncio.create_task(later())
    messages, _ = await queues.receive_wait("q", 1, FAST, wait_secs=2)
    await task
    assert [m.body for m in messages] == [b"x"]
    empty, _ = await queues.receive_wait("q", 1, FAST, wait_secs=0.1)
    assert empty == []
