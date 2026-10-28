"""Unit tests for `ResultDispatcher`: batching, lanes, ordering, timeout, cancel, wake-up.

A manual clock drives the poll loop, so every poll is counted exactly and no test waits on
real intervals (only short real sleeps to let fakeredis round trips finish).
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Sequence
from typing import Any

import fakeredis
import pytest
from redis.asyncio.client import Pipeline

from taskiq_llm_queue import KeySpace, Lane, LLMQueueTimeout, LLMReply, QueueUnavailable
from taskiq_llm_queue.results import LLMQResultBackend, ResultDispatcher, publish_done


def reply(task_id: str, text: str = "hi") -> LLMReply:
    return LLMReply(request_id=task_id, ok=True, response={"text": text})


async def settle() -> None:
    """Let fakeredis round trips and callbacks finish (real time; the dispatcher clock is fake)."""
    await asyncio.sleep(0.05)


class ManualClock:
    """Clock plus sleep that only move when the test calls `advance`."""

    def __init__(self) -> None:
        self.now = 0.0
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._sleepers.append((self.now + secs, future))
        await future

    async def advance(self, secs: float) -> None:
        self.now += secs
        due = [f for t, f in self._sleepers if t <= self.now]
        self._sleepers = [(t, f) for t, f in self._sleepers if t > self.now]
        for future in due:
            if not future.done():
                future.set_result(None)
        await settle()


class DictStore:
    """In-memory ResultStore that can fail on demand."""

    def __init__(self) -> None:
        self.data: dict[str, LLMReply] = {}
        self.fail_next = 0

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        self.data[reply.request_id] = reply

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        if self.fail_next:
            self.fail_next -= 1
            raise ConnectionError("down")
        return {i: self.data.pop(i) for i in task_ids if i in self.data}


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
async def store_dispatcher(
    clock: ManualClock,
) -> AsyncIterator[tuple[DictStore, ResultDispatcher]]:
    store = DictStore()
    dispatcher = ResultDispatcher(store, clock=clock, sleep=clock.sleep)
    yield store, dispatcher
    await dispatcher.stop()


async def test_many_waiters_one_mget(
    backend: LLMQResultBackend, clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """1,000 waiters with results arriving out of order resolve with one poll and one MGET."""
    mgets = 0
    original = Pipeline.mget

    def counting_mget(self: Pipeline, *args: Any, **kwargs: Any) -> Any:
        nonlocal mgets
        mgets += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Pipeline, "mget", counting_mget)
    dispatcher = ResultDispatcher(backend, clock=clock, sleep=clock.sleep)
    ids = [f"t{i}" for i in range(1000)]
    waits = [asyncio.create_task(dispatcher.wait(i, 1e6, Lane.RT)) for i in ids]
    await settle()
    assert (dispatcher.pending, dispatcher.polls) == (1000, 0)
    for task_id in random.Random(7).sample(ids, len(ids)):
        await backend.put(reply(task_id, task_id), ttl_secs=60)
    await clock.advance(0.1)
    got = await asyncio.gather(*waits)
    assert [r.response for r in got] == [{"text": i} for i in ids]
    assert (dispatcher.polls, mgets, dispatcher.pending) == (1, 1, 0)
    await dispatcher.stop()


async def test_out_of_order_and_partial(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    store, dispatcher = store_dispatcher
    waits = {i: asyncio.create_task(dispatcher.wait(i, 1e6, Lane.RT)) for i in ("a", "b", "c")}
    await settle()
    store.data.update({"c": reply("c"), "a": reply("a")})
    await clock.advance(0.1)
    assert [waits[k].done() for k in "abc"] == [True, False, True]
    store.data["b"] = reply("b")
    await clock.advance(0.1)
    assert (await waits["b"]).request_id == "b"
    assert dispatcher.pending == 0


async def test_poll_interval_follows_fastest_lane(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    _, dispatcher = store_dispatcher
    bulk = asyncio.create_task(dispatcher.wait("bulk", 1e6, Lane.BULK))
    await settle()
    await clock.advance(1.9)
    assert dispatcher.polls == 0
    await clock.advance(0.1)
    assert dispatcher.polls == 1
    rt = asyncio.create_task(dispatcher.wait("rt", 1e6, Lane.RT))
    await settle()  # a faster lane wakes the 2 s nap at once
    assert dispatcher.polls == 2
    await clock.advance(0.1)
    assert dispatcher.polls == 3
    for task in (bulk, rt):
        task.cancel()


async def test_idle_dispatcher_does_not_poll(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    _, dispatcher = store_dispatcher
    await dispatcher.start()
    await clock.advance(10)
    assert dispatcher.polls == 0


async def test_timeout_raises_and_cleans_map(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    store, dispatcher = store_dispatcher
    waiting = asyncio.create_task(dispatcher.wait("t", 1.0, Lane.BULK))
    await settle()
    await clock.advance(0.5)
    assert not waiting.done()
    await clock.advance(0.6)  # deadline caps the 2 s bulk nap
    with pytest.raises(LLMQueueTimeout):
        await waiting
    assert dispatcher.pending == 0
    store.data["t"] = reply("t")  # a late result is ignored
    await clock.advance(5)
    assert dispatcher.pending == 0


async def test_cancellation_removes_waiter(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    store, dispatcher = store_dispatcher
    waiting = asyncio.create_task(dispatcher.wait("t", 1e6, Lane.RT))
    await settle()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert dispatcher.pending == 0
    store.data["t"] = reply("t")
    await clock.advance(1)
    assert dispatcher.polls == 0


async def test_store_error_is_retried(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    store, dispatcher = store_dispatcher
    store.fail_next = 1
    store.data["t"] = reply("t")
    waiting = asyncio.create_task(dispatcher.wait("t", 1e6, Lane.RT))
    await settle()
    await clock.advance(0.1)
    assert not waiting.done()
    await clock.advance(0.1)
    assert (await waiting).ok
    assert dispatcher.polls == 2


async def test_duplicate_wait_is_rejected(
    store_dispatcher: tuple[DictStore, ResultDispatcher],
) -> None:
    _, dispatcher = store_dispatcher
    first = asyncio.create_task(dispatcher.wait("t", 1e6))
    await settle()
    with pytest.raises(ValueError, match="already waiting"):
        await dispatcher.wait("t", 1e6)
    first.cancel()


async def test_start_stop_idempotent_and_stop_fails_waiters(
    store_dispatcher: tuple[DictStore, ResultDispatcher], clock: ManualClock
) -> None:
    store, dispatcher = store_dispatcher
    await dispatcher.start()
    await dispatcher.start()
    waiting = asyncio.create_task(dispatcher.wait("t", 1e6))
    await settle()
    await dispatcher.stop()
    await dispatcher.stop()
    with pytest.raises(QueueUnavailable):
        await waiting
    store.data["u"] = reply("u")  # restart after stop works
    again = asyncio.create_task(dispatcher.wait("u", 1e6, Lane.RT))
    await settle()
    await clock.advance(0.1)
    assert (await again).request_id == "u"


async def test_pubsub_wakeup_beats_poll_interval() -> None:
    """A done message resolves the waiter long before the 5 s poll interval (real time)."""
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeAsyncRedis(server=server)
    keys = KeySpace("llmq:unit")
    backend = LLMQResultBackend(keys, redis=redis)
    dispatcher = ResultDispatcher(
        backend,
        redis=fakeredis.FakeAsyncRedis(server=server),
        channel=keys.done_channel("pod-a"),
        intervals_ms={Lane.BG: 5000},
    )
    try:
        waiting = asyncio.create_task(dispatcher.wait("t", 30, Lane.BG))
        await asyncio.sleep(0.2)  # subscribed and napping
        started = time.perf_counter()
        await backend.put(reply("t"), ttl_secs=60)
        await publish_done(redis, keys, "pod-a", "t")
        assert (await asyncio.wait_for(waiting, 2)).ok
        assert time.perf_counter() - started < 1.0
    finally:
        await dispatcher.stop()
        await redis.aclose()
