"""Integration tests for the result backend and dispatcher against the real Valkey in Docker.

Needs `docker/compose.yaml` up (localhost:6379, password `localdev`). Every test uses its own
key prefix and deletes its keys afterwards, so runs can share one Valkey with other suites.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from redis.asyncio import Redis
from taskiq import TaskiqResult

from taskiq_llm_queue import KeySpace, Lane, LLMQueueTimeout, LLMReply
from taskiq_llm_queue.results import LLMQResultBackend, ResultDispatcher, publish_done

pytestmark = pytest.mark.integration

HOST, PORT, PASSWORD = "localhost", 6379, "localdev"


def reply(task_id: str) -> LLMReply:
    return LLMReply(request_id=task_id, ok=True, response={"text": task_id})


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    client = Redis(host=HOST, port=PORT, password=PASSWORD)
    try:
        await client.ping()
    except Exception as exc:  # stack not running
        await client.aclose()
        pytest.skip(f"Valkey not reachable on {HOST}:{PORT}: {exc}")
    yield client
    await client.aclose()


@pytest.fixture
async def keys(redis: Redis) -> AsyncIterator[KeySpace]:
    space = KeySpace(f"llmq:test:{uuid.uuid4().hex[:12]}")
    yield space
    stale = [key async for key in redis.scan_iter(match=f"{space.prefix}:*", count=1000)]
    if stale:
        await redis.delete(*stale)


@pytest.fixture
def backend(redis: Redis, keys: KeySpace) -> LLMQResultBackend:
    return LLMQResultBackend(keys, redis=redis, result_ttl_secs=300)


class CountingStore:
    """Wraps a ResultStore to count batched takes (each one is one MGET for <=1000 ids)."""

    def __init__(self, inner: LLMQResultBackend) -> None:
        self.inner = inner
        self.calls = 0

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        await self.inner.put(reply, ttl_secs=ttl_secs)

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        self.calls += 1
        return await self.inner.take_many(task_ids)


async def _mget_calls(redis: Redis) -> int:
    stats: dict[str, Any] = await redis.info("commandstats")
    return int(stats.get("cmdstat_mget", {}).get("calls", 0))


async def test_taskiq_round_trip_ttl_json_and_getdel(
    backend: LLMQResultBackend, redis: Redis, keys: KeySpace
) -> None:
    value = reply("t1").model_dump(mode="json")
    await backend.set_result(
        "t1", TaskiqResult[Any](is_err=False, return_value=value, execution_time=0)
    )
    raw = await redis.get(keys.result("t1"))
    assert raw is not None
    assert json.loads(raw)["return_value"] == value  # plain JSON, not pickle
    assert 295 <= await redis.ttl(keys.result("t1")) <= 300
    assert await backend.is_result_ready("t1")
    assert LLMReply.model_validate((await backend.get_result("t1")).return_value) == reply("t1")
    assert not await backend.is_result_ready("t1")


async def test_put_take_many_round_trip(
    backend: LLMQResultBackend, redis: Redis, keys: KeySpace
) -> None:
    await backend.put(reply("b"), ttl_secs=60)
    await backend.put(reply("a"), ttl_secs=60)
    assert 55 <= await redis.ttl(keys.result("a")) <= 60
    assert await backend.take_many(["a", "missing", "b"]) == {"a": reply("a"), "b": reply("b")}
    assert await backend.take_many(["a", "b"]) == {}


async def test_thousand_waiters_few_mgets(backend: LLMQResultBackend, redis: Redis) -> None:
    """1,000 waiters resolve with one MGET per poll, not one GET each (Valkey commandstats)."""
    store = CountingStore(backend)
    dispatcher = ResultDispatcher(store, intervals_ms={Lane.RT: 100})
    ids = [f"t{i}" for i in range(1000)]
    try:
        before = await _mget_calls(redis)
        waits = [asyncio.create_task(dispatcher.wait(i, 30, Lane.RT)) for i in ids]
        await asyncio.sleep(0)
        for task_id in reversed(ids):  # results arrive over time, out of order
            await backend.put(reply(task_id), ttl_secs=60)
        got = await asyncio.gather(*waits)
        mgets = await _mget_calls(redis) - before
    finally:
        await dispatcher.stop()
    assert [r.request_id for r in got] == ids
    print(f"\n1000 waiters: take_many calls={store.calls}, MGET delta={mgets}")
    assert store.calls <= 20  # one per 100 ms while 1,000 puts trickle in
    assert mgets <= store.calls + 5  # slack for other suites sharing this Valkey


async def test_timeout_and_late_result(backend: LLMQResultBackend) -> None:
    dispatcher = ResultDispatcher(backend, intervals_ms={Lane.RT: 50})
    try:
        with pytest.raises(LLMQueueTimeout):
            await dispatcher.wait("late", 0.3, Lane.RT)
        assert dispatcher.pending == 0
        await backend.put(reply("late"), ttl_secs=60)
        await asyncio.sleep(0.2)  # ignored: nobody waits
        assert dispatcher.pending == 0
    finally:
        await dispatcher.stop()


async def test_pubsub_wakeup_is_faster_than_poll(
    backend: LLMQResultBackend, redis: Redis, keys: KeySpace
) -> None:
    pod = f"pod-{uuid.uuid4().hex[:6]}"
    listener = Redis(host=HOST, port=PORT, password=PASSWORD)
    dispatcher = ResultDispatcher(
        backend, redis=listener, channel=keys.done_channel(pod), intervals_ms={Lane.BG: 5000}
    )
    try:
        waiting = asyncio.create_task(dispatcher.wait("t", 30, Lane.BG))
        await asyncio.sleep(0.3)  # subscribed and in a 5 s nap
        started = time.perf_counter()
        await backend.put(reply("t"), ttl_secs=60)
        await publish_done(redis, keys, pod, "t")
        assert (await asyncio.wait_for(waiting, 3)).ok
        elapsed = time.perf_counter() - started
        print(f"\npub/sub wake-up resolved in {elapsed * 1000:.1f} ms (poll interval 5000 ms)")
        assert elapsed < 1.0
    finally:
        await dispatcher.stop()
        await listener.aclose()


async def test_shared_owned_backend_survives_one_shutdown(keys: KeySpace) -> None:
    """Shared-backend rule: the first broker's shutdown must not close the shared pool."""
    shared = LLMQResultBackend(keys, host=HOST, port=PORT, password=PASSWORD, max_connections=4)
    await shared.startup()
    await shared.startup()
    await shared.shutdown()
    await shared.put(reply("x"), ttl_secs=60)
    assert await shared.take_many(["x"]) == {"x": reply("x")}
    await shared.shutdown()
    assert shared._redis is None


async def test_thousand_ready_results_one_mget(backend: LLMQResultBackend, redis: Redis) -> None:
    """With all 1,000 results already stored, the first poll resolves every waiter: one MGET."""
    ids = [f"r{i}" for i in range(1000)]
    for task_id in ids:
        await backend.put(reply(task_id), ttl_secs=60)
    store = CountingStore(backend)
    dispatcher = ResultDispatcher(store, intervals_ms={Lane.RT: 100})
    try:
        before = await _mget_calls(redis)
        got = await asyncio.gather(*(dispatcher.wait(i, 30, Lane.RT) for i in ids))
        mgets = await _mget_calls(redis) - before
    finally:
        await dispatcher.stop()
    assert [r.request_id for r in got] == ids
    print(f"\n1000 ready results: take_many calls={store.calls}, MGET delta={mgets}")
    assert store.calls == 1
    assert mgets <= 1 + 5  # slack for other suites sharing this Valkey
