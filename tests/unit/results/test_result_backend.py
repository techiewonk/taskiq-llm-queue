"""Unit tests for `LLMQResultBackend` on fakeredis: JSON format, TTL, take semantics, sharing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import fakeredis
import pytest
from redis.asyncio.client import Pipeline
from taskiq import InMemoryBroker, TaskiqResult
from taskiq.exceptions import ResultGetError
from taskiq.task import AsyncTaskiqTask

from taskiq_llm_queue import KeySpace, LLMReply, QueueConfig, ResultStore
from taskiq_llm_queue.results import LLMQResultBackend, decode_reply
from taskiq_llm_queue.results.backend import encode_reply

KEYS = KeySpace("llmq:unit")


def reply(task_id: str, text: str = "hi") -> LLMReply:
    return LLMReply(request_id=task_id, ok=True, response={"text": text})


def _taskiq_result(value: Any) -> TaskiqResult[Any]:
    return TaskiqResult[Any](is_err=False, return_value=value, execution_time=0.1)


def test_implements_result_store_protocol(backend: LLMQResultBackend) -> None:
    assert isinstance(backend, ResultStore)


async def test_taskiq_api_round_trip_and_getdel(backend: LLMQResultBackend) -> None:
    """set_result -> is_result_ready -> get_result returns the reply, then the key is gone."""
    expected = reply("t1")
    await backend.set_result("t1", _taskiq_result(expected.model_dump(mode="json")))
    assert await backend.is_result_ready("t1")
    got = await backend.get_result("t1")
    assert LLMReply.model_validate(got.return_value) == expected
    assert not await backend.is_result_ready("t1")
    with pytest.raises(ResultGetError):
        await backend.get_result("t1")


async def test_keep_on_read_when_delete_disabled(fake_redis: fakeredis.FakeAsyncRedis) -> None:
    backend = LLMQResultBackend(KEYS, redis=fake_redis, delete_on_read=False)
    await backend.set_result("t1", _taskiq_result(1))
    assert (await backend.get_result("t1")).return_value == 1
    assert (await backend.get_result("t1")).return_value == 1


async def test_ttl_and_json_bytes(
    backend: LLMQResultBackend, fake_redis: fakeredis.FakeAsyncRedis
) -> None:
    """Both write paths set the TTL and store plain JSON, never a pickle stream."""
    await backend.set_result("a", _taskiq_result({"x": 1}))
    await backend.put(reply("b"), ttl_secs=42)
    assert 295 <= await fake_redis.ttl(KEYS.result("a")) <= 300
    assert 40 <= await fake_redis.ttl(KEYS.result("b")) <= 42
    for task_id in ("a", "b"):
        raw = await fake_redis.get(KEYS.result(task_id))
        assert isinstance(raw, bytes)
        assert not raw.startswith(b"\x80")  # pickle protocol 2+ header
        assert isinstance(json.loads(raw), dict)


def test_no_pickle_in_results_package() -> None:
    import taskiq_llm_queue.results as pkg

    for source in Path(pkg.__file__).parent.glob("*.py"):
        assert "import pickle" not in source.read_text(encoding="utf-8"), source


async def test_take_many_returns_ready_and_deletes(backend: LLMQResultBackend) -> None:
    await backend.put(reply("t3", "three"), ttl_secs=60)
    await backend.put(reply("t1", "one"), ttl_secs=60)
    got = await backend.take_many(["t1", "t2", "t3"])
    assert got == {"t1": reply("t1", "one"), "t3": reply("t3", "three")}
    assert await backend.take_many(["t1", "t2", "t3"]) == {}


async def test_take_many_batches(fake_redis: fakeredis.FakeAsyncRedis) -> None:
    backend = LLMQResultBackend(KEYS, redis=fake_redis, take_batch=3)
    ids = [f"t{i}" for i in range(7)]
    for task_id in ids[::2]:
        await backend.put(reply(task_id), ttl_secs=60)
    assert sorted(await backend.take_many(ids)) == ["t0", "t2", "t4", "t6"]
    assert await backend.take_many([]) == {}
    with pytest.raises(ValueError, match="take_batch"):
        LLMQResultBackend(KEYS, take_batch=0)


async def test_taskiq_written_result_is_readable_by_take_many(backend: LLMQResultBackend) -> None:
    """A worker writing through taskiq's set_result feeds the dispatcher's take_many path."""
    await backend.set_result("t1", _taskiq_result(reply("t1").model_dump(mode="json")))
    assert await backend.take_many(["t1"]) == {"t1": reply("t1")}


def test_decode_reply_turns_errors_into_values() -> None:
    err = TaskiqResult[Any](
        is_err=True, return_value=None, execution_time=0.0, error=RuntimeError("boom")
    )
    failed = decode_reply("t1", err.model_dump_json())
    assert (failed.ok, failed.request_id, failed.error) == (
        False,
        "t1",
        {"type": "RuntimeError", "message": "boom", "retryable": False},
    )
    for raw in (b"not json", _taskiq_result({"no": "reply"}).model_dump_json()):
        bad = decode_reply("t2", raw)
        assert not bad.ok
        assert bad.error is not None
        assert bad.error["type"] == "invalid_result"


async def test_inmemory_broker_end_to_end(backend: LLMQResultBackend) -> None:
    """A real taskiq broker stores a task's return value as JSON and wait_result reads it."""
    broker = InMemoryBroker().with_result_backend(backend)

    @broker.task(task_name="unit.echo")
    async def echo(task_id: str) -> dict[str, Any]:
        return reply(task_id).model_dump(mode="json")

    await broker.startup()
    try:
        task = await echo.kiq("x")
        result = await task.wait_result(timeout=5)
        assert LLMReply.model_validate(result.return_value) == reply("x")
        typed = AsyncTaskiqTask(task.task_id, backend, return_type=LLMReply)
        assert not await typed.is_ready()  # GETDEL consumed it
    finally:
        await broker.shutdown()


async def test_shared_owned_backend_survives_one_shutdown() -> None:
    """Shared-backend rule: two brokers share one backend; the first shutdown keeps the pool."""
    backend = LLMQResultBackend(KEYS, host="localhost", port=1, max_connections=2)
    await backend.startup()
    await backend.startup()
    client = backend.redis
    await backend.shutdown()
    assert backend.redis is client
    await backend.shutdown()
    assert backend._redis is None
    await backend.shutdown()  # extra shutdowns are harmless
    await backend.aclose()


async def test_injected_client_is_never_closed(
    backend: LLMQResultBackend, fake_redis: fakeredis.FakeAsyncRedis
) -> None:
    await backend.startup()
    await backend.shutdown()
    await backend.aclose()
    assert backend.redis is fake_redis
    assert await fake_redis.ping()


def test_from_config() -> None:
    cfg = QueueConfig(key_prefix="llmq:x", result_ttl_secs=77, redis_max_connections=4)
    backend = LLMQResultBackend.from_config(cfg)
    assert backend.keys == KeySpace("llmq:x")
    assert backend.result_ttl_secs == 77


def _old_put_bytes(value: LLMReply) -> bytes:
    """What `put` wrote before the single-serialization change (older worker pods)."""
    result = TaskiqResult[Any](
        is_err=False, return_value=value.model_dump(mode="json"), execution_time=0.0
    )
    return result.model_dump_json().encode()


async def test_put_format_is_compatible_both_ways(
    backend: LLMQResultBackend, fake_redis: fakeredis.FakeAsyncRedis
) -> None:
    """Rolling deploy: new and old result bytes are the same JSON and decode both ways."""
    value = LLMReply(request_id="t1", ok=True, response={"text": "नमस्ते 你好", "n": 1.5})
    new = encode_reply(value)
    assert json.loads(new) == json.loads(_old_put_bytes(value))
    assert decode_reply("t1", _old_put_bytes(value)) == value
    assert LLMReply.model_validate(TaskiqResult[Any].model_validate_json(new).return_value) == value
    await backend.put(value, ttl_secs=60)
    assert await fake_redis.get(KEYS.result("t1")) == new
    assert LLMReply.model_validate((await backend.get_result("t1")).return_value) == value


async def test_take_many_on_cluster_client_uses_getdel(
    fake_redis: fakeredis.FakeAsyncRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Item 8: hash-tagged keys sit in many slots, so a cluster client takes per key."""
    import taskiq_llm_queue.results.backend as module

    monkeypatch.setattr(module, "RedisCluster", fakeredis.FakeAsyncRedis)
    getdels = 0
    original = Pipeline.getdel

    def counting(self: Pipeline, *args: Any, **kwargs: Any) -> Any:
        nonlocal getdels
        getdels += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Pipeline, "getdel", counting)
    backend = LLMQResultBackend(KEYS, redis=fake_redis)
    await backend.put(reply("a"), ttl_secs=60)
    await backend.put(reply("c"), ttl_secs=60)
    assert await backend.take_many(["a", "b", "c"]) == {"a": reply("a"), "c": reply("c")}
    assert getdels == 3
    assert await backend.take_many(["a", "c"]) == {}
