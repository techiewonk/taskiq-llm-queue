"""S3 offload middleware and S3 result backend against an in-process moto S3 server.

Covers the P9 round trip (pre_send -> pre_execute -> post_execute), small payloads staying
inline, missing-object errors, bucket declaration, and the P10 backend's taskiq contract.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq.exceptions import ResultGetError

from taskiq_llm_queue.broker.s3_offload import (
    DEFAULT_THRESHOLD_BYTES,
    OFFLOAD_LABEL,
    OffloadedPayloadMissingError,
    S3OffloadError,
    S3OffloadMiddleware,
)
from taskiq_llm_queue.results.s3_backend import S3ResultBackend

pytestmark = pytest.mark.integration


async def _keys(s3_client: Any, bucket: str) -> list[str]:
    response = await s3_client.list_objects_v2(Bucket=bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def _message(task_id: str, args: list[Any]) -> TaskiqMessage:
    return TaskiqMessage(task_id=task_id, task_name="t", labels={}, args=args, kwargs={"k": 1})


async def test_offload_round_trip_and_delete(
    s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    middleware = S3OffloadMiddleware(bucket, **s3_kwargs)
    middleware.set_broker(InMemoryBroker())
    await middleware.startup()
    try:
        big = "x" * (DEFAULT_THRESHOLD_BYTES + 1)
        sent = await middleware.pre_send(_message("task-1", [big]))
        assert sent.labels[OFFLOAD_LABEL] == "taskiq-offload/task-1.json"
        # the message the broker batches is now tiny (DS15 byte-aware batching)
        assert len(InMemoryBroker().formatter.dumps(sent).message) < 1_000
        stored = await s3_client.get_object(Bucket=bucket, Key="taskiq-offload/task-1.json")
        async with stored["Body"] as stream:
            assert json.loads(await stream.read()) == {"args": [big], "kwargs": {"k": 1}}

        restored = await middleware.pre_execute(sent)
        assert (restored.args, restored.kwargs) == ([big], {"k": 1})
        ok = TaskiqResult[Any](is_err=False, return_value=1, execution_time=0.0)
        await middleware.post_execute(restored, ok)
        assert await _keys(s3_client, bucket) == []
    finally:
        await middleware.shutdown()


async def test_small_payload_stays_inline(
    s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    middleware = S3OffloadMiddleware(bucket, **s3_kwargs)
    await middleware.startup()
    try:
        sent = await middleware.pre_send(_message("task-2", ["small"]))
        assert sent.args == ["small"]
        assert OFFLOAD_LABEL not in sent.labels
        assert await _keys(s3_client, bucket) == []
    finally:
        await middleware.shutdown()


async def test_missing_object_raises(s3_kwargs: dict[str, Any], bucket: str) -> None:
    middleware = S3OffloadMiddleware(bucket, **s3_kwargs)
    await middleware.startup()
    try:
        message = _message("task-3", [])
        message.labels[OFFLOAD_LABEL] = "taskiq-offload/task-3.json"
        with pytest.raises(OffloadedPayloadMissingError):
            await middleware.pre_execute(message)
    finally:
        await middleware.shutdown()


async def test_bucket_missing_fails_unless_declared(
    s3_kwargs: dict[str, Any], s3_client: Any
) -> None:
    name = f"llmq-new-{uuid.uuid4().hex[:8]}"
    with pytest.raises(S3OffloadError, match="does not exist"):
        await S3OffloadMiddleware(name, **s3_kwargs).startup()
    declared = S3OffloadMiddleware(name, declare=True, **s3_kwargs)
    await declared.startup()
    await declared.shutdown()
    await s3_client.head_bucket(Bucket=name)


async def test_end_to_end_through_taskiq_broker(
    s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    """A real taskiq kick/receive with both pieces wired in: offloaded args, S3 result."""
    backend = S3ResultBackend(bucket, delete_on_read=True, **s3_kwargs)
    broker = InMemoryBroker().with_result_backend(backend)
    broker.add_middlewares(S3OffloadMiddleware(bucket, threshold_bytes=1_000, **s3_kwargs))

    @broker.task
    async def length(text: str) -> int:
        return len(text)

    await broker.startup()
    try:
        task = await length.kiq("q" * 5_000)
        result = await task.wait_result(timeout=10)
        assert result.return_value == 5_000
        assert await _keys(s3_client, bucket) == []  # payload deleted, result deleted on read
    finally:
        await broker.shutdown()


async def test_result_backend_set_get_ready(
    s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    backend = S3ResultBackend(bucket, prefix="res", **s3_kwargs)
    await backend.startup()
    try:
        assert not await backend.is_result_ready("r1")
        with pytest.raises(ResultGetError):
            await backend.get_result("r1")
        value = TaskiqResult[Any](is_err=False, return_value={"a": [1, 2]}, execution_time=0.5)
        await backend.set_result("r1", value)
        assert await backend.is_result_ready("r1")
        got = await backend.get_result("r1")
        assert got.return_value == {"a": [1, 2]}
        assert got.execution_time == 0.5
        assert await _keys(s3_client, bucket) == ["res/r1.json"]  # kept: delete_on_read off
    finally:
        await backend.shutdown()


async def test_result_backend_delete_on_read(s3_kwargs: dict[str, Any], bucket: str) -> None:
    backend = S3ResultBackend(bucket, delete_on_read=True, **s3_kwargs)
    await backend.startup()
    try:
        error = TaskiqResult[Any](
            is_err=True, return_value=None, execution_time=0.0, error=ValueError("boom")
        )
        await backend.set_result("r2", error)
        got = await backend.get_result("r2")
        assert got.is_err
        assert not await backend.is_result_ready("r2")
        with pytest.raises(ResultGetError):
            await backend.get_result("r2")
    finally:
        await backend.shutdown()


async def test_shared_backend_survives_one_shutdown(s3_kwargs: dict[str, Any], bucket: str) -> None:
    backend = S3ResultBackend(bucket, **s3_kwargs)
    await backend.startup()
    await backend.startup()
    await backend.shutdown()
    await backend.set_result(
        "r3", TaskiqResult[Any](is_err=False, return_value=3, execution_time=0)
    )
    assert await backend.is_result_ready("r3")
    await backend.shutdown()
    with pytest.raises(S3OffloadError, match="not started"):
        await backend.is_result_ready("r3")
