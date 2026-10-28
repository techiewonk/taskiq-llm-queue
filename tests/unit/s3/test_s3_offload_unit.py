"""Offline tests for S3 offload: key layout, JSON payload codec, threshold and label logic.

A recording fake stands in for the aiobotocore client, so no network or moto is needed.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult

from taskiq_llm_queue.broker.s3_offload import (
    OFFLOAD_LABEL,
    OffloadedPayloadMissingError,
    S3Connection,
    S3OffloadError,
    S3OffloadMiddleware,
    decode_payload,
    encode_payload,
    object_key,
)


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aenter__(self) -> _Body:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def read(self) -> bytes:
        return self._data


def _missing(op: str) -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey", "Message": "gone"}}, op)


class FakeS3:
    """Dict-backed subset of the S3 client API used by the middleware."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[str] = []

    async def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_: Any) -> None:
        self.calls.append(f"put {Key}")
        self.objects[Key] = Body

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.calls.append(f"get {Key}")
        if Key not in self.objects:
            raise _missing("GetObject")
        return {"Body": _Body(self.objects[Key])}

    async def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.calls.append(f"delete {Key}")
        self.objects.pop(Key, None)

    async def head_bucket(self, *, Bucket: str) -> None:
        self.calls.append("head_bucket")


def _middleware(fake: FakeS3, **kwargs: Any) -> S3OffloadMiddleware:
    return S3OffloadMiddleware("bucket", client=cast(Any, fake), **kwargs)


def _message(args: list[Any], kwargs: dict[str, Any] | None = None) -> TaskiqMessage:
    return TaskiqMessage(task_id="t-1", task_name="task", labels={}, args=args, kwargs=kwargs or {})


def _ok() -> TaskiqResult[Any]:
    return TaskiqResult[Any](is_err=False, return_value=None, execution_time=0.0)


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [("", "t.json"), ("p", "p/t.json"), ("/a/b/", "a/b/t.json")],
)
def test_object_key_layout(prefix: str, expected: str) -> None:
    assert object_key(prefix, "t") == expected


@pytest.mark.parametrize("task_id", ["", "a/b", "..", "."])
def test_object_key_rejects_unsafe_task_ids(task_id: str) -> None:
    with pytest.raises(ValueError, match="S3 key segment"):
        object_key("p", task_id)


def test_payload_round_trip_is_json_with_unicode() -> None:
    args, kwargs = [1, "é", {"x": [None, True]}], {"k": 2.5}
    body = encode_payload(args, kwargs)
    assert body.startswith(b'{"args":')
    assert decode_payload(body) == (args, kwargs)


def test_encode_rejects_non_json_values() -> None:
    with pytest.raises(S3OffloadError, match="JSON-serializable"):
        encode_payload([object()], {})


@pytest.mark.parametrize(
    "body", [b"not json", b"[]", b'{"args": {}, "kwargs": {}}', b'{"args": []}']
)
def test_decode_rejects_bad_payloads(body: bytes) -> None:
    with pytest.raises(S3OffloadError):
        decode_payload(body)


def test_constructor_validates_without_io() -> None:
    with pytest.raises(ValueError, match="threshold_bytes"):
        S3OffloadMiddleware("b", threshold_bytes=0)
    with pytest.raises(ValueError, match="bucket"):
        S3Connection("")
    middleware = S3OffloadMiddleware("b")  # no client is created at construction
    with pytest.raises(S3OffloadError, match="not started"):
        _ = middleware.s3.client


async def test_small_payload_is_untouched() -> None:
    fake = FakeS3()
    message = _message(["small"])
    out = await _middleware(fake, threshold_bytes=1000).pre_send(message)
    assert out.args == ["small"]
    assert OFFLOAD_LABEL not in out.labels
    assert fake.calls == []


async def test_threshold_is_inclusive_and_measured_on_payload_without_broker() -> None:
    message = _message(["x" * 50])
    size = len(encode_payload(message.args, message.kwargs))
    fake = FakeS3()
    await _middleware(fake, threshold_bytes=size).pre_send(message)
    assert fake.calls == []
    await _middleware(fake, threshold_bytes=size - 1).pre_send(message)
    assert fake.calls == ["put taskiq-offload/t-1.json"]


async def test_threshold_uses_broker_formatter_when_attached() -> None:
    """The DS15 limit is on the serialized taskiq message, labels and task name included."""
    middleware = _middleware(FakeS3())
    middleware.set_broker(InMemoryBroker())
    message = _message(["x" * 10])
    whole = len(InMemoryBroker().formatter.dumps(message).message)
    assert middleware.message_size(message) == whole
    assert whole > len(encode_payload(message.args, message.kwargs))


async def test_large_payload_offloaded_restored_and_deleted() -> None:
    fake = FakeS3()
    middleware = _middleware(fake, threshold_bytes=100, base_path="big")
    message = _message(["y" * 500], {"n": 1})
    sent = await middleware.pre_send(message)
    assert (sent.args, sent.kwargs) == ([], {})
    assert sent.labels[OFFLOAD_LABEL] == "big/t-1.json"
    restored = await middleware.pre_execute(sent)
    assert (restored.args, restored.kwargs) == (["y" * 500], {"n": 1})
    await middleware.post_execute(restored, _ok())
    assert fake.objects == {}


async def test_failed_run_keeps_object_for_redelivery() -> None:
    fake = FakeS3()
    middleware = _middleware(fake, threshold_bytes=10)
    sent = await middleware.pre_send(_message(["z" * 100]))
    failed = TaskiqResult[Any](is_err=True, return_value=None, execution_time=0.0)
    await middleware.post_execute(sent, failed)
    assert "taskiq-offload/t-1.json" in fake.objects


async def test_delete_after_execute_disabled_keeps_object() -> None:
    fake = FakeS3()
    middleware = _middleware(fake, threshold_bytes=10, delete_after_execute=False)
    sent = await middleware.pre_send(_message(["z" * 100]))
    await middleware.post_execute(sent, _ok())
    assert list(fake.objects) == ["taskiq-offload/t-1.json"]


async def test_stale_label_is_dropped_on_re_kick() -> None:
    """A re-kicked message with restored args must not keep pointing at a deleted object."""
    message = _message(["small"])
    message.labels[OFFLOAD_LABEL] = "old/key.json"
    out = await _middleware(FakeS3(), threshold_bytes=1000).pre_send(message)
    assert OFFLOAD_LABEL not in out.labels


async def test_missing_object_raises_typed_error() -> None:
    message = _message([])
    message.labels[OFFLOAD_LABEL] = "gone.json"
    with pytest.raises(OffloadedPayloadMissingError, match=r"s3://bucket/gone\.json"):
        await _middleware(FakeS3()).pre_execute(message)


async def test_injected_client_is_never_closed() -> None:
    fake = FakeS3()
    middleware = _middleware(fake)
    await middleware.startup()
    await middleware.shutdown()
    assert middleware.s3.client is cast(Any, fake)
    assert fake.calls == ["head_bucket"]
