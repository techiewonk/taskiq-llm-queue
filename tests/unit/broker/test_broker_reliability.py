"""Receive-side reliability against a fake SQS client: poller survival, budget, retries, acks.

Regression tests for review findings 16 (silent poller death), 18 (retry stacking), DS4
(budget ignores buffered messages), DS6 (heartbeat past the deadline) and the ack-only
`after_ack` hook plus `S3OffloadMiddleware` delete/missing-object handling (finding 13).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any

import pytest
from botocore.exceptions import ClientError
from taskiq import AckableMessage, TaskiqMessage, TaskiqResult

from taskiq_llm_queue.broker import BrokerHooks, Delivery, Heartbeat, HybridSQSBroker, SQSQueue
from taskiq_llm_queue.broker.acker import Acker
from taskiq_llm_queue.broker.heartbeat import HeartbeatManager
from taskiq_llm_queue.broker.s3_offload import (
    OFFLOAD_LABEL,
    OffloadedPayloadMissingError,
    S3OffloadMiddleware,
    encode_payload,
)

TASK = "unit.task"


class FakeSQS:
    """Serves queued messages to `receive_message` and records every call."""

    def __init__(self) -> None:
        self.inbox: list[dict[str, Any]] = []
        self.receives: list[int] = []
        self.deleted: list[str] = []
        self.visibility: list[tuple[str, int]] = []
        self.config: Any = None
        self._n = 0

    async def __aenter__(self) -> FakeSQS:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def push(
        self,
        broker: HybridSQSBroker,
        task_id: str,
        expiry: float | None = None,
        labels: dict[str, Any] | None = None,
    ) -> str:
        self._n += 1
        body = broker.formatter.dumps(_taskiq_message(task_id, labels)).message.decode()
        message: dict[str, Any] = {
            "MessageId": f"m{self._n}",
            "ReceiptHandle": f"h{self._n}",
            "Body": body,
            "Attributes": {"ApproximateReceiveCount": "1"},
        }
        if expiry is not None:
            message["MessageAttributes"] = {
                "expiry": {"DataType": "Number", "StringValue": repr(expiry)}
            }
        self.inbox.append(message)
        return str(message["ReceiptHandle"])

    async def get_queue_url(self, QueueName: str) -> dict[str, str]:
        return {"QueueUrl": f"http://fake/000/{QueueName}"}

    async def get_queue_attributes(self, **_: Any) -> dict[str, Any]:
        return {"Attributes": {"VisibilityTimeout": "30"}}

    async def receive_message(self, *, MaxNumberOfMessages: int, **_: Any) -> dict[str, Any]:
        self.receives.append(MaxNumberOfMessages)
        taken, self.inbox = self.inbox[:MaxNumberOfMessages], self.inbox[MaxNumberOfMessages:]
        if not taken:
            await asyncio.sleep(0.02)
        return {"Messages": taken}

    async def delete_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        self.deleted += [e["ReceiptHandle"] for e in Entries]
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    async def change_message_visibility_batch(
        self, QueueUrl: str, Entries: list[dict[str, Any]]
    ) -> Any:
        self.visibility += [(e["ReceiptHandle"], e["VisibilityTimeout"]) for e in Entries]
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    async def change_message_visibility(
        self, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int
    ) -> Any:
        self.visibility.append((ReceiptHandle, VisibilityTimeout))
        return {}


class FakeSession:
    def __init__(self, client: FakeSQS) -> None:
        self.client = client

    def create_client(self, *_: Any, config: Any = None, **__: Any) -> FakeSQS:
        self.client.config = config
        return self.client


def _taskiq_message(task_id: str, labels: dict[str, Any] | None = None) -> TaskiqMessage:
    return TaskiqMessage(task_id=task_id, task_name=TASK, labels=labels or {}, args=[], kwargs={})


class ErrorCounter(BrokerHooks):
    def __init__(self) -> None:
        self.errors: list[tuple[str, str]] = []

    def on_error(self, queue: str, operation: str, family: str) -> None:
        self.errors.append((operation, family))


def _broker(fake: FakeSQS, queue: SQSQueue | None = None, **kwargs: Any) -> HybridSQSBroker:
    kwargs.setdefault("backoff_base_secs", 0.001)
    kwargs.setdefault("backoff_cap_secs", 0.01)
    kwargs.setdefault("ack_flush_secs", 0.01)
    kwargs.setdefault("heartbeat", None)
    broker = HybridSQSBroker(
        [queue or SQSQueue("q", wait_time_secs=0)],
        session=FakeSession(fake),  # type: ignore[arg-type]
        **kwargs,
    )

    @broker.task(task_name=TASK)
    async def _task() -> None:
        return None

    return broker


@contextlib.asynccontextmanager
async def listening(
    broker: HybridSQSBroker,
) -> AsyncIterator[AsyncGenerator[bytes | AckableMessage, None]]:
    await broker.startup()
    stream = broker.listen()
    try:
        yield stream
    finally:
        await stream.aclose()
        await broker.shutdown()


async def _quiesce(broker: HybridSQSBroker) -> None:
    """Stop receiving so an idle long poll no longer holds its one reserved slot."""
    await broker.pause_receiving()
    await asyncio.sleep(0.1)


async def _next(stream: AsyncGenerator[bytes | AckableMessage, None]) -> AckableMessage:
    message = await asyncio.wait_for(stream.__anext__(), 2)
    assert isinstance(message, AckableMessage)
    return message


async def test_poller_survives_receive_budget_error() -> None:
    """Finding 16: a non-Redis error in `receive_budget` used to end the queue's poller."""
    fake, hooks = FakeSQS(), ErrorCounter()
    calls = 0

    async def budget(_: SQSQueue) -> int:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise RuntimeError("limiter bug")
        return 10

    broker = _broker(fake, receive_budget=budget, hooks=hooks)
    fake.push(broker, "t1")
    async with listening(broker) as stream:
        await _next(stream)
    assert calls >= 3
    assert hooks.errors[:2] == [("poll", "other"), ("poll", "other")]


async def test_poller_survives_gate_error() -> None:
    """Finding 16: an exception from `gate.wait_open` must not kill the poller."""

    class FlakyGate:
        def __init__(self) -> None:
            self.calls = 0

        def is_open(self) -> bool:
            return self.calls >= 1

        async def wait_open(self) -> None:
            self.calls += 1
            raise ConnectionError("gate store down")

    fake, gate = FakeSQS(), FlakyGate()
    broker = _broker(fake, SQSQueue("q", wait_time_secs=0, gate=gate))
    fake.push(broker, "t1")
    async with listening(broker) as stream:
        await _next(stream)
    assert gate.calls == 1


async def test_accept_failure_releases_message_and_frees_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 16: an `_accept` crash leaked the slot and killed the poller; now it releases."""
    fake = FakeSQS()
    broker = _broker(fake)
    bad = fake.push(broker, "bad")
    fake.push(broker, "good")
    screen = broker._screen

    def flaky_screen(delivery: Delivery) -> tuple[str | None, str]:
        if delivery.receipt_handle == bad:
            raise ValueError("boom")
        return screen(delivery)

    monkeypatch.setattr(broker, "_screen", flaky_screen)
    async with listening(broker) as stream:
        message = await _next(stream)
        await message.ack()  # type: ignore[misc]
        await _quiesce(broker)
        assert broker.scheduler.held == 0
    assert (bad, 0) in fake.visibility


async def test_receive_budget_subtracts_buffered_messages() -> None:
    """DS4: received-but-not-started messages count against the limiter headroom."""
    fake = FakeSQS()

    async def budget(_: SQSQueue) -> int:
        return 3

    broker = _broker(fake, receive_budget=budget)
    for i in range(6):
        fake.push(broker, f"t{i}")
    async with listening(broker) as stream:
        await _next(stream)  # one of three leaves the buffer; headroom now 3 - 2 = 1
        await asyncio.sleep(0.4)
        assert fake.receives[0] == 3
        assert fake.receives[1] == 1
        assert fake.inbox  # nothing hoarded beyond the headroom


async def test_sqs_client_uses_one_quick_botocore_retry() -> None:
    """Finding 18: adaptive x 5 attempts under the Acker's 3 retries stacked to ~15 calls."""
    fake = FakeSQS()
    broker = _broker(fake)
    await broker.startup()
    try:
        retries = fake.config.retries
        assert retries["mode"] == "standard"
        assert retries["total_max_attempts"] == 2
    finally:
        await broker.shutdown()


async def test_after_ack_runs_only_when_acked_not_when_released() -> None:
    """Finding 13: deletion hooks must wait for a real ack; a released message keeps them."""
    fake = FakeSQS()
    broker = _broker(fake)
    fake.push(broker, "acked")
    fake.push(broker, "released")
    ran: list[str] = []
    async with listening(broker) as stream:
        first, second = await _next(stream), await _next(stream)
        acked = broker.delivery_for_task("acked")
        released = broker.delivery_for_task("released")
        assert acked is not None
        assert released is not None

        async def mark_acked() -> None:
            ran.append("acked")

        async def mark_released() -> None:
            ran.append("released")

        acked.after_ack.append(mark_acked)
        released.after_ack.append(mark_released)
        assert await broker.release(released, visibility_secs=5)
        await second.ack()  # type: ignore[misc]  # taskiq still acks after the release
        await first.ack()  # type: ignore[misc]
        await asyncio.sleep(0.1)
        assert broker.delivery_for_task("acked") is None
    assert ran == ["acked"]
    assert (acked.outcome, released.outcome) == ("acked", "released")


async def test_discard_acks_without_running() -> None:
    """Finding 13: `discard` finishes a delivery (slot freed, message deleted)."""
    fake = FakeSQS()
    broker = _broker(fake)
    handle = fake.push(broker, "poison")
    async with listening(broker) as stream:
        await _next(stream)
        assert await broker.discard(handle)
        assert not await broker.discard(handle)
        await _quiesce(broker)
        assert broker.scheduler.held == 0
    assert fake.deleted == [handle]


def _delivery(expires_at: float | None) -> Delivery:
    return Delivery(
        queue=SQSQueue("q"),
        queue_url="url",
        message_id="m",
        receipt_handle="h",
        body="{}",
        receive_count=1,
        received_at=0.0,
        visibility_secs=3,
        expires_at=expires_at,
    )


async def test_heartbeat_stops_extending_past_deadline_plus_grace() -> None:
    """DS6: with `max_extensions=None` a stuck delivery used to be extended forever."""
    fake = FakeSQS()
    wall = [1000.0]
    acker = Acker(fake, hooks=BrokerHooks(), visibility_flush_secs=0.01)  # type: ignore[arg-type]
    hb = HeartbeatManager(
        acker, Heartbeat(deadline_grace_secs=10), hooks=BrokerHooks(), wall_clock=lambda: wall[0]
    )
    d = _delivery(expires_at=1005.0)
    hb.track(d)
    assert hb.beat(1.0) == 1  # before the deadline
    wall[0] = 1014.0
    assert hb.beat(2.0) == 1  # inside the grace
    wall[0] = 1016.0
    assert hb.beat(3.0) == 0  # past deadline + grace: untracked, visibility left to lapse
    assert hb.tracked == 0
    await acker.close()


async def test_heartbeat_without_expiry_is_unbounded_by_default() -> None:
    """DS6: deliveries without an `expiry` keep the previous behaviour."""
    acker = Acker(FakeSQS(), hooks=BrokerHooks())  # type: ignore[arg-type]
    hb = HeartbeatManager(acker, Heartbeat(), hooks=BrokerHooks(), wall_clock=lambda: 1e12)
    hb.track(_delivery(expires_at=None))
    assert hb.beat(1.0) == 1
    await acker.close()


async def test_broker_records_expiry_on_delivery() -> None:
    """DS6: the broker hands the message's `expiry` to the heartbeat via `Delivery`."""
    fake = FakeSQS()
    broker = _broker(fake)
    deadline = time.time() + 600
    fake.push(broker, "t1", expiry=deadline)
    async with listening(broker) as stream:
        await _next(stream)
        delivery = broker.delivery_for_task("t1")
        assert delivery is not None
        assert delivery.expires_at == pytest.approx(deadline)


class FakeS3:
    """Dict-backed S3 subset; `fail_get` raises a non-missing error (an S3 blip)."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_get = False

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if self.fail_get:
            raise ClientError({"Error": {"Code": "InternalError"}}, "GetObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": _Body(self.objects[Key])}

    async def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop(Key, None)

    async def head_bucket(self, *, Bucket: str) -> None:
        return None


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aenter__(self) -> _Body:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def read(self) -> bytes:
        return self._data


_KEY = "taskiq-offload/t1.json"
_OK = TaskiqResult[Any](is_err=False, return_value=None, execution_time=0.0)


def _offload_setup(
    with_object: bool = True,
) -> tuple[FakeSQS, FakeS3, HybridSQSBroker, S3OffloadMiddleware, str]:
    fake, s3 = FakeSQS(), FakeS3()
    broker = _broker(fake)
    middleware = S3OffloadMiddleware("bucket", client=s3)  # type: ignore[arg-type]
    broker.add_middlewares(middleware)
    if with_object:
        s3.objects[_KEY] = encode_payload(["big"], {})
    handle = fake.push(broker, "t1", labels={OFFLOAD_LABEL: _KEY})
    return fake, s3, broker, middleware, handle


def _parse(broker: HybridSQSBroker, message: AckableMessage) -> TaskiqMessage:
    return broker.formatter.loads(message.data)


async def test_offload_kept_when_task_releases_message() -> None:
    """Finding 13: post_execute deleted the object on DEFER / release; redelivery then failed."""
    _, s3, broker, middleware, handle = _offload_setup()
    async with listening(broker) as stream:
        raw = await _next(stream)
        msg = await middleware.pre_execute(_parse(broker, raw))
        assert msg.args == ["big"]
        assert await broker.release(handle, visibility_secs=20)  # what DEFER does
        await middleware.post_execute(msg, _OK)
        await raw.ack()  # type: ignore[misc]  # taskiq acks anyway; a no-op after release
        await asyncio.sleep(0.05)
    assert _KEY in s3.objects


async def test_offload_deleted_only_after_ack_is_confirmed() -> None:
    """Finding 13: the object goes only once SQS confirmed the delete of the message."""
    fake, s3, broker, middleware, handle = _offload_setup()
    async with listening(broker) as stream:
        raw = await _next(stream)
        msg = await middleware.pre_execute(_parse(broker, raw))
        await middleware.post_execute(msg, _OK)
        assert _KEY in s3.objects  # not acked yet
        await raw.ack()  # type: ignore[misc]
        await asyncio.sleep(0.1)
        assert fake.deleted == [handle]
        assert _KEY not in s3.objects


async def test_missing_offload_object_finishes_delivery() -> None:
    """Finding 13: pre_execute raised outside taskiq's ack path, leaking slot and heartbeat."""
    fake, _, broker, middleware, handle = _offload_setup(with_object=False)
    async with listening(broker) as stream:
        raw = await _next(stream)
        with pytest.raises(OffloadedPayloadMissingError):
            await middleware.pre_execute(_parse(broker, raw))
        assert broker.delivery_for_task("t1") is None
        await _quiesce(broker)
        assert broker.scheduler.held == 0
    assert fake.deleted == [handle]


async def test_offload_read_failure_releases_delivery() -> None:
    """Finding 13: a transient S3 error releases the message for a later retry, not a leak."""
    fake, s3, broker, middleware, handle = _offload_setup()
    s3.fail_get = True
    async with listening(broker) as stream:
        raw = await _next(stream)
        with pytest.raises(ClientError):
            await middleware.pre_execute(_parse(broker, raw))
        assert broker.delivery_for_task("t1") is None
    assert (handle, 30) in fake.visibility
    assert fake.deleted == []
    assert _KEY in s3.objects
