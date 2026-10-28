"""Send path through an injected fake session: routing, labels, batching and size splits."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from botocore.exceptions import ClientError
from taskiq import BrokerMessage

from taskiq_llm_queue.broker import (
    Batching,
    HybridSQSBroker,
    InvalidLabelError,
    MessageTooLargeError,
    SQSQueue,
    SQSSendError,
)
from taskiq_llm_queue.errors import QueueUnavailable


class FakeClient:
    """Fake SQS client; batches whose body total exceeds `batch_limit` fail like ElasticMQ."""

    def __init__(self, batch_limit: int = 1_048_576) -> None:
        self.sent: list[dict[str, Any]] = []
        self.batches: list[int] = []
        self.batch_limit = batch_limit
        self.fail_send = False

    async def __aenter__(self) -> FakeClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get_queue_url(self, QueueName: str) -> dict[str, str]:
        return {"QueueUrl": f"http://fake/000/{QueueName}"}

    async def get_queue_attributes(self, **_: Any) -> dict[str, Any]:
        return {"Attributes": {"VisibilityTimeout": "30"}}

    async def send_message(self, QueueUrl: str, **entry: Any) -> dict[str, str]:
        if self.fail_send:
            denied: Any = {"Error": {"Code": "AccessDenied", "Message": "no"}}
            raise ClientError(denied, "SendMessage")
        self.sent.append(entry)
        return {"MessageId": f"m{len(self.sent)}"}

    async def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        if sum(len(e["MessageBody"]) for e in Entries) > self.batch_limit:
            too_long: Any = {
                "Error": {"Code": "InvalidAttributeValue", "Message": "MessageTooLong"}
            }
            raise ClientError(too_long, "SendMessageBatch")
        self.batches.append(len(Entries))
        for e in Entries:
            self.sent.append({k: v for k, v in e.items() if k != "Id"})
        return {"Successful": [{"Id": e["Id"], "MessageId": "x"} for e in Entries], "Failed": []}


class FakeSession:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def create_client(self, *_: Any, **__: Any) -> FakeClient:
        return self.client


def _broker(client: FakeClient, queues: list[SQSQueue], **kwargs: Any) -> HybridSQSBroker:
    return HybridSQSBroker(queues, session=FakeSession(client), **kwargs)  # type: ignore[arg-type]


def _msg(
    labels: dict[str, Any] | None = None, body: bytes = b"{}", task_id: str = "t1"
) -> BrokerMessage:
    return BrokerMessage(task_id=task_id, task_name="mod:task", message=body, labels=labels or {})


async def test_kick_before_startup_raises() -> None:
    broker = _broker(FakeClient(), [SQSQueue("a")])
    with pytest.raises(QueueUnavailable):
        await broker.kick(_msg())


async def test_routes_by_queue_name_label_and_default() -> None:
    client = FakeClient()
    broker = _broker(client, [SQSQueue("a"), SQSQueue("b", priority=1)], default_queue="b")
    await broker.startup()
    await broker.kick(_msg({"queue_name": "a"}))
    await broker.kick(_msg())
    with pytest.raises(InvalidLabelError):
        await broker.kick(_msg({"queue_name": "nope"}))
    assert len(client.sent) == 2
    await broker.shutdown()


async def test_labels_map_to_sqs_fields() -> None:
    client = FakeClient()
    broker = _broker(
        client,
        [SQSQueue("std", is_fair=True), SQSQueue("f.fifo")],
        use_task_id_for_deduplication=True,
    )
    await broker.startup()
    expiry = time.time() + 60
    await broker.kick(
        _msg({"queue_name": "std", "delay": "30", "expiry": str(expiry), "group_id": "g"})
    )
    await broker.kick(_msg({"queue_name": "f.fifo"}, task_id="tid"))
    await broker.kick(_msg({"queue_name": "f.fifo", "deduplication_id": "d", "group_id": "g2"}))
    std, fifo_default, fifo_labels = client.sent
    assert std["DelaySeconds"] == 30
    assert std["MessageGroupId"] == "g"  # fair queue (P7)
    assert float(std["MessageAttributes"]["expiry"]["StringValue"]) == pytest.approx(expiry)
    assert fifo_default["MessageGroupId"] == "mod:task"
    assert fifo_default["MessageDeduplicationId"] == "tid"
    assert (fifo_labels["MessageGroupId"], fifo_labels["MessageDeduplicationId"]) == ("g2", "d")
    await broker.shutdown()


@pytest.mark.parametrize(
    ("queue", "labels"),
    [
        ("std", {"delay": "901"}),
        ("std", {"delay": "-1"}),
        ("std", {"delay": "soon"}),
        ("f.fifo", {"delay": "5"}),  # DS19
        ("f.fifo", {"group_id": "x" * 129}),
        ("std", {"expiry": "tomorrow"}),
        ("std", {"expiry": "-5"}),
    ],
)
async def test_invalid_labels_rejected(queue: str, labels: dict[str, str]) -> None:
    client = FakeClient()
    broker = _broker(client, [SQSQueue("std"), SQSQueue("f.fifo")])
    await broker.startup()
    with pytest.raises(InvalidLabelError):
        await broker.kick(_msg({"queue_name": queue, **labels}))
    assert client.sent == []
    await broker.shutdown()


async def test_oversized_message_rejected_before_send() -> None:
    client = FakeClient()
    broker = _broker(client, [SQSQueue("a")])
    await broker.startup()
    with pytest.raises(MessageTooLargeError):
        await broker.kick(_msg(body=b"x" * (1_048_576 + 1)))
    await broker.shutdown()


async def test_batched_send_splits_by_bytes_and_halves_on_size_error() -> None:
    """DS15: byte-aware flush first; if SQS still refuses, the batch is halved and resent."""
    client = FakeClient(batch_limit=500_000)
    broker = _broker(client, [SQSQueue("a", batching=Batching(timeout_secs=0.05))])
    await broker.startup()
    body = b"x" * 200_000
    await asyncio.gather(*(broker.kick(_msg(body=body)) for _ in range(6)))
    assert len(client.sent) == 6
    assert sum(client.batches) == 6
    assert max(client.batches) <= 2
    await broker.shutdown()


async def test_skip_batching_label_and_task_list() -> None:
    client = FakeClient()
    broker = _broker(
        client, [SQSQueue("a", batching=Batching(timeout_secs=0.05))], skip_batch_tasks=["mod:task"]
    )
    await broker.startup()
    await broker.kick(_msg())
    assert client.batches == []
    assert len(client.sent) == 1
    await broker.shutdown()


async def test_send_failure_raises_typed_error() -> None:
    client = FakeClient()
    client.fail_send = True
    broker = _broker(client, [SQSQueue("a")])
    await broker.startup()
    with pytest.raises(SQSSendError) as info:
        await broker.kick(_msg())
    assert info.value.family.value == "other"
    assert isinstance(info.value, QueueUnavailable)
    await broker.shutdown()
