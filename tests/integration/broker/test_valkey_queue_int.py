"""`ValkeyQueueClient` against the real Valkey of the Docker stack (plan V1).

The client must behave like SQS where the broker depends on it: visibility and redelivery,
receipt-handle generations, delay, long polls, DLQ after `maxReceiveCount`, batch partial
failures and the same error codes. Skipped when Valkey is not reachable.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import redis.asyncio
from botocore.exceptions import ClientError
from redis.exceptions import RedisError

from taskiq_llm_queue.broker import ErrorFamily, classify_error
from taskiq_llm_queue.broker.valkey_queue import ValkeyQueueClient, queue_url

pytestmark = pytest.mark.integration

HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "localhost")
PORT = int(os.environ.get("LLMQ_TEST_REDIS_PORT", "6379"))
PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")


@pytest.fixture
async def client() -> AsyncIterator[ValkeyQueueClient]:
    """A client under a unique prefix; every key under it is deleted afterwards."""
    r = redis.asyncio.Redis(host=HOST, port=PORT, password=PASSWORD)
    try:
        await r.ping()
    except (RedisError, OSError) as exc:
        await r.aclose()
        pytest.skip(f"Valkey not reachable: {exc}")
    prefix = f"llmqtest:{uuid.uuid4().hex[:10]}"
    c = ValkeyQueueClient(r, prefix=prefix)
    yield c
    await c.close()
    found = [k async for k in r.scan_iter(match=f"*{prefix}*", count=500)]
    if found:
        await r.delete(*found)
    await r.aclose()


async def _queue(client: ValkeyQueueClient, visibility: int = 30, receives: int = 0) -> str:
    attrs = {"VisibilityTimeout": str(visibility)}
    if receives:
        attrs["RedrivePolicy"] = json.dumps(
            {"maxReceiveCount": receives, "deadLetterTargetArn": "arn:aws:sqs:x:1:q-dlq"}
        )
    await client.create_queue(QueueName="q-dlq")
    created = await client.create_queue(QueueName="q", Attributes=attrs)
    return str(created["QueueUrl"])


async def _receive(
    client: ValkeyQueueClient, url: str, n: int = 10, wait: int = 0
) -> list[dict[str, Any]]:
    resp = await client.receive_message(QueueUrl=url, MaxNumberOfMessages=n, WaitTimeSeconds=wait)
    messages: list[dict[str, Any]] = resp.get("Messages", [])
    return messages


async def test_missing_queue_is_queue_does_not_exist(client: ValkeyQueueClient) -> None:
    with pytest.raises(ClientError) as info:
        await client.get_queue_url(QueueName="nope")
    assert classify_error(info.value) is ErrorFamily.NOT_FOUND
    with pytest.raises(ClientError) as info:
        await client.send_message(QueueUrl=queue_url("nope"), MessageBody="x")
    assert classify_error(info.value) is ErrorFamily.NOT_FOUND


async def test_send_receive_delete_round_trip(client: ValkeyQueueClient) -> None:
    url = await _queue(client)
    attrs = {"expiry": {"DataType": "Number", "StringValue": "123.5"}}
    sent = await client.send_message(QueueUrl=url, MessageBody="hello", MessageAttributes=attrs)
    [msg] = await _receive(client, url)
    assert msg["MessageId"] == sent["MessageId"]
    assert msg["Body"] == "hello"
    assert msg["MessageAttributes"] == attrs
    assert msg["Attributes"]["ApproximateReceiveCount"] == "1"
    assert abs(int(msg["Attributes"]["SentTimestamp"]) / 1000 - time.time()) < 5
    assert await _receive(client, url) == []  # in flight: invisible
    await client.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
    stats = (await client.get_queue_attributes(QueueUrl=url))["Attributes"]
    assert (
        stats["ApproximateNumberOfMessages"],
        stats["ApproximateNumberOfMessagesNotVisible"],
    ) == ("0", "0")


async def test_visibility_timeout_redelivers_and_old_handle_goes_stale(
    client: ValkeyQueueClient,
) -> None:
    url = await _queue(client, visibility=1)
    await client.send_message(QueueUrl=url, MessageBody="m")
    [first] = await _receive(client, url)
    await asyncio.sleep(1.2)
    [second] = await _receive(client, url)
    assert second["Attributes"]["ApproximateReceiveCount"] == "2"
    assert second["ReceiptHandle"] != first["ReceiptHandle"]
    for call in (
        client.delete_message(QueueUrl=url, ReceiptHandle=first["ReceiptHandle"]),
        client.change_message_visibility(
            QueueUrl=url, ReceiptHandle=first["ReceiptHandle"], VisibilityTimeout=5
        ),
    ):
        with pytest.raises(ClientError) as info:
            await call
        assert classify_error(info.value) is ErrorFamily.INVALID_HANDLE
    await client.delete_message(QueueUrl=url, ReceiptHandle=second["ReceiptHandle"])


async def test_heartbeat_extends_and_zero_releases_at_once(client: ValkeyQueueClient) -> None:
    url = await _queue(client, visibility=1)
    await client.send_message(QueueUrl=url, MessageBody="m")
    [msg] = await _receive(client, url)
    await client.change_message_visibility(
        QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=5
    )
    await asyncio.sleep(1.3)
    assert await _receive(client, url) == []  # extended past the 1 s default
    await client.change_message_visibility(
        QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=0
    )
    [again] = await _receive(client, url)
    assert again["Attributes"]["ApproximateReceiveCount"] == "2"


async def test_lapsed_visibility_is_not_in_flight(client: ValkeyQueueClient) -> None:
    """A heartbeat after the timeout lapsed must fail (another pod may take the message)."""
    url = await _queue(client, visibility=1)
    await client.send_message(QueueUrl=url, MessageBody="m")
    [msg] = await _receive(client, url)
    await asyncio.sleep(1.2)
    with pytest.raises(ClientError) as info:
        await client.change_message_visibility(
            QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=5
        )
    assert info.value.response["Error"]["Code"] == "MessageNotInflight"


async def test_delay_seconds(client: ValkeyQueueClient) -> None:
    url = await _queue(client)
    await client.send_message(QueueUrl=url, MessageBody="later", DelaySeconds=1)
    assert await _receive(client, url) == []
    assert (await client.get_queue_attributes(QueueUrl=url))["Attributes"][
        "ApproximateNumberOfMessagesDelayed"
    ] == "1"
    t0 = time.monotonic()
    [msg] = await _receive(client, url, wait=3)
    assert msg["Body"] == "later"
    assert 0.5 < time.monotonic() - t0 < 2.5


async def test_long_poll_wakes_on_send_and_times_out_when_idle(client: ValkeyQueueClient) -> None:
    url = await _queue(client)
    t0 = time.monotonic()
    assert await _receive(client, url, wait=1) == []
    assert 0.9 < time.monotonic() - t0 < 1.6

    async def later() -> None:
        await asyncio.sleep(0.3)
        await client.send_message(QueueUrl=url, MessageBody="ping")

    task = asyncio.create_task(later())
    t0 = time.monotonic()
    [msg] = await _receive(client, url, wait=10)
    await task
    assert msg["Body"] == "ping"
    assert time.monotonic() - t0 < 0.9  # woken, not the 1 s re-check


async def test_dead_letter_after_max_receives_and_redrive(client: ValkeyQueueClient) -> None:
    url = await _queue(client, visibility=1, receives=2)
    await client.send_message(QueueUrl=url, MessageBody="poison")
    for _ in range(2):
        assert len(await _receive(client, url)) == 1
        await asyncio.sleep(1.1)
    assert await _receive(client, url) == []  # third receive moved it to the DLQ
    dlq = (await client.get_queue_attributes(QueueUrl=queue_url("q-dlq")))["Attributes"]
    assert dlq["ApproximateNumberOfMessages"] == "1"
    [dead] = await client.dead_letters("q")
    assert dead["body"] == "poison"
    assert await client.redrive("q") == 1
    [msg] = await _receive(client, url)
    assert msg["Body"] == "poison"
    assert msg["Attributes"]["ApproximateReceiveCount"] == "1"


async def test_batches_fail_per_entry(client: ValkeyQueueClient) -> None:
    url = await _queue(client)
    resp = await client.send_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "0", "MessageBody": "a"},
            {"Id": "1", "MessageBody": "x" * (1_048_576 + 1)},
            {"Id": "2", "MessageBody": "c"},
        ],
    )
    assert [e["Id"] for e in resp["Successful"]] == ["0", "2"]
    [failed] = resp["Failed"]
    assert (failed["Id"], failed["Code"], failed["SenderFault"]) == ("1", "MessageTooLong", True)
    msgs = await _receive(client, url)
    entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)]
    entries.append({"Id": "bad", "ReceiptHandle": "not-a-handle"})
    resp = await client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp["Successful"]) == 2
    assert resp["Failed"][0]["Code"] == "ReceiptHandleIsInvalid"


async def test_fifo_is_refused(client: ValkeyQueueClient) -> None:
    with pytest.raises(ClientError):
        await client.create_queue(QueueName="q.fifo")
    url = await _queue(client)
    with pytest.raises(ClientError):
        await client.send_message(QueueUrl=url, MessageBody="x", MessageGroupId="g")


async def test_receive_returns_at_most_n_and_respects_priority_of_redelivery(
    client: ValkeyQueueClient,
) -> None:
    """Redeliveries come before new messages; a receive never returns more than asked."""
    url = await _queue(client, visibility=1)
    await client.send_message(QueueUrl=url, MessageBody="old")
    [old] = await _receive(client, url)
    for i in range(5):
        await client.send_message(QueueUrl=url, MessageBody=f"new{i}")
    await client.change_message_visibility(
        QueueUrl=url, ReceiptHandle=old["ReceiptHandle"], VisibilityTimeout=0
    )
    got = await _receive(client, url, n=3)
    assert [m["Body"] for m in got] == ["old", "new0", "new1"]


async def test_depth_reports_counts_and_oldest_age(client: ValkeyQueueClient) -> None:
    """The gauges that replace CloudWatch's queue metrics on the Valkey backend."""
    url = await _queue(client)
    assert (await client.depth("q"))["oldest_secs"] == 0.0
    await client.send_message(QueueUrl=url, MessageBody="a")
    await client.send_message(QueueUrl=url, MessageBody="b")
    await client.send_message(QueueUrl=url, MessageBody="later", DelaySeconds=60)
    await asyncio.sleep(0.3)
    await _receive(client, url, n=1)
    depth = await client.depth("q")
    assert (depth["visible"], depth["in_flight"], depth["delayed"], depth["dead"]) == (1, 1, 1, 0)
    assert 0.2 < depth["oldest_secs"] < 5


async def test_queue_wiped_by_a_restart_is_recreated(client: ValkeyQueueClient) -> None:
    """S21: Valkey restarted without persistence: every key is gone. A client that knew the
    queue re-creates it (attributes, stream, group) and keeps serving; the messages are lost."""
    url = await _queue(client, visibility=7, receives=3)
    await client.get_queue_url(QueueName="q")
    redis = client._redis
    wiped = [k async for k in redis.scan_iter(match="*:q:q}*", count=500)]
    assert wiped
    await redis.delete(*wiped)
    await client.send_message(QueueUrl=url, MessageBody="after")
    [msg] = await _receive(client, url)
    assert msg["Body"] == "after"
    attrs = (await client.get_queue_attributes(QueueUrl=url))["Attributes"]
    assert attrs["VisibilityTimeout"] == "7"
    assert json.loads(attrs["RedrivePolicy"])["maxReceiveCount"] == 3
    await redis.delete(*[k async for k in redis.scan_iter(match="*:q:q}:s", count=10)])
    assert await _receive(client, url) == []  # stream and group gone, cfg kept: NOGROUP healed
