"""S3 offload through `HybridSQSBroker` and taskiq's real Receiver (ElasticMQ + moto S3).

Regression for finding 13: the offloaded object must survive a task that releases its
message (DEFER), and a message whose object is gone must be finished, not left in flight
with its slot held. Skipped when nothing answers at `LLMQ_TEST_SQS_ENDPOINT`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from taskiq import Context, TaskiqDepends
from taskiq.receiver import Receiver

from taskiq_llm_queue.broker import SQS_RECEIPT_HANDLE_LABEL, HybridSQSBroker, SQSQueue
from taskiq_llm_queue.broker.s3_offload import S3OffloadMiddleware

pytestmark = pytest.mark.integration

ENDPOINT = os.environ.get("LLMQ_TEST_SQS_ENDPOINT", "http://localhost:9324")
REGION = "ap-south-1"
KEY = "test"


@pytest.fixture
async def sqs_queue() -> AsyncIterator[tuple[Any, str]]:
    """A fresh uniquely named ElasticMQ queue and a raw client; skips if SQS is down."""
    async with get_session().create_client(
        "sqs",
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=AioConfig(retries={"max_attempts": 1}, connect_timeout=2, read_timeout=30),
        aws_access_key_id=KEY,
        aws_secret_access_key=KEY,
    ) as client:
        try:
            await client.list_queues()
        except Exception as exc:
            pytest.skip(f"no SQS endpoint at {ENDPOINT}: {exc}")
        name = f"llmq-it-s3off-{uuid.uuid4().hex[:8]}"
        url = (await client.create_queue(QueueName=name))["QueueUrl"]
        try:
            yield client, name
        finally:
            with contextlib.suppress(Exception):
                await client.delete_queue(QueueUrl=url)


async def _counts(sqs: Any, name: str) -> tuple[int, int]:
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    attrs = (
        await sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]), int(
        attrs["ApproximateNumberOfMessagesNotVisible"]
    )


async def _wait_for(predicate: Callable[[], bool], within: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


def _broker(name: str, bucket: str, s3_kwargs: dict[str, Any]) -> HybridSQSBroker:
    broker = HybridSQSBroker(
        [SQSQueue(name, wait_time_secs=1)],
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=KEY,
        expose_message_metadata=True,
        backoff_base_secs=0.05,
        backoff_cap_secs=0.5,
    )
    broker.add_middlewares(S3OffloadMiddleware(bucket, threshold_bytes=100, **s3_kwargs))
    return broker


@contextlib.asynccontextmanager
async def _running(broker: HybridSQSBroker) -> AsyncIterator[None]:
    receiver = Receiver(broker, run_startup=False, max_async_tasks=5, wait_tasks_timeout=2)
    finish = asyncio.Event()
    await broker.startup()
    task = asyncio.create_task(receiver.listen(finish))
    try:
        yield
    finally:
        finish.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 30)
        await broker.shutdown()


async def _keys(s3_client: Any, bucket: str) -> list[str]:
    response = await s3_client.list_objects_v2(Bucket=bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


async def test_deferred_task_keeps_offloaded_object_then_ack_deletes_it(
    sqs_queue: tuple[Any, str], s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    """Finding 13: a DEFER release used to delete the object, so the redelivery failed."""
    sqs, name = sqs_queue
    broker = _broker(name, bucket, s3_kwargs)
    runs: list[int] = []

    @broker.task(task_name="it.s3off.defer")
    async def defer_once(payload: str, context: Context = TaskiqDepends()) -> None:  # noqa: B008
        runs.append(len(payload))
        if len(runs) == 1:  # what the LLM task does on ClaimDecision.DEFER
            handle = context.message.labels[SQS_RECEIPT_HANDLE_LABEL]
            await broker.release(handle, visibility_secs=1, reason="task")

    async with _running(broker):
        await defer_once.kiq("x" * 500)
        await _wait_for(lambda: len(runs) == 2, 15)
        await _wait_for(lambda: broker.scheduler.held <= 1)  # an idle poll may hold one
        await asyncio.sleep(1.0)  # ack flush + after-ack delete
    assert runs == [500, 500]  # the redelivery could still read the payload
    assert await _keys(s3_client, bucket) == []
    assert await _counts(sqs, name) == (0, 0)


async def test_missing_offloaded_object_is_discarded_not_leaked(
    sqs_queue: tuple[Any, str], s3_kwargs: dict[str, Any], s3_client: Any, bucket: str
) -> None:
    """Finding 13: a missing object raised in pre_execute and left the message in flight."""
    sqs, name = sqs_queue
    broker = _broker(name, bucket, s3_kwargs)
    runs: list[int] = []

    @broker.task(task_name="it.s3off.gone")
    async def gone(payload: str) -> None:
        runs.append(len(payload))

    await broker.startup()
    await gone.kiq("y" * 500)
    for key in await _keys(s3_client, bucket):
        await s3_client.delete_object(Bucket=bucket, Key=key)
    async with _running(broker):
        await asyncio.sleep(2.5)
        assert broker.scheduler.held <= 1  # only an idle long poll, no leaked slot
    assert runs == []
    assert await _counts(sqs, name) == (0, 0)
