"""Broker integration tests on the real ElasticMQ container (docker/compose.yaml).

Covers both directions: kick -> listen -> ack with taskiq's own Receiver in-process,
priority, capacity, gates, long polls, heartbeat, release, delay, expiry, FIFO, batching
and DS9 screening. Each test creates uniquely named queues and deletes them afterwards;
the module is skipped when nothing answers at `LLMQ_TEST_SQS_ENDPOINT`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from taskiq import AckableMessage, Context, TaskiqDepends
from taskiq.exceptions import SendTaskError
from taskiq.receiver import Receiver

from taskiq_llm_queue.broker import (
    SQS_MESSAGE_ID_LABEL,
    SQS_QUEUE_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
    SQS_RECEIVE_COUNT_LABEL,
    Batching,
    BrokerConfigError,
    BrokerHooks,
    Delivery,
    ErrorFamily,
    Heartbeat,
    HybridSQSBroker,
    InvalidLabelError,
    SQSQueue,
    classify_error,
)
from taskiq_llm_queue.envelope import ENVELOPE_VERSION

pytestmark = pytest.mark.integration
MakeQueue = Callable[..., Awaitable[str]]

ENDPOINT = os.environ.get("LLMQ_TEST_SQS_ENDPOINT", "http://localhost:9324")
REGION = "ap-south-1"
KEY = "test"


class Recorder(BrokerHooks):
    """Hooks that keep every event, for assertions."""

    def __init__(self) -> None:
        self.receives: list[tuple[str, int]] = []
        self.acks: list[tuple[str, int]] = []
        self.heartbeats: list[tuple[str, int, int]] = []
        self.releases: list[tuple[str, int, str]] = []
        self.expired: list[tuple[str, int]] = []
        self.sends: list[tuple[str, int]] = []
        self.waits: list[float] = []

    def on_receive(self, queue: str, count: int) -> None:
        self.receives.append((queue, count))

    def on_queue_wait(self, queue: str, wait_secs: float) -> None:
        self.waits.append(wait_secs)

    def on_ack(self, queue: str, count: int) -> None:
        self.acks.append((queue, count))

    def on_heartbeat(self, queue: str, extended: int, lost: int) -> None:
        self.heartbeats.append((queue, extended, lost))

    def on_release(self, queue: str, count: int, reason: str) -> None:
        self.releases.append((queue, count, reason))

    def on_expired(self, queue: str, count: int) -> None:
        self.expired.append((queue, count))

    def on_send(self, queue: str, count: int) -> None:
        self.sends.append((queue, count))


def make_broker(queues: list[SQSQueue], **kwargs: Any) -> HybridSQSBroker:
    """Broker wired to the test endpoint with short backoff."""
    kwargs.setdefault("backoff_base_secs", 0.05)
    kwargs.setdefault("backoff_cap_secs", 0.5)
    return HybridSQSBroker(
        queues,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=KEY,
        **kwargs,
    )


@contextlib.asynccontextmanager
async def running(broker: HybridSQSBroker, **receiver_kwargs: Any) -> AsyncIterator[Receiver]:
    """Run taskiq's real Receiver in-process; stop it and shut the broker down on exit."""
    receiver_kwargs.setdefault("max_async_tasks", 20)
    receiver_kwargs.setdefault("wait_tasks_timeout", 2)
    receiver = Receiver(broker, run_startup=False, **receiver_kwargs)
    finish = asyncio.Event()
    await broker.startup()
    task = asyncio.create_task(receiver.listen(finish))
    try:
        yield receiver
    finally:
        finish.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 30)
        await broker.shutdown()


async def counts(sqs: Any, name: str) -> tuple[int, int]:
    """(visible, in-flight) approximate message counts of one queue."""
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


async def wait_for(predicate: Callable[[], bool], within: float = 10.0) -> None:
    """Poll a condition every 20 ms; fail the test on timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


@pytest.fixture
async def sqs() -> AsyncIterator[Any]:
    """Raw SQS client for setup and out-of-band checks; skips if ElasticMQ is down."""
    session = get_session()
    async with session.create_client(
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
        yield client


@pytest.fixture
async def make_queue(sqs: Any) -> AsyncIterator[Callable[..., Awaitable[str]]]:
    """Create a uniquely named queue (name returned); every created queue is deleted after."""
    urls: list[str] = []

    async def create(prefix: str = "t", *, fifo: bool = False, **attributes: str) -> str:
        name = f"llmq-ws1-{prefix}-{uuid.uuid4().hex[:8]}" + (".fifo" if fifo else "")
        attrs = dict(attributes)
        if fifo:
            attrs["FifoQueue"] = "true"
        resp = await sqs.create_queue(QueueName=name, Attributes=attrs)
        urls.append(resp["QueueUrl"])
        return name

    yield create
    for url in urls:
        with contextlib.suppress(Exception):
            await sqs.delete_queue(QueueUrl=url)


async def test_kick_listen_ack_round_trip(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("rt")
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], hooks=hooks)
    done: list[int] = []

    @broker.task(task_name="ws1.echo")
    async def echo(x: int) -> int:
        done.append(x)
        return x * 2

    async with running(broker):
        for i in range(3):
            await echo.kiq(i)
        await wait_for(lambda: len(done) == 3)
        await wait_for(lambda: sum(c for _, c in hooks.acks) == 3)
    assert sorted(done) == [0, 1, 2]
    assert await counts(sqs, name) == (0, 0)
    assert len(hooks.waits) == 3
    assert all(0 <= w < 10 for w in hooks.waits)


async def test_strict_priority_order(make_queue: MakeQueue) -> None:
    hi, lo = await make_queue("hi"), await make_queue("lo")
    broker = make_broker(
        [SQSQueue(hi, priority=0, wait_time_secs=1), SQSQueue(lo, priority=1, wait_time_secs=1)],
        max_inflight=2,
    )
    order: list[str] = []

    @broker.task(task_name="ws1.prio")
    async def prio(tag: str) -> None:
        order.append(tag)
        await asyncio.sleep(0.05)

    await broker.startup()
    for _ in range(5):
        await prio.kicker().with_labels(queue_name=lo).kiq("lo")
    for _ in range(5):
        await prio.kicker().with_labels(queue_name=hi).kiq("hi")
    async with running(broker):
        await wait_for(lambda: len(order) == 10, 20)
    assert order[:5] == ["hi"] * 5


async def test_capacity_aware_receive_never_exceeds_free_slots(
    sqs: Any, make_queue: MakeQueue
) -> None:
    name = await make_queue("cap")
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], max_inflight=3, hooks=hooks)
    gate = asyncio.Event()
    started: list[int] = []

    @broker.task(task_name="ws1.block")
    async def block(i: int) -> None:
        started.append(i)
        await gate.wait()

    await broker.startup()
    for i in range(10):
        await block.kiq(i)
    async with running(broker, max_async_tasks=10):
        await wait_for(lambda: len(started) == 3)
        await asyncio.sleep(1.5)  # pollers would have taken more by now if they could
        assert len(started) == 3
        assert broker.scheduler.held == 3
        assert sum(c for _, c in hooks.receives) == 3
        assert await counts(sqs, name) == (7, 3)
        gate.set()
        await wait_for(lambda: len(started) == 10)
    assert max(c for _, c in hooks.receives) <= 3


class ManualGate:
    def __init__(self) -> None:
        self.opened = asyncio.Event()

    def is_open(self) -> bool:
        return self.opened.is_set()

    async def wait_open(self) -> None:
        await self.opened.wait()


async def test_closed_gate_blocks_receive(make_queue: MakeQueue) -> None:
    name = await make_queue("gate")
    hooks = Recorder()
    gate = ManualGate()
    broker = make_broker([SQSQueue(name, wait_time_secs=1, gate=gate)], hooks=hooks)
    ran: list[int] = []

    @broker.task(task_name="ws1.gated")
    async def gated() -> None:
        ran.append(1)

    async with running(broker):
        await gated.kiq()
        await asyncio.sleep(1.5)
        assert ran == []
        assert hooks.receives == []  # not even an empty ReceiveMessage call
        gate.opened.set()
        await wait_for(lambda: ran == [1])


async def test_real_long_poll_waits_in_one_call_and_returns_on_arrival(
    make_queue: MakeQueue,
) -> None:
    """A3: an empty queue costs one ReceiveMessage per WaitTimeSeconds, not one per second."""
    name = await make_queue("lp")
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=3)], hooks=hooks, stop_grace_secs=0)
    ran: list[float] = []
    loop = asyncio.get_running_loop()

    @broker.task(task_name="ws1.lp")
    async def lp() -> None:
        ran.append(loop.time())

    async with running(broker):
        await asyncio.sleep(3.5)
        assert 1 <= len(hooks.receives) <= 2
        sent_at = loop.time()
        await lp.kiq()
        await wait_for(lambda: bool(ran), 5)
    assert ran[0] - sent_at < 1.0


async def test_heartbeat_keeps_message_invisible_and_acks_are_batched(
    sqs: Any, make_queue: MakeQueue
) -> None:
    name = await make_queue("hb", VisibilityTimeout="2")
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], hooks=hooks, heartbeat=Heartbeat())
    started = asyncio.Event()
    finished: list[int] = []

    @broker.task(task_name="ws1.slow")
    async def slow() -> None:
        started.set()
        await asyncio.sleep(5)
        finished.append(1)

    @broker.task(task_name="ws1.quick")
    async def quick(i: int) -> None:
        finished.append(i)

    async with running(broker):
        await slow.kiq()
        await asyncio.wait_for(started.wait(), 10)
        await asyncio.sleep(3)  # well past the 2 s visibility timeout
        stolen = await sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0, MaxNumberOfMessages=10)
        assert stolen.get("Messages", []) == []
        await asyncio.gather(*(quick.kiq(i) for i in range(10)))
        await wait_for(lambda: len(finished) == 11, 15)
        await wait_for(lambda: sum(c for _, c in hooks.acks) == 11)
    assert sum(e for _, e, _ in hooks.heartbeats) >= 3
    assert max(c for _, c in hooks.acks) > 1  # DeleteMessageBatch carried several acks
    assert await counts(sqs, name) == (0, 0)


async def _receive_all(sqs: Any, url: str, expected: int) -> int:
    got = 0
    for _ in range(5):
        resp = await sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1, MaxNumberOfMessages=10)
        got += len(resp.get("Messages", []))
        if got >= expected:
            break
    return got


async def test_release_on_stop_and_shutdown_makes_messages_visible_now(
    sqs: Any, make_queue: MakeQueue
) -> None:
    """A5: buffered messages released when listen stops, the running one on shutdown."""
    name = await make_queue("rel", VisibilityTimeout="60")
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], max_inflight=5, hooks=hooks)

    @broker.task(task_name="ws1.rel")
    async def rel(i: int) -> None:
        return None

    await broker.startup()
    for i in range(5):
        await rel.kiq(i)
    gen = broker.listen()
    first = await anext(gen)
    await wait_for(lambda: broker.scheduler.held == 5)
    await gen.aclose()
    assert "stopped" in {r for _, _, r in hooks.releases}
    assert sum(c for _, c, r in hooks.releases if r == "stopped") == 4
    assert await counts(sqs, name) == (4, 1)
    await broker.shutdown()
    assert sum(c for _, c, r in hooks.releases if r == "shutdown_running") == 1
    assert await _receive_all(sqs, url, 5) == 5
    assert first is not None


async def test_pause_receiving_releases_buffer_and_stops_polling(
    sqs: Any, make_queue: MakeQueue
) -> None:
    name = await make_queue("drain", VisibilityTimeout="60")
    hooks = Recorder()
    broker = make_broker(
        [SQSQueue(name, wait_time_secs=1)], max_inflight=4, hooks=hooks, stop_grace_secs=2
    )

    @broker.task(task_name="ws1.drain")
    async def drain(i: int) -> None:
        return None

    await broker.startup()
    for i in range(4):
        await drain.kiq(i)
    gen = broker.listen()
    message = await anext(gen)
    await wait_for(lambda: broker.scheduler.held == 4)
    await broker.pause_receiving()
    assert sum(c for _, c, r in hooks.releases if r == "drain") == 3
    receives = len(hooks.receives)
    await asyncio.sleep(2.5)
    assert len(hooks.receives) <= receives + 1  # at most the poll already in flight
    assert await counts(sqs, name) == (3, 1)
    await message.ack()  # type: ignore[union-attr, misc]
    await gen.aclose()
    await broker.shutdown()
    assert await counts(sqs, name) == (3, 0)


async def test_lost_handle_signals_and_metadata_labels(sqs: Any, make_queue: MakeQueue) -> None:
    """DS6 + P12: the task sees SQS metadata; deleting its handle elsewhere marks it lost."""
    name = await make_queue("lost", VisibilityTimeout="3")
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    lost: list[Delivery] = []
    broker = make_broker(
        [SQSQueue(name, wait_time_secs=1)], expose_message_metadata=True, on_lost=lost.append
    )
    seen: dict[str, Any] = {}
    release = asyncio.Event()

    @broker.task(task_name="ws1.meta")
    async def meta(context: Context = TaskiqDepends()) -> None:  # noqa: B008  # taskiq DI
        seen.update(context.message.labels)
        await release.wait()

    async with running(broker):
        await meta.kiq()
        await wait_for(lambda: bool(seen))
        assert seen[SQS_QUEUE_LABEL] == name
        assert seen[SQS_RECEIVE_COUNT_LABEL] == "1"
        delivery = broker.delivery(seen[SQS_MESSAGE_ID_LABEL])
        assert delivery is not None
        await sqs.delete_message(QueueUrl=url, ReceiptHandle=seen[SQS_RECEIPT_HANDLE_LABEL])
        await asyncio.wait_for(delivery.lost.wait(), 5)
        assert lost == [delivery]
        release.set()
        await asyncio.sleep(1)  # the ack of a lost message is tolerated
    assert await counts(sqs, name) == (0, 0)


async def test_manual_release_of_running_message(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("man", VisibilityTimeout="60")
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], expose_message_metadata=True)

    @broker.task(task_name="ws1.man")
    async def man() -> None:
        return None

    await broker.startup()
    await man.kiq()
    gen = broker.listen()
    message = await anext(gen)
    assert isinstance(message, AckableMessage)
    message_id = broker.formatter.loads(message.data).labels[SQS_MESSAGE_ID_LABEL]
    await gen.aclose()  # stop polling so the released message is not received again at once
    assert await broker.release(message_id) is True
    assert await broker.release(message_id) is False
    await message.ack()  # type: ignore[misc]  # no-op after release
    await broker.shutdown()
    assert await counts(sqs, name) == (1, 0)


async def test_delay_label_defers_delivery(make_queue: MakeQueue) -> None:
    name = await make_queue("delay")
    broker = make_broker([SQSQueue(name, wait_time_secs=1)])
    loop = asyncio.get_running_loop()
    ran: list[float] = []

    @broker.task(task_name="ws1.delayed")
    async def delayed() -> None:
        ran.append(loop.time())

    async with running(broker):
        sent_at = loop.time()
        await delayed.kicker().with_labels(delay=2).kiq()
        await wait_for(lambda: bool(ran), 10)
    assert ran[0] - sent_at >= 1.8


async def test_expired_messages_deleted_unrun(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("exp")
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], hooks=hooks)
    ran: list[str] = []

    @broker.task(task_name="ws1.exp")
    async def exp(tag: str) -> None:
        ran.append(tag)

    async with running(broker):
        await exp.kicker().with_labels(expiry=time.time() - 1).kiq("stale")
        await exp.kicker().with_labels(expiry=time.time() + 60).kiq("fresh")
        await wait_for(lambda: ran == ["fresh"] and bool(hooks.expired))
        await asyncio.sleep(1)  # let the batched delete flush
    assert ran == ["fresh"]
    assert hooks.expired == [(name, 1)]
    assert await counts(sqs, name) == (0, 0)


async def test_fifo_dedup_group_and_delay_rejected(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("fifo", fifo=True)
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    broker = make_broker([SQSQueue(name, wait_time_secs=1)])
    by_task_id = make_broker([SQSQueue(name, wait_time_secs=1)], use_task_id_for_deduplication=True)

    @broker.task(task_name="ws1.fifo")
    async def fifo(i: int) -> None:
        return None

    await broker.startup()
    await by_task_id.startup()
    for i in range(3):  # same dedup id -> one message
        await fifo.kicker().with_labels(deduplication_id="same", group_id="g").kiq(i)
    await fifo.kicker().with_labels(deduplication_id="other", group_id="g").kiq(9)
    await fifo.kicker().with_broker(by_task_id).kiq(10)  # task id as dedup id
    with pytest.raises(SendTaskError) as rejected:  # taskiq wraps kick errors
        await fifo.kicker().with_labels(delay=5, deduplication_id="d").kiq(11)
    assert isinstance(rejected.value.__cause__, InvalidLabelError)
    bodies: list[str] = []
    for _ in range(3):
        resp = await sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            MessageSystemAttributeNames=["MessageGroupId"],
        )
        for m in resp.get("Messages", []):
            bodies.append(m["Body"])
            await sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    assert len(bodies) == 3
    assert '"args":[0]' in bodies[0].replace(" ", "")  # first of the duplicates kept, in order
    await broker.shutdown()
    await by_task_id.shutdown()


async def test_byte_aware_batch_split(sqs: Any, make_queue: MakeQueue) -> None:
    """DS15: 6 x 200 KB go out as two SendMessageBatch calls (<= 900 KB each)."""
    name = await make_queue("batch")
    hooks = Recorder()
    broker = make_broker(
        [SQSQueue(name, wait_time_secs=1, batching=Batching(timeout_secs=0.2))], hooks=hooks
    )

    @broker.task(task_name="ws1.big")
    async def big(payload: str) -> None:
        return None

    await broker.startup()
    await asyncio.gather(*(big.kiq("x" * 200_000) for _ in range(6)))
    await broker.shutdown()
    assert sorted(c for _, c in hooks.sends) == [2, 4]
    assert await counts(sqs, name) == (6, 0)


async def test_unknown_task_released_not_acked(sqs: Any, make_queue: MakeQueue) -> None:
    """DS9: taskiq's Receiver would ack (lose) it; the broker releases it for 30 s instead."""
    name = await make_queue("unk")
    sender = make_broker([SQSQueue(name)])

    @sender.task(task_name="ws1.ghost")
    async def ghost() -> None:
        return None

    await sender.startup()
    await ghost.kiq()
    await sender.shutdown()

    hooks = Recorder()
    worker = make_broker([SQSQueue(name, wait_time_secs=1)], hooks=hooks, unknown_release_secs=2)

    @worker.task(task_name="ws1.known")
    async def known() -> None:
        return None

    async with running(worker):
        await wait_for(lambda: bool(hooks.releases))
        assert hooks.releases[0][2] == "unknown_task:ws1.ghost"
        assert await counts(sqs, name) == (0, 1)  # invisible, not deleted
        await wait_for(lambda: len(hooks.releases) >= 2, 6)  # back after 2 s, released again
    assert sum(await counts(sqs, name)) == 1
    assert hooks.acks == []


async def test_newer_envelope_released_not_run(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("env")
    hooks = Recorder()
    broker = make_broker([SQSQueue(name, wait_time_secs=1)], hooks=hooks)
    ran: list[dict[str, Any]] = []

    @broker.task(task_name="ws1.call")
    async def call(envelope: dict[str, Any]) -> None:
        ran.append(envelope)

    async with running(broker):
        await call.kiq({"v": ENVELOPE_VERSION + 1, "request_id": uuid.uuid4().hex})
        await call.kiq({"v": ENVELOPE_VERSION, "request_id": uuid.uuid4().hex})
        await wait_for(lambda: len(ran) == 1 and bool(hooks.releases))
    assert ran[0]["v"] == ENVELOPE_VERSION
    assert [r for _, _, r in hooks.releases] == ["unsupported_envelope"]
    assert sum(await counts(sqs, name)) == 1


async def test_classifier_on_real_elasticmq_errors(sqs: Any, make_queue: MakeQueue) -> None:
    name = await make_queue("err")
    url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    with pytest.raises(ClientError) as too_big:
        await sqs.send_message(QueueUrl=url, MessageBody="x" * (1_048_576 + 1))
    assert classify_error(too_big.value) is ErrorFamily.SIZE_LIMIT
    with pytest.raises(ClientError) as bad_handle:
        await sqs.delete_message(QueueUrl=url, ReceiptHandle="bogus")
    assert classify_error(bad_handle.value) is ErrorFamily.INVALID_HANDLE
    with pytest.raises(ClientError) as missing:
        await sqs.get_queue_url(QueueName=f"missing-{uuid.uuid4().hex}")
    assert classify_error(missing.value) is ErrorFamily.NOT_FOUND


async def test_startup_missing_queue_and_declare(sqs: Any) -> None:
    missing = f"llmq-ws1-missing-{uuid.uuid4().hex[:8]}"
    broker = make_broker([SQSQueue(missing)])
    with pytest.raises(BrokerConfigError):
        await broker.startup()
    declared = make_broker(
        [
            SQSQueue(
                missing,
                is_declare=True,
                visibility_timeout_secs=45,
                attributes={"DelaySeconds": "0"},
            )
        ]
    )
    await declared.startup()
    try:
        url = (await sqs.get_queue_url(QueueName=missing))["QueueUrl"]
        attrs = await sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "45"
    finally:
        await declared.shutdown()
        await sqs.delete_queue(QueueUrl=url)
