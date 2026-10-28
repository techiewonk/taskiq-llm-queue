"""`HybridSQSBroker`: a taskiq broker on Amazon SQS with priority, capacity and batching.

Feature parity with taskiq-aio-sqs and taskiq-sqs (multi-queue routing, FIFO / fair
queues, delay, expiry, batched send, heartbeat, metadata, declare) plus strict priority
across queues, capacity-aware real long polls, receive gates, batched ack / heartbeat and
release on shutdown (plan §7.2). Both upstream projects are MIT; this is our own code.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import math
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypedDict

from aiobotocore.config import AioConfig
from aiobotocore.session import AioSession, get_session
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from taskiq_llm_queue.broker.acker import Acker, VisibilityResult
from taskiq_llm_queue.broker.batching import MicroBatcher, message_bytes
from taskiq_llm_queue.broker.delivery import Delivery
from taskiq_llm_queue.broker.errors import (
    BrokerConfigError,
    ErrorFamily,
    InvalidLabelError,
    MessageTooLargeError,
    SQSSendError,
    backoff_delay,
    classify_code,
    classify_error,
    error_code,
)
from taskiq_llm_queue.broker.events import BrokerHooks, safe_call
from taskiq_llm_queue.broker.filters import MessageFilter, envelope_supported
from taskiq_llm_queue.broker.heartbeat import HeartbeatManager
from taskiq_llm_queue.broker.queue import (
    MAX_DELAY_SECS,
    MAX_FIFO_ID_CHARS,
    MAX_MESSAGE_BYTES,
    MAX_VISIBILITY_SECS,
    Heartbeat,
    SQSQueue,
)
from taskiq_llm_queue.broker.scheduler import SlotScheduler
from taskiq_llm_queue.errors import QueueUnavailable

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import MessageTypeDef

logger = logging.getLogger(__name__)

QUEUE_LABEL = "queue_name"
DELAY_LABEL = "delay"
GROUP_ID_LABEL = "group_id"
DEDUPLICATION_ID_LABEL = "deduplication_id"
EXPIRY_LABEL = "expiry"
SKIP_BATCHING_LABEL = "skip_batching"
EXPIRY_ATTRIBUTE = "expiry"
SQS_MESSAGE_ID_LABEL = "sqs_message_id"
SQS_RECEIPT_HANDLE_LABEL = "sqs_receipt_handle"
SQS_QUEUE_LABEL = "sqs_queue_name"
SQS_RECEIVE_COUNT_LABEL = "sqs_approximate_receive_count"
DEFAULT_HEARTBEAT = Heartbeat()

ReceiveBudget = Callable[[SQSQueue], int | Awaitable[int]]
_SendItem = tuple[dict[str, Any], int]


class _ClientKwargs(TypedDict):
    endpoint_url: str | None
    region_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None


class HybridSQSBroker(AsyncBroker):
    """taskiq broker over one or more SQS queues with strict priority (0 = highest).

    Capacity is `max_inflight` delivered-not-acked messages across all queues; pollers
    receive only into free slots (`MaxNumberOfMessages = min(10, free)`) with real long
    polls. `receive_budget(queue)` can cap a receive further (e.g. limiter headroom, DS4).
    Unknown tasks and newer envelopes are released, not acked (DS9).
    """

    def __init__(
        self,
        queues: Sequence[SQSQueue],
        *,
        default_queue: str | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        client_config: AioConfig | None = None,
        session: AioSession | None = None,
        max_inflight: int = 10,
        reserve_for_priority0: int = 0,
        min_slots_for_lower: int = 0,
        receive_budget: ReceiveBudget | None = None,
        heartbeat: Heartbeat | None = DEFAULT_HEARTBEAT,
        ack_flush_secs: float = 0.5,
        use_task_id_for_deduplication: bool = False,
        expose_message_metadata: bool = False,
        skip_batch_tasks: Iterable[str] = (),
        unknown_release_secs: int = 30,
        message_filter: MessageFilter | None = None,
        hooks: BrokerHooks | None = None,
        on_lost: Callable[[Delivery], None] | None = None,
        release_running_on_shutdown: bool = True,
        stop_grace_secs: float | None = None,
        backoff_base_secs: float = 0.5,
        backoff_cap_secs: float = 20.0,
    ) -> None:
        """Validate everything up front; no client or task is created until `startup()`."""
        super().__init__()
        errors = []
        if not queues:
            errors.append("at least one SQSQueue is required")
        names = [q.name for q in queues]
        if len(set(names)) != len(names):
            errors.append(f"queue names must be unique: {names}")
        if default_queue is not None and default_queue not in names:
            errors.append(f"default_queue {default_queue!r} is not one of {names}")
        if not 0 <= unknown_release_secs <= MAX_VISIBILITY_SECS:
            errors.append(f"unknown_release_secs must be 0..{MAX_VISIBILITY_SECS}")
        if not 0 < ack_flush_secs <= 1:
            errors.append("ack_flush_secs must be in (0, 1] (DS7)")
        if stop_grace_secs is not None and stop_grace_secs < 0:
            errors.append("stop_grace_secs must be >= 0")
        if errors:
            raise BrokerConfigError("; ".join(errors))
        self._queues = {q.name: q for q in queues}
        self._default_queue = default_queue or names[0]
        self._client_kwargs: _ClientKwargs = {
            "endpoint_url": endpoint_url,
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
        }
        self._client_config = client_config
        self._session = session
        self._scheduler = SlotScheduler(
            max_inflight,
            reserve_for_priority0=reserve_for_priority0,
            min_slots_for_lower=min_slots_for_lower,
        )
        self._receive_budget = receive_budget
        self._heartbeat_config = heartbeat
        self._ack_flush_secs = ack_flush_secs
        self._use_task_id_for_dedup = use_task_id_for_deduplication
        self._expose_metadata = expose_message_metadata
        self._skip_batch_tasks = frozenset(skip_batch_tasks)
        self._unknown_release_secs = unknown_release_secs
        self._message_filter = message_filter
        self.hooks = hooks or BrokerHooks()
        self._on_lost = on_lost
        self._release_running_on_shutdown = release_running_on_shutdown
        # A long poll cancelled client-side can still complete server-side and take a message
        # we release right after (seen on ElasticMQ), so by default polls finish naturally.
        self._stop_grace_secs = (
            max((q.wait_time_secs for q in queues), default=0) + 2.0
            if stop_grace_secs is None
            else stop_grace_secs
        )
        self._backoff_base_secs = backoff_base_secs
        self._backoff_cap_secs = backoff_cap_secs

        self._stack: contextlib.AsyncExitStack | None = None
        self._client: SQSClient | None = None
        self._urls: dict[str, str] = {}
        self._visibility: dict[str, int] = {}
        self._acker: Acker | None = None
        self._heartbeat: HeartbeatManager | None = None
        self._senders: dict[str, MicroBatcher[_SendItem, str]] = {}
        self._active: dict[str, Delivery] = {}
        self._buffer: asyncio.Queue[Delivery] | None = None
        self._pollers: dict[str, asyncio.Task[None]] = {}
        self._in_receive: set[str] = set()
        self._receiving = asyncio.Event()
        self._receiving.set()
        self._stopping = False

    # ------------------------------------------------------------------ lifecycle

    @property
    def queues(self) -> dict[str, SQSQueue]:
        """Configured queues by routing name."""
        return dict(self._queues)

    @property
    def scheduler(self) -> SlotScheduler:
        """Slot accounting (read `held`, `budget()`; `resize()` changes capacity live)."""
        return self._scheduler

    @property
    def is_started(self) -> bool:
        """True between `startup()` and `shutdown()`."""
        return self._client is not None

    async def startup(self) -> None:
        """Create the SQS client, resolve (or declare) queues, start batchers; idempotent."""
        if self._client is not None:
            return
        await super().startup()
        stack = contextlib.AsyncExitStack()
        base = AioConfig(
            retries={"mode": "adaptive", "max_attempts": 5},
            max_pool_connections=4 * len(self._queues) + 10,
            read_timeout=60,
        )
        config = base.merge(self._client_config) if self._client_config else base
        session = self._session or get_session()
        try:
            client: SQSClient = await stack.enter_async_context(
                session.create_client("sqs", config=config, **self._client_kwargs)
            )
            for queue in self._queues.values():
                url = await self._resolve_url(client, queue)
                self._urls[queue.name] = url
                self._visibility[queue.name] = await self._resolve_visibility(client, queue, url)
        except BaseException:
            await stack.aclose()
            raise
        self._stack = stack
        self._client = client
        self._stopping = False
        self._acker = Acker(
            client,
            hooks=self.hooks,
            ack_flush_secs=self._ack_flush_secs,
            visibility_flush_secs=(
                self._heartbeat_config.flush_secs if self._heartbeat_config else 0.5
            ),
        )
        if self._heartbeat_config is not None:
            self._heartbeat = HeartbeatManager(
                self._acker, self._heartbeat_config, hooks=self.hooks, on_lost=self._on_lost
            )
        for queue in self._queues.values():
            if queue.batching is not None:
                self._senders[queue.name] = MicroBatcher[_SendItem, str](
                    functools.partial(self._send_batch, queue, self._urls[queue.name]),
                    max_items=queue.batching.size,
                    max_bytes=queue.batching.max_bytes,
                    size_of=lambda item: item[1],
                    max_delay_secs=queue.batching.timeout_secs,
                    name=f"send:{queue.name}",
                )

    async def shutdown(self) -> None:
        """Stop pollers, release unstarted (and leftover running) messages, flush, close (A5)."""
        if self._client is None:
            await super().shutdown()
            return
        self._stopping = True
        self._receiving.set()
        await self._stop_pollers()
        await self._release_where(lambda d: not d.started, "shutdown")
        if self._release_running_on_shutdown:
            await self._release_where(lambda d: True, "shutdown_running")
        if self._heartbeat is not None:
            await self._heartbeat.stop()
        await asyncio.gather(*(s.close() for s in self._senders.values()))
        if self._acker is not None:
            await self._acker.close()
        if self._stack is not None:
            await self._stack.aclose()
        self._senders.clear()
        self._client = None
        self._stack = None
        self._acker = None
        self._heartbeat = None
        await super().shutdown()

    async def pause_receiving(self, *, release_buffered: bool = True) -> None:
        """Stop taking new messages (for a /drain endpoint); running tasks continue.

        Polls already in flight finish; whatever they return, and any received-not-started
        message when `release_buffered`, is made visible again at once.
        """
        self._receiving.clear()
        if release_buffered:
            await self._release_where(lambda d: not d.started, "drain")

    def resume_receiving(self) -> None:
        """Undo `pause_receiving`."""
        self._receiving.set()

    def delivery(self, message_id: str) -> Delivery | None:
        """The active delivery for an SQS message id (e.g. to await `lost`), if any."""
        return next((d for d in self._active.values() if d.message_id == message_id), None)

    async def release(
        self, target: Delivery | str, *, visibility_secs: int = 0, reason: str = "manual"
    ) -> bool:
        """Hand a received message back to SQS now (visibility 0 by default), without acking.

        `target` is a `Delivery`, an SQS message id or a receipt handle. A later taskiq ack of
        the same message is a no-op. Returns False if it was already acked or released.
        """
        if not 0 <= visibility_secs <= MAX_VISIBILITY_SECS:
            raise BrokerConfigError(f"visibility_secs must be 0..{MAX_VISIBILITY_SECS}")
        delivery = target if isinstance(target, Delivery) else self._find(target)
        if delivery is None or not self._finish(delivery):
            return False
        client, acker = self._require_client(), self._require_acker()
        try:
            await acker.call(
                delivery.queue.name,
                "release",
                lambda: client.change_message_visibility(
                    QueueUrl=delivery.queue_url,
                    ReceiptHandle=delivery.receipt_handle,
                    VisibilityTimeout=visibility_secs,
                ),
                raise_final=True,
            )
        except Exception as exc:
            if classify_error(exc) is not ErrorFamily.INVALID_HANDLE:
                logger.warning("release on %s failed: %s", delivery.queue.name, exc)
            return False
        safe_call(self.hooks.on_release, delivery.queue.name, 1, reason)
        return True

    # ------------------------------------------------------------------ receive side

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """Run one poller per queue and yield messages as taskiq `AckableMessage`s.

        Closing the generator (Receiver stop) stops the pollers and releases every message
        that was received but not yet handed to the Receiver.
        """
        self._require_client()
        self._buffer = asyncio.Queue()
        self._receiving.set()
        self._pollers = {
            name: asyncio.create_task(self._poll(q), name=f"llmq-poll-{name}")
            for name, q in self._queues.items()
        }
        if self._heartbeat is not None:
            self._heartbeat.start()
        try:
            while True:
                delivery = await self._buffer.get()
                if delivery.done:
                    continue
                delivery.started = True
                yield AckableMessage(
                    data=delivery.body.encode("utf-8"),
                    ack=functools.partial(self._ack, delivery),
                )
        finally:
            with contextlib.suppress(Exception):
                await self._stop_pollers()
                if self._client is not None:
                    await self._release_where(lambda d: not d.started, "stopped")

    async def _poll(self, queue: SQSQueue) -> None:
        """Receive loop for one queue: gate, budget, slots, long poll, accept."""
        url = self._urls[queue.name]
        attempt = 0
        # After an empty poll, ask for one message: an idle long poll then holds one slot,
        # not ten, and still returns on the first arrival (A2).
        idle = False
        while not self._stopping:
            if not self._receiving.is_set():
                await self._receiving.wait()
                continue
            if queue.gate is not None and not queue.gate.is_open():
                await queue.gate.wait_open()
                continue
            want = 1 if idle else queue.max_messages
            if self._receive_budget is not None:
                headroom = self._receive_budget(queue)
                if not isinstance(headroom, int):
                    headroom = await headroom
                if headroom <= 0:
                    await asyncio.sleep(0.25)
                    continue
                want = min(want, headroom)
            reserved = await self._scheduler.acquire(queue.priority, want)
            gate_closed = queue.gate is not None and not queue.gate.is_open()
            if self._stopping or not self._receiving.is_set() or gate_closed:
                self._scheduler.release(queue.priority, reserved)
                continue
            self._in_receive.add(queue.name)
            try:
                resp = await self._require_client().receive_message(
                    QueueUrl=url,
                    MaxNumberOfMessages=reserved,
                    WaitTimeSeconds=queue.wait_time_secs,
                    MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
                    MessageAttributeNames=["All"],
                )
            except asyncio.CancelledError:
                self._scheduler.release(queue.priority, reserved)
                raise
            except Exception as exc:
                self._scheduler.release(queue.priority, reserved)
                family = classify_error(exc)
                safe_call(self.hooks.on_error, queue.name, "receive", family.value)
                delay = backoff_delay(
                    attempt, base_secs=self._backoff_base_secs, cap_secs=self._backoff_cap_secs
                )
                logger.warning(
                    "receive on %s failed (%s, %s); retrying in %.2fs",
                    queue.name,
                    family.value,
                    error_code(exc),
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)
                continue
            finally:
                self._in_receive.discard(queue.name)
            attempt = 0
            messages = resp.get("Messages", [])
            idle = not messages
            self._scheduler.release(queue.priority, reserved - len(messages))
            safe_call(self.hooks.on_receive, queue.name, len(messages))
            for message in messages:
                self._accept(queue, url, message)

    def _accept(self, queue: SQSQueue, url: str, message: MessageTypeDef) -> None:
        """Turn one received message into a buffered delivery, or expire / release it."""
        attributes: Mapping[Any, str] = message.get("Attributes", {})
        sent_ms = attributes.get("SentTimestamp")
        sent_at = int(sent_ms) / 1000 if sent_ms else None
        delivery = Delivery(
            queue=queue,
            queue_url=url,
            message_id=message.get("MessageId", ""),
            receipt_handle=message.get("ReceiptHandle", ""),
            body=message.get("Body", ""),
            receive_count=int(attributes.get("ApproximateReceiveCount", "1")),
            received_at=asyncio.get_running_loop().time(),
            visibility_secs=self._visibility[queue.name],
            sent_at=sent_at,
        )
        self._active[delivery.receipt_handle] = delivery
        if sent_at is not None:
            safe_call(self.hooks.on_queue_wait, queue.name, max(0.0, time.time() - sent_at))
        expiry_attr = message.get("MessageAttributes", {}).get(EXPIRY_ATTRIBUTE)
        expiry = expiry_attr.get("StringValue") if expiry_attr is not None else None
        if expiry is not None and _is_expired(expiry):
            self._finish(delivery)
            self._require_acker().delete(queue.name, url, delivery.receipt_handle)
            safe_call(self.hooks.on_expired, queue.name, 1)
            return
        reason, body = self._screen(delivery)
        if reason is not None:
            logger.warning(
                "releasing message %s on %s for %ss: %s",
                delivery.message_id,
                queue.name,
                self._unknown_release_secs,
                reason,
            )
            self._finish(delivery)
            self._require_acker().change_visibility(
                queue.name, url, delivery.receipt_handle, self._unknown_release_secs
            )
            safe_call(self.hooks.on_release, queue.name, 1, reason)
            return
        delivery.body = body
        if self._stopping or not self._receiving.is_set() or self._buffer is None:
            self._finish(delivery)
            self._require_acker().change_visibility(queue.name, url, delivery.receipt_handle, 0)
            safe_call(self.hooks.on_release, queue.name, 1, "drain")
            return
        if self._heartbeat is not None:
            self._heartbeat.track(delivery)
        self._buffer.put_nowait(delivery)

    def _screen(self, delivery: Delivery) -> tuple[str | None, str]:
        """DS9 pre-filter: (reject reason or None, body to yield, with metadata if enabled)."""
        try:
            parsed = self.formatter.loads(delivery.body.encode("utf-8"))
        except Exception:
            return "unparseable", delivery.body
        if self.find_task(parsed.task_name) is None:
            return f"unknown_task:{parsed.task_name}", delivery.body
        if not envelope_supported(parsed):
            return "unsupported_envelope", delivery.body
        if self._message_filter is not None and not self._message_filter(parsed):
            return "filtered", delivery.body
        if not self._expose_metadata:
            return None, delivery.body
        parsed.labels.update(
            {
                SQS_MESSAGE_ID_LABEL: delivery.message_id,
                SQS_RECEIPT_HANDLE_LABEL: delivery.receipt_handle,
                SQS_QUEUE_LABEL: delivery.queue.name,
                SQS_RECEIVE_COUNT_LABEL: str(delivery.receive_count),
            }
        )
        return None, self.formatter.dumps(parsed).message.decode("utf-8")

    async def _ack(self, delivery: Delivery) -> None:
        """Taskiq ack: free the slot, stop the heartbeat, queue a batched delete."""
        if self._finish(delivery) and self._acker is not None:
            self._acker.delete(delivery.queue.name, delivery.queue_url, delivery.receipt_handle)

    def _finish(self, delivery: Delivery) -> bool:
        """Mark a delivery done exactly once and free its slot; False if already done."""
        if delivery.done:
            return False
        delivery.done = True
        self._active.pop(delivery.receipt_handle, None)
        if self._heartbeat is not None:
            self._heartbeat.untrack(delivery)
        self._scheduler.release(delivery.queue.priority, 1)
        return True

    def _find(self, key: str) -> Delivery | None:
        return self._active.get(key) or self.delivery(key)

    async def _release_where(self, predicate: Callable[[Delivery], bool], reason: str) -> None:
        """Make matching active deliveries visible now, in batches, and wait for SQS."""
        acker = self._acker
        if acker is None:
            return
        chosen = [d for d in list(self._active.values()) if predicate(d) and self._finish(d)]
        futures = [
            acker.change_visibility(d.queue.name, d.queue_url, d.receipt_handle, 0) for d in chosen
        ]
        await acker.flush()
        counts: dict[str, int] = {}
        for delivery, future in zip(chosen, futures, strict=True):
            if (
                future.done()
                and not future.cancelled()
                and future.exception() is None
                and future.result() is VisibilityResult.OK
            ):
                counts[delivery.queue.name] = counts.get(delivery.queue.name, 0) + 1
        for name, count in counts.items():
            safe_call(self.hooks.on_release, name, count, reason)

    async def _stop_pollers(self) -> None:
        """Cancel idle pollers at once; let in-flight long polls finish within the grace."""
        pollers = self._pollers
        self._pollers = {}
        if not pollers:
            return
        stopping, self._stopping = self._stopping, True
        for name, task in pollers.items():
            if name not in self._in_receive:
                task.cancel()
        _, pending = await asyncio.wait(pollers.values(), timeout=self._stop_grace_secs)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pollers.values(), return_exceptions=True)
        self._stopping = stopping

    # ------------------------------------------------------------------ send side

    async def kick(self, message: BrokerMessage) -> None:
        """Send one task to the queue named by its `queue_name` label (or the default).

        Batched queues await the batch flush, so send failures still reach the caller.
        """
        queue = self._route(message)
        url = self._urls.get(queue.name)
        if url is None or self._client is None:
            raise QueueUnavailable("broker is not started; call startup() first")
        entry = self._entry(message, queue)
        size = message_bytes(entry["MessageBody"], entry.get("MessageAttributes"))
        if size > MAX_MESSAGE_BYTES:
            raise MessageTooLargeError(
                f"message for {message.task_name} is {size} bytes (SQS limit {MAX_MESSAGE_BYTES})"
            )
        sender = self._senders.get(queue.name)
        skip = message.labels.get(SKIP_BATCHING_LABEL)
        if (
            sender is not None
            and not _truthy(skip)
            and message.task_name not in self._skip_batch_tasks
        ):
            await sender.submit((entry, size))
            return
        await self._send_one(queue, url, entry)

    def _route(self, message: BrokerMessage) -> SQSQueue:
        name = str(message.labels.get(QUEUE_LABEL) or self._default_queue)
        queue = self._queues.get(name)
        if queue is None:
            raise InvalidLabelError(f"queue_name {name!r} is not one of {sorted(self._queues)}")
        return queue

    def _entry(self, message: BrokerMessage, queue: SQSQueue) -> dict[str, Any]:
        """Build the SendMessage fields for one task from its labels (P5, P6, P7, P11)."""
        labels = message.labels
        try:
            body = message.message.decode("utf-8")
        except UnicodeDecodeError:
            raise BrokerConfigError("SQS bodies are text; use a UTF-8 serializer") from None
        entry: dict[str, Any] = {"MessageBody": body}
        delay = _int_label(labels.get(DELAY_LABEL, queue.default_delay_secs), DELAY_LABEL)
        if not 0 <= delay <= MAX_DELAY_SECS:
            raise InvalidLabelError(f"delay must be 0..{MAX_DELAY_SECS} seconds, got {delay}")
        if delay and queue.fifo:
            raise InvalidLabelError(f"delay is not supported on FIFO queue {queue.name!r}")
        if delay:
            entry["DelaySeconds"] = delay
        if queue.fifo or queue.is_fair:
            group = str(labels.get(GROUP_ID_LABEL) or message.task_name)
            entry["MessageGroupId"] = _fifo_id(group, GROUP_ID_LABEL)
        if queue.fifo:
            dedup = labels.get(DEDUPLICATION_ID_LABEL)
            if dedup is None and self._use_task_id_for_dedup:
                dedup = message.task_id
            if dedup is not None:
                entry["MessageDeduplicationId"] = _fifo_id(str(dedup), DEDUPLICATION_ID_LABEL)
        expiry = labels.get(EXPIRY_LABEL)
        if expiry is not None:
            try:
                expiry_at = float(expiry)
            except (TypeError, ValueError):
                raise InvalidLabelError(f"expiry must be epoch seconds, got {expiry!r}") from None
            if not math.isfinite(expiry_at) or expiry_at <= 0:
                raise InvalidLabelError(f"expiry must be positive epoch seconds, got {expiry!r}")
            entry["MessageAttributes"] = {
                EXPIRY_ATTRIBUTE: {"DataType": "Number", "StringValue": repr(expiry_at)}
            }
        return entry

    async def _send_one(self, queue: SQSQueue, url: str, entry: dict[str, Any]) -> str:
        client, acker = self._require_client(), self._require_acker()
        try:
            resp = await acker.call(
                queue.name,
                "send",
                lambda: client.send_message(QueueUrl=url, **entry),
                raise_final=True,
            )
        except Exception as exc:
            raise SQSSendError(
                f"send to {queue.name} failed: {exc}",
                family=classify_error(exc),
                code=error_code(exc),
            ) from exc
        safe_call(self.hooks.on_send, queue.name, 1)
        return str(resp.get("MessageId", ""))

    async def _send_batch(
        self, queue: SQSQueue, url: str, items: Sequence[_SendItem]
    ) -> list[str | BaseException]:
        """One `SendMessageBatch`; halves and retries if SQS still says it's too large."""
        client = self._require_client()
        entries: list[Any] = [{**entry, "Id": str(i)} for i, (entry, _) in enumerate(items)]
        try:
            resp = await self._require_acker().call(
                queue.name,
                "send_batch",
                lambda: client.send_message_batch(QueueUrl=url, Entries=entries),
                raise_final=True,
            )
        except Exception as exc:
            family = classify_error(exc)
            if family is ErrorFamily.SIZE_LIMIT and len(items) > 1:
                half = len(items) // 2
                first = await self._send_batch(queue, url, items[:half])
                return first + await self._send_batch(queue, url, items[half:])
            error = SQSSendError(
                f"batch send to {queue.name} failed: {exc}", family=family, code=error_code(exc)
            )
            return [error] * len(items)
        results: list[str | BaseException] = [""] * len(items)
        for ok in resp.get("Successful", []):
            results[int(ok["Id"])] = ok.get("MessageId", "")
        sent = len(resp.get("Successful", []))
        for failed in resp.get("Failed", []):
            index = int(failed["Id"])
            family = classify_code(failed.get("Code", ""), failed.get("Message", ""))
            if not failed.get("SenderFault", False) or family.retryable:
                try:
                    results[index] = await self._send_one(queue, url, items[index][0])
                    continue
                except SQSSendError as exc:
                    results[index] = exc
                    continue
            results[index] = SQSSendError(
                f"send to {queue.name} failed: {failed.get('Message', '')}",
                family=family,
                code=failed.get("Code", ""),
            )
        if sent:
            safe_call(self.hooks.on_send, queue.name, sent)
        return results

    # ------------------------------------------------------------------ helpers

    async def _resolve_url(self, client: SQSClient, queue: SQSQueue) -> str:
        if queue.url:
            return queue.url
        if queue.is_declare:
            attributes: dict[Any, str] = dict(queue.attributes)
            if queue.fifo:
                attributes.setdefault("FifoQueue", "true")
            if queue.visibility_timeout_secs is not None:
                attributes.setdefault("VisibilityTimeout", str(queue.visibility_timeout_secs))
            created = await client.create_queue(QueueName=queue.name, Attributes=attributes)
            return created["QueueUrl"]
        try:
            return (await client.get_queue_url(QueueName=queue.name))["QueueUrl"]
        except Exception as exc:
            if classify_error(exc) is ErrorFamily.NOT_FOUND:
                raise BrokerConfigError(
                    f"SQS queue {queue.name!r} does not exist (create it or set is_declare=True)"
                ) from exc
            raise

    async def _resolve_visibility(self, client: SQSClient, queue: SQSQueue, url: str) -> int:
        if queue.visibility_timeout_secs is not None:
            return queue.visibility_timeout_secs
        resp = await client.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])
        return int(resp.get("Attributes", {}).get("VisibilityTimeout", "30"))

    def _require_client(self) -> SQSClient:
        if self._client is None:
            raise QueueUnavailable("broker is not started; call startup() first")
        return self._client

    def _require_acker(self) -> Acker:
        if self._acker is None:
            raise QueueUnavailable("broker is not started; call startup() first")
        return self._acker


def _is_expired(raw: str) -> bool:
    try:
        return float(raw) < time.time()
    except ValueError:
        return False


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"} if value is not None else False


def _int_label(value: object, name: str) -> int:
    """Parse an integer label; taskiq sends label values as strings."""
    try:
        return round(float(str(value)))
    except (TypeError, ValueError):
        raise InvalidLabelError(f"{name} must be a number, got {value!r}") from None


def _fifo_id(value: str, name: str) -> str:
    if not 1 <= len(value) <= MAX_FIFO_ID_CHARS:
        raise InvalidLabelError(f"{name} must be 1..{MAX_FIFO_ID_CHARS} characters")
    return value
