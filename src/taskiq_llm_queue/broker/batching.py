"""Micro-batching for SQS batch APIs: send, delete (ack) and change-visibility (heartbeat).

Items are grouped until `max_items` or `max_bytes` is reached or the oldest item has waited
`max_delay_secs`, then handed to one flush call. Batches of one batcher are flushed one at a
time and in order, which keeps FIFO group order for sends (P8, A6, DS7, DS15).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from taskiq_llm_queue._logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass(slots=True)
class _Pending(Generic[T, R]):
    item: T
    size: int
    future: asyncio.Future[R]
    queued_at: float


class MicroBatcher(Generic[T, R]):
    """Collects items and flushes them in bounded batches through one async callable.

    `flush` receives the items of one batch and returns one result per item, in order; an
    item whose result is a `BaseException` fails its future with it. Results are always
    retrieved, so fire-and-forget callers (acks) never log "exception never retrieved".
    """

    def __init__(
        self,
        flush: Callable[[Sequence[T]], Awaitable[Sequence[R | BaseException]]],
        *,
        max_items: int = 10,
        max_bytes: int | None = None,
        size_of: Callable[[T], int] | None = None,
        max_delay_secs: float = 0.5,
        name: str = "batcher",
    ) -> None:
        """Configure limits; the background flush task starts on the first `submit`."""
        self._flush = flush
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._size_of = size_of
        self._max_delay_secs = max_delay_secs
        self._name = name
        self._pending: deque[_Pending[T, R]] = deque()
        self._pending_bytes = 0
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closing = False
        self._force = False
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    def __len__(self) -> int:
        return len(self._pending) + self._in_flight

    def submit(self, item: T) -> asyncio.Future[R]:
        """Queue one item and return a future for its result; never blocks."""
        if self._closing:
            raise RuntimeError(f"{self._name} is closed")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[R] = loop.create_future()
        future.add_done_callback(_retrieve)
        size = self._size_of(item) if self._size_of else 0
        self._pending.append(_Pending(item, size, future, loop.time()))
        self._pending_bytes += size
        self._idle.clear()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"llmq-{self._name}")
        self._wake.set()
        return future

    async def flush_now(self) -> None:
        """Send everything queued so far and wait until those batches are done."""
        if self._idle.is_set():
            return
        self._force = True
        self._wake.set()
        try:
            await self._idle.wait()
        finally:
            self._force = False

    async def close(self) -> None:
        """Flush what is queued, then stop the background task; idempotent."""
        self._closing = True
        await self.flush_now()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def _full(self) -> bool:
        return len(self._pending) >= self._max_items or (
            self._max_bytes is not None and self._pending_bytes >= self._max_bytes
        )

    def _take(self) -> list[_Pending[T, R]]:
        batch: list[_Pending[T, R]] = []
        total = 0
        while self._pending and len(batch) < self._max_items:
            nxt = self._pending[0]
            if batch and self._max_bytes is not None and total + nxt.size > self._max_bytes:
                break
            batch.append(self._pending.popleft())
            total += nxt.size
            self._pending_bytes -= nxt.size
        return batch

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            if not self._pending:
                self._wake.clear()
                await self._wake.wait()
                continue
            deadline = self._pending[0].queued_at + self._max_delay_secs
            while not (self._full() or self._force or self._closing):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                self._wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), remaining)
            batch = self._take()
            if batch:
                await self._send(batch)
            if not self._pending:
                self._idle.set()

    async def _send(self, batch: list[_Pending[T, R]]) -> None:
        self._in_flight += len(batch)
        try:
            results: Sequence[R | BaseException] = await self._flush([p.item for p in batch])
        except Exception as exc:
            logger.warning(
                "batch_flush_failed", batcher=self._name, size=len(batch), error=str(exc)
            )
            results = [exc] * len(batch)
        finally:
            self._in_flight -= len(batch)
        if len(results) != len(batch):
            mismatch = RuntimeError(f"{self._name}: flush returned {len(results)} results")
            results = [mismatch] * len(batch)
        for pending, result in zip(batch, results, strict=True):
            if pending.future.done():
                continue
            if isinstance(result, BaseException):
                pending.future.set_exception(result)
            else:
                pending.future.set_result(result)


def _retrieve(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


def message_bytes(body: str, attributes: Mapping[str, Mapping[str, Any]] | None = None) -> int:
    """Size SQS counts for one message: UTF-8 body plus attribute names, types and values.

    Used to keep `SendMessageBatch` under the 1 MiB request cap (DS15).
    """
    size = len(body.encode("utf-8"))
    for name, value in (attributes or {}).items():
        size += len(name.encode("utf-8")) + len(str(value.get("DataType", "")).encode("utf-8"))
        string_value = value.get("StringValue")
        if string_value is not None:
            size += len(str(string_value).encode("utf-8"))
        binary_value = value.get("BinaryValue")
        if isinstance(binary_value, bytes | bytearray):
            size += len(binary_value)
    return size
