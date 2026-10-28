"""Batched acks (`DeleteMessageBatch`) and visibility changes (`ChangeMessageVisibilityBatch`).

At 10K messages in flight one call per message is too many requests, so acks, heartbeats
and releases go through per-queue micro-batches (A6). Failed batch entries are retried one
by one; an invalid receipt handle on delete is fine (the message is already gone or owned
elsewhere and its claim is `done`, DS7), while on a visibility change it means "lost" (DS6).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.broker.batching import MicroBatcher
from taskiq_llm_queue.broker.errors import (
    ErrorFamily,
    backoff_delay,
    classify_code,
    classify_error,
    error_code,
)
from taskiq_llm_queue.broker.events import BrokerHooks, safe_call
from taskiq_llm_queue.broker.queue import MAX_BATCH_ENTRIES

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient

logger = logging.getLogger(__name__)


class VisibilityResult(StrEnum):
    """Outcome of one visibility change: done, handle invalid (lost), or failed (retry later)."""

    OK = "ok"
    LOST = "lost"
    FAILED = "failed"


class Acker:
    """Owns the delete and change-visibility batchers for every queue of one broker.

    `delete` and `change_visibility` return futures immediately; `flush` and `close` push
    out whatever is queued (used on shutdown and drain, A5).
    """

    def __init__(
        self,
        client: SQSClient,
        *,
        hooks: BrokerHooks,
        ack_flush_secs: float = 0.5,
        visibility_flush_secs: float = 0.5,
        retry_attempts: int = 3,
        backoff_base_secs: float = 0.2,
        backoff_cap_secs: float = 2.0,
    ) -> None:
        """Create no batchers yet; one pair is made per queue on first use."""
        self._client = client
        self._hooks = hooks
        self._ack_flush_secs = ack_flush_secs
        self._visibility_flush_secs = visibility_flush_secs
        self._retry_attempts = retry_attempts
        self._backoff_base_secs = backoff_base_secs
        self._backoff_cap_secs = backoff_cap_secs
        self._deleters: dict[str, MicroBatcher[str, bool]] = {}
        self._changers: dict[str, MicroBatcher[tuple[str, int], VisibilityResult]] = {}

    def delete(self, queue: str, url: str, receipt_handle: str) -> asyncio.Future[bool]:
        """Queue one ack; resolves True when deleted or already gone, False on failure."""
        batcher = self._deleters.get(url)
        if batcher is None:
            batcher = MicroBatcher[str, bool](
                lambda items: self._delete_batch(queue, url, items),
                max_items=MAX_BATCH_ENTRIES,
                max_delay_secs=self._ack_flush_secs,
                name=f"ack:{queue}",
            )
            self._deleters[url] = batcher
        return batcher.submit(receipt_handle)

    def change_visibility(
        self, queue: str, url: str, receipt_handle: str, visibility_secs: int
    ) -> asyncio.Future[VisibilityResult]:
        """Queue one visibility change (heartbeat extend, or 0 / N for a release)."""
        batcher = self._changers.get(url)
        if batcher is None:
            batcher = MicroBatcher[tuple[str, int], VisibilityResult](
                lambda items: self._visibility_batch(queue, url, items),
                max_items=MAX_BATCH_ENTRIES,
                max_delay_secs=self._visibility_flush_secs,
                name=f"visibility:{queue}",
            )
            self._changers[url] = batcher
        return batcher.submit((receipt_handle, visibility_secs))

    async def flush(self) -> None:
        """Send every queued ack and visibility change now and wait for the results."""
        batchers: list[MicroBatcher[Any, Any]] = [
            *self._changers.values(),
            *self._deleters.values(),
        ]
        await asyncio.gather(*(b.flush_now() for b in batchers))

    async def close(self) -> None:
        """Flush and stop all batchers; idempotent."""
        batchers: list[MicroBatcher[Any, Any]] = [
            *self._changers.values(),
            *self._deleters.values(),
        ]
        await asyncio.gather(*(b.close() for b in batchers))
        self._deleters.clear()
        self._changers.clear()

    def _batchers(self) -> list[MicroBatcher[Any, Any]]:
        return [*self._changers.values(), *self._deleters.values()]

    async def _delete_batch(self, queue: str, url: str, handles: Sequence[str]) -> list[bool]:
        entries: list[Any] = [{"Id": str(i), "ReceiptHandle": h} for i, h in enumerate(handles)]
        resp = await self.call(
            queue,
            "delete_batch",
            lambda: self._client.delete_message_batch(QueueUrl=url, Entries=entries),
        )
        if resp is None:
            return [False] * len(handles)
        results = [True] * len(handles)
        for failed in resp.get("Failed", []):
            index = int(failed["Id"])
            family = classify_code(failed.get("Code", ""), failed.get("Message", ""))
            if family is ErrorFamily.INVALID_HANDLE:
                logger.debug("ack of %s: receipt handle already invalid", queue)
                continue
            results[index] = await self._delete_one(queue, url, handles[index])
        acked = sum(results)
        if acked:
            safe_call(self._hooks.on_ack, queue, acked)
        return results

    async def _delete_one(self, queue: str, url: str, handle: str) -> bool:
        try:
            await self.call(
                queue,
                "delete",
                lambda: self._client.delete_message(QueueUrl=url, ReceiptHandle=handle),
                raise_final=True,
            )
        except Exception as exc:
            if classify_error(exc) is ErrorFamily.INVALID_HANDLE:
                return True
            logger.warning("ack failed on %s (%s); message will be redelivered", queue, exc)
            return False
        return True

    async def _visibility_batch(
        self, queue: str, url: str, items: Sequence[tuple[str, int]]
    ) -> list[VisibilityResult]:
        entries: list[Any] = [
            {"Id": str(i), "ReceiptHandle": h, "VisibilityTimeout": secs}
            for i, (h, secs) in enumerate(items)
        ]
        resp = await self.call(
            queue,
            "visibility_batch",
            lambda: self._client.change_message_visibility_batch(QueueUrl=url, Entries=entries),
        )
        if resp is None:
            return [VisibilityResult.FAILED] * len(items)
        results = [VisibilityResult.OK] * len(items)
        for failed in resp.get("Failed", []):
            index = int(failed["Id"])
            family = classify_code(failed.get("Code", ""), failed.get("Message", ""))
            if family is ErrorFamily.INVALID_HANDLE:
                results[index] = VisibilityResult.LOST
            else:
                results[index] = await self._visibility_one(queue, url, *items[index])
        return results

    async def _visibility_one(
        self, queue: str, url: str, handle: str, secs: int
    ) -> VisibilityResult:
        try:
            await self.call(
                queue,
                "visibility",
                lambda: self._client.change_message_visibility(
                    QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=secs
                ),
                raise_final=True,
            )
        except Exception as exc:
            if classify_error(exc) is ErrorFamily.INVALID_HANDLE:
                return VisibilityResult.LOST
            return VisibilityResult.FAILED
        return VisibilityResult.OK

    async def call(
        self,
        queue: str,
        operation: str,
        call: Callable[[], Awaitable[Any]],
        *,
        raise_final: bool = False,
    ) -> Any:
        """Run one SQS call, retrying throttles and 5xx with jittered backoff (DS16)."""
        attempt = 0
        while True:
            try:
                return await call()
            except Exception as exc:
                family = classify_error(exc)
                safe_call(self._hooks.on_error, queue, operation, family.value)
                if family.retryable and attempt + 1 < self._retry_attempts:
                    await asyncio.sleep(
                        backoff_delay(
                            attempt,
                            base_secs=self._backoff_base_secs,
                            cap_secs=self._backoff_cap_secs,
                        )
                    )
                    attempt += 1
                    continue
                if raise_final:
                    raise
                logger.warning("%s on %s failed: %s %s", operation, queue, error_code(exc), exc)
                return None
