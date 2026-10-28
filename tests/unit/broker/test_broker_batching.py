"""MicroBatcher: count, byte and time flush triggers, order, failures and shutdown flush."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from taskiq_llm_queue.broker.batching import MicroBatcher, message_bytes


class Recorder:
    def __init__(self, fail: set[int] | None = None, raise_all: bool = False) -> None:
        self.batches: list[list[int]] = []
        self.fail = fail or set()
        self.raise_all = raise_all

    async def __call__(self, items: Sequence[int]) -> list[int | BaseException]:
        self.batches.append(list(items))
        if self.raise_all:
            raise RuntimeError("boom")
        return [ValueError(i) if i in self.fail else i * 10 for i in items]


async def test_flushes_at_max_items_without_waiting() -> None:
    rec = Recorder()
    b = MicroBatcher[int, int](rec, max_items=3, max_delay_secs=10)
    futures = [b.submit(i) for i in range(3)]
    assert await asyncio.wait_for(asyncio.gather(*futures), 1) == [0, 10, 20]
    assert rec.batches == [[0, 1, 2]]
    await b.close()


async def test_flushes_partial_batch_after_delay() -> None:
    rec = Recorder()
    b = MicroBatcher[int, int](rec, max_items=10, max_delay_secs=0.05)
    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await asyncio.wait_for(b.submit(7), 1) == 70
    assert 0.04 <= loop.time() - start < 0.5
    await b.close()


async def test_byte_limit_splits_batches_in_order() -> None:
    """DS15: 6 x 200 KB must not go out as one 1.2 MB batch."""
    rec = Recorder()
    b = MicroBatcher[int, int](
        rec, max_items=10, max_bytes=900_000, size_of=lambda _: 200_000, max_delay_secs=0.05
    )
    await asyncio.gather(*(b.submit(i) for i in range(6)))
    assert rec.batches == [[0, 1, 2, 3], [4, 5]]
    await b.close()


async def test_oversized_single_item_still_sent_alone() -> None:
    rec = Recorder()
    b = MicroBatcher[int, int](rec, max_bytes=10, size_of=lambda _: 50, max_delay_secs=0.01)
    await asyncio.gather(b.submit(1), b.submit(2))
    assert rec.batches == [[1], [2]]
    await b.close()


async def test_per_item_failures_and_whole_batch_failure() -> None:
    rec = Recorder(fail={1})
    b = MicroBatcher[int, int](rec, max_items=2, max_delay_secs=0.01)
    ok, bad = b.submit(0), b.submit(1)
    assert await ok == 0
    with pytest.raises(ValueError, match="1"):
        await bad
    await b.close()
    boom = MicroBatcher[int, int](Recorder(raise_all=True), max_delay_secs=0.01)
    with pytest.raises(RuntimeError, match="boom"):
        await boom.submit(1)
    await boom.close()


async def test_unawaited_failures_are_retrieved() -> None:
    """Fire-and-forget acks must not log 'Future exception was never retrieved'."""
    rec = Recorder(fail={0})
    b = MicroBatcher[int, int](rec, max_delay_secs=0.01)
    future = b.submit(0)
    await b.flush_now()
    assert future.done()
    assert future._log_traceback is False
    await b.close()


async def test_close_flushes_pending_and_rejects_new_items() -> None:
    rec = Recorder()
    b = MicroBatcher[int, int](rec, max_items=10, max_delay_secs=60)
    future = b.submit(3)
    await asyncio.wait_for(b.close(), 1)
    assert future.result() == 30
    with pytest.raises(RuntimeError):
        b.submit(4)
    assert len(b) == 0


def test_message_bytes_counts_body_and_attributes() -> None:
    assert message_bytes("héllo") == 6
    attrs = {"expiry": {"DataType": "Number", "StringValue": "12.5"}}
    assert message_bytes("ab", attrs) == 2 + len("expiry") + len("Number") + len("12.5")
    assert message_bytes("", {"b": {"DataType": "Binary", "BinaryValue": b"xyz"}}) == 1 + 6 + 3
