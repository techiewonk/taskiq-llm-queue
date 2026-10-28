"""`ResultDispatcher` health reporting: read errors reach the failover policy (S21).

Real clock with 10 ms poll intervals; each test finishes in well under a second.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from taskiq_llm_queue import Lane, LLMReply
from taskiq_llm_queue.results import ResultDispatcher
from taskiq_llm_queue.results.dispatcher import ResultsUnavailable

FAST = {Lane.RT: 10, Lane.BG: 10, Lane.BULK: 10}


class FlakyStore:
    """ResultStore whose reads fail while `down` is set."""

    def __init__(self) -> None:
        self.down = True
        self.data: dict[str, LLMReply] = {}

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        self.data[reply.request_id] = reply

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        if self.down:
            raise ConnectionError("valkey down")
        return {i: self.data.pop(i) for i in task_ids if i in self.data}


async def test_poll_errors_and_recovery_reach_the_callbacks() -> None:
    """S21 bug: the dispatcher swallowed read errors, so a send-only pod never entered
    Valkey-down mode. Errors now reach `on_error`; the first good poll after them reaches
    `on_recovered` once."""
    store = FlakyStore()
    errors: list[BaseException] = []
    recovered: list[int] = []
    dispatcher = ResultDispatcher(
        store,
        intervals_ms=FAST,
        on_error=errors.append,
        on_recovered=lambda: recovered.append(1),
    )
    waiter = asyncio.create_task(dispatcher.wait("t-1", 5.0))
    await asyncio.sleep(0.05)
    assert errors
    assert all(isinstance(e, ConnectionError) for e in errors)
    assert recovered == []
    store.down = False
    await store.put(LLMReply(request_id="t-1", ok=True, response={}), ttl_secs=60)
    assert (await waiter).ok
    assert recovered == [1]
    await dispatcher.stop()


async def test_waiters_are_abandoned_after_a_sustained_outage() -> None:
    """S21 bug: callers whose call was already queued waited to their deadline while Valkey
    was down. After `abandon_after_secs` of failed reads they get `ResultsUnavailable` so the
    client can serve them direct."""
    dispatcher = ResultDispatcher(FlakyStore(), intervals_ms=FAST, abandon_after_secs=0.05)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    with pytest.raises(ResultsUnavailable):
        await dispatcher.wait("t-1", 5.0)
    assert loop.time() - t0 < 1.0
    assert dispatcher.pending == 0
    await dispatcher.stop()


async def test_a_failing_callback_never_breaks_the_poll_loop() -> None:
    """A health callback that raises is logged; waiters still resolve once reads work."""
    store = FlakyStore()

    def boom(exc: BaseException) -> None:
        raise RuntimeError("hook bug")

    dispatcher = ResultDispatcher(store, intervals_ms=FAST, on_error=boom)
    waiter = asyncio.create_task(dispatcher.wait("t-1", 5.0))
    await asyncio.sleep(0.05)
    store.down = False
    await store.put(LLMReply(request_id="t-1", ok=True, response={}), ttl_secs=60)
    assert (await waiter).ok
    await dispatcher.stop()
