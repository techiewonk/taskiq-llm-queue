"""Per-pod result dispatcher: one batched read resolves every waiting caller (plan D80).

Callers `await dispatcher.wait(task_id, timeout_secs, lane)`; a background loop polls all
pending ids with one `ResultStore.take_many` per interval instead of one poll per waiter, and
an optional pub/sub message on `KeySpace.done_channel(pod)` wakes it at once.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import logging
import math
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.errors import LLMQueueTimeout, QueueUnavailable

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from taskiq_llm_queue.envelope import LLMReply
    from taskiq_llm_queue.keys import KeySpace
    from taskiq_llm_queue.protocols import ResultStore

logger = logging.getLogger(__name__)

# Poll interval per lane; the loop polls at the fastest interval among pending waiters.
DEFAULT_INTERVALS_MS: Mapping[Lane, int] = {Lane.RT: 100, Lane.BG: 500, Lane.BULK: 2000}
_PUBSUB_RETRY_SECS = 1.0


@dataclass(eq=False, slots=True)
class _Waiter:
    """One caller waiting for one task id; identity tells a stale heap entry from a live one."""

    future: asyncio.Future[LLMReply]
    lane: Lane
    deadline: float = field(default=0.0)


class ResultDispatcher:
    """Resolves all of one pod's waiting callers from one batched result read per interval.

    Used by the queued clients after they kiq a task. `clock` and `sleep` are injectable so
    tests can run timeouts without real waiting. Pub/sub wake-up is on when both `redis` and
    `channel` are given; results are still found by polling if a message is lost.
    """

    def __init__(
        self,
        store: ResultStore,
        *,
        redis: Redis | None = None,
        channel: str | None = None,
        intervals_ms: Mapping[Lane, int] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Keep collaborators only; nothing starts until `start` or the first `wait`."""
        self._store = store
        self._redis = redis
        self._channel = channel
        self._intervals = {**DEFAULT_INTERVALS_MS, **(intervals_ms or {})}
        self._clock = clock
        self._sleep = sleep
        self._waiters: dict[str, _Waiter] = {}
        self._lanes: Counter[Lane] = Counter()
        self._deadlines: list[tuple[float, int, str, _Waiter]] = []
        self._seq = 0
        self._wake = asyncio.Event()  # poll now
        self._work = asyncio.Event()  # first waiter arrived while idle
        self._nap_secs = math.inf
        self._loop_task: asyncio.Task[None] | None = None
        self._pubsub_task: asyncio.Task[None] | None = None
        self.polls = 0  # take_many calls made; read by tests and metrics

    @property
    def pending(self) -> int:
        """Number of callers currently waiting."""
        return len(self._waiters)

    async def start(self) -> None:
        """Start the poll loop (and the pub/sub listener if configured); idempotent."""
        if self._loop_task is not None:
            return
        self._wake, self._work = asyncio.Event(), asyncio.Event()  # bind to the running loop
        self._loop_task = asyncio.create_task(self._run(), name="llmq-result-dispatcher")
        self._loop_task.add_done_callback(self._on_loop_exit)
        if self._redis is not None and self._channel is not None:
            self._pubsub_task = asyncio.create_task(
                self._listen(self._redis, self._channel), name="llmq-result-wakeup"
            )

    async def stop(self) -> None:
        """Stop the background tasks and fail any remaining waiters; idempotent."""
        tasks = [t for t in (self._loop_task, self._pubsub_task) if t is not None]
        self._loop_task = self._pubsub_task = None
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._fail_all(QueueUnavailable("result dispatcher stopped"))

    async def wait(self, task_id: str, timeout_secs: float, lane: Lane = Lane.BG) -> LLMReply:
        """Wait for the result of `task_id`; raises `LLMQueueTimeout` after `timeout_secs`.

        Cancelling the calling coroutine removes the waiter. One waiter per task id; a second
        concurrent `wait` on the same id is a programming error (`ValueError`).
        """
        if task_id in self._waiters:
            raise ValueError(f"already waiting for task {task_id!r}")
        await self.start()
        waiter = _Waiter(asyncio.get_running_loop().create_future(), lane)
        waiter.deadline = self._clock() + timeout_secs
        self._waiters[task_id] = waiter
        self._lanes[lane] += 1
        self._seq += 1
        heapq.heappush(self._deadlines, (waiter.deadline, self._seq, task_id, waiter))
        if math.isinf(self._nap_secs):
            self._work.set()
        elif (
            self._intervals[lane] / 1000 < self._nap_secs
            or waiter.deadline < self._clock() + self._nap_secs
        ):
            self._wake.set()  # loop naps longer than this waiter allows
        try:
            return await waiter.future
        finally:
            self._discard(task_id, waiter)

    def _discard(self, task_id: str, waiter: _Waiter) -> _Waiter | None:
        """Remove `waiter` if it is still the one registered for `task_id`."""
        if self._waiters.get(task_id) is not waiter:
            return None
        del self._waiters[task_id]
        self._lanes[waiter.lane] -= 1
        if not self._lanes[waiter.lane]:
            del self._lanes[waiter.lane]
        return waiter

    async def _run(self) -> None:
        """Poll loop: nap for the fastest pending lane's interval, take all, expire overdue."""
        while True:
            if not self._waiters:
                self._nap_secs = math.inf
                self._work.clear()
                await self._work.wait()
                self._wake.clear()
                continue
            interval = min(self._intervals[lane] for lane in self._lanes) / 1000
            self._drop_stale_deadlines()
            if self._deadlines:
                interval = min(interval, max(0.0, self._deadlines[0][0] - self._clock()))
            self._nap_secs = interval
            await self._nap(interval)
            if self._waiters:
                await self._poll()
            self._expire()

    async def _nap(self, secs: float) -> None:
        """Sleep `secs` with the injected sleep, returning early when woken."""
        if not self._wake.is_set():
            sleeper = asyncio.ensure_future(self._sleep(secs))
            waker = asyncio.ensure_future(self._wake.wait())
            try:
                await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                sleeper.cancel()
                waker.cancel()
        self._wake.clear()

    async def _poll(self) -> None:
        """One batched take over every pending id; store errors are logged and retried."""
        self.polls += 1
        try:
            replies = await self._store.take_many(list(self._waiters))
        except Exception as exc:  # Redis blip: keep waiting, deadlines still apply
            logger.warning("result poll failed: %s", type(exc).__name__)
            return
        for task_id, reply in replies.items():
            waiter = self._waiters.get(task_id)
            if waiter is not None and self._discard(task_id, waiter) and not waiter.future.done():
                waiter.future.set_result(reply)

    def _drop_stale_deadlines(self) -> None:
        while (
            self._deadlines
            and self._waiters.get(self._deadlines[0][2]) is not (self._deadlines[0][3])
        ):
            heapq.heappop(self._deadlines)

    def _expire(self) -> None:
        """Fail every waiter whose deadline has passed with `LLMQueueTimeout`."""
        now = self._clock()
        while self._deadlines and self._deadlines[0][0] <= now:
            _, _, task_id, waiter = heapq.heappop(self._deadlines)
            if self._discard(task_id, waiter) and not waiter.future.done():
                waiter.future.set_exception(LLMQueueTimeout(f"no result for task {task_id}"))

    def _fail_all(self, exc: Exception) -> None:
        for task_id, waiter in list(self._waiters.items()):
            if self._discard(task_id, waiter) and not waiter.future.done():
                waiter.future.set_exception(exc)
        self._deadlines.clear()

    def _on_loop_exit(self, task: asyncio.Task[None]) -> None:
        """A crashed loop must not leave callers hanging: fail them all."""
        if not task.cancelled() and task.exception() is not None:
            logger.error("result dispatcher loop crashed", exc_info=task.exception())
            self._loop_task = None
            self._fail_all(QueueUnavailable("result dispatcher loop crashed"))

    async def _listen(self, redis: Redis, channel: str) -> None:
        """Subscribe to the pod's done channel and wake the poll loop on every message."""
        while True:
            pubsub = redis.pubsub()
            try:
                await pubsub.subscribe(channel)
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is not None and self._waiters:
                        self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # reconnect; polling keeps working meanwhile
                logger.warning("result wake-up channel failed: %s", type(exc).__name__)
                await self._sleep(_PUBSUB_RETRY_SECS)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap


async def publish_done(redis: Redis, keys: KeySpace, pod: str, task_id: str) -> None:
    """Tell pod `pod`'s dispatcher that `task_id`'s result is stored (workers call this).

    Best effort: a lost message only delays the result until the next poll.
    """
    await redis.publish(keys.done_channel(pod), task_id)
