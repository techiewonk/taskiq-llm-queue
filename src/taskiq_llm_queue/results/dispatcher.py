"""Per-pod result dispatcher: one batched read resolves every waiting caller.

Callers `await dispatcher.wait(task_id, timeout_secs, lane)`; a background loop polls all
pending ids with one `ResultStore.take_many` per interval instead of one poll per waiter, and
an optional pub/sub message on `KeySpace.done_channel(pod)` (carrying the task id) wakes it at
once to take just the announced ids; the interval sweep stays as the safety net.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import math
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.errors import LLMQueueTimeout, QueueUnavailable

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from taskiq_llm_queue.envelope import LLMReply
    from taskiq_llm_queue.keys import KeySpace
    from taskiq_llm_queue.protocols import ResultStore

logger = get_logger(__name__)

# Poll interval per lane; the loop polls at the fastest interval among pending waiters.
DEFAULT_INTERVALS_MS: Mapping[Lane, int] = {Lane.RT: 100, Lane.BG: 500, Lane.BULK: 2000}
_PUBSUB_RETRY_SECS = 1.0
# Rebuild the deadline heap once it holds this many entries and more stale than live ones.
_COMPACT_MIN_ENTRIES = 64
# A new waiter wakes the loop only if it needs a poll this much before the planned wake-up.
_WAKE_SLACK_SECS = 0.001


class ResultsUnavailable(QueueUnavailable):
    """Result reads have failed for `abandon_after_secs`: the waiter should serve itself.

    Raised to waiting callers so a queued call can run `direct` (local cap) instead of waiting
    to its deadline while Valkey is gone (S21).
    """


@dataclass(eq=False, slots=True)
class _Waiter:
    """One caller waiting for one task id; `seq` tells a stale heap entry from a live one.

    Heap entries hold only `(deadline, seq, task_id)`, never the waiter: a resolved waiter's
    future (and the reply in it) must be collectable as soon as the caller has it.
    """

    future: asyncio.Future[LLMReply]
    lane: Lane
    deadline: float = field(default=0.0)
    seq: int = 0


class ResultDispatcher:
    """Resolves all of one pod's waiting callers from one batched result read per interval.

    Used by the queued clients after they kiq a task. `clock` and `sleep` are injectable so
    tests can run timeouts without real waiting. Pub/sub wake-up is on when both `redis` and
    `channel` are given; results are still found by polling if a message is lost.
    `on_error` hears every failed read (poll or pub/sub) and `on_recovered` the first good poll
    after one, so a send-only pod notices a Valkey outage (S21) without a call of its own. With
    `abandon_after_secs`, waiters get `ResultsUnavailable` once reads have failed that long.
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
        on_error: Callable[[BaseException], object] | None = None,
        on_recovered: Callable[[], object] | None = None,
        abandon_after_secs: float | None = None,
    ) -> None:
        """Keep collaborators only; nothing starts until `start` or the first `wait`."""
        self._store = store
        self._on_error = on_error
        self._on_recovered = on_recovered
        self._abandon_after_secs = abandon_after_secs
        self._failing_since: float | None = None  # first failed read of the current outage
        self._redis = redis
        self._channel = channel
        self._intervals = {**DEFAULT_INTERVALS_MS, **(intervals_ms or {})}
        self._clock = clock
        self._sleep = sleep
        self._waiters: dict[str, _Waiter] = {}
        self._lanes: Counter[Lane] = Counter()
        self._deadlines: list[tuple[float, int, str]] = []
        self._seq = 0
        self._wake = asyncio.Event()  # poll now
        self._work = asyncio.Event()  # first waiter arrived while idle
        self._wake_at = math.inf  # clock time the current nap ends; inf while idle
        self._hinted: set[str] = set()  # ids announced on the done channel since the last take
        self._next_sweep: float | None = None  # when the next take over every waiter is due
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
        self._seq += 1
        waiter = _Waiter(
            asyncio.get_running_loop().create_future(),
            lane,
            deadline=self._clock() + timeout_secs,
            seq=self._seq,
        )
        self._waiters[task_id] = waiter
        self._lanes[lane] += 1
        heapq.heappush(self._deadlines, (waiter.deadline, waiter.seq, task_id))
        self._maybe_compact()
        if math.isinf(self._wake_at):
            self._work.set()
        elif (
            min(self._clock() + self._intervals[lane] / 1000, waiter.deadline)
            < self._wake_at - _WAKE_SLACK_SECS
        ):
            # S31: comparing the interval with the nap *length* woke the loop for most new
            # same-lane waiters ((t + 0.1) - t > 0.1 in floats): ~90 extra full sweeps per 1K.
            self._wake.set()  # loop naps longer than this waiter allows
        try:
            return await waiter.future
        finally:
            self._discard(task_id, waiter)
            self._maybe_compact()

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
        """Poll loop: sweep every waiter each fastest-lane interval, take hinted ids on wake-up.

        A pub/sub wake takes only the announced ids (not every pending one, which cost one
        `GET` per waiter per finished task); a deadline wake takes the ids that are due.
        """
        while True:
            if not self._waiters:
                self._wake_at = math.inf
                self._next_sweep = None
                self._hinted.clear()
                self._work.clear()
                await self._work.wait()
                self._wake.clear()
                continue
            interval = min(self._intervals[lane] for lane in self._lanes) / 1000
            now = self._clock()
            due = now + interval  # a faster lane that just arrived pulls the sweep in
            self._next_sweep = due if self._next_sweep is None else min(self._next_sweep, due)
            nap = max(0.0, self._next_sweep - now)
            self._drop_stale_deadlines()
            if self._deadlines:
                nap = min(nap, max(0.0, self._deadlines[0][0] - now))
            self._wake_at = now + nap
            woken = await self._nap(nap)
            if self._waiters:
                await self._take(woken)
            self._expire()

    async def _take(self, woken: bool) -> None:
        """Full sweep when due (or woken without hints, e.g. a new waiter); else hinted/due ids."""
        hinted, self._hinted = self._hinted, set()
        if self._clock() >= (self._next_sweep or 0.0) or (woken and not hinted):
            self._next_sweep = None
            await self._poll(list(self._waiters))
            return
        now = self._clock()
        due: set[str] = set()
        if self._deadlines and self._deadlines[0][0] <= now:  # last look before they time out
            due = {e[2] for e in self._deadlines if e[0] <= now and self._live(e) is not None}
        ids = [i for i in hinted | due if i in self._waiters]
        if ids:
            await self._poll(ids)

    async def _nap(self, secs: float) -> bool:
        """Sleep `secs` with the injected sleep; True when woken early by the event."""
        if not self._wake.is_set():
            sleeper = asyncio.ensure_future(self._sleep(secs))
            waker = asyncio.ensure_future(self._wake.wait())
            try:
                await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                sleeper.cancel()
                waker.cancel()
        woken = self._wake.is_set()
        self._wake.clear()
        return woken

    async def _poll(self, task_ids: list[str]) -> None:
        """One batched take over `task_ids`; store errors are logged and retried."""
        self.polls += 1
        try:
            replies = await self._store.take_many(task_ids)
        except Exception as exc:  # Redis blip: keep waiting, deadlines still apply
            logger.warning(
                "result_poll_failed", pending=len(task_ids), error_type=type(exc).__name__
            )
            self._report_error(exc)
            since = self._failing_since
            limit = self._abandon_after_secs
            if since is not None and limit is not None and self._clock() - since >= limit:
                self._fail_all(ResultsUnavailable(f"result reads failing for {limit:g}s"))
            return
        self._read_ok()
        for task_id, reply in replies.items():
            waiter = self._waiters.get(task_id)
            if waiter is not None and self._discard(task_id, waiter) and not waiter.future.done():
                waiter.future.set_result(reply)

    def _read_ok(self) -> None:
        """A read worked: end the current outage (reported once, on the transition)."""
        if self._failing_since is not None:
            self._failing_since = None
            self._notify(self._on_recovered)

    def _report_error(self, exc: BaseException) -> None:
        if self._failing_since is None:
            self._failing_since = self._clock()
        self._notify(self._on_error, exc)

    @staticmethod
    def _notify(callback: Callable[..., object] | None, *args: object) -> None:
        """Run a health callback; it must never break the poll loop or the listener."""
        if callback is None:
            return
        try:
            callback(*args)
        except Exception:
            logger.exception("result_health_callback_failed")

    def _hint(self, data: object) -> None:
        """Note an announced task id and wake the loop; an unreadable message wakes a sweep."""
        task_id = data.decode(errors="replace") if isinstance(data, bytes) else data
        if not isinstance(task_id, str):
            self._next_sweep = 0.0
        elif task_id in self._waiters:
            self._hinted.add(task_id)
        else:
            return  # not ours any more (resolved or timed out): no take needed
        self._wake.set()

    def _live(self, entry: tuple[float, int, str]) -> _Waiter | None:
        """The waiter a heap entry still stands for, or None when the entry is stale."""
        waiter = self._waiters.get(entry[2])
        return waiter if waiter is not None and waiter.seq == entry[1] else None

    def _drop_stale_deadlines(self) -> None:
        while self._deadlines and self._live(self._deadlines[0]) is None:
            heapq.heappop(self._deadlines)

    def _maybe_compact(self) -> None:
        """Rebuild the heap from live waiters once stale entries outnumber them.

        Resolved waiters leave their entry behind until its deadline; without this a pod
        serving many fast calls with long timeouts would grow the heap without bound.
        """
        size = len(self._deadlines)
        if size >= _COMPACT_MIN_ENTRIES and size > 2 * len(self._waiters):
            self._deadlines = [e for e in self._deadlines if self._live(e) is not None]
            heapq.heapify(self._deadlines)

    def _expire(self) -> None:
        """Fail every waiter whose deadline has passed with `LLMQueueTimeout`."""
        now = self._clock()
        while self._deadlines and self._deadlines[0][0] <= now:
            entry = heapq.heappop(self._deadlines)
            waiter = self._live(entry)
            if waiter is not None and self._discard(entry[2], waiter) and not waiter.future.done():
                waiter.future.set_exception(LLMQueueTimeout(f"no result for task {entry[2]}"))

    def _fail_all(self, exc: Exception) -> None:
        for task_id, waiter in list(self._waiters.items()):
            if self._discard(task_id, waiter) and not waiter.future.done():
                waiter.future.set_exception(exc)
        self._deadlines.clear()

    def _on_loop_exit(self, task: asyncio.Task[None]) -> None:
        """A crashed loop must not leave callers hanging: fail them all."""
        if not task.cancelled() and task.exception() is not None:
            logger.error("result_dispatcher_crashed", exc_info=task.exception())
            self._loop_task = None
            self._fail_all(QueueUnavailable("result dispatcher loop crashed"))

    async def _listen(self, redis: Redis, channel: str) -> None:
        """Subscribe to the pod's done channel; each message names a task id to take now."""
        while True:
            pubsub = redis.pubsub()
            try:
                await pubsub.subscribe(channel)
                self._read_ok()
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is not None and self._waiters:
                        self._hint(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # reconnect; polling keeps working meanwhile
                logger.warning("result_channel_failed", error_type=type(exc).__name__)
                self._report_error(exc)
                await self._sleep(_PUBSUB_RETRY_SECS)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap


async def publish_done(redis: Redis, keys: KeySpace, pod: str, task_id: str) -> None:
    """Tell pod `pod`'s dispatcher that `task_id`'s result is stored (workers call this).

    Best effort: a lost message only delays the result until the next poll.
    """
    await redis.publish(keys.done_channel(pod), task_id)
