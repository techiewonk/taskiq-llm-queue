"""Keeps received messages invisible while they wait or run, with batched extensions (DS6).

One loop serves every tracked delivery: each is extended by its queue's visibility timeout
every `interval` (≤ visibility / 3), so two missed beats still leave margin. Tracking stops
on ack or release, after `max_extensions`, once the message's `expiry` (deadline) plus a grace
has passed, or when SQS says the receipt handle is invalid; the last case sets
`Delivery.lost` and calls `on_lost` so the task can be cancelled.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import math
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

import structlog

from taskiq_llm_queue.broker.acker import VisibilityResult
from taskiq_llm_queue.broker.events import BrokerHooks, safe_call

if TYPE_CHECKING:
    from taskiq_llm_queue.broker.acker import Acker
    from taskiq_llm_queue.broker.delivery import Delivery
    from taskiq_llm_queue.broker.queue import Heartbeat

logger = structlog.get_logger(__name__)


class HeartbeatManager:
    """Schedules visibility extensions for tracked deliveries through the `Acker`."""

    def __init__(
        self,
        acker: Acker,
        config: Heartbeat,
        *,
        hooks: BrokerHooks,
        on_lost: Callable[[Delivery], None] | None = None,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        """Nothing runs until `start()`; `track` before `start` is allowed.

        `wall_clock` (epoch seconds) is compared with `Delivery.expires_at`; injectable for tests.
        """
        self._acker = acker
        self._wall_clock = wall_clock
        self._config = config
        self._hooks = hooks
        self._on_lost = on_lost
        self._tracked: dict[int, Delivery] = {}
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def tracked(self) -> int:
        """Deliveries currently being extended."""
        return len(self._tracked)

    def track(self, delivery: Delivery) -> None:
        """Start extending a delivery; the first beat is one interval after it was received."""
        interval = self._config.interval_for(delivery.visibility_secs)
        delivery.next_heartbeat_at = delivery.received_at + interval
        self._tracked[id(delivery)] = delivery
        self._wake.set()

    def untrack(self, delivery: Delivery) -> None:
        """Stop extending a delivery (ack, release, or extensions exhausted)."""
        self._tracked.pop(id(delivery), None)
        delivery.next_heartbeat_at = math.inf

    def start(self) -> None:
        """Start the background loop; idempotent."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="llmq-heartbeat")

    async def stop(self) -> None:
        """Stop the loop; tracked deliveries are left to their visibility timeout."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def due(self, now: float) -> list[Delivery]:
        """Deliveries whose next beat is at or before `now` (oldest first)."""
        ready = [d for d in self._tracked.values() if d.next_heartbeat_at <= now]
        return sorted(ready, key=lambda d: d.next_heartbeat_at)

    def beat(self, now: float) -> int:
        """Submit one extension for every due delivery; returns how many were submitted."""
        submitted = 0
        for delivery in self.due(now):
            if delivery.done:
                self.untrack(delivery)
                continue
            limit = self._config.max_extensions
            if limit is not None and delivery.extensions >= limit:
                logger.warning(
                    "sqs_heartbeat_max_extensions",
                    queue=delivery.queue.name,
                    max_extensions=limit,
                )
                self.untrack(delivery)
                continue
            if (
                delivery.expires_at is not None
                and self._wall_clock() > delivery.expires_at + self._config.deadline_grace_secs
            ):
                # DS6: a delivery stuck past its deadline must not stay invisible forever; once
                # it reappears the broker deletes it as expired (P11).
                logger.warning(
                    "sqs_heartbeat_deadline_passed",
                    queue=delivery.queue.name,
                    message_id=delivery.message_id,
                    extensions=delivery.extensions,
                )
                self.untrack(delivery)
                continue
            delivery.extensions += 1
            delivery.next_heartbeat_at = now + self._config.interval_for(delivery.visibility_secs)
            future = self._acker.change_visibility(
                delivery.queue.name,
                delivery.queue_url,
                delivery.receipt_handle,
                delivery.visibility_secs,
            )
            future.add_done_callback(functools.partial(self._settle, delivery))
            submitted += 1
        return submitted

    def _settle(self, delivery: Delivery, future: asyncio.Future[VisibilityResult]) -> None:
        if future.cancelled() or future.exception() is not None:
            return
        result = future.result()
        if result is VisibilityResult.OK:
            safe_call(self._hooks.on_heartbeat, delivery.queue.name, 1, 0)
            return
        if result is VisibilityResult.LOST and not delivery.done:
            safe_call(self._hooks.on_heartbeat, delivery.queue.name, 0, 1)
            self.untrack(delivery)
            delivery.lost.set()
            if self._on_lost is not None:
                safe_call(self._on_lost, delivery)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            now = loop.time()
            self.beat(now)
            nxt = min((d.next_heartbeat_at for d in self._tracked.values()), default=math.inf)
            self._wake.clear()
            timeout = None if math.isinf(nxt) else max(0.0, nxt - loop.time())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout)
