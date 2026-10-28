"""Capacity and priority accounting shared by the broker's per-queue pollers (A1, A2, DS4).

A slot is held from the moment a poller reserves it for a `ReceiveMessage` call until the
delivered message is acked or released; unused reservations return as soon as the receive
comes back. Pollers therefore never receive more than the free slots, even when several
long polls run at once.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from taskiq_llm_queue.broker.errors import BrokerConfigError


class SlotScheduler:
    """Strict-priority slot accounting for `max_inflight` delivered-not-acked messages.

    `reserve_for_priority0` slots are usable only by priority 0; `min_slots_for_lower` slots
    are always kept for priorities > 0 (priority 0 can never take them). A lower priority
    also yields freed slots to any higher priority that is waiting for capacity, except
    inside its guaranteed share.
    """

    def __init__(
        self,
        max_inflight: int,
        *,
        reserve_for_priority0: int = 0,
        min_slots_for_lower: int = 0,
    ) -> None:
        """Validate the split; the three numbers must fit inside `max_inflight`."""
        if max_inflight < 1:
            raise BrokerConfigError(f"max_inflight must be >= 1, got {max_inflight}")
        if reserve_for_priority0 < 0 or min_slots_for_lower < 0:
            raise BrokerConfigError("reserve_for_priority0 and min_slots_for_lower must be >= 0")
        if reserve_for_priority0 + min_slots_for_lower > max_inflight:
            raise BrokerConfigError(
                "reserve_for_priority0 + min_slots_for_lower must not exceed max_inflight"
            )
        self.max_inflight = max_inflight
        self.reserve_for_priority0 = reserve_for_priority0
        self.min_slots_for_lower = min_slots_for_lower
        self._held: Counter[int] = Counter()
        self._waiting: Counter[int] = Counter()
        self._changed: asyncio.Event | None = None

    @property
    def held(self) -> int:
        """Slots in use: reserved for a pending receive or delivered and not yet acked."""
        return sum(self._held.values())

    def held_by(self, priority: int) -> int:
        """Slots held by one priority level."""
        return self._held[priority]

    def budget(self, priority: int) -> int:
        """How many more messages a poller of this priority may receive right now."""
        free = self.max_inflight - self.held
        lower_held = sum(n for p, n in self._held.items() if p > 0)
        if priority == 0:
            owed_to_lower = max(0, self.min_slots_for_lower - lower_held)
            return max(0, free - owed_to_lower)
        lower_cap = max(self.max_inflight - self.reserve_for_priority0, self.min_slots_for_lower)
        allowed = min(free, lower_cap - lower_held)
        if any(n for p, n in self._waiting.items() if p < priority):
            allowed = min(allowed, self.min_slots_for_lower - lower_held)
        return max(0, allowed)

    async def acquire(self, priority: int, want: int) -> int:
        """Wait until this priority has budget, then reserve and return up to `want` slots."""
        if want < 1:
            return 0
        self._waiting[priority] += 1
        try:
            while (budget := self.budget(priority)) <= 0:
                await self._event().wait()
        finally:
            self._waiting[priority] -= 1
            self._notify()
        taken = min(want, budget)
        self._held[priority] += taken
        return taken

    def release(self, priority: int, count: int = 1) -> None:
        """Return slots (unused reservation, ack, release or expired delete)."""
        if count <= 0:
            return
        self._held[priority] = max(0, self._held[priority] - count)
        self._notify()

    def resize(self, max_inflight: int) -> None:
        """Change capacity at runtime, e.g. from a drain or a measured pod limit."""
        if max_inflight < self.reserve_for_priority0 + self.min_slots_for_lower or max_inflight < 1:
            raise BrokerConfigError("max_inflight too small for the reserved slots")
        self.max_inflight = max_inflight
        self._notify()

    async def wait_changed(self) -> None:
        """Return at the next capacity change (a slot freed, a waiter left, a resize)."""
        await self._event().wait()

    def _event(self) -> asyncio.Event:
        if self._changed is None:
            self._changed = asyncio.Event()
        return self._changed

    def _notify(self) -> None:
        # Waiters hold the old event; setting it wakes them, the next check uses a fresh one.
        if self._changed is not None:
            self._changed.set()
            self._changed = None
