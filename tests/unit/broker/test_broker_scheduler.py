"""Slot scheduler: capacity, priority-0 reserve, guaranteed lower slots and strict priority."""

from __future__ import annotations

import asyncio

import pytest

from taskiq_llm_queue.broker import BrokerConfigError, SlotScheduler


def test_budget_never_exceeds_free_slots() -> None:
    s = SlotScheduler(5)
    assert s.budget(0) == 5
    assert s.budget(1) == 5
    s._held[1] = 4
    assert s.budget(0) == 1
    assert s.budget(1) == 1


def test_reserve_for_priority0_only_usable_by_priority0() -> None:
    s = SlotScheduler(10, reserve_for_priority0=3)
    assert s.budget(1) == 7
    s._held[1] = 7
    assert s.budget(1) == 0
    assert s.budget(2) == 0
    assert s.budget(0) == 3


def test_min_slots_for_lower_kept_from_priority0() -> None:
    s = SlotScheduler(10, min_slots_for_lower=2)
    assert s.budget(0) == 8
    s._held[0] = 8
    assert s.budget(0) == 0
    assert s.budget(1) == 2
    s._held[1] = 2
    assert s.budget(0) == 0


def test_lower_yields_to_waiting_higher_except_guaranteed_share() -> None:
    s = SlotScheduler(4, min_slots_for_lower=1)
    s._waiting[0] = 1
    assert s.budget(1) == 1  # its guaranteed slot only
    s._held[1] = 1
    assert s.budget(1) == 0
    s._waiting[0] = 0
    assert s.budget(1) == 3


def test_priority1_yields_to_waiting_priority1_not_to_equal_priority() -> None:
    s = SlotScheduler(4)
    s._waiting[1] = 1
    assert s.budget(1) == 4  # same priority does not block
    assert s.budget(2) == 0  # lower priority does


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        ((0,), {}),
        ((5,), {"reserve_for_priority0": 4, "min_slots_for_lower": 2}),
        ((5,), {"reserve_for_priority0": -1}),
    ],
)
def test_invalid_split_rejected(args: tuple[int], kwargs: dict[str, int]) -> None:
    with pytest.raises(BrokerConfigError):
        SlotScheduler(*args, **kwargs)


async def test_acquire_waits_for_release_and_prefers_higher_priority() -> None:
    s = SlotScheduler(2)
    assert await s.acquire(0, 10) == 2
    order: list[int] = []

    async def take(priority: int) -> None:
        await s.acquire(priority, 1)
        order.append(priority)

    low = asyncio.create_task(take(1))
    await asyncio.sleep(0)
    high = asyncio.create_task(take(0))
    await asyncio.sleep(0.01)
    assert order == []
    s.release(0, 1)
    await asyncio.sleep(0.01)
    assert order == [0]  # the waiting higher priority wins the freed slot
    s.release(0, 1)
    await asyncio.wait_for(asyncio.gather(low, high), 1)
    assert order == [0, 1]
    assert s.held == 2


async def test_acquire_returns_at_most_budget() -> None:
    s = SlotScheduler(3)
    assert await s.acquire(1, 10) == 3
    s.release(1, 2)
    assert await s.acquire(1, 10) == 2
    assert await s.acquire(1, 0) == 0


async def test_resize_wakes_waiters() -> None:
    s = SlotScheduler(1)
    await s.acquire(0, 1)
    waiter = asyncio.create_task(s.acquire(0, 5))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    s.resize(3)
    assert await asyncio.wait_for(waiter, 1) == 2
    with pytest.raises(BrokerConfigError):
        s.resize(0)
