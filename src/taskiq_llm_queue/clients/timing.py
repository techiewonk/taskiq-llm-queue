"""Attempt timers: time to the first response, then an idle limit between chunks (plan E117).

A healthy long answer keeps sending chunks and is never cut; a region that accepts the
connection and never answers is given up after `first_response_secs`. Raised errors are
`AttemptTimeout`, a `TimeoutError`, so they classify as retryable.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import TypeVar

_T = TypeVar("_T")  # PEP 695 syntax needs 3.12; the package supports 3.11


class AttemptTimeout(TimeoutError):
    """An attempt got no first response, or went idle mid-answer, for too long."""


@dataclass(frozen=True, slots=True)
class Timing:
    """Limits of one attempt: until the first response, and between later chunks."""

    first_response_secs: float = 60.0
    idle_secs: float = 30.0


async def first(awaitable: Awaitable[_T], secs: float, what: str) -> _T:
    """Await within `secs`, else raise `AttemptTimeout` naming `what`."""
    try:
        return await asyncio.wait_for(awaitable, secs)
    except TimeoutError as exc:
        raise AttemptTimeout(f"no {what} within {secs:g}s") from exc


async def paced(stream: AsyncIterator[_T], timing: Timing, *, started: bool) -> AsyncIterator[_T]:
    """Yield a stream's items, each within the idle limit.

    The first item gets the first-response limit unless `started` (the response already came).
    """
    iterator = stream.__aiter__()
    limit = timing.idle_secs if started else timing.first_response_secs
    what = "chunk" if started else "first response"
    while True:
        try:
            item = await first(iterator.__anext__(), limit, what)
        except StopAsyncIteration:
            return
        yield item
        limit, what = timing.idle_secs, "chunk"
