"""Per-request context (lane, session, user) carried through async tasks and threads.

Routers set it once with `request_context(...)`; queued adapters read it on every call to pick
the lane and the canary bucket. Raw threads must run under `contextvars.copy_context().run`.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace

from taskiq_llm_queue.config import Lane


@dataclass(frozen=True, slots=True)
class RequestContext:
    """What the current request is: its lane and the ids used for routing and tracing.

    `defaulted` is True when no `request_context` was set, so callers can warn once about
    calls that silently land in the background lane.
    """

    lane: Lane = Lane.BG
    session_id: str | None = None
    user_id: str | None = None
    defaulted: bool = True


_CURRENT: ContextVar[RequestContext | None] = ContextVar("taskiq_llm_queue_context", default=None)


def current_context() -> RequestContext:
    """The active context, or the default (`bg` lane, `defaulted=True`) when none is set."""
    return _CURRENT.get() or RequestContext()


@contextmanager
def request_context(
    *,
    lane: Lane | str | None = None,
    session_id: str | None = None,
    user_id: str | None = None,
) -> Iterator[RequestContext]:
    """Set the lane and ids for everything run inside the block; unset fields are inherited.

    Nesting is allowed, so a background job inside a request can switch only the lane.
    """
    base = current_context()
    ctx = replace(
        base,
        lane=Lane(lane) if lane is not None else base.lane,
        session_id=session_id if session_id is not None else base.session_id,
        user_id=user_id if user_id is not None else base.user_id,
        defaulted=False,
    )
    token = _CURRENT.set(ctx)
    try:
        yield ctx
    finally:
        _CURRENT.reset(token)
