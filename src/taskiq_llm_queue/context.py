"""Per-request context (priority, session, user, tenant) carried through tasks and threads.

Routers set it once with `request_context(...)`; queued calls and the LangChain adapters read
it when the call itself does not say. Raw threads must run under `contextvars.copy_context()`.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class RequestContext:
    """What the current request is: its class and the ids used for sticky sessions and records."""

    priority: bool = False
    session_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None


_CURRENT: ContextVar[RequestContext | None] = ContextVar("taskiq_llm_queue_context", default=None)


def current_context() -> RequestContext:
    """The active context, or the default (normal priority, no ids)."""
    return _CURRENT.get() or RequestContext()


@contextmanager
def request_context(
    *,
    priority: bool | None = None,
    session_id: str | None = None,
    user_id: str | None = None,
    tenant_id: str | None = None,
) -> Iterator[RequestContext]:
    """Set priority and ids for everything run inside the block; unset fields are inherited."""
    base = current_context()
    ctx = replace(
        base,
        priority=base.priority if priority is None else priority,
        session_id=session_id if session_id is not None else base.session_id,
        user_id=user_id if user_id is not None else base.user_id,
        tenant_id=tenant_id if tenant_id is not None else base.tenant_id,
    )
    token = _CURRENT.set(ctx)
    try:
        yield ctx
    finally:
        _CURRENT.reset(token)
