"""The client interface the adapters call, and the process-wide default queue.

Adapters type against `QueueClient` instead of importing `LLMQueue`, so they work with any object
that has the same methods (tests use a fake). Classes created at import time find the running
queue on first call through `get_default_queue`. This module never imports langchain or httpx.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, cast, runtime_checkable

from taskiq_llm_queue.errors import LLMQueueError


class ReplyLike(Protocol):
    """What `converse` / `chat` return: the provider-native body and how it was served."""

    @property
    def response(self) -> dict[str, Any] | None: ...

    @property
    def meta(self) -> Any: ...


class EmbedResultLike(Protocol):
    """What `embed` returns: one vector per text, in input order."""

    @property
    def vectors(self) -> list[list[float]]: ...


@runtime_checkable
class QueueClient(Protocol):
    """The subset of `LLMQueue` the LangChain adapters and the HTTP transport use.

    `priority=None` takes it from `request_context`. Sync methods run on the direct path.
    """

    async def converse(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> ReplyLike: ...

    def converse_sync(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> ReplyLike: ...

    async def chat(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> ReplyLike: ...

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        **kw: Any,
    ) -> EmbedResultLike: ...

    def embed_sync(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        **kw: Any,
    ) -> EmbedResultLike: ...


_default_queue: QueueClient | None = None


def set_default_queue(llmq: QueueClient | None) -> None:
    """Register the queue that adapters built without `llmq=` use; `None` clears it."""
    global _default_queue
    _default_queue = llmq


def get_default_queue() -> QueueClient:
    """The registered default queue; raises `LLMQueueError` when none has been set yet."""
    if _default_queue is None:
        raise LLMQueueError(
            "no LLM queue: pass llmq=... to the adapter or call "
            "taskiq_llm_queue.langchain.set_default_queue(llmq) (LLMQueue.start() does this)"
        )
    return _default_queue


def resolve_queue(explicit: Any) -> QueueClient:
    """The adapter's own queue if it has one, else the default, looked up on every call."""
    return cast("QueueClient", explicit) if explicit is not None else get_default_queue()
