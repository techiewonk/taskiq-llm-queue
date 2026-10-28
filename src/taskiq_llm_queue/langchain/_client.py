"""The client interface the adapters call, and the process-wide default queue (D85).

Adapters type against `QueueClient` instead of importing `LLMQueue`, so they work with any object
that has the same methods (tests use a fake). Classes created at import time find the running
queue on first call through `get_default_queue`. This module never imports langchain or httpx.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from taskiq_llm_queue.errors import LLMQueueError

if TYPE_CHECKING:
    from taskiq_llm_queue.config import Lane
    from taskiq_llm_queue.envelope import CallMeta, FallbackPolicy


class ConverseResultLike(Protocol):
    """What `converse` returns: the exact Bedrock Converse dict and how it was served."""

    @property
    def response(self) -> dict[str, Any]: ...

    @property
    def meta(self) -> CallMeta: ...


@runtime_checkable
class QueueClient(Protocol):
    """The subset of `LLMQueue` the LangChain adapters and the HTTP transport use.

    `lane=None` means "take the lane from `current_context()`"; adapters pass only explicit
    overrides. Sync methods run on the caller's thread (direct path under the shared limiter).
    """

    async def converse(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API (D87)
        deadline: float | None = None,
    ) -> ConverseResultLike: ...

    def converse_sync(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> ConverseResultLike: ...

    async def embed(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API (D87)
    ) -> list[list[float]]: ...

    def embed_sync(
        self,
        model: str,
        texts: list[str],
        *,
        lane: Lane | str | None = None,
        input_type: str | None = None,
        timeout: float | None = None,
    ) -> list[list[float]]: ...

    async def chat(
        self,
        model: str,
        request: dict[str, Any],
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # frozen client API (D87)
        deadline: float | None = None,
    ) -> dict[str, Any]: ...


_default_queue: QueueClient | None = None


def set_default_queue(llmq: QueueClient | None) -> None:
    """Register the queue that adapters built without `llmq=` use; `None` clears it.

    `LLMQueue.start()` is expected to call this, and `stop()` to clear it.
    """
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
