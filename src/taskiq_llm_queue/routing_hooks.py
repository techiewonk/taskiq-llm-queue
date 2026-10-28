"""Extension points of `LLMQueue`: path routing, SQS/Valkey failover and observability hooks.

`PathRouter` (canary routing) and `FailoverPolicy` are implemented in `routing.py` /
`failover.py`; `QueueHooks` is subclassed by the metrics layer. This module has no heavy
imports, so implementations can depend on it freely.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

import structlog

if TYPE_CHECKING:
    from taskiq_llm_queue.config import Lane

logger = structlog.get_logger(__name__)

CallPath = Literal["sqs", "direct"]


@runtime_checkable
class PathRouter(Protocol):
    """Chooses how one call runs: through its model's SQS queue or `direct` in this process.

    Without a router `LLMQueue` uses `sqs` when the model has queues, else `direct`.
    """

    def route(
        self, model: str, lane: Lane, session_id: str | None, user_id: str | None
    ) -> CallPath:
        """Path for one call of model alias `model` in `lane` (sticky per session)."""
        ...


@runtime_checkable
class FailoverPolicy(Protocol):
    """Sender-side failover state: SQS outages per model and a pod-wide Valkey outage.

    `LLMQueue` reports failures here and asks `path_override` / `redis_down` before each call;
    implementations must never block or raise.
    """

    def path_override(self, model: str) -> Literal["direct"] | None:
        """`direct` while `model` is failed over from SQS, else None (use the router)."""
        ...

    def report_sqs_failure(self, model: str, exc: BaseException) -> None:
        """An SQS send for `model` failed; may start a failover window."""
        ...

    def report_redis_failure(self, exc: BaseException) -> None:
        """A Valkey call failed; may switch the pod to Valkey-down mode (DS18)."""
        ...

    def redis_down(self) -> bool:
        """True while the pod treats Valkey as unreachable."""
        ...


class QueueHooks:
    """No-op callbacks for client and worker events; the metrics layer overrides what it needs.

    Every hook is called through `safe_hook`, so a failing hook never breaks a call. Labels are
    low-cardinality only (model alias, target, lane, path, kind, step).
    """

    def on_call(self, model: str, lane: str, path: str, ok: bool, secs: float) -> None:
        """One client call finished (`ok` False for errors and timeouts)."""

    def on_attempt(self, model: str, target: str, kind: str, secs: float) -> None:
        """One provider attempt finished; `kind` is `ok` or an `ErrorKind` value."""

    def on_fallback(self, model: str, step: str) -> None:
        """A call was served by fallback step `step` (`alias@target`) instead of the first."""

    def on_queue_wait_ms(self, model: str, lane: str, wait_ms: int) -> None:
        """A worker started a task `wait_ms` after it was enqueued."""

    def on_sync_fallback(self, model: str) -> None:
        """A sync call ran `direct` under the shared limiter (`llmq_sync_fallback_total`)."""


def safe_hook(hook: Callable[..., Any], *args: Any) -> None:
    """Run one hook, logging (never raising) whatever it throws."""
    try:
        hook(*args)
    except Exception:
        logger.exception("routing_hook_failed", hook=getattr(hook, "__name__", repr(hook)))
