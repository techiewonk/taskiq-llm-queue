"""Broker event hooks (A7): counts and timings for metrics, without a metrics dependency.

Subclass `BrokerHooks` and override what you need; the broker calls every hook through
`safe_call`, so a failing hook is logged and never breaks receiving, acking or sending.
Queue names are the `SQSQueue.name` routing keys, safe to use as a `queue` metric label.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from taskiq_llm_queue._logging import get_logger

logger = get_logger(__name__)


class BrokerHooks:
    """No-op callbacks for broker events; override the ones a metrics layer cares about."""

    def on_receive(self, queue: str, count: int) -> None:
        """`count` messages arrived from one `ReceiveMessage` call (0 for an empty poll)."""

    def on_queue_wait(self, queue: str, wait_secs: float) -> None:
        """One message waited `wait_secs` in SQS (now - `SentTimestamp`)."""

    def on_ack(self, queue: str, count: int) -> None:
        """`count` messages were deleted (acks after the task, and expired-message deletes)."""

    def on_heartbeat(self, queue: str, extended: int, lost: int) -> None:
        """A heartbeat batch extended `extended` messages; `lost` had invalid handles."""

    def on_release(self, queue: str, count: int, reason: str) -> None:
        """Messages made visible again; reason is shutdown, drain, unknown_task, manual..."""

    def on_expired(self, queue: str, count: int) -> None:
        """Messages past their `expiry` label were deleted without running (P11)."""

    def on_error(self, queue: str, operation: str, family: str) -> None:
        """An SQS call failed; `family` is an `ErrorFamily` value."""

    def on_send(self, queue: str, count: int) -> None:
        """`count` messages were sent (one call, batched or not)."""


def safe_call(hook: Callable[..., Any], *args: Any) -> None:
    """Run one hook, logging (not raising) anything it throws."""
    try:
        hook(*args)
    except Exception:
        logger.exception("broker_hook_failed", hook=getattr(hook, "__name__", repr(hook)))
