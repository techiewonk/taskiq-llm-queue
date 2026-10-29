"""Exception tree and provider-error classification shared by every layer.

Callers catch one type, `LLMQueueError`; workers turn provider exceptions into `ErrorInfo` so
the limiter, retry and fallback logic never depend on SDK-specific exception classes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskiq_llm_queue.envelope import CallMeta


class LLMQueueError(Exception):
    """Base class for every error this package raises to callers.

    `meta` is how a worker served the failed call (attempts, queue wait, last target tried);
    None when no worker answered (e.g. the caller timed out waiting).
    """

    meta: CallMeta | None = None


class LLMQueueTimeout(LLMQueueError):  # public name is part of the frozen API
    """The caller waited longer than its lane timeout or deadline for a result."""


class LLMQueueCancelled(LLMQueueError):
    """The call was cancelled (`LLMQueue.cancel`, or its caller went away) before it finished."""


class QueueUnavailable(LLMQueueError):  # public name is part of the frozen API
    """SQS or Redis/Valkey could not be reached and no fallback path was allowed."""


class ConfigError(LLMQueueError):
    """Invalid configuration, raised at startup (never on a live request path)."""


class UnsupportedEnvelopeError(LLMQueueError):
    """A message carries an envelope version newer than this worker understands.

    Workers release such messages for another pod instead of acking them, so a rolling deploy
    never drops work.
    """


class ProviderError(LLMQueueError):
    """The provider call failed after the worker's retries and fallbacks were exhausted."""

    def __init__(
        self, message: str, *, retryable: bool, retry_after_secs: float | None = None
    ) -> None:
        """Keep whether the caller may retry later and any provider-advised wait."""
        super().__init__(message)
        self.retryable = retryable
        self.retry_after_secs = retry_after_secs


class ErrorKind(StrEnum):
    """How the worker reacts to a provider error: shrink limits, retry, or give up."""

    THROTTLE = "throttle"
    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Provider-neutral classification of one failed call, produced by `Provider.classify`.

    `code` keeps the provider's own spelling for logs only; decisions use `kind`, because
    fakes and real services spell the same failure differently.
    """

    kind: ErrorKind
    code: str = ""
    retry_after_secs: float | None = None
