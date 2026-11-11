"""Exception base classes and provider-error classification shared by every layer.

Callers catch one type, `LLMQueueError` (calls raise `CallFailed`, a subclass with a `kind`);
workers turn provider exceptions into `ErrorInfo` so retry, budgets and health never depend on
SDK-specific exception classes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class LLMQueueError(Exception):
    """Base class for every error this package raises to callers."""


class ConfigError(LLMQueueError):
    """Invalid configuration, raised at startup or on a refused edit (never mid-call)."""


class UnsupportedEnvelopeError(LLMQueueError):
    """A message is newer than this worker understands; it is returned, never dropped."""


class ErrorKind(StrEnum):
    """How the worker reacts to a provider error: give back and retry, retry, or give up."""

    THROTTLE = "throttle"
    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Provider-neutral classification of one failed attempt.

    `code` keeps the provider's own spelling for logs only; decisions use `kind`, because
    fakes and real services spell the same failure differently.
    """

    kind: ErrorKind
    code: str = ""
    retry_after_secs: float | None = None
