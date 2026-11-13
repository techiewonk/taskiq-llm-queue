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


class UnknownModel(ConfigError, KeyError):  # named for what it is
    """A model name that is neither an alias nor a model id in the registry.

    Also a `KeyError`, so code that looks models up by key keeps working (HTTP 404, CLI).
    """

    def __str__(self) -> str:
        return f"unknown model {self.args[0]!r}"


class StoreUnavailable(LLMQueueError, ConnectionError):  # named for what it is
    """The state store could not serve an admin operation (unreachable, read-only, full).

    Raised by `cancel`, `status`, `send_job` and the registry's edits and reads; a call
    raises `CallFailed` instead. Also a `ConnectionError`, so existing handlers keep working.
    """


class MissingExtra(ConfigError, ImportError):  # named for what it is
    """An optional feature's dependencies are not installed; names the extra to install.

    Also an `ImportError`, so `except ImportError` around optional imports keeps working.
    """

    def __init__(self, feature: str, extra: str) -> None:
        """`feature` needs `pip install 'taskiq-llm-queue[extra]'`."""
        super().__init__(f"{feature} needs: pip install 'taskiq-llm-queue[{extra}]'")


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
