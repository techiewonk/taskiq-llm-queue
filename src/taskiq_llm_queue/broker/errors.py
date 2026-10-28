"""Broker exceptions and SQS error-family classification.

ElasticMQ, moto and AWS spell the same failure differently (an oversized batch is
`InvalidAttributeValue`, `InvalidParameterValue` or `BatchRequestTooLong`), so every broker
decision uses `ErrorFamily`, never the raw code. The raw code is kept for logs only.
"""

from __future__ import annotations

import random
import re
from collections.abc import Callable
from enum import StrEnum

from taskiq_llm_queue.errors import ConfigError, LLMQueueError, QueueUnavailable


class BrokerConfigError(ConfigError):
    """Invalid broker or queue settings (limits, priorities, names), raised at construction."""


class InvalidLabelError(LLMQueueError, ValueError):
    """A task label the broker reads (`delay`, `group_id`, `expiry`...) is out of range.

    Raised by `kick`, i.e. by the caller's `.kiq()`, so a bad label never reaches SQS.
    """


class MessageTooLargeError(LLMQueueError, ValueError):
    """One serialized message is over the SQS 1 MiB limit; offload the payload instead."""


class SQSSendError(QueueUnavailable):
    """SQS refused or failed a send; `family` says whether retrying can help."""

    def __init__(self, message: str, *, family: ErrorFamily, code: str = "") -> None:
        """Keep the error family for decisions and the provider's code for logs."""
        super().__init__(message)
        self.family = family
        self.code = code


class ErrorFamily(StrEnum):
    """What kind of SQS failure this is, independent of which fake or region spelled it.

    `TRANSIENT` (5xx, connection errors) and `THROTTLED` are both retried with backoff.
    """

    SIZE_LIMIT = "size_limit"
    INVALID_HANDLE = "invalid_handle"
    THROTTLED = "throttled"
    TRANSIENT = "transient"
    NOT_FOUND = "not_found"
    OTHER = "other"

    @property
    def retryable(self) -> bool:
        """True for families where the same call may succeed after a pause."""
        return self in (ErrorFamily.THROTTLED, ErrorFamily.TRANSIENT)


_SIZE_CODES = frozenset(
    {"batchrequesttoolong", "messagetoolong", "requestentitytoolarge", "requesttoolarge"}
)
# Generic "bad parameter" codes that mean "too big" only when the message says so.
_GENERIC_CODES = frozenset({"invalidattributevalue", "invalidparametervalue"})
_SIZE_TEXT = re.compile(
    r"too ?long|shorter than|too large|exceed\w* .*(size|bytes|limit)|maximum .*size", re.I
)
_HANDLE_CODES = frozenset(
    {"receipthandleisinvalid", "invalidreceipthandle", "messagenotinflight", "invalididformat"}
)
_HANDLE_TEXT = re.compile(r"receipt ?handle|not ?in ?flight", re.I)
_THROTTLE_CODES = frozenset(
    {
        "requestthrottled",
        "throttlingexception",
        "throttling",
        "throttled",
        "slowdown",
        "overlimit",
        "toomanyrequestsexception",
        "requestlimitexceeded",
        "kmsthrottlingexception",
    }
)
_TRANSIENT_CODES = frozenset(
    {
        "internalerror",
        "internalfailure",
        "serviceunavailable",
        "serviceunavailableexception",
        "requesttimeout",
        "requesttimeoutexception",
    }
)
_NOT_FOUND_CODES = frozenset(
    {
        "queuedoesnotexist",
        "nonexistentqueue",
        "resourcenotfoundexception",
        "queuedeletedrecently",
    }
)


def classify_code(code: str, message: str = "", status: int | None = None) -> ErrorFamily:
    """Map one SQS error code (and its message / HTTP status) to an `ErrorFamily`.

    Accepts both legacy (`AWS.SimpleQueueService.NonExistentQueue`) and JSON-protocol
    spellings; used for whole-call errors and for `Failed[]` batch entries alike.
    """
    key = code.rsplit(".", 1)[-1].lower()
    if key in _SIZE_CODES or (key in _GENERIC_CODES and _SIZE_TEXT.search(message)):
        return ErrorFamily.SIZE_LIMIT
    if key in _HANDLE_CODES or (key in _GENERIC_CODES and _HANDLE_TEXT.search(message)):
        return ErrorFamily.INVALID_HANDLE
    if key in _THROTTLE_CODES or status == 429:
        return ErrorFamily.THROTTLED
    if key in _NOT_FOUND_CODES:
        return ErrorFamily.NOT_FOUND
    if key in _TRANSIENT_CODES or (status is not None and status >= 500):
        return ErrorFamily.TRANSIENT
    return ErrorFamily.OTHER


def error_code(exc: BaseException) -> str:
    """The provider's error code of a botocore `ClientError`, or the exception class name."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code", "")) or type(exc).__name__
    return type(exc).__name__


def classify_error(exc: BaseException) -> ErrorFamily:
    """Classify any exception raised by an SQS call into an `ErrorFamily`.

    `ClientError` is read through its response; connection and read-timeout errors are
    `TRANSIENT`; anything else is `OTHER`.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error", {})
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return classify_code(
            str(error.get("Code", "")),
            str(error.get("Message", "")),
            status if isinstance(status, int) else None,
        )
    # Imported here so this module stays importable (and testable) without botocore loaded.
    from botocore.exceptions import (
        ConnectionClosedError,
        EndpointConnectionError,
        ReadTimeoutError,
    )

    if isinstance(
        exc,
        EndpointConnectionError | ConnectionClosedError | ReadTimeoutError | ConnectionError,
    ):
        return ErrorFamily.TRANSIENT
    if isinstance(exc, TimeoutError):
        return ErrorFamily.TRANSIENT
    return ErrorFamily.OTHER


def backoff_delay(
    attempt: int,
    *,
    base_secs: float,
    cap_secs: float,
    rand: Callable[[float, float], float] = random.uniform,
) -> float:
    """Full-jitter exponential backoff: uniform(0, min(cap, base * 2**attempt)).

    Each poller and flusher keeps its own attempt counter, so pods spread their retries.
    """
    return rand(0.0, min(cap_secs, base_secs * (2 ** min(attempt, 30))))
