"""Map Bedrock and OpenAI SDK exceptions to `ErrorInfo` (throttle / retryable / fatal).

Decisions use error *families*, not exact class names or codes, because real services and
fakes spell the same failure differently (`ThrottlingException`, `Throttling`, `throttled`).
Used by `BedrockProvider.classify` and `OpenAICompatProvider.classify`; never imports an SDK.
"""

from __future__ import annotations

import re
import sys
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from typing import Any

from taskiq_llm_queue.errors import ErrorInfo, ErrorKind

# Prefixes of normalised codes (lower-case letters only, "exception"/"error" suffix dropped).
_THROTTLE_PREFIXES = (
    "throttl",
    "toomanyrequests",
    "modelnotready",
    "servicequotaexceeded",
    "requestlimitexceeded",
    "provisionedthroughputexceeded",
    "ratelimit",
    "slowdown",
)
_RETRYABLE_PREFIXES = (
    "serviceunavailable",
    "internal",
    "modeltimeout",
    "modelerror",
    "modelstreamerror",
    "requesttimeout",
    "timeout",
    "serviceerror",
    "unavailable",
)
_FATAL_PREFIXES = (
    "validation",
    "accessdenied",
    "resourcenotfound",
    "unrecognizedclient",
    "notauthorized",
    "unauthorized",
    "expiredtoken",
    "invalid",
    "badrequest",
)
# Built lazily from whichever modules are already imported; an SDK that was never imported
# cannot have raised the exception being classified.
_TRANSPORT_TYPES: dict[str, tuple[str, ...]] = {
    "botocore.exceptions": (
        "ReadTimeoutError",
        "ConnectTimeoutError",
        "EndpointConnectionError",
        "ConnectionClosedError",
        "HTTPClientError",
    ),
    "aiohttp": ("ClientError",),
    "httpx": ("TransportError",),
    "openai": ("APITimeoutError", "APIConnectionError"),
}


def error_family(code: str) -> str:
    """Normalise a provider error code: letters only, lower case, no exception suffix.

    `ThrottlingException`, `throttling_exception` and `Throttling` all become `throttling`.
    """
    family = re.sub(r"[^a-z]", "", code.lower())
    for suffix in ("exception", "error", "fault"):
        if family.endswith(suffix) and len(family) > len(suffix):
            return family[: -len(suffix)]
    return family


def kind_for_code(code: str, status: int | None = None) -> ErrorKind | None:
    """Classify a provider error code by family, falling back to the HTTP status.

    Returns None when neither the code nor the status says anything.
    """
    family = error_family(code)
    if family:
        for prefixes, kind in (
            (_THROTTLE_PREFIXES, ErrorKind.THROTTLE),
            (_RETRYABLE_PREFIXES, ErrorKind.RETRYABLE),
            (_FATAL_PREFIXES, ErrorKind.FATAL),
        ):
            if family.startswith(prefixes):
                return kind
    return kind_for_status(status)


def kind_for_status(status: int | None) -> ErrorKind | None:
    """Classify by HTTP status alone: 429 throttle, 408/409/5xx retryable, other 4xx fatal."""
    if status is None:
        return None
    if status == 429:
        return ErrorKind.THROTTLE
    if status in (408, 409) or status >= 500:
        return ErrorKind.RETRYABLE
    if 400 <= status < 500:
        return ErrorKind.FATAL
    return None


def parse_retry_after(
    headers: Mapping[str, Any] | None, *, now: float | None = None
) -> float | None:
    """Seconds to wait from `retry-after-ms`, else `Retry-After` (seconds or an HTTP date).

    Header names are matched case-blind; unparsable values are ignored and past dates give 0.
    """
    if not headers:
        return None
    lowered = {str(k).lower(): str(v).strip() for k, v in headers.items()}
    if (raw_ms := lowered.get("retry-after-ms")) is not None:
        try:
            return max(float(raw_ms) / 1000.0, 0.0)
        except ValueError:
            pass
    raw = lowered.get("retry-after")
    if raw is None:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return None
    current = time.time() if now is None else now
    return max(when.timestamp() - current, 0.0)


def is_transport_error(exc: BaseException) -> bool:
    """True for timeouts and connection failures from asyncio, botocore, aiohttp, httpx, openai."""
    if isinstance(exc, TimeoutError | ConnectionError):
        return True
    for module_name, names in _TRANSPORT_TYPES.items():
        module = sys.modules.get(module_name)
        if module is None:
            continue
        types = tuple(t for n in names if isinstance(t := getattr(module, n, None), type))
        if types and isinstance(exc, types):
            return True
    return False


def classify_bedrock(exc: BaseException) -> ErrorInfo:
    """Classify a botocore `ClientError` (or transport error) from a Bedrock runtime call.

    Unknown codes fall back to the HTTP status; anything else that is not a transport error is
    fatal, so a bug in the request never loops through retries.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping) and isinstance(response.get("Error"), Mapping):
        code = str(response["Error"].get("Code") or "")
        meta = response.get("ResponseMetadata") or {}
        status = meta.get("HTTPStatusCode")
        kind = kind_for_code(code, status if isinstance(status, int) else None)
        retry_after = parse_retry_after(meta.get("HTTPHeaders"))
        return ErrorInfo(kind=kind or ErrorKind.FATAL, code=code, retry_after_secs=retry_after)
    if is_transport_error(exc):
        return ErrorInfo(kind=ErrorKind.RETRYABLE, code=type(exc).__name__)
    return ErrorInfo(kind=ErrorKind.FATAL, code=type(exc).__name__)


def classify_openai(exc: BaseException) -> ErrorInfo:
    """Classify an openai SDK exception (or transport error) from an OpenAI-compatible server.

    429 is a throttle carrying the server's advised wait, except `insufficient_quota`, which
    no wait fixes. 408/409/5xx, timeouts and connection errors are retryable; other 4xx fatal.
    """
    if is_transport_error(exc):
        return ErrorInfo(kind=ErrorKind.RETRYABLE, code=type(exc).__name__)
    status = getattr(exc, "status_code", None)
    if not isinstance(status, int):
        return ErrorInfo(kind=ErrorKind.FATAL, code=type(exc).__name__)
    code = str(getattr(exc, "code", None) or getattr(exc, "type", None) or type(exc).__name__)
    response = getattr(exc, "response", None)
    retry_after = parse_retry_after(getattr(response, "headers", None))
    kind = kind_for_status(status) or ErrorKind.FATAL
    if kind is ErrorKind.THROTTLE and code == "insufficient_quota":
        kind = ErrorKind.FATAL
    return ErrorInfo(kind=kind, code=code, retry_after_secs=retry_after)
