"""Error-family classification across ElasticMQ, moto and AWS spellings, and backoff bounds."""

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from taskiq_llm_queue.broker import ErrorFamily, backoff_delay, classify_code, classify_error


def _client_error(code: str, message: str = "", status: int = 400) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "SendMessageBatch")


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("InvalidAttributeValue", "MessageTooLong"),  # ElasticMQ 1.7.1 (measured)
        (
            "InvalidParameterValue",
            "One or more parameters are invalid. Reason: Message must be shorter than "
            "1048576 bytes.",
        ),  # moto
        ("BatchRequestTooLong", "Batch requests cannot be longer than 1048576 bytes."),  # moto
        ("AWS.SimpleQueueService.BatchRequestTooLong", ""),  # AWS legacy query spelling
        ("MessageTooLong", ""),
    ],
)
def test_size_limit_all_spellings(code: str, message: str) -> None:
    assert classify_code(code, message) is ErrorFamily.SIZE_LIMIT
    assert classify_error(_client_error(code, message)) is ErrorFamily.SIZE_LIMIT


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("ReceiptHandleIsInvalid", 'The receipt handle "bogus" is not valid.'),  # ElasticMQ
        ("AWS.SimpleQueueService.MessageNotInflight", ""),
        ("MessageNotInflight", ""),
        ("InvalidParameterValue", "Value bogus for parameter ReceiptHandle is invalid."),
        ("ReceiptHandleIsInvalid", ""),
    ],
)
def test_invalid_handle_spellings(code: str, message: str) -> None:
    assert classify_code(code, message) is ErrorFamily.INVALID_HANDLE


@pytest.mark.parametrize(
    ("code", "status", "family"),
    [
        ("RequestThrottled", 400, ErrorFamily.THROTTLED),
        ("AWS.SimpleQueueService.RequestThrottled", 403, ErrorFamily.THROTTLED),
        ("ThrottlingException", 400, ErrorFamily.THROTTLED),
        ("Anything", 429, ErrorFamily.THROTTLED),
        ("QueueDoesNotExist", 400, ErrorFamily.NOT_FOUND),  # ElasticMQ / JSON protocol
        ("AWS.SimpleQueueService.NonExistentQueue", 400, ErrorFamily.NOT_FOUND),
        ("InternalError", 500, ErrorFamily.TRANSIENT),
        ("ServiceUnavailable", 503, ErrorFamily.TRANSIENT),
        ("Unknown", 502, ErrorFamily.TRANSIENT),
        ("AccessDenied", 403, ErrorFamily.OTHER),
    ],
)
def test_other_families(code: str, status: int, family: ErrorFamily) -> None:
    assert classify_error(_client_error(code, "", status)) is family


def test_generic_invalid_parameter_is_not_size() -> None:
    """FIFO dedup errors share InvalidParameterValue with size errors; message decides."""
    message = (
        "The queue should either have ContentBasedDeduplication enabled or "
        "MessageDeduplicationId provided explicitly"
    )
    assert classify_code("InvalidParameterValue", message) is ErrorFamily.OTHER


def test_connection_errors_are_transient() -> None:
    exc = EndpointConnectionError(endpoint_url="http://localhost:1")
    assert classify_error(exc) is ErrorFamily.TRANSIENT
    assert classify_error(TimeoutError()) is ErrorFamily.TRANSIENT
    assert classify_error(ValueError("x")) is ErrorFamily.OTHER


def test_retryable_flag() -> None:
    assert ErrorFamily.THROTTLED.retryable
    assert ErrorFamily.TRANSIENT.retryable
    assert not ErrorFamily.SIZE_LIMIT.retryable
    assert not ErrorFamily.INVALID_HANDLE.retryable


def test_backoff_full_jitter_bounds() -> None:
    upper = [backoff_delay(a, base_secs=0.5, cap_secs=4, rand=lambda lo, hi: hi) for a in range(6)]
    assert upper == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]
    assert backoff_delay(3, base_secs=0.5, cap_secs=4, rand=lambda lo, hi: lo) == 0.0
    assert backoff_delay(10_000, base_secs=1, cap_secs=20, rand=lambda lo, hi: hi) == 20
