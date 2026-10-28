"""Unit tests for `is_target_error`: which fatal errors move a call to the next target (S38).

Bug S38: `run_call` ended the call on any FATAL error, so a model missing in one region never
reached the APAC profile. Only errors about the target may fall through; request errors end.
"""

from __future__ import annotations

from typing import Any

import httpx
import openai
import pytest
from botocore.exceptions import ClientError

from taskiq_llm_queue import ErrorKind
from taskiq_llm_queue.errors import ErrorInfo
from taskiq_llm_queue.providers import classify_bedrock, classify_openai
from taskiq_llm_queue.providers.classify import is_target_error


def _bedrock(code: str, message: str, status: int) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": {}},
    }
    return ClientError(response, "Converse")


def _openai(status: int, code: str | None, message: str) -> openai.APIStatusError:
    request = httpx.Request("POST", "http://h/v1/chat/completions")
    response = httpx.Response(status, request=request)
    body = {"error": {"message": message, "code": code, "type": "invalid_request_error"}}
    return openai.APIStatusError(message, response=response, body=body["error"])


@pytest.mark.parametrize(
    ("code", "message", "status", "target"),
    [
        ("ResourceNotFoundException", "The specified model was not found.", 404, True),
        ("AccessDeniedException", "You don't have access to the model with the ID.", 403, True),
        ("ValidationException", "The provided model identifier is invalid.", 400, True),
        (
            "ValidationException",
            "Invocation of model ID x with on-demand throughput isn't supported. Retry your "
            "request with the ID or ARN of an inference profile that contains this model.",
            400,
            True,
        ),
        ("ValidationException", "Model is not available in this region.", 400, True),
        ("ValidationException", "This model version has reached the end of its life.", 400, True),
        ("UnknownOperationException", "", 404, True),
        ("ValidationException", "The request failed validation.", 400, False),
        ("ValidationException", "messages must be a non-empty list", 400, False),
        ("ValidationException", "Input is too long for requested model.", 400, False),
        ("ValidationException", "The model returned: prompt is too long", 400, False),
        ("ValidationException", "Malformed input request: model field missing", 400, False),
        (
            "ExpiredTokenException",
            "The security token included in the request is expired",
            403,
            False,
        ),
        ("UnrecognizedClientException", "The security token is invalid.", 403, False),
    ],
)
def test_bedrock_target_errors(code: str, message: str, status: int, target: bool) -> None:
    """Each Bedrock fatal code: model/region/profile problems fall through, request ones end."""
    exc = _bedrock(code, message, status)
    info = classify_bedrock(exc)
    assert info.kind is ErrorKind.FATAL
    assert is_target_error(exc, info) is target


@pytest.mark.parametrize(
    ("status", "code", "message", "target"),
    [
        (404, "model_not_found", "The model `x` does not exist", True),
        (404, None, "Not Found", True),
        (403, "permission_denied", "You don't have access to the model", True),
        (400, "invalid_request", "The model `llama-x` does not exist or you lack access", True),
        (400, "invalid_request", "messages must be a non-empty list", False),
        (400, "context_length_exceeded", "maximum context length is 8192 tokens", False),
        (401, "invalid_api_key", "Incorrect API key provided", False),
    ],
)
def test_openai_target_errors(status: int, code: str | None, message: str, target: bool) -> None:
    """Each OpenAI-compatible status: unknown model / endpoint falls through, bad input ends."""
    exc = _openai(status, code, message)
    info = classify_openai(exc)
    assert info.kind is ErrorKind.FATAL
    assert is_target_error(exc, info) is target


def test_only_fatal_errors_can_fall_through() -> None:
    """Throttles and retryable errors keep their own handling, whatever the message says."""
    exc = _bedrock("ThrottlingException", "model not found", 429)
    assert not is_target_error(exc, classify_bedrock(exc))
    assert not is_target_error(ValueError("x"), ErrorInfo(ErrorKind.RETRYABLE, "x"))
