"""Error table shared by the Bedrock and OpenAI-compatible fronts of fakellm.

One row per Bedrock error name (TESTING.md "Error fidelity"), with the HTTP status and
the OpenAI-shaped equivalent, so a scenario names an error once and both fronts agree.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    """How one named error is rendered by the Bedrock and the OpenAI fronts."""

    status: int
    message: str
    openai_status: int
    openai_type: str
    openai_code: str


ERRORS: dict[str, ErrorSpec] = {
    "ThrottlingException": ErrorSpec(
        429,
        "Too many requests, please wait before trying again.",
        429,
        "requests",
        "rate_limit_exceeded",
    ),
    "ModelNotReadyException": ErrorSpec(
        429,
        "Model is not ready to serve inference requests. Retry later.",
        429,
        "requests",
        "model_not_ready",
    ),
    "ServiceUnavailableException": ErrorSpec(
        503,
        "Service is temporarily unavailable. Try again later.",
        503,
        "server_error",
        "service_unavailable",
    ),
    "InternalServerException": ErrorSpec(
        500,
        "An internal server error occurred. Retry your request.",
        500,
        "server_error",
        "internal_error",
    ),
    "ModelTimeoutException": ErrorSpec(
        408,
        "Model has timed out in processing the request. Try your request again.",
        408,
        "timeout",
        "timeout",
    ),
    "ModelErrorException": ErrorSpec(
        424,
        "The model returned an error while processing the request.",
        500,
        "server_error",
        "model_error",
    ),
    "ValidationException": ErrorSpec(
        400,
        "The request failed validation.",
        400,
        "invalid_request_error",
        "invalid_request",
    ),
    "AccessDeniedException": ErrorSpec(
        403,
        "You don't have access to the model with the specified model ID.",
        403,
        "invalid_request_error",
        "permission_denied",
    ),
    "ResourceNotFoundException": ErrorSpec(
        404,
        "The specified model was not found.",
        404,
        "invalid_request_error",
        "model_not_found",
    ),
}

# Errors counted as "throttled" in the stats rather than "errors".
THROTTLE_ERRORS = frozenset({"ThrottlingException", "ModelNotReadyException"})
