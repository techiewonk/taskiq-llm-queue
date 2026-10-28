"""Wire contract: the task envelope sent through SQS and the result returned through Redis.

Both sides of a rolling deploy read these, so the format is versioned (`v`). A worker accepts
its own version and the previous one; anything newer raises `UnsupportedEnvelopeError` so the
worker releases the message instead of dropping it.
"""

from __future__ import annotations

import json
import time
from typing import Any, Literal

import xxhash
from pydantic import BaseModel, ConfigDict, Field, model_validator

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.errors import UnsupportedEnvelopeError

ENVELOPE_VERSION = 1
TASK_CALL = "taskiq_llm_queue.call"
TASK_EMBED = "taskiq_llm_queue.embed"
TASK_JOB = "taskiq_llm_queue.job"

Op = Literal["converse", "invoke_model", "chat", "embeddings"]
FallbackPolicy = Literal["same_model", "chain"]


class Envelope(BaseModel):
    """One LLM call as a task argument; `params` are provider-native and passed through as-is.

    Large params travel by reference (`params_ref`, a Redis key) instead of inline.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    v: int = ENVELOPE_VERSION
    request_id: str
    parent_request_id: str | None = None
    session_id: str | None = None
    user_id: str | None = None
    reply_to: str | None = None
    lane: Lane = Lane.BG
    op: Op
    fallback: FallbackPolicy = "same_model"
    model: str
    params: dict[str, Any] | None = None
    params_ref: str | None = None
    est_tokens: int = Field(default=0, ge=0)
    enqueued_at: float = Field(default_factory=time.time)
    deadline_at: float
    trace: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _params_xor_ref(self) -> Envelope:
        if (self.params is None) == (self.params_ref is None):
            raise ValueError("exactly one of params / params_ref must be set")
        if self.deadline_at < self.enqueued_at:
            raise ValueError("deadline_at is before enqueued_at")
        return self


class CallMeta(BaseModel):
    """How a call was actually served; surfaced as `response_metadata` / `ConverseResult.meta`."""

    model_config = ConfigDict(frozen=True)

    served_model: str = ""
    served_target: str = ""
    fallback_used: bool = False
    queue_wait_ms: int = 0
    provider_ms: int = 0
    attempts: int = 0
    worker: str = ""


class LLMReply(BaseModel):
    """Result stored by the worker: the provider-native response, or a classified error."""

    model_config = ConfigDict(frozen=True)

    request_id: str
    ok: bool
    response: dict[str, Any] | None = None
    meta: CallMeta = Field(default_factory=CallMeta)
    error: dict[str, Any] | None = None
    cancelled: bool = False


def parse_envelope(data: bytes | str | dict[str, Any]) -> Envelope:
    """Decode an envelope, enforcing the version rule before field validation.

    Raises `UnsupportedEnvelopeError` for a newer version and `ValueError` for malformed input.
    """
    raw = json.loads(data) if isinstance(data, bytes | str) else data
    if not isinstance(raw, dict):
        raise ValueError("envelope is not a JSON object")
    version = raw.get("v")
    if not isinstance(version, int):
        raise ValueError("envelope has no integer 'v'")
    if version > ENVELOPE_VERSION:
        raise UnsupportedEnvelopeError(f"envelope v{version} is newer than v{ENVELOPE_VERSION}")
    if version < ENVELOPE_VERSION - 1:
        raise ValueError(f"envelope v{version} is no longer supported")
    return Envelope.model_validate(raw)


def hashed_id(value: str | None) -> str | None:
    """Stable, non-reversible id for labels, logs and metrics (raw ids stay in the body)."""
    return None if value is None else xxhash.xxh3_64_hexdigest(value.encode("utf-8"))


def task_labels(envelope: Envelope, *, hash_ids: bool = True) -> dict[str, str]:
    """Taskiq labels for one envelope: routing and context fields, ids hashed by default."""
    labels = {"lane": envelope.lane.value, "model": envelope.model}
    for key, value in (("session_id", envelope.session_id), ("user_id", envelope.user_id)):
        shown = hashed_id(value) if hash_ids else value
        if shown is not None:
            labels[key] = shown
    if envelope.reply_to:
        labels["reply_to"] = envelope.reply_to
    return labels
