"""Message format version 2 (plan E80) and the reply stored for the caller.

A call's deadline is not an absolute pod time: the message carries `timeout_secs` and the
worker measures it from the queue position (the store's clock), so pod clock skew does not
matter (plan §3.18). Version 2 only: a newer message is returned to the queue, never dropped.
"""

from __future__ import annotations

import hashlib
from typing import Any, Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field

from taskiq_llm_queue.errors import UnsupportedEnvelopeError

VERSION = 2
Op = Literal["converse", "invoke_model", "chat", "embeddings"]
AllowFallback = Literal["all", "regions", "none"]


class Envelope(BaseModel):
    """One LLM call on the queue. `params` are provider-native and travel inside the message."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    v: int = VERSION
    request_id: str
    op: Op
    model: str
    params: dict[str, Any]
    priority: bool = False
    caller: str
    timeout_secs: float = Field(gt=0)
    max_wait_secs: float | None = Field(default=None, gt=0)
    allow_fallback: AllowFallback = "all"
    allowed_regions: tuple[str, ...] | None = None
    allowed_accounts: tuple[str, ...] | None = None
    session_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None
    app_request_id: bool = False
    stream: bool = False

    def dumps(self) -> bytes:
        """JSON bytes for the queue (fields directly: `model_dump` re-walked the params)."""
        return orjson.dumps({k: v for k, v in self.__dict__.items() if v is not None})


def parse(data: bytes | str) -> Envelope:
    """Decode a message; `UnsupportedEnvelopeError` for a newer version (return it, do not drop)."""
    raw = orjson.loads(data)
    if not isinstance(raw, dict) or not isinstance(raw.get("v"), int):
        raise ValueError("not an envelope")
    if raw["v"] > VERSION:
        raise UnsupportedEnvelopeError(f"message version {raw['v']} is newer than {VERSION}")
    if raw["v"] < VERSION:
        raise ValueError(f"message version {raw['v']} is not supported (0.3 reads version 2)")
    return Envelope.model_validate(raw)


def params_hash(params: Any) -> str:
    """Stable hash of a call's params (duplicate request ids must carry the same params)."""
    return hashlib.sha256(orjson.dumps(params, option=orjson.OPT_SORT_KEYS)).hexdigest()[:32]


class Meta(BaseModel):
    """How a call was served: deployment, fallback, attempts, timings, tokens."""

    model_config = ConfigDict(frozen=True)

    served_model: str = ""
    deployment: str = ""
    fallback: Literal["region", "model"] | None = None
    attempts: int = 0
    queue_wait_ms: int = 0
    provider_ms: int = 0
    tokens_in: int | None = None
    tokens_out: int | None = None
    worker: str = ""


class Reply(BaseModel):
    """The stored result of a call: the provider reply, or an error kind with its details."""

    model_config = ConfigDict(frozen=True)

    request_id: str
    ok: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    message: str = ""
    retry_after_secs: float | None = None
    partial_text: str | None = None
    meta: Meta = Field(default_factory=Meta)

    def dumps(self) -> bytes:
        """JSON bytes for the store (fields directly: `model_dump` re-walked the response)."""
        out = {k: v for k, v in self.__dict__.items() if v is not None}
        out["meta"] = {k: v for k, v in self.meta.__dict__.items() if v is not None}
        return orjson.dumps(out)
