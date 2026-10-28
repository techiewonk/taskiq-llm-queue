"""Queued OpenAI-compatible adapters: `ChatOpenAI` / `OpenAIEmbeddings` over `QueuedHTTPTransport`.

Both SDK clients get an httpx client whose transport answers from the LLM queue (D88), so every
LangChain/SDK code path is queued and parsed unchanged. Streaming and SDK retries are forced off:
the queue's workers own retry and fallback.
"""

from __future__ import annotations

from typing import Any

import httpx
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from pydantic import Field, model_validator

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.envelope import FallbackPolicy
from taskiq_llm_queue.http_transport import QUEUED_BASE_URL, QueuedHTTPTransport

# Placeholder only: the SDK requires a key, but workers hold the real provider credentials.
_PLACEHOLDER_API_KEY = "llmq-queued"


def _queued_client_values(values: Any) -> Any:
    """Point both SDK clients at a queued transport built from the raw constructor values."""
    if not isinstance(values, dict):
        return values
    transport = QueuedHTTPTransport(
        values.get("llmq"),
        values.get("model_alias"),
        lane=values.get("lane"),
        fallback=values.get("fallback_policy"),
        timeout=values.get("queue_timeout"),
    )
    values["http_async_client"] = httpx.AsyncClient(transport=transport, base_url=QUEUED_BASE_URL)
    values["http_client"] = httpx.Client(transport=transport, base_url=QUEUED_BASE_URL)
    values.pop("openai_api_base", None)
    values["base_url"] = QUEUED_BASE_URL
    values["openai_proxy"] = None  # an OPENAI_PROXY env var would conflict with http_client
    if values.get("api_key") is None and values.get("openai_api_key") is None:
        values["api_key"] = _PLACEHOLDER_API_KEY
    values["max_retries"] = 0
    return values


class QueuedChatOpenAI(ChatOpenAI):
    """`ChatOpenAI` whose HTTP calls are answered by the LLM queue's `chat` op.

    `model_alias` is the queue alias; when unset, the request's `model` (this class's `model`)
    is used as the alias. Streaming is always off, so `astream()` yields one full message.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    fallback_policy: FallbackPolicy | None = None
    lane: Lane | str | None = None
    queue_timeout: float | None = None

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        values = _queued_client_values(values)
        if isinstance(values, dict):
            values["streaming"] = False
            values["disable_streaming"] = True
        return values


class QueuedOpenAIEmbeddings(OpenAIEmbeddings):
    """`OpenAIEmbeddings` whose HTTP calls are answered by the LLM queue's `embed` op.

    `check_embedding_ctx_length` defaults to False because the queue embeds text, not tokens,
    and non-OpenAI servers do not share OpenAI's tokenizer.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    lane: Lane | str | None = None
    queue_timeout: float | None = None
    check_embedding_ctx_length: bool = False

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        return _queued_client_values(values)
