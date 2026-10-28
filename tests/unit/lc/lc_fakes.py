"""In-memory `QueueClient` for the LangChain adapter tests, plus canned provider bodies.

Records every call as (op, model, payload, kwargs) and replies with canned Converse / OpenAI
bodies, or raises `error` when set, so the adapters are tested without SQS, Redis or AWS.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from taskiq_llm_queue.envelope import CallMeta

META = CallMeta(
    served_model="gpt120b",
    served_target="apac",
    fallback_used=True,
    queue_wait_ms=42,
    provider_ms=900,
    attempts=2,
    worker="pod-a",
)

CONVERSE_TOOL_RESPONSE: dict[str, Any] = {
    "ResponseMetadata": {
        "RequestId": "req-1",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {},
        "RetryAttempts": 0,
    },
    "output": {
        "message": {
            "role": "assistant",
            "content": [
                {"text": "Let me check."},
                {"toolUse": {"toolUseId": "t-1", "name": "get_price", "input": {"symbol": "INFY"}}},
            ],
        }
    },
    "stopReason": "tool_use",
    "usage": {
        "inputTokens": 12,
        "outputTokens": 7,
        "totalTokens": 19,
        "cacheReadInputTokens": 5,
        "cacheWriteInputTokens": 2,
    },
    "metrics": {"latencyMs": 120},
}

CONVERSE_TEXT_RESPONSE: dict[str, Any] = {
    "output": {"message": {"role": "assistant", "content": [{"text": "Markets rose."}]}},
    "stopReason": "end_turn",
    "usage": {"inputTokens": 4, "outputTokens": 3, "totalTokens": 7},
    "metrics": {"latencyMs": 50},
}

CHAT_TEXT_RESPONSE: dict[str, Any] = {
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1,
    "model": "llama-3.1-70b",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "hello"},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
}

CHAT_TOOL_RESPONSE: dict[str, Any] = {
    **CHAT_TEXT_RESPONSE,
    "choices": [
        {
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_price", "arguments": '{"symbol": "INFY"}'},
                    }
                ],
            },
            "finish_reason": "tool_calls",
        }
    ],
}


@dataclass(frozen=True)
class FakeResult:
    """Same shape as `LLMQueue.converse` / `chat` results (`response`, `meta`)."""

    response: dict[str, Any]
    meta: CallMeta


@dataclass(frozen=True)
class FakeEmbedResult:
    """Same shape as `LLMQueue.embed`'s `EmbedResult`."""

    vectors: list[list[float]]
    meta: CallMeta


@dataclass
class FakeQueueClient:
    """Implements the adapters' `QueueClient` protocol in memory."""

    converse_response: dict[str, Any] = field(default_factory=lambda: CONVERSE_TOOL_RESPONSE)
    chat_response: dict[str, Any] = field(default_factory=lambda: CHAT_TEXT_RESPONSE)
    error: Exception | None = None
    calls: list[tuple[str, str, Any, dict[str, Any]]] = field(default_factory=list)

    def _record(self, op: str, model: str, payload: Any, kwargs: dict[str, Any]) -> None:
        self.calls.append((op, model, copy.deepcopy(payload), kwargs))
        if self.error is not None:
            raise self.error

    def _converse(self, op: str, model: str, request: dict[str, Any], **kw: Any) -> Any:
        self._record(op, model, request, kw)
        return FakeResult(copy.deepcopy(self.converse_response), META)

    async def converse(self, model: str, request: dict[str, Any], **kw: Any) -> Any:
        return self._converse("converse", model, request, **kw)

    def converse_sync(self, model: str, request: dict[str, Any], **kw: Any) -> Any:
        return self._converse("converse_sync", model, request, **kw)

    def _embed(self, op: str, model: str, texts: list[str], **kw: Any) -> FakeEmbedResult:
        self._record(op, model, texts, kw)
        return FakeEmbedResult([[float(len(text)), 0.5] for text in texts], META)

    async def embed(self, model: str, texts: list[str], **kw: Any) -> FakeEmbedResult:
        return self._embed("embed", model, texts, **kw)

    def embed_sync(self, model: str, texts: list[str], **kw: Any) -> FakeEmbedResult:
        return self._embed("embed_sync", model, texts, **kw)

    async def chat(self, model: str, request: dict[str, Any], **kw: Any) -> FakeResult:
        self._record("chat", model, request, kw)
        return FakeResult(copy.deepcopy(self.chat_response), META)

    @property
    def ops(self) -> list[str]:
        return [call[0] for call in self.calls]
