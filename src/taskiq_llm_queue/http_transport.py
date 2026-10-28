"""httpx transport that turns OpenAI SDK HTTP requests into LLM queue calls (D88).

Given to `openai.AsyncOpenAI(http_client=httpx.AsyncClient(transport=...))`, it answers
`/chat/completions` with `llmq.chat` and `/embeddings` with `llmq.embed`, so the SDK and
LangChain parse replies exactly as from a real server. Queue errors become OpenAI-style error
bodies with a matching status, so the SDK raises its usual exceptions (429 → `RateLimitError`).
Chat replies carry how the queue served them under the extra body key `llmq_meta`.
Needs the `openai` extra (httpx); the core package never imports this module.
"""

from __future__ import annotations

import array
import base64
from typing import TYPE_CHECKING, Any

import httpx
import orjson

from taskiq_llm_queue.errors import (
    ConfigError,
    LLMQueueCancelled,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
)
from taskiq_llm_queue.langchain._client import resolve_queue

if TYPE_CHECKING:
    from taskiq_llm_queue.config import Lane
    from taskiq_llm_queue.envelope import FallbackPolicy
    from taskiq_llm_queue.langchain._client import ChatResultLike, QueueClient

# Never resolved: every request is answered in-process by the transport.
QUEUED_BASE_URL = "http://llmq.invalid/v1"
# Extra key of a queued chat completion body: the call's `CallMeta` (served model, attempts...).
META_KEY = "llmq_meta"


class _RequestRejected(Exception):
    """A request the transport cannot queue (bad body, streaming, unknown route)."""

    def __init__(self, status: int, message: str, code: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class QueuedHTTPTransport(httpx.AsyncBaseTransport, httpx.BaseTransport):
    """Answer OpenAI-compatible HTTP requests from the LLM queue instead of the network.

    `model_alias` overrides the body's `model` (use it when the SDK model name differs from the
    queue alias). `llmq=None` uses the default queue, looked up per request. Auth headers are
    ignored: workers hold the provider credentials. The sync side needs `chat_sync` on the client
    for chat (embeddings use `embed_sync`); without it the SDK gets a 501 error.
    """

    def __init__(
        self,
        llmq: QueueClient | None = None,
        model_alias: str | None = None,
        *,
        lane: Lane | str | None = None,
        fallback: FallbackPolicy | None = None,
        timeout_secs: float | None = None,
    ) -> None:
        """Keep the queue, alias and per-call overrides; nothing is resolved until a request."""
        self._llmq = llmq
        self._alias = model_alias
        self._lane = lane
        self._fallback = fallback
        self._timeout_secs = timeout_secs

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Route one SDK request through the async queue API."""
        try:
            route, alias, body = self._parse(request, await request.aread())
            queue = resolve_queue(self._llmq)
            if route == "chat":
                chat = await queue.chat(
                    alias,
                    body,
                    lane=self._lane,
                    fallback=self._fallback,
                    timeout_secs=self._timeout_secs,
                )
                data = _chat_response(chat)
            else:
                texts = _embedding_inputs(body)
                embedded = await queue.embed(
                    alias, texts, lane=self._lane, timeout_secs=self._timeout_secs
                )
                data = _embedding_response(alias, embedded.vectors, body.get("encoding_format"))
        except (_RequestRejected, LLMQueueError) as exc:
            return _error_response(request, exc)
        return httpx.Response(200, json=data, request=request)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Route one SDK request through the sync queue API (`chat_sync` / `embed_sync`)."""
        try:
            route, alias, body = self._parse(request, request.read())
            queue = resolve_queue(self._llmq)
            if route == "chat":
                chat_sync = getattr(queue, "chat_sync", None)
                if chat_sync is None:
                    raise _RequestRejected(
                        501, "sync chat is not supported by this queue; use ainvoke", "sync"
                    )
                chat = chat_sync(
                    alias,
                    body,
                    lane=self._lane,
                    fallback=self._fallback,
                    timeout_secs=self._timeout_secs,
                )
                data = _chat_response(chat)
            else:
                texts = _embedding_inputs(body)
                embedded = queue.embed_sync(
                    alias, texts, lane=self._lane, timeout_secs=self._timeout_secs
                )
                data = _embedding_response(alias, embedded.vectors, body.get("encoding_format"))
        except (_RequestRejected, LLMQueueError) as exc:
            return _error_response(request, exc)
        return httpx.Response(200, json=data, request=request)

    def _parse(self, request: httpx.Request, raw: bytes) -> tuple[str, str, dict[str, Any]]:
        """Return (route, alias, JSON body), rejecting what the queue cannot serve."""
        path = request.url.path.rstrip("/")
        if request.method != "POST":
            raise _RequestRejected(405, f"{request.method} {path} is not queued", "method")
        if path.endswith("/chat/completions"):
            route = "chat"
        elif path.endswith("/embeddings"):
            route = "embeddings"
        else:
            raise _RequestRejected(404, f"route {path} is not supported by the queue", "route")
        try:
            body = orjson.loads(raw or b"{}")
        except ValueError as exc:
            raise _RequestRejected(400, f"request body is not JSON: {exc}", "body") from exc
        if not isinstance(body, dict):
            raise _RequestRejected(400, "request body must be a JSON object", "body")
        if body.get("stream"):
            raise _RequestRejected(400, "streaming is not supported through the queue", "stream")
        alias = self._alias or body.get("model")
        if not isinstance(alias, str) or not alias:
            raise _RequestRejected(400, "no model alias in the transport or the body", "model")
        return route, alias, body


def _chat_response(result: ChatResultLike) -> dict[str, Any]:
    """The completion body plus `llmq_meta` (the SDK keeps unknown keys as extras)."""
    return {**result.response, META_KEY: result.meta.model_dump(mode="json")}


def _embedding_inputs(body: dict[str, Any]) -> list[str]:
    """Texts from an embeddings body; token-id inputs are rejected (queue embeds text only)."""
    value = body.get("input")
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return value
    raise _RequestRejected(400, "embeddings input must be a string or a list of strings", "input")


def _embedding_response(
    alias: str, vectors: list[list[float]], encoding_format: object
) -> dict[str, Any]:
    """OpenAI embeddings body; base64 is float32 in native order, as the SDK decodes it."""
    as_base64 = encoding_format == "base64"
    return {
        "object": "list",
        "model": alias,
        "data": [
            {
                "object": "embedding",
                "index": i,
                "embedding": (
                    base64.b64encode(array.array("f", vec).tobytes()).decode("ascii")
                    if as_base64
                    else vec
                ),
            }
            for i, vec in enumerate(vectors)
        ],
        "usage": {"prompt_tokens": 0, "total_tokens": 0},
    }


def _error_response(request: httpx.Request, exc: Exception) -> httpx.Response:
    """Map a queue error to the status and body an OpenAI server would send.

    `ProviderError` uses its `status_code` when the client sets one, else 429 when retryable
    (the SDK raises `RateLimitError`) and 400 when not; timeouts are 504, queue outages 503.
    """
    headers = {"x-llmq-error": type(exc).__name__}
    if isinstance(exc, _RequestRejected):
        status, err_type, code = exc.status, "invalid_request_error", exc.code
    elif isinstance(exc, ProviderError):
        explicit = getattr(exc, "status_code", None)
        status = explicit if isinstance(explicit, int) else (429 if exc.retryable else 400)
        err_type = "rate_limit_error" if status == 429 else "provider_error"
        code = "provider_error"
        if exc.retry_after_secs is not None:
            headers["retry-after"] = f"{exc.retry_after_secs:g}"
            headers["retry-after-ms"] = str(int(exc.retry_after_secs * 1000))
    elif isinstance(exc, LLMQueueCancelled):
        status, err_type, code = 499, "cancelled", "cancelled"
    elif isinstance(exc, LLMQueueTimeout):
        status, err_type, code = 504, "timeout", "queue_timeout"
    elif isinstance(exc, QueueUnavailable):
        status, err_type, code = 503, "service_unavailable", "queue_unavailable"
    elif isinstance(exc, ConfigError):
        status, err_type, code = 404, "invalid_request_error", "model_not_found"
    else:
        status, err_type, code = 500, "server_error", "llmq_error"
    body = {"error": {"message": str(exc), "type": err_type, "param": None, "code": code}}
    return httpx.Response(status, json=body, headers=headers, request=request)
