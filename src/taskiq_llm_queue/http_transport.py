"""httpx transport that turns OpenAI SDK HTTP requests into LLM queue calls.

Given to `openai.AsyncOpenAI(http_client=httpx.AsyncClient(transport=...))`, it answers
`/chat/completions` with `llmq.chat` and `/embeddings` with `llmq.embed`, so the SDK and
LangChain parse replies exactly as from a real server. Queue errors become OpenAI-style error
bodies with the status an OpenAI server would send (rate limits are 429 with `retry-after`).
Chat replies carry how the queue served them under the extra body key `llmq_meta`. Needs the
`openai` extra (httpx); the core package never imports this module.
"""

from __future__ import annotations

import array
import base64
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import httpx
import orjson

from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.errors import ConfigError, LLMQueueError
from taskiq_llm_queue.langchain._client import resolve_queue

if TYPE_CHECKING:
    from taskiq_llm_queue.langchain._client import QueueClient, ReplyLike

# Never resolved: every request is answered in-process by the transport.
QUEUED_BASE_URL = "http://llmq.invalid/v1"
# Extra key of a queued chat completion body: how the call was served.
META_KEY = "llmq_meta"
# Error kind -> (HTTP status, OpenAI error type).
_STATUS = {
    "rate_limited": (429, "rate_limit_error"),
    "queue_full": (429, "rate_limit_error"),
    "retry_budget_exhausted": (429, "rate_limit_error"),
    "deadline_exceeded": (504, "timeout"),
    "queue_unavailable": (503, "service_unavailable"),
    "model_unavailable": (503, "service_unavailable"),
    "credentials_unavailable": (503, "service_unavailable"),
    "model_disabled": (404, "invalid_request_error"),
    "cancelled": (499, "cancelled"),
    "worker_lost": (502, "server_error"),
    "dead_lettered": (500, "server_error"),
}


class _RequestRejected(Exception):
    """A request the transport cannot queue (bad body, streaming, unknown route)."""

    def __init__(self, status: int, message: str, code: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class QueuedHTTPTransport(httpx.AsyncBaseTransport, httpx.BaseTransport):
    """Answer OpenAI-compatible HTTP requests from the LLM queue instead of the network.

    `model_alias` overrides the body's `model`. `llmq=None` uses the default queue, looked up per
    request. Auth headers are ignored: workers hold the provider credentials.
    """

    def __init__(
        self,
        llmq: QueueClient | None = None,
        model_alias: str | None = None,
        *,
        priority: bool | None = None,
        allow_fallback: str = "all",
        timeout_secs: float | None = None,
    ) -> None:
        """Keep the queue, alias and per-call overrides; nothing is resolved until a request."""
        self._llmq = llmq
        self._alias = model_alias
        self._priority = priority
        self._timeout = timeout_secs
        self._fallback = allow_fallback

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Route one SDK request through the async queue API."""
        try:
            route, alias, body = self._parse(request, await request.aread())
            queue = resolve_queue(self._llmq)
            if route == "chat":
                reply = await queue.chat(
                    alias,
                    body,
                    priority=self._priority,
                    allow_fallback=self._fallback,
                    timeout_secs=self._timeout,
                )
                data = _chat_response(reply)
            else:
                texts = _embedding_inputs(body)
                embedded = await queue.embed(
                    alias, texts, priority=self._priority, timeout_secs=self._timeout
                )
                data = _embedding_response(alias, embedded.vectors, body.get("encoding_format"))
        except (_RequestRejected, LLMQueueError) as exc:
            return _error_response(request, exc)
        return httpx.Response(200, json=data, request=request)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Route one SDK request through the sync queue API (direct path)."""
        try:
            route, alias, body = self._parse(request, request.read())
            queue = resolve_queue(self._llmq)
            if route == "chat":
                chat_sync = getattr(queue, "chat_sync", None) or queue.converse_sync
                reply = chat_sync(
                    alias,
                    body,
                    priority=self._priority,
                    allow_fallback=self._fallback,
                    timeout_secs=self._timeout,
                )
                data = _chat_response(reply)
            else:
                texts = _embedding_inputs(body)
                embedded = queue.embed_sync(
                    alias, texts, priority=self._priority, timeout_secs=self._timeout
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
        body.pop("model", None)  # the registry decides the provider model id
        return route, alias, body


def _chat_response(result: ReplyLike) -> dict[str, Any]:
    """The completion body plus `llmq_meta` (the SDK keeps unknown keys as extras)."""
    return {**(result.response or {}), META_KEY: result.meta.model_dump(mode="json")}


def _embedding_inputs(body: dict[str, Any]) -> list[str]:
    """Texts from an embeddings body; token-id inputs are rejected (the queue embeds text)."""
    value = body.get("input")
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return value
    raise _RequestRejected(400, "embeddings input must be a string or a list of strings", "input")


def _embedding_response(
    alias: str, vectors: Sequence[Sequence[float]], encoding_format: object
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
                    else list(vec)
                ),
            }
            for i, vec in enumerate(vectors)
        ],
        "usage": {"prompt_tokens": 0, "total_tokens": 0},
    }


def _error_response(request: httpx.Request, exc: Exception) -> httpx.Response:
    """Map a queue error to the status and body an OpenAI server would send."""
    headers = {"x-llmq-error": type(exc).__name__}
    if isinstance(exc, _RequestRejected):
        status, err_type, code = exc.status, "invalid_request_error", exc.code
    elif isinstance(exc, CallFailed):
        status, err_type = _STATUS.get(exc.kind, (400, "invalid_request_error"))
        code = exc.kind
        headers["x-llmq-error"] = exc.kind
        if exc.retry_after_secs is not None:
            headers["retry-after"] = f"{exc.retry_after_secs:g}"
            headers["retry-after-ms"] = str(int(exc.retry_after_secs * 1000))
    elif isinstance(exc, ConfigError | KeyError):
        status, err_type, code = 404, "invalid_request_error", "model_not_found"
    else:
        status, err_type, code = 500, "server_error", "llmq_error"
    body = {"error": {"message": str(exc), "type": err_type, "param": None, "code": code}}
    return httpx.Response(status, json=body, headers=headers, request=request)
