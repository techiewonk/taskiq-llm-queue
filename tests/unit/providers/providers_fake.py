"""In-process fake of the Bedrock runtime and OpenAI HTTP APIs for provider tests.

Mirrors the wire format the real SDKs parse (TESTING.md "Error fidelity"): Bedrock errors are
an HTTP status + `x-amzn-ErrorType` + `{"message"}`; OpenAI errors are `{"error": {...}}` with
`Retry-After` / `retry-after-ms`. SigV4 is ignored. Every request is counted and recorded.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from aiohttp import web

BEDROCK_STATUS = {
    "ThrottlingException": 429,
    "ModelNotReadyException": 429,
    "ServiceQuotaExceededException": 400,
    "ServiceUnavailableException": 503,
    "InternalServerException": 500,
    "ModelTimeoutException": 408,
    "ModelErrorException": 424,
    "ValidationException": 400,
    "AccessDeniedException": 403,
    "ResourceNotFoundException": 404,
}


@dataclass
class Failure:
    """One canned error response: status, headers and JSON body."""

    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] = field(default_factory=dict)


def bedrock_error(code: str, status: int | None = None, *, header: bool = True) -> Failure:
    """A Bedrock-style error; `header=False` sends a bare status with only a message body."""
    headers = {"x-amzn-ErrorType": f"{code}:http://internal.amazon.com/coral/"} if header else {}
    return Failure(status or BEDROCK_STATUS.get(code, 400), headers, {"message": f"{code} fake"})


def openai_error(status: int, headers: dict[str, str] | None = None, code: str = "") -> Failure:
    """An OpenAI-style error body with the given status and headers."""
    error = {"message": "fake error", "type": "fake", "code": code or None}
    return Failure(status, headers or {}, {"error": error})


class FakeLLM:
    """aiohttp app state: queued failures per route, call counters and the last request bodies."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.failures: list[Failure] = []
        self.delay_secs = 0.0
        self.url = ""

    def fail_next(self, *failures: Failure) -> None:
        """Answer the next requests (any route) with these errors, in order."""
        self.failures.extend(failures)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/model/{model_id}/converse", self._converse)
        app.router.add_post("/model/{model_id}/invoke", self._invoke)
        app.router.add_post("/v1/chat/completions", self._chat)
        app.router.add_post("/v1/embeddings", self._embeddings)
        return app

    async def _begin(
        self, kind: str, request: web.Request
    ) -> tuple[dict[str, Any], Failure | None]:
        raw = await request.read()
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            body = {"_raw": raw.decode("utf-8", "replace")}
        model_id = unquote(request.match_info.get("model_id", ""))
        if model_id:
            body["_model_id"] = model_id
        self.calls[kind] += 1
        self.requests.append((kind, body))
        if self.delay_secs:
            await asyncio.sleep(self.delay_secs)
        return body, (self.failures.pop(0) if self.failures else None)

    @staticmethod
    def _error(failure: Failure) -> web.Response:
        return web.json_response(failure.body, status=failure.status, headers=failure.headers)

    async def _converse(self, request: web.Request) -> web.Response:
        body, failure = await self._begin("converse", request)
        if failure:
            return self._error(failure)
        tools = body.get("toolConfig", {}).get("tools", [])
        if tools:
            name = tools[0]["toolSpec"]["name"]
            content: list[dict[str, Any]] = [
                {"toolUse": {"toolUseId": "tooluse_1", "name": name, "input": {"q": "x"}}}
            ]
            stop = "tool_use"
        else:
            content, stop = [{"text": f"echo from {body['_model_id']}"}], "end_turn"
        reply = {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop,
            "usage": {
                "inputTokens": 11,
                "outputTokens": 7,
                "totalTokens": 18,
                "cacheReadInputTokens": 5,
                "cacheWriteInputTokens": 2,
            },
            "metrics": {"latencyMs": 42},
        }
        return web.json_response(reply)

    async def _invoke(self, request: web.Request) -> web.Response:
        body, failure = await self._begin("invoke", request)
        if failure:
            return self._error(failure)
        if "cohere" in body["_model_id"]:
            texts = body["texts"]
            reply: dict[str, Any] = {
                "id": "emb-1",
                "texts": texts,
                "embeddings": [[float(i), 0.5] for i in range(len(texts))],
                "response_type": "embeddings_floats",
            }
        else:
            dims = body.get("dimensions", 3)
            reply = {"embedding": [0.25] * dims, "inputTextTokenCount": len(body["inputText"])}
        return web.json_response(reply)

    async def _chat(self, request: web.Request) -> web.Response:
        body, failure = await self._begin("chat", request)
        if failure:
            return self._error(failure)
        reply = {
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1_700_000_000,
            "model": body["model"],
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "hello"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
        }
        return web.json_response(reply)

    async def _embeddings(self, request: web.Request) -> web.Response:
        body, failure = await self._begin("embeddings", request)
        if failure:
            return self._error(failure)
        inputs = body["input"] if isinstance(body["input"], list) else [body["input"]]
        data = []
        for i, _ in enumerate(inputs):
            vector = [float(i), 0.5, -1.0]
            encoded: Any = vector
            if body.get("encoding_format") == "base64":
                encoded = base64.b64encode(struct.pack(f"<{len(vector)}f", *vector)).decode()
            data.append({"object": "embedding", "index": i, "embedding": encoded})
        reply = {
            "object": "list",
            "data": data,
            "model": body["model"],
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        }
        return web.json_response(reply)
