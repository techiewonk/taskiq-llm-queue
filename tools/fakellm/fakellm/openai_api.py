"""Fake OpenAI-compatible API: `POST /v1/chat/completions` and `POST /v1/embeddings`.

The behaviour key is the request's `model`. Rate limits answer 429 with `Retry-After`,
`retry-after-ms` and `x-ratelimit-*` headers so SDK and queue backoff can be tested.
"""

from __future__ import annotations

import base64
import json
import math
import struct
import time
from typing import Any

from aiohttp import web

from fakellm.errors import ERRORS
from fakellm.state import ENGINE
from fakellm.synth import collect_text, count_tokens, digest, embedding, reply_text, tool_arguments


def openai_error(name: str, message: str | None = None, retry_after_s: float = 1.0) -> web.Response:
    """OpenAI-shaped error `{"error": {message, type, code, param}}` for a named error."""
    spec = ERRORS[name]
    headers: dict[str, str] = {}
    if spec.openai_status == 429:
        headers = {
            "Retry-After": str(max(1, math.ceil(retry_after_s))),
            "retry-after-ms": str(round(retry_after_s * 1000)),
            "x-ratelimit-limit-requests": "0",
            "x-ratelimit-remaining-requests": "0",
            "x-ratelimit-reset-requests": f"{retry_after_s:g}s",
            "x-ratelimit-limit-tokens": "0",
            "x-ratelimit-remaining-tokens": "0",
            "x-ratelimit-reset-tokens": f"{retry_after_s:g}s",
        }
    body = {
        "error": {
            "message": message or spec.message,
            "type": spec.openai_type,
            "code": spec.openai_code,
            "param": None,
        }
    }
    return web.json_response(body, status=spec.openai_status, headers=headers)


async def _body(request: web.Request) -> dict[str, Any] | None:
    try:
        body = json.loads(await request.read() or b"null")
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def _chat_message(body: dict[str, Any], tool_mode: str) -> tuple[dict[str, Any], str, str]:
    messages = body["messages"]
    tools = [t for t in body.get("tools") or [] if isinstance(t, dict)]
    choice = body.get("tool_choice")
    last = messages[-1] if isinstance(messages[-1], dict) else {}
    seed = {k: body.get(k) for k in ("messages", "tools")}
    text = reply_text(seed)
    if tools and tool_mode == "echo" and choice != "none" and last.get("role") != "tool":
        wanted = (choice.get("function") or {}).get("name") if isinstance(choice, dict) else None
        funcs = [t.get("function") or {} for t in tools]
        func = next((f for f in funcs if f.get("name") == wanted), funcs[0])
        args = tool_arguments(func.get("parameters"), collect_text(last) or text)
        call = {
            "id": f"call_{digest(seed)[:24]}",
            "type": "function",
            "function": {"name": func.get("name", "tool"), "arguments": json.dumps(args)},
        }
        message = {"role": "assistant", "content": None, "tool_calls": [call]}
        return message, "tool_calls", json.dumps(call)
    return {"role": "assistant", "content": text}, "stop", text


async def chat_completions(request: web.Request) -> web.Response:
    """OpenAI chat completion with deterministic content or echoed `tool_calls`."""
    engine = request.app[ENGINE]
    body = await _body(request)
    model = str((body or {}).get("model") or "unknown")
    messages = (body or {}).get("messages")
    if body is None or not isinstance(messages, list) or not messages:
        engine.record_error(model)
        return openai_error("ValidationException", "messages must be a non-empty list")
    if body.get("stream"):
        engine.record_error(model)
        return openai_error("ValidationException", "fakellm does not stream (stream=false)")
    outcome = await engine.call(model)
    if outcome.error:
        return openai_error(outcome.error, retry_after_s=outcome.behaviour.retry_after_s)
    message, finish, out_text = _chat_message(body, outcome.behaviour.tool_calls)
    prompt_tokens = count_tokens(collect_text(messages))
    completion_tokens = count_tokens(out_text)
    return web.json_response(
        {
            "id": f"chatcmpl-{digest(body)[:24]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {"index": 0, "message": message, "finish_reason": finish, "logprobs": None}
            ],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
        }
    )


async def embeddings(request: web.Request) -> web.Response:
    """OpenAI embeddings; honours `dimensions` and `encoding_format` (the SDK asks base64)."""
    engine = request.app[ENGINE]
    body = await _body(request)
    model = str((body or {}).get("model") or "unknown")
    raw = (body or {}).get("input")
    inputs = [raw] if isinstance(raw, str) else raw
    if body is None or not isinstance(inputs, list) or not inputs:
        engine.record_error(model)
        return openai_error("ValidationException", "input must be a string or non-empty list")
    texts = [t if isinstance(t, str) else json.dumps(t) for t in inputs]
    outcome = await engine.call(model)
    if outcome.error:
        return openai_error(outcome.error, retry_after_s=outcome.behaviour.retry_after_s)
    dims = body.get("dimensions") or outcome.behaviour.dims
    as_base64 = body.get("encoding_format") == "base64"
    data = []
    for index, text in enumerate(texts):
        vec: Any = embedding(text, dims)
        if as_base64:
            vec = base64.b64encode(struct.pack(f"<{len(vec)}f", *vec)).decode()
        data.append({"object": "embedding", "index": index, "embedding": vec})
    tokens = sum(count_tokens(t) for t in texts)
    return web.json_response(
        {
            "object": "list",
            "data": data,
            "model": model,
            "usage": {"prompt_tokens": tokens, "total_tokens": tokens},
        }
    )


def add_routes(app: web.Application) -> None:
    """Register the OpenAI-compatible routes under /v1."""
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_post("/v1/embeddings", embeddings)
