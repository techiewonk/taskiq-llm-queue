"""Fake OpenAI-compatible API: `POST /v1/chat/completions` and `POST /v1/embeddings`.

The behaviour key is `<model>@<api key>` when the scenario has it, else the request's `model`
(the API key plays the account). Chat streams as server-sent events when `stream` is true.
Rate limits answer 429 with `Retry-After`, `retry-after-ms` and `x-ratelimit-*` headers so SDK
and queue backoff can be tested.
"""

from __future__ import annotations

import asyncio
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


def _key(request: web.Request, model: str) -> str:
    auth = request.headers.get("Authorization", "")
    account = auth.removeprefix("Bearer ").strip() or None
    return request.app[ENGINE].resolve_openai_key(model, account)


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


async def chat_completions(request: web.Request) -> web.StreamResponse:
    """OpenAI chat completion with deterministic content or echoed `tool_calls`."""
    engine = request.app[ENGINE]
    body = await _body(request)
    model = str((body or {}).get("model") or "unknown")
    key = _key(request, model)
    messages = (body or {}).get("messages")
    if body is None or not isinstance(messages, list) or not messages:
        engine.record_error(key)
        return openai_error("ValidationException", "messages must be a non-empty list")
    prompt_tokens = count_tokens(collect_text(messages))
    max_tokens = body.get("max_tokens") or body.get("max_completion_tokens") or 0
    outcome = await engine.call(key, prompt_tokens + int(max_tokens))
    if outcome.error:
        return openai_error(outcome.error, retry_after_s=outcome.behaviour.retry_after_s)
    message, finish, out_text = _chat_message(body, outcome.behaviour.tool_calls)
    wanted = outcome.behaviour.output_tokens
    if wanted and message.get("content"):
        out_text = ((out_text + " ") * (wanted * 4 // (len(out_text) + 1) + 1))[: wanted * 4]
        message = {"role": "assistant", "content": out_text}
    completion_tokens = count_tokens(out_text)
    usage = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }
    ident = f"chatcmpl-{digest(body)[:24]}"
    speed = outcome.behaviour.tokens_per_s
    if body.get("stream"):
        return await _stream(request, body, ident, model, message, finish, usage, speed, outcome)
    if speed > 0:
        await asyncio.sleep(completion_tokens / speed)
    engine.settle(key, outcome, prompt_tokens + completion_tokens)
    reply: dict[str, Any] = {
        "id": ident,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish, "logprobs": None}],
    }
    if not outcome.behaviour.usage_missing:
        reply["usage"] = usage
    return web.json_response(reply)


async def _stream(
    request: web.Request,
    body: dict[str, Any],
    ident: str,
    model: str,
    message: dict[str, Any],
    finish: str,
    usage: dict[str, int],
    speed: float,
    outcome: Any,
) -> web.StreamResponse:
    """Server-sent events: a role chunk, content in 8-token pieces, the finish, usage, [DONE]."""
    response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
    await response.prepare(request)

    async def send(delta: dict[str, Any], reason: str | None = None) -> None:
        chunk = {
            "id": ident,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": reason}],
        }
        await response.write(f"data: {json.dumps(chunk)}\n\n".encode())

    await send({"role": "assistant", "content": ""})
    content = message.get("content") or ""
    for i in range(0, len(content), 32):
        await send({"content": content[i : i + 32]})
        if speed > 0:
            await asyncio.sleep(count_tokens(content[i : i + 32]) / speed)
    if message.get("tool_calls"):
        calls = [{"index": 0, **call} for call in message["tool_calls"]]
        await send({"tool_calls": calls})
    await send({}, finish)
    include = (body.get("stream_options") or {}).get("include_usage")
    if include and not outcome.behaviour.usage_missing:
        tail = {
            "id": ident,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [],
            "usage": usage,
        }
        await response.write(f"data: {json.dumps(tail)}\n\n".encode())
    await response.write(b"data: [DONE]\n\n")
    request.app[ENGINE].settle(_key(request, model), outcome, usage["total_tokens"])
    await response.write_eof()
    return response


async def embeddings(request: web.Request) -> web.Response:
    """OpenAI embeddings; honours `dimensions` and `encoding_format` (the SDK asks base64)."""
    engine = request.app[ENGINE]
    body = await _body(request)
    model = str((body or {}).get("model") or "unknown")
    key = _key(request, model)
    raw = (body or {}).get("input")
    inputs = [raw] if isinstance(raw, str) else raw
    if body is None or not isinstance(inputs, list) or not inputs:
        engine.record_error(key)
        return openai_error("ValidationException", "input must be a string or non-empty list")
    texts = [t if isinstance(t, str) else json.dumps(t) for t in inputs]
    outcome = await engine.call(key, sum(count_tokens(t) for t in texts))
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
    engine.settle(key, outcome, tokens)
    reply: dict[str, Any] = {"object": "list", "data": data, "model": model}
    if not outcome.behaviour.usage_missing:
        reply["usage"] = {"prompt_tokens": tokens, "total_tokens": tokens}
    return web.json_response(reply)


def add_routes(app: web.Application) -> None:
    """Register the OpenAI-compatible routes under /v1."""
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_post("/v1/embeddings", embeddings)
