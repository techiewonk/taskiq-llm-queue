"""Fake Bedrock runtime: `POST /model/{modelId}/converse` and `/model/{modelId}/invoke`.

The region (the target) comes from the SigV4 credential scope, never verified, so every
region can share one endpoint URL. Errors carry `x-amzn-ErrorType` like the real service.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import unquote

from aiohttp import web

from fakellm.behaviour import Engine
from fakellm.errors import ERRORS
from fakellm.state import ENGINE
from fakellm.synth import (
    collect_text,
    count_tokens,
    digest,
    embedding,
    reply_text,
    tool_arguments,
)

_SCOPE = re.compile(r"Credential=[^/,\s]+/\d{8}/([^/,\s]+)/[^/,\s]+/aws4_request")
_TITAN_DIMS = frozenset({256, 512, 1024})


def region_from_auth(authorization: str) -> str:
    """Region from a SigV4 `Authorization` header, or "unknown" when absent."""
    match = _SCOPE.search(authorization)
    return match.group(1) if match else "unknown"


def bedrock_error(name: str, message: str | None = None) -> web.Response:
    """Bedrock-shaped error: status from the table, `x-amzn-ErrorType`, `{"message"}` body."""
    spec = ERRORS[name]
    return web.json_response(
        {"message": message or spec.message},
        status=spec.status,
        headers={"x-amzn-ErrorType": name},
    )


def _target(request: web.Request) -> tuple[Engine, str]:
    engine = request.app[ENGINE]
    model_id = request.match_info["model_id"]
    # botocore percent-encodes ':' and '/'; decode whatever the router left encoded.
    model_id = unquote(model_id) if "%" in model_id else model_id
    region = region_from_auth(request.headers.get("Authorization", ""))
    return engine, engine.resolve_bedrock_key(model_id, region)


async def _json_body(request: web.Request) -> Any:
    try:
        return json.loads(await request.read() or b"null")
    except ValueError:
        return None


def _converse_content(body: dict[str, Any], tool_mode: str) -> tuple[list[Any], str, str]:
    messages = body["messages"]
    tools = (body.get("toolConfig") or {}).get("tools") or []
    last = (messages[-1].get("content") or []) if isinstance(messages[-1], dict) else []
    answered_tool = any(isinstance(b, dict) and "toolResult" in b for b in last)
    seed = {k: body.get(k) for k in ("messages", "system", "toolConfig")}
    text = reply_text(seed)
    if tools and tool_mode == "echo" and not answered_tool:
        choice = ((body.get("toolConfig") or {}).get("toolChoice") or {}).get("tool") or {}
        specs = [t.get("toolSpec") or {} for t in tools if isinstance(t, dict)]
        spec = next((s for s in specs if s.get("name") == choice.get("name")), specs[0])
        schema = (spec.get("inputSchema") or {}).get("json")
        block = {
            "toolUse": {
                "toolUseId": f"tooluse_{digest(seed)[:22]}",
                "name": spec.get("name", "tool"),
                "input": tool_arguments(schema, collect_text(last) or text),
            }
        }
        return [block], "tool_use", json.dumps(block)
    return [{"text": text}], "end_turn", text


async def converse(request: web.Request) -> web.Response:
    """Bedrock Converse: deterministic text (or a toolUse block), usage and metrics."""
    engine, key = _target(request)
    body = await _json_body(request)
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(messages, list) or not messages:
        engine.record_error(key)
        return bedrock_error("ValidationException", "messages must be a non-empty list")
    outcome = await engine.call(key)
    if outcome.error:
        return bedrock_error(outcome.error)
    content, stop_reason, out_text = _converse_content(body, outcome.behaviour.tool_calls)
    input_tokens = count_tokens(collect_text([body.get("system"), messages]))
    output_tokens = count_tokens(out_text)
    max_tokens = (body.get("inferenceConfig") or {}).get("maxTokens")
    if stop_reason == "end_turn" and isinstance(max_tokens, int) and output_tokens > max_tokens:
        content = [{"text": out_text[: max(1, max_tokens) * 4]}]
        stop_reason, output_tokens = "max_tokens", max_tokens
    return web.json_response(
        {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop_reason,
            "usage": {
                "inputTokens": input_tokens,
                "outputTokens": output_tokens,
                "totalTokens": input_tokens + output_tokens,
                "cacheReadInputTokens": 0,
                "cacheWriteInputTokens": 0,
            },
            "metrics": {"latencyMs": outcome.latency_ms},
        }
    )


async def invoke(request: web.Request) -> web.Response:
    """Bedrock InvokeModel for Titan v2 and Cohere v3 embedding bodies (deterministic)."""
    engine, key = _target(request)
    body = await _json_body(request)
    problem = None
    if not isinstance(body, dict):
        problem = "body must be a JSON object"
    elif "inputText" in body:
        dims = body.get("dimensions")
        if not isinstance(body["inputText"], str) or not body["inputText"]:
            problem = "inputText must be a non-empty string"
        elif dims is not None and dims not in _TITAN_DIMS:
            problem = f"dimensions must be one of {sorted(_TITAN_DIMS)}"
    elif "texts" in body:
        texts = body["texts"]
        if not isinstance(texts, list) or not texts or not all(isinstance(t, str) for t in texts):
            problem = "texts must be a non-empty list of strings"
        elif len(texts) > 96:
            problem = "texts accepts at most 96 items"
        elif "input_type" not in body:
            problem = "input_type is required"
    else:
        problem = "fakellm supports Titan v2 (inputText) and Cohere v3 (texts) bodies"
    if problem is not None:
        engine.record_error(key)
        return bedrock_error("ValidationException", problem)
    assert isinstance(body, dict)  # narrowed by the checks above
    outcome = await engine.call(key)
    if outcome.error:
        return bedrock_error(outcome.error)
    if "inputText" in body:
        text = body["inputText"]
        tokens = count_tokens(text)
        payload: dict[str, Any] = {
            "embedding": embedding(text, body.get("dimensions") or outcome.behaviour.dims),
            "inputTextTokenCount": tokens,
        }
    else:
        texts = body["texts"]
        tokens = sum(count_tokens(t) for t in texts)
        vectors = [embedding(t, outcome.behaviour.dims) for t in texts]
        payload = {
            "id": digest(body)[:32],
            "texts": texts,
            "response_type": "embeddings_floats",
            "embeddings": {"float": vectors} if body.get("embedding_types") else vectors,
        }
    return web.json_response(
        payload,
        headers={
            "X-Amzn-Bedrock-Input-Token-Count": str(tokens),
            "X-Amzn-Bedrock-Output-Token-Count": "0",
            "X-Amzn-Bedrock-Invocation-Latency": str(outcome.latency_ms),
        },
    )


def add_routes(app: web.Application) -> None:
    """Register the Bedrock runtime routes (model ids may contain `/` and `:`)."""
    app.router.add_post("/model/{model_id:.+}/converse", converse)
    app.router.add_post("/model/{model_id:.+}/invoke", invoke)
