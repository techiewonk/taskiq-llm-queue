"""Fake Bedrock runtime: `converse`, `converse-stream` and `invoke` under `/model/{modelId}/`.

The region comes from the SigV4 credential scope and the account from its access key id (never
verified), so every account and region can share one endpoint URL. Errors carry
`x-amzn-ErrorType` like the real service. Tokens are reserved (input + maxTokens) when a call
starts and settled to the real usage when it ends, as Bedrock does.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.parse import unquote

from aiohttp import web

from fakellm import eventstream
from fakellm.behaviour import Engine, Outcome
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

_SCOPE = re.compile(r"Credential=([^/,\s]+)/\d{8}/([^/,\s]+)/[^/,\s]+/aws4_request")
_TITAN_DIMS = frozenset({256, 512, 1024})


def region_from_auth(authorization: str) -> str:
    """Region from a SigV4 `Authorization` header, or "unknown" when absent."""
    match = _SCOPE.search(authorization)
    return match.group(2) if match else "unknown"


def account_from_auth(authorization: str) -> str | None:
    """The access key id of a SigV4 `Authorization` header (the fake account), or None."""
    match = _SCOPE.search(authorization)
    return match.group(1) if match else None


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
    auth = request.headers.get("Authorization", "")
    return engine, engine.resolve_bedrock_key(
        model_id, region_from_auth(auth), account_from_auth(auth)
    )


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


def _reply(body: dict[str, Any], outcome: Outcome) -> tuple[list[Any], str, str, int, int]:
    """Content blocks, stop reason, output text and token counts of one Converse reply."""
    behaviour = outcome.behaviour
    content, stop_reason, out_text = _converse_content(body, behaviour.tool_calls)
    if behaviour.output_tokens and stop_reason == "end_turn":
        out_text = (out_text + " ") * (behaviour.output_tokens * 4 // (len(out_text) + 1) + 1)
        out_text = out_text[: behaviour.output_tokens * 4]
        content = [{"text": out_text}]
    input_tokens = count_tokens(collect_text([body.get("system"), body["messages"]]))
    output_tokens = count_tokens(out_text)
    max_tokens = (body.get("inferenceConfig") or {}).get("maxTokens")
    if stop_reason == "end_turn" and isinstance(max_tokens, int) and output_tokens > max_tokens:
        out_text = out_text[: max(1, max_tokens) * 4]
        content = [{"text": out_text}]
        stop_reason, output_tokens = "max_tokens", max_tokens
    return content, stop_reason, out_text, input_tokens, output_tokens


def _reserve(body: dict[str, Any]) -> int:
    """Tokens Bedrock holds when a call starts: the input plus maxTokens."""
    max_tokens = (body.get("inferenceConfig") or {}).get("maxTokens")
    inputs = count_tokens(collect_text([body.get("system"), body.get("messages")]))
    return inputs + (max_tokens if isinstance(max_tokens, int) else 0)


def _usage(input_tokens: int, output_tokens: int) -> dict[str, int]:
    return {
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "totalTokens": input_tokens + output_tokens,
        "cacheReadInputTokens": 0,
        "cacheWriteInputTokens": 0,
    }


async def _start(
    request: web.Request,
) -> tuple[Engine, str, dict[str, Any], Outcome | web.Response]:
    """Validate a Converse body and run the engine; the outcome, or an error response."""
    engine, key = _target(request)
    body = await _json_body(request)
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(body, dict) or not isinstance(messages, list) or not messages:
        engine.record_error(key)
        error = bedrock_error("ValidationException", "messages must be a non-empty list")
        return engine, key, {}, error
    outcome = await engine.call(key, _reserve(body))
    if outcome.error:
        return engine, key, body, bedrock_error(outcome.error)
    return engine, key, body, outcome


async def converse(request: web.Request) -> web.Response:
    """Bedrock Converse: deterministic text (or a toolUse block), usage and metrics."""
    engine, key, body, outcome = await _start(request)
    if isinstance(outcome, web.Response):
        return outcome
    content, stop_reason, _, input_tokens, output_tokens = _reply(body, outcome)
    speed = outcome.behaviour.tokens_per_s
    if speed > 0:
        await asyncio.sleep(output_tokens / speed)
    engine.settle(key, outcome, input_tokens + output_tokens)
    reply: dict[str, Any] = {
        "output": {"message": {"role": "assistant", "content": content}},
        "stopReason": stop_reason,
        "metrics": {"latencyMs": outcome.latency_ms},
    }
    if not outcome.behaviour.usage_missing:
        reply["usage"] = _usage(input_tokens, output_tokens)
    return web.json_response(reply)


async def converse_stream(request: web.Request) -> web.StreamResponse:
    """Bedrock ConverseStream: the same reply as Converse, as paced event-stream chunks.

    The first event goes out after the latency (time to first response); text then follows at
    `tokens_per_s`, 8 tokens per chunk. Usage arrives in the final `metadata` event.
    """
    engine, key, body, outcome = await _start(request)
    if isinstance(outcome, web.Response):
        return outcome
    content, stop_reason, out_text, input_tokens, output_tokens = _reply(body, outcome)
    response = web.StreamResponse(headers={"Content-Type": eventstream.CONTENT_TYPE})
    await response.prepare(request)
    await response.write(eventstream.event("messageStart", {"role": "assistant"}))
    speed = outcome.behaviour.tokens_per_s
    if "toolUse" in content[0]:
        tool = content[0]["toolUse"]
        start = {"toolUse": {"toolUseId": tool["toolUseId"], "name": tool["name"]}}
        await response.write(
            eventstream.event("contentBlockStart", {"contentBlockIndex": 0, "start": start})
        )
        delta = {"toolUse": {"input": json.dumps(tool["input"])}}
        await response.write(
            eventstream.event("contentBlockDelta", {"contentBlockIndex": 0, "delta": delta})
        )
    else:
        chunk_chars = 32  # 8 tokens of 4 characters
        cut = outcome.behaviour.cut_after_chunks
        for n, i in enumerate(range(0, len(out_text), chunk_chars)):
            if cut is not None and n >= cut:
                await response.write(
                    eventstream.exception("modelStreamErrorException", "the stream was cut")
                )
                engine.settle(key, outcome, input_tokens)
                await response.write_eof()
                return response
            piece = out_text[i : i + chunk_chars]
            chunk: dict[str, Any] = {"contentBlockIndex": 0, "delta": {"text": piece}}
            await response.write(eventstream.event("contentBlockDelta", chunk))
            if speed > 0:
                await asyncio.sleep(count_tokens(piece) / speed)
    await response.write(eventstream.event("contentBlockStop", {"contentBlockIndex": 0}))
    await response.write(eventstream.event("messageStop", {"stopReason": stop_reason}))
    metadata: dict[str, Any] = {"metrics": {"latencyMs": outcome.latency_ms}}
    if not outcome.behaviour.usage_missing:
        metadata["usage"] = _usage(input_tokens, output_tokens)
    await response.write(eventstream.event("metadata", metadata))
    engine.settle(key, outcome, input_tokens + output_tokens)
    await response.write_eof()
    return response


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
    outcome = await engine.call(key, count_tokens(collect_text(body)))
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
    engine.settle(key, outcome, tokens)
    headers = {"X-Amzn-Bedrock-Invocation-Latency": str(outcome.latency_ms)}
    if outcome.behaviour.usage_missing:
        payload.pop("inputTextTokenCount", None)
    else:
        headers["X-Amzn-Bedrock-Input-Token-Count"] = str(tokens)
        headers["X-Amzn-Bedrock-Output-Token-Count"] = "0"
    return web.json_response(payload, headers=headers)


def add_routes(app: web.Application) -> None:
    """Register the Bedrock runtime routes (model ids may contain `/` and `:`)."""
    app.router.add_post("/model/{model_id:.+}/converse-stream", converse_stream)
    app.router.add_post("/model/{model_id:.+}/converse", converse)
    app.router.add_post("/model/{model_id:.+}/invoke", invoke)
