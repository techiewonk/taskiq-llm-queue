"""Providers on routes: one attempt of an operation, its usage, and error classification.

Chat runs on the provider's streaming API under the hood and is assembled into the plain reply
shape (Converse body, chat.completion), so the attempt timers apply to every chat call; the
caller still gets a normal reply. `register_provider` adds a provider for a custom account kind.
"""

from __future__ import annotations

import base64
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import orjson

from taskiq_llm_queue.clients.pool import ClientPool, Route
from taskiq_llm_queue.clients.timing import Timing, first, paced
from taskiq_llm_queue.errors import ConfigError, ErrorInfo
from taskiq_llm_queue.providers.classify import classify_bedrock, classify_openai

_TOKEN_HEADER = "x-amzn-bedrock-input-token-count"


@dataclass(frozen=True, slots=True)
class Usage:
    """Tokens of one attempt as the provider reported them; None when it reported nothing."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass(frozen=True, slots=True)
class Result:
    """One successful attempt: the provider-native reply and its usage."""

    response: dict[str, Any]
    usage: Usage


Sink = Callable[[Any], Awaitable[None]]
"""Receives each streamed event of an attempt (streaming to the caller, plan §3.13)."""


class Provider(Protocol):
    """One provider kind: runs an operation on a route and classifies its failures."""

    name: str

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
    ) -> Result:
        """Run one attempt; chat operations pass each streamed event to `sink`."""
        ...

    def classify(self, exc: BaseException) -> ErrorInfo:
        """Throttle / retryable / fatal."""
        ...


# ------------------------------------------------------------------------------- Bedrock


def _converse_body(events: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Assemble ConverseStream events into the body `converse` returns."""
    role = "assistant"
    blocks: dict[int, dict[str, Any]] = {}
    tool_input: dict[int, str] = {}
    body: dict[str, Any] = {}
    for event in events:
        if "messageStart" in event:
            role = event["messageStart"].get("role", role)
        elif "contentBlockStart" in event:
            index = event["contentBlockStart"].get("contentBlockIndex", 0)
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                blocks[index] = {"toolUse": dict(start["toolUse"])}
                tool_input[index] = ""
        elif "contentBlockDelta" in event:
            index = event["contentBlockDelta"].get("contentBlockIndex", 0)
            delta = event["contentBlockDelta"].get("delta", {})
            if "text" in delta:
                block = blocks.setdefault(index, {"text": ""})
                block["text"] = block.get("text", "") + delta["text"]
            elif "toolUse" in delta:
                tool_input[index] = tool_input.get(index, "") + delta["toolUse"].get("input", "")
                blocks.setdefault(index, {"toolUse": {}})
            elif "reasoningContent" in delta:
                block = blocks.setdefault(index, {"reasoningContent": {"reasoningText": {}}})
                reasoning = block["reasoningContent"].setdefault("reasoningText", {})
                part = delta["reasoningContent"]
                if "text" in part:
                    reasoning["text"] = reasoning.get("text", "") + part["text"]
                if "signature" in part:
                    reasoning["signature"] = part["signature"]
        elif "messageStop" in event:
            body["stopReason"] = event["messageStop"].get("stopReason")
            extra = event["messageStop"].get("additionalModelResponseFields")
            if extra is not None:
                body["additionalModelResponseFields"] = extra
        elif "metadata" in event:
            for key in ("usage", "metrics", "trace"):
                if key in event["metadata"]:
                    body[key] = event["metadata"][key]
    for index, text in tool_input.items():
        blocks[index]["toolUse"]["input"] = orjson.loads(text) if text else {}
    content = [blocks[i] for i in sorted(blocks)]
    return {"output": {"message": {"role": role, "content": content}}, **body}


def _bedrock_usage(usage: Mapping[str, Any] | None) -> Usage:
    if not usage:
        return Usage()
    return Usage(
        input_tokens=usage.get("inputTokens"),
        output_tokens=usage.get("outputTokens"),
        cache_read_tokens=int(usage.get("cacheReadInputTokens") or 0),
        cache_write_tokens=int(usage.get("cacheWriteInputTokens") or 0),
    )


class BedrockProvider:
    """Bedrock runtime: `converse` (over ConverseStream) and `invoke_model` (embeddings)."""

    name = "bedrock"

    def __init__(self, pool: ClientPool) -> None:
        """Use `pool` for clients."""
        self._pool = pool

    async def stream(
        self, params: Mapping[str, Any], route: Route, timing: Timing
    ) -> AsyncIterator[Mapping[str, Any]]:
        """ConverseStream events of one attempt, each within the attempt timers."""
        client = await self._pool.bedrock(route)
        request = {**params, "modelId": route.model_id}
        reply = await first(
            client.converse_stream(**request), timing.first_response_secs, "first response"
        )
        async for event in paced(reply["stream"], timing, started=False):
            yield event

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
    ) -> Result:
        """Run one `converse` or `invoke_model` attempt."""
        if op == "converse":
            events = []
            async for event in self.stream(params, route, timing):
                events.append(event)
                if sink is not None:
                    await sink(event)
            body = _converse_body(events)
            return Result(body, _bedrock_usage(body.get("usage")))
        if op == "invoke_model":
            client = await self._pool.bedrock(route)
            request = {**params, "modelId": route.model_id}
            if isinstance(request.get("body"), Mapping | list):
                request["body"] = orjson.dumps(request["body"])
            request.setdefault("contentType", "application/json")
            request.setdefault("accept", "application/json")
            raw = await first(
                client.invoke_model(**request), timing.first_response_secs, "first response"
            )
            async with raw["body"] as stream:
                data = await first(stream.read(), timing.idle_secs, "body")
            headers = raw.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            counted = headers.get(_TOKEN_HEADER)
            response = {
                "body_b64": base64.b64encode(data).decode("ascii"),
                "contentType": raw.get("contentType", ""),
            }
            return Result(response, Usage(input_tokens=int(counted) if counted else None))
        raise ValueError(f"Bedrock does not support op {op!r}")

    def classify(self, exc: BaseException) -> ErrorInfo:
        """By Bedrock error family (`classify_bedrock`)."""
        return classify_bedrock(exc)


def decode_invoke_body(response: Mapping[str, Any]) -> Any:
    """The model body of an `invoke_model` result: parsed JSON, or raw bytes."""
    data = base64.b64decode(response["body_b64"])
    if "json" in str(response.get("contentType", "")).lower():
        return orjson.loads(data)
    return data


# ------------------------------------------------------------------------------- OpenAI


def _chat_body(chunks: list[Any]) -> dict[str, Any]:
    """Assemble chat.completion.chunk objects into one chat.completion body."""
    text: list[str] = []
    calls: dict[int, dict[str, Any]] = {}
    finish = None
    usage = None
    ident, model, created = "", "", int(time.time())
    for chunk in chunks:
        ident, model, created = chunk.id or ident, chunk.model or model, chunk.created or created
        if chunk.usage is not None:
            usage = chunk.usage.model_dump(mode="json", exclude_none=True)
        for choice in chunk.choices or []:
            delta = choice.delta
            if delta.content:
                text.append(delta.content)
            for call in delta.tool_calls or []:
                slot = calls.setdefault(
                    call.index,
                    {"id": "", "type": "function", "function": {"name": "", "arguments": ""}},
                )
                if call.id:
                    slot["id"] = call.id
                if call.function is not None:
                    slot["function"]["name"] += call.function.name or ""
                    slot["function"]["arguments"] += call.function.arguments or ""
            if choice.finish_reason:
                finish = choice.finish_reason
    message: dict[str, Any] = {"role": "assistant", "content": "".join(text) or None}
    if calls:
        message["tool_calls"] = [calls[i] for i in sorted(calls)]
    body: dict[str, Any] = {
        "id": ident,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
    }
    if usage is not None:
        body["usage"] = usage
    return body


def _openai_usage(usage: Mapping[str, Any] | None) -> Usage:
    if not usage:
        return Usage()
    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
    return Usage(
        input_tokens=usage.get("prompt_tokens"),
        output_tokens=usage.get("completion_tokens", 0),
        cache_read_tokens=int(cached),
    )


class OpenAIProvider:
    """OpenAI-compatible servers: `chat` (over streaming) and `embeddings`."""

    name = "openai"

    def __init__(self, pool: ClientPool) -> None:
        """Use `pool` for clients."""
        self._pool = pool

    async def stream(
        self, params: Mapping[str, Any], route: Route, timing: Timing
    ) -> AsyncIterator[Any]:
        """Chat completion chunks of one attempt, each within the attempt timers."""
        client = self._pool.openai(route)
        request = {
            **params,
            "model": route.model_id,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        reply = await first(
            client.chat.completions.create(**request), timing.first_response_secs, "first response"
        )
        async for chunk in paced(reply, timing, started=False):
            yield chunk

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
    ) -> Result:
        """Run one `chat` or `embeddings` attempt."""
        if op == "chat":
            chunks = []
            async for chunk in self.stream(params, route, timing):
                chunks.append(chunk)
                if sink is not None:
                    await sink(chunk)
            body = _chat_body(chunks)
            return Result(body, _openai_usage(body.get("usage")))
        if op == "embeddings":
            client = self._pool.openai(route)
            reply = await first(
                client.embeddings.create(**{**params, "model": route.model_id}),
                timing.first_response_secs,
                "response",
            )
            body = reply.model_dump(mode="json", exclude_unset=True)
            return Result(body, _openai_usage(body.get("usage")))
        raise ValueError(f"OpenAI-compatible provider does not support op {op!r}")

    def classify(self, exc: BaseException) -> ErrorInfo:
        """By HTTP status (`classify_openai`)."""
        return classify_openai(exc)


# ------------------------------------------------------------------------------- registry

ProviderFactory = Callable[[ClientPool], Provider]
_FACTORIES: dict[str, ProviderFactory] = {"bedrock": BedrockProvider, "openai": OpenAIProvider}


def register_provider(name: str, factory: ProviderFactory) -> None:
    """Add a provider kind (pair it with `registry.register_account_kind`)."""
    if not name or not callable(factory):
        raise ConfigError("register_provider needs a name and a callable factory")
    _FACTORIES[name] = factory


class Providers:
    """One provider instance per kind over a shared `ClientPool`."""

    def __init__(self, pool: ClientPool) -> None:
        """Build lazily over `pool`."""
        self.pool = pool
        self._built: dict[str, Provider] = {}

    def get(self, name: str) -> Provider:
        """The provider for a kind; `ConfigError` for an unknown one."""
        if name not in self._built:
            if name not in _FACTORIES:
                raise ConfigError(f"no provider registered for {name!r}")
            self._built[name] = _FACTORIES[name](self.pool)
        return self._built[name]

    async def aclose(self) -> None:
        """Close the pool's clients."""
        await self.pool.aclose()
