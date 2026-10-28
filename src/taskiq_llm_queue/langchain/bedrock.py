"""Queued Bedrock adapters: `ChatBedrockConverse` and `BedrockEmbeddings` that call the LLM queue.

The chat class runs the parent's own `_generate` against a placeholder client that hands the
Converse request to the queue, so messages, tools, cache points, reasoning and usage are built
and parsed exactly as the installed langchain-aws does (0.2 and 1.x lines alike). No boto3
client is ever created: both classes get a placeholder client that refuses direct calls.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from langchain_aws import BedrockEmbeddings, ChatBedrockConverse
from pydantic import Field, model_validator

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.envelope import FallbackPolicy
from taskiq_llm_queue.errors import LLMQueueError
from taskiq_llm_queue.langchain._client import resolve_queue

if TYPE_CHECKING:
    from langchain_core.callbacks import (
        AsyncCallbackManagerForLLMRun,
        CallbackManagerForLLMRun,
    )
    from langchain_core.messages import BaseMessage
    from langchain_core.outputs import ChatResult

    from taskiq_llm_queue.langchain._client import ConverseResultLike, QueueClient

# Per-call Converse handler of the placeholder client (a ContextVar: concurrent calls on one
# model instance never see each other's handler).
_Handler = Callable[[dict[str, Any]], dict[str, Any]]
_HANDLER: ContextVar[_Handler | None] = ContextVar("llmq_converse_handler", default=None)


class _Captured(Exception):  # control flow, not an error
    """Raised by the capturing handler to stop the parent right after it built the request."""

    def __init__(self, request: dict[str, Any]) -> None:
        super().__init__("captured")
        self.request = request


class _QueueOnlyClient:
    """Placeholder for the boto3 client: stops the parent creating one and refuses direct calls.

    `converse` answers only inside a queued call (through the current handler); any other path
    that would reach Bedrock directly (e.g. a parent `_stream`) fails loudly.
    """

    def converse(self, **request: Any) -> dict[str, Any]:
        handler = _HANDLER.get()
        if handler is None:
            self._refuse()
        assert handler is not None
        return handler(request)

    def _refuse(self, *_args: Any, **_kwargs: Any) -> Any:
        raise LLMQueueError("this model sends every call through the LLM queue")

    converse_stream = invoke_model = get_inference_profile = _refuse


def _queue_only_values(values: Any, *slots: str) -> Any:
    """Fill the given boto3 client slots with the placeholder unless the caller supplied them."""
    if isinstance(values, dict):
        for slot in slots:
            values.setdefault(slot, _QueueOnlyClient())
    return values


def _looks_like_bedrock_id(model_id: str) -> bool:
    """Bedrock ids are `provider.model[...]` (optionally region-prefixed) or ARNs."""
    return "." in model_id or model_id.startswith("arn:")


def _alias_values(values: Any) -> Any:
    """Resolve `model_alias` / `model_id` and reject a queue alias passed as the model id.

    Without `model_id`, it is looked up from `llmq.config` when a queue is given.
    """
    if not isinstance(values, dict):
        return values
    model_id = values.get("model_id", values.get("model"))
    alias = values.get("model_alias")
    if model_id is None and alias is not None:
        config = getattr(values.get("llmq"), "config", None)
        resolve = getattr(config, "resolve", None)
        if not callable(resolve):
            raise ValueError(
                f"model_alias={alias!r} needs model_id='<Bedrock model id>' "
                "(or llmq=<LLMQueue> so it can be looked up from the queue config)"
            )
        values["model_id"] = resolve(alias)[1].model_id
    elif isinstance(model_id, str) and alias is None and not _looks_like_bedrock_id(model_id):
        raise ValueError(
            f"model={model_id!r} looks like a queue alias, not a Bedrock model id; pass "
            f"model_alias={model_id!r} and model_id='<Bedrock model id>' "
            "(e.g. 'openai.gpt-oss-120b-1:0'), or model_alias plus llmq=<LLMQueue>"
        )
    return values


def _service_tier(tier: Any) -> dict[str, Any]:
    return tier if isinstance(tier, dict) else {"type": tier}


class QueuedChatBedrockConverse(ChatBedrockConverse):
    """`ChatBedrockConverse` whose Converse calls go through the LLM queue.

    `model_alias` is the queue alias (`gpt120b`, `gpt120b@apac`); `model_id` is the Bedrock id
    (looked up from `llmq.config` when omitted) and drives the parent's provider-specific
    request building. Streaming is always off (one full message).
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    fallback_policy: FallbackPolicy | None = None
    lane: Lane | str | None = None
    service_tier: str | None = None
    queue_timeout_secs: float | None = None

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        values = _queue_only_values(_alias_values(values), "client", "bedrock_client")
        if isinstance(values, dict):
            values["disable_streaming"] = True
        return values

    def _queue(self) -> QueueClient:
        return resolve_queue(self.llmq)

    @property
    def _alias(self) -> str:
        return self.model_alias or self.model_id

    def _run_parent(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None,
        kwargs: dict[str, Any],
        handler: _Handler,
    ) -> ChatResult:
        token = _HANDLER.set(handler)
        try:
            return super()._generate(messages, stop, None, **kwargs)
        finally:
            _HANDLER.reset(token)

    def _capture(
        self, messages: list[BaseMessage], stop: list[str] | None, kwargs: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """(Converse request without `modelId`, kwargs for the parse pass) from the parent."""
        call_kwargs = dict(kwargs)
        tier = call_kwargs.pop("service_tier", None)  # langchain-aws 0.2 has no such kwarg

        def capture(request: dict[str, Any]) -> dict[str, Any]:
            raise _Captured(request)

        try:
            self._run_parent(messages, stop, call_kwargs, capture)
        except _Captured as captured:
            request = dict(captured.request)
        else:  # pragma: no cover - the parent always calls converse
            raise LLMQueueError("ChatBedrockConverse did not build a Converse request")
        request.pop("modelId", None)  # the queue alias picks the model and target
        if tier is not None:
            request["serviceTier"] = _service_tier(tier)
        elif self.service_tier and "serviceTier" not in request:
            request["serviceTier"] = _service_tier(self.service_tier)
        return request, call_kwargs

    def _parse(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None,
        call_kwargs: dict[str, Any],
        result: ConverseResultLike,
    ) -> ChatResult:
        """Parse like the parent (same call, canned reply), then add how the queue served it."""
        chat = self._run_parent(messages, stop, call_kwargs, lambda _request: result.response)
        meta = result.meta.model_dump()
        for generation in chat.generations:
            generation.message.response_metadata.update(meta)
        return chat

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Sync path: `converse_sync` (direct under the shared limiter, see README)."""
        request, call_kwargs = self._capture(messages, stop, kwargs)
        result = self._queue().converse_sync(
            self._alias,
            request,
            lane=self.lane,
            fallback=self.fallback_policy,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._parse(messages, stop, call_kwargs, result)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Native async path: one queued Converse call (the parent only has a thread fallback)."""
        request, call_kwargs = self._capture(messages, stop, kwargs)
        result = await self._queue().converse(
            self._alias,
            request,
            lane=self.lane,
            fallback=self.fallback_policy,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._parse(messages, stop, call_kwargs, result)


class QueuedBedrockEmbeddings(BedrockEmbeddings):
    """`BedrockEmbeddings` whose calls go through the queue's `embed` op (batched per provider).

    `model_alias` is the queue alias (defaults to `model_id`; the queue builds the provider
    body). Queries use `input_type="search_query"`, documents `"search_document"`; the provider
    ignores it where the model has no such field. `normalize` is applied locally.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    lane: Lane | str | None = None
    queue_timeout_secs: float | None = None

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        return _queue_only_values(values, "client")

    def _queue(self) -> QueueClient:
        return resolve_queue(self.llmq)

    def _prepare(self, texts: list[str]) -> list[str]:
        """Same newline handling as the parent, which says newlines hurt embedding quality."""
        return [text.replace(os.linesep, " ") for text in texts]

    def _finish(self, vectors: list[list[float]]) -> list[list[float]]:
        return [self._normalize_vector(v) for v in vectors] if self.normalize else vectors

    def _sync(self, texts: list[str], input_type: str) -> list[list[float]]:
        result = self._queue().embed_sync(
            self.model_alias or self.model_id,
            self._prepare(texts),
            lane=self.lane,
            input_type=input_type,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._finish(result.vectors)

    async def _async(self, texts: list[str], input_type: str) -> list[list[float]]:
        result = await self._queue().embed(
            self.model_alias or self.model_id,
            self._prepare(texts),
            lane=self.lane,
            input_type=input_type,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._finish(result.vectors)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents on the sync path (`embed_sync`)."""
        return self._sync(texts, "search_document")

    def embed_query(self, text: str) -> list[float]:
        """Embed one query on the sync path (`embed_sync`)."""
        return self._sync([text], "search_query")[0]

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents through the queue as one call (the parent fans out per text)."""
        return await self._async(texts, "search_document")

    async def aembed_query(self, text: str) -> list[float]:
        """Embed one query through the queue."""
        return (await self._async([text], "search_query"))[0]
