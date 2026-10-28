"""Queued Bedrock adapters: `ChatBedrockConverse` and `BedrockEmbeddings` that call the LLM queue.

Requests are built with langchain_aws's own private helpers (plan §10.3), so messages, tools,
cache points and usage parse exactly as the parent does; only the transport changes. No boto3
client is created: both classes get a placeholder client that refuses direct calls.
"""

from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING, Any

from langchain_aws import BedrockEmbeddings, ChatBedrockConverse
from langchain_aws.chat_models.bedrock_converse import (
    _convert_tool_blocks_to_text,
    _has_tool_use_or_result_blocks,
    _messages_to_bedrock,
    _parse_response,
    _snake_to_camel_keys,
)
from langchain_core.outputs import ChatGeneration, ChatResult
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

    from taskiq_llm_queue.langchain._client import ConverseResultLike, QueueClient

# Keys _snake_to_camel_keys must leave alone (JSON-schema bodies), as in the parent's _generate.
_CAMEL_EXCLUDED = {"inputSchema", "properties", "thinking"}


class _QueueOnlyClient:
    """Placeholder for the boto3 client: stops the parent creating one and refuses direct calls.

    Any code path that would still reach Bedrock directly (e.g. a parent `_stream`) fails loudly
    instead of silently bypassing the queue.
    """

    def _refuse(self, *_args: Any, **_kwargs: Any) -> Any:
        raise LLMQueueError("this model sends every call through the LLM queue")

    converse = converse_stream = invoke_model = get_inference_profile = _refuse


def _queue_only_values(values: Any, *slots: str) -> Any:
    """Fill the given boto3 client slots with the placeholder unless the caller supplied them."""
    if isinstance(values, dict):
        for slot in slots:
            values.setdefault(slot, _QueueOnlyClient())
    return values


class QueuedChatBedrockConverse(ChatBedrockConverse):
    """`ChatBedrockConverse` whose Converse calls go through the LLM queue.

    `model_alias` is the queue alias (`gpt120b`, `gpt120b@apac`) and defaults to `model`, so
    `QueuedChatBedrockConverse(model="gpt120b")` works; pass the real Bedrock id as `model` and
    the alias separately to keep provider detection. Streaming is always off (one full message).
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    fallback_policy: FallbackPolicy | None = None
    lane: Lane | str | None = None
    service_tier: str | None = None
    queue_timeout: float | None = None

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        values = _queue_only_values(values, "client", "bedrock_client")
        if isinstance(values, dict):
            values["disable_streaming"] = True
        return values

    def _queue(self) -> QueueClient:
        return resolve_queue(self.llmq)

    def _converse_request(
        self, messages: list[BaseMessage], stop: list[str] | None, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Build the Converse request exactly as the parent's `_generate`, minus `modelId`."""
        system: list[dict[str, Any]]
        if self.raw_blocks is not None:
            bedrock_messages, system = self.raw_blocks, []
        else:
            bedrock_messages, system = _messages_to_bedrock(messages)
            if self.guard_last_turn_only:
                self._apply_guard_last_turn_only(bedrock_messages)
        camel = _snake_to_camel_keys(kwargs, excluded_keys=_CAMEL_EXCLUDED)
        service_tier = camel.pop("serviceTier", None)
        params = self._converse_params(stop=stop, **camel)
        params.pop("modelId", None)  # the queue alias picks the model and target
        if params.get("toolConfig") is None and _has_tool_use_or_result_blocks(bedrock_messages):
            warnings.warn(
                "Tool messages were passed without toolConfig, converting to text format",
                RuntimeWarning,
                stacklevel=2,
            )
            bedrock_messages = _convert_tool_blocks_to_text(bedrock_messages)
        tier = service_tier or self.service_tier
        if tier:
            params["serviceTier"] = tier if isinstance(tier, dict) else {"type": tier}
        return {"messages": bedrock_messages, "system": system, **params}

    def _chat_result(self, result: ConverseResultLike) -> ChatResult:
        """Parse like the parent, then add how the queue served the call."""
        message = _parse_response(result.response)
        message.response_metadata["model_name"] = self.model_id
        message.response_metadata.update(result.meta.model_dump())
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Sync path: `converse_sync` (direct under the shared limiter, see README)."""
        request = self._converse_request(messages, stop, kwargs)
        result = self._queue().converse_sync(
            self.model_alias or self.model_id,
            request,
            lane=self.lane,
            fallback=self.fallback_policy,
            timeout=self.queue_timeout,
        )
        return self._chat_result(result)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Native async path: one queued Converse call (the parent only has a thread fallback)."""
        request = self._converse_request(messages, stop, kwargs)
        result = await self._queue().converse(
            self.model_alias or self.model_id,
            request,
            lane=self.lane,
            fallback=self.fallback_policy,
            timeout=self.queue_timeout,
        )
        return self._chat_result(result)


class QueuedBedrockEmbeddings(BedrockEmbeddings):
    """`BedrockEmbeddings` whose calls go through the queue's `embed` op (batched per provider).

    Queries are sent with `input_type="search_query"` and documents with `"search_document"`;
    the provider ignores it where the model has no such field. `normalize` is applied locally.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    lane: Lane | str | None = None
    queue_timeout: float | None = None

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

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents on the sync path (`embed_sync`)."""
        vectors = self._queue().embed_sync(
            self.model_alias or self.model_id,
            self._prepare(texts),
            lane=self.lane,
            input_type="search_document",
            timeout=self.queue_timeout,
        )
        return self._finish(vectors)

    def embed_query(self, text: str) -> list[float]:
        """Embed one query on the sync path (`embed_sync`)."""
        vectors = self._queue().embed_sync(
            self.model_alias or self.model_id,
            self._prepare([text]),
            lane=self.lane,
            input_type="search_query",
            timeout=self.queue_timeout,
        )
        return self._finish(vectors)[0]

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents through the queue as one call (the parent fans out per text)."""
        vectors = await self._queue().embed(
            self.model_alias or self.model_id,
            self._prepare(texts),
            lane=self.lane,
            input_type="search_document",
            timeout=self.queue_timeout,
        )
        return self._finish(vectors)

    async def aembed_query(self, text: str) -> list[float]:
        """Embed one query through the queue."""
        vectors = await self._queue().embed(
            self.model_alias or self.model_id,
            self._prepare([text]),
            lane=self.lane,
            input_type="search_query",
            timeout=self.queue_timeout,
        )
        return self._finish(vectors)[0]
