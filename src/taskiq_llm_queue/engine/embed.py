"""Embeddings over the queue (plan step A13): batching, provider bodies, vectors in order.

`embed()` splits texts into batches of the model's `embed_batch_size` (capped by what the
provider accepts: Titan 1, Cohere 96, OpenAI 2048), sends each batch as one call (one request
against RPM; tokens only when token limiting is on for the model), runs them concurrently and
returns one vector per text in the input order. Inference defaults (dimensions, input_type,
...) are added by the worker from the registry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.clients.providers import decode_invoke_body
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.providers.embeddings import batches, max_batch_size, parse_embeddings

if TYPE_CHECKING:
    from taskiq_llm_queue.engine.runtime import Engine


def batch_size(provider: str, model_id: str, setting: int) -> int:
    """Texts per call: the model's `embed_batch_size`, never above the provider's limit."""
    return max(1, min(int(setting), max_batch_size(provider, model_id)))


def request_for(
    provider: str, model_id: str, texts: Sequence[str], input_type: str | None = None
) -> dict[str, Any]:
    """The provider-native request of one batch (defaults are merged by the worker).

    `input_type` (Cohere: `search_query` / `search_document`) wins over the registry default.
    """
    if provider == "openai":
        return {"input": list(texts)}
    if "cohere" in model_id.lower():
        body: dict[str, Any] = {"texts": list(texts)}
        if input_type:
            body["input_type"] = input_type
        return {"body": body}
    return {"body": {"inputText": texts[0]}}


def vectors_of(provider: str, response: dict[str, Any]) -> list[list[float]]:
    """The vectors of one reply, in the order of its texts."""
    if provider == "openai":
        data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
        return [list(item["embedding"]) for item in data]
    return parse_embeddings(decode_invoke_body(response))


async def embed(
    engine: Engine,
    model: str,
    texts: Sequence[str],
    *,
    priority: bool = False,
    timeout_secs: float | None = None,
    concurrency: int = 16,
    input_type: str | None = None,
    **call: Any,
) -> list[list[float]]:
    """One vector per text, in order; the batches run through the queue concurrently."""
    if not texts:
        return []
    view = engine.registry.current
    alias = view.registry.alias_of(model)
    entry = view.registry.models[alias]
    size = batch_size(entry.provider, entry.model_id, int(view.value(alias, "embed_batch_size")))
    groups = list(batches(list(texts), size))
    gate = asyncio.Semaphore(concurrency)

    async def one(group: list[str]) -> list[list[float]]:
        async with gate:
            reply = await engine.call(
                alias,
                request_for(entry.provider, entry.model_id, group, input_type),
                priority=priority,
                timeout_secs=timeout_secs,
                **call,
            )
        if reply.response is None:
            raise CallFailed(
                "provider_error", f"{alias}: no embedding body", request_id=reply.request_id
            )
        vectors = vectors_of(entry.provider, reply.response)
        if len(vectors) != len(group):
            message = f"{alias}: {len(group)} texts gave {len(vectors)} vectors"
            raise CallFailed("provider_error", message, request_id=reply.request_id)
        return vectors

    tasks = [asyncio.ensure_future(one(group)) for group in groups]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        # One batch failed: stop the rest, which would only spend budget on a lost result.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return [vector for group in results for vector in group]
