"""Blocking (sync) calls: `converse_sync`, `chat_sync`, `embed_sync` and sync LangChain methods.

Sync callers cannot await a queued result, so sync methods run the call `direct` (in this
process, on a private event loop thread) under the *same* shared adaptive limiter the queue
uses. Each one counts as a "sync fallback" (`llmq.sync_fallback_count`, metric
`llmq_sync_fallback_total`); set `QueueConfig(strict_async=True)` to refuse them instead.

Call sync methods from a worker thread (`asyncio.to_thread`, a thread pool, a sync web
framework), never directly on a running event loop: they block until the reply arrives.
`meta.worker` is `sync:<pod id>` for these calls.

Run: .venv/Scripts/python samples/11_sync_calls.py
"""

from __future__ import annotations

import asyncio

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue, LLMQueueError
from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


def legacy_sync_code(llmq: LLMQueue) -> None:
    """Sync code (e.g. a library without async support) that needs LLM calls."""
    result = llmq.converse_sync("gpt20b", HELLO, lane="rt")
    print("converse_sync :", result.meta.served_model, "worker =", result.meta.worker)

    chat = llmq.chat_sync("llama", {"messages": [{"role": "user", "content": "hi"}]})
    print("chat_sync     :", chat.response["choices"][0]["message"]["content"][:40])

    vectors = llmq.embed_sync("cohere", ["a", "b", "c"], input_type="search_query")
    print("embed_sync    :", len(vectors), "vectors")

    # Sync LangChain methods (embed_query, invoke, ...) take the same path.
    embeddings = QueuedBedrockEmbeddings(
        llmq=llmq, model_id="amazon.titan-embed-text-v2:0", model_alias="titan"
    )
    print("embed_query   :", len(embeddings.embed_query("hello")), "dims")


async def main() -> None:
    """Run the sync code in a thread so the event loop keeps serving other work."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start(mode="send")  # no worker needed: sync calls run direct
    try:
        await asyncio.to_thread(legacy_sync_code, llmq)
        print("sync_fallback_count =", llmq.sync_fallback_count)
    finally:
        await llmq.stop()

    # With strict_async=True, sync calls are refused instead of silently running direct.
    strict = LLMQueue(local_config(strict_async=True))
    try:
        strict.converse_sync("gpt20b", HELLO)
    except LLMQueueError as exc:
        print("strict_async  :", exc)


if __name__ == "__main__":
    asyncio.run(main())
