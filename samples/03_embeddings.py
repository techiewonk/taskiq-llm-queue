"""Embeddings through the queue: Titan, Cohere and OpenAI-compatible, batched per provider.

`llmq.embed(alias, texts)` splits `texts` into one task per provider batch (Titan 1 text,
Cohere 96, OpenAI 2048), runs them in parallel and returns the vectors in input order.
Embeddings never switch model on fallback (vectors from two models are not comparable).

Also shows `invoke_model` (the raw Bedrock body as bytes) and the `QueuedBedrockEmbeddings`
LangChain adapter. Bulk ingestion should use `lane="bulk"` so it never delays live traffic.

Run: .venv/Scripts/python samples/03_embeddings.py
"""

from __future__ import annotations

import asyncio
import time

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue
from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings

TEXTS = [f"research note {i}: margins improved quarter on quarter" for i in range(100)]


async def main() -> None:
    """Embed the same 100 texts with three providers and compare shapes."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        for alias, input_type in (
            ("titan", None),  # 100 tasks of 1 text
            ("cohere", "search_document"),  # 2 tasks (96 + 4); input_type is Cohere-only
            ("llama", None),  # 1 task (OpenAI-compatible /v1/embeddings)
        ):
            t0 = time.perf_counter()
            vectors = await llmq.embed(alias, TEXTS, lane="bulk", input_type=input_type)
            secs = time.perf_counter() - t0
            print(f"{alias:7s}: {len(vectors)} vectors x {len(vectors[0])} dims in {secs:.2f}s")

        # Vectors are deterministic per text in fakellm, so a repeat matches.
        again = await llmq.embed("titan", [TEXTS[1]])
        first = await llmq.embed("titan", TEXTS[:2])
        print("titan deterministic:", again[0] == first[1])

        # invoke_model: raw provider body (bytes) + content type, for anything not covered.
        raw = await llmq.invoke_model("titan", {"inputText": "hello", "dimensions": 256})
        body = raw.json()
        print("invoke_model       :", raw.content_type, "->", len(body["embedding"]), "dims")

        # LangChain adapter: aembed_documents is one queued `embed` call.
        # Pass the real Bedrock id as model_id (LangChain uses it to detect the provider) and
        # the queue alias as model_alias.
        embeddings = QueuedBedrockEmbeddings(
            llmq=llmq, model_id="cohere.embed-english-v3", model_alias="cohere"
        )
        docs = await embeddings.aembed_documents(TEXTS[:3])
        query = await embeddings.aembed_query("margins")
        print("QueuedBedrockEmbeddings:", len(docs), "docs,", len(query), "dims per query")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
