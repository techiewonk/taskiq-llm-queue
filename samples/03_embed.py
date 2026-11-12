"""Embed texts with Titan and Cohere (search indexing, retrieval, follow-up cards).

`embed` returns one vector per text, in order. The library splits the texts into batches of
the model's `embed_batch_size` (Titan takes 1 text per request, Cohere up to 96), and every
batch goes through the queue and the model's RPM budget. Cohere needs `input_type`:
`search_document` when indexing, `search_query` when searching.

    python scripts/samples.py 03_embed [--live]
"""

from __future__ import annotations

import asyncio
import math

from taskiq_llm_queue import LLMQueue

TEXTS = [
    "Nifty 50 closed higher on banking gains.",
    "Banking stocks lifted the Nifty index today.",
    "The monsoon arrived early in Kerala.",
]


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        for model, input_type in (("titan", None), ("cohere", "search_document")):
            result = await llmq.embed(model, TEXTS, input_type=input_type)
            vectors = result.vectors
            assert len(vectors) == len(TEXTS), f"{model}: {len(vectors)} vectors"
            close, far = cosine(vectors[0], vectors[1]), cosine(vectors[0], vectors[2])
            print(
                f"{model}: {len(vectors)} vectors of {len(vectors[0])} dims; "
                f"similar pair {close:.3f}, unrelated pair {far:.3f}"
            )
        query = await llmq.embed("cohere", ["bank index rally"], input_type="search_query")
        print(f"cohere query vector: {len(query.vectors[0])} dims")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
