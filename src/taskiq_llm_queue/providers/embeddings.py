"""Embedding batching limits and Bedrock request-body builders (Titan, Cohere).

An embed call is split into tasks of at most `max_batch_size` texts: Titan takes one
text per request, Cohere v3 up to 96, OpenAI `chunk_size` (2048 by default).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Literal, TypeVar

_T = TypeVar("_T")  # PEP 695 syntax needs 3.12; the package supports 3.11

TITAN_MAX_BATCH = 1
COHERE_MAX_BATCH = 96
OPENAI_DEFAULT_CHUNK_SIZE = 2048

CohereInputType = Literal["search_document", "search_query", "classification", "clustering"]


def max_batch_size(provider: str, model_id: str, *, chunk_size: int | None = None) -> int:
    """Largest number of texts one embedding request may carry for this provider and model.

    Bedrock models other than Cohere are treated like Titan (one text per request).
    """
    if provider == "openai":
        return chunk_size or OPENAI_DEFAULT_CHUNK_SIZE
    return COHERE_MAX_BATCH if "cohere" in model_id.lower() else TITAN_MAX_BATCH


def batches(items: Sequence[_T], size: int) -> Iterator[list[_T]]:
    """Yield consecutive slices of at most `size` items, preserving order."""
    if size < 1:
        raise ValueError("batch size must be >= 1")
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def titan_body(
    text: str, *, dimensions: int | None = None, normalize: bool | None = None
) -> dict[str, Any]:
    """InvokeModel body for Amazon Titan text embeddings (v2 accepts dimensions/normalize)."""
    body: dict[str, Any] = {"inputText": text}
    if dimensions is not None:
        body["dimensions"] = dimensions
    if normalize is not None:
        body["normalize"] = normalize
    return body


def cohere_body(
    texts: Sequence[str],
    *,
    input_type: CohereInputType = "search_document",
    truncate: Literal["NONE", "START", "END"] | None = None,
) -> dict[str, Any]:
    """InvokeModel body for Cohere embed v3 on Bedrock (at most 96 texts)."""
    if len(texts) > COHERE_MAX_BATCH:
        raise ValueError(f"Cohere takes at most {COHERE_MAX_BATCH} texts, got {len(texts)}")
    body: dict[str, Any] = {"texts": list(texts), "input_type": input_type}
    if truncate is not None:
        body["truncate"] = truncate
    return body


def embedding_body(model_id: str, texts: Sequence[str], **options: Any) -> dict[str, Any]:
    """Pick the Titan or Cohere body builder by model id; Titan takes exactly one text."""
    if "cohere" in model_id.lower():
        return cohere_body(texts, **options)
    if len(texts) != TITAN_MAX_BATCH:
        raise ValueError(f"Titan takes exactly one text per request, got {len(texts)}")
    return titan_body(texts[0], **options)


def parse_embeddings(decoded: Mapping[str, Any]) -> list[list[float]]:
    """Vectors from a decoded Titan (`embedding`) or Cohere (`embeddings`) response body.

    Cohere's typed form (`{"embeddings": {"float": [...]}}`) is accepted too.
    """
    if "embedding" in decoded:
        return [list(decoded["embedding"])]
    vectors = decoded.get("embeddings")
    if isinstance(vectors, Mapping):
        vectors = vectors.get("float")
    if not isinstance(vectors, list):
        raise ValueError("response has no 'embedding' or 'embeddings'")
    return [list(v) for v in vectors]
