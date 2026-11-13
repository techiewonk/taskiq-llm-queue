"""Provider helpers shared by the clients: error classification, embedding bodies, host checks.

The providers themselves (one attempt on a route) live in `taskiq_llm_queue.clients`. Importing
this package never imports aiobotocore or openai.
"""

from __future__ import annotations

from taskiq_llm_queue.providers.classify import (
    classify_bedrock,
    classify_openai,
    error_family,
    is_target_error,
    parse_retry_after,
)
from taskiq_llm_queue.providers.embeddings import (
    COHERE_MAX_BATCH,
    OPENAI_DEFAULT_CHUNK_SIZE,
    TITAN_MAX_BATCH,
    batches,
    cohere_body,
    embedding_body,
    max_batch_size,
    parse_embeddings,
    titan_body,
)

__all__ = [
    "COHERE_MAX_BATCH",
    "OPENAI_DEFAULT_CHUNK_SIZE",
    "TITAN_MAX_BATCH",
    "batches",
    "classify_bedrock",
    "classify_openai",
    "cohere_body",
    "embedding_body",
    "error_family",
    "is_target_error",
    "max_batch_size",
    "parse_embeddings",
    "parse_retry_after",
    "titan_body",
]
