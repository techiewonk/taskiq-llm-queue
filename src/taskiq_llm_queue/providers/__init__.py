"""LLM providers: Amazon Bedrock and OpenAI-compatible servers behind the `Provider` protocol.

Workers call `ProviderRegistry.for_model(alias).call(op, params, target)` and classify
failures with `.classify(exc)`. Importing this package never imports aiobotocore or openai;
each SDK is imported on a provider's first call.
"""

from __future__ import annotations

from taskiq_llm_queue.providers.bedrock import BedrockProvider, decode_invoke_body
from taskiq_llm_queue.providers.classify import (
    classify_bedrock,
    classify_openai,
    error_family,
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
from taskiq_llm_queue.providers.hosts import check_url, denied_pattern
from taskiq_llm_queue.providers.openai_compat import OpenAICompatProvider
from taskiq_llm_queue.providers.registry import (
    BUILTIN_PROVIDERS,
    ENTRY_POINT_GROUP,
    ProviderFactory,
    ProviderRegistry,
    load_object,
)

__all__ = [
    "BUILTIN_PROVIDERS",
    "COHERE_MAX_BATCH",
    "ENTRY_POINT_GROUP",
    "OPENAI_DEFAULT_CHUNK_SIZE",
    "TITAN_MAX_BATCH",
    "BedrockProvider",
    "OpenAICompatProvider",
    "ProviderFactory",
    "ProviderRegistry",
    "batches",
    "check_url",
    "classify_bedrock",
    "classify_openai",
    "cohere_body",
    "decode_invoke_body",
    "denied_pattern",
    "embedding_body",
    "error_family",
    "load_object",
    "max_batch_size",
    "parse_embeddings",
    "parse_retry_after",
    "titan_body",
]
