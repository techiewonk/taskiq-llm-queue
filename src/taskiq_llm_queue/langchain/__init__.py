"""LangChain adapters that send model calls through the LLM queue (extras `langchain-*`).

The adapter classes are loaded on first attribute access, so importing this package (for
example to call `set_default_queue` from `LLMQueue.start()`) never imports langchain, boto3 or
httpx. `QueuedChatBedrockConverse` / `QueuedBedrockEmbeddings` need `[langchain-aws]`;
`QueuedChatOpenAI` / `QueuedOpenAIEmbeddings` need `[openai,langchain-openai]`.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.langchain._client import (
    ConverseResultLike,
    QueueClient,
    get_default_queue,
    set_default_queue,
)

if TYPE_CHECKING:
    from taskiq_llm_queue.langchain.bedrock import (
        QueuedBedrockEmbeddings,
        QueuedChatBedrockConverse,
    )
    from taskiq_llm_queue.langchain.openai import QueuedChatOpenAI, QueuedOpenAIEmbeddings

# Lazy exports: attribute name -> (module, extra that provides its dependencies).
_LAZY = {
    "QueuedChatBedrockConverse": ("bedrock", "langchain-aws"),
    "QueuedBedrockEmbeddings": ("bedrock", "langchain-aws"),
    "QueuedChatOpenAI": ("openai", "openai,langchain-openai"),
    "QueuedOpenAIEmbeddings": ("openai", "openai,langchain-openai"),
}

__all__ = [
    "ConverseResultLike",
    "QueueClient",
    "QueuedBedrockEmbeddings",
    "QueuedChatBedrockConverse",
    "QueuedChatOpenAI",
    "QueuedOpenAIEmbeddings",
    "get_default_queue",
    "set_default_queue",
]


def __getattr__(name: str) -> Any:
    """Import an adapter's module on first use, naming the extra to install if it is missing."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, extra = _LAZY[name]
    try:
        value = getattr(import_module(f"{__name__}.{module}"), name)
    except ImportError as exc:
        raise ImportError(
            f"{name} needs the optional dependencies: uv add 'taskiq-llm-queue[{extra}]'"
        ) from exc
    globals()[name] = value
    return value
