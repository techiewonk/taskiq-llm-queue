"""taskiq-llm-queue: LLM calls through a queue, balanced across accounts and regions.

This is the public front door. `LLMQueue` (loaded on first use, so importing the package has
no side effects) runs calls through each model's queue under RPM / TPM budgets per deployment
(model x account x region), with fallback regions and models, health and status. The registry
(`LLMQ_REGISTRY`) describes accounts, models and settings; the extension points are plain
functions registered by name.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue._version import __version__
from taskiq_llm_queue.context import RequestContext, current_context, request_context
from taskiq_llm_queue.engine.errors import KINDS, CallFailed
from taskiq_llm_queue.engine.hooks import (
    CallContext,
    DeploymentView,
    Reject,
    register_admission,
    register_balancer,
    register_fallback_policy,
    register_probe,
    register_sticky_key,
)
from taskiq_llm_queue.errors import (
    ConfigError,
    LLMQueueError,
    MissingExtra,
    StoreUnavailable,
    UnknownModel,
)
from taskiq_llm_queue.tuning import (
    BedrockOptions,
    EngineTimings,
    RegistryOptions,
    StoreOptions,
    Tuning,
)

if TYPE_CHECKING:
    from taskiq_llm_queue.clients.providers import register_provider
    from taskiq_llm_queue.engine.envelope import Meta, Reply
    from taskiq_llm_queue.engine.tokens import register_token_estimator
    from taskiq_llm_queue.llm_queue import EmbedResult, LLMQueue
    from taskiq_llm_queue.registry import RegistryError, register_account_kind

# Loaded on first access: these pull in redis, pydantic models or the SDK layer.
_LAZY = {
    "LLMQueue": "taskiq_llm_queue.llm_queue",
    "EmbedResult": "taskiq_llm_queue.llm_queue",
    "Reply": "taskiq_llm_queue.engine.envelope",
    "Meta": "taskiq_llm_queue.engine.envelope",
    "RegistryError": "taskiq_llm_queue.registry",
    "register_account_kind": "taskiq_llm_queue.registry",
    "register_provider": "taskiq_llm_queue.clients.providers",
    "register_token_estimator": "taskiq_llm_queue.engine.tokens",
}

__all__ = [
    "KINDS",
    "BedrockOptions",
    "CallContext",
    "CallFailed",
    "ConfigError",
    "DeploymentView",
    "EmbedResult",
    "EngineTimings",
    "LLMQueue",
    "LLMQueueError",
    "Meta",
    "MissingExtra",
    "RegistryError",
    "RegistryOptions",
    "Reject",
    "Reply",
    "RequestContext",
    "StoreOptions",
    "StoreUnavailable",
    "Tuning",
    "UnknownModel",
    "__version__",
    "current_context",
    "register_account_kind",
    "register_admission",
    "register_balancer",
    "register_fallback_policy",
    "register_probe",
    "register_provider",
    "register_sticky_key",
    "register_token_estimator",
    "request_context",
]


def __getattr__(name: str) -> Any:
    """Import `LLMQueue` and the heavier names on first use."""
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
