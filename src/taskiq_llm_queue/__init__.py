"""taskiq-llm-queue: an LLM request client with its own durable queue (taskiq + Amazon SQS).

This is the public front door; everything callers need is re-exported here and listed in
`__all__`. Importing the package has no side effects: no clients, threads or network I/O.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue._version import __version__
from taskiq_llm_queue.config import Lane, ModelConfig, QueueConfig, Target
from taskiq_llm_queue.context import RequestContext, current_context, request_context
from taskiq_llm_queue.envelope import (
    ENVELOPE_VERSION,
    TASK_CALL,
    TASK_EMBED,
    TASK_JOB,
    CallMeta,
    Envelope,
    LLMReply,
    parse_envelope,
)
from taskiq_llm_queue.errors import (
    ConfigError,
    ErrorInfo,
    ErrorKind,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
    UnsupportedEnvelopeError,
)
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import (
    ClaimDecision,
    ClaimState,
    ClaimStore,
    Lease,
    Limiter,
    Provider,
    ReceiveGate,
    ResultStore,
)
from taskiq_llm_queue.routing_hooks import CallPath, FailoverPolicy, PathRouter, QueueHooks

if TYPE_CHECKING:
    from taskiq_llm_queue.client import ConverseResult, LLMQueue

# Loaded on first access: the client pulls in taskiq, redis and aiobotocore, and importing the
# package must stay free of them (library rule; see tests/unit/test_contracts.py).
_LAZY = {"LLMQueue": "taskiq_llm_queue.client", "ConverseResult": "taskiq_llm_queue.client"}

__all__ = [
    "ENVELOPE_VERSION",
    "TASK_CALL",
    "TASK_EMBED",
    "TASK_JOB",
    "CallMeta",
    "CallPath",
    "ClaimDecision",
    "ClaimState",
    "ClaimStore",
    "ConfigError",
    "ConverseResult",
    "Envelope",
    "ErrorInfo",
    "ErrorKind",
    "FailoverPolicy",
    "KeySpace",
    "LLMQueue",
    "LLMQueueError",
    "LLMQueueTimeout",
    "LLMReply",
    "Lane",
    "Lease",
    "Limiter",
    "ModelConfig",
    "PathRouter",
    "Provider",
    "ProviderError",
    "QueueConfig",
    "QueueHooks",
    "QueueUnavailable",
    "ReceiveGate",
    "RequestContext",
    "ResultStore",
    "Target",
    "UnsupportedEnvelopeError",
    "__version__",
    "current_context",
    "parse_envelope",
    "request_context",
]


def __getattr__(name: str) -> Any:
    """Import the client on first use of `LLMQueue` / `ConverseResult`."""
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
