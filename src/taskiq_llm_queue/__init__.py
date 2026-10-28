"""taskiq-llm-queue: an LLM request client with its own durable queue (taskiq + Amazon SQS).

This is the public front door: `__all__` lists the client surface (the client and its
results, config, request context, errors and extension points). Wire and plumbing names live
in their submodules (`envelope`, `keys`, `protocols`, `errors`, `routing_hooks`). Importing
the package has no side effects: no clients, threads or network I/O.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue._version import __version__
from taskiq_llm_queue.config import (
    BreakerTuning,
    Lane,
    LimiterTuning,
    ModelConfig,
    QueueConfig,
    Target,
)
from taskiq_llm_queue.context import RequestContext, current_context, request_context
from taskiq_llm_queue.envelope import CallMeta, FallbackPolicy
from taskiq_llm_queue.errors import (
    ConfigError,
    LLMQueueCancelled,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
    UnsupportedEnvelopeError,
)
from taskiq_llm_queue.routing_hooks import FailoverPolicy, PathRouter, QueueHooks

if TYPE_CHECKING:
    from taskiq_llm_queue.client import (
        ChatResult,
        ConverseResult,
        EmbedResult,
        InvokeResult,
        LLMQueue,
    )

    # Plumbing kept importable for compatibility (typed for mypy; not in __all__).
    from taskiq_llm_queue.envelope import ENVELOPE_VERSION as ENVELOPE_VERSION
    from taskiq_llm_queue.envelope import TASK_CALL as TASK_CALL
    from taskiq_llm_queue.envelope import TASK_EMBED as TASK_EMBED
    from taskiq_llm_queue.envelope import TASK_JOB as TASK_JOB
    from taskiq_llm_queue.envelope import Envelope as Envelope
    from taskiq_llm_queue.envelope import LLMReply as LLMReply
    from taskiq_llm_queue.envelope import parse_envelope as parse_envelope
    from taskiq_llm_queue.errors import ErrorInfo as ErrorInfo
    from taskiq_llm_queue.errors import ErrorKind as ErrorKind
    from taskiq_llm_queue.keys import KeySpace as KeySpace
    from taskiq_llm_queue.protocols import ClaimDecision as ClaimDecision
    from taskiq_llm_queue.protocols import ClaimState as ClaimState
    from taskiq_llm_queue.protocols import ClaimStore as ClaimStore
    from taskiq_llm_queue.protocols import Lease as Lease
    from taskiq_llm_queue.protocols import Limiter as Limiter
    from taskiq_llm_queue.protocols import Provider as Provider
    from taskiq_llm_queue.protocols import ReceiveGate as ReceiveGate
    from taskiq_llm_queue.protocols import ResultStore as ResultStore
    from taskiq_llm_queue.routing_hooks import CallPath as CallPath

# Loaded on first access: the client pulls in taskiq, redis and aiobotocore, and importing the
# package must stay free of them (library rule; see tests/unit/test_contracts.py).
_CLIENT = "taskiq_llm_queue.client"
_LAZY = {
    "LLMQueue": _CLIENT,
    "ConverseResult": _CLIENT,
    "ChatResult": _CLIENT,
    "InvokeResult": _CLIENT,
    "EmbedResult": _CLIENT,
}
# Wire / plumbing names that used to be re-exported here: still importable from the package
# for compatibility, but not public API; import them from their submodules.
_PLUMBING = {
    "ENVELOPE_VERSION": "taskiq_llm_queue.envelope",
    "TASK_CALL": "taskiq_llm_queue.envelope",
    "TASK_EMBED": "taskiq_llm_queue.envelope",
    "TASK_JOB": "taskiq_llm_queue.envelope",
    "Envelope": "taskiq_llm_queue.envelope",
    "LLMReply": "taskiq_llm_queue.envelope",
    "parse_envelope": "taskiq_llm_queue.envelope",
    "KeySpace": "taskiq_llm_queue.keys",
    "ErrorInfo": "taskiq_llm_queue.errors",
    "ErrorKind": "taskiq_llm_queue.errors",
    "CallPath": "taskiq_llm_queue.routing_hooks",
    "ClaimDecision": "taskiq_llm_queue.protocols",
    "ClaimState": "taskiq_llm_queue.protocols",
    "ClaimStore": "taskiq_llm_queue.protocols",
    "Lease": "taskiq_llm_queue.protocols",
    "Limiter": "taskiq_llm_queue.protocols",
    "Provider": "taskiq_llm_queue.protocols",
    "ReceiveGate": "taskiq_llm_queue.protocols",
    "ResultStore": "taskiq_llm_queue.protocols",
}

__all__ = [
    "BreakerTuning",
    "CallMeta",
    "ChatResult",
    "ConfigError",
    "ConverseResult",
    "EmbedResult",
    "FailoverPolicy",
    "FallbackPolicy",
    "InvokeResult",
    "LLMQueue",
    "LLMQueueCancelled",
    "LLMQueueError",
    "LLMQueueTimeout",
    "Lane",
    "LimiterTuning",
    "ModelConfig",
    "PathRouter",
    "ProviderError",
    "QueueConfig",
    "QueueHooks",
    "QueueUnavailable",
    "RequestContext",
    "Target",
    "UnsupportedEnvelopeError",
    "__version__",
    "current_context",
    "request_context",
]


def __getattr__(name: str) -> Any:
    """Import the client on first use of `LLMQueue` / a result type; plumbing names likewise."""
    module = _LAZY.get(name) or _PLUMBING.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
