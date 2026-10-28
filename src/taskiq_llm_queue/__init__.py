"""taskiq-llm-queue: an LLM request client with its own durable queue (taskiq + Amazon SQS).

This is the public front door; everything callers need is re-exported here and listed in
`__all__`. Importing the package has no side effects: no clients, threads or network I/O.
"""

from __future__ import annotations

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

__all__ = [
    "ENVELOPE_VERSION",
    "TASK_CALL",
    "TASK_EMBED",
    "TASK_JOB",
    "CallMeta",
    "ClaimDecision",
    "ClaimState",
    "ClaimStore",
    "ConfigError",
    "Envelope",
    "ErrorInfo",
    "ErrorKind",
    "KeySpace",
    "LLMQueueError",
    "LLMQueueTimeout",
    "LLMReply",
    "Lane",
    "Lease",
    "Limiter",
    "ModelConfig",
    "Provider",
    "ProviderError",
    "QueueConfig",
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
