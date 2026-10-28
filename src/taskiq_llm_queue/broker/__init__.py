"""SQS broker for taskiq with priority, capacity-aware receive and batching (see docs/DESIGN.md).

`HybridSQSBroker` plus its settings (`SQSQueue`, `Batching`, `Heartbeat`), event hooks and
the SQS error-family classifier. Importing this package creates no client and does no I/O.
"""

from __future__ import annotations

from taskiq_llm_queue.broker.acker import VisibilityResult
from taskiq_llm_queue.broker.broker import (
    DEDUPLICATION_ID_LABEL,
    DELAY_LABEL,
    EXPIRY_ATTRIBUTE,
    EXPIRY_LABEL,
    GROUP_ID_LABEL,
    QUEUE_LABEL,
    SKIP_BATCHING_LABEL,
    SQS_MESSAGE_ID_LABEL,
    SQS_QUEUE_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
    SQS_RECEIVE_COUNT_LABEL,
    HybridSQSBroker,
    ReceiveBudget,
)
from taskiq_llm_queue.broker.delivery import Delivery
from taskiq_llm_queue.broker.errors import (
    BrokerConfigError,
    ErrorFamily,
    InvalidLabelError,
    MessageTooLargeError,
    SQSSendError,
    backoff_delay,
    classify_code,
    classify_error,
)
from taskiq_llm_queue.broker.events import BrokerHooks
from taskiq_llm_queue.broker.filters import MessageFilter, envelope_supported
from taskiq_llm_queue.broker.queue import Batching, Heartbeat, SQSQueue
from taskiq_llm_queue.broker.s3_offload import (
    OFFLOAD_LABEL,
    OffloadedPayloadMissingError,
    S3OffloadError,
    S3OffloadMiddleware,
)
from taskiq_llm_queue.broker.scheduler import SlotScheduler

__all__ = [
    "DEDUPLICATION_ID_LABEL",
    "DELAY_LABEL",
    "EXPIRY_ATTRIBUTE",
    "EXPIRY_LABEL",
    "GROUP_ID_LABEL",
    "OFFLOAD_LABEL",
    "QUEUE_LABEL",
    "SKIP_BATCHING_LABEL",
    "SQS_MESSAGE_ID_LABEL",
    "SQS_QUEUE_LABEL",
    "SQS_RECEIPT_HANDLE_LABEL",
    "SQS_RECEIVE_COUNT_LABEL",
    "Batching",
    "BrokerConfigError",
    "BrokerHooks",
    "Delivery",
    "ErrorFamily",
    "Heartbeat",
    "HybridSQSBroker",
    "InvalidLabelError",
    "MessageFilter",
    "MessageTooLargeError",
    "OffloadedPayloadMissingError",
    "ReceiveBudget",
    "S3OffloadError",
    "S3OffloadMiddleware",
    "SQSQueue",
    "SQSSendError",
    "SlotScheduler",
    "VisibilityResult",
    "backoff_delay",
    "classify_code",
    "classify_error",
    "envelope_supported",
]
