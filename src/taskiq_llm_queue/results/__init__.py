"""Results: the JSON result backend and the per-pod batched result dispatcher (plan D80).

Workers write results through `LLMQResultBackend` and wake the sender's pod with
`publish_done`; senders wait on `ResultDispatcher.wait`.
"""

from __future__ import annotations

from taskiq_llm_queue.results.backend import DEFAULT_TAKE_BATCH, LLMQResultBackend, decode_reply
from taskiq_llm_queue.results.dispatcher import (
    DEFAULT_INTERVALS_MS,
    ResultDispatcher,
    publish_done,
)

__all__ = [
    "DEFAULT_INTERVALS_MS",
    "DEFAULT_TAKE_BATCH",
    "LLMQResultBackend",
    "ResultDispatcher",
    "decode_reply",
    "publish_done",
]
