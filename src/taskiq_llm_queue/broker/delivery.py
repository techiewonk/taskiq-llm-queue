"""One received SQS message and its local lifecycle: buffered, started, then acked or released.

The broker, the heartbeat and `release()` all act on the same `Delivery`, so its `done`
flag makes ack, release and heartbeat stop idempotent and race-free within one event loop.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskiq_llm_queue.broker.queue import SQSQueue


@dataclass(slots=True, eq=False)
class Delivery:
    """A received message: where it came from, its receipt handle and its local state.

    `lost` is set when a heartbeat finds the receipt handle invalid (another consumer may
    now own the message), so the running task can stop without writing a result (DS6).
    """

    queue: SQSQueue
    queue_url: str
    message_id: str
    receipt_handle: str
    body: str
    receive_count: int
    received_at: float
    visibility_secs: int
    sent_at: float | None = None
    started: bool = False
    done: bool = False
    extensions: int = 0
    next_heartbeat_at: float = math.inf
    lost: asyncio.Event = field(default_factory=asyncio.Event)
