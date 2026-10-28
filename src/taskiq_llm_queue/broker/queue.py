"""Queue, send-batching and heartbeat settings for `HybridSQSBroker`, validated on creation.

Every SQS hard limit (10 messages per call, 20 s long poll, 900 s delay, 1 MiB payload,
128-character FIFO ids) is checked here, so a bad value fails at boot instead of on the
first send or receive (plan P3, P16).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from taskiq_llm_queue.broker.errors import BrokerConfigError

if TYPE_CHECKING:
    from taskiq_llm_queue.protocols import ReceiveGate

MAX_BATCH_ENTRIES = 10
MAX_WAIT_TIME_SECS = 20
MAX_DELAY_SECS = 900
MAX_VISIBILITY_SECS = 43_200
MAX_MESSAGE_BYTES = 1_048_576
MAX_BATCH_BYTES = 1_048_576
# DS15: flush a send batch before 900 KB so attribute overhead never crosses the 1 MiB cap.
DEFAULT_BATCH_BYTES = 900_000
MAX_FIFO_ID_CHARS = 128


@dataclass(frozen=True, slots=True)
class Batching:
    """Send batching for one queue (P8): up to `size` messages or `max_bytes` per call.

    A partial batch is sent after `timeout_secs`; `kick` awaits the send so callers still
    see failures.
    """

    size: int = MAX_BATCH_ENTRIES
    timeout_secs: float = 0.05
    max_bytes: int = DEFAULT_BATCH_BYTES

    def __post_init__(self) -> None:
        """Reject values SQS would refuse."""
        errors = []
        if not 1 <= self.size <= MAX_BATCH_ENTRIES:
            errors.append(f"batching.size must be 1..{MAX_BATCH_ENTRIES}, got {self.size}")
        if not 0 <= self.timeout_secs <= 10:
            errors.append(f"batching.timeout_secs must be 0..10, got {self.timeout_secs}")
        if not 1024 <= self.max_bytes <= MAX_BATCH_BYTES:
            errors.append(f"batching.max_bytes must be 1024..{MAX_BATCH_BYTES}")
        if errors:
            raise BrokerConfigError("; ".join(errors))


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """Visibility extension for received messages (P13, DS6), sent in batches.

    `interval_secs` defaults to (and is capped at) a third of the queue's visibility timeout;
    `max_extensions=None` extends until the message is acked or released.
    """

    interval_secs: float | None = None
    max_extensions: int | None = None
    flush_secs: float = 0.5

    def __post_init__(self) -> None:
        """Reject non-positive intervals and negative extension counts."""
        if self.interval_secs is not None and self.interval_secs <= 0:
            raise BrokerConfigError("heartbeat.interval_secs must be > 0")
        if self.max_extensions is not None and self.max_extensions < 0:
            raise BrokerConfigError("heartbeat.max_extensions must be >= 0")
        if not 0 < self.flush_secs <= 1:
            raise BrokerConfigError("heartbeat.flush_secs must be in (0, 1] (DS6)")

    def interval_for(self, visibility_secs: int) -> float:
        """Seconds between extensions for a queue with this visibility timeout (≤ visibility/3)."""
        limit = max(visibility_secs / 3, 0.1)
        return limit if self.interval_secs is None else min(self.interval_secs, limit)


@dataclass(frozen=True, slots=True)
class SQSQueue:
    """One SQS queue the broker sends to and receives from.

    `name` is the routing key (`queue_name` label) and the SQS queue name unless `url` is
    given. `priority` 0 is highest. `visibility_timeout_secs=None` reads the queue's own
    setting at startup. `gate` pauses receiving while closed (A4).
    """

    name: str
    priority: int = 0
    url: str | None = None
    wait_time_secs: int = MAX_WAIT_TIME_SECS
    max_messages: int = MAX_BATCH_ENTRIES
    visibility_timeout_secs: int | None = None
    is_fifo: bool | None = None
    is_fair: bool = False
    default_delay_secs: int = 0
    batching: Batching | None = None
    is_declare: bool = False
    attributes: Mapping[str, str] = field(default_factory=dict)
    gate: ReceiveGate | None = None

    def __post_init__(self) -> None:
        """Validate every limit at once and derive `is_fifo` from the `.fifo` suffix."""
        sqs_name = (self.url or self.name).rstrip("/").rsplit("/", 1)[-1]
        if self.is_fifo is None:
            object.__setattr__(self, "is_fifo", sqs_name.endswith(".fifo"))
        errors = []
        if not self.name:
            errors.append("name is empty")
        if self.priority < 0:
            errors.append(f"priority must be >= 0, got {self.priority}")
        if not 0 <= self.wait_time_secs <= MAX_WAIT_TIME_SECS:
            errors.append(f"wait_time_secs must be 0..{MAX_WAIT_TIME_SECS}")
        if not 1 <= self.max_messages <= MAX_BATCH_ENTRIES:
            errors.append(f"max_messages must be 1..{MAX_BATCH_ENTRIES}")
        if self.visibility_timeout_secs is not None and not (
            0 <= self.visibility_timeout_secs <= MAX_VISIBILITY_SECS
        ):
            errors.append(f"visibility_timeout_secs must be 0..{MAX_VISIBILITY_SECS}")
        if not 0 <= self.default_delay_secs <= MAX_DELAY_SECS:
            errors.append(f"default_delay_secs must be 0..{MAX_DELAY_SECS}")
        if self.is_fifo and not sqs_name.endswith(".fifo"):
            errors.append("a FIFO queue name must end with '.fifo'")
        if self.is_fifo and self.is_fair:
            errors.append("is_fair applies to standard queues only")
        if self.is_fifo and self.default_delay_secs:
            errors.append("per-message delay is not supported on FIFO queues (DS19)")
        if errors:
            raise BrokerConfigError(f"queue {self.name!r}: " + "; ".join(errors))

    @property
    def fifo(self) -> bool:
        """`is_fifo` as a plain bool (resolved in `__post_init__`)."""
        return bool(self.is_fifo)
