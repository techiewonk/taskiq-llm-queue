"""Pre-receive checks that decide whether a message may reach taskiq's Receiver (DS9).

taskiq's Receiver acks (deletes) messages whose task it does not know, which would lose
work during a rolling deploy. The broker runs these checks first and releases rejected
messages with a short visibility instead, so a pod that knows the task picks them up.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.envelope import ENVELOPE_VERSION

if TYPE_CHECKING:
    from taskiq.message import TaskiqMessage

MessageFilter = Callable[["TaskiqMessage"], bool]


def envelope_supported(message: TaskiqMessage) -> bool:
    """False when any argument is an envelope (`v` + `request_id`) newer than this worker.

    Older-but-supported and malformed envelopes pass; the task itself rejects malformed ones.
    """
    values: list[Any] = [*message.args, *message.kwargs.values()]
    for value in values:
        if isinstance(value, dict) and "request_id" in value:
            version = value.get("v")
            if isinstance(version, int) and version > ENVELOPE_VERSION:
                return False
    return True
