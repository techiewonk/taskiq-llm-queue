"""Error kinds a caller can receive (plan §3.19), as one exception type with a `kind`.

Callers catch `CallFailed` (an `LLMQueueError`) and branch on `kind`; `retry_after_secs` is set
where the plan says a later retry can work, `partial_text` for a stream cut mid-answer.
"""

from __future__ import annotations

from taskiq_llm_queue.errors import LLMQueueError

KINDS = frozenset(
    {
        "rate_limited",
        "deadline_exceeded",
        "request_too_large",
        "no_deployment",
        "model_unavailable",
        "model_disabled",
        "queue_full",
        "queue_unavailable",
        "credentials_unavailable",
        "retry_budget_exhausted",
        "request_id_conflict",
        "result_expired",
        "dead_lettered",
        "worker_lost",
        "cancelled",
        "provider_error",
        "rejected",
    }
)


class CallFailed(LLMQueueError):
    """A call ended without a reply; `kind` is one of `KINDS`."""

    def __init__(
        self,
        kind: str,
        message: str = "",
        *,
        retry_after_secs: float | None = None,
        partial_text: str | None = None,
        request_id: str | None = None,
    ) -> None:
        """Keep the kind and its details."""
        super().__init__(f"{kind}: {message}" if message else kind)
        self.kind = kind
        self.retry_after_secs = retry_after_secs
        self.partial_text = partial_text
        self.request_id = request_id
