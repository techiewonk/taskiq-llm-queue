"""Key names of the 0.3 engine (key schema 3), all built from one environment prefix.

Keys of one model share the tag `{m:ALIAS}` and keys of one request `{r:ID}`; the admission and
finish scripts touch both, which is fine on the single-primary store 0.3 supports (plan §3.2).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Keys:
    """Key builder for one prefix, e.g. `llmq:prod` -> `llmq:prod:{m:gpt120b}:wait:p`."""

    prefix: str

    # ---------------------------------------------------------------- per model

    def model(self, alias: str, name: str) -> str:
        """A key of one model, e.g. `cfg`, `rot`, `health`, `inprog`, `wait:p`."""
        return f"{self.prefix}:{{m:{alias}}}:{name}"

    def rate(self, alias: str, deployment: str) -> str:
        """RPM / TPM budget of one deployment (hash of clocks and minute counters)."""
        return self.model(alias, f"rate:{deployment}")

    def limiter(self, alias: str, deployment: str) -> str:
        """Adaptive concurrency limit of one deployment (hash)."""
        return self.model(alias, f"ac:{deployment}")

    def breaker(self, alias: str, deployment: str) -> str:
        """Circuit breaker of one deployment (hash)."""
        return self.model(alias, f"cb:{deployment}")

    # ---------------------------------------------------------------- per request

    def request(self, request_id: str, name: str) -> str:
        """A key of one request: `job` (claim), `res` (result), `params`, `cancel`, `chunks`."""
        return f"{self.prefix}:{{r:{request_id}}}:{name}"

    def status(self, request_id: str) -> str:
        """Request status (hash, `request_status`)."""
        return self.request(request_id, "status")

    # ---------------------------------------------------------------- per process

    def caller(self, caller: str) -> str:
        """Liveness of one calling process (`pod:boot`)."""
        return f"{self.prefix}:caller:{caller}"

    def done_channel(self, caller: str) -> str:
        """Pub/sub channel that wakes one caller's result reader."""
        return f"{self.prefix}:done:{caller}"

    @property
    def cancel_channel(self) -> str:
        """Pub/sub channel naming request ids just cancelled."""
        return f"{self.prefix}:cancel"

    @property
    def epoch(self) -> str:
        """Set once per store life; missing means the store restarted or lost its data."""
        return f"{self.prefix}:epoch"

    # ---------------------------------------------------------------- queues

    def queue(self, name: str, part: str) -> str:
        """A key of one queue: `s` (stream), `vis`, `gen`, `fails`, `st`, `dlq`, `meta`."""
        return f"{self.prefix}:{{q:{name}}}:{part}"
