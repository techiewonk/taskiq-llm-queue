"""Every Redis/Valkey key and channel name, built from one configurable prefix.

Keeping names in one place lets several environments share a Redis without collisions and
keeps the state schema (versioned under `schema`) easy to migrate.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class KeySpace:
    """Key builder for one prefix, e.g. `llmq:prod` -> `llmq:prod:job:<id>`."""

    prefix: str

    def job(self, request_id: str) -> str:
        """Claim state of one task (hash): state, worker, heartbeat, failures."""
        return f"{self.prefix}:job:{request_id}"

    def params(self, request_id: str) -> str:
        """Large params stored by reference (string)."""
        return f"{self.prefix}:req:{request_id}"

    def cancel(self, request_id: str) -> str:
        """Cancel flag for one task (string)."""
        return f"{self.prefix}:cancel:{request_id}"

    def result(self, task_id: str) -> str:
        """JSON result of one task, deleted on read (string)."""
        return f"{self.prefix}:res:{task_id}"

    def limiter(self, model: str, target: str) -> str:
        """Adaptive limiter state for one model target (hash, no TTL)."""
        return f"{self.prefix}:ac:{model}:{target}"

    def leases(self, model: str, target: str) -> str:
        """In-flight slot leases scored by expiry (sorted set)."""
        return f"{self.prefix}:lease:{model}:{target}"

    def breaker(self, model: str, target: str) -> str:
        """Circuit breaker state for one model target (hash, no TTL)."""
        return f"{self.prefix}:cb:{model}:{target}"

    def done_channel(self, pod: str) -> str:
        """Pub/sub channel that wakes one pod's result dispatcher."""
        return f"{self.prefix}:done:{pod}"

    @property
    def routing(self) -> str:
        """Live canary routing config (JSON string)."""
        return f"{self.prefix}:cfg:routing"

    @property
    def fallback(self) -> str:
        """Live fallback chains (JSON string)."""
        return f"{self.prefix}:cfg:fallback"

    @property
    def audit(self) -> str:
        """Audit stream of config changes (who, before, after)."""
        return f"{self.prefix}:cfg:audit"

    @property
    def schema(self) -> str:
        """State schema version marker."""
        return f"{self.prefix}:schema"
