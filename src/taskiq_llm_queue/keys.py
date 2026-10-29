"""Every Redis/Valkey key and channel name, built from one configurable prefix.

Keeping names in one place lets several environments share a Redis without collisions and
keeps the state schema (versioned under `schema`) easy to migrate. Keys that one Lua script
touches together share a hash tag (`{...}`), so they map to one slot on cluster-mode Valkey.
"""

from __future__ import annotations

from dataclasses import dataclass

# 2: hash tags for cluster mode (`{m:<model>:<target>}`, `{r:<request_id>}`, `{cfg}`).
SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class KeySpace:
    """Key builder for one prefix, e.g. `llmq:prod` -> `llmq:prod:{r:<id>}:job`.

    Tags group the keys of one script: `{r:<request_id>}` (claim, cancel, params, result),
    `{m:<model>:<target>}` (limiter state, leases, breaker) and `{cfg}` (live config, audit).
    """

    prefix: str

    def _request(self, request_id: str, name: str) -> str:
        return f"{self.prefix}:{{r:{request_id}}}:{name}"

    def _target(self, model: str, target: str, name: str) -> str:
        return f"{self.prefix}:{{m:{model}:{target}}}:{name}"

    def job(self, request_id: str) -> str:
        """Claim state of one task (hash): state, worker, heartbeat, failures."""
        return self._request(request_id, "job")

    def params(self, request_id: str) -> str:
        """Large params stored by reference (string)."""
        return self._request(request_id, "params")

    def cancel(self, request_id: str) -> str:
        """Cancel flag for one task (string)."""
        return self._request(request_id, "cancel")

    def result(self, task_id: str) -> str:
        """JSON result of one task, deleted on read (string); same slot as its claim."""
        return self._request(task_id, "res")

    def limiter(self, model: str, target: str) -> str:
        """Adaptive limiter state for one model target (hash, no TTL)."""
        return self._target(model, target, "ac")

    def leases(self, model: str, target: str) -> str:
        """In-flight slot leases scored by expiry (sorted set)."""
        return self._target(model, target, "lease")

    def rate(self, model: str, target: str) -> str:
        """RPM / TPM budget state of one model target or `@<provider>` scope (hash, TTL)."""
        return self._target(model, target, "rate")

    def breaker(self, model: str, target: str) -> str:
        """Circuit breaker state for one model target (hash, no TTL)."""
        return self._target(model, target, "cb")

    def cancel_channel(self) -> str:
        """Pub/sub channel naming request ids just cancelled (workers stop running attempts)."""
        return f"{self.prefix}:cancel"

    def done_channel(self, pod: str) -> str:
        """Pub/sub channel that wakes one pod's result dispatcher."""
        return f"{self.prefix}:done:{pod}"

    @property
    def routing(self) -> str:
        """Live canary routing config (JSON string)."""
        return f"{self.prefix}:{{cfg}}:routing"

    @property
    def fallback(self) -> str:
        """Live fallback chains (JSON string)."""
        return f"{self.prefix}:{{cfg}}:fallback"

    @property
    def model_settings(self) -> str:
        """Live per-provider / per-model settings (JSON string, `ModelSettings`)."""
        return f"{self.prefix}:{{cfg}}:models"

    @property
    def audit(self) -> str:
        """Audit stream of config changes (who, before, after)."""
        return f"{self.prefix}:{{cfg}}:audit"

    @property
    def pods(self) -> str:
        """Live pods of this prefix (sorted set scored by expiry)."""
        return f"{self.prefix}:pods"

    @property
    def schema(self) -> str:
        """State schema version marker."""
        return f"{self.prefix}:schema"
