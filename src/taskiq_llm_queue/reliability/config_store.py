"""Live JSON config in Valkey (routing, fallback chains) with last-good fallback and audit.

Readers validate with pydantic and keep the last good value when the stored JSON is invalid or
Valkey is unreachable, so a bad edit never breaks live calls. Every write appends who changed
what to `KeySpace.audit` in the same Lua call, trimmed to the retention window.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError
from redis.exceptions import RedisError

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import _lua
from taskiq_llm_queue.reliability.fallback import FallbackConfig

if TYPE_CHECKING:
    import redis.asyncio

log = logging.getLogger(__name__)
ModelT = TypeVar("ModelT", bound=BaseModel)
AUDIT_RETENTION_SECS = 90 * 86400


class LiveConfigStore(Generic[ModelT]):  # PEP 695 needs 3.12; package supports 3.11
    """One live config value of type `model_type` at `key`, with audit trail and reload loop.

    `current` is the last good value (the startup default until a load succeeds); hot paths read
    it without I/O, and `run_reload` refreshes it every `interval_secs` (`LLMQ_CFG_RELOAD_S`).
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        key: str,
        model_type: type[ModelT],
        default: ModelT,
        *,
        audit_key: str,
        audit_retention_secs: int = AUDIT_RETENTION_SECS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Keep the client and the startup default; nothing is read until `load`."""
        self._client = client
        self.key = key
        self._type = model_type
        self.default = default
        self._audit_key = audit_key
        self._retention_ms = audit_retention_secs * 1000
        self._sleep = sleep
        self._current = default
        self._warned: set[bytes] = set()
        self.invalid_count = 0
        self._set = client.register_script(_lua.CONFIG_SET)

    @property
    def current(self) -> ModelT:
        """Last good value; never raises and never does I/O."""
        return self._current

    def parse(self, raw: str | bytes | Mapping[str, Any] | ModelT) -> ModelT:
        """Validate JSON, a mapping or a model instance; raises `ConfigError` when invalid."""
        try:
            if isinstance(raw, self._type):
                return raw
            if isinstance(raw, str | bytes):
                return self._type.model_validate_json(raw)
            return self._type.model_validate(raw)
        except ValidationError as exc:
            raise ConfigError(f"invalid {self.key}: {exc}") from exc

    async def load(self) -> ModelT:
        """Read and validate the stored value; on bad data or no Valkey keep the last good one.

        A missing key means the startup default. Invalid data is logged once per distinct value
        and counted in `invalid_count` (exported as `llmq_config_invalid_total`).
        """
        try:
            raw = await self._client.get(self.key)
        except RedisError as exc:
            log.warning("live config %s: Valkey unreachable, keeping last good (%s)", self.key, exc)
            return self._current
        if raw is None:
            self._current = self.default
            return self._current
        raw_bytes = raw if isinstance(raw, bytes) else str(raw).encode()
        try:
            self._current = self.parse(raw_bytes)
        except ConfigError as exc:
            self.invalid_count += 1
            if raw_bytes not in self._warned:
                self._warned.add(raw_bytes)
                log.warning("live config %s is invalid, keeping last good: %s", self.key, exc)
        return self._current

    async def set(self, value: str | bytes | Mapping[str, Any] | ModelT, *, actor: str) -> ModelT:
        """Validate and store a new value, appending an audit entry; returns the stored value.

        Raises `ConfigError` for invalid input, so a bad CLI edit never reaches Valkey.
        """
        parsed = self.parse(value)
        await self._set(
            [self.key, self._audit_key], [parsed.model_dump_json(), actor, self._retention_ms]
        )
        self._current = parsed
        return parsed

    async def reset(self, *, actor: str) -> ModelT:
        """Delete the stored value (back to the startup default), with an audit entry."""
        await self._set([self.key, self._audit_key], ["", actor, self._retention_ms])
        self._current = self.default
        return self._current

    async def history(self, count: int = 20) -> list[dict[str, str]]:
        """Latest audit entries for this key, newest first (for `llm-queue ... get`)."""
        entries = await self._client.xrevrange(self._audit_key, count=count * 4)
        out: list[dict[str, str]] = []
        for _id, fields in entries or ():
            row = {_lua.as_text(k): _lua.as_text(v) for k, v in (fields or {}).items()}
            if row.get("key") == self.key:
                out.append(row)
        return out[:count]

    async def run_reload(self, interval_secs: float) -> None:
        """Reload every `interval_secs` until cancelled; errors never end the loop."""
        while True:
            await self.load()
            await self._sleep(interval_secs)


def fallback_store(
    client: redis.asyncio.Redis, keys: KeySpace, default: FallbackConfig | None = None
) -> LiveConfigStore[FallbackConfig]:
    """Live store for fallback chains at `KeySpace.fallback`, audited to `KeySpace.audit`."""
    return LiveConfigStore(
        client, keys.fallback, FallbackConfig, default or FallbackConfig(), audit_key=keys.audit
    )
