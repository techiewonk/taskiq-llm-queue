"""Canary router: picks `sqs` or `direct` per call from live, audited routing config (§15.1).

`RoutingConfig` lives as JSON at `KeySpace.routing` and is reloaded every 30 s, so an operator
can promote, roll back or kill the queue path with `llm-queue routing ...` and no restart.
`LLMQueue` calls `CanaryRouter.route` on every request; it never does I/O.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Literal

import xxhash
from pydantic import BaseModel, ConfigDict, Field

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.metrics_names import CONFIG_INVALID_TOTAL
from taskiq_llm_queue.reliability.config_store import LiveConfigStore

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.keys import KeySpace
    from taskiq_llm_queue.metrics import MetricsSink

logger = get_logger(__name__)

RoutePath = Literal["sqs", "direct"]
Pct = Annotated[int, Field(ge=0, le=100)]
DEFAULT_RELOAD_SECS = 30.0


class RoutingConfig(BaseModel):
    """Live canary config: kill switch, sqs percentage per model alias and lane, allowlist.

    A model or lane that is not listed routes `direct` (0 %), so the empty default sends
    everything direct. `allow_users` always go `sqs` unless the kill switch is on.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kill_switch: bool = False
    models: dict[str, dict[Lane, Pct]] = Field(default_factory=dict)
    allow_users: frozenset[str] = frozenset()

    def pct(self, model: str, lane: Lane) -> int:
        """Share of calls (0-100) for `model` (alias or `alias@target`) and lane sent to sqs."""
        return self.models.get(model.partition("@")[0], {}).get(lane, 0)

    def with_pct(self, model: str, lane: Lane, pct: int) -> RoutingConfig:
        """Copy with one model x lane percentage changed (promote / roll back)."""
        models = {alias: dict(lanes) for alias, lanes in self.models.items()}
        models.setdefault(model.partition("@")[0], {})[lane] = pct
        return RoutingConfig.model_validate({**self.model_dump(), "models": models})

    def with_kill(self, on: bool) -> RoutingConfig:
        """Copy with the kill switch set (`on` routes every call `direct`)."""
        return self.model_copy(update={"kill_switch": on})

    def with_user(self, user_id: str, *, allowed: bool) -> RoutingConfig:
        """Copy with `user_id` added to or removed from the allowlist."""
        users = self.allow_users | {user_id} if allowed else self.allow_users - {user_id}
        return self.model_copy(update={"allow_users": users})


def bucket(key: str) -> int:
    """Stable bucket 0-99 for a session or user id; equal on every pod and every restart."""
    return xxhash.xxh3_64_intdigest(key.encode("utf-8")) % 100


def routing_store(
    client: redis.asyncio.Redis, keys: KeySpace, default: RoutingConfig | None = None
) -> LiveConfigStore[RoutingConfig]:
    """Live store for routing at `KeySpace.routing`, audited to `KeySpace.audit`."""
    return LiveConfigStore(
        client, keys.routing, RoutingConfig, default or RoutingConfig(), audit_key=keys.audit
    )


class CanaryRouter:
    """`PathRouter` over a `LiveConfigStore[RoutingConfig]`, sticky per session (fallback user).

    `route` reads the last good config only. `start` runs the reload loop as a task; an invalid
    stored config keeps the last good one and emits `routing_config_invalid` (log + metric).
    """

    def __init__(
        self,
        store: LiveConfigStore[RoutingConfig],
        *,
        metrics: MetricsSink | None = None,
        reload_secs: float = DEFAULT_RELOAD_SECS,
        rand: Callable[[], float] = random.random,
    ) -> None:
        """Keep the store and metrics sink; `rand` buckets calls that carry no id at all."""
        self.store = store
        self._metrics = metrics
        self._reload_secs = reload_secs
        self._rand = rand
        self._seen_invalid = store.invalid_count
        self._task: asyncio.Task[None] | None = None

    @property
    def config(self) -> RoutingConfig:
        """The config `route` uses now (last good)."""
        return self.store.current

    def route(
        self, model: str, lane: Lane, session_id: str | None, user_id: str | None
    ) -> RoutePath:
        """Decide the path of one call: kill switch, then allowlist, then sticky percentage."""
        cfg = self.store.current
        if cfg.kill_switch:
            return "direct"
        if user_id is not None and user_id in cfg.allow_users:
            return "sqs"
        pct = cfg.pct(model, lane)
        if pct <= 0:
            return "direct"
        if pct >= 100:
            return "sqs"
        key = session_id or user_id
        # No id at all: not sticky, but still honours the percentage on average.
        point = bucket(key) if key else int(self._rand() * 100)
        return "sqs" if point < pct else "direct"

    async def reload(self) -> RoutingConfig:
        """Load the stored config once; report `routing_config_invalid` when it was rejected."""
        cfg = await self.store.load()
        invalid = self.store.invalid_count
        if invalid > self._seen_invalid:
            # The store warns once per bad value; this event is what the alarm (§13) keys on.
            logger.error("routing_config_invalid", key=self.store.key, invalid_count=invalid)
            if self._metrics is not None:
                self._metrics.incr(
                    CONFIG_INVALID_TOTAL, invalid - self._seen_invalid, {"kind": "routing"}
                )
            self._seen_invalid = invalid
        return cfg

    async def run(self) -> None:
        """Reload every `reload_secs` until cancelled; a failed reload never ends the loop."""
        while True:
            try:
                await self.reload()
            except Exception:  # the loop must survive anything; route() keeps last good
                logger.exception("routing_reload_failed")
            await asyncio.sleep(self._reload_secs)

    def start(self) -> asyncio.Task[None]:
        """Start the reload loop on the running event loop (idempotent)."""
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(
                self.run(), name="llmq-routing-reload"
            )
        return self._task

    async def aclose(self) -> None:
        """Stop the reload loop; safe to call twice."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
