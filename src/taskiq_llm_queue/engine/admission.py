"""Admission: pick a deployment and take everything a call needs in one store call.

`Admission.pick_take` asks the store script to choose a deployment of a model by its strategy
and take the RPM / TPM budgets, the account budget, a concurrency slot of the call's class and an
in-progress entry, all or nothing, honouring waiter order and the claim. `finish` corrects
tokens and learns; `undo` gives a grant back; `renew` is the heartbeat; `sweep` clears the dead.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import orjson
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.engine import _admission_lua as lua
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.tuning import EngineTimings

if TYPE_CHECKING:
    import redis.asyncio

Class = Literal["p", "n"]
Outcome = Literal["ok", "throttle", "fail", "none"]

logger = get_logger(__name__)


def owner_token(request_id: str, worker: str, attempt: int) -> str:
    """`request|worker|attempt`: identifies one grant (lease, entry) of one attempt."""
    return f"{request_id}|{worker}|{attempt}"


def position_of(message_id: str) -> float:
    """Queue position as a number (stream id `ms-seq` -> ms * 1000 + seq)."""
    ms, _, seq = message_id.partition("-")
    return float(int(ms) * 1000 + int(seq or 0))


@dataclass(frozen=True, slots=True)
class Candidate:
    """A deployment the call may use: its tier, and whether it is only a last choice."""

    key: str
    tier: Literal["confirmed", "fallback"] = "confirmed"
    last: bool = False


@dataclass(frozen=True, slots=True)
class Ask:
    """One admission request (one attempt of one call)."""

    alias: str
    request_id: str
    worker: str
    attempt: int
    cls: Class
    position: float
    candidates: Sequence[Candidate]
    strategy: str = "ordered"
    pick: str = "percent"
    cost_r: int = 1
    cost_t: int = 0
    claim: bool = True
    sticky_hash: str | None = None
    sticky_ttl_secs: float = 1800.0
    sticky_move_secs: float = 1.0
    foreign_from: str | None = None
    require_pct: int = 0
    tag: str = ""

    @property
    def owner(self) -> str:
        """The owner token of this attempt's grant (`tag` tells two deliveries apart)."""
        worker = f"{self.worker}@{self.tag}" if self.tag else self.worker
        return owner_token(self.request_id, worker, self.attempt)


@dataclass(frozen=True, slots=True)
class Decision:
    """What the script decided.

    `kind`: granted, wait, duplicate, done, cancelled, stale_config, no_config, none (no
    deployment usable), busy (another model refused a fallback call).
    """

    kind: str
    deployment: str | None = None
    minute: int | None = None
    wait_secs: float = 0.0
    reason: str = ""
    changes: int | None = None
    tier: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class Admission:
    """The admission scripts of one environment (key prefix)."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: Keys,
        *,
        lease_secs: float = 60.0,
        waiter_secs: float = 2.0,
        rng: random.Random | None = None,
        timings: EngineTimings | None = None,
    ) -> None:
        """Register the scripts; no I/O until the first call.

        `timings` (`Tuning.engine`) carries the scripts' pacing: probe and slot waits, the
        limiter's cut, budget key lifetime; each script falls back to the same defaults.
        """
        self._client = client
        self.keys = keys
        self.lease_ms = int(lease_secs * 1000)
        self.waiter_ms = int(waiter_secs * 1000)
        self._rng = rng or random.Random()
        pace = timings or EngineTimings()
        self._t = {
            "probe_wait_ms": pace.admission_probe_wait_ms,
            "slot_wait_ms": pace.admission_slot_wait_ms,
            "slot_jitter_ms": pace.admission_slot_jitter_ms,
            "wait_ms": pace.admission_wait_ms,
            "limiter_cut_ms": pace.limiter_cut_ms,
            "limiter_cut_factor": pace.limiter_cut_factor,
            "budget_key_ms": pace.budget_key_ms,
        }
        self._pick = client.register_script(lua.PICK_TAKE)
        self._finish = client.register_script(lua.FINISH)
        self._undo = client.register_script(lua.UNDO)
        self._renew = client.register_script(lua.RENEW)
        self._sweep = client.register_script(lua.SWEEP)
        self._reregister = client.register_script(lua.REREGISTER)

    async def _call(self, script: Any, body: dict[str, Any]) -> Any:
        return await script([], [self.keys.prefix, orjson.dumps({**body, "t": self._t})])

    async def pick_take(self, ask: Ask, changes: int) -> Decision:
        """Choose and take, or say how long to wait and why."""
        body: dict[str, Any] = {
            "alias": ask.alias,
            "changes": changes,
            "request_id": ask.request_id,
            "owner": ask.owner,
            "worker": ask.worker,
            "cls": ask.cls,
            "pos": ask.position,
            "cost_r": ask.cost_r,
            "cost_t": ask.cost_t,
            "candidates": [{"key": c.key, "tier": c.tier, "last": c.last} for c in ask.candidates],
            "strategy": ask.strategy,
            "pick": ask.pick,
            "claim": ask.claim,
            "lease_ms": self.lease_ms,
            "waiter_ms": self.waiter_ms,
            "rand": self._rng.random(),
        }
        if ask.sticky_hash:
            body["sticky"] = {
                "hash": ask.sticky_hash,
                "ttl_ms": int(ask.sticky_ttl_secs * 1000),
                "move_ms": int(ask.sticky_move_secs * 1000),
            }
        if ask.foreign_from is not None:
            body["foreign"] = {"from_alias": ask.foreign_from, "require_pct": ask.require_pct}
        raw = orjson.loads(await self._call(self._pick, body))
        kind = str(raw.pop("r"))
        return Decision(
            kind=kind,
            deployment=raw.pop("dep", None),
            minute=raw.pop("minute", None),
            wait_secs=float(raw.pop("ms", 0)) / 1000.0,
            reason=str(raw.pop("reason", "")),
            changes=raw.pop("changes", None),
            tier=raw.pop("tier", None),
            extra=raw,
        )

    async def finish(
        self,
        alias: str,
        owner: str,
        request_id: str,
        *,
        refund_r: int,
        refund_t: int,
        outcome: Outcome,
        latency_ms: float = -1,
    ) -> bool:
        """Settle a grant: give back what was not used, release the slot, learn the outcome.

        `refund_t` may be negative (the call used more than it reserved). If the store is
        unreachable this is skipped (False): the grant's lease expires and SWEEP frees the slot,
        so a store blip after the provider answered never throws the answer away.
        """
        body = {
            "alias": alias,
            "owner": owner,
            "request_id": request_id,
            "refund_r": refund_r,
            "refund_t": refund_t,
            "outcome": outcome,
            "latency_ms": latency_ms,
        }
        try:
            return bool(await self._call(self._finish, body))
        except (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OSError) as exc:
            logger.warning("finish_skipped", model=alias, request_id=request_id, error=str(exc))
            return False

    async def undo(self, alias: str, owner: str, request_id: str) -> bool:
        """Give a grant back entirely, without learning (lost reply, refused claim)."""
        body = {"alias": alias, "owner": owner, "request_id": request_id}
        return bool(await self._call(self._undo, body))

    async def renew(self, items: Sequence[tuple[str, str, str]]) -> int:
        """Extend leases of held grants: (alias, owner, request id) each."""
        if not items:
            return 0
        body = {
            "lease_ms": self.lease_ms,
            "items": [{"alias": a, "owner": o, "request_id": r} for a, o, r in items],
        }
        return int(await self._call(self._renew, body))

    async def reregister(self, items: Sequence[dict[str, Any]]) -> int:
        """After a store reset, put back the slots and entries of running calls."""
        if not items:
            return 0
        body = {"lease_ms": self.lease_ms, "items": list(items)}
        return int(await self._call(self._reregister, body))

    async def sweep(self, alias: str) -> int:
        """Remove in-progress entries of owners that stopped renewing."""
        return int(await self._call(self._sweep, {"alias": alias}))
