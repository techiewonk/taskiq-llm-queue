"""PICK_TAKE, FINISH, UNDO, RENEW, SWEEP on Valkey and Redis (plan step A6).

Budgets and class shares, slots per class, the waiter reservation rule, ownership and the
claim, corrections and refunds, strategies, tiers and last choice, usability, account budget,
sticky sessions, foreign calls (model fallback) and the sweeper. Scenarios B01-B04, B58-B67,
B93, B94, B100, B102 at the script level.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import pytest
import redis.asyncio

from taskiq_llm_queue.engine.admission import Admission, Ask, Candidate, owner_token
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration

BASE: dict[str, Any] = {
    "registry_version": 1,
    "accounts": {"aws-a": {"kind": "aws"}},
    "models": {
        "m": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "x.model-1:0",
            "rate_window": "fixed",
            "priority_reserve": 0,
            "normal_min_share": 0,
            "deployments": {"aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"]}},
        },
        "other": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "x.other-1:0",
            "rate_window": "fixed",
            "rpm": 100,
            "deployments": {"aws-a": {"confirmed": ["r1"]}},
        },
    },
}
CONFIRMED = [Candidate("aws-a/r1"), Candidate("aws-a/r2")]
ALL = [*CONFIRMED, Candidate("aws-a/r3", tier="fallback")]


class Rig:
    """A published registry and an `Admission` over one prefix, with ask helpers."""

    def __init__(self, admission: Admission, changes: int, store: RegistryStore) -> None:
        self.admission = admission
        self.changes = changes
        self.store = store
        self.seq = 0

    async def ask(
        self,
        rid: str | None = None,
        *,
        cls: str = "n",
        pos: float | None = None,
        candidates: list[Candidate] | None = None,
        worker: str = "w1",
        attempt: int = 1,
        alias: str = "m",
        **kw: Any,
    ) -> Any:
        self.seq += 1
        ask = Ask(
            alias=alias,
            request_id=rid or f"req-{self.seq}",
            worker=worker,
            attempt=attempt,
            cls=cls,  # type: ignore[arg-type]
            position=pos if pos is not None else float(self.seq),
            candidates=candidates or CONFIRMED,
            **kw,
        )
        return await self.admission.pick_take(ask, self.changes)


async def rig(
    client: redis.asyncio.Redis,
    prefix: str,
    *,
    model: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
    account: dict[str, Any] | None = None,
    lease_secs: float = 60,
    waiter_secs: float = 2,
) -> Rig:
    doc = copy.deepcopy(BASE)
    doc["models"]["m"].update(model or {})
    if overrides:
        doc["models"]["m"]["deployments"]["aws-a"]["overrides"] = overrides
    if account:
        doc["accounts"]["aws-a"]["settings"] = account
    store = RegistryStore(client, prefix, config=doc)
    await store.boot()
    admission = Admission(client, Keys(prefix), lease_secs=lease_secs, waiter_secs=waiter_secs)
    return Rig(admission, store.changes, store)


async def test_rpm_fixed_window_exact(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 5})
    kinds = [(await r.ask(candidates=[Candidate("aws-a/r1")])).kind for _ in range(6)]
    assert kinds == ["granted"] * 5 + ["wait"]
    refused = await r.ask(candidates=[Candidate("aws-a/r1")])
    assert refused.reason in ("rpm", "not_head")
    assert 0 < refused.wait_secs <= 60


async def test_smooth_window_burst(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 60, "rate_window": "smooth"})
    one = [Candidate("aws-a/r1")]
    kinds = [(await r.ask(candidates=one)).kind for _ in range(7)]
    assert kinds.count("granted") == 6  # burst = 10 % of 60
    refused = await r.ask(candidates=one)
    assert refused.kind == "wait"
    assert 0.5 < refused.wait_secs <= 2.5


async def test_tpm_and_correction(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"tpm": 100})
    one = [Candidate("aws-a/r1")]
    first = await r.ask("a", candidates=one, cost_t=80)
    assert first.kind == "granted"
    refused = await r.ask("b", candidates=one, cost_t=80)
    assert (refused.kind, refused.reason) == ("wait", "tpm")
    # The first call used 10 tokens: 70 go back.
    assert await r.admission.finish(
        "m", owner_token("a", "w1", 1), "a", refund_r=0, refund_t=70, outcome="ok"
    )
    assert (await r.ask("b", candidates=one, cost_t=80)).kind == "granted"


async def test_class_shares(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 10, "priority_reserve": 0.5})
    one = [Candidate("aws-a/r1")]
    normal = [await r.ask(candidates=one) for _ in range(6)]
    assert [d.kind for d in normal] == ["granted"] * 5 + ["wait"]  # normal may use 50 %
    assert normal[-1].reason == "reserve"
    assert (await r.ask(candidates=one, cls="p")).kind == "granted"


async def test_slots_per_class(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"max_concurrency": 4, "priority_reserve": 0.5})
    one = [Candidate("aws-a/r1")]
    normal = [await r.ask(candidates=one) for _ in range(3)]
    assert [d.kind for d in normal] == ["granted", "granted", "wait"]  # floor(0.5 x 4) = 2
    refused = normal[-1]
    assert refused.reason == "slot"
    assert 0.04 <= refused.wait_secs <= 0.21  # never a busy loop
    assert (await r.ask(candidates=one, cls="p")).kind == "granted"
    small = await rig(store, prefix + "s", model={"max_concurrency": 2, "priority_reserve": 0.2})
    assert (await small.ask(candidates=one)).kind == "granted"
    assert (await small.ask(candidates=one, cls="p")).kind == "granted"  # one slot each


async def test_reservation_for_older_waiters(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"tpm": 100})
    one = [Candidate("aws-a/r1")]
    assert (await r.ask("x", pos=1, candidates=one, cost_t=60)).kind == "granted"
    waiter = await r.ask("w", pos=2, candidates=one, cost_t=50)
    assert waiter.kind == "wait"
    younger = await r.ask("y", pos=3, candidates=one, cost_t=30)  # would fit alone
    assert (younger.kind, younger.reason) == ("wait", "not_head")
    await r.admission.finish(
        "m", owner_token("x", "w1", 1), "x", refund_r=0, refund_t=50, outcome="ok"
    )
    assert (await r.ask("w", pos=2, candidates=one, cost_t=50)).kind == "granted"
    assert (await r.ask("y", pos=3, candidates=one, cost_t=30)).kind == "granted"


async def test_waiter_only_holds_its_own_deployments(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    r = await rig(store, prefix, model={"rpm": 1})
    assert (await r.ask("a", pos=1, candidates=[Candidate("aws-a/r1")])).kind == "granted"
    held = await r.ask("w", pos=2, candidates=[Candidate("aws-a/r1")])  # restricted to r1
    assert held.kind == "wait"
    # A younger call that may use r2 is not held back by the r1-only waiter.
    granted = await r.ask("y", pos=3)
    assert (granted.kind, granted.deployment) == ("granted", "aws-a/r2")


async def test_dead_waiter_expires(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"tpm": 100}, waiter_secs=0.3)
    one = [Candidate("aws-a/r1")]
    await r.ask("x", pos=1, candidates=one, cost_t=60)
    assert (await r.ask("w", pos=2, candidates=one, cost_t=50)).kind == "wait"
    assert (await r.ask("y", pos=3, candidates=one, cost_t=30)).reason == "not_head"
    await asyncio.sleep(0.4)  # the waiter's holder died: its entry lapsed
    assert (await r.ask("y", pos=3, candidates=one, cost_t=30)).kind == "granted"


async def test_ownership_and_claim(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, lease_secs=0.4)
    first = await r.ask("req", worker="A")
    assert first.kind == "granted"
    assert (await r.ask("req", worker="A")).extra.get("repeat_") is True  # lost reply: same
    assert (await r.ask("req", worker="B")).kind == "duplicate"
    await asyncio.sleep(0.5)  # A stopped heart-beating
    taken = await r.ask("req", worker="B")
    assert taken.kind == "granted"
    leases = await store.zrange(f"{prefix}:{{m:m}}:lease:{taken.deployment}:n", 0, -1)
    assert leases == [owner_token("req", "B", 1).encode()]
    await store.set(f"{prefix}:{{r:c}}:cancel", 1)
    assert (await r.ask("c")).kind == "cancelled"
    await store.set(f"{prefix}:{{r:d}}:res", "{}")
    assert (await r.ask("d")).kind == "done"


async def test_undo_and_stale_config(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 1})
    one = [Candidate("aws-a/r1")]
    assert (await r.ask("a", candidates=one)).kind == "granted"
    assert (await r.ask("b", candidates=one)).kind == "wait"
    assert await r.admission.undo("m", owner_token("a", "w1", 1), "a")
    assert (await r.ask("b", candidates=one)).kind == "granted"
    r.changes -= 1
    assert (await r.ask("c")).kind == "stale_config"


async def test_strategies(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix)
    assert {(await r.ask()).deployment for _ in range(3)} == {"aws-a/r1"}
    rr = [(await r.ask(strategy="round_robin")).deployment for _ in range(4)]
    assert rr == ["aws-a/r1", "aws-a/r2", "aws-a/r1", "aws-a/r2"]
    w = await rig(store, prefix + "w", overrides={"r1": {"weight": 3}, "r2": {"weight": 1}})
    picks = [(await w.ask(strategy="weighted")).deployment for _ in range(8)]
    assert picks.count("aws-a/r1") == 6
    h = await rig(store, prefix + "h", model={"rpm": 10}, overrides={"r2": {"rpm": 4}})
    for _ in range(3):
        await h.ask(candidates=[Candidate("aws-a/r1")])
    # r1 has 7 / 10 left, r2 4 / 4: headroom by share picks r2.
    assert (await h.ask(strategy="headroom")).deployment == "aws-a/r2"


async def test_tiers_and_last_choice(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 1})
    await r.ask(candidates=[Candidate("aws-a/r1")])
    await r.ask(candidates=[Candidate("aws-a/r2")])
    assert (await r.ask(candidates=CONFIRMED)).kind == "wait"
    assert (await r.ask(candidates=ALL)).deployment == "aws-a/r3"  # fallback tier
    s = await rig(store, prefix + "s")
    last = [Candidate("aws-a/r1", last=True), Candidate("aws-a/r2")]
    assert (await s.ask(candidates=last)).deployment == "aws-a/r2"


async def test_unusable_deployments(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, overrides={"r1": {"enabled": False}})
    assert (await r.ask()).deployment == "aws-a/r2"
    gone = await rig(store, prefix + "g", model={"breaker_threshold": 2})
    for i in range(2):
        rid = f"f{i}"
        assert (await gone.ask(rid, candidates=[Candidate("aws-a/r1")])).kind == "granted"
        await gone.admission.finish(
            "m", owner_token(rid, "w1", 1), rid, refund_r=0, refund_t=0, outcome="fail"
        )
    assert (await gone.ask(candidates=[Candidate("aws-a/r1")])).kind == "none"  # breaker open
    await gone.store.block(region="r2", on=True, actor="t")
    gone.changes = gone.store.changes
    assert (await gone.ask()).kind == "none"


async def test_account_budget(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, account={"shared_rpm": 2})
    kinds = [(await r.ask(strategy="round_robin")) for _ in range(3)]
    assert [k.kind for k in kinds] == ["granted", "granted", "wait"]
    assert kinds[2].reason == "account"


async def test_sticky_session(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix)
    first = await r.ask(strategy="round_robin", sticky_hash="s1")
    again = [(await r.ask(strategy="round_robin", sticky_hash="s1")).deployment for _ in range(3)]
    assert again == [first.deployment] * 3


async def test_foreign_call(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, model={"rpm": 1})
    await r.ask("a", candidates=[Candidate("aws-a/r1")])
    await r.ask("b", candidates=[Candidate("aws-a/r2")])
    assert (await r.ask("w", pos=5)).kind == "wait"  # waiter on m
    other = [Candidate("aws-a/r1")]
    taken = await r.ask("w", pos=5, alias="other", candidates=other, foreign_from="m")
    assert taken.kind == "granted"
    assert not await store.zrange(f"{prefix}:{{m:m}}:wait:n", 0, -1)  # left m's waiters
    assert (await r.ask("x", pos=6)).kind == "wait"
    busy = await r.ask("z", pos=7, alias="m", candidates=CONFIRMED, foreign_from="other")
    assert (busy.kind, busy.reason) == ("busy", "waiters")


async def test_renew_and_sweep(store: redis.asyncio.Redis, prefix: str) -> None:
    r = await rig(store, prefix, lease_secs=0.3)
    await r.ask("kept")
    await r.ask("dead")
    await asyncio.sleep(0.15)
    assert await r.admission.renew([("m", owner_token("kept", "w1", 1), "kept")]) == 1
    await asyncio.sleep(0.2)
    assert await r.admission.sweep("m") == 1
    left = await store.zrange(f"{prefix}:{{m:m}}:inprog", 0, -1)
    assert left == [owner_token("kept", "w1", 1).encode()]
