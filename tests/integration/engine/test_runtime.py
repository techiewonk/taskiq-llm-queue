"""The engine end to end: caller, queue, admission, provider (fakellm), result (plan A7, A8).

Each test runs a real `Engine` on Valkey and on Redis against an in-process fakellm: chat and
embeddings, priority, budgets and expiry, region and model fallback, a hung region, duplicate
request ids, the waiting limit and an unusable model. No real LLM is called.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, GPT, SMALL, Make, stats

from taskiq_llm_queue.clients import CredentialsUnavailable
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.engine.queue import QueueSettings

pytestmark = pytest.mark.integration

_stats = stats


async def test_chat_round_trip(make: Make) -> None:
    engine, fake = await make()
    reply = await engine.call("big", CHAT, priority=True)
    assert reply.ok
    assert reply.response is not None
    assert reply.response["output"]["message"]["content"][0]["text"].startswith("fakellm")
    assert reply.meta.deployment == "aws-a/r1"
    assert reply.meta.tokens_out is not None
    status = await engine.requests.status(reply.request_id)
    assert status["state"] == "done"
    assert status["deployment"] == "aws-a/r1"
    assert _stats(fake, f"{GPT}@r1")["ok"] == 1


async def test_embeddings_with_inference_defaults(make: Make) -> None:
    engine, _ = await make()
    reply = await engine.call("titan", {"body": {"inputText": "a b c"}})
    assert reply.response is not None
    assert reply.meta.tokens_in is not None


async def test_rpm_budget_and_expiry(make: Make) -> None:
    engine, fake = await make({"big": {"rpm": 3, "max_wait_secs": 1, "priority_reserve": 0}})
    minute = engine.clock.now() // 60
    results = await asyncio.gather(
        *[engine.call("big", CHAT, timeout_secs=5) for _ in range(8)], return_exceptions=True
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    failed = [r for r in results if isinstance(r, CallFailed)]
    if engine.clock.now() // 60 == minute:  # a fixed window resets at the minute boundary
        assert len(ok) == 6  # 3 per minute on each of r1 and r2
    else:
        assert 6 <= len(ok) <= 8
    assert {f.kind for f in failed} <= {"rate_limited"}
    calls = _stats(fake, f"{GPT}@r1").get("calls", 0) + _stats(fake, f"{GPT}@r2").get("calls", 0)
    assert calls == len(ok)  # nothing above the budget reached the provider


async def test_throttles_move_to_fallback_region(make: Make) -> None:
    engine, fake = await make(
        {"big": {"fallback": {"on": ["exhausted", "broken", "error"], "after_secs": 0}}},
        {
            "models": {
                f"{GPT}@r1": {"force_error": "ThrottlingException"},
                f"{GPT}@r2": {"force_error": "ThrottlingException"},
            }
        },
    )
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r3"
    assert reply.meta.fallback == "region"
    assert _stats(fake, f"{GPT}@r3")["ok"] == 1


async def test_hung_region_is_left(make: Make) -> None:
    engine, _ = await make(
        {"big": {"attempt_timeout_secs": 0.5}},
        {"models": {f"{GPT}@r1": {"hang": True}}},
    )
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r2"
    assert reply.meta.attempts == 2


async def test_model_fallback(make: Make) -> None:
    engine, _ = await make(
        {
            "big": {
                "rpm": 1,
                "rate_window": "smooth",  # no minute boundary to cross mid-test
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
                "model_fallback": {"on": ["exhausted"], "after_secs": 0, "models": ["small"]},
            }
        }
    )
    first = await engine.call("big", CHAT)
    second = await engine.call("big", CHAT)
    assert first.meta.served_model == "big"
    assert second.meta.served_model == "small"
    assert second.meta.fallback == "model"


async def test_duplicate_request_ids(make: Make) -> None:
    engine, fake = await make(scenario={"defaults": {"latency_ms": {"p50": 200, "p95": 200}}})
    one, two = await asyncio.gather(
        engine.call("big", CHAT, request_id="same"), engine.call("big", CHAT, request_id="same")
    )
    assert one.response == two.response
    assert _stats(fake, f"{GPT}@r1")["calls"] == 1
    with pytest.raises(CallFailed) as info:
        await engine.call(
            "big", {"messages": [{"role": "user", "content": [{"text": "x"}]}]}, request_id="same"
        )
    assert info.value.kind == "request_id_conflict"


async def test_queue_full(make: Make) -> None:
    engine, _ = await make({"big": {"max_waiting": 1}}, worker=False)
    first = asyncio.create_task(engine.call("big", CHAT, timeout_secs=2))
    await asyncio.sleep(0.2)
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, timeout_secs=2)
    assert info.value.kind == "queue_full"
    first.cancel()


async def test_model_unavailable(make: Make) -> None:
    engine, _ = await make(
        {
            "small": {
                "deployments": {
                    "aws-a": {"confirmed": ["r1"], "overrides": {"r1": {"enabled": False}}}
                }
            }
        }
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "model_unavailable"


async def test_call_filter_leaves_nothing(make: Make) -> None:
    engine, _ = await make()
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, allowed_regions=["nowhere"])
    assert info.value.kind == "no_deployment"


# ---------------------------------------------------------------- A9: hooks and fallback causes


async def test_custom_balancer_orders_candidates(make: Make) -> None:
    from taskiq_llm_queue.engine import hooks

    hooks.register_balancer("prefer_r2", lambda cands, ctx: ["aws-a/r2"])
    engine, _ = await make({"big": {"balance": "custom:prefer_r2"}})
    assert (await engine.call("big", CHAT)).meta.deployment == "aws-a/r2"


async def test_admission_hook_rejects(make: Make) -> None:
    from taskiq_llm_queue.engine import hooks

    hooks.register_admission(
        "no_bob", lambda ctx: hooks.Reject("blocked user") if ctx.user_id == "bob" else None
    )
    engine, _ = await make({"big": {"admission": "no_bob"}})
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, user_id="bob")
    assert info.value.kind == "rejected"
    assert (await engine.call("big", CHAT, user_id="ann")).ok


async def test_fallback_only_for_listed_causes(make: Make) -> None:
    engine, _ = await make(
        {
            "big": {
                "rpm": 1,
                "priority_reserve": 0,
                "max_wait_secs": 1,
                "fallback": {"on": ["error"], "after_secs": 0},
            }
        }
    )
    await engine.call("big", CHAT)
    await engine.call("big", CHAT)
    with pytest.raises(CallFailed) as info:  # exhausted is not a listed cause: no spill to r3
        await engine.call("big", CHAT, timeout_secs=3)
    assert info.value.kind == "rate_limited"


async def test_broken_confirmed_uses_fallback_tier(make: Make) -> None:
    disabled = {"r1": {"enabled": False}, "r2": {"enabled": False}}
    engine, _ = await make(
        {
            "big": {
                "fallback": {"on": ["broken"], "after_secs": 0},
                "deployments": {
                    "aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"], "overrides": disabled}
                },
            }
        }
    )
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r3"
    assert reply.meta.fallback == "region"


async def test_fallback_policy_hook(make: Make) -> None:
    from taskiq_llm_queue.engine import hooks

    hooks.register_fallback_policy("never", lambda cause, waited, rule, ctx: False)
    disabled = {"r1": {"enabled": False}, "r2": {"enabled": False}}
    engine, _ = await make(
        {
            "big": {
                "fallback": {"on": ["broken"], "after_secs": 0},
                "fallback_policy": "never",
                "deployments": {
                    "aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"], "overrides": disabled}
                },
            }
        }
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT)
    assert info.value.kind == "model_unavailable"


# ---------------------------------------------------------------- A10: operations


async def test_kill_switch_runs_direct_with_budgets(make: Make) -> None:
    engine, fake = await make(
        {
            "big": {
                "rpm": 2,
                "priority_reserve": 0,
                "max_wait_secs": 1,
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            }
        }
    )
    await engine.registry.edit(
        lambda d: (
            d["routing"].update(kill_switch=True)
            if "routing" in d
            else d.update(routing={"kill_switch": True})
        ),
        actor="t",
    )
    minute = engine.clock.now() // 60
    results = await asyncio.gather(
        *[engine.call("big", CHAT, timeout_secs=3) for _ in range(4)], return_exceptions=True
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    if engine.clock.now() // 60 == minute:
        assert len(ok) == 2
    depth = await engine.queues.depth(engine._queue_name(engine.registry.current, "big", False))
    assert depth == (0, 0, 0)  # nothing went through the queue
    assert _stats(fake, f"{GPT}@r1")["calls"] == len(ok)


async def test_cancel_waiting_and_running(make: Make) -> None:
    engine, _ = await make(
        {"big": {"rpm": 1, "priority_reserve": 0, "deployments": {"aws-a": {"confirmed": ["r1"]}}}},
        {"defaults": {"latency_ms": {"p50": 3000, "p95": 3000}}},
    )
    running = asyncio.create_task(engine.call("big", CHAT, request_id="run", timeout_secs=9))
    await asyncio.sleep(0.5)
    waiting = asyncio.create_task(engine.call("big", CHAT, request_id="wait", timeout_secs=9))
    await asyncio.sleep(0.5)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await engine.cancel("run")
    await engine.cancel("wait")
    for task in (running, waiting):
        with pytest.raises(CallFailed) as info:
            await task
        assert info.value.kind == "cancelled"
    assert loop.time() - started < 2.5  # the running attempt was stopped, not waited out


async def test_cancelling_the_awaiting_task_cancels_the_call(make: Make) -> None:
    engine, _ = await make(scenario={"defaults": {"latency_ms": {"p50": 3000, "p95": 3000}}})
    task = asyncio.create_task(engine.call("big", CHAT, request_id="gone"))
    await asyncio.sleep(0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await engine.state.exists(engine.keys.request("gone", "cancel"))


async def test_caller_gone_is_dropped(make: Make, store: redis.asyncio.Redis) -> None:
    from taskiq_llm_queue.engine.envelope import Envelope

    engine, fake = await make()
    secs, _ = await store.time()
    await store.set(engine.keys.epoch, str(secs - 100))  # the store has been up for a while
    env = Envelope(
        request_id="ghost-call",
        op="converse",
        model="big",
        params=CHAT,
        caller="ghost",
        timeout_secs=5,
    )
    await engine.requests.register(
        "ghost-call",
        caller="ghost",
        user_hash="u",
        params_hash="p",
        model="big",
        priority=False,
        app_id=False,
    )
    await engine.requests.confirm("ghost-call", keep_secs=5)
    await engine.queues.send(
        engine._queue_name(engine.registry.current, "big", False), env.dumps(), QueueSettings()
    )
    for _ in range(50):
        status = await engine.requests.status("ghost-call")
        if status.get("state") == "cancelled":
            break
        await asyncio.sleep(0.1)
    assert status["state"] == "cancelled"
    assert _stats(fake, f"{GPT}@r1") == {}  # never reached the provider


async def test_store_down_modes(make: Make) -> None:
    from taskiq_llm_queue.engine.envelope import Envelope

    engine, fake = await make(
        {"small": {"store_down": "static_cap"}, "big": {"store_down": "fail_fast"}}
    )
    view = engine.registry.current
    env = Envelope(
        request_id="x", op="converse", model="small", params=CHAT, caller="c", timeout_secs=5
    )
    reply = await engine._store_down(view, "small", env, ConnectionError("down"))
    assert reply.ok
    assert _stats(fake, f"{SMALL}@r1")["ok"] == 1
    fast = Envelope(
        request_id="y", op="converse", model="big", params=CHAT, caller="c", timeout_secs=5
    )
    with pytest.raises(CallFailed) as info:
        await engine._store_down(view, "big", fast, ConnectionError("down"))
    assert info.value.kind == "queue_unavailable"


async def test_store_reset_reregisters_running_calls(
    make: Make, store: redis.asyncio.Redis
) -> None:
    engine, _ = await make(scenario={"defaults": {"latency_ms": {"p50": 2000, "p95": 2000}}})
    task = asyncio.create_task(engine.call("big", CHAT, request_id="long"))
    await asyncio.sleep(0.6)
    keys = [k async for k in store.scan_iter(match=f"{engine.keys.prefix}:*", count=500)]
    await store.delete(*keys)  # the store lost everything (flush / failover)
    await engine._check_epoch()
    assert await store.exists(engine.keys.model("big", "inprog:long"))
    assert await store.exists(engine.keys.epoch)
    with contextlib.suppress(CallFailed):  # the call's own records were lost with the store
        await task


# ---------------------------------------------------------------- A11: health


async def test_target_errors_disable_a_deployment(make: Make) -> None:
    engine, fake = await make(
        {"big": {"health": {"disable_errors": 3, "probe_after_secs": 600}}},
        {"models": {f"{GPT}@r1": {"force_error": "AccessDeniedException"}}},
    )
    for _ in range(3):
        reply = await engine.call("big", CHAT)
        assert reply.meta.deployment == "aws-a/r2"
        assert reply.meta.attempts == 2
    assert "aws-a/r1" in await engine.health.disabled("big")
    after = await engine.call("big", CHAT)
    assert after.meta.attempts == 1  # r1 is skipped now
    assert _stats(fake, f"{GPT}@r1")["calls"] == 3


async def test_request_caused_errors_are_forgiven(make: Make) -> None:
    denied = {"force_error": "AccessDeniedException"}
    engine, _ = await make(
        {"big": {"health": {"disable_errors": 1}}},
        {"models": {f"{GPT}@r1": denied, f"{GPT}@r2": denied, f"{GPT}@r3": denied}},
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT)
    assert info.value.kind == "provider_error"
    assert await engine.health.disabled("big") == {}  # nothing disabled by a bad request


async def test_last_usable_deployment_is_never_disabled(make: Make) -> None:
    engine, _ = await make(
        {"small": {"health": {"disable_errors": 1}, "max_attempts": 1}},
        {"models": {f"{SMALL}@r1": {"force_error": "AccessDeniedException"}}},
    )
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT)
    assert await engine.health.disabled("small") == {}


async def test_probe_recovers_or_backs_off(make: Make) -> None:
    engine, fake = await make()
    await engine.health.disable("big", "aws-a/r1", probe_after_secs=0)
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, allowed_regions=["r1"])
    assert info.value.kind == "model_unavailable"
    assert await engine.probe_due("big") == {"aws-a/r1": "recovered"}
    assert (await engine.call("big", CHAT, allowed_regions=["r1"])).ok
    fake.scenario.patch_model(f"{GPT}@r2", {"force_error": "ResourceNotFoundException"})
    await engine.health.disable("big", "aws-a/r2", probe_after_secs=0)
    assert await engine.probe_due("big") == {"aws-a/r2": "waiting"}
    health = await engine.health.snapshot("big")
    assert int(health["aws-a/r2|wait"]) == 60000  # doubles, never below 60 s


async def test_credential_failure_is_per_process(make: Make) -> None:
    engine, _ = await make(
        {"small": {"max_attempts": 3}},
        {"models": {f"{SMALL}@r1": {"force_error": "UnrecognizedClientException"}}},
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "credentials_unavailable"
    assert "aws-a" in engine._bad_accounts


async def test_assume_role_failure_is_a_credential_failure(make: Make) -> None:
    """A15: STS refusing AssumeRole counts like a bad key: per process, never shared health."""
    engine, _ = await make({"small": {"max_attempts": 3}})

    async def refuse(route: Any) -> Any:
        raise CredentialsUnavailable(f"account {route.account.name}: AssumeRole failed: denied")

    engine.providers.pool.bedrock = refuse
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "credentials_unavailable"
    assert "aws-a" in engine._bad_accounts
    assert not await engine.health.snapshot("small")


# ---------------------------------------------------------------- A16: slow deployments


async def test_slow_deployment_becomes_last_choice(make: Make) -> None:
    three = {"aws-a": {"confirmed": ["r1", "r2", "r3"]}}
    engine, fake = await make(
        {
            "big": {
                "deployments": three,
                "health": {"slow_ratio": 3, "slow_min_secs": 0.1, "slow_min_samples": 2},
            }
        },
        {
            "models": {
                f"{GPT}@r1": {"latency_ms": {"p50": 600, "p95": 600}},
                f"{GPT}@r2": {"latency_ms": {"p50": 20, "p95": 20}},
                f"{GPT}@r3": {"latency_ms": {"p50": 20, "p95": 20}},
            }
        },
    )
    for region in ("r1", "r2", "r3"):
        for _ in range(2):  # samples for every deployment
            await engine.call("big", CHAT, allowed_regions=[region])
    served = [(await engine.call("big", CHAT)).meta.deployment for _ in range(4)]
    assert "aws-a/r1" not in served  # ordered would pick r1 first; it is slow
    only = await engine.call("big", CHAT, allowed_regions=["r1"])
    assert only.meta.deployment == "aws-a/r1"  # never excluded when it is the only one
    assert stats(fake, f"{GPT}@r1")["calls"] == 3
