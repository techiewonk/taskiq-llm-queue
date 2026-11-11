"""The engine end to end: caller, queue, admission, provider (fakellm), result (plan A7, A8).

Each test runs a real `Engine` on Valkey and on Redis against an in-process fakellm: chat and
embeddings, priority, budgets and expiry, region and model fallback, a hung region, duplicate
request ids, the waiting limit and an unusable model. No real LLM is called.
"""

from __future__ import annotations

import asyncio
import copy
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer

from taskiq_llm_queue.clients import ClientPool, Providers
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.engine.runtime import Engine, EngineSettings
from taskiq_llm_queue.registry.store import RegistryStore

TOOLS = Path(__file__).resolve().parents[3] / "tools" / "fakellm"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))
from fakellm import Engine as FakeEngine  # noqa: E402  # needs the sys.path entry
from fakellm import Scenario, create_app  # noqa: E402

pytestmark = pytest.mark.integration

GPT = "openai.gpt-oss-120b-1:0"
SMALL = "openai.gpt-oss-20b-1:0"
TITAN = "amazon.titan-embed-text-v2:0"
CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


def registry(models: dict[str, Any] | None = None) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "registry_version": 1,
        "defaults": {"rate_window": "fixed", "timeout_secs": 10, "attempt_timeout_secs": 2},
        "accounts": {"aws-a": {"kind": "aws"}},
        "models": {
            "big": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": GPT,
                "retry_base_secs": 0.05,
                "deployments": {"aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"]}},
            },
            "small": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": SMALL,
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            },
            "titan": {
                "type": "embedding",
                "provider": "bedrock",
                "model_id": TITAN,
                "inference": {"dimensions": 256},
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            },
        },
    }
    for alias, extra in (models or {}).items():
        doc["models"][alias].update(copy.deepcopy(extra))
    return doc


Make = Callable[..., Awaitable[tuple[Engine, FakeEngine]]]


@pytest.fixture
async def make(store: redis.asyncio.Redis, prefix: str) -> AsyncIterator[Make]:
    servers: list[TestServer] = []
    engines: list[Engine] = []

    async def build(
        models: dict[str, Any] | None = None,
        scenario: dict[str, Any] | None = None,
        *,
        worker: bool = True,
    ) -> tuple[Engine, FakeEngine]:
        fake = FakeEngine(Scenario.from_mapping(scenario or {}))
        server = TestServer(create_app(engine=fake), host="127.0.0.1")
        await server.start_server()
        servers.append(server)
        url = str(server.make_url("")).rstrip("/")
        pool = ClientPool(
            bedrock_endpoint_url=url,
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
        )
        reg = RegistryStore(store, prefix, config=registry(models))
        engine = Engine(
            store,
            reg,
            Providers(pool),
            settings=EngineSettings(lease_secs=3, poll_secs=0.2, sweep_secs=1),
        )
        await engine.start(worker=worker)
        engines.append(engine)
        return engine, fake

    yield build
    for engine in engines:
        await engine.stop(drain_secs=1)
        await engine.providers.aclose()
    for server in servers:
        await server.close()


def _stats(fake: FakeEngine, key: str) -> dict[str, int]:
    models = fake.snapshot()["models"]
    assert isinstance(models, dict)
    return dict(models.get(key, {}))


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
