"""Operations views on Valkey and Redis (plan step A12; scenarios B13, B14, B36, B55, B79).

Status per model and per request, readiness, alerts (gauges and one log per change), metrics
and the per-call record, model drain, the HTTP handlers, the command line and the environment
settings. No real LLM is called.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from taskiq_llm_queue.engine import cli
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.engine.http import Handlers
from taskiq_llm_queue.engine.metrics import EmfSink
from taskiq_llm_queue.engine.settings import InfraSettings
from taskiq_llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration


async def test_status_of_model_and_request(make: Make) -> None:
    engine, _ = await make(
        {"big": {"rpm": 50}},
        {"defaults": {"latency_ms": {"p50": 600, "p95": 600}}},
    )
    minute = engine.clock.now() // 60
    task = asyncio.create_task(engine.call("big", CHAT, request_id="r-status"))
    for _ in range(100):  # until it runs (not a fixed sleep: client start-up time varies)
        running = await engine.request_status("r-status")
        if running.get("state") == "in_progress":
            break
        await asyncio.sleep(0.03)
    assert running["state"] == "in_progress"
    assert running["deployment"] == "aws-a/r1"
    await task
    done = await engine.request_status("r-status")
    assert done["state"] == "done"
    status = await engine.status("big")
    dep = next(d for d in status["deployments"] if d["name"] == "aws-a/r1")
    if engine.clock.now() // 60 == minute:  # the counters are per calendar minute
        assert dep["rpm"] == {"limit": 50, "used": 1, "left": 49}
    else:
        assert dep["rpm"]["used"] + dep["rpm"]["left"] == 50
    assert dep["healthy"]
    assert dep["breaker"] == "closed"
    assert dep["concurrency"]["in_use"] == 0
    assert status["waiting"] == {"priority": 0, "normal": 0}
    everything = await engine.status()
    assert {m["model"] for m in everything["models"]} == {"big", "small", "titan", "cohere"}
    assert await engine.request_status("nope") == {}


async def test_readiness_never_depends_on_providers(make: Make) -> None:
    engine, _ = await make(scenario={"defaults": {"force_error": "ServiceUnavailableException"}})
    health = await engine.health_check()
    assert health == {"live": True, "ready": True, "stores": {"state": "ok", "queue": "ok"}}
    await engine.stop(drain_secs=0)
    assert (await engine.health_check())["ready"] is False


async def test_alerts_log_once_per_change(make: Make) -> None:
    engine, _ = await make()
    await engine.health.disable("big", "aws-a/r1", probe_after_secs=600)
    view = engine.registry.current
    alerts = engine.alerts.evaluate(view, await engine.status())
    on = {(a.name, a.labels.get("deployment")) for a in alerts if a.on}
    assert ("deployment_disabled", "aws-a/r1") in on
    first = await engine.alerts.changed(alerts)
    assert any(a.name == "deployment_disabled" for a in first)
    assert await engine.alerts.changed(alerts) == []  # no change: no new log line


async def test_metrics_and_call_record(make: Make, caplog: pytest.LogCaptureFixture) -> None:
    lines: list[str] = []
    engine, _ = await make()
    engine.metrics = EmfSink(lines.append)
    caplog.set_level(logging.INFO, logger="taskiq_llm_queue.engine.runtime")
    await engine.call("big", CHAT, user_id="ann", tenant_id="desk")
    logs = [getattr(r, "llmq", {}) for r in caplog.records]
    names = [
        next(
            k
            for k in json.loads(line)
            if not k.startswith("_")
            and k not in ("model", "account", "region", "outcome", "priority", "kind", "reason")
        )
        for line in lines
    ]
    assert "llmq_calls_total" in names
    assert "llmq_tokens_output_total" in names
    point = json.loads(lines[names.index("llmq_calls_total")])
    assert (point["model"], point["region"], point["outcome"]) == ("big", "r1", "ok")
    record = [entry for entry in logs if entry.get("event") == "llmq_call_finished"]
    assert len(record) == 1
    assert record[0]["tenant"] == "desk"
    assert record[0]["user"] != "ann"  # the user id is hashed
    assert record[0]["tokens_out"] is not None


async def test_model_drain(make: Make) -> None:
    engine, _ = await make()
    await engine.registry.set({"enabled": False}, model="small", actor="t")
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "model_disabled"
    rows = await engine.state.hget(engine.keys.model("small", "cfg"), "numbers")
    assert json.loads(rows)["deployments"][0]["enabled"] is True  # waiting calls still served


async def test_http_handlers(make: Make) -> None:
    engine, _ = await make()
    reader = Handlers(engine)
    assert (await reader.ready())[0] == 200
    assert (await reader.status("nope"))[0] == 404
    assert (await reader.request("nope"))[0] == 404
    code, body = await reader.registry()
    assert code == 200
    assert body["registry"]["models"]["big"]["model_id"]
    assert (await reader.set({"rpm": 5}, actor="ann", model="big"))[0] == 403
    writer = Handlers(engine, allow_writes=True)
    code, body = await writer.set({"rpm": 5}, actor="ann", model="big")
    assert code == 200
    assert body["diff"] == [{"path": "models.big.rpm", "before": None, "after": 5}]
    code, body = await writer.set({"deployments.aws-a.confirmed": ["r1"]}, actor="ann", model="big")
    assert code == 400  # protected field
    code, body = await writer.block(actor="ann", region="r2")
    assert body["blocked_regions"] == ["r2"]


async def test_command_line(make: Make, store: redis.asyncio.Redis, prefix: str) -> None:
    engine, _ = await make()
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(dict(engine.registry.current.registry.raw)),
    }

    async def run(*argv: str) -> Any:
        return await cli.amain(["--actor", "tester", *argv], env)

    assert (await run("registry", "get", "--model", "big"))["model_id"]
    table = await run("settings", "get", "--model", "big", "--deployment", "aws-a/r1")
    assert table["timeout_secs"] == {"value": 10, "level": "defaults"}
    assert (await run("registry", "set", "--model", "big", "rpm=7"))["changes"] >= 1
    log = await run("registry", "log", "--count", "1")
    assert log[0]["actor"] == "tester"
    assert (await run("block", "--region", "r3"))["blocked_regions"] == ["r3"]
    assert (await run("unblock", "--region", "r3"))["blocked_regions"] == []
    assert "models.big.rpm" in [c["path"] for c in await run("registry", "diff")]
    assert (await run("status", "--model", "big"))["model"] == "big"
    assert await run("dlq", "list", "--model", "big") == []
    queues = await run("queues", "list")
    assert any(name.endswith("llm-big-priority") for name in queues)
    drained = await run("drain", "small", "--wait", "--timeout", "2")
    assert drained == {"model": "small", "enabled": False, "drained": True}


def test_environment_settings() -> None:
    ok = InfraSettings.from_env({"LLMQ_STORE_URL": "redis://localhost:6379/0"})
    assert ok.registry_live_edit == "safe"
    with pytest.raises(ConfigError) as info:
        InfraSettings.from_env(
            {
                "LLMQ_STORE_URL": "redis://cache.example.com:6379",
                "LLMQ_ENVIRONMENT": "prod",
                "LLMQ_REDIS_HOST": "old",
                "LLMQ_REGISTRY": "{}",
                "LLMQ_REGISTRY_FILE": "x.json",
            }
        )
    text = str(info.value)
    assert "LLMQ_REDIS_HOST is removed in 0.3; use LLMQ_STORE_URL" in text
    assert "must use TLS" in text
    assert "LLMQ_ID_HASH_SALT is required outside dev (prod)" in text
    assert "not both" in text


async def test_debug_capture(make: Make) -> None:
    engine, _ = await make({"big": {"debug_capture": True}})
    reply = await engine.call("big", CHAT)  # without the process opt-in: nothing is kept
    assert await engine.state.get(engine.keys.request(reply.request_id, "debug")) is None
    engine.settings = dataclasses.replace(engine.settings, debug_capture_allowed=True)
    reply = await engine.call("big", CHAT)
    captured = await engine.state.get(engine.keys.request(reply.request_id, "debug"))
    assert captured is not None
    assert json.loads(captured)["request"].startswith("{")
    assert 0 < await engine.state.ttl(engine.keys.request(reply.request_id, "debug")) <= 3600
