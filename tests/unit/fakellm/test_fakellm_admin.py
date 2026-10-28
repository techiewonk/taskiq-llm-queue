"""Admin API of fakellm over HTTP: scenario get/put, model patch, reset, stats, metrics.

These are the endpoints the scenario harness drives between and during runs.
"""

from __future__ import annotations

from typing import Any

import aiohttp
from fakellm_testkit import Fake, StartFake

CHAT: Any = {"model": "m", "messages": [{"role": "user", "content": "x"}]}


async def call(
    fake: Fake,
    method: str,
    path: str,
    body: Any = None,
    *,
    data: str | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    async with (
        aiohttp.ClientSession() as session,
        session.request(method, fake.url + path, json=body, data=data, headers=headers) as resp,
    ):
        payload = await (resp.json() if resp.content_type == "application/json" else resp.text())
        return resp.status, payload


async def test_health_and_metrics(start_fake: StartFake) -> None:
    fake = await start_fake()
    assert await call(fake, "GET", "/health") == (200, {"status": "ok"})
    await call(fake, "POST", "/v1/chat/completions", CHAT)
    status, text = await call(fake, "GET", "/metrics")
    assert status == 200
    assert "# TYPE fakellm_calls_total counter" in text
    assert 'fakellm_calls_total{key="m"} 1' in text
    assert 'fakellm_ok_total{key="m"} 1' in text
    assert "fakellm_elapsed_seconds" in text


async def test_get_and_put_scenario(start_fake: StartFake) -> None:
    fake = await start_fake({"seed": 1, "models": {"m": {"rpm": 5}}})
    status, body = await call(fake, "GET", "/admin/scenario")
    assert status == 200
    assert body["models"] == {"m": {"rpm": 5}}
    await call(fake, "POST", "/v1/chat/completions", CHAT)
    new = {"seed": 2, "models": {"m": {"force_error": "ThrottlingException"}}}
    status, body = await call(fake, "PUT", "/admin/scenario", new)
    assert status == 200
    assert body["seed"] == 2
    assert fake.engine.stats == {}
    status, _ = await call(fake, "POST", "/v1/chat/completions", CHAT)
    assert status == 429


async def test_put_scenario_yaml_and_rejects_bad(start_fake: StartFake) -> None:
    fake = await start_fake()
    yaml_text = "models:\n  m: {dims: 16}\n"
    status, body = await call(
        fake, "PUT", "/admin/scenario", data=yaml_text, headers={"Content-Type": "application/yaml"}
    )
    assert (status, body["models"]) == (200, {"m": {"dims": 16}})
    status, body = await call(fake, "PUT", "/admin/scenario", {"models": {"m": {"x": 1}}})
    assert status == 400
    assert "unknown behaviour field" in body["message"]
    assert fake.engine.scenario.models == {"m": {"dims": 16}}


async def test_patch_model_mid_test(start_fake: StartFake) -> None:
    """PATCH changes one key (incl. profile ids with ':'); null drops an override."""
    fake = await start_fake({"models": {"apac.x-1:0": {"max_concurrency": 5}}})
    status, body = await call(fake, "PATCH", "/admin/models/apac.x-1%3A0", {"throttle_rate": 1.0})
    assert status == 200
    assert body["block"] == {"max_concurrency": 5, "throttle_rate": 1.0}
    assert body["behaviour"]["throttle_rate"] == 1.0
    status, body = await call(fake, "PATCH", "/admin/models/apac.x-1:0", {"throttle_rate": None})
    assert body["block"] == {"max_concurrency": 5}
    status, body = await call(fake, "PATCH", "/admin/models/m", {"throttle_rate": 2})
    assert status == 400


async def test_stats_and_reset(start_fake: StartFake) -> None:
    fake = await start_fake({"models": {"m": {"max_concurrency": 1, "throttle_rate": 0.0}}})
    for _ in range(2):
        await call(fake, "POST", "/v1/chat/completions", CHAT)
    status, body = await call(fake, "GET", "/admin/stats")
    assert status == 200
    assert body["models"]["m"] == {
        "calls": 2,
        "ok": 2,
        "concurrent": 0,
        "max_concurrent_seen": 1,
        "throttled": 0,
        "errors": 0,
    }
    assert await call(fake, "POST", "/admin/reset") == (200, {"status": "reset"})
    _, body = await call(fake, "GET", "/admin/stats")
    assert body["models"] == {}
    assert body["elapsed_s"] < 5
