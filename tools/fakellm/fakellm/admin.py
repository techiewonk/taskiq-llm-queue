"""Admin API: read/replace the scenario, patch one key, reset, stats, metrics and health.

Scenario harnesses call `POST /admin/reset` before each run and read `GET /admin/stats`
for pass criteria; `PATCH /admin/models/{key}` changes one key's behaviour mid-test.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any
from urllib.parse import unquote

from aiohttp import web

from fakellm.scenario import Scenario, ScenarioError
from fakellm.state import ENGINE


async def _mapping(request: web.Request) -> Any:
    text = await request.text()
    if "yaml" in request.content_type:
        import yaml  # type: ignore[import-untyped]  # no types-PyYAML in the venv

        return yaml.safe_load(text)
    return json.loads(text or "null")


def _bad(message: str) -> web.Response:
    return web.json_response({"message": message}, status=400)


async def get_scenario(request: web.Request) -> web.Response:
    """Return the whole scenario in its file shape."""
    return web.json_response(request.app[ENGINE].scenario.to_mapping())


async def put_scenario(request: web.Request) -> web.Response:
    """Replace the scenario (JSON, or YAML with a yaml content type) and reset counters."""
    try:
        scenario = Scenario.from_mapping(await _mapping(request))
    except (ValueError, ScenarioError) as exc:
        return _bad(str(exc))
    request.app[ENGINE].replace_scenario(scenario)
    return web.json_response(scenario.to_mapping())


async def patch_model(request: web.Request) -> web.Response:
    """Merge behaviour fields into one key's block; returns the key's effective behaviour."""
    key = unquote(request.match_info["key"])
    try:
        patch = await _mapping(request)
        if not isinstance(patch, dict):
            raise ScenarioError("patch body must be a mapping")
        engine = request.app[ENGINE]
        engine.scenario.patch_model(key, patch)
    except (ValueError, ScenarioError) as exc:
        return _bad(str(exc))
    block = engine.scenario.models[key]
    return web.json_response(
        {"key": key, "block": block, "behaviour": asdict(engine.behaviour(key))}
    )


async def reset(request: web.Request) -> web.Response:
    """Clear counters and restart the phase clock (scenario and patches are kept)."""
    request.app[ENGINE].reset()
    return web.json_response({"status": "reset"})


async def stats(request: web.Request) -> web.Response:
    """Per-key counters: calls, ok, concurrent, max_concurrent_seen, throttled, errors."""
    return web.json_response(request.app[ENGINE].snapshot())


async def metrics(request: web.Request) -> web.Response:
    """Prometheus text exposition of the same counters."""
    return web.Response(text=request.app[ENGINE].prometheus(), content_type="text/plain")


async def health(request: web.Request) -> web.Response:
    """Liveness probe for compose and harnesses."""
    return web.json_response({"status": "ok"})


def add_routes(app: web.Application) -> None:
    """Register the admin, metrics and health routes."""
    app.router.add_get("/admin/scenario", get_scenario)
    app.router.add_put("/admin/scenario", put_scenario)
    app.router.add_patch("/admin/models/{key:.+}", patch_model)
    app.router.add_post("/admin/reset", reset)
    app.router.add_get("/admin/stats", stats)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/health", health)
