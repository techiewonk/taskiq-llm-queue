"""fakellm with a probe: per-request call log and scripted faults, keyed by a `rid=` marker.

Scenarios put `rid=<id>` in each prompt or embedding text; the probe records every provider
call carrying that marker (time, status, model) so tests can prove exactly-once execution, and
can script per-request faults (an error N times, or a hang) on top of fakellm's own behaviour.

Run: `python -m tools.scenarios.fakellm_probe --port 18080 --scenario <yaml>`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from aiohttp import web
from fakellm.app import create_app
from fakellm.bedrock import bedrock_error, region_from_auth
from fakellm.behaviour import Engine
from fakellm.openai_api import openai_error
from fakellm.scenario import Scenario, load_scenario

RID_RE = re.compile(r"rid=([A-Za-z0-9_.:-]+)")
_ADMIN = ("/admin", "/scn", "/health", "/metrics")


@dataclass(slots=True)
class Script:
    """Faults for calls whose rid starts with `rid_prefix`: `actions` consumed one per call.

    An action is an error name (fakellm `ERRORS` key) or `hang:<secs>`; `model` limits it to
    one model id substring. `repeat` keeps the last action forever.
    """

    rid_prefix: str
    actions: list[str]
    model: str = ""
    repeat: bool = False
    used: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class Probe:
    """Call log and scripts shared by the middleware and the `/scn/*` routes."""

    calls: dict[str, list[list[Any]]] = field(default_factory=lambda: defaultdict(list))
    unmarked: int = 0
    scripts: list[Script] = field(default_factory=list)

    def action(self, rid: str, model: str) -> str | None:
        """Next scripted action for this call, if any."""
        for script in self.scripts:
            if not rid.startswith(script.rid_prefix) or script.model not in model:
                continue
            n = script.used.get(rid, 0)
            script.used[rid] = n + 1
            if n < len(script.actions):
                return script.actions[n]
            if script.repeat and script.actions:
                return script.actions[-1]
        return None


PROBE = web.AppKey("scn_probe", Probe)


def _model_of(request: web.Request, body: bytes) -> str:
    if request.path.startswith("/model/"):
        model_id = unquote(request.path[len("/model/") :].rsplit("/", 1)[0])
        region = region_from_auth(request.headers.get("Authorization", ""))
        return f"{model_id}@{region}"
    try:
        return str(json.loads(body or b"{}").get("model") or "unknown")
    except ValueError:
        return "unknown"


def _scripted_error(request: web.Request, name: str) -> web.Response:
    if name.startswith("err:"):  # any AWS-style error: err:<Code>:<status>
        _, code, status = name.split(":", 2)
        return web.json_response(
            {"message": f"{code} (scripted)"},
            status=int(status),
            headers={"x-amzn-ErrorType": code},
        )
    if request.path.startswith("/v1/"):
        return openai_error(name, retry_after_s=1.0)
    return bedrock_error(name)


@web.middleware
async def probe_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    """Record marked provider calls and apply scripted faults before fakellm runs."""
    if request.path.startswith(_ADMIN):
        return await handler(request)  # type: ignore[no-any-return]
    probe = request.app[PROBE]
    body = await request.read()
    found = RID_RE.findall(body.decode("utf-8", "replace"))
    model = _model_of(request, body)
    t0 = time.time()
    if not found:
        probe.unmarked += 1
        return await handler(request)  # type: ignore[no-any-return]
    rid = found[0]
    action = probe.action(rid, model)
    response: web.StreamResponse
    if action is not None and action.startswith("hang:"):
        await asyncio.sleep(float(action.split(":", 1)[1]))
        action = None
    if action is not None:
        response = _scripted_error(request, action)
    else:
        response = await handler(request)
    probe.calls[rid].append([round(t0, 4), round(time.time(), 4), response.status, model])
    return response


async def calls(request: web.Request) -> web.Response:
    """`GET /scn/calls[?prefix=]`: rid -> [[t_start, t_end, status, model], ...]."""
    probe = request.app[PROBE]
    prefix = request.query.get("prefix", "")
    data = {k: v for k, v in probe.calls.items() if k.startswith(prefix)}
    return web.json_response({"calls": data, "unmarked": probe.unmarked})


async def reset(request: web.Request) -> web.Response:
    """`POST /scn/reset`: clear the call log and scripts."""
    probe = request.app[PROBE]
    probe.calls.clear()
    probe.scripts.clear()
    probe.unmarked = 0
    return web.json_response({"status": "reset"})


async def add_script(request: web.Request) -> web.Response:
    """`POST /scn/script {rid_prefix, actions, model?, repeat?}`."""
    raw = await request.json()
    request.app[PROBE].scripts.append(
        Script(
            str(raw["rid_prefix"]),
            [str(a) for a in raw["actions"]],
            str(raw.get("model", "")),
            bool(raw.get("repeat", False)),
        )
    )
    return web.json_response({"status": "ok"})


async def cpu(request: web.Request) -> web.Response:
    """`GET /scn/cpu`: CPU seconds this fakellm process used (one process serves all calls)."""
    return web.json_response({"cpu_s": time.process_time()})


def build_app(scenario: Scenario, seed: int | None) -> web.Application:
    """Fakellm app plus the probe middleware and `/scn/*` routes."""
    app = create_app(engine=Engine(scenario, seed=seed))
    app.middlewares.append(probe_middleware)
    app[PROBE] = Probe()
    app.router.add_get("/scn/calls", calls)
    app.router.add_get("/scn/cpu", cpu)
    app.router.add_post("/scn/reset", reset)
    app.router.add_post("/scn/script", add_script)
    return app


def main(argv: list[str] | None = None) -> None:
    """Serve fakellm + probe until killed."""
    parser = argparse.ArgumentParser(prog="fakellm_probe")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--scenario")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    scenario = load_scenario(args.scenario) if args.scenario else Scenario()
    web.run_app(
        build_app(scenario, args.seed),
        host=args.host,
        port=args.port,
        access_log=None,
        print=None,
        backlog=4096,
    )


if __name__ == "__main__":
    main()
