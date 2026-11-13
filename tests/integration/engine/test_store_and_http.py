"""Store failover handling, preStop pause and the FastAPI router (found by the 0.3.0 live runs).

A failover leaves pooled connections on the old primary, now a replica, so writes get
`READONLY`; the store client must close such a connection and retry on a new one, and
readiness must report the store as `read_only` (PING alone passes on a replica). The full
failover with a real replica is `scripts/failover.py`. No real LLM is called.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import redis.asyncio
from engine_testkit import CHAT, Make
from fastapi import FastAPI, Header, HTTPException
from redis.asyncio.connection import Connection
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError

from taskiq_llm_queue.engine.http import status_router
from taskiq_llm_queue.engine.runtime import Engine
from taskiq_llm_queue.engine.store_client import store_client

pytestmark = pytest.mark.integration


def _url(store: redis.asyncio.Redis) -> str:
    port = store.connection_pool.connection_kwargs["port"]
    return f"redis://:localdev@127.0.0.1:{port}/0"


async def test_readonly_reply_reconnects_and_retries(
    store: redis.asyncio.Redis, prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: after a failover every write failed with READONLY until the process restarted."""
    client = store_client(_url(store))
    await client.ping()
    real = Connection.read_response
    fail = {"left": 2}
    disconnects = {"n": 0}
    real_disconnect = Connection.disconnect

    async def read_response(self: Connection, *args: Any, **kwargs: Any) -> Any:
        if fail["left"]:
            fail["left"] -= 1
            await real(self, *args, **kwargs)  # consume the real reply first
            raise ReadOnlyError("You can't write against a read only replica.")
        return await real(self, *args, **kwargs)

    async def disconnect(self: Connection, *args: Any, **kwargs: Any) -> None:
        disconnects["n"] += 1
        await real_disconnect(self, *args, **kwargs)

    monkeypatch.setattr(Connection, "read_response", read_response)
    monkeypatch.setattr(Connection, "disconnect", disconnect)
    assert await client.set(f"{prefix}:ro", b"1") is True
    assert fail["left"] == 0
    assert disconnects["n"] >= 2  # each READONLY dropped its connection
    await client.aclose()


async def test_readiness_reports_a_read_only_store(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, _ = await make()
    assert (await engine.health_check())["stores"] == {"state": "ok", "queue": "ok"}

    async def refuse(*args: Any, **kwargs: Any) -> Any:
        raise ReadOnlyError("You can't write against a read only replica.")

    monkeypatch.setattr(engine.state, "set", refuse)
    health = await engine.health_check()
    assert health["stores"] == {"state": "read_only", "queue": "read_only"}
    assert health["ready"] is True  # the stores never make the process unready


def test_writes_need_an_actor(make: Make) -> None:
    """Bug: the audit actor came from a query parameter any admin could set."""
    from taskiq_llm_queue.errors import ConfigError

    with pytest.raises(ConfigError, match="actor"):
        status_router(object(), allow_writes=True)  # type: ignore[arg-type]


async def test_pause_receiving_fails_readiness_and_takes_no_calls(make: Make) -> None:
    worker, _ = await make()
    caller, _ = await make(worker=False)
    worker.pause_receiving()
    assert (await worker.health_check())["ready"] is False
    task = asyncio.create_task(caller.call("small", CHAT, request_id="r-paused"))
    await asyncio.sleep(1.0)
    assert not task.done()
    assert (await caller.request_status("r-paused")).get("state") != "in_progress"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.fixture
async def http(make: Make) -> AsyncIterator[tuple[Engine, httpx.AsyncClient]]:
    engine, _ = await make()

    def admin(x_admin: str = Header(default="")) -> None:
        if x_admin != "yes":
            raise HTTPException(status_code=401)

    app = FastAPI()
    router = status_router(
        engine,
        dependencies=[admin],
        allow_writes=True,
        actor=lambda request: request.headers.get("x-user", "nobody"),
    )
    app.include_router(router, prefix="/q")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://app") as client:
        yield engine, client


async def test_http_router_routes_and_auth(http: tuple[Engine, httpx.AsyncClient]) -> None:
    engine, client = http
    assert (await client.get("/q/health/live")).json() == {"live": True}
    ready = await client.get("/q/health/ready")  # no auth on health
    assert ready.status_code == 200
    assert ready.json()["stores"] == {"state": "ok", "queue": "ok"}
    assert (await client.get("/q/status")).status_code == 401
    admin = {"x-admin": "yes"}
    everything = await client.get("/q/status", headers=admin)
    assert {m["model"] for m in everything.json()["models"]} >= {"big", "small"}
    assert (
        await client.get("/q/status", params={"model": "nope"}, headers=admin)
    ).status_code == 404
    reply = await engine.call("small", CHAT, request_id="r-http")
    found = await client.get(f"/q/requests/{reply.request_id}", headers=admin)
    assert found.json()["state"] == "done"
    assert (await client.get("/q/requests/nope", headers=admin)).status_code == 404
    reg = await client.get("/q/registry", headers=admin)
    assert reg.json()["registry"]["models"]["big"]["model_id"]


async def test_http_router_writes(http: tuple[Engine, httpx.AsyncClient]) -> None:
    engine, client = http
    admin = {"x-admin": "yes", "x-user": "ann"}
    params = {"actor": "mallory", "model": "big"}  # a client-supplied actor is ignored
    assert (await client.post("/q/registry", json={"rpm": 5}, params=params)).status_code == 401
    done = await client.post("/q/registry", json={"rpm": 5}, params=params, headers=admin)
    assert done.status_code == 200, done.text
    assert done.json()["diff"] == [{"path": "models.big.rpm", "before": None, "after": 5}]
    refused = await client.post(
        "/q/registry", json={"deployments.aws-a.confirmed": ["r1"]}, params=params, headers=admin
    )
    assert refused.status_code == 400  # protected field
    blocked = await client.post("/q/registry/block", params={"region": "r2"}, headers=admin)
    assert blocked.json()["blocked_regions"] == ["r2"]
    typo = await client.post("/q/registry/block", params={"region": "r9"}, headers=admin)
    assert typo.status_code == 400  # a region no deployment uses would block nothing
    assert (await client.post("/q/registry/block", headers=admin)).status_code == 400
    log = await engine.registry.log(count=5)
    actors = {row.get("actor") for row in log}
    assert "http:ann" in actors
    assert "http:mallory" not in actors
    read_only = FastAPI()
    read_only.include_router(status_router(engine))
    transport = httpx.ASGITransport(app=read_only)
    async with httpx.AsyncClient(transport=transport, base_url="http://app") as other:
        assert (await other.post("/registry", json={"rpm": 6})).status_code == 403


async def test_finish_skips_on_store_error(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bug: a store blip after the provider answered killed the run and threw the answer away."""
    engine, _ = await make()

    async def down(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("Connection closed by server.")

    monkeypatch.setattr(engine.admission, "_call", down)
    assert (
        await engine.admission.finish("big", "o", "r", refund_r=0, refund_t=0, outcome="ok")
        is False
    )
