"""Mount the queue's status, registry and health routes on a FastAPI app (admin pages, probes).

`llmq.http_router()` returns an `APIRouter` (extra `http`). `/health/live` and `/health/ready`
take no auth (Kubernetes probes); every other route runs the app's admin dependencies. Writes
(live registry edits, region / account blocks) stay off unless `allow_writes=True`. A preStop
hook calls `llmq.pause_receiving()`: readiness fails and no new queued work starts.

Here the app is called in process (httpx's ASGI transport), so no port is opened; in the app
the same router is served by uvicorn.

    python scripts/samples.py 09_http_router [--live]
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import httpx
from fastapi import FastAPI, Header, HTTPException

from taskiq_llm_queue import LLMQueue

llmq = LLMQueue.from_env()


def admin_only(x_admin_token: str = Header(default="")) -> None:
    """The app's own admin check (here a fixed header; use the app's auth)."""
    if x_admin_token != "sample-admin":
        raise HTTPException(status_code=401, detail="admin only")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await llmq.start()
    yield
    await llmq.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(
    llmq.http_router(
        dependencies=[admin_only],
        allow_writes=True,
        actor=lambda request: "sample-admin",  # the authenticated user's name in an app
    ),
    prefix="/llm-queue",
)


@app.post("/drain")
async def drain() -> dict[str, bool]:
    """PreStop hook: fail readiness and take no new queued calls; running ones finish."""
    llmq.pause_receiving()
    return {"draining": True}


async def main() -> None:
    admin = {"x-admin-token": "sample-admin"}
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        ready = await client.get("/llm-queue/health/ready")
        print(f"GET /health/ready -> {ready.status_code} {ready.json()}")
        assert ready.status_code == 200

        denied = await client.get("/llm-queue/status")
        print(f"GET /status without the admin header -> {denied.status_code}")
        assert denied.status_code == 401

        reply = await llmq.converse(
            "gpt20b",
            {"messages": [{"role": "user", "content": [{"text": "Say hi."}]}],
             "inferenceConfig": {"maxTokens": 200}},
        )  # fmt: skip
        found = await client.get(f"/llm-queue/requests/{reply.request_id}", headers=admin)
        print(f"GET /requests/{{id}} -> {found.json()['state']}")
        assert found.json()["state"] == "done"

        status = (await client.get("/llm-queue/status?model=gpt20b", headers=admin)).json()
        for dep in status["deployments"]:
            print(f"  {dep['name']}: rpm {dep['rpm']['used']}/{dep['rpm']['limit']}")

        edit = await client.post(
            "/llm-queue/registry",
            params={"model": "gpt20b"},
            json={"rpm": 50},
            headers=admin,
        )
        print(f"POST /registry rpm=50 -> {edit.json()['diff']}")
        assert edit.status_code == 200
        await client.post(
            "/llm-queue/registry",
            params={"model": "gpt20b"},
            json={"rpm": 60},
            headers=admin,
        )  # put the sample value back

        await client.post("/drain")
        after = await client.get("/llm-queue/health/ready")
        print(f"after /drain: GET /health/ready -> {after.status_code}")
        assert after.status_code == 503


if __name__ == "__main__":
    asyncio.run(main())
