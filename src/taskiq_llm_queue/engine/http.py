"""HTTP views of the engine (plan §3.12, E132, E138): status, requests, registry, health.

`Handlers` are plain async functions returning (status code, JSON body), so they are tested
without a web framework; `status_router()` mounts them on FastAPI (extra `http`). Health routes
take no auth; the app puts its admin dependency on the rest. Writes (safe fields only) are off
unless the app passes `allow_writes=True`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.registry.store import LiveEditRefused, audit_diff
from taskiq_llm_queue.registry.validate import RegistryError

if TYPE_CHECKING:
    from taskiq_llm_queue.engine.runtime import Engine

_REDACT = frozenset({"role_arn", "external_id_ref", "api_key_ref"})
Response = tuple[int, dict[str, Any]]


def redacted(doc: Any) -> Any:
    """The registry with role ARNs and secret references hidden."""
    if isinstance(doc, Mapping):
        return {k: "[redacted]" if k in _REDACT else redacted(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [redacted(v) for v in doc]
    return doc


class Handlers:
    """The HTTP handlers of one engine."""

    def __init__(self, engine: Engine, *, allow_writes: bool = False) -> None:
        """Writes stay refused unless `allow_writes`."""
        self.engine = engine
        self.allow_writes = allow_writes

    async def live(self) -> Response:
        """200 while the process runs."""
        return 200, {"live": True}

    async def ready(self) -> Response:
        """200 when the registry is loaded and the process is not draining (stores in the body)."""
        health = await self.engine.health_check()
        return (200 if health["ready"] else 503), health

    async def status(self, model: str | None = None) -> Response:
        """Status of a model or of everything."""
        try:
            return 200, await self.engine.status(model)
        except KeyError:
            return 404, {"error": f"unknown model {model}"}

    async def request(self, request_id: str) -> Response:
        """Status of one call."""
        found = await self.engine.request_status(request_id)
        return (200, found) if found else (404, {"error": "unknown or expired request"})

    async def registry(self) -> Response:
        """The live registry, secrets hidden, with what is live but not in config."""
        store = self.engine.registry
        return 200, {
            "registry": redacted(dict(store.current.registry.raw)),
            "changes": store.changes,
            "live_edits": redacted(await store.diff_with_config()),
        }

    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> Response:
        """A live edit of safe fields (only with `allow_writes`); audited with `actor`."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        before = dict(self.engine.registry.current.registry.raw)
        try:
            view = await self.engine.registry.set(values, actor=f"http:{actor}", model=model)
        except (LiveEditRefused, RegistryError, ConfigError) as exc:
            return 400, {"error": str(exc)}
        return 200, {"diff": redacted(audit_diff(before, dict(view.registry.raw)))}

    async def block(
        self, *, actor: str, region: str | None = None, account: str | None = None, on: bool = True
    ) -> Response:
        """Block or unblock a region or account (only with `allow_writes`)."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        await self.engine.registry.block(
            region=region, account=account, on=on, actor=f"http:{actor}"
        )
        routing = self.engine.registry.current.registry.routing
        return 200, {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }


def status_router(
    engine: Engine, *, dependencies: Sequence[Any] = (), allow_writes: bool = False
) -> Any:
    """A FastAPI `APIRouter` with the handlers (`pip install 'taskiq-llm-queue[http]'`).

    `dependencies` (the app's admin check) guard every route except `/health/live` and
    `/health/ready`.
    """
    try:
        from fastapi import APIRouter, Body, Depends
        from fastapi.responses import JSONResponse
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        hint = "status_router needs FastAPI: pip install 'taskiq-llm-queue[http]'"
        raise ConfigError(hint) from exc

    handlers = Handlers(engine, allow_writes=allow_writes)
    router = APIRouter()
    guarded = [Depends(d) if callable(d) else d for d in dependencies]

    def reply(result: Response) -> Any:
        code, body = result
        return JSONResponse(body, status_code=code)

    @router.get("/health/live")
    async def live() -> Any:
        return reply(await handlers.live())

    @router.get("/health/ready")
    async def ready() -> Any:
        return reply(await handlers.ready())

    @router.get("/status", dependencies=guarded)
    async def status(model: str | None = None) -> Any:
        return reply(await handlers.status(model))

    @router.get("/requests/{request_id}", dependencies=guarded)
    async def request(request_id: str) -> Any:
        return reply(await handlers.request(request_id))

    @router.get("/registry", dependencies=guarded)
    async def registry() -> Any:
        return reply(await handlers.registry())

    @router.post("/registry", dependencies=guarded)
    async def set_values(
        values: dict[str, Any] = Body(...),  # noqa: B008  # FastAPI's dependency style
        actor: str = "unknown",
        model: str | None = None,
    ) -> Any:
        return reply(await handlers.set(values, actor=actor, model=model))

    return router
