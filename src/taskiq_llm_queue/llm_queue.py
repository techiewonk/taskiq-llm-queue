"""`LLMQueue`: the library's front door (plan §3.15).

Built from the environment (`from_env`: stores, registry, clients) or from parts. `start()`
publishes / loads the registry and runs the worker side in this process; `converse`, `chat` and
`embed` send calls through the model's queue; `*_sync` run on the direct path in a private
thread (E135); jobs, cancel, status, health and the registry are here too.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Callable, Coroutine, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, TypeVar

import orjson
import redis.asyncio
from redis.credentials import CredentialProvider

from taskiq_llm_queue.clients import ClientPool, Providers
from taskiq_llm_queue.context import current_context
from taskiq_llm_queue.engine.envelope import Meta, Reply
from taskiq_llm_queue.engine.jobs import JobFn, JobRunner
from taskiq_llm_queue.engine.metrics import Metrics, sink_for
from taskiq_llm_queue.engine.offload import ResultOffload
from taskiq_llm_queue.engine.runtime import Engine, EngineSettings
from taskiq_llm_queue.engine.settings import InfraSettings
from taskiq_llm_queue.errors import LLMQueueError
from taskiq_llm_queue.registry.store import RegistryStore

_T = TypeVar("_T")
Secrets = Callable[[str], "str | None"]


@dataclass(frozen=True, slots=True)
class EmbedResult:
    """Vectors (one per text, in order) and how the batches were served."""

    vectors: list[list[float]]
    meta: Meta


class _FreshPassword(CredentialProvider):
    """Reads the store password from the secret on every new connection (rotation, R42)."""

    def __init__(self, secrets: Secrets, name: str, username: str | None = None) -> None:
        self._secrets = secrets
        self._name = name
        self._username = username

    def get_credentials(self) -> tuple[str] | tuple[str, str]:
        password = self._secrets(self._name) or ""
        return (self._username, password) if self._username else (password,)


def _client(url: str, secrets: Secrets | None, name: str) -> redis.asyncio.Redis:
    """An async client (bytes replies) for a store URL; a password secret wins when present."""
    if secrets is not None and secrets(name):
        return redis.asyncio.Redis.from_url(url, credential_provider=_FreshPassword(secrets, name))
    return redis.asyncio.Redis.from_url(url)


class LLMQueue:
    """One process's LLM queue: caller, worker, jobs and admin over the stores."""

    def __init__(
        self,
        engine: Engine,
        *,
        settings: InfraSettings | None = None,
        builder: Callable[[], Engine] | None = None,
    ) -> None:
        """Wrap a built `Engine`; `builder` makes a second, caller-only engine for sync calls."""
        self.engine = engine
        self.settings = settings
        self.jobs = JobRunner(engine.queues, queue_prefix=engine.settings.queue_prefix)
        self._builder = builder
        self._sync: _SyncRunner | None = None
        self.started = False

    # ---------------------------------------------------------------- construction

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Secrets | None = None,
        metrics: Metrics | None = None,
        worker_id: str | None = None,
        credentials: Callable[[Any], Mapping[str, Any] | None] | None = None,
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
    ) -> LLMQueue:
        """Build from `LLMQ_*` (secret first when `secrets` is given, then the environment)."""
        settings = InfraSettings.from_env(environ, secrets=secrets)

        def build() -> Engine:
            state = _client(settings.store_url, secrets, "LLMQ_STORE_PASSWORD")
            queue_store = (
                _client(settings.queue_store_url, secrets, "LLMQ_QUEUE_STORE_PASSWORD")
                if settings.queue_store_url
                else None
            )
            registry = RegistryStore(
                state,
                settings.key_prefix,
                config=orjson.loads(settings.registry_document()),
                live_edit=settings.registry_live_edit,
                allowed_hosts=settings.base_url_allowed_hosts,
                audit_retention_days=settings.audit_retention_days,
            )
            pool_kwargs: dict[str, Any] = {
                "bedrock_endpoint_url": settings.bedrock_endpoint_url,
                "denied_hosts": (),
            }
            if credentials is not None:
                pool_kwargs["credentials"] = credentials
            if secrets is not None:
                pool_kwargs["secrets"] = secrets
            engine_settings = EngineSettings(
                queue_prefix=settings.queue_prefix,
                lease_secs=settings.lease_secs,
                result_ttl_secs=settings.result_ttl_secs,
                result_dedupe_secs=settings.result_dedupe_secs,
                status_ttl_secs=settings.status_ttl_secs,
                caller_ttl_secs=settings.caller_ttl_secs,
                drain_secs=settings.drain_secs,
                local_limit_cap=settings.local_limit_cap,
            )
            return Engine(
                state,
                registry,
                Providers(ClientPool(**pool_kwargs)),
                queue_store=queue_store,
                settings=engine_settings,
                worker_id=worker_id,
                metrics=metrics or sink_for(settings.metrics_sink),
                offload=offload,
                offload_bytes=offload_bytes,
            )

        return cls(build(), settings=settings, builder=build)

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Publish / load the registry; run the worker side unless `mode=caller`."""
        mode = self.settings.mode if self.settings else "both"
        await self.engine.start(
            worker=mode in ("both", "worker"), caller=mode in ("both", "caller")
        )
        if mode in ("both", "worker"):
            await self.jobs.start()
        from taskiq_llm_queue.langchain import set_default_queue

        set_default_queue(self)
        self.started = True

    async def stop(self) -> None:
        """Shut down in order (plan §3.18) and close the clients."""
        from taskiq_llm_queue.langchain import get_default_queue, set_default_queue

        with contextlib.suppress(LLMQueueError):
            if get_default_queue() is self:
                set_default_queue(None)
        self.started = False
        await self.jobs.stop()
        await self.engine.stop()
        await self.engine.providers.aclose()
        if self._sync is not None:
            self._sync.close()
        await self.engine.state.aclose()

    # ---------------------------------------------------------------- calls

    def _call_kwargs(self, priority: bool | None, **kw: Any) -> dict[str, Any]:
        ctx = current_context()
        out = {
            "priority": ctx.priority if priority is None else priority,
            "session_id": kw.pop("session_id", None) or ctx.session_id,
            "user_id": kw.pop("user_id", None) or ctx.user_id,
            "tenant_id": kw.pop("tenant_id", None) or ctx.tenant_id,
        }
        out.update({k: v for k, v in kw.items() if v is not None})
        return out

    async def call(
        self, model: str, params: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """Any operation of a model (`converse`, `chat`, `invoke_model`, `embeddings`)."""
        return await self.engine.call(model, params, **self._call_kwargs(priority, **kw))

    async def converse(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """One Bedrock Converse call (the reply's `response` is the Converse body)."""
        return await self.call(model, request, priority=priority, **kw)

    async def chat(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """One OpenAI-compatible chat completion."""
        return await self.call(model, request, priority=priority, **kw)

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        **kw: Any,
    ) -> EmbedResult:
        """One vector per text, in order (batched by the model's `embed_batch_size`)."""
        kwargs = self._call_kwargs(priority, **kw)
        vectors = await self.engine.embed(model, texts, input_type=input_type, **kwargs)
        alias = self.engine.registry.current.registry.alias_of(model)
        return EmbedResult(vectors, Meta(served_model=alias))

    # ---------------------------------------------------------------- sync (direct path)

    def _sync_runner(self) -> _SyncRunner:
        if self._sync is None:
            if self._builder is None:
                raise LLMQueueError("sync calls need LLMQueue.from_env (a second, private engine)")
            self._sync = _SyncRunner(self._builder)
        return self._sync

    def converse_sync(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """`converse` on the direct path from sync code (same budgets, no queue)."""
        kwargs = self._call_kwargs(priority, **kw)
        return self._sync_runner().run(
            lambda e: e.call(model, request, force_direct=True, **kwargs)
        )

    def chat_sync(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """`chat` on the direct path from sync code."""
        return self.converse_sync(model, request, priority=priority, **kw)

    def embed_sync(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        **kw: Any,
    ) -> EmbedResult:
        """`embed` on the direct path from sync code."""
        kwargs = self._call_kwargs(priority, **kw)

        async def run(engine: Engine) -> EmbedResult:
            vectors = await engine.embed(
                model, texts, input_type=input_type, force_direct=True, **kwargs
            )
            return EmbedResult(vectors, Meta(served_model=model))

        return self._sync_runner().run(run)

    # ---------------------------------------------------------------- jobs and admin

    def register_job(self, name: str, fn: JobFn) -> None:
        """Allow this process to run job `name` (call before `start`)."""
        self.jobs.register(name, fn)

    async def send_job(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once)."""
        return await self.jobs.send(name, payload, job_id=job_id)

    async def cancel(self, request_id: str) -> None:
        """Cancel a call (waiting or running)."""
        await self.engine.cancel(request_id)

    async def status(self, model: str | None = None) -> dict[str, Any]:
        """Status of a model or of everything."""
        return await self.engine.status(model)

    async def request_status(self, request_id: str) -> dict[str, Any]:
        """Status of one call."""
        return await self.engine.request_status(request_id)

    async def health(self) -> dict[str, Any]:
        """Liveness, readiness and the stores' state."""
        return await self.engine.health_check()

    @property
    def registry(self) -> RegistryStore:
        """The registry store (live edits, audit, rollback)."""
        return self.engine.registry

    def http_router(self, *, dependencies: Sequence[Any] = (), allow_writes: bool = False) -> Any:
        """A FastAPI router with status, requests, registry and health (extra `http`)."""
        from taskiq_llm_queue.engine.http import status_router

        return status_router(self.engine, dependencies=dependencies, allow_writes=allow_writes)


class _SyncRunner:
    """A private event loop in a thread with its own caller-only engine, for sync calls."""

    def __init__(self, build: Callable[[], Engine]) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="llmq-sync", daemon=True
        )
        self._thread.start()

        async def boot() -> Engine:
            engine = build()
            await engine.start(worker=False, caller=True)
            return engine

        self._engine: Engine = self._submit(boot()).result()

    def _submit(self, coro: Coroutine[Any, Any, _T]) -> Future[_T]:
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def run(self, fn: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run one call on the private loop and wait for it."""
        return self._submit(fn(self._engine)).result()

    def close(self) -> None:
        """Stop the private engine and loop."""

        async def stop() -> None:
            await self._engine.stop(drain_secs=1)
            await self._engine.providers.aclose()

        with contextlib.suppress(Exception):
            self._submit(stop()).result(timeout=10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
