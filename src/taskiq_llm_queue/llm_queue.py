"""`LLMQueue`: the library's front door (plan §3.15).

Built from the environment (`from_env`: stores, registry, clients) or from parts. `start()`
publishes / loads the registry and runs the worker side in this process; `converse`, `chat` and
`embed` send calls through the model's queue; `*_sync` run on the direct path in a private
thread (E135); jobs, cancel, status, health and the registry are here too.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, TypeVar

import orjson
import redis.asyncio
from redis.credentials import CredentialProvider

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.clients import ClientPool, Providers
from taskiq_llm_queue.context import current_context
from taskiq_llm_queue.engine.envelope import Meta, Reply
from taskiq_llm_queue.engine.jobs import JobFn, JobRunner
from taskiq_llm_queue.engine.metrics import Metrics, sink_for
from taskiq_llm_queue.engine.offload import ResultOffload
from taskiq_llm_queue.engine.runtime import Engine, EngineSettings
from taskiq_llm_queue.engine.settings import InfraSettings
from taskiq_llm_queue.engine.store_client import store_client
from taskiq_llm_queue.engine.store_guard import store_op
from taskiq_llm_queue.errors import ConfigError, LLMQueueError
from taskiq_llm_queue.registry.store import RegistryStore
from taskiq_llm_queue.tuning import StoreOptions, Tuning

logger = get_logger(__name__)
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


def _client(
    url: str, secrets: Secrets | None, name: str, max_connections: int, options: StoreOptions
) -> redis.asyncio.Redis:
    """An async client (bytes replies) for a store URL; a password secret wins when present."""
    provider = _FreshPassword(secrets, name) if secrets is not None and secrets(name) else None
    return store_client(url, provider, max_connections=max_connections, options=options)


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
        self._sync_lock = threading.Lock()
        self.started = False
        self._stopped = False

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
        tuning: Tuning | None = None,
    ) -> LLMQueue:
        """Build from `LLMQ_*` (secret first when `secrets` is given, then the environment).

        `tuning` sets timeouts, pool sizes and loop pacing in code (`Tuning()` is the
        defaults); a bad value or combination raises `ConfigError` listing every problem.
        """
        settings = InfraSettings.from_env(environ, secrets=secrets)
        tuning = tuning or Tuning()
        if problems := tuning.problems(queue_wait_ms=settings.queue_wait_ms):
            raise ConfigError("invalid tuning:\n  " + "\n  ".join(problems))

        def build() -> Engine:
            cap = settings.store_max_connections
            state = _client(settings.store_url, secrets, "LLMQ_STORE_PASSWORD", cap, tuning.store)
            queue_store = (
                _client(
                    settings.queue_store_url,
                    secrets,
                    "LLMQ_QUEUE_STORE_PASSWORD",
                    cap,
                    tuning.store,
                )
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
                history_size=tuning.registry.history_size,
                edit_retries=tuning.registry.edit_retries,
            )
            pool_kwargs: dict[str, Any] = {
                "bedrock_endpoint_url": settings.bedrock_endpoint_url,
                "sts_endpoint_url": settings.sts_endpoint_url,
                "session_name": f"llmq-{settings.environment}-{worker_id or socket.gethostname()}",
                "options": tuning.bedrock,
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
                id_hash_salt=settings.id_hash_salt or "",
                cfg_reload_secs=settings.cfg_reload_secs,
                queue_failover_secs=settings.queue_failover_secs,
                queue_wait_replicas=settings.queue_wait_replicas,
                queue_wait_ms=settings.queue_wait_ms,
                debug_capture_allowed=settings.debug_capture_from_config,
                timings=tuning.engine,
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
        """Publish / load the registry; run the worker side unless `mode=caller`.

        A second `start()` does nothing; a stopped queue cannot start again (build a new one).
        """
        if self.started:
            return
        if self._stopped:
            raise LLMQueueError("this LLMQueue was stopped; build a new one to start again")
        mode = self.settings.mode if self.settings else "both"
        await self.engine.start(
            worker=mode in ("both", "worker"), caller=mode in ("both", "caller")
        )
        if mode in ("both", "worker"):
            await self.jobs.start()
        from taskiq_llm_queue.langchain import set_default_queue

        set_default_queue(self)
        self.started = True

    def pause_receiving(self) -> None:
        """Fail readiness and take no new calls or jobs; running work goes on (preStop hook)."""
        self.engine.pause_receiving()
        self.jobs.pause()

    async def stop(self, drain_secs: float | None = None) -> None:
        """Shut down in order (plan §3.18) and close every client; a second call does nothing.

        `drain_secs` (default `LLMQ_DRAIN_SECS`) bounds the whole wait: jobs first (they make
        calls), then running calls get what is left of it.
        """
        from taskiq_llm_queue.langchain import get_default_queue, set_default_queue

        if self._stopped:
            return
        self._stopped = True
        with contextlib.suppress(LLMQueueError):
            if get_default_queue() is self:
                set_default_queue(None)
        self.started = False
        loop = asyncio.get_running_loop()
        drain = self.engine.settings.drain_secs if drain_secs is None else drain_secs
        until = loop.time() + drain
        await self.jobs.stop(drain_secs=drain)
        await self.engine.stop(drain_secs=max(0.0, until - loop.time()))
        await asyncio.to_thread(self.close_sync)  # blocking: off the event loop
        await self.engine.aclose()

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

    async def converse_stream(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream a chat call's events as they arrive (plan §3.13).

        Bedrock models yield ConverseStream events, OpenAI-compatible ones chat completion
        chunks. A failure after events were yielded raises `CallFailed` with `partial_text`.
        """
        kwargs = self._call_kwargs(priority, **kw)
        async for event in self.engine.stream(model, request, **kwargs):
            yield event

    chat_stream = converse_stream

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
        if threading.current_thread().name == _SyncRunner.THREAD_NAME:
            raise LLMQueueError("a sync call from inside the sync runner would deadlock")
        _warn_if_loop_running()
        runner = self._sync
        if runner is None:
            if self._builder is None:
                raise LLMQueueError("sync calls need LLMQueue.from_env (a second, private engine)")
            with self._sync_lock:  # threads making their first sync call together build one
                if self._sync is None:
                    self._sync = _SyncRunner(self._builder)
                runner = self._sync
        return runner

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
            alias = engine.registry.current.registry.alias_of(model)
            return EmbedResult(vectors, Meta(served_model=alias))

        return self._sync_runner().run(run)

    def close_sync(self) -> None:
        """Stop the private engine of the sync calls (sync-only processes; `stop()` does too)."""
        with self._sync_lock:
            runner, self._sync = self._sync, None
        if runner is not None:
            runner.close()

    # ---------------------------------------------------------------- jobs and admin

    def register_job(self, name: str, fn: JobFn) -> None:
        """Allow this process to run job `name` (call before `start`)."""
        self.jobs.register(name, fn)

    @store_op
    async def send_job(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once)."""
        return await self.jobs.send(name, payload, job_id=job_id)

    @store_op
    async def cancel(self, request_id: str) -> None:
        """Cancel a call (waiting or running)."""
        await self.engine.cancel(request_id)

    @store_op
    async def status(self, model: str | None = None) -> dict[str, Any]:
        """Status of a model or of everything."""
        return await self.engine.status(model)

    @store_op
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

    def http_router(
        self,
        *,
        dependencies: Sequence[Any] = (),
        allow_writes: bool = False,
        actor: Callable[[Any], str] | None = None,
    ) -> Any:
        """A FastAPI router with status, requests, registry and health (extra `http`).

        Writes need `actor(request) -> str`, the authenticated name recorded in the audit.
        """
        from taskiq_llm_queue.engine.http import status_router

        return status_router(
            self.engine, dependencies=dependencies, allow_writes=allow_writes, actor=actor
        )


class _SyncRunner:
    """A private event loop in a thread with its own caller-only engine, for sync calls."""

    THREAD_NAME = "llmq-sync"

    def __init__(self, build: Callable[[], Engine]) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name=self.THREAD_NAME, daemon=True
        )
        self._thread.start()
        self._closing = False

        async def boot() -> Engine:
            engine = build()
            await engine.start(worker=False, caller=True)
            return engine

        try:
            self._engine: Engine = self._submit(boot()).result()
        except BaseException:
            self._loop.call_soon_threadsafe(self._loop.stop)  # no thread left behind
            self._thread.join(timeout=5)
            self._loop.close()
            raise

    def _submit(self, coro: Coroutine[Any, Any, _T]) -> Future[_T]:
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def run(self, fn: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run one call on the private loop and wait for it."""
        if self._closing:
            raise LLMQueueError("the sync runner is closed")
        return self._submit(fn(self._engine)).result()

    def close(self) -> None:
        """Stop the private engine and loop; calls still running are cancelled.

        Their callers get the error: before, a call running at close never returned.
        """
        self._closing = True

        async def stop() -> None:
            await self._engine.stop(drain_secs=1)
            current = asyncio.current_task()
            rest = [t for t in asyncio.all_tasks() if t is not current]
            for task in rest:
                task.cancel()
            await asyncio.gather(*rest, return_exceptions=True)
            await self._engine.aclose()

        with contextlib.suppress(Exception):
            self._submit(stop()).result(timeout=10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        if not self._thread.is_alive():
            self._loop.close()


_warned_loop = False


def _warn_if_loop_running() -> None:
    """Warn once when a sync call blocks a running event loop (use the async methods there)."""
    global _warned_loop
    if _warned_loop:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _warned_loop = True
    logger.warning(
        "sync_call_on_event_loop",
        hint="converse_sync / embed_sync block this event loop; await converse / embed instead",
    )
