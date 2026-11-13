"""The engine of one process: caller and worker over the registry, queues and admission.

`Engine.call` registers a call (duplicate ids attach to the first run), sends it to its model's
queue and waits for the result. The worker side reads each model's priority queue first, admits
calls with one store call each, holds a refused call (and stops reading its class) until it is
granted or expires, runs granted calls with the attempt timers, retries by the refund rules and
falls back to fallback regions and other models by the registry's rules (plan §3.3 to §3.18).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import random
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ResponseError
from redis.exceptions import ReadOnlyError as StoreReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.clients.pool import CredentialsUnavailable, route_for
from taskiq_llm_queue.clients.providers import Providers
from taskiq_llm_queue.clients.timing import AttemptTimeout, Timing
from taskiq_llm_queue.engine import hooks, inference, tokens
from taskiq_llm_queue.engine.admission import (
    Admission,
    Ask,
    Candidate,
    Decision,
    owner_token,
    position_of,
)
from taskiq_llm_queue.engine.envelope import Envelope, Meta, Reply, params_hash, parse
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.engine.health import Health
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.metrics import Metrics, NullSink
from taskiq_llm_queue.engine.offload import ResultOffload
from taskiq_llm_queue.engine.queue import (
    Message,
    NotReplicated,
    QueueFull,
    QueueSettings,
    StreamQueues,
)
from taskiq_llm_queue.engine.requests import RequestStore
from taskiq_llm_queue.engine.status import AlertEvaluator, StatusReader
from taskiq_llm_queue.engine.streaming import ChunkReader, ChunkWriter, StreamTooLarge
from taskiq_llm_queue.errors import ErrorKind, UnsupportedEnvelopeError
from taskiq_llm_queue.ids import new_id
from taskiq_llm_queue.providers.classify import is_target_error
from taskiq_llm_queue.tuning import EngineTimings

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.registry import Deployment, ResolvedRegistry
    from taskiq_llm_queue.registry.store import RegistryStore

logger = get_logger(__name__)
OPS = {("bedrock", "chat"): "converse", ("bedrock", "embedding"): "invoke_model"}


def _probe_params(provider: str, kind: str, model_id: str) -> dict[str, Any]:
    """The default probe request: one short word, one output token."""
    if kind == "embedding":
        if provider == "openai":
            return {"input": "ping"}
        if "cohere" in model_id:
            return {"body": {"texts": ["ping"], "input_type": "search_query"}}
        return {"body": {"inputText": "ping"}}
    if provider == "openai":
        return {"messages": [{"role": "user", "content": "ping"}], "max_tokens": 1}
    return {
        "messages": [{"role": "user", "content": [{"text": "ping"}]}],
        "inferenceConfig": {"maxTokens": 1},
    }


def _auth_error(code: str) -> bool:
    """Credential errors: drop the cached client so the secret is read again."""
    lowered = code.lower()
    return any(
        part in lowered
        for part in ("unrecognizedclient", "expiredtoken", "authentication", "invalid_api_key")
    )


@dataclass(frozen=True, slots=True)
class EngineSettings:
    """Process-level settings (plan §3.18 infrastructure table)."""

    queue_prefix: str = ""
    lease_secs: float = 60.0
    waiter_secs: float = 2.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 0
    status_ttl_secs: int = 1800
    caller_ttl_secs: int = 30
    poll_secs: float = 5.0  # safety-net result check (the wake-up is published with the result)
    sweep_secs: float = 10.0
    receive_batch: int = 10
    caller_margin_secs: float = 5.0
    local_limit_cap: int = 10
    drain_secs: float = 45.0
    id_hash_salt: str = ""  # LLMQ_ID_HASH_SALT: the key of every user / session id hash
    cfg_reload_secs: float = 30.0  # LLMQ_CFG_RELOAD_SECS: how often the registry is re-read
    queue_failover_secs: float = 5.0  # queue-store errors this long before calls run direct
    queue_wait_replicas: int = 0  # a send waits for this many replicas (0: off)
    queue_wait_ms: int = 100  # how long it waits for them
    debug_capture_allowed: bool = False  # LLMQ_DEBUG_CAPTURE_FROM_CONFIG (never in prod)
    timings: EngineTimings = field(default_factory=EngineTimings)  # `Tuning.engine`


# ARGV: key prefix, request ids... Reply: the 0-based indexes of the ids with a stored result.
_RESULTS_EXIST = """
local out = {}
for i = 2, #ARGV do
  if redis.call('EXISTS', ARGV[1] .. ':{r:' .. ARGV[i] .. '}:res') == 1 then
    table.insert(out, i - 2)
  end
end
return out
"""


def _refusal(exc: BaseException) -> str:
    """A caller-facing message for a store that answered with a refusal."""
    if isinstance(exc, OutOfMemoryError) or "OOM" in str(exc):
        return "the state store is out of memory"  # redis-py strips the "OOM" prefix
    return f"the state store refused the call: {exc}"


# READONLY: a failover left this connection on the old primary (now a replica).
STORE_ERRORS = (
    StoreConnectionError,
    StoreTimeoutError,
    StoreReadOnlyError,
    ConnectionError,
    OSError,
)


@dataclass(slots=True)
class _Held:
    """A call this process holds (waiting for budget, or running).

    `message` is None on the direct path (kill switch, `queue_pct`, queue-store failover); its
    position is then the store's time at registration.
    """

    message: Message | None
    envelope: Envelope
    first_seen: float
    position_ms: int
    tag: str
    grants: set[tuple[str, str]] = field(default_factory=set)
    meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    broken: bool = False
    cancelled: bool = False
    target_failed: list[tuple[str, str]] = field(default_factory=list)
    run: asyncio.Task[None] | None = None
    # Model-level request and estimate, built once per registry view (a deep copy of the
    # prompt each time: ~0.7 ms for 200 KB, and it was rebuilt in _ask, _decide and _attempt).
    charged: tuple[Any, str, tokens.Estimate] | None = None

    @property
    def position(self) -> float:
        """Queue position as a number (direct calls: their registration time)."""
        if self.message is not None:
            return position_of(self.message.id)
        return float(self.position_ms * 1000)


class _QueueDown(Exception):
    """The queue store refused a send while the state store works: run the call direct."""


class _SendNotReplicated(Exception):  # internal signal, never raised to callers
    """The send reached the primary but no replica in time: the caller runs it direct."""


class StoreClock:
    """The state store's clock, from one `TIME` read and the local monotonic clock."""

    def __init__(self) -> None:
        """No offset until `sync`."""
        self._offset = 0.0

    async def sync(self, client: redis.asyncio.Redis) -> None:
        """Measure the offset between the store's clock and this process."""
        before = time.monotonic()
        secs, micros = await client.time()
        after = time.monotonic()
        self._offset = (secs + micros / 1e6) - (before + after) / 2

    def now(self) -> float:
        """Seconds on the store's clock."""
        return time.monotonic() + self._offset


class Engine:
    """Caller and worker of one process (plan E72: workers run in the app pods)."""

    def __init__(
        self,
        state: redis.asyncio.Redis,
        registry: RegistryStore,
        providers: Providers,
        *,
        queue_store: redis.asyncio.Redis | None = None,
        settings: EngineSettings | None = None,
        worker_id: str | None = None,
        metrics: Metrics | None = None,
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
    ) -> None:
        """Wire the stores; nothing runs until `start`."""
        self.settings = settings or EngineSettings()
        self.keys = Keys(registry.prefix)
        self.state = state
        self.registry = registry
        self.providers = providers
        self._queue_store = queue_store
        self.queues = StreamQueues(queue_store or state, self.keys)
        self.requests = RequestStore(
            state,
            self.keys,
            result_ttl_secs=self.settings.result_ttl_secs,
            result_dedupe_secs=self.settings.result_dedupe_secs,
            status_ttl_secs=self.settings.status_ttl_secs,
            offload=offload,
            offload_bytes=offload_bytes,
        )
        self.admission = Admission(
            state,
            self.keys,
            lease_secs=self.settings.lease_secs,
            waiter_secs=self.settings.waiter_secs,
            timings=self.settings.timings,
        )
        self.worker_id = worker_id or new_id()[-14:]  # the random tail: the head is time
        self.clock = StoreClock()
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._held: dict[str, _Held] = {}
        self._held_class: set[tuple[str, str]] = set()
        self._slots: dict[str, asyncio.Semaphore] = {}
        self._waiters: dict[str, asyncio.Future[None]] = {}
        self._pubsub: Any = None
        self._cancels: Any = None
        self._stopping = False
        self._paused = False
        self._local_callers: dict[str, int] = {}  # request id -> coroutines waiting on it here
        self._wake: dict[str, asyncio.Event] = {}  # model -> wakes its receive loop
        self._beat_ok_at = time.monotonic()  # last heartbeat that reached the store
        self._store_trouble_at = -1e9  # monotonic time this process last saw the store fail
        self._results_exist = state.register_script(_RESULTS_EXIST)
        self.ready = False
        self._direct_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self.health = Health(state, self.keys)
        self.metrics: Metrics = metrics or NullSink()
        self.status_reader = StatusReader(state, self.keys, self.queues, self.settings.queue_prefix)
        self.alerts = AlertEvaluator(state, self.keys)
        # Credentials that failed in this process, per account and region: (account, region)
        # -> (until, backoff). Per region: a token can be refused in one region only (an
        # opt-in region such as ap-south-2 refuses v1 STS tokens), and keying by account took
        # every region of it down (scenario P5).
        self._bad_accounts: dict[tuple[str, str], tuple[float, float]] = {}
        self._idle = asyncio.Event()
        self._static: dict[str, asyncio.Semaphore] = {}
        self._rng = random.Random()
        self.chunks = ChunkReader(state, block_ms=self.settings.timings.stream_read_block_ms)

    # ================================================================ lifecycle

    async def start(self, *, worker: bool = True, caller: bool = True) -> None:
        """Boot the registry, then start the worker loops and the result reader."""
        await self.registry.boot()  # runs on its own config when the store is down
        try:
            await self.clock.sync(self.state)
            await self.state.set(self.keys.epoch, str(self.clock.now()), nx=True)
        except STORE_ERRORS as exc:
            # Start anyway (plan §3.18): calls follow each model's `store_down` rule and
            # the background loops keep retrying until the store answers.
            logger.error("state_store_unreachable_at_start", error=str(exc))
        if caller:
            self._pubsub = self.state.pubsub()
            self._tasks.append(asyncio.create_task(self._result_reader()))
            self._tasks.append(asyncio.create_task(self._result_sweep()))
        if worker:
            for alias in self.registry.current.registry.models:
                slots = int(self.registry.current.value(alias, "worker_slots"))
                self._slots[alias] = asyncio.Semaphore(slots)
                self._tasks.append(asyncio.create_task(self._model_loop(alias)))
            self._tasks.append(asyncio.create_task(self._sweeper()))
            self._tasks.append(asyncio.create_task(self._prober()))
            self._tasks.append(asyncio.create_task(self._alert_loop()))
            self._cancels = self.state.pubsub()
            self._tasks.append(asyncio.create_task(self._cancel_reader()))
            self._tasks.append(asyncio.create_task(self._warm_clients()))
        self._tasks.append(asyncio.create_task(self._heartbeat()))
        self._tasks.append(asyncio.create_task(self.registry.run(self.settings.cfg_reload_secs)))
        self.ready = True

    def pause_receiving(self) -> None:
        """Not ready, and take no new calls from the queues; running and held calls go on.

        The first step of a shutdown, for a preStop hook: the load balancer stops sending
        requests while this pod finishes its work. `stop()` follows.
        """
        self.ready = False
        self._paused = True

    async def stop(self, drain_secs: float | None = None) -> None:
        """Shut down in order (plan §3.18): not ready, stop receiving, finish running calls.

        Held calls go back to the queue; results keep being read for this pod's own callers
        until they finish or the drain ends; then everything stops.
        """
        drain = self.settings.drain_secs if drain_secs is None else drain_secs
        loop = asyncio.get_running_loop()
        until = loop.time() + drain
        self.ready = False
        self._stopping = True
        for held in list(self._held.values()):
            if not held.grants and held.run is None and held.message is not None:
                with contextlib.suppress(*STORE_ERRORS):
                    await self.queues.return_(held.message)
                self._drop(held)  # else the heartbeat hides it again for the whole drain
        if self._running:
            await asyncio.wait(set(self._running), timeout=max(0.0, until - loop.time()))
        if self._waiters:
            self._idle.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._idle.wait(), max(0.0, until - loop.time()))
        for task in [*self._tasks, *self._running]:
            task.cancel()
        for task in [*self._tasks, *self._running]:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await self.chunks.aclose()
        for pubsub in (self._pubsub, self._cancels):
            if pubsub is not None:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()

    async def aclose(self) -> None:
        """Close the provider clients and both store clients (after `stop()`)."""
        await self.providers.aclose()
        if self._queue_store is not None:
            await self._queue_store.aclose()
        await self.state.aclose()

    # ================================================================ caller

    def _queue_name(self, view: ResolvedRegistry, alias: str, priority: bool) -> str:
        name = self.settings.queue_prefix + view.queue_name(alias)
        return f"{name}-priority" if priority else name

    async def call(
        self,
        model: str,
        params: Mapping[str, Any],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        max_wait_secs: float | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        allow_fallback: str = "all",
        allowed_regions: Sequence[str] | None = None,
        allowed_accounts: Sequence[str] | None = None,
        force_direct: bool = False,
        stream: bool = False,
        stream_id: str | None = None,
    ) -> Reply:
        """Run one call through the queue and return its reply; `CallFailed` on failure.

        `force_direct` runs it in this process (sync calls), with the same admission.
        `stream` has the worker write events to the chunk stream of `stream_id` (`stream`).
        """
        view = self.registry.current
        alias = view.registry.alias_of(model)
        if view.effective(alias, "enabled").level == "model" and not view.value(alias, "enabled"):
            rule = view.value(alias, "model_fallback")
            if rule and allow_fallback == "all" and rule["models"]:
                alias = rule["models"][0]["model"]
            else:
                raise CallFailed("model_disabled", f"{alias} is disabled")
        entry = view.registry.models[alias]
        op = OPS.get((entry.provider, entry.type), "chat" if entry.type == "chat" else "embeddings")
        timeout = float(timeout_secs or view.value(alias, "timeout_secs"))
        timeout = min(timeout, float(view.value(alias, "timeout_max_secs")))
        rid = request_id or stream_id or new_id()
        envelope = Envelope(
            request_id=rid,
            op=op,
            model=alias,
            params=dict(params),
            priority=priority,
            caller=self.worker_id,
            timeout_secs=timeout,
            max_wait_secs=max_wait_secs or view.value(alias, "max_wait_secs"),
            allow_fallback=allow_fallback,
            allowed_regions=tuple(allowed_regions) if allowed_regions else None,
            allowed_accounts=tuple(allowed_accounts) if allowed_accounts else None,
            session_id=session_id,
            user_id=user_id,
            tenant_id=tenant_id,
            app_request_id=request_id is not None,
            stream=stream,
        )
        hook_name = view.value(alias, "admission")
        hook = hooks.get("admission", hook_name)
        if hook is not None:
            ctx = hooks.CallContext(alias, rid, priority, session_id, user_id, tenant_id)
            verdict = hooks.call_safely("admission", hook_name, hook, ctx, default=None)
            if isinstance(verdict, hooks.Reject):
                raise CallFailed("rejected", verdict.reason, request_id=rid)
        direct = force_direct or self._direct_route(view, alias, rid, user_id)
        self._local_callers[rid] = self._local_callers.get(rid, 0) + 1
        # One deadline for the whole call, whatever path it takes: a store outage sent calls
        # down the store_down path with a fresh timeout, and the soak saw 135 answers come
        # after their deadline (20 s store block, 30 s calls).
        deadline = asyncio.get_running_loop().time() + timeout + self.settings.caller_margin_secs
        try:
            async with asyncio.timeout_at(deadline):
                try:
                    return await self._call_once(
                        view, alias, envelope, params, priority, timeout, request_id, direct
                    )
                except _QueueDown:
                    return await self._call_once(
                        view, alias, envelope, params, priority, timeout, request_id, True
                    )
                except STORE_ERRORS as exc:
                    if stream:  # chunks travel through the state store
                        message = "the state store is unreachable"
                        raise CallFailed("queue_unavailable", message) from exc
                    return await self._store_down(view, alias, envelope, exc, deadline)
                except ResponseError as exc:
                    # The store answered but refused (out of memory, a script error): still one
                    # error family for callers. A refused registration raised redis-py's
                    # OutOfMemoryError straight to the app (scenario S9).
                    raise CallFailed("queue_unavailable", _refusal(exc), request_id=rid) from exc
        except TimeoutError as exc:
            raise CallFailed("deadline_exceeded", "no reply in time", request_id=rid) from exc
        except asyncio.CancelledError:
            # The awaiting task was cancelled: cancel the call unless others wait on its id,
            # in other processes or in this one (they share this process's caller entry).
            with contextlib.suppress(Exception):
                callers = await asyncio.shield(self.requests.callers(rid))
                alone_here = self._local_callers.get(rid, 0) <= 1
                if alone_here and set(callers) <= {self.worker_id}:
                    await asyncio.shield(self.cancel(rid))
            raise
        finally:
            left = self._local_callers.get(rid, 1) - 1
            if left:
                self._local_callers[rid] = left
            else:
                self._local_callers.pop(rid, None)

    async def stream(
        self, model: str, params: Mapping[str, Any], **call: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Run a chat call and yield the provider's events as they arrive (plan §3.13, F6).

        Takes `call`'s options. The events are ConverseStream events (Bedrock) or chat
        completion chunks (OpenAI-compatible). A failure after events were yielded raises
        `CallFailed` with `partial_text`; no switch to another deployment happens then.
        """
        view = self.registry.current
        alias = view.registry.alias_of(model)
        idle = float(view.value(alias, "stream_idle_secs"))
        rid = call.pop("request_id", None) or new_id()
        key = self.keys.request(rid, "chunks")
        queue = self.chunks.open(key)
        task = asyncio.create_task(self.call(model, params, stream=True, stream_id=rid, **call))
        expected, started = 1, False
        try:
            while True:
                getter = asyncio.ensure_future(queue.get())
                finished = task.done()
                done, _ = await asyncio.wait(
                    {getter} if finished else {getter, task},
                    timeout=idle if started or finished else None,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if getter not in done:
                    getter.cancel()
                    if task in done:
                        if not started and not await self.state.xlen(key):
                            task.result()  # raises the call's failure (nothing was streamed)
                            return  # a finished call whose chunks expired: nothing to replay
                        continue  # its last entries are still on the way
                    if finished:
                        task.result()
                        raise CallFailed(
                            "worker_lost", "the stream ended without its end entry", request_id=rid
                        )
                    raise CallFailed(
                        "deadline_exceeded",
                        f"no chunk for {idle:g} s (stream_idle_secs)",
                        request_id=rid,
                    )
                entry = getter.result()
                if entry["seq"] != expected:
                    raise CallFailed(
                        "worker_lost", f"gap in the stream at {expected}", request_id=rid
                    )
                expected += 1
                started = True
                if entry["t"] == "chunk":
                    for event in entry["d"]:
                        yield event
                elif entry["t"] == "end":
                    await task
                    return
                else:
                    data = entry["d"]
                    raise CallFailed(
                        data.get("kind", "provider_error"),
                        data.get("message", ""),
                        partial_text=data.get("partial") or None,
                        request_id=rid,
                    )
        finally:
            self.chunks.close(key)
            if not task.done():
                task.cancel()  # the call's own handler cancels the request
            with contextlib.suppress(BaseException):
                await task

    def hash_id(self, value: str) -> str:
        """Keyed hash (HMAC-SHA256, `LLMQ_ID_HASH_SALT`) of a user or session id, 16 hex chars.

        Every id outside the message itself is stored and logged only as this (plan §3.17):
        status records, sticky keys, logs and `routing.allow_users`. Unkeyed, a low-entropy id
        (a client code, a phone number) is brute-forced back from the hash.
        """
        key = self.settings.id_hash_salt.encode()
        return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:16]

    def _direct_route(
        self, view: ResolvedRegistry, alias: str, rid: str, user_id: str | None
    ) -> bool:
        """True when this call runs direct: kill switch, or outside `queue_pct` (canary)."""
        routing = view.registry.routing
        if user_id and self.hash_id(user_id) in routing.allow_users:
            return False
        if routing.kill_switch:
            return True
        pct = int(view.value(alias, "queue_pct"))
        return pct < 100 and int(hashlib.sha256(rid.encode()).hexdigest()[:8], 16) % 100 >= pct

    async def _call_once(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        params: Mapping[str, Any],
        priority: bool,
        call_secs: float,
        request_id: str | None,
        direct: bool,
    ) -> Reply:
        rid = envelope.request_id
        user_id, session_id = envelope.user_id, envelope.session_id
        user_hash = self.hash_id(user_id or session_id or "")
        found = await self.requests.register(
            rid,
            caller=self.worker_id,
            user_hash=user_hash,
            params_hash=params_hash({"m": alias, "p": params}),
            model=alias,
            priority=priority,
            app_id=request_id is not None,
        )
        if found.outcome == "conflict":
            raise CallFailed(
                "request_id_conflict", "the id was used with other params", request_id=rid
            )
        if found.outcome == "expired":
            raise CallFailed("result_expired", "the id finished earlier", request_id=rid)
        waiter = self._waiters.setdefault(rid, asyncio.get_running_loop().create_future())
        loop = asyncio.get_running_loop()
        until = loop.time() + call_secs + self.settings.caller_margin_secs
        outcome = found.outcome
        try:
            while True:
                if outcome in ("new", "takeover"):
                    if direct:
                        await self.requests.confirm(rid, keep_secs=call_secs)
                        task = asyncio.create_task(self._direct(envelope))
                        self._running.add(task)
                        task.add_done_callback(self._running.discard)
                    else:
                        try:
                            await self._send(rid, envelope, view, alias, priority, call_secs)
                        except _SendNotReplicated:
                            await self.requests.confirm(rid, keep_secs=call_secs)
                            task = asyncio.create_task(self._direct(envelope))
                            self._running.add(task)
                            task.add_done_callback(self._running.discard)
                reply = await self._wait_result(
                    rid, waiter, until - loop.time(), recheck=outcome == "attached"
                )
                if reply is not None:
                    return reply
                # The send this caller attached to never happened: register again.
                again = await self.requests.register(
                    rid,
                    caller=self.worker_id,
                    user_hash=user_hash,
                    params_hash=params_hash({"m": alias, "p": params}),
                    model=alias,
                    priority=priority,
                    app_id=request_id is not None,
                )
                outcome = again.outcome
                if outcome == "conflict":
                    raise CallFailed("request_id_conflict", "the id was used with other params")
        finally:
            # Other coroutines here may wait on the same id and share its wake-up: removing
            # it left them asleep until their deadline's last read (one waiter cancelled).
            if self._local_callers.get(rid, 1) <= 1:
                self._waiters.pop(rid, None)
            if not self._waiters:
                self._idle.set()

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        input_type: str | None = None,
        **call: Any,
    ) -> list[list[float]]:
        """One vector per text, in order (batched by the model's `embed_batch_size`)."""
        from taskiq_llm_queue.engine.embed import embed

        return await embed(
            self,
            model,
            texts,
            priority=priority,
            timeout_secs=timeout_secs,
            input_type=input_type,
            **call,
        )

    async def _send(
        self,
        rid: str,
        envelope: Envelope,
        view: ResolvedRegistry,
        alias: str,
        priority: bool,
        keep_secs: float,
    ) -> None:
        """Put the message on its queue and confirm the record; undo the record on failure.

        A separate queue store that keeps failing for `queue_failover_secs` sends the call
        down the direct path; shorter blips are retried. With `queue_wait_replicas`, a send
        that no replica acknowledged in time is a failed send (plan B23) and the call runs
        direct.
        """
        settings = QueueSettings(max_waiting=int(view.value(alias, "max_waiting")))
        loop = asyncio.get_running_loop()
        give_up = loop.time() + self.settings.queue_failover_secs
        body = envelope.dumps()
        while True:
            try:
                await self.queues.send(
                    self._queue_name(view, alias, priority),
                    body,
                    settings,
                    wait_replicas=self.settings.queue_wait_replicas,
                    wait_ms=self.settings.queue_wait_ms,
                )
                break
            except QueueFull as exc:
                await self.requests.abandon(rid, caller=self.worker_id)
                raise CallFailed("queue_full", str(exc), retry_after_secs=5) from exc
            except NotReplicated as exc:
                # A failed send (plan B23): the call runs direct now, under this same record
                # (re-registering raced the unreplicated copy's claim into a conflict). Not
                # re-sent: each try would add a copy; the copy meets a claimed job and stops.
                logger.warning("send_not_replicated", request_id=rid, error=str(exc))
                raise _SendNotReplicated from exc
            except ResponseError as exc:
                await self.requests.abandon(rid, caller=self.worker_id)
                if isinstance(exc, OutOfMemoryError) or "OOM" in str(exc):
                    raise CallFailed("queue_unavailable", _refusal(exc)) from exc
                raise
            except STORE_ERRORS as exc:
                if not self.queues_shared and loop.time() < give_up:
                    await asyncio.sleep(0.25)
                    continue
                await self.requests.abandon(rid, caller=self.worker_id)
                if self.queues_shared:
                    raise
                raise _QueueDown from exc
            except BaseException:
                await self.requests.abandon(rid, caller=self.worker_id)
                raise
        try:
            await self.requests.confirm(rid, keep_secs=keep_secs)
        except STORE_ERRORS as exc:  # the message is on the queue: never run it a second time
            logger.warning("confirm_failed", request_id=rid, error=str(exc))

    @property
    def queues_shared(self) -> bool:
        """True when queues live in the state store (one store for both)."""
        return self._queue_store is None

    async def cancel(self, request_id: str) -> None:
        """Cancel a call: a waiting one never starts, a running one is stopped."""
        await self.state.set(
            self.keys.request(request_id, "cancel"), "1", ex=self.settings.status_ttl_secs
        )
        await self.state.publish(self.keys.cancel_channel, request_id)

    async def _cancel_reader(self) -> None:
        """Stop running attempts whose call was cancelled (by any process)."""
        subscribed = False
        while True:
            try:
                if not subscribed:
                    await self._cancels.subscribe(self.keys.cancel_channel)
                    subscribed = True
                message = await self._cancels.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except Exception as exc:
                logger.warning("cancel_reader_error", error=str(exc))
                subscribed = False
                with contextlib.suppress(Exception):
                    await self._cancels.reset()
                await asyncio.sleep(1)
                continue
            if not message:
                continue
            data = message["data"]
            rid = data.decode() if isinstance(data, bytes) else str(data)
            held = self._held.get(rid)
            if held is not None and held.run is not None and not held.run.done():
                held.cancelled = True
                held.run.cancel()

    async def _store_down(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        exc: BaseException,
        deadline: float,
    ) -> Reply:
        """The state store is unreachable: the model's `store_down` rule (plan §3.11).

        `deadline` is the call's own (loop time, margin included): `pause` waits only until
        then, never a fresh timeout from the moment the store failed.
        """
        mode = view.value(alias, "store_down")
        logger.warning("store_down", model=alias, mode=mode, error=str(exc))
        if mode == "static_cap":
            return await self._static_cap(view, alias, envelope)
        if mode == "pause":
            loop = asyncio.get_running_loop()
            until = deadline - self.settings.caller_margin_secs
            while loop.time() < until:
                await asyncio.sleep(1.0)
                try:
                    await self.state.ping()
                except STORE_ERRORS:
                    continue
                return await self.call(
                    alias,
                    envelope.params or {},
                    priority=envelope.priority,
                    timeout_secs=max(1.0, until - loop.time()),
                    request_id=envelope.request_id if envelope.app_request_id else None,
                    session_id=envelope.session_id,
                    user_id=envelope.user_id,
                    tenant_id=envelope.tenant_id,
                )
        raise CallFailed("queue_unavailable", "the state store is unreachable") from exc

    async def _static_cap(self, view: ResolvedRegistry, alias: str, envelope: Envelope) -> Reply:
        """Run without the store: at most `local_limit_cap` calls per process and model."""
        entry = view.registry.models[alias]
        cap = self._static.setdefault(alias, asyncio.Semaphore(self.settings.local_limit_cap))
        provider = self.providers.get(entry.provider)
        error: BaseException | None = None
        async with cap:
            for dep in view.deployments(alias, "confirmed"):
                if not view.value(alias, "enabled", dep.key) or view.outside_residency(dep):
                    continue
                route = route_for(view, dep)
                params = self._params(view, alias, envelope, dep.key)
                timing = Timing(
                    float(view.value(alias, "attempt_timeout_secs", dep.key)),
                    float(view.value(alias, "stream_idle_secs")),
                )
                try:
                    result = await provider.call(envelope.op, params, route, timing)
                except Exception as exc:
                    error = exc
                    continue
                meta = Meta(served_model=alias, deployment=dep.key, attempts=1)
                return Reply(
                    request_id=envelope.request_id, ok=True, response=result.response, meta=meta
                )
        raise CallFailed("provider_error", str(error or "no usable deployment"))

    async def _direct(self, env: Envelope) -> None:
        """Run a call in this process (no queue), with the same admission and waiter order.

        One waiter per process, model and class registers in the store; the others wait here.
        """
        held = _Held(
            message=None,
            envelope=env,
            first_seen=self.clock.now(),
            position_ms=int(self.clock.now() * 1000),
            tag=f"d{new_id()[-10:]}",  # random tail (a v7 id starts with the time)
        )
        self._held[env.request_id] = held
        alias = env.model
        cls = "p" if env.priority else "n"
        lock = self._direct_locks.setdefault((alias, cls), asyncio.Lock())
        try:
            async with lock:
                decision = await self._direct_wait(held)
            if decision is not None:
                await self._run(held, decision, 1, alias)
        finally:
            if not held.grants:
                self._drop(held)

    async def _direct_wait(self, held: _Held) -> Decision | None:
        """Admission for a direct call until granted (the decision) or settled (None)."""
        env = held.envelope
        alias = env.model
        while True:
            view = self.registry.current
            now = self.clock.now()
            if now >= self._deadline(held):
                await self._finish_call(
                    held, "rate_limited", "no budget before the deadline", final="expired"
                )
                return None
            waited = now - held.position_ms / 1000.0
            if env.max_wait_secs is not None and waited >= env.max_wait_secs:
                await self._finish_call(
                    held, "rate_limited", "max_wait_secs passed", final="expired"
                )
                return None
            decision = await self._decide(view, alias, held, 1, frozenset())
            if decision is None:
                return None
            if decision.kind == "granted":
                return decision
            if await self._model_fallback(view, alias, held, 1, waited):
                return None
            await asyncio.sleep(min(max(decision.wait_secs, 0.01), 1.0))

    def _rearm(self, rid: str, done: asyncio.Future[None]) -> asyncio.Future[None]:
        """The id's next wake-up after `done` fired: made by the first waiter to see it, shared.

        Each waiter making its own replaced the others', so all but the last slept until a
        recheck (two coroutines waiting on one id in one process).
        """
        current = self._waiters.get(rid)
        if current is None or current is done:
            current = asyncio.get_running_loop().create_future()
            self._waiters[rid] = current
        return current

    async def _wait_result(
        self, rid: str, waiter: asyncio.Future[None], secs: float, *, recheck: bool = False
    ) -> Reply | None:
        """The reply; None (only with `recheck`) when the call's record disappeared."""
        loop = asyncio.get_running_loop()
        until = loop.time() + secs
        if not recheck:
            # Just sent: the reply cannot be there yet. Wait for the wake-up (or one poll)
            # before the first read; reading at once cost a round trip on every call.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(waiter), min(secs, self.settings.poll_secs))
            if waiter.done():
                waiter = self._rearm(rid, waiter)
        while True:
            try:
                reply = await self.requests.take_result(rid)
                gone = reply is None and recheck and not await self.requests.job(rid)
            except STORE_ERRORS as exc:
                # The call is already sent or running: wait for the store, never re-run it
                # on the store_down path (that ran it twice, once outside the budget).
                if until - loop.time() <= 0:
                    raise CallFailed(
                        "deadline_exceeded", "the store did not answer in time", request_id=rid
                    ) from exc
                await asyncio.sleep(min(0.5, max(0.0, until - loop.time())))
                continue
            if gone:
                return None
            if reply is not None:
                if reply.ok:
                    return reply
                raise CallFailed(
                    reply.error or "provider_error",
                    reply.message,
                    retry_after_secs=reply.retry_after_secs,
                    partial_text=reply.partial_text,
                    request_id=rid,
                )
            left = until - loop.time()
            if left <= 0:
                raise CallFailed("deadline_exceeded", "no reply in time", request_id=rid)
            # The wake-up comes by pub/sub (published with the result), or from this process's
            # one result sweep (`_result_sweep`); a read per waiting caller made store load
            # grow with the backlog (B56). The timeout is only a last-resort recheck.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.shield(waiter), min(left, self.settings.timings.result_recheck_secs)
                )
            if waiter.done():
                waiter = self._rearm(rid, waiter)

    async def _warm_clients(self) -> None:
        """Create every Bedrock deployment's client now, not on its first call.

        A client takes about a second to build (measured: the first call of a fresh worker took
        1,032 ms against ~140 ms after; B92 asks for <= 500 ms of first-call overhead).
        Failures are ignored here: a call meets them again, and handles them.
        """
        view = self.registry.current
        for alias, entry in view.registry.models.items():
            if entry.provider != "bedrock":
                continue
            for deployment in view.deployments(alias):
                with contextlib.suppress(Exception):
                    await self.providers.pool.bedrock(route_for(view, deployment))

    async def _result_sweep(self) -> None:
        """Every `poll_secs`, one store call finds results of this process's waiting callers.

        The safety net behind the pub/sub wake-up: one script per process per period, however
        many calls wait (a read per waiting caller grew with the backlog, plan B56).
        """
        while True:
            await asyncio.sleep(self.settings.poll_secs)
            waiting = [rid for rid, w in self._waiters.items() if not w.done()]
            for start in range(0, len(waiting), 500):
                chunk = waiting[start : start + 500]
                try:
                    found = await self._results_exist([], [self.keys.prefix, *chunk])
                except Exception as exc:  # the sweep must survive store hiccups
                    logger.warning("result_sweep_failed", error=str(exc))
                    break
                for index in found or ():
                    waiter = self._waiters.get(chunk[int(index)])
                    if waiter is not None and not waiter.done():
                        waiter.set_result(None)

    async def _result_reader(self) -> None:
        """Wake waiting callers when their result is published (re-subscribes after errors)."""
        subscribed = False
        while True:
            try:
                if not subscribed:
                    await self._pubsub.subscribe(self.keys.done_channel(self.worker_id))
                    subscribed = True
                    # Results published while unsubscribed were missed: every waiter re-reads.
                    for pending in list(self._waiters.values()):
                        if not pending.done():
                            pending.set_result(None)
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except Exception as exc:  # the reader must survive store hiccups
                logger.warning("result_reader_error", error=str(exc))
                subscribed = False
                with contextlib.suppress(Exception):
                    await self._pubsub.reset()
                await asyncio.sleep(1)
                continue
            if not message:
                continue
            rid = (
                message["data"].decode()
                if isinstance(message["data"], bytes)
                else str(message["data"])
            )
            waiter = self._waiters.get(rid)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)

    # ================================================================ worker: receiving

    async def _model_loop(self, alias: str) -> None:
        """Receive this model's calls (priority first) and admit them in order.

        Idle, it blocks on its queues' doorbell (a send wakes one worker at once) for 50 ms at a
        time, then 1 s after a second with nothing: polling idle loops made ~96 receive
        scripts/s per pod with 4 models (measured); a busy loop never waits.
        """
        empty = 0
        wake = self._wake.setdefault(alias, asyncio.Event())
        while not self._stopping:
            if self._paused:
                await asyncio.sleep(0.2)
                continue
            try:
                got = await self._receive_once(alias)
            except Exception as exc:  # a store blip must not end the loop
                logger.warning("receive_failed", model=alias, error=str(exc))
                if isinstance(exc, STORE_ERRORS):
                    # Not only the heartbeat (every lease/3): a block between two beats went
                    # unseen, and callers whose keys lapsed in it were dropped as gone (soak).
                    self._store_trouble_at = time.monotonic()
                await asyncio.sleep(1)
                continue
            held = (alias, "p") in self._held_class or (alias, "n") in self._held_class
            # Idle only when the queues were empty, not when a class waits on its held call.
            empty = 0 if got or held else empty + 1
            if not got:
                pace = self.settings.timings
                pause = (
                    pace.idle_poll_secs if empty < pace.idle_after_empty else pace.idle_slow_secs
                )
                if held:  # a held call ending (granted or settled) wakes it at once
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(wake.wait(), pause)
                else:  # a send rings the doorbell: one idle worker wakes per call
                    view = self.registry.current
                    names = [self._queue_name(view, alias, p) for p in (True, False)]
                    try:
                        await self.queues.wait_bell(names, pause)
                    except STORE_ERRORS:
                        self._store_trouble_at = time.monotonic()  # (was swallowed unseen)
                wake.clear()

    async def _receive_once(self, alias: str) -> bool:
        view = self.registry.current
        if alias not in view.registry.models:
            await asyncio.sleep(1)
            return False
        slots = self._slots[alias]
        free = slots._value  # free worker slots of this model, read only
        if free <= 0:
            await asyncio.sleep(0.02)
            return False
        settings = self._queue_settings(view, alias)
        got_any = False
        for priority in (True, False):
            cls = "p" if priority else "n"
            if (alias, cls) in self._held_class:
                continue
            name = self._queue_name(view, alias, priority)
            messages, dead = await self.queues.receive(
                name, min(free, self.settings.receive_batch), settings
            )
            for letter in dead:
                await self._dead_lettered(letter.body)
            if not messages:
                continue
            got_any = True
            for index, message in enumerate(messages):
                try:
                    held = await self._admit_new(alias, message)
                except Exception:
                    for rest in messages[index + 1 :]:  # not seen: back to the queue now
                        with contextlib.suppress(Exception):
                            await self.queues.return_(rest)
                    raise
                if held:
                    for rest in messages[index + 1 :]:
                        await self.queues.return_(rest)
                    break
            if got_any:
                break
        return got_any

    def _queue_settings(self, view: ResolvedRegistry, alias: str) -> QueueSettings:
        queue = view.value(alias, "queue")
        return QueueSettings(
            visibility_secs=float(queue["visibility_secs"]),
            max_receives=int(queue["max_receives"]),
            retention_secs=int(queue["retention_secs"]),
            dlq_retention_secs=int(queue["dlq_retention_secs"]),
            max_waiting=int(view.value(alias, "max_waiting")),
        )

    async def _dead_lettered(self, body: bytes) -> None:
        try:
            env = parse(body)
        except Exception:
            return
        await self._reply(env, "dead_lettered", "the call failed too many times", final="failed")

    async def _admit_new(self, alias: str, message: Message) -> bool:
        """Admit a newly received message; True when it is now held (its class stops)."""
        try:
            env = parse(message.body)
        except UnsupportedEnvelopeError:
            await self.queues.return_(message, delay_secs=5)
            return False
        except Exception as exc:
            logger.warning("bad_message", queue=message.queue, error=str(exc))
            await self.queues.fail(message, QueueSettings())
            return False
        held = _Held(
            message,
            env,
            first_seen=self.clock.now(),
            position_ms=message.position_ms,
            tag=message.id,
        )
        self._held[env.request_id] = held
        try:
            if not await self._caller_alive(env):
                # A warning: a dropped call is worth seeing (the soak and scenario S5 found
                # calls dropped wrongly, and INFO logs alone did not show it).
                logger.warning("caller_gone", request_id=env.request_id, model=env.model)
                await self._finish_call(held, "cancelled", "caller_gone", final="cancelled")
                return False
            return await self._try(held, attempt=1)
        except Exception:
            await self._release(held)
            raise

    async def _caller_alive(self, env: Envelope) -> bool:
        """False when every caller of this call is gone (plan §3.18 R24).

        After a store outage the callers' liveness keys may have expired only because nobody
        could renew them: for a caller TTL and a heartbeat after this process last saw the
        store fail, callers count as alive (B95: a 45 s block dropped 429 of 500 calls).

        Judged from this process's own heartbeat too: a half-open store hung every command for
        the read timeout, the doorbell wait swallowed its error, and calls were dropped the
        moment the store came back (scenario S5). A heartbeat that is late or just recovered
        from a gap means the store was out of reach here as well, so a lapsed key proves
        nothing.
        """
        beat = self.settings.lease_secs / 3
        grace = self.settings.caller_ttl_secs + beat + 5
        now = time.monotonic()
        if now - self._store_trouble_at < grace or now - self._beat_ok_at > 2 * beat + 1:
            return True
        try:
            return await self.requests.callers_alive(
                env.request_id, prefix=self.keys.prefix, caller_ttl=self.settings.caller_ttl_secs
            )
        except STORE_ERRORS:
            self._store_trouble_at = time.monotonic()
            return True

    # ================================================================ worker: admission

    def _deadline(self, held: _Held) -> float:
        return held.position_ms / 1000.0 + held.envelope.timeout_secs

    def _candidates(
        self,
        view: ResolvedRegistry,
        alias: str,
        env: Envelope,
        age: float,
        failed: frozenset[str],
        cause: str,
    ) -> list[Candidate]:
        deps: list[Deployment] = list(view.deployments(alias))
        if env.allowed_regions:
            deps = [d for d in deps if d.region in env.allowed_regions]
        if env.allowed_accounts:
            deps = [d for d in deps if d.account in env.allowed_accounts]
        fallback_ok = self._may_spill(view, alias, env, age, cause, "fallback")
        now = time.monotonic()
        deps = [
            d for d in deps if self._bad_accounts.get((d.account, d.region), (0.0, 0.0))[0] <= now
        ]
        out = []
        for dep in deps:
            if dep.group == "fallback" and not fallback_ok:
                continue
            out.append(Candidate(dep.key, dep.group, last=dep.key in failed))  # type: ignore[arg-type]
        return out

    def _ctx(self, alias: str, held: _Held, waited: float) -> hooks.CallContext:
        env = held.envelope
        return hooks.CallContext(
            alias, env.request_id, env.priority, env.session_id, env.user_id, env.tenant_id, waited
        )

    def _may_spill(
        self,
        view: ResolvedRegistry,
        alias: str,
        env: Envelope,
        age: float,
        cause: str,
        rule_name: str,
    ) -> bool:
        """Whether a call may use fallback regions / models now: rule or `fallback_policy`."""
        allowed = ("all", "regions") if rule_name == "fallback" else ("all",)
        rule = view.value(alias, rule_name)
        if env.allow_fallback not in allowed or not rule:
            return False
        policy_name = view.value(alias, "fallback_policy")
        policy = hooks.get("fallback_policy", policy_name)
        if policy is not None:
            ctx = hooks.CallContext(
                alias, env.request_id, env.priority, env.session_id, env.user_id, env.tenant_id, age
            )
            default = cause in rule["on"] and age >= float(rule["after_secs"])
            return bool(
                hooks.call_safely(
                    "fallback_policy", policy_name, policy, cause, age, rule, ctx, default=default
                )
            )
        return cause in rule["on"] and age >= float(rule["after_secs"])

    def _ask(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        failed: frozenset[str],
        *,
        foreign_from: str | None = None,
        require_pct: int = 0,
    ) -> Ask | None:
        env = held.envelope
        age = self.clock.now() - held.position_ms / 1000.0
        cause = "error" if failed else ("broken" if held.broken else "exhausted")
        candidates = self._candidates(view, alias, env, age, failed, cause)
        if not candidates:
            return None
        entry = view.registry.models[alias]
        estimate = self._charged(view, alias, held)
        strategy = view.value(alias, "balance")
        if env.priority and view.value(alias, "balance_priority"):
            strategy = view.value(alias, "balance_priority")
        if str(strategy).startswith("custom:"):
            candidates = self._custom_order(view, alias, held, candidates, str(strategy)[7:], age)
            strategy = "ordered"
        sticky = view.value(alias, "sticky")
        sticky_hash = None
        if sticky and entry.type == "chat":
            key: str | None = env.session_id
            if str(sticky["key"]).startswith("custom:"):
                name = str(sticky["key"])[7:]
                fn = hooks.get("sticky_key", name)
                ctx = self._ctx(alias, held, age)
                key = hooks.call_safely("sticky_key", name, fn, ctx, default=None) if fn else None
            if key:
                sticky_hash = self.hash_id(str(key))
        return Ask(
            alias=alias,
            request_id=env.request_id,
            worker=self.worker_id,
            attempt=attempt,
            cls="p" if env.priority else "n",
            position=held.position,
            candidates=candidates,
            strategy=str(strategy),
            pick=str((view.value(alias, "balance_opts") or {}).get("pick", "percent")),
            cost_r=1,
            cost_t=estimate.total,
            claim=foreign_from is None,
            sticky_hash=sticky_hash,
            sticky_ttl_secs=float(sticky["idle_ttl_secs"]) if sticky else 1800.0,
            sticky_move_secs=float(view.value(alias, "sticky_move_after_secs")),
            foreign_from=foreign_from,
            require_pct=require_pct,
            tag=held.tag,
        )

    def _custom_order(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        candidates: list[Candidate],
        name: str,
        age: float,
    ) -> list[Candidate]:
        """Candidates in the order a custom balancer prefers (unknown keys ignored)."""
        fn = hooks.get("balancer", name)
        if fn is None:
            return candidates
        deps = {d.key: d for d in view.deployments(alias)}
        views = [
            hooks.DeploymentView(
                key=c.key,
                account=deps[c.key].account,
                region=deps[c.key].region,
                group=c.tier,
                rpm=view.value(alias, "rpm", c.key),
                tpm=view.value(alias, "tpm", c.key),
                weight=float(view.value(alias, "weight", c.key)),
                tags=view.value(alias, "tags", c.key) or {},
            )
            for c in candidates
        ]
        ctx = self._ctx(alias, held, age)
        order = hooks.call_safely("balancer", name, fn, views, ctx, default=None)
        if not order:
            return candidates
        by_key = {c.key: c for c in candidates}
        ranked = [by_key[k] for k in order if k in by_key]
        return ranked + [c for c in candidates if c.key not in set(order)]

    def _owner(self, held: _Held, attempt: int) -> str:
        """Owner token of an attempt; the message id tells two deliveries of one id apart."""
        worker = f"{self.worker_id}@{held.tag}"
        return owner_token(held.envelope.request_id, worker, attempt)

    def _params(
        self, view: ResolvedRegistry, alias: str, env: Envelope, dep: str | None
    ) -> dict[str, Any]:
        entry = view.registry.models[alias]
        request = inference.apply(
            entry.provider,
            env.op,
            env.params or {},
            view.value(alias, "inference", dep) or {},
            view.value(alias, "inference_limits", dep) or {},
        )
        extra = view.value(alias, "params", dep) if dep else None
        return inference.merge_params(request, extra or {})

    def _charged(self, view: ResolvedRegistry, alias: str, held: _Held) -> tokens.Estimate:
        """The token estimate admission charges: from model-level params, cached per view."""
        cached = held.charged
        if cached is not None and cached[0] is view and cached[1] == alias:
            return cached[2]
        params = self._params(view, alias, held.envelope, None)
        estimate = self._estimate(view, alias, held.envelope, params)
        held.charged = (view, alias, estimate)
        return estimate

    def _estimate(
        self, view: ResolvedRegistry, alias: str, env: Envelope, params: Mapping[str, Any]
    ) -> tokens.Estimate:
        entry = view.registry.models[alias]
        return tokens.estimate(
            env.op,
            params,
            embedding=entry.type == "embedding",
            default_output_tokens=int(view.value(alias, "default_output_tokens")),
            output_weight=float(view.value(alias, "tpm_output_weight")),
            estimator=view.value(alias, "token_estimator"),
        )

    async def _try(self, held: _Held, attempt: int, failed: frozenset[str] = frozenset()) -> bool:
        """Admit (or re-admit) a held call; start it when granted. True when it stays held."""
        view = self.registry.current
        env = held.envelope
        alias = env.model
        now = self.clock.now()
        deadline = self._deadline(held)
        if now >= deadline:
            await self._finish_call(
                held, "rate_limited", "no budget before the deadline", final="expired"
            )
            return False
        max_wait = env.max_wait_secs
        if max_wait is not None and attempt == 1 and now - held.position_ms / 1000.0 >= max_wait:
            # It already waited its limit in the queue, unreceived: refuse it now rather than
            # start it with no time left (it ran past its deadline and the caller gave up).
            await self._finish_call(
                held, "rate_limited", "max_wait_secs passed", final="expired", retry_after=1.0
            )
            return False
        decision = await self._decide(view, alias, held, attempt, failed)
        if decision is None:
            return False
        if decision.kind == "granted":
            self._start_run(held, decision, attempt)
            return False
        if decision.kind == "wait":
            max_wait = env.max_wait_secs
            waited = now - held.position_ms / 1000.0
            if max_wait is not None and attempt == 1 and waited >= max_wait:
                await self._finish_call(
                    held,
                    "rate_limited",
                    "max_wait_secs passed",
                    final="expired",
                    retry_after=decision.wait_secs,
                )
                return False
            fallen = await self._model_fallback(view, alias, held, attempt, waited)
            if fallen:
                return False
            if attempt == 1:
                cls = "p" if env.priority else "n"
                self._held_class.add((alias, cls))
                task = asyncio.create_task(self._hold(held, decision.wait_secs))
                self._running.add(task)
                task.add_done_callback(self._running.discard)
                return True
            await asyncio.sleep(min(decision.wait_secs, 1.0))
            return await self._try(held, attempt, failed)
        return False

    async def _decide(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        failed: frozenset[str],
    ) -> Decision | None:
        """One admission call and the terminal outcomes; None when the call was settled."""
        ask = self._ask(view, alias, held, attempt, failed)
        if ask is None:
            if self._bad_accounts and any(
                (d.account, d.region) in self._bad_accounts for d in view.deployments(alias)
            ):
                await self._finish_call(
                    held,
                    "credentials_unavailable",
                    "no account with working credentials",
                    retry_after=60,
                )
            else:
                await self._finish_call(
                    held, "no_deployment", "the call's filter leaves no deployment"
                )
            return None
        estimate = self._charged(view, alias, held)
        tpm = [view.value(alias, "tpm", c.key) for c in ask.candidates]
        if all(isinstance(v, int) for v in tpm) and tpm and estimate.input > max(tpm):
            await self._finish_call(held, "request_too_large", "input above the token budget")
            return None
        decision = await self.admission.pick_take(ask, self.registry.changes)
        if decision.kind == "wait":
            self.metrics.count("llmq_admission_refusals_total", model=alias, reason=decision.reason)
        if decision.kind == "stale_config":
            await self.registry.load()
            decision = await self.admission.pick_take(ask, self.registry.changes)
        if decision.kind == "no_config":  # the store lost the numbers: restore, then once more
            await self.registry.refresh()
            decision = await self.admission.pick_take(ask, self.registry.changes)
        if decision.kind in ("no_config", "stale_config"):
            # Still no usable numbers. Before, the call stayed held and hidden for good.
            logger.error("admission_config_missing", model=alias, kind=decision.kind)
            if held.message is None:
                await self._release(held)
                raise CallFailed("queue_unavailable", f"{alias}: admission numbers are missing")
            await self._release(held, delay_secs=5.0)
            return None
        if decision.kind in ("done", "cancelled"):
            if decision.kind == "cancelled":
                await self._finish_call(held, "cancelled", "cancelled", final="cancelled")
            else:
                await self._ack(held)
            return None
        if decision.kind == "duplicate":
            self._drop(held)
            if held.message is not None:
                await self.queues.return_(held.message, delay_secs=self.settings.lease_secs / 2)
            return None
        if decision.kind == "none":
            has_fallback = any(d.group == "fallback" for d in view.deployments(alias))
            rule = view.value(alias, "fallback")
            if not held.broken and has_fallback and rule and "broken" in rule["on"]:
                held.broken = True
                age = self.clock.now() - held.position_ms / 1000.0
                if age < float(rule["after_secs"]):
                    return Decision(
                        "wait", wait_secs=float(rule["after_secs"]) - age, reason="broken"
                    )
                return await self._decide(view, alias, held, attempt, failed)
            fell = await self._model_fallback(view, alias, held, attempt, 1e9, broken=True)
            if not fell:
                due = await self.health.disabled(alias)
                wait = max(1.0, min(due.values()) - self.clock.now()) if due else 60.0
                logger.error("model_unavailable", model=alias)
                await self._finish_call(
                    held, "model_unavailable", "no usable deployment", retry_after=wait
                )
            return None
        return decision

    async def _hold(self, held: _Held, wait: float) -> None:
        """Keep a refused call: retry admission until granted, settled or expired."""
        alias = held.envelope.model
        cls = "p" if held.envelope.priority else "n"
        try:
            while not self._stopping:
                await asyncio.sleep(min(max(wait, 0.01), 1.0))
                view = self.registry.current
                if held.message is not None:
                    await self.queues.extend(
                        held.message, float(view.value(alias, "queue")["visibility_secs"])
                    )
                now = self.clock.now()
                if now >= self._deadline(held):
                    await self._finish_call(
                        held, "rate_limited", "no budget before the deadline", final="expired"
                    )
                    return
                waited = now - held.position_ms / 1000.0
                if (
                    held.envelope.max_wait_secs is not None
                    and waited >= held.envelope.max_wait_secs
                ):
                    await self._finish_call(
                        held,
                        "rate_limited",
                        "max_wait_secs passed",
                        final="expired",
                        retry_after=wait,
                    )
                    return
                if await self._model_fallback(view, alias, held, 1, waited):
                    return
                decision = await self._decide(view, alias, held, 1, frozenset())
                if decision is None:
                    return
                if decision.kind == "granted":
                    self._start_run(held, decision, 1)
                    return
                wait = decision.wait_secs
        except Exception as exc:  # never leave the call held with nobody working on it
            if isinstance(exc, STORE_ERRORS):
                self._store_trouble_at = time.monotonic()
            logger.warning("hold_failed", request_id=held.envelope.request_id, error=str(exc))
            await self._release(held)
        finally:
            self._held_class.discard((alias, cls))
            if (event := self._wake.get(alias)) is not None:
                event.set()  # the class is free: receive its next call now

    async def _model_fallback(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        waited: float,
        *,
        broken: bool = False,
    ) -> bool:
        """Try the model's fallback models when its rule allows; True when one granted."""
        rule = view.value(alias, "model_fallback")
        env = held.envelope
        cause = "broken" if broken else "exhausted"
        if not self._may_spill(view, alias, env, waited, cause, "model_fallback"):
            return False
        for item in rule["models"]:
            other = item["model"]
            if other not in view.registry.models:
                continue
            pct = int(view.value(other, "require_headroom_pct")) if item["require_headroom"] else 0
            ask = self._ask(
                view, other, held, attempt, frozenset(), foreign_from=alias, require_pct=pct
            )
            if ask is None:
                continue
            decision = await self.admission.pick_take(ask, self.registry.changes)
            if decision.kind == "granted":
                await self.requests.set_status(env.request_id, "in_progress", served_model=other)
                self._start_run(held, decision, attempt, served=other)
                return True
        return False

    # ================================================================ worker: running

    def _start_run(
        self, held: _Held, decision: Decision, attempt: int, served: str | None = None
    ) -> None:
        alias = served or held.envelope.model
        held.grants.add((alias, self._owner(held, attempt)))
        task = asyncio.create_task(self._run(held, decision, attempt, alias))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    async def _run(self, held: _Held, decision: Decision, attempt: int, alias: str) -> None:
        slots = self._slots.get(held.envelope.model)
        held.run = asyncio.current_task()
        try:
            if slots is None:
                await self._attempts(held, decision, attempt, alias)
            else:
                async with slots:
                    await self._attempts(held, decision, attempt, alias)
        except asyncio.CancelledError:
            if not held.cancelled:
                raise
            # Cancelled by a caller: give the grant back as a cut attempt, reply `cancelled`.
            try:
                await self._give_back(held)
                await self._finish_call(held, "cancelled", "cancelled", final="cancelled")
            except Exception as exc:
                logger.warning("cancel_failed", request_id=held.envelope.request_id, error=str(exc))
                await self._release(held)
        except Exception as exc:
            # A store outage, a store refusal (OOM) or a bug mid-call. Never leave the call held
            # with nobody working on it (the heartbeat would hide its message for good): give
            # the grants back and return the message, so it runs again (at least once; a stored
            # result is reused). A direct call has no message: its caller gets the error.
            if isinstance(exc, STORE_ERRORS):
                self._store_trouble_at = time.monotonic()
            logger.warning(
                "run_failed",
                request_id=held.envelope.request_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            await self._release(held)
            if held.message is None:
                raise

    async def _attempts(self, held: _Held, decision: Decision, attempt: int, alias: str) -> None:
        throttles = 0
        tries = 0
        failed: set[str] = set()
        retry_secs = 0.0
        while True:
            view = self.registry.current
            dep_key = decision.deployment or ""
            owner = self._owner(held, attempt)
            held.grants.add((alias, owner))
            outcome, reply_or_error = await self._attempt(
                view, alias, held, dep_key, owner, attempt
            )
            held.grants.discard((alias, owner))
            if outcome == "ok":
                return
            kind, info, exc = reply_or_error
            if outcome == "stream_failed":  # never switched silently once chunks were sent
                await self._finish_call(held, kind, str(exc), partial=info)
                return
            entry = view.registry.models[alias]
            if outcome == "throttle":
                retry_secs = 0.0  # a throttle moves on at once (a 5xx's backoff is not reused)
                throttles += 1
                limit = int(view.value(alias, "throttle_retry_attempts", dep_key))
                if throttles > limit:
                    await self._finish_call(
                        held, "rate_limited", "the provider kept throttling", retry_after=info
                    )
                    return
            else:
                tries += 1
                if outcome == "fatal" or tries >= int(view.value(alias, "max_attempts")):
                    await self._finish_call(held, "provider_error", str(exc))
                    return
                # A move to a deployment this call has not tried is free: it is failover, not
                # a retry storm (bounded by the deployments and max_attempts). Counting it
                # failed calls as retry_budget_exhausted while another region was healthy
                # (scenario P3: 20 of 60 calls with one region hanging).
                tried = frozenset(failed | {dep_key})
                age = self.clock.now() - held.position_ms / 1000.0
                untried = any(
                    not c.last
                    for c in self._candidates(view, alias, held.envelope, age, tried, "error")
                )
                if not untried and not await self._retry_allowed(alias, view):
                    await self._finish_call(held, "retry_budget_exhausted", str(exc))
                    return
                retry_secs = self._backoff(view, alias, dep_key, tries)
            failed.add(dep_key)
            attempt += 1
            if retry_secs:  # never past the deadline: the caller is told in time
                left = self._deadline(held) - self.clock.now()
                await asyncio.sleep(max(0.0, min(retry_secs, left)))
            while True:
                if self.clock.now() >= self._deadline(held):
                    await self._finish_call(held, "deadline_exceeded", f"last error: {kind}")
                    return
                next_decision = await self._decide(view, alias, held, attempt, frozenset(failed))
                if next_decision is None:
                    return
                if next_decision.kind == "granted":
                    decision = next_decision
                    break
                if next_decision.kind == "busy":
                    await self._finish_call(held, "rate_limited", "fallback model is busy")
                    return
                await asyncio.sleep(min(next_decision.wait_secs, 1.0))
            del entry

    def _backoff(self, view: ResolvedRegistry, alias: str, dep: str, tries: int) -> float:
        base = float(view.value(alias, "retry_base_secs", dep))
        factor = float(view.value(alias, "retry_multiplier", dep))
        spread = float(view.value(alias, "retry_spread", dep))
        low = base * factor ** (tries - 1)
        return min(
            low * (1 + (spread - 1) * self._rng.random()),
            float(view.value(alias, "retry_max_secs", dep)),
        )

    async def _retry_allowed(self, alias: str, view: ResolvedRegistry) -> bool:
        minute = int(self.clock.now() // 60)
        key = self.keys.model(alias, f"retries:{minute}")
        async with self.state.pipeline(transaction=False) as pipe:
            pipe.hincrby(key, "retries", 1)
            pipe.hget(key, "first")
            pipe.expire(key, 120)
            retries, first, _ = await pipe.execute()
        first_n = int(first or 0)
        allowed = max(
            int(view.value(alias, "retry_budget_min")),
            first_n * int(view.value(alias, "retry_budget_pct")) // 100,
        )
        return int(retries) <= allowed

    async def _attempt(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        dep_key: str,
        owner: str,
        attempt: int,
    ) -> tuple[str, Any]:
        """One provider attempt and its settlement: ('ok', None) or (outcome, (kind, info, exc))."""
        env = held.envelope
        entry = view.registry.models[alias]
        deployment = next(d for d in view.deployments(alias) if d.key == dep_key)
        route = route_for(view, deployment)
        params = self._params(view, alias, env, dep_key)
        # Refunds settle what admission charged: `_ask`'s estimate from model-level params. One
        # from this deployment's params (e.g. a larger account max_tokens) refunded more than
        # was taken, and TPM stopped holding (found in the 0.3.0 review).
        estimate = self._charged(view, alias, held)
        remaining = max(0.1, self._deadline(held) - self.clock.now())
        attempt_budget = float(view.value(alias, "attempt_timeout_secs", dep_key))
        timing = Timing(
            first_response_secs=min(
                attempt_budget,
                remaining / 2 if attempt == 1 else remaining,
            ),
            idle_secs=float(view.value(alias, "stream_idle_secs")),
        )
        provider = self.providers.get(entry.provider)
        held.meta[owner] = {
            "alias": alias,
            "owner": owner,
            "request_id": env.request_id,
            "dep": dep_key,
            "cls": "p" if env.priority else "n",
            "cost_r": 1,
            "cost_t": estimate.total,
        }
        if attempt == 1:
            minute = int(self.clock.now() // 60)
            await self.state.hincrby(self.keys.model(env.model, f"retries:{minute}"), "first", 1)
        writer = None
        if env.stream and env.op in ("converse", "chat"):
            writer = ChunkWriter(
                self.state,
                self.keys.request(env.request_id, "chunks"),
                owner,
                flush_ms=int(view.value(alias, "stream_flush_ms", dep_key)),
                max_bytes=int(view.value(alias, "stream_max_bytes")),
                ttl_secs=self.settings.result_ttl_secs + int(env.timeout_secs),
            )
            if await self.state.xlen(writer.key):
                # Another worker already streamed part of this answer (plan R19).
                await self.admission.finish(
                    alias,
                    owner,
                    env.request_id,
                    refund_r=1,
                    refund_t=estimate.total,
                    outcome="none",
                    latency_ms=-1,
                )
                lost = "the worker serving this stream was lost"
                await writer.error("worker_lost", lost)
                return "stream_failed", ("worker_lost", None, RuntimeError(lost))
        started = time.monotonic()
        try:
            result = await asyncio.wait_for(
                provider.call(env.op, params, route, timing, sink=writer.add if writer else None),
                remaining,
            )
            if writer is not None:
                await writer.end(
                    {
                        "inputTokens": result.usage.input_tokens,
                        "outputTokens": result.usage.output_tokens,
                    }
                )
        except Exception as exc:
            latency = (time.monotonic() - started) * 1000
            info = provider.classify(exc)
            if writer is not None and writer.sent:
                # Chunks reached the caller: end the stream with the partial text (plan §3.13).
                refund = tokens.refund(estimate, "cut")
                await self.admission.finish(
                    alias,
                    owner,
                    env.request_id,
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="fail",
                    latency_ms=latency,
                )
                kind = "rate_limited" if info.kind is ErrorKind.THROTTLE else "provider_error"
                message = (
                    "stream above stream_max_bytes" if isinstance(exc, StreamTooLarge) else str(exc)
                )
                with contextlib.suppress(Exception):
                    await writer.error(kind, message)
                return "stream_failed", (kind, writer.partial_text, exc)
            cut = isinstance(exc, AttemptTimeout) and "chunk" in str(exc)
            # The caller's own deadline cut this attempt (the outer timer, or a first-response
            # timer shortened below `attempt_timeout_secs`): not the provider's fault. Counted
            # as a failure, late grants under a rate limit opened the breaker and turned the
            # rest into model_unavailable (found by scripts/load.py).
            deadline_cut = isinstance(exc, TimeoutError) and (
                not isinstance(exc, AttemptTimeout)
                or ("first response" in str(exc) and timing.first_response_secs < attempt_budget)
            )
            if deadline_cut:
                await self.admission.finish(
                    alias,
                    owner,
                    env.request_id,
                    refund_r=0,
                    refund_t=0,
                    outcome="none",
                    latency_ms=-1,
                )
                return "retry", ("retryable", None, exc)
            if info.kind is ErrorKind.THROTTLE:
                refund = tokens.refund(estimate, "throttle")
                await self.admission.finish(
                    alias,
                    owner,
                    env.request_id,
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="throttle",
                    latency_ms=-1,
                )
                return "throttle", ("throttled", info.retry_after_secs, exc)
            if info.kind is ErrorKind.RETRYABLE or isinstance(exc, TimeoutError):
                refund = tokens.refund(estimate, "cut" if cut else "fail")
                await self.admission.finish(
                    alias,
                    owner,
                    env.request_id,
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="fail",
                    latency_ms=latency,
                )
                return "retry", ("retryable", None, exc)
            refund = tokens.refund(estimate, "throttle")  # refused before it ran: all back
            await self.admission.finish(
                alias,
                owner,
                env.request_id,
                refund_r=refund.requests,
                refund_t=refund.tokens,
                outcome="none",
                latency_ms=-1,
            )
            if is_target_error(exc, info):
                return await self._target_error(view, alias, held, dep_key, exc)
            if isinstance(exc, CredentialsUnavailable) or _auth_error(info.code):
                self.providers.pool.forget(route)
                where = (deployment.account, deployment.region)
                _, backoff = self._bad_accounts.get(where, (0.0, 2.5))
                backoff = min(backoff * 2, 60.0)
                self._bad_accounts[where] = (time.monotonic() + backoff, backoff)
                logger.error(
                    "account_credentials_failed",
                    account=deployment.account,
                    region=deployment.region,
                )
                return "retry", ("auth", None, exc)
            return "fatal", ("fatal", None, exc)
        latency = (time.monotonic() - started) * 1000
        used = tokens.used_tokens(
            result.usage,
            output_weight=float(view.value(alias, "tpm_output_weight", dep_key)),
            count_cached=bool(view.value(alias, "tpm_count_cached", dep_key)),
        )
        refund = tokens.refund(estimate, "ok", used)
        await self.admission.finish(
            alias,
            owner,
            env.request_id,
            refund_r=refund.requests,
            refund_t=refund.tokens,
            outcome="ok",
            latency_ms=latency,
        )
        meta = Meta(
            served_model=alias,
            deployment=dep_key,
            fallback="model"
            if alias != env.model
            else ("region" if deployment.group == "fallback" else None),
            attempts=attempt,
            queue_wait_ms=int(max(0.0, held.first_seen - held.position_ms / 1000.0) * 1000),
            provider_ms=int(latency),
            tokens_in=result.usage.input_tokens,
            tokens_out=result.usage.output_tokens,
            worker=self.worker_id,
        )
        reply = Reply(request_id=env.request_id, ok=True, response=result.response, meta=meta)
        await self.requests.put_result(reply, final_state="done")
        await self._count_target_errors(held, succeeded=True)
        self._record(held, alias, deployment.account, deployment.region, "ok", meta)
        if self.settings.debug_capture_allowed and view.value(alias, "debug_capture"):
            await self._debug(env.request_id, params, result.response)
        await self.requests.set_status(
            env.request_id,
            "done",
            deployment=dep_key,
            served_model=alias,
            attempts=attempt,
            tokens_in=result.usage.input_tokens,
            tokens_out=result.usage.output_tokens,
        )
        await self._ack(held)
        return "ok", None

    async def _target_error(
        self, view: ResolvedRegistry, alias: str, held: _Held, dep_key: str, exc: BaseException
    ) -> tuple[str, Any]:
        """Note a target error; a request failing this way on two deployments is the cause.

        Nothing is counted yet: `_count_target_errors` records them once the call's outcome
        shows whether the deployment or the request was at fault (plan §3.18 R16).
        """
        held.target_failed.append((alias, dep_key))
        if len({d for _, d in held.target_failed}) >= 2:
            return "fatal", ("request", None, exc)
        return "retry", ("target", None, exc)

    async def _count_target_errors(self, held: _Held, *, succeeded: bool) -> None:
        """Count noted target errors: after a success elsewhere, or a failure on one deployment."""
        noted = held.target_failed
        held.target_failed = []
        if not noted or (not succeeded and len({d for _, d in noted}) >= 2):
            return
        view = self.registry.current
        for alias, dep_key in noted:
            settings = view.value(alias, "health", dep_key)
            others = [d.key for d in view.deployments(alias) if d.key != dep_key]
            outcome = await self.health.record_target_error(
                alias,
                dep_key,
                held.envelope.request_id,
                others=others,
                window_secs=float(settings["disable_window_secs"]),
                needed=int(settings["disable_errors"]),
                probe_after_secs=float(settings["probe_after_secs"]),
            )
            if outcome == "disabled":
                logger.error("deployment_disabled", model=alias, deployment=dep_key)
            elif outcome == "last":
                logger.error("deployment_failing_last_usable", model=alias, deployment=dep_key)

    async def _prober(self) -> None:
        """One process per model probes disabled deployments when they are due."""
        while True:
            await asyncio.sleep(5)
            try:
                view = self.registry.current
                for alias in view.registry.models:
                    lock = await self.state.set(
                        f"{self.keys.prefix}:lock:probe:{alias}", self.worker_id, nx=True, ex=5
                    )
                    if lock:
                        await self.probe_due(alias)
            except Exception as exc:
                logger.warning("prober_failed", error=str(exc))

    async def probe_due(self, alias: str) -> dict[str, str]:
        """Probe every disabled deployment of a model that is due; their outcomes."""
        view = self.registry.current
        now = self.clock.now()
        out: dict[str, str] = {}
        for dep_key, due in (await self.health.disabled(alias)).items():
            if due > now + 0.5:  # the local view of the store clock may lag slightly
                continue
            ok = await self._probe(view, alias, dep_key)
            settings = view.value(alias, "health", dep_key)
            out[dep_key] = await self.health.probed(
                alias, dep_key, ok=ok, probe_max_secs=float(settings["probe_max_secs"])
            )
            if out[dep_key] == "recovered":
                logger.info("deployment_recovered", model=alias, deployment=dep_key)
        return out

    async def _probe(self, view: ResolvedRegistry, alias: str, dep_key: str) -> bool:
        """One small real call on a deployment: reachable and allowed (a throttle counts)."""
        entry = view.registry.models[alias]
        deployment = next((d for d in view.deployments(alias) if d.key == dep_key), None)
        if deployment is None:
            return True
        op = OPS.get((entry.provider, entry.type), "chat" if entry.type == "chat" else "embeddings")
        name = view.value(alias, "probe")
        hook = hooks.get("probe", name)
        params = (
            hooks.call_safely("probe", name, hook, alias, op, default=None) if hook else None
        ) or _probe_params(entry.provider, entry.type, entry.model_id)
        provider = self.providers.get(entry.provider)
        try:
            await provider.call(op, params, route_for(view, deployment), Timing(30, 30))
        except Exception as exc:
            return provider.classify(exc).kind is ErrorKind.THROTTLE
        return True

    # ================================================================ worker: settling

    async def _reply(
        self,
        env: Envelope,
        kind: str,
        message: str,
        *,
        final: str = "failed",
        retry_after: float | None = None,
        partial: str | None = None,
    ) -> None:
        reply = Reply(
            request_id=env.request_id,
            ok=False,
            error=kind,
            message=message,
            retry_after_secs=retry_after,
            partial_text=partial or None,
            meta=Meta(served_model=env.model, worker=self.worker_id),
        )
        await self.requests.put_result(reply, final_state=final)
        await self.requests.set_status(env.request_id, final, error=kind)

    async def _finish_call(
        self,
        held: _Held,
        kind: str,
        message: str,
        *,
        final: str = "failed",
        retry_after: float | None = None,
        partial: str | None = None,
    ) -> None:
        await self._reply(
            held.envelope, kind, message, final=final, retry_after=retry_after, partial=partial
        )
        await self._count_target_errors(held, succeeded=False)
        self._record(held, held.envelope.model, "", "", kind, None)
        await self._ack(held)

    def _record(
        self, held: _Held, alias: str, account: str, region: str, outcome: str, meta: Meta | None
    ) -> None:
        """Metrics and the durable per-call record (`llmq_call_finished`, no text; E134)."""
        env = held.envelope
        labels = {"model": alias, "account": account or "-", "region": region or "-"}
        self.metrics.count("llmq_calls_total", 1.0, outcome=outcome, **labels)
        wait = max(0.0, held.first_seen - held.position_ms / 1000.0)
        self.metrics.observe(
            "llmq_wait_seconds", wait, model=env.model, priority="1" if env.priority else "0"
        )
        if meta is not None:
            self.metrics.count("llmq_tokens_input_total", float(meta.tokens_in or 0), **labels)
            self.metrics.count("llmq_tokens_output_total", float(meta.tokens_out or 0), **labels)
            if meta.fallback:
                self.metrics.count("llmq_fallback_total", kind=meta.fallback, model=env.model)
        user = self.hash_id(env.user_id) if env.user_id else None
        logger.info(
            "llmq_call_finished",
            request_id=env.request_id,
            model=env.model,
            served_model=alias,
            deployment=meta.deployment if meta else None,
            outcome=outcome,
            priority=env.priority,
            attempts=meta.attempts if meta else None,
            tokens_in=meta.tokens_in if meta else None,
            tokens_out=meta.tokens_out if meta else None,
            queue_wait_ms=int(wait * 1000),
            provider_ms=meta.provider_ms if meta else None,
            user=user,
            tenant=env.tenant_id,
        )

    async def _debug(
        self, request_id: str, params: Mapping[str, Any], response: Mapping[str, Any]
    ) -> None:
        """`debug_capture`: truncated text for at most 60 minutes (plan §3.12)."""
        import orjson

        body = orjson.dumps(
            {
                "request": orjson.dumps(params).decode()[:2000],
                "response": orjson.dumps(response).decode()[:2000],
            }
        )
        await self.state.set(self.keys.request(request_id, "debug"), body, ex=3600)

    async def status(self, model: str | None = None) -> dict[str, Any]:
        """Status of one model, or of everything (plan §3.12)."""
        view = self.registry.current
        if model is not None:
            return await self.status_reader.model(view, view.registry.alias_of(model))
        return await self.status_reader.report(view)

    async def request_status(self, request_id: str) -> dict[str, Any]:
        """Status of one call (empty when unknown or expired)."""
        return await self.status_reader.request(request_id)

    async def health_check(self) -> dict[str, Any]:
        """Liveness and readiness (plan E124): ready = registry loaded and not draining.

        The stores' state is reported but never makes the process unready. It is checked with a
        short-lived write, not PING: a replica answers PING but refuses writes (`read_only`).
        """
        probe = f"{self.keys.prefix}:health:{self.worker_id}"

        async def check(client: Any) -> str:
            # Bounded: a blackholed store took 41 s (connect timeout x retries), past any probe.
            try:
                await asyncio.wait_for(
                    client.set(probe, b"1", px=5000), self.settings.timings.health_probe_secs
                )
            except StoreReadOnlyError:
                return "read_only"
            except (*STORE_ERRORS, TimeoutError):
                return "down"
            return "ok"

        if self._queue_store is None:
            state = queue = await check(self.state)  # one store: probe it once
        else:
            state, queue = await asyncio.gather(check(self.state), check(self._queue_store))
        stores = {"state": state, "queue": queue}
        loaded = self.registry._current is not None
        return {"live": True, "ready": bool(self.ready and loaded), "stores": stores}

    async def _alert_loop(self) -> None:
        """Every 30 s one process evaluates alerts: gauges every time, a log line per change."""
        while True:
            await asyncio.sleep(30)
            try:
                lock = await self.state.set(
                    f"{self.keys.prefix}:lock:alerts", self.worker_id, nx=True, ex=25
                )
                if not lock:
                    continue
                view = self.registry.current
                report = await self.status_reader.report(view)
                alerts = self.alerts.evaluate(view, report)
                for alert in alerts:
                    self.metrics.gauge(
                        f"llmq_alert_{alert.name}", 1.0 if alert.on else 0.0, **alert.labels
                    )
                await self.alerts.changed(alerts)
            except Exception as exc:
                logger.warning("alerts_failed", error=str(exc))

    async def _ack(self, held: _Held) -> None:
        if held.message is not None:
            await self.queues.ack(held.message)
        self._drop(held)

    def _drop(self, held: _Held) -> None:
        """Forget a call, but only this one: a newer delivery may hold the same request id."""
        if self._held.get(held.envelope.request_id) is held:
            del self._held[held.envelope.request_id]

    async def _give_back(self, held: _Held) -> None:
        """Settle every grant of a call as a cut attempt (nothing learned, unused budget back)."""
        for served, owner in list(held.grants):
            with contextlib.suppress(Exception):
                await self.admission.finish(
                    served, owner, held.envelope.request_id, refund_r=0, refund_t=0, outcome="none"
                )
        held.grants.clear()

    async def _release(self, held: _Held, delay_secs: float = 0.0) -> None:
        """Let go of a call after an unexpected error: grants back, not held, message returned."""
        await self._give_back(held)
        self._drop(held)
        if held.message is not None:
            with contextlib.suppress(Exception):
                await self.queues.return_(held.message, delay_secs=delay_secs)

    # ================================================================ background

    def _free_stuck(self) -> None:
        """Let go of calls held a lease past their deadline; nothing should still hold them.

        Every path settles a call by its deadline, so one still held a lease later is a bug or
        a hung store call. The heartbeat renewed its grants for good (one-hour soak: two calls
        in progress for 50 minutes after a store block). Now it is logged, its task cancelled,
        its grants given back and its message returned (the next worker finds it expired).
        """
        late = self.clock.now() - self.settings.lease_secs - self.settings.caller_margin_secs
        for held in list(self._held.values()):
            if self._deadline(held) >= late:
                continue
            running = held.run is not None and not held.run.done()
            logger.error(
                "held_call_stuck",
                request_id=held.envelope.request_id,
                model=held.envelope.model,
                grants=sorted(held.grants),
                running=running,
                past_deadline_secs=round(self.clock.now() - self._deadline(held), 1),
            )
            self._drop(held)  # never renewed again, even if the release below fails
            if running:
                assert held.run is not None
                held.run.cancel()
            task = asyncio.create_task(self._release(held))
            self._running.add(task)
            task.add_done_callback(self._running.discard)

    async def _heartbeat(self) -> None:
        """Renew leases and holds; keep the caller liveness key; resync the clock.

        It beats at least three times per lease and per held message's visibility timeout, so
        a short `visibility_secs` never lets a running call's message reappear. Each step fails
        on its own: one bad message (e.g. of a model removed by a reload) skips only itself.
        """
        while True:
            beat = self.settings.lease_secs / 3
            self._free_stuck()
            try:
                await self.state.set(
                    self.keys.caller(self.worker_id), "1", ex=self.settings.caller_ttl_secs
                )
                items = [
                    (alias, owner, held.envelope.request_id)
                    for held in list(self._held.values())
                    for alias, owner in list(held.grants)
                ]
                await self.admission.renew(items)
                now = time.monotonic()
                if now - self._beat_ok_at > 2 * beat + 1:  # a gap: the store was out of reach
                    self._store_trouble_at = now
                self._beat_ok_at = now
            except Exception as exc:  # the heartbeat must survive store hiccups
                logger.warning("heartbeat_failed", step="renew", error=str(exc))
                self._store_trouble_at = time.monotonic()
                beat = 1.0  # renew the caller key as soon as the store is back
            view = self.registry.current
            for held in list(self._held.values()):
                if held.message is None:
                    continue
                try:
                    visibility = float(view.value(held.envelope.model, "queue")["visibility_secs"])
                    beat = min(beat, visibility / 3)
                    await self.queues.extend(held.message, visibility)
                except Exception as exc:
                    logger.warning(
                        "heartbeat_failed",
                        step="extend",
                        request_id=held.envelope.request_id,
                        error=str(exc),
                    )
            try:
                await self.clock.sync(self.state)
                await self._check_epoch()
            except Exception as exc:
                logger.warning("heartbeat_failed", step="clock", error=str(exc))
            await asyncio.sleep(max(0.2, beat))

    async def _check_epoch(self) -> None:
        """The store lost its data (restart, failover): re-register running calls (T10)."""
        if await self.state.exists(self.keys.epoch):
            return
        logger.error("state_store_reset_detected")
        await self.state.set(self.keys.epoch, str(self.clock.now()), nx=True)
        await self.state.set(
            f"{self.keys.prefix}:reset_minute", int(self.clock.now() // 60), ex=120
        )
        items = [
            meta
            for held in list(self._held.values())
            for owner, meta in held.meta.items()
            if any(owner == o for _, o in held.grants)
        ]
        await self.admission.reregister(items)

    async def _sweeper(self) -> None:
        """One process at a time removes in-progress entries of dead owners."""
        while True:
            await asyncio.sleep(self.settings.sweep_secs)
            try:
                lock = await self.state.set(
                    f"{self.keys.prefix}:lock:sweep",
                    self.worker_id,
                    nx=True,
                    ex=int(self.settings.sweep_secs),
                )
                if lock:
                    for alias in self.registry.current.registry.models:
                        await self.admission.sweep(alias)
            except Exception as exc:
                logger.warning("sweep_failed", error=str(exc))
