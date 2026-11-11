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
import random
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.clients.pool import route_for
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
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.queue import Message, QueueFull, QueueSettings, StreamQueues
from taskiq_llm_queue.engine.requests import RequestStore
from taskiq_llm_queue.errors import ErrorKind, UnsupportedEnvelopeError
from taskiq_llm_queue.providers.classify import is_target_error

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.registry import Deployment, ResolvedRegistry
    from taskiq_llm_queue.registry.store import RegistryStore

logger = get_logger(__name__)
OPS = {("bedrock", "chat"): "converse", ("bedrock", "embedding"): "invoke_model"}


@dataclass(frozen=True, slots=True)
class EngineSettings:
    """Process-level settings (plan §3.18 infrastructure table)."""

    queue_prefix: str = ""
    lease_secs: float = 60.0
    waiter_secs: float = 2.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 30
    status_ttl_secs: int = 1800
    caller_ttl_secs: int = 30
    poll_secs: float = 1.0
    sweep_secs: float = 10.0
    receive_batch: int = 10
    caller_margin_secs: float = 5.0


@dataclass(slots=True)
class _Held:
    """A message this process holds (waiting for budget, or running)."""

    message: Message
    envelope: Envelope
    first_seen: float
    grants: set[tuple[str, str]] = field(default_factory=set)
    broken: bool = False


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
    ) -> None:
        """Wire the stores; nothing runs until `start`."""
        self.settings = settings or EngineSettings()
        self.keys = Keys(registry.prefix)
        self.state = state
        self.registry = registry
        self.providers = providers
        self.queues = StreamQueues(queue_store or state, self.keys)
        self.requests = RequestStore(
            state,
            self.keys,
            result_ttl_secs=self.settings.result_ttl_secs,
            result_dedupe_secs=self.settings.result_dedupe_secs,
            status_ttl_secs=self.settings.status_ttl_secs,
        )
        self.admission = Admission(
            state,
            self.keys,
            lease_secs=self.settings.lease_secs,
            waiter_secs=self.settings.waiter_secs,
        )
        self.worker_id = worker_id or f"{uuid.uuid4().hex[:8]}:{uuid.uuid4().hex[:6]}"
        self.clock = StoreClock()
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._held: dict[str, _Held] = {}
        self._held_class: set[tuple[str, str]] = set()
        self._slots: dict[str, asyncio.Semaphore] = {}
        self._waiters: dict[str, asyncio.Future[None]] = {}
        self._pubsub: Any = None
        self._stopping = False
        self._rng = random.Random()

    # ================================================================ lifecycle

    async def start(self, *, worker: bool = True, caller: bool = True) -> None:
        """Boot the registry, then start the worker loops and the result reader."""
        await self.registry.boot()
        await self.clock.sync(self.state)
        await self.state.set(self.keys.epoch, str(self.clock.now()), nx=True)
        if caller:
            self._pubsub = self.state.pubsub()
            await self._pubsub.subscribe(self.keys.done_channel(self.worker_id))
            self._tasks.append(asyncio.create_task(self._result_reader()))
        if worker:
            for alias in self.registry.current.registry.models:
                slots = int(self.registry.current.value(alias, "worker_slots"))
                self._slots[alias] = asyncio.Semaphore(slots)
                self._tasks.append(asyncio.create_task(self._model_loop(alias)))
            self._tasks.append(asyncio.create_task(self._sweeper()))
        self._tasks.append(asyncio.create_task(self._heartbeat()))
        self._tasks.append(asyncio.create_task(self.registry.run(30)))

    async def stop(self, drain_secs: float = 10.0) -> None:
        """Stop receiving, let running calls finish (up to `drain_secs`), return held calls."""
        self._stopping = True
        if self._running:
            await asyncio.wait(set(self._running), timeout=drain_secs)
        for held in list(self._held.values()):
            if not held.grants:
                await self.queues.return_(held.message)
        for task in [*self._tasks, *self._running]:
            task.cancel()
        for task in [*self._tasks, *self._running]:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self._pubsub is not None:
            await self._pubsub.aclose()

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
    ) -> Reply:
        """Run one call through the queue and return its reply; `CallFailed` on failure."""
        view = self.registry.current
        alias = view.registry.alias_of(model)
        entry = view.registry.models[alias]
        op = OPS.get((entry.provider, entry.type), "chat" if entry.type == "chat" else "embeddings")
        timeout = float(timeout_secs or view.value(alias, "timeout_secs"))
        timeout = min(timeout, float(view.value(alias, "timeout_max_secs")))
        rid = request_id or uuid.uuid4().hex
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
        )
        hook_name = view.value(alias, "admission")
        hook = hooks.get("admission", hook_name)
        if hook is not None:
            ctx = hooks.CallContext(alias, rid, priority, session_id, user_id, tenant_id)
            verdict = hooks.call_safely("admission", hook_name, hook, ctx, default=None)
            if isinstance(verdict, hooks.Reject):
                raise CallFailed("rejected", verdict.reason, request_id=rid)
        user_hash = hashlib.sha256((user_id or session_id or "").encode()).hexdigest()[:16]
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
        until = loop.time() + timeout + self.settings.caller_margin_secs
        outcome = found.outcome
        try:
            while True:
                if outcome in ("new", "takeover"):
                    await self._send(rid, envelope, view, alias, priority, timeout)
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
            self._waiters.pop(rid, None)

    async def _send(
        self,
        rid: str,
        envelope: Envelope,
        view: ResolvedRegistry,
        alias: str,
        priority: bool,
        keep_secs: float,
    ) -> None:
        """Put the message on its queue and confirm the record; undo the record on failure."""
        settings = QueueSettings(max_waiting=int(view.value(alias, "max_waiting")))
        try:
            await self.queues.send(
                self._queue_name(view, alias, priority), envelope.dumps(), settings
            )
        except QueueFull as exc:
            await self.requests.abandon(rid, caller=self.worker_id)
            raise CallFailed("queue_full", str(exc), retry_after_secs=5) from exc
        except BaseException:
            await self.requests.abandon(rid, caller=self.worker_id)
            raise
        await self.requests.confirm(rid, keep_secs=keep_secs)

    async def _wait_result(
        self, rid: str, waiter: asyncio.Future[None], secs: float, *, recheck: bool = False
    ) -> Reply | None:
        """The reply; None (only with `recheck`) when the call's record disappeared."""
        loop = asyncio.get_running_loop()
        until = loop.time() + secs
        while True:
            reply = await self.requests.take_result(rid)
            if reply is None and recheck and not await self.requests.job(rid):
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
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(waiter), min(left, self.settings.poll_secs))
            if waiter.done():
                waiter = loop.create_future()
                self._waiters[rid] = waiter

    async def _result_reader(self) -> None:
        """Wake waiting callers when their result is published."""
        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except Exception as exc:  # the reader must survive store hiccups
                logger.warning("result_reader_error", error=str(exc))
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
        """Receive this model's calls (priority first) and admit them in order."""
        while not self._stopping:
            try:
                got = await self._receive_once(alias)
            except Exception as exc:  # a store blip must not end the loop
                logger.warning("receive_failed", model=alias, error=str(exc))
                await asyncio.sleep(1)
                continue
            if not got:
                await asyncio.sleep(0.05)

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
                held = await self._admit_new(alias, message)
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
        held = _Held(message, env, first_seen=self.clock.now())
        self._held[env.request_id] = held
        return await self._try(held, attempt=1)

    # ================================================================ worker: admission

    def _deadline(self, held: _Held) -> float:
        return held.message.position_ms / 1000.0 + held.envelope.timeout_secs

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
        age = self.clock.now() - held.message.position_ms / 1000.0
        cause = "error" if failed else ("broken" if held.broken else "exhausted")
        candidates = self._candidates(view, alias, env, age, failed, cause)
        if not candidates:
            return None
        entry = view.registry.models[alias]
        params = self._params(view, alias, env, None)
        estimate = self._estimate(view, alias, env, params)
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
                sticky_hash = hashlib.sha256(str(key).encode()).hexdigest()[:16]
        return Ask(
            alias=alias,
            request_id=env.request_id,
            worker=self.worker_id,
            attempt=attempt,
            cls="p" if env.priority else "n",
            position=position_of(held.message.id),
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
            tag=held.message.id,
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
        worker = f"{self.worker_id}@{held.message.id}"
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
        decision = await self._decide(view, alias, held, attempt, failed)
        if decision is None:
            return False
        if decision.kind == "granted":
            self._start_run(held, decision, attempt)
            return False
        if decision.kind == "wait":
            max_wait = env.max_wait_secs
            waited = now - held.message.position_ms / 1000.0
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
        env = held.envelope
        ask = self._ask(view, alias, held, attempt, failed)
        if ask is None:
            await self._finish_call(held, "no_deployment", "the call's filter leaves no deployment")
            return None
        params = self._params(view, alias, env, None)
        estimate = self._estimate(view, alias, env, params)
        tpm = [view.value(alias, "tpm", c.key) for c in ask.candidates]
        if all(isinstance(v, int) for v in tpm) and tpm and estimate.input > max(tpm):
            await self._finish_call(held, "request_too_large", "input above the token budget")
            return None
        decision = await self.admission.pick_take(ask, self.registry.changes)
        if decision.kind == "stale_config":
            await self.registry.load()
            decision = await self.admission.pick_take(ask, self.registry.changes)
        if decision.kind in ("done", "cancelled"):
            if decision.kind == "cancelled":
                await self._finish_call(held, "cancelled", "cancelled", final="cancelled")
            else:
                await self._ack(held)
            return None
        if decision.kind == "duplicate":
            self._held.pop(env.request_id, None)
            await self.queues.return_(held.message, delay_secs=self.settings.lease_secs / 2)
            return None
        if decision.kind == "none":
            has_fallback = any(d.group == "fallback" for d in view.deployments(alias))
            rule = view.value(alias, "fallback")
            if not held.broken and has_fallback and rule and "broken" in rule["on"]:
                held.broken = True
                age = self.clock.now() - held.message.position_ms / 1000.0
                if age < float(rule["after_secs"]):
                    return Decision(
                        "wait", wait_secs=float(rule["after_secs"]) - age, reason="broken"
                    )
                return await self._decide(view, alias, held, attempt, failed)
            fell = await self._model_fallback(view, alias, held, attempt, 1e9, broken=True)
            if not fell:
                await self._finish_call(
                    held, "model_unavailable", "no usable deployment", retry_after=60
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
                await self.queues.extend(
                    held.message, float(view.value(alias, "queue")["visibility_secs"])
                )
                now = self.clock.now()
                if now >= self._deadline(held):
                    await self._finish_call(
                        held, "rate_limited", "no budget before the deadline", final="expired"
                    )
                    return
                waited = now - held.message.position_ms / 1000.0
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
        finally:
            self._held_class.discard((alias, cls))

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
        slots = self._slots.get(held.envelope.model) or asyncio.Semaphore(1)
        async with slots:
            await self._attempts(held, decision, attempt, alias)

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
            entry = view.registry.models[alias]
            if outcome == "throttle":
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
                if not await self._retry_allowed(alias, view):
                    await self._finish_call(held, "retry_budget_exhausted", str(exc))
                    return
                retry_secs = self._backoff(view, alias, dep_key, tries)
            failed.add(dep_key)
            attempt += 1
            if retry_secs:
                await asyncio.sleep(retry_secs)
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
        estimate = self._estimate(view, alias, env, params)
        remaining = max(0.1, self._deadline(held) - self.clock.now())
        timing = Timing(
            first_response_secs=min(
                float(view.value(alias, "attempt_timeout_secs", dep_key)),
                remaining / 2 if attempt == 1 else remaining,
            ),
            idle_secs=float(view.value(alias, "stream_idle_secs")),
        )
        provider = self.providers.get(entry.provider)
        if attempt == 1:
            minute = int(self.clock.now() // 60)
            await self.state.hincrby(self.keys.model(env.model, f"retries:{minute}"), "first", 1)
        started = time.monotonic()
        try:
            result = await asyncio.wait_for(provider.call(env.op, params, route, timing), remaining)
        except Exception as exc:
            latency = (time.monotonic() - started) * 1000
            info = provider.classify(exc)
            cut = isinstance(exc, AttemptTimeout) and "chunk" in str(exc)
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
                return "retry", ("target", None, exc)
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
            queue_wait_ms=int(max(0.0, held.first_seen - held.message.position_ms / 1000.0) * 1000),
            provider_ms=int(latency),
            tokens_in=result.usage.input_tokens,
            tokens_out=result.usage.output_tokens,
            worker=self.worker_id,
        )
        reply = Reply(request_id=env.request_id, ok=True, response=result.response, meta=meta)
        await self.requests.put_result(reply, final_state="done")
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

    # ================================================================ worker: settling

    async def _reply(
        self,
        env: Envelope,
        kind: str,
        message: str,
        *,
        final: str = "failed",
        retry_after: float | None = None,
    ) -> None:
        reply = Reply(
            request_id=env.request_id,
            ok=False,
            error=kind,
            message=message,
            retry_after_secs=retry_after,
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
    ) -> None:
        await self._reply(held.envelope, kind, message, final=final, retry_after=retry_after)
        await self._ack(held)

    async def _ack(self, held: _Held) -> None:
        await self.queues.ack(held.message)
        self._held.pop(held.envelope.request_id, None)

    # ================================================================ background

    async def _heartbeat(self) -> None:
        """Renew leases and holds; keep the caller liveness key; resync the clock."""
        beat = max(0.2, self.settings.lease_secs / 3)
        while True:
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
                view = self.registry.current
                for held in list(self._held.values()):
                    queue = view.value(held.envelope.model, "queue")
                    await self.queues.extend(held.message, float(queue["visibility_secs"]))
                await self.clock.sync(self.state)
            except Exception as exc:  # the heartbeat must survive store hiccups
                logger.warning("heartbeat_failed", error=str(exc))
            await asyncio.sleep(beat)

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
