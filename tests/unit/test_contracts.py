"""Wave 0 contract tests: the frozen public surface every other layer builds on.

These pin the wire format, key schema, config rules and context behaviour, so a change to any
of them is a deliberate, visible decision rather than a side effect of other work.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from typing import Any

import pytest
from redis.crc import key_slot

import taskiq_llm_queue as llmq
from taskiq_llm_queue import (
    ConfigError,
    Lane,
    LLMQueueCancelled,
    LLMQueueError,
    LLMQueueTimeout,
    ModelConfig,
    ProviderError,
    QueueConfig,
    QueueUnavailable,
    Target,
    UnsupportedEnvelopeError,
    current_context,
    request_context,
)
from taskiq_llm_queue.envelope import (
    ENVELOPE_VERSION,
    Envelope,
    hashed_id,
    parse_envelope,
    task_labels,
)
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import ClaimState
from taskiq_llm_queue.reliability import _lua
from taskiq_llm_queue.reliability.breaker import CircuitBreaker
from taskiq_llm_queue.reliability.claims import RedisClaimStore
from taskiq_llm_queue.reliability.config_store import fallback_store
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter, SyncAdaptiveLimiter
from taskiq_llm_queue.routing import routing_store


def _envelope(**overrides: object) -> Envelope:
    now = time.time()
    fields: dict[str, object] = {
        "request_id": "llm-1",
        "op": "converse",
        "model": "gpt120b",
        "params": {"messages": [{"role": "user", "content": [{"text": "hi"}]}]},
        "deadline_at": now + 90,
        "enqueued_at": now,
        "session_id": "s-1",
        "user_id": "u-1",
    }
    fields.update(overrides)
    return Envelope.model_validate(fields)


# ---------------------------------------------------------------- public surface


def test_all_exports_resolve() -> None:
    """Every name in __all__ exists, so callers never import from internal modules."""
    missing = [name for name in llmq.__all__ if not hasattr(llmq, name)]
    assert missing == []


def test_all_is_the_client_surface() -> None:
    """Wire / plumbing names left __all__ but stay importable (from submodules and here)."""
    for name in ("LLMQueue", "ConverseResult", "ChatResult", "InvokeResult", "EmbedResult"):
        assert name in llmq.__all__
    for name in ("Envelope", "KeySpace", "TASK_CALL", "ENVELOPE_VERSION", "Limiter", "LLMReply"):
        assert name not in llmq.__all__
        assert getattr(llmq, name) is not None


def test_import_has_no_heavy_side_effects() -> None:
    """Importing the package loads no AWS/Redis clients and starts no threads (library rule)."""
    code = (
        "import sys, threading; import taskiq_llm_queue; "
        "heavy = [m for m in ('aiobotocore', 'botocore', 'redis', 'taskiq') if m in sys.modules]; "
        "print(heavy, threading.active_count())"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "[] 1"


def test_error_tree_has_one_root() -> None:
    """Callers catch LLMQueueError for everything the package raises."""
    for exc_type in (
        LLMQueueTimeout,
        LLMQueueCancelled,
        QueueUnavailable,
        ProviderError,
        ConfigError,
        UnsupportedEnvelopeError,
    ):
        assert issubclass(exc_type, LLMQueueError)
    err = ProviderError("throttled", retryable=True, retry_after_secs=2.0)
    assert (err.retryable, err.retry_after_secs) == (True, 2.0)


# ---------------------------------------------------------------- envelope


def test_envelope_round_trip_is_lossless() -> None:
    """JSON out and back gives an equal envelope (what SQS carries between pods)."""
    env = _envelope(lane=Lane.RT, fallback="chain", trace={"langfuse_trace_id": "t"})
    assert parse_envelope(env.model_dump_json()) == env


def test_envelope_requires_exactly_one_of_params_and_ref() -> None:
    """Inline params and a by-reference key are mutually exclusive."""
    with pytest.raises(ValueError, match="exactly one"):
        _envelope(params=None)
    with pytest.raises(ValueError, match="exactly one"):
        _envelope(params_ref="llmq:local:req:llm-1")
    assert _envelope(params=None, params_ref="llmq:local:req:llm-1").params_ref


def test_newer_envelope_version_is_released_not_dropped() -> None:
    """A newer `v` raises UnsupportedEnvelopeError so the worker releases the message."""
    raw = json.loads(_envelope().model_dump_json())
    raw["v"] = ENVELOPE_VERSION + 1
    with pytest.raises(UnsupportedEnvelopeError):
        parse_envelope(raw)


@pytest.mark.parametrize("payload", [b"not json", b"[]", b'{"request_id": "x"}'])
def test_malformed_envelopes_raise_value_error(payload: bytes) -> None:
    """Garbage on the queue is a ValueError (poison), never a crash in the receiver."""
    with pytest.raises(ValueError):  # noqa: PT011  # JSONDecodeError and pydantic errors both derive from it
        parse_envelope(payload)


def test_labels_hash_ids_by_default() -> None:
    """Session and user ids never appear raw in labels, logs or metrics."""
    labels = task_labels(_envelope(reply_to="pod-a"))
    assert labels["session_id"] == hashed_id("s-1") != "s-1"
    assert labels["user_id"] == hashed_id("u-1")
    assert task_labels(_envelope(), salt=b"other")["user_id"] != labels["user_id"]
    assert (labels["lane"], labels["model"], labels["reply_to"]) == ("bg", "gpt120b", "pod-a")
    assert task_labels(_envelope(), hash_ids=False)["user_id"] == "u-1"


# ---------------------------------------------------------------- config


def _models() -> dict[str, ModelConfig]:
    return {
        "gpt120b": ModelConfig(
            provider="bedrock",
            model_id="openai.gpt-oss-120b-1:0",
            targets={"primary": Target(region="ap-south-1"), "apac": Target(profile_id="apac.x")},
            queues={"rt": "llm-gpt120b-rt", "default": "llm-gpt120b"},
        )
    }


def test_resolve_alias_and_target() -> None:
    """`alias[@target]` resolves; the first target is the primary."""
    cfg = QueueConfig(models=_models())
    assert cfg.resolve("gpt120b")[2] == "primary"
    assert cfg.resolve("gpt120b@apac")[2] == "apac"
    with pytest.raises(ConfigError, match="unknown model alias"):
        cfg.resolve("gpt-5")
    with pytest.raises(ConfigError, match="unknown target"):
        cfg.resolve("gpt120b@eu")


def test_disallowed_provider_fails_at_construction() -> None:
    """A model whose provider isn't allowed (e.g. Azure blocked by policy) fails at boot."""
    with pytest.raises(ValueError, match="allowed_providers"):
        QueueConfig(models=_models(), allowed_providers=frozenset({"openai"}))


def test_from_env_builds_config_and_collects_errors() -> None:
    """LLMQ_* variables build a config; every bad value is reported in one error."""
    cfg = QueueConfig.from_env(
        {
            "LLMQ_KEY_PREFIX": "llmq:test",
            "LLMQ_REDIS_PORT": "6380",
            "LLMQ_REDIS_SSL": "false",
            "LLMQ_MODELS": json.dumps({k: v.model_dump() for k, v in _models().items()}),
            "LLMQ_DENIED_HOSTS": "*.openai.azure.com",
        }
    )
    assert (cfg.key_prefix, cfg.redis_port, cfg.redis_ssl) == ("llmq:test", 6380, False)
    assert cfg.denied_hosts == ("*.openai.azure.com",)
    with pytest.raises(ConfigError) as info:
        QueueConfig.from_env(
            {"LLMQ_REDIS_PORT": "x", "LLMQ_REDIS_SSL": "maybe", "LLMQ_MODELS": "{"}
        )
    message = str(info.value)
    for name in ("REDIS_PORT", "REDIS_SSL", "MODELS"):
        assert name in message


def test_config_defaults_follow_plan() -> None:
    """Lane timeouts and TTLs match the design (state outlives the longest deadline)."""
    cfg = QueueConfig()
    assert cfg.timeout_secs == {Lane.RT: 90.0, Lane.BG: 300.0, Lane.BULK: 900.0}
    assert cfg.state_ttl_secs >= max(cfg.timeout_secs.values()) + 300
    assert (cfg.result_ttl_secs, cfg.inline_max_bytes) == (300, 200_000)


# ---------------------------------------------------------------- keys


def test_key_schema() -> None:
    """Every key lives under the prefix, so environments sharing a Redis never collide."""
    ks = KeySpace("llmq:prod")
    keys = [
        ks.job("a"),
        ks.params("a"),
        ks.cancel("a"),
        ks.result("t"),
        ks.limiter("m", "p"),
        ks.leases("m", "p"),
        ks.breaker("m", "p"),
        ks.done_channel("pod"),
        ks.routing,
        ks.fallback,
        ks.audit,
        ks.pods,
        ks.schema,
    ]
    assert all(k.startswith("llmq:prod:") for k in keys)
    assert ks.job("a") == "llmq:prod:{r:a}:job"
    assert ks.limiter("m", "p") == "llmq:prod:{m:m:p}:ac"
    assert ks.routing == "llmq:prod:{cfg}:routing"
    assert ks.pods == "llmq:prod:pods"
    assert len(set(keys)) == len(keys)


def test_hashed_id_is_keyed_blake2b_and_stable() -> None:
    """Same salt → same 16-hex id on every pod; another salt → another id (not guessable)."""
    assert hashed_id("u-1") == "60c492a1cb883215"  # pinned: a change re-buckets every user
    assert len(hashed_id("u-1") or "") == 16
    assert hashed_id("u-1", b"prod-salt") == hashed_id("u-1", "prod-salt")
    assert hashed_id("u-1", b"prod-salt") != hashed_id("u-1")
    assert hashed_id(None) is None


# ---------------------------------------------------------------- context


def test_context_defaults_to_background_and_flags_it() -> None:
    """Without request_context, calls land in `bg` and are marked as defaulted."""
    ctx = current_context()
    assert (ctx.lane, ctx.defaulted) == (Lane.BG, True)


async def test_context_flows_into_tasks_and_nests() -> None:
    """Context reaches create_task'd coroutines; nested blocks override only what they set."""
    seen: list[tuple[str, str | None]] = []

    async def worker() -> None:
        ctx = current_context()
        seen.append((ctx.lane.value, ctx.session_id))

    with request_context(lane="rt", session_id="s-1", user_id="u-1"):
        await asyncio.create_task(worker())
        with request_context(lane=Lane.BULK):
            await asyncio.create_task(worker())
    await asyncio.create_task(worker())
    assert seen == [("rt", "s-1"), ("bulk", "s-1"), ("bg", None)]


def test_context_reaches_threads_only_with_copy_context() -> None:
    """Raw threads lose the context unless started under contextvars.copy_context().run."""
    seen: dict[str, str | None] = {}

    def record(name: str) -> None:
        seen[name] = current_context().session_id

    with request_context(session_id="s-9"):
        plain = threading.Thread(target=record, args=("plain",))
        copied = threading.Thread(target=contextvars.copy_context().run, args=(record, "copied"))
        for thread in (plain, copied):
            thread.start()
            thread.join()
    assert seen == {"plain": None, "copied": "s-9"}


# ---------------------------------------------------------------- cluster slots


class _Recorder:
    """Stands in for a registered Lua script: records each call's keys, returns a canned reply."""

    def __init__(self, calls: list[tuple[str, list[str]]], name: str, reply: object) -> None:
        self._calls, self._name, self._reply = calls, name, reply
        self.sha = name

    def _record(self, keys: Sequence[str] | None) -> object:
        self._calls.append((self._name, list(keys or [])))
        return self._reply

    def __call__(self, keys: Sequence[str] | None = None, args: object = None) -> object:
        return self._record(keys)


class _AsyncRecorder(_Recorder):
    async def __call__(self, keys: Sequence[str] | None = None, args: object = None) -> object:
        return self._record(keys)


def _script_names() -> dict[str, str]:
    """Every public Lua script in `_lua`, by source text (what `register_script` receives)."""
    return {
        value: name
        for name, value in vars(_lua).items()
        if name.isupper()
        and not name.startswith("_")
        and isinstance(value, str)
        and "KEYS" in value
    }


_REPLIES: dict[str, object] = {
    "LIMITER_ACQUIRE": [1, 1, 10, b"123.0"],
    "LIMITER_RELEASE": [10, 1, b"0"],
    "LIMITER_PEEK": [10, 0, b"0"],
    "LEASE_RENEW": [b"123.0", 1],
    "BREAKER": [b"closed", 1],
    "CLAIM": b"run",
    "CLAIM_HEARTBEAT": [],
    "CLAIM_COMPLETE": 1,
    "CLAIM_RELEASE": 1,
    "CONFIG_SET": b"",
}


class _Pipeline:
    """Records `EVALSHA` calls queued by `_lua.run_many` (sha = script name here)."""

    def __init__(self, calls: list[tuple[str, list[str]]]) -> None:
        self._calls = calls
        self._replies: list[object] = []

    async def __aenter__(self) -> _Pipeline:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: object) -> None:
        self._calls.append((sha, [str(k) for k in keys_and_args[:numkeys]]))
        self._replies.append(_REPLIES[sha])

    async def execute(self, raise_on_error: bool = True) -> list[object]:
        return self._replies


class _ScriptClient:
    """Fake Redis client: `register_script` hands out recorders (async or sync)."""

    def __init__(self, calls: list[tuple[str, list[str]]], *, sync: bool = False) -> None:
        self.calls, self._sync, self._names = calls, sync, _script_names()

    def register_script(self, source: str) -> _Recorder:
        name = self._names[source]
        kind = _Recorder if self._sync else _AsyncRecorder
        return kind(self.calls, name, _REPLIES[name])

    def pipeline(self, transaction: bool = True) -> _Pipeline:
        return _Pipeline(self.calls)


def _slots(keys: Sequence[str]) -> set[int]:
    return {key_slot(key.encode()) for key in keys}


async def test_every_lua_call_site_uses_one_cluster_slot() -> None:
    """Each script call's keys share one hash slot, as cluster-mode Valkey requires."""
    calls: list[tuple[str, list[str]]] = []
    client: Any = _ScriptClient(calls)
    keys = KeySpace("llmq:prod")
    limiter = AdaptiveLimiter(client, keys)
    lease = await limiter.try_acquire("gpt120b", "apac", Lane.RT)
    assert lease is not None
    await limiter.release(lease, throttled=True)
    await limiter.state("gpt120b", "apac")
    await limiter.renew([lease])
    sync_limiter = SyncAdaptiveLimiter(_ScriptClient(calls, sync=True), keys)  # type: ignore[arg-type]  # fake client
    sync_lease = sync_limiter.acquire("gpt120b", "primary", Lane.BG, 1.0)
    assert sync_lease is not None
    sync_limiter.release(sync_lease, throttled=False)
    breaker = CircuitBreaker(client, keys)
    await breaker.allow("gpt120b", "apac")
    claims = RedisClaimStore(client, keys)
    await claims.claim("llm-1", deadline_at=time.time() + 60, worker="pod-a")
    await claims.heartbeat_owned(["llm-1"], worker="pod-a")
    await claims.complete_owned("llm-1", ClaimState.DONE, worker="pod-a")
    await claims.release_owned("llm-1", worker="pod-a")
    for store in (routing_store(client, keys), fallback_store(client, keys)):
        await store.reset(actor="t")

    assert {name for name, _ in calls} == set(_script_names().values())  # every script ran
    for name, call_keys in calls:
        assert len(_slots(call_keys)) == 1, (name, call_keys)


async def test_claim_heartbeat_batch_uses_one_slot() -> None:
    calls: list[tuple[str, list[str]]] = []
    claims = RedisClaimStore(_ScriptClient(calls), KeySpace("llmq:prod"))  # type: ignore[arg-type]  # fake client
    await claims.heartbeat_owned(["llm-1", "llm-2", "llm-3"], worker="pod-a")
    assert all(len(_slots(keys)) == 1 for _, keys in calls)


def test_tagged_keys_share_slots_by_design() -> None:
    """Per-request and per-target keys group; different requests / targets spread out."""
    ks = KeySpace("llmq:prod")
    assert len(_slots([ks.job("a"), ks.cancel("a"), ks.params("a"), ks.result("a")])) == 1
    assert len(_slots([ks.limiter("m", "t"), ks.leases("m", "t"), ks.breaker("m", "t")])) == 1
    assert len(_slots([ks.routing, ks.fallback, ks.audit])) == 1
    assert len(_slots([ks.job(f"r-{i}") for i in range(20)])) > 1
