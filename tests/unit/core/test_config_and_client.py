"""Unit tests for the runtime config fields, the client's pure helpers and its boot rules.

Nothing here needs Valkey or SQS: they cover env parsing, error mapping, embedding batching,
routing before start and the strict-async switch.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

import taskiq_llm_queue as llmq
from taskiq_llm_queue import (
    CallMeta,
    ConfigError,
    Lane,
    LLMQueueCancelled,
    LLMQueueError,
    LLMQueueTimeout,
    ModelConfig,
    ProviderError,
    QueueConfig,
    Target,
)
from taskiq_llm_queue.client import (
    EmbedResult,
    InvokeResult,
    LLMQueue,
    _batch_ids,
    _invoke_body,
    _merge_meta,
    _unwrap,
    decode_invoke,
)
from taskiq_llm_queue.config import create_redis, create_sync_redis
from taskiq_llm_queue.envelope import LLMReply
from taskiq_llm_queue.lifecycle import limiter_settings, queue_for_lane, queue_name

MODELS = {
    "titan": ModelConfig(provider="bedrock", model_id="amazon.titan-embed-text-v2:0"),
    "cohere": ModelConfig(provider="bedrock", model_id="cohere.embed-english-v3"),
    "llama": ModelConfig(
        provider="openai", model_id="llama-3.1-70b", targets={"p": Target(base_url="http://x/v1")}
    ),
    "big": ModelConfig(
        provider="bedrock",
        model_id="big",
        queues={"rt": "llm-big-rt", "default": "http://sqs:9324/000/llm-big"},
        limiter_initial=7,
    ),
}


def test_from_env_reads_runtime_tuning() -> None:
    """LLMQ_* runtime names from plan §11 land in the new fields (nested and per-lane)."""
    cfg = QueueConfig.from_env(
        {
            "LLMQ_AC_MIN": "3",
            "LLMQ_AC_PAUSE_SECS": "1.5",
            "LLMQ_AC_INITIAL": '{"big": 40}',
            "LLMQ_CB_THRESHOLD": "9",
            "LLMQ_WORKER_SLOTS": "big:300, jobs:4",
            "LLMQ_RESULT_DISPATCH_MS": "rt:50,bulk:1000",
            "LLMQ_TIMEOUT_RT_SECS": "30",
            "LLMQ_TIMEOUT_EMBED_BULK_SECS": "120",
            "LLMQ_CONSUMER_DRAIN_SECS": "20",
            "LLMQ_MAX_FAILURES": "5",
            "LLMQ_FALLBACK_CHAINS": json.dumps({"chains": {"big": ["other"]}}),
            "LLMQ_JOB_QUEUES": '{"ingest": "jobs-ingest"}',
            "LLMQ_ROUTING_ENABLED": "true",
        }
    )
    assert (cfg.limiter.min_limit, cfg.limiter.pause_secs, cfg.limiter.initial) == (
        3,
        1.5,
        {"big": 40},
    )
    assert cfg.breaker.threshold == 9
    assert cfg.worker_slots == {"big": 300, "jobs": 4}
    assert cfg.result_dispatch_ms == {Lane.RT: 50, Lane.BULK: 1000}
    assert cfg.timeout_secs == {Lane.RT: 30.0, Lane.BG: 300.0, Lane.BULK: 900.0}
    assert cfg.embed_timeout_secs[Lane.BULK] == 120.0
    assert (cfg.drain_secs, cfg.max_failures, cfg.routing_enabled) == (20.0, 5, True)
    assert cfg.job_queues == {"ingest": "jobs-ingest"}


def test_from_env_collects_bad_tuning_values() -> None:
    """Every bad runtime value is reported in one ConfigError."""
    with pytest.raises(ConfigError) as info:
        QueueConfig.from_env(
            {"LLMQ_AC_MIN": "x", "LLMQ_WORKER_SLOTS": "big", "LLMQ_TIMEOUT_RT_SECS": "soon"}
        )
    for name in ("AC_MIN", "WORKER_SLOTS", "TIMEOUT_RT_SECS"):
        assert name in str(info.value)


def test_invalid_fallback_default_fails_at_boot() -> None:
    """A self-referencing chain is rejected when the config is built, not on a call."""
    with pytest.raises(ValueError, match="primary model itself"):
        QueueConfig(models=MODELS, fallback_default={"chains": {"big": ["big"]}})


def test_limiter_settings_merge_model_and_env_starts() -> None:
    """`limiter_initial` per model, overridden by `limiter.initial` (LLMQ_AC_INITIAL)."""
    cfg = QueueConfig(models=MODELS, limiter={"initial": {"titan": 99}, "pause_secs": 1.0})
    settings = limiter_settings(cfg)
    assert (settings.start_for("big"), settings.start_for("titan")) == (7, 99)
    assert settings.pause_secs == 1.0


def test_lane_queue_choice_and_names() -> None:
    """rt → rt queue; bg/bulk → default; URLs route by their last path segment."""
    big = MODELS["big"]
    assert queue_for_lane(big, Lane.RT) == "llm-big-rt"
    assert queue_for_lane(big, Lane.BULK) == "http://sqs:9324/000/llm-big"
    assert queue_name("http://sqs:9324/000/llm-big") == "llm-big"
    assert queue_for_lane(MODELS["titan"], Lane.RT) is None


def test_unwrap_maps_errors() -> None:
    """Error replies become the documented exception types (README "Errors")."""
    ok = LLMReply(request_id="a", ok=True, response={"x": 1}, meta=CallMeta(attempts=1))
    assert _unwrap(ok) is ok
    throttled = LLMReply(
        request_id="a",
        ok=False,
        error={"type": "Throttling", "retryable": True, "kind": "throttle", "retry_after_secs": 2},
    )
    with pytest.raises(ProviderError) as info:
        _unwrap(throttled)
    assert (info.value.retryable, info.value.retry_after_secs) == (True, 2.0)
    assert getattr(info.value, "status_code", None) == 429
    with pytest.raises(LLMQueueTimeout):
        _unwrap(LLMReply(request_id="a", ok=False, error={"type": "deadline_exceeded"}))
    with pytest.raises(LLMQueueCancelled, match="cancelled"):
        _unwrap(LLMReply(request_id="a", ok=False, cancelled=True, error={"type": "cancelled"}))


def test_decode_invoke_returns_bytes_body() -> None:
    """Workers ship InvokeModel bodies as base64; callers get raw bytes back."""
    raw = {"body_b64": base64.b64encode(b'{"a": 1}').decode(), "contentType": "application/json"}
    assert decode_invoke(raw) == {"body": b'{"a": 1}', "contentType": "application/json"}


def test_embed_plan_batches_per_provider_limit() -> None:
    """Titan: one text per task; Cohere: 96 per task with input_type; OpenAI: one chunk."""
    q = LLMQueue(QueueConfig(models=MODELS))
    assert len(q._embed_plan("titan", ["a", "b", "c"], "search_query")) == 3
    titan_body = q._embed_plan("titan", ["a"], "search_query")[0][1]["body"]
    assert titan_body == {"inputText": "a"}
    cohere = q._embed_plan("cohere", [str(i) for i in range(100)], "search_query")
    assert [len(p["body"]["texts"]) for _, p in cohere] == [96, 4]
    assert cohere[0][1]["body"]["input_type"] == "search_query"
    assert q._embed_plan("llama", ["a", "b"], None) == [("embeddings", {"input": ["a", "b"]})]


async def test_calls_before_start_are_refused() -> None:
    """A call on a queue that was never started fails loudly instead of hanging."""
    q = LLMQueue(QueueConfig(models=MODELS))
    with pytest.raises(llmq.QueueUnavailable):
        await q.converse("big", {"messages": []})
    with pytest.raises(ConfigError, match="unknown model alias"):
        await q.converse("nope", {"messages": []})


def test_strict_async_refuses_sync_calls() -> None:
    """`strict_async=True` turns the sync fallback into an error (README "Sync callers")."""
    q = LLMQueue(QueueConfig(models=MODELS, strict_async=True))
    with pytest.raises(LLMQueueError, match="strict_async"):
        q.converse_sync("big", {"messages": []})


def test_lazy_exports_resolve() -> None:
    """`LLMQueue` and `ConverseResult` are importable from the package front door."""
    assert llmq.LLMQueue is LLMQueue
    result: Any = llmq.ConverseResult(response={}, meta=CallMeta())
    assert result.response == {}
    assert llmq.ChatResult(response={"id": "x"}, meta=CallMeta()).response == {"id": "x"}


@pytest.mark.parametrize(
    ("code", "message", "expected"),
    [
        ("ThrottlingException", "ThrottlingException: ThrottlingException", "ThrottlingException"),
        (
            "ThrottlingException",
            "ClientError: ThrottlingException",
            "ThrottlingException: ClientError",
        ),
        ("no_capacity", "no capacity", "no_capacity: no capacity"),
    ],
)
def test_provider_error_message_does_not_repeat_the_code(
    code: str, message: str, expected: str
) -> None:
    reply = LLMReply(request_id="a", ok=False, error={"type": code, "message": message})
    with pytest.raises(ProviderError) as info:
        _unwrap(reply)
    assert str(info.value) == expected


def test_result_objects_are_easy_to_use() -> None:
    """`InvokeResult` keeps the boto3-style `response`; `EmbedResult` acts as the vectors."""
    inv = InvokeResult(b'{"a": 1}', "application/json", CallMeta(attempts=1))
    assert inv.response == {"body": b'{"a": 1}', "contentType": "application/json"}
    assert inv.json() == {"a": 1}
    emb = EmbedResult([[1.0], [2.0]], CallMeta())
    assert (len(emb), emb[1], list(emb)) == (2, [2.0], [[1.0], [2.0]])


def test_embed_meta_merges_batches_and_ids_follow_request_id() -> None:
    a = CallMeta(served_model="titan", served_target="p", queue_wait_ms=5, attempts=1)
    b = CallMeta(served_model="titan", fallback_used=True, queue_wait_ms=9, attempts=2)
    merged = _merge_meta([a, b])
    assert (merged.served_target, merged.queue_wait_ms, merged.attempts) == ("p", 9, 3)
    assert merged.fallback_used is True
    assert _batch_ids("r", 1) == ["r"]
    assert _batch_ids("r", 3) == ["r-0", "r-1", "r-2"]


def test_invoke_body_accepts_json_text_and_bytes() -> None:
    assert _invoke_body({"inputText": "a"}) == {"inputText": "a"}
    assert _invoke_body(b'{"x": 1}') == '{"x": 1}'
    with pytest.raises(ValueError, match="UTF-8"):
        _invoke_body(b"\xff\xfe")


def test_old_s_env_names_are_ignored() -> None:
    """Old `_S` / suffix-less spellings are not read: 0.1 ships only the `_SECS` names."""
    cfg = QueueConfig.from_env({"LLMQ_CB_WINDOW_S": "12", "LLMQ_TIMEOUT_RT": "33"})
    default = QueueConfig.from_env({})
    assert cfg.breaker.window_secs == default.breaker.window_secs
    assert cfg.timeout_secs[Lane.RT] == default.timeout_secs[Lane.RT]


def test_secrets_and_region_defaults() -> None:
    """Passwords and the hash salt never show in repr; the AWS chain picks the region."""
    cfg = QueueConfig.from_env(
        {"LLMQ_REDIS_PASSWORD": "pw", "LLMQ_ID_HASH_SALT": "s3cret", "LLMQ_REDIS_USERNAME": "u"}
    )
    assert "pw" not in repr(cfg)
    assert "s3cret" not in repr(cfg)
    assert cfg.redis_password is not None
    assert cfg.redis_password.get_secret_value() == "pw"
    assert (cfg.aws_region, cfg.id_hash_key) == (None, b"s3cret")
    with pytest.raises(ValueError, match="id_hash_salt"):
        QueueConfig(id_hash_salt="x" * 65)


def test_redis_factory_applies_pool_settings() -> None:
    """One factory: auth, socket timeouts, health checks, retries, client name; URL wins."""
    cfg = QueueConfig(redis_password="pw", redis_username="u", redis_max_connections=7)
    client = create_redis(cfg, client_name="llmq:pod-1")
    kw = client.connection_pool.connection_kwargs
    assert (kw["username"], kw["password"], kw["client_name"]) == ("u", "pw", "llmq:pod-1")
    assert (kw["socket_timeout"], kw["socket_connect_timeout"]) == (5.0, 2.0)
    assert kw["health_check_interval"] == 30
    assert kw["retry"].get_retries() == 3
    assert client.connection_pool.max_connections == 7
    by_url = QueueConfig(redis_url="rediss://a:b@valkey.example:6390/2", redis_password="pw")
    kw = create_redis(by_url).connection_pool.connection_kwargs
    assert (kw["host"], kw["port"], kw["password"], kw["db"]) == ("valkey.example", 6390, "b", 2)
    sync_kw = create_sync_redis(cfg).connection_pool.connection_kwargs
    assert (sync_kw["socket_timeout"], sync_kw["password"]) == (5.0, "pw")


class _FakeRunner:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


async def test_stop_closes_sync_runner_without_start() -> None:
    """Sync-only use (no start()) still gets its runner thread closed by stop()/close_sync()."""
    q = LLMQueue(QueueConfig(models=MODELS))
    runner = _FakeRunner()
    q._sync = runner  # type: ignore[assignment]  # stand-in runner
    await q.stop()
    assert runner.closed
    assert q._sync is None
    q.close_sync()  # idempotent


def test_unwrap_puts_the_reply_meta_on_the_raised_error() -> None:
    """Callers see attempts, queue wait and the last target on a failed call too."""
    from taskiq_llm_queue.client import _unwrap
    from taskiq_llm_queue.envelope import CallMeta, LLMReply
    from taskiq_llm_queue.errors import LLMQueueError, LLMQueueTimeout, ProviderError

    meta = CallMeta(served_model="big", served_target="apac", attempts=6, queue_wait_ms=1500)
    throttled = LLMReply(
        request_id="r",
        ok=False,
        meta=meta,
        error={
            "type": "ThrottlingException",
            "message": "x",
            "retryable": True,
            "kind": "throttle",
        },
    )
    with pytest.raises(ProviderError) as caught:
        _unwrap(throttled)
    assert caught.value.meta == meta
    expired = throttled.model_copy(update={"error": {"type": "deadline_exceeded"}})
    with pytest.raises(LLMQueueTimeout) as timed_out:
        _unwrap(expired)
    assert timed_out.value.meta == meta
    assert LLMQueueError("plain").meta is None


async def test_schema_key_written_once_and_newer_state_refused(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """I15: start writes `<prefix>:schema`; newer state raises, older state logs an error."""
    import fakeredis

    from taskiq_llm_queue.keys import SCHEMA_VERSION

    redis = fakeredis.FakeAsyncRedis()
    q = LLMQueue(QueueConfig(models=MODELS))
    key = q.keys.schema
    await q._check_schema(redis)
    assert await redis.get(key) == str(SCHEMA_VERSION).encode()
    await redis.set(key, SCHEMA_VERSION + 1)
    with pytest.raises(ConfigError, match="newer"):
        await q._check_schema(redis)
    await redis.set(key, SCHEMA_VERSION - 1)
    await q._check_schema(redis)
    assert "schema_migration_needed" in caplog.text
    assert await redis.get(key) == str(SCHEMA_VERSION - 1).encode()  # never overwritten
