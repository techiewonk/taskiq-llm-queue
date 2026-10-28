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
    LLMQueueError,
    LLMQueueTimeout,
    LLMReply,
    ModelConfig,
    ProviderError,
    QueueConfig,
    Target,
)
from taskiq_llm_queue.client import LLMQueue, _unwrap, decode_invoke
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
            "LLMQ_AC_PAUSE_S": "1.5",
            "LLMQ_AC_INITIAL": '{"big": 40}',
            "LLMQ_CB_THRESHOLD": "9",
            "LLMQ_WORKER_SLOTS": "big:300, jobs:4",
            "LLMQ_RESULT_DISPATCH_MS": "rt:50,bulk:1000",
            "LLMQ_TIMEOUT_RT": "30",
            "LLMQ_TIMEOUT_EMBED_BULK": "120",
            "LLMQ_CONSUMER_DRAIN_S": "20",
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
            {"LLMQ_AC_MIN": "x", "LLMQ_WORKER_SLOTS": "big", "LLMQ_TIMEOUT_RT": "soon"}
        )
    for name in ("AC_MIN", "WORKER_SLOTS", "TIMEOUT_RT"):
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
    with pytest.raises(LLMQueueError, match="cancelled"):
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
