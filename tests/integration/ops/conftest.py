"""Fixtures for the ops (CLI, routing, job send) tests against the real Docker stack.

Each test gets its own key prefix, removed at teardown; the CLI runs in-process with an
explicit `LLMQ_*` environment. Tests skip when Valkey or ElasticMQ does not answer.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator

import pytest
import redis
from redis.exceptions import RedisError

VALKEY_HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("LLMQ_TEST_REDIS_PORT", "6379"))
VALKEY_PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")
SQS_ENDPOINT = os.environ.get("LLMQ_TEST_SQS_ENDPOINT", "http://localhost:9324")

MODELS = {
    "gpt120b": {
        "provider": "openai",
        "model_id": "gpt-oss-120b",
        "targets": {"primary": {}, "apac": {"region": "ap-southeast-1"}},
    },
    "gpt20b": {"provider": "openai", "model_id": "gpt-oss-20b"},
}


@pytest.fixture
def sync_valkey() -> Iterator[redis.Redis]:
    """Blocking client on the stack's Valkey (no decode_responses), skipped if it is down."""
    client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD)
    try:
        client.ping()
    except (RedisError, OSError) as exc:
        client.close()
        pytest.skip(f"Valkey not reachable: {exc}")
    yield client
    client.close()


@pytest.fixture
def prefix(sync_valkey: redis.Redis) -> Iterator[str]:
    """Unique key prefix; every key under it is deleted at teardown."""
    value = f"llmqtest:ops:{uuid.uuid4().hex[:12]}"
    yield value
    found = list(sync_valkey.scan_iter(match=f"{value}:*", count=500))
    if found:
        sync_valkey.delete(*found)


@pytest.fixture
def cli_env(prefix: str, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """`LLMQ_*` settings for `cli.main(environ=...)`; AWS test credentials for ElasticMQ."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    return {
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REDIS_HOST": VALKEY_HOST,
        "LLMQ_REDIS_PORT": str(VALKEY_PORT),
        "LLMQ_REDIS_PASSWORD": VALKEY_PASSWORD,
        "LLMQ_SQS_ENDPOINT_URL": SQS_ENDPOINT,
        "LLMQ_AWS_REGION": "ap-south-1",
        "LLMQ_MODELS": json.dumps(MODELS),
    }
