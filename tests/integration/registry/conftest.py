"""Fixtures for the registry store tests against the Docker stack's Valkey.

Each test gets its own key prefix, deleted at teardown; tests skip when Valkey does not answer.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
import redis.asyncio
from redis.exceptions import RedisError

HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "localhost")
PORT = int(os.environ.get("LLMQ_TEST_REDIS_PORT", "6379"))
PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")
FIXTURES = Path(__file__).parents[2] / "unit" / "registry" / "fixtures"


@pytest.fixture
def prefix() -> Iterator[str]:
    """Unique key prefix; every key under it is deleted at teardown."""
    client = redis.Redis(host=HOST, port=PORT, password=PASSWORD)
    try:
        client.ping()
    except (RedisError, OSError) as exc:
        client.close()
        pytest.skip(f"Valkey not reachable: {exc}")
    value = f"llmqtest:reg:{uuid.uuid4().hex[:12]}"
    yield value
    found = list(client.scan_iter(match=f"{value}:*", count=500))
    if found:
        client.delete(*found)
    client.close()


@pytest.fixture
async def valkey(prefix: str) -> AsyncIterator[redis.asyncio.Redis]:
    """Async client on the stack's Valkey (no decode_responses, as in the library)."""
    client = redis.asyncio.Redis(host=HOST, port=PORT, password=PASSWORD)
    yield client
    await client.aclose()


@pytest.fixture
def full() -> dict[str, Any]:
    """The plan's full example registry."""
    data: dict[str, Any] = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    return data
