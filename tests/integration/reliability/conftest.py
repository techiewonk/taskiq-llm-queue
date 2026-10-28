"""Fixtures for reliability tests against the real Valkey of the Docker stack.

Each test gets its own key prefix and its keys are deleted afterwards. Lua is only trusted on a
real server (fakeredis Lua differs), so these tests skip when Valkey is not reachable.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import redis
import redis.asyncio
from redis.exceptions import RedisError

from taskiq_llm_queue.keys import KeySpace

VALKEY_HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("LLMQ_TEST_REDIS_PORT", "6379"))
VALKEY_PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")


@pytest.fixture
async def valkey() -> AsyncIterator[redis.asyncio.Redis]:
    """A client on the package's settings (no decode_responses), skipped if Valkey is down."""
    client = redis.asyncio.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD)
    try:
        await client.ping()
    except (RedisError, OSError) as exc:
        await client.aclose()
        pytest.skip(f"Valkey not reachable: {exc}")
    yield client
    await client.aclose()


@pytest.fixture
async def keys(valkey: redis.asyncio.Redis) -> AsyncIterator[KeySpace]:
    """A unique `KeySpace` per test; every key under it is deleted at teardown."""
    space = KeySpace(f"llmqtest:{uuid.uuid4().hex[:12]}")
    yield space
    found = [key async for key in valkey.scan_iter(match=f"{space.prefix}:*", count=500)]
    if found:
        await valkey.delete(*found)


@pytest.fixture
def sync_valkey(valkey: redis.asyncio.Redis) -> Iterator[redis.Redis]:
    """A blocking client for the sync limiter path (DS18)."""
    client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD)
    yield client
    client.close()
