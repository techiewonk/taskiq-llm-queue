"""Fixtures for engine tests: each test runs on Valkey 8.1 and on Redis 7.4 (plan E8).

Every test gets its own key prefix, deleted at teardown. The stack comes from
`scripts/stack.py up`; a store that does not answer fails the test (no silent skip).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import redis.asyncio

PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")
ENGINES = {"valkey": 6379, "redis": 6380}


@pytest.fixture(params=sorted(ENGINES))
async def store(request: pytest.FixtureRequest) -> AsyncIterator[redis.asyncio.Redis]:
    """An async client (no decode_responses, as in the library) on one engine."""
    client = redis.asyncio.Redis(host="127.0.0.1", port=ENGINES[request.param], password=PASSWORD)
    await client.ping()
    yield client
    await client.aclose()


@pytest.fixture
async def prefix(store: redis.asyncio.Redis) -> AsyncIterator[str]:
    """Unique key prefix; every key under it is deleted at teardown."""
    value = f"llmqtest:eng:{uuid.uuid4().hex[:12]}"
    yield value
    keys = [k async for k in store.scan_iter(match=f"{value}:*", count=500)]
    if keys:
        await store.delete(*keys)
