"""Fixtures for the results unit tests: a fakeredis client and a backend on top of it."""

from __future__ import annotations

from collections.abc import AsyncIterator

import fakeredis
import pytest

from taskiq_llm_queue import KeySpace
from taskiq_llm_queue.results import LLMQResultBackend


@pytest.fixture
async def fake_redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    client = fakeredis.FakeAsyncRedis()
    yield client
    await client.aclose()


@pytest.fixture
def backend(fake_redis: fakeredis.FakeAsyncRedis) -> LLMQResultBackend:
    return LLMQResultBackend(KeySpace("llmq:unit"), redis=fake_redis, result_ttl_secs=300)
