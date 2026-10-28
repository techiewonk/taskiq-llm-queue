"""Fixtures: a local fake LLM server and real-SDK providers pointed at it."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from aiohttp.test_utils import TestServer
from providers_fake import FakeLLM

from taskiq_llm_queue.providers import BedrockProvider, OpenAICompatProvider

FAKE_CREDENTIALS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}


@pytest.fixture
async def fake() -> AsyncIterator[FakeLLM]:
    state = FakeLLM()
    server = TestServer(state.app(), host="127.0.0.1")
    await server.start_server()
    state.url = str(server.make_url("")).rstrip("/")
    try:
        yield state
    finally:
        await server.close()


@pytest.fixture
async def bedrock(fake: FakeLLM) -> AsyncIterator[BedrockProvider]:
    provider = BedrockProvider(
        region="ap-south-1",
        endpoint_url=fake.url,
        read_timeout_secs=5,
        client_kwargs=FAKE_CREDENTIALS,
    )
    try:
        yield provider
    finally:
        await provider.aclose()


@pytest.fixture
async def openai_provider(fake: FakeLLM) -> AsyncIterator[OpenAICompatProvider]:
    provider = OpenAICompatProvider(
        base_url=f"{fake.url}/v1", api_keys={None: "sk-test"}, timeout_secs=5
    )
    try:
        yield provider
    finally:
        await provider.aclose()
