"""Fixtures for fakellm tests: a live aiohttp server plus real aiobotocore / openai clients.

`fakellm_testkit` (imported first) puts tools/fakellm on sys.path. Every SDK runs with
retries off so one request maps to exactly one fakellm call.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack
from typing import Any

import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from aiohttp.test_utils import TestServer
from fakellm_testkit import Engine, Fake, MakeBedrock, Scenario, StartFake, create_app
from openai import AsyncOpenAI
from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient


@pytest.fixture
async def stack() -> AsyncIterator[AsyncExitStack]:
    async with AsyncExitStack() as exit_stack:
        yield exit_stack


@pytest.fixture
def start_fake(stack: AsyncExitStack) -> StartFake:
    async def start(scenario: dict[str, Any] | None = None, **engine_kwargs: Any) -> Fake:
        engine = Engine(Scenario.from_mapping(scenario or {}), **engine_kwargs)
        server = TestServer(create_app(engine=engine), host="127.0.0.1")
        await server.start_server()
        stack.push_async_callback(server.close)
        return Fake(str(server.make_url("")).rstrip("/"), engine)

    return start


@pytest.fixture
def make_bedrock(stack: AsyncExitStack) -> MakeBedrock:
    async def make(fake: Fake, region: str = "ap-south-1") -> BedrockRuntimeClient:
        client = get_session().create_client(
            "bedrock-runtime",
            region_name=region,
            endpoint_url=fake.url,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=AioConfig(retries={"max_attempts": 1, "mode": "standard"}),
        )
        return await stack.enter_async_context(client)

    return make


@pytest.fixture
def make_openai(stack: AsyncExitStack) -> Callable[[Fake], AsyncOpenAI]:
    def make(fake: Fake) -> AsyncOpenAI:
        client = AsyncOpenAI(base_url=f"{fake.url}/v1", api_key="test", max_retries=0)
        stack.push_async_callback(client.close)
        return client

    return make
