"""Fixtures for the client and provider tests: an in-process fakellm per test."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiohttp.test_utils import TestServer
from clients_testkit import Engine, Scenario, StartFake, create_app


@pytest.fixture
async def start_fake() -> AsyncIterator[StartFake]:
    """Start fakellm with a scenario; returns (base URL, engine). Stopped at teardown."""
    servers: list[TestServer] = []

    async def start(scenario: dict[str, Any] | None = None) -> tuple[str, Engine]:
        engine = Engine(Scenario.from_mapping(scenario or {}))
        server = TestServer(create_app(engine=engine), host="127.0.0.1")
        await server.start_server()
        servers.append(server)
        return str(server.make_url("")).rstrip("/"), engine

    yield start
    for server in servers:
        await server.close()
