"""Config store read path on fakeredis: default, last good on invalid data or no Valkey."""

from __future__ import annotations

import asyncio
import subprocess
import sys

import fakeredis
import pytest
import structlog

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import FallbackConfig, LiveConfigStore, fallback_store

KEYS = KeySpace("llmq:unit")
GOOD = b'{"chains": {"a": ["b"]}}'


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


@pytest.fixture
def client(server: fakeredis.FakeServer) -> fakeredis.FakeAsyncRedis:
    return fakeredis.FakeAsyncRedis(server=server)


async def test_missing_key_gives_default(client: fakeredis.FakeAsyncRedis) -> None:
    default = FallbackConfig.model_validate({"chains": {"x": ["y"]}})
    store = fallback_store(client, KEYS, default)
    assert store.current == default
    assert await store.load() == default


async def test_invalid_data_keeps_last_good_and_warns_once(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    store = fallback_store(client, KEYS)
    await client.set(KEYS.fallback, GOOD)
    good = await store.load()
    assert good.chains["a"][0].model == "b"
    await client.set(KEYS.fallback, b'{"chains": {"a": ["a"]}}')
    with structlog.testing.capture_logs() as logs:
        for _ in range(3):
            assert await store.load() == good
    assert store.invalid_count == 3
    (event,) = logs
    assert (event["event"], event["log_level"]) == ("live_config_invalid", "warning")
    assert (event["key"], event["invalid_count"]) == (KEYS.fallback, 1)


async def test_valkey_down_keeps_last_good(
    server: fakeredis.FakeServer, client: fakeredis.FakeAsyncRedis
) -> None:
    store = fallback_store(client, KEYS)
    await client.set(KEYS.fallback, GOOD)
    good = await store.load()
    server.connected = False
    assert await store.load() == good


def test_parse_accepts_json_mapping_and_model() -> None:
    store = fallback_store(fakeredis.FakeAsyncRedis(), KEYS)
    model = store.parse(GOOD)
    assert store.parse({"chains": {"a": ["b"]}}) == model
    assert store.parse(model) is model
    with pytest.raises(ConfigError):
        store.parse(b"{")


async def test_run_reload_polls_until_cancelled(client: fakeredis.FakeAsyncRedis) -> None:
    sleeps: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)
        if len(sleeps) == 2:
            await client.set(KEYS.fallback, GOOD)
        if len(sleeps) == 4:
            raise asyncio.CancelledError

    store = LiveConfigStore(
        client,
        KEYS.fallback,
        FallbackConfig,
        FallbackConfig(),
        audit_key=KEYS.audit,
        sleep=fake_sleep,
    )
    with pytest.raises(asyncio.CancelledError):
        await store.run_reload(30.0)
    assert sleeps == [30.0] * 4
    assert store.current.chains


def test_import_has_no_side_effects() -> None:
    """Library rule: importing the package must not open connections or start threads."""
    code = (
        "import threading, taskiq_llm_queue.reliability as r;"
        "assert threading.active_count() == 1, threading.enumerate();"
        "assert r.AdaptiveLimiter"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
