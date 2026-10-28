"""Live config store on real Valkey: audited set/reset, last-good on invalid data, trimming."""

from __future__ import annotations

import json

import pytest
import redis.asyncio

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import FallbackConfig, LiveConfigStore, fallback_store

pytestmark = pytest.mark.integration

CHAIN = {"chains": {"gpt20b": ["gpt120b"]}}


async def test_set_writes_value_and_audit(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    store = fallback_store(valkey, keys)
    stored = await store.set(CHAIN, actor="alice")
    assert stored.chains["gpt20b"][0].model == "gpt120b"
    assert json.loads(await valkey.get(keys.fallback) or b"{}")["chains"]["gpt20b"]
    await store.set({"chains": {}}, actor="bob")
    [newest, oldest] = await store.history()
    assert oldest["actor"] == "alice"
    assert oldest["before"] == ""
    assert newest["actor"] == "bob"
    assert json.loads(newest["before"]) == json.loads(oldest["after"])
    assert newest["key"] == keys.fallback
    assert float(newest["ts"]) > 0


async def test_invalid_set_raises_and_writes_nothing(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    store = fallback_store(valkey, keys)
    with pytest.raises(ConfigError):
        await store.set({"chains": {"a": ["a"]}}, actor="alice")
    assert await valkey.get(keys.fallback) is None
    assert await valkey.exists(keys.audit) == 0


async def test_invalid_stored_value_keeps_last_good(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    store = fallback_store(valkey, keys)
    await store.set(CHAIN, actor="alice")
    reader = fallback_store(valkey, keys)
    good = await reader.load()
    assert good.chains
    await valkey.set(keys.fallback, b'{"chains": 42}')
    assert await reader.load() == good
    assert reader.invalid_count == 1
    await valkey.set(keys.fallback, b"not json")
    assert await reader.load() == good
    assert reader.invalid_count == 2


async def test_reset_returns_default_with_audit(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    default = FallbackConfig.model_validate({"chains": {"x": ["y"]}})
    store = fallback_store(valkey, keys, default)
    await store.set(CHAIN, actor="alice")
    assert await store.reset(actor="bob") == default
    assert await valkey.get(keys.fallback) is None
    assert await store.load() == default
    assert (await store.history())[0]["after"] == ""


async def test_audit_is_trimmed_to_retention(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    await valkey.xadd(keys.audit, {"key": keys.fallback, "actor": "ancient"}, id="1-0")
    store = LiveConfigStore(
        valkey, keys.fallback, FallbackConfig, FallbackConfig(), audit_key=keys.audit
    )
    await store.set(CHAIN, actor="alice")
    actors = [row["actor"] for row in await store.history()]
    assert actors == ["alice"]


async def test_history_filters_by_key(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    fallback = fallback_store(valkey, keys)
    routing = LiveConfigStore(
        valkey, keys.routing, FallbackConfig, FallbackConfig(), audit_key=keys.audit
    )
    await fallback.set(CHAIN, actor="alice")
    await routing.set({}, actor="bob")
    assert [row["actor"] for row in await fallback.history()] == ["alice"]
    assert [row["actor"] for row in await routing.history()] == ["bob"]
