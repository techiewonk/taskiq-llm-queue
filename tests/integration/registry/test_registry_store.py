"""Registry store against real Valkey (step A2; scenarios B12 stored half, B40, B85, B69).

Covers the version rule, compare-and-set edits, protected fields, audit diffs with secrets
redacted, rollback, the lenient read and restoring a lost registry.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import orjson
import pytest
import redis.asyncio

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.registry import RegistryError
from taskiq_llm_queue.registry.store import LiveEditRefused, RegistryStore

pytestmark = pytest.mark.integration


def _store(
    client: redis.asyncio.Redis, prefix: str, doc: dict[str, Any], mode: str = "safe"
) -> RegistryStore:
    return RegistryStore(client, prefix, config=doc, live_edit=mode)


async def test_version_rule(valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]) -> None:
    store = _store(valkey, prefix, full)
    view = await store.boot()
    assert view.registry.version == 1
    assert (await store.publish(full, actor="t")).action == "kept"

    changed = copy.deepcopy(full)
    changed["models"]["gpt20b"]["rpm"] = 999
    result = await store.publish(changed, actor="t")  # same version, other content
    assert result.action == "not_bumped"
    assert store.current.value("gpt20b", "rpm") == 1000

    older = copy.deepcopy(full)
    older["registry_version"] = 0 + 1
    await store.set({"rpm": 700}, model="gpt20b", actor="op")
    assert (await store.publish(older, actor="t")).action == "kept"  # equal keeps live edits
    assert store.current.value("gpt20b", "rpm") == 700

    newer = copy.deepcopy(full)
    newer["registry_version"] = 2
    assert (await store.publish(newer, actor="t")).action == "published"
    assert store.current.value("gpt20b", "rpm") == 1000  # higher replaces, edit gone
    assert (await store.publish(full, actor="t")).action == "ignored"  # lower ignored
    assert store.current.registry.version == 2


async def test_live_edit_updates_numbers_and_audit(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    before = store.changes
    await store.set(
        {"deployments.aws-main.overrides.ap-south-1.rpm": 400}, model="gpt120b", actor="op"
    )
    assert store.changes == before + 1
    assert store.current.value("gpt120b", "rpm", "aws-main/ap-south-1") == 400

    numbers = await valkey.hgetall(store.numbers_key("gpt120b"))
    assert int(numbers[b"changes"]) == store.changes
    rows = orjson.loads(numbers[b"numbers"])["deployments"]
    assert next(r for r in rows if r["key"] == "aws-main/ap-south-1")["rpm"] == 400

    entries = await store.log(5, path_prefix="models.gpt120b")
    assert entries[0]["actor"] == "op"
    assert entries[0]["op"] == "edit"
    assert entries[0]["diff"] == [
        {
            "path": "models.gpt120b.deployments.aws-main.overrides.ap-south-1.rpm",
            "before": None,
            "after": 400,
        }
    ]


@pytest.mark.parametrize(
    ("values", "model", "fragment"),
    [
        ({"accounts.vllm-dc1.base_url": "https://evil.example.com/v1"}, None, "account `base_url`"),
        ({"deployments.aws-main.confirmed": ["ap-south-1"]}, "gpt120b", "region lists"),
        (
            {"deployments.aws-main.overrides.ap-south-1.params": {"x": 1}},
            "gpt120b",
            "deployment params",
        ),
        ({"residency": {"allowed_regions": ["us-east-1"]}}, None, "residency"),
        ({"model_id": "openai.other"}, "gpt20b", "changes only with a deploy"),
        ({"models.new-model": None, "models.gpt20b": None}, None, "adding or removing a model"),
    ],
)
async def test_protected_fields_refused_under_safe(
    valkey: redis.asyncio.Redis,
    prefix: str,
    full: dict[str, Any],
    values: dict[str, Any],
    model: str | None,
    fragment: str,
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    changes = store.changes
    with pytest.raises(LiveEditRefused, match=fragment):
        await store.set(values, model=model, actor="op")
    await store.load()
    assert store.changes == changes  # nothing written


async def test_modes_all_and_off(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full, mode="all")
    await store.boot()
    await store.set({"deployments.aws-main.fallback": ["us-east-1"]}, model="gpt20b", actor="op")
    assert [d.key for d in store.current.deployments("gpt20b", "fallback")] == [
        "aws-main/us-east-1"
    ]
    with pytest.raises(LiveEditRefused, match="deploy"):  # deploy-only stays refused
        await store.set({"model_id": "x"}, model="gpt20b", actor="op")
    off = _store(valkey, prefix, full, mode="off")
    await off.load()
    with pytest.raises(LiveEditRefused, match="live edits are off"):
        await off.set({"rpm": 1}, model="gpt20b", actor="op")


async def test_invalid_edit_writes_nothing(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    with pytest.raises(RegistryError, match="attempt_timeout_secs"):
        await store.set({"attempt_timeout_secs": 500}, model="gpt20b", actor="op")
    assert store.current.value("gpt20b", "attempt_timeout_secs") == 60


async def test_concurrent_edits_both_applied(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    first = _store(valkey, prefix, full)
    second = _store(valkey, prefix, full)
    await first.boot()
    await second.load()
    await asyncio.gather(
        first.set({"rpm": 111}, model="gpt20b", actor="a"),
        second.set({"rpm": 222}, model="gpt120b-vllm", actor="b"),
    )
    await first.load()
    assert first.current.value("gpt20b", "rpm") == 111
    assert first.current.value("gpt120b-vllm", "rpm") == 222


async def test_rollback_publishes_old_content_as_new_version(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    v2 = copy.deepcopy(full)
    v2["registry_version"] = 2
    v2["models"]["gpt20b"]["rpm"] = 5
    await store.publish(v2, actor="deploy")
    assert await store.versions() == [1, 2]
    result = await store.rollback(1, actor="op")
    assert (result.action, result.version) == ("published", 3)
    assert store.current.value("gpt20b", "rpm") == 1000
    with pytest.raises(ConfigError, match="not kept"):
        await store.rollback(99, actor="op")


async def test_lenient_read_and_last_good(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    newer = copy.deepcopy(full)
    newer["models"]["gpt20b"]["future_setting"] = 1  # written by a newer library version
    await valkey.set(store.doc_key, orjson.dumps(newer))
    assert await store.load() is not None
    assert any("future_setting" in w for w in store.current.registry.warnings)

    await valkey.set(store.doc_key, b"{broken")
    view = await store.load()
    assert view is not None
    assert view.value("gpt20b", "rpm") == 1000  # last good kept
    assert store.invalid_count == 1


async def test_lost_registry_is_restored(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    await valkey.delete(store.doc_key, store.meta_key)  # a flush or failover lost it
    await store.refresh()
    assert await valkey.get(store.doc_key) is not None
    assert store.current.registry.version == 1


async def test_audit_redacts_secrets(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full, mode="all")
    await store.boot()
    await store.set({"accounts.aws-dr.role_arn": "arn:aws:iam::333:role/new"}, actor="op")
    change = (await store.log(1))[0]["diff"][0]
    assert change["before"] == "[redacted]"
    assert change["after"] == "[redacted]"
    publish = (await store.log(5))[-1]
    assert "222222222222" not in orjson.dumps(publish).decode()


async def test_diff_enable_block(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    await store.set_enabled("gpt120b", "aws-main/ap-south-2", False, actor="op")
    assert store.current.value("gpt120b", "enabled", "aws-main/ap-south-2") is False
    await store.block(region="us-east-1", on=True, actor="op")
    assert store.current.registry.routing.blocked_regions == ("us-east-1",)
    paths = [c["path"] for c in await store.diff_with_config()]
    assert "models.gpt120b.deployments.aws-main.overrides.ap-south-2.enabled" in paths
    assert "routing.blocked_regions" in paths
    await store.set_enabled("gpt120b", "aws-main/ap-south-2", True, actor="op")
    await store.block(region="us-east-1", on=False, actor="op")
    assert store.current.value("gpt120b", "enabled", "aws-main/ap-south-2") is True
    assert not store.current.registry.routing.blocked_regions


async def test_boot_without_store_runs_on_config(full: dict[str, Any]) -> None:
    client = redis.asyncio.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    store = RegistryStore(client, "llmqtest:nostore", config=full)
    view = await store.boot()
    assert view.value("gpt20b", "rpm") == 1000
    await client.aclose()
