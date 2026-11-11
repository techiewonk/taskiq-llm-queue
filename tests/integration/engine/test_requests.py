"""Request records on Valkey and Redis (plan step A4; scenarios B48, B74, B101 store parts).

Duplicate ids attach to the first run, conflict on other params, take over an unconfirmed send;
results are stored once, read without deleting, and wake every waiting caller; a final status
is never replaced.
"""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio

from taskiq_llm_queue.engine.envelope import Envelope, Reply, params_hash, parse
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.requests import RequestStore
from taskiq_llm_queue.errors import UnsupportedEnvelopeError

pytestmark = pytest.mark.integration


def _store(client: redis.asyncio.Redis, prefix: str) -> RequestStore:
    return RequestStore(client, Keys(prefix), result_ttl_secs=300, result_dedupe_secs=2)


async def _register(store: RequestStore, caller: str, params: str = "p", user: str = "u") -> str:
    found = await store.register(
        "req-1",
        caller=caller,
        user_hash=user,
        params_hash=params,
        model="gpt120b",
        priority=True,
        app_id=False,
    )
    return found.outcome


async def test_duplicate_ids(store: redis.asyncio.Redis, prefix: str) -> None:
    requests = _store(store, prefix)
    assert await _register(requests, "pod-a") == "new"
    assert await _register(requests, "pod-b") == "takeover"  # not confirmed yet
    await requests.confirm("req-1", keep_secs=120)
    assert await _register(requests, "pod-c") == "attached"
    assert await _register(requests, "pod-d", params="other") == "conflict"
    assert await _register(requests, "pod-d", user="other") == "conflict"
    assert set(await requests.callers("req-1")) == {"pod-a", "pod-b", "pod-c"}
    status = await requests.status("req-1")
    assert status["state"] == "waiting"
    assert status["model"] == "gpt120b"


async def test_failed_send_leaves_nothing(store: redis.asyncio.Redis, prefix: str) -> None:
    requests = _store(store, prefix)
    assert await _register(requests, "pod-a") == "new"
    await requests.abandon("req-1", caller="pod-b")  # not the owner: kept
    assert await requests.job("req-1")
    await requests.abandon("req-1", caller="pod-a")
    assert not await requests.job("req-1")
    assert not await requests.status("req-1")
    assert await _register(requests, "pod-a") == "new"


async def test_result_once_and_wakes_every_caller(store: redis.asyncio.Redis, prefix: str) -> None:
    requests = _store(store, prefix)
    await _register(requests, "pod-a")
    await requests.confirm("req-1", keep_secs=120)
    await _register(requests, "pod-b")
    pubsub = store.pubsub()
    await pubsub.subscribe(f"{prefix}:done:pod-a", f"{prefix}:done:pod-b")
    reply = Reply(request_id="req-1", ok=True, response={"x": 1})
    assert await requests.put_result(reply, final_state="done") is True
    assert (
        await requests.put_result(
            reply.model_copy(update={"response": {"x": 2}}), final_state="done"
        )
        is False
    )
    woken = set()
    for _ in range(10):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if message:
            woken.add(message["channel"].decode().rsplit(":", 1)[1])
        if len(woken) == 2:
            break
    assert woken == {"pod-a", "pod-b"}
    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis-py stubs lack it
    first = await requests.take_result("req-1")
    again = await requests.take_result("req-1")  # read without deleting
    assert first is not None
    assert again is not None
    assert first.response == again.response == {"x": 1}
    ttl = await store.ttl(f"{prefix}:{{r:req-1}}:res")
    assert 0 < ttl <= 2  # library id: short life after the first read
    assert await _register(requests, "pod-c") == "done"
    await asyncio.sleep(2.2)
    assert await requests.take_result("req-1") is None
    assert await _register(requests, "pod-c") == "expired"  # result_expired


async def test_app_ids_keep_their_result(store: redis.asyncio.Redis, prefix: str) -> None:
    requests = _store(store, prefix)
    await requests.register(
        "req-1",
        caller="pod-a",
        user_hash="u",
        params_hash="p",
        model="m",
        priority=False,
        app_id=True,
    )
    await requests.put_result(Reply(request_id="req-1", ok=True), final_state="done")
    await requests.take_result("req-1")
    assert await store.ttl(f"{prefix}:{{r:req-1}}:res") > 250


async def test_final_status_is_never_replaced(store: redis.asyncio.Redis, prefix: str) -> None:
    requests = _store(store, prefix)
    assert await requests.set_status("req-1", "waiting", model="m")
    assert await requests.set_status("req-1", "done", tokens_in=10)
    assert not await requests.set_status("req-1", "waiting")  # a late write loses
    status = await requests.status("req-1")
    assert status["state"] == "done"
    assert status["tokens_in"] == "10"
    assert "done_at" in status
    assert await store.ttl(f"{prefix}:{{r:req-1}}:status") > 1000


def test_envelope_version_rule() -> None:
    env = Envelope(
        request_id="r", op="converse", model="m", params={"a": 1}, caller="c", timeout_secs=120
    )
    assert parse(env.dumps()) == env
    with pytest.raises(UnsupportedEnvelopeError):
        parse(b'{"v": 3, "request_id": "r"}')
    with pytest.raises(ValueError, match="not supported"):
        parse(b'{"v": 1, "request_id": "r"}')
    with pytest.raises(ValueError, match="params"):
        Envelope(request_id="r", op="converse", model="m", caller="c", timeout_secs=1)
    assert params_hash({"b": 1, "a": 2}) == params_hash({"a": 2, "b": 1})
