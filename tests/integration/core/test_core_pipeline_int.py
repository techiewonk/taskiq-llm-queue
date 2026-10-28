"""`execute_job` on real Valkey with the real stores: round trips per call and DS8 tail order.

Each test gets its own key prefix; the provider is an in-memory fake, everything else (claim,
limiter and result Lua, pipelines, pub/sub) runs on the Docker stack's Valkey.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from typing import Any

import orjson
import pytest
import redis.asyncio
from redis.asyncio.client import Pipeline
from redis.exceptions import RedisError

from taskiq_llm_queue.config import ModelConfig, QueueConfig, Target
from taskiq_llm_queue.core import CoreDeps, execute_job
from taskiq_llm_queue.envelope import Envelope
from taskiq_llm_queue.errors import ErrorInfo, ErrorKind
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import ClaimState
from taskiq_llm_queue.reliability import AdaptiveLimiter, ClaimSettings, RedisClaimStore
from taskiq_llm_queue.results import LLMQResultBackend

pytestmark = pytest.mark.integration

HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "localhost")
PORT = int(os.environ.get("LLMQ_TEST_REDIS_PORT", "6379"))
PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")


class EchoProvider:
    """Answers every call at once."""

    name = "echo"

    async def call(self, op: str, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        return {"output": "ok"}

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(ErrorKind.FATAL, "unknown")


@pytest.fixture
async def valkey() -> AsyncIterator[redis.asyncio.Redis]:
    client = redis.asyncio.Redis(host=HOST, port=PORT, password=PASSWORD)
    try:
        await client.ping()
    except (RedisError, OSError) as exc:
        await client.aclose()
        pytest.skip(f"Valkey not reachable: {exc}")
    yield client
    await client.aclose()


@pytest.fixture
async def keys(valkey: redis.asyncio.Redis) -> AsyncIterator[KeySpace]:
    space = KeySpace(f"llmqtest:{uuid.uuid4().hex[:12]}")
    yield space
    found = [key async for key in valkey.scan_iter(match=f"{space.prefix}:*", count=500)]
    if found:
        await valkey.delete(*found)


def make_deps(valkey: redis.asyncio.Redis, keys: KeySpace) -> CoreDeps:
    config = QueueConfig(
        models={
            "m": ModelConfig(
                provider="bedrock", model_id="m-id", targets={"primary": Target(region="r")}
            )
        }
    )
    return CoreDeps(
        config=config,
        keys=keys,
        providers=lambda alias: EchoProvider(),
        limiter=AdaptiveLimiter(valkey, keys),
        claims=RedisClaimStore(valkey, keys, ClaimSettings(state_ttl_secs=60)),
        results=LLMQResultBackend(keys, redis=valkey),
        redis=valkey,
        worker="pod-1",
    )


async def by_ref(valkey: redis.asyncio.Redis, keys: KeySpace, rid: str) -> Envelope:
    """An envelope whose params travel by reference and whose result wakes pod `caller`."""
    ref = keys.params(rid)
    await valkey.set(ref, orjson.dumps({"messages": []}), ex=60)
    now = time.time()
    return Envelope(
        request_id=rid,
        op="converse",
        model="m",
        params_ref=ref,
        reply_to="caller",
        enqueued_at=now,
        deadline_at=now + 30,
    )


class Trips:
    """Counts direct commands on one client and pipeline executions (one round trip each)."""

    def __init__(self, client: redis.asyncio.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
        self.direct: list[str] = []
        self.pipelines = 0
        original: Any = client.execute_command
        execute = Pipeline.execute

        async def direct(*args: Any, **options: Any) -> Any:
            self.direct.append(str(args[0]))
            return await original(*args, **options)

        async def pipelined(pipe: Pipeline, raise_on_error: bool = True) -> Any:
            self.pipelines += 1
            return await execute(pipe, raise_on_error)

        monkeypatch.setattr(client, "execute_command", direct)
        monkeypatch.setattr(Pipeline, "execute", pipelined)


async def test_worker_call_round_trips(
    valkey: redis.asyncio.Redis, keys: KeySpace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Item 5: claim, params, acquire, cancel check, release, then ONE pipeline for the tail.

    Before: 10 round trips (pre-acquire EXISTS, and SET / complete / PUBLISH / DEL one by one).
    """
    deps = make_deps(valkey, keys)
    await execute_job(await by_ref(valkey, keys, "warm"), deps)  # loads every script once
    pubsub = valkey.pubsub()
    await pubsub.subscribe(keys.done_channel("caller"))
    envelope = await by_ref(valkey, keys, "r-1")
    trips = Trips(valkey, monkeypatch)
    result = await execute_job(envelope, deps)
    monkeypatch.undo()
    assert result.reply is not None
    assert result.reply.ok
    assert trips.direct == ["EVALSHA", "GET", "EVALSHA", "EXISTS", "EVALSHA"]
    assert trips.pipelines == 1
    assert deps.results is not None
    assert (await deps.results.take_many(["r-1"]))["r-1"].response == {"output": "ok"}
    assert deps.claims is not None
    assert await deps.claims.state("r-1") is ClaimState.DONE
    assert not await valkey.exists(keys.params("r-1"))
    message = None
    for _ in range(20):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if message is not None:
            break
    assert message is not None
    assert message["data"] == b"r-1"
    await pubsub.aclose()  # type: ignore[no-untyped-call]


async def test_tail_reloads_a_flushed_claim_script(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """NOSCRIPT inside the tail pipeline (server restarted): the CAS is re-run on its own."""
    deps = make_deps(valkey, keys)
    assert isinstance(deps.claims, RedisClaimStore)
    deps.claims._complete.sha = "0" * 40  # a script the server does not know
    envelope = await by_ref(valkey, keys, "r-2")
    result = await execute_job(envelope, deps)
    assert result.reply is not None
    assert result.reply.ok
    assert await deps.claims.state("r-2") is ClaimState.DONE
    assert await valkey.exists(keys.result("r-2"))


async def test_cancel_flag_set_before_claim_is_still_honoured(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """DS14 with the folded check: the claim script sees the flag, nothing calls the provider."""
    deps = make_deps(valkey, keys)
    assert deps.claims is not None
    await deps.claims.cancel("r-3")
    envelope = await by_ref(valkey, keys, "r-3")
    result = await execute_job(envelope, deps)
    assert result.reply is None
    reply = (await LLMQResultBackend(keys, redis=valkey).take_many(["r-3"]))["r-3"]
    assert reply.cancelled
