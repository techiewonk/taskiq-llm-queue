"""End-to-end tests of `LLMQueue` on the Docker stack: Valkey, ElasticMQ and fakellm.

Valkey (localhost:6379) and ElasticMQ (localhost:9324) are the shared stack; every test gets its
own key prefix and its own freshly created SQS queues. fakellm runs in-process on a random port
(same app and admin API as the Docker service) so behaviour patches never leak into other suites.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiobotocore.session import get_session
from aiohttp.test_utils import TestServer
from redis.asyncio import Redis

from taskiq_llm_queue import (
    LLMQueueCancelled,
    LLMQueueTimeout,
    ModelConfig,
    QueueConfig,
    QueueHooks,
    Target,
    request_context,
)
from taskiq_llm_queue.broker.errors import ErrorFamily, InvalidLabelError, SQSSendError
from taskiq_llm_queue.client import LLMQueue
from taskiq_llm_queue.providers import BedrockProvider, OpenAICompatProvider
from taskiq_llm_queue.routing import RoutingConfig

TOOLS = Path(__file__).resolve().parents[3] / "tools" / "fakellm"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from fakellm import Engine, Scenario, create_app  # noqa: E402  # needs the sys.path entry above

pytestmark = pytest.mark.integration

SQS_URL = os.environ.get("LLMQ_TEST_SQS_URL", "http://localhost:9324")
REDIS_HOST, REDIS_PORT, REDIS_PASSWORD = "localhost", 6379, "localdev"
REGION = "ap-south-1"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
BIG_KEY = "openai.gpt-oss-120b-1:0@ap-south-1"
SMALL_KEY = "openai.gpt-oss-20b-1:0@ap-south-1"
HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}

MakeQueue = Callable[..., Awaitable[LLMQueue]]


class Recorder(QueueHooks):
    """Records the path of every finished call."""

    def __init__(self) -> None:
        self.paths: list[tuple[str, str, bool]] = []

    def on_call(self, model: str, lane: str, path: str, ok: bool, secs: float) -> None:
        self.paths.append((model, path, ok))


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    client = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"Valkey not reachable: {exc}")
    yield client
    await client.aclose()


@pytest.fixture
async def fake() -> AsyncIterator[str]:
    """In-process fakellm with fast latencies; yields its base URL."""
    scenario = Scenario.from_mapping(
        {"seed": 1, "defaults": {"latency_ms": {"p50": 20, "p95": 40}}, "models": {}}
    )
    server = TestServer(create_app(engine=Engine(scenario)), host="127.0.0.1")
    await server.start_server()
    yield str(server.make_url("")).rstrip("/")
    await server.close()


async def patch_model(url: str, key: str, patch: dict[str, Any]) -> None:
    """fakellm admin API: change one model key's behaviour mid-test."""
    async with (
        aiohttp.ClientSession() as session,
        session.patch(f"{url}/admin/models/{key}", json=patch) as resp,
    ):
        assert resp.status == 200, await resp.text()


@pytest.fixture
async def queues() -> AsyncIterator[Callable[[str], str]]:
    """Creates uniquely named SQS queues on ElasticMQ on demand; deletes them afterwards."""
    tag = uuid.uuid4().hex[:8]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            get_session().create_client(
                "sqs",
                region_name=REGION,
                endpoint_url=SQS_URL,
                aws_access_key_id="test",
                aws_secret_access_key="test",
            )
        )
        try:
            await client.list_queues()
        except Exception as exc:
            pytest.skip(f"ElasticMQ not reachable: {exc}")
        names: dict[str, str] = {}

        def name(kind: str) -> str:
            return names.setdefault(kind, f"t{tag}-{kind}")

        yield name
        for queue in names.values():
            try:
                url = (await client.get_queue_url(QueueName=queue))["QueueUrl"]
                await client.delete_queue(QueueUrl=url)
            except Exception:  # never created (test skipped early)
                continue


async def ensure_queues(names: list[str]) -> None:
    async with get_session().create_client(
        "sqs",
        region_name=REGION,
        endpoint_url=SQS_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        for queue in names:
            await client.create_queue(QueueName=queue, Attributes={"VisibilityTimeout": "30"})


def base_config(fake: str, q: Callable[[str], str], **overrides: Any) -> QueueConfig:
    models = {
        "big": ModelConfig(
            provider="bedrock",
            model_id="openai.gpt-oss-120b-1:0",
            targets={
                "primary": Target(region=REGION),
                "apac": Target(profile_id="apac.openai.gpt-oss-120b-1:0"),
            },
            queues={"rt": q("big-rt"), "default": q("big")},
        ),
        "small": ModelConfig(
            provider="bedrock",
            model_id="openai.gpt-oss-20b-1:0",
            targets={"primary": Target(region=REGION)},
            queues={"rt": q("small-rt"), "default": q("small")},
        ),
        "titan": ModelConfig(
            provider="bedrock",
            model_id="amazon.titan-embed-text-v2:0",
            targets={"primary": Target(region=REGION)},
            queues={"default": q("embed"), "bulk": q("embed-bulk")},
        ),
        "cohere": ModelConfig(
            provider="bedrock",
            model_id="cohere.embed-english-v3",
            targets={"primary": Target(region=REGION)},
            queues={"default": q("embed")},
        ),
        "llama": ModelConfig(
            provider="openai",
            model_id="llama-3.1-70b",
            targets={"local": Target(base_url=f"{fake}/v1")},
            queues={"default": q("llama")},
            api_key_env="LLMQ_TEST_KEY",
        ),
    }
    fields: dict[str, Any] = {
        "key_prefix": f"llmqtest:{uuid.uuid4().hex[:10]}",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_password": REDIS_PASSWORD,
        "sqs_endpoint_url": SQS_URL,
        "bedrock_endpoint_url": fake,
        "aws_region": REGION,
        "models": models,
        "sqs_wait_time_secs": 1,
        "retry_base_secs": 0.05,
        "limiter": {"pause_secs": 0.2},
        "result_dispatch_ms": {"rt": 50, "bg": 50, "bulk": 50},
        "cfg_reload_secs": 1.0,
        "drain_secs": 2.0,
    }
    fields.update(overrides)
    return QueueConfig(**fields)


FACTORIES: dict[str, Any] = {
    "bedrock": lambda cfg: BedrockProvider.from_config(
        cfg, client_kwargs=CREDS, read_timeout_secs=10
    ),
    "openai": lambda cfg: OpenAICompatProvider.from_config(
        cfg, environ={"LLMQ_TEST_KEY": "sk-test"}
    ),
}


@pytest.fixture
async def make_queue(
    redis: Redis, fake: str, queues: Callable[[str], str]
) -> AsyncIterator[MakeQueue]:
    """Builds and starts `LLMQueue`s on fresh queues; stops them and drops their keys."""
    started: list[LLMQueue] = []
    prefixes: set[str] = set()

    async def make(
        mode: str = "both", config: QueueConfig | None = None, **kwargs: Any
    ) -> LLMQueue:
        cfg = config or base_config(fake, queues)
        await ensure_queues(sorted({q for m in cfg.models.values() for q in m.queues.values()}))
        llmq = LLMQueue(cfg, provider_factories=FACTORIES, **kwargs)
        await llmq.start(mode)  # type: ignore[arg-type]
        started.append(llmq)
        prefixes.add(cfg.key_prefix)
        return llmq

    yield make
    for llmq in started:
        await llmq.stop(drain_secs=1)
    for prefix in prefixes:
        found = [k async for k in redis.scan_iter(match=f"{prefix}:*", count=500)]
        if found:
            await redis.delete(*found)


# ---------------------------------------------------------------- happy paths


async def test_converse_end_to_end_via_sqs(make_queue: MakeQueue) -> None:
    """rt converse goes through the rt queue to an in-process worker and back (§6.1)."""
    hooks = Recorder()
    llmq = await make_queue(hooks=hooks)
    with request_context(lane="rt", session_id="s-1", user_id="u-1"):
        result = await llmq.converse("big", HELLO)
    assert result.response["output"]["message"]["role"] == "assistant"
    assert "ResponseMetadata" not in result.response
    meta = result.meta
    assert (meta.served_model, meta.served_target, meta.fallback_used) == ("big", "primary", False)
    assert meta.worker == llmq.pod_id
    assert hooks.paths == [("big", "sqs", True)]


async def test_invoke_model_returns_raw_body(make_queue: MakeQueue) -> None:
    """invoke_model gives Bedrock's raw bytes body and content type."""
    llmq = await make_queue()
    raw = await llmq.invoke_model("titan", {"inputText": "a"})
    assert raw.content_type.startswith("application/json")
    assert len(raw.json()["embedding"]) == 1024


async def test_embed_batches_per_provider(make_queue: MakeQueue) -> None:
    """Titan: one task per text; Cohere: 96 per task; vectors come back in order."""
    llmq = await make_queue()
    titan = await llmq.embed("titan", ["a", "b", "c"], lane="bulk")
    assert len(titan) == 3
    assert all(len(v) == 1024 for v in titan)
    assert titan[0] != titan[1]
    again = await llmq.embed("titan", ["b"])
    assert again[0] == titan[1]
    cohere = await llmq.embed("cohere", [f"t{i}" for i in range(100)], input_type="search_query")
    assert len(cohere) == 100


async def test_chat_via_openai_compatible(make_queue: MakeQueue) -> None:
    """OpenAI-compatible chat through SQS; the caller's `model` key is replaced by the alias'."""
    llmq = await make_queue()
    body = await llmq.chat(
        "llama", {"model": "ignored", "messages": [{"role": "user", "content": "hi"}]}
    )
    assert body.response["object"] == "chat.completion"
    assert body.response["model"] == "llama-3.1-70b"
    vectors = await llmq.embed("llama", ["x", "y"])
    assert len(vectors) == 2


async def test_large_params_travel_by_reference(
    make_queue: MakeQueue, fake: str, queues: Callable[[str], str], redis: Redis
) -> None:
    """Params over inline_max_bytes go to Valkey (`req:<id>`) and are removed after the call."""
    cfg = base_config(fake, queues, inline_max_bytes=2048)
    llmq = await make_queue(config=cfg)
    big = {"messages": [{"role": "user", "content": [{"text": "x" * 20_000}]}]}
    result = await llmq.converse("big", big, request_id="big-params-1")
    assert result.response["output"]["message"]["role"] == "assistant"
    assert not await redis.exists(llmq.keys.params("big-params-1"))


# ---------------------------------------------------------------- reliability


async def test_fallback_when_primary_throttles(make_queue: MakeQueue, fake: str) -> None:
    """fakellm throttles the primary region: retries, then the APAC profile serves (§8).

    A throttled step gets `throttle_retry_attempts` tries (S35b), so 6 on the primary, 1 on APAC.
    """
    await patch_model(fake, BIG_KEY, {"force_error": "ThrottlingException"})
    llmq = await make_queue()
    result = await llmq.converse("big", HELLO, lane="rt")
    meta = result.meta
    assert (meta.served_target, meta.fallback_used) == ("apac", True)
    assert meta.attempts == llmq.config.throttle_retry_attempts + 1


async def test_cross_instance_execution(make_queue: MakeQueue, fake: str, queues: Any) -> None:
    """A send-only instance's call runs on a separate work-only instance."""
    cfg = base_config(fake, queues)
    sender = await make_queue("send", config=cfg)
    worker = await make_queue("work", config=cfg)
    result = await sender.converse("big", HELLO)
    assert result.meta.worker == worker.pod_id != sender.pod_id


async def test_cancel_before_a_worker_picks_it_up(
    make_queue: MakeQueue, fake: str, queues: Any
) -> None:
    """DS14: a call cancelled while queued is acked as cancelled; the caller gets an error."""
    cfg = base_config(fake, queues)
    sender = await make_queue("send", config=cfg)
    call = asyncio.create_task(sender.converse("big", HELLO, request_id="cancel-me"))
    await asyncio.sleep(0.5)
    await sender.cancel("cancel-me")
    await make_queue("work", config=cfg)
    with pytest.raises(LLMQueueCancelled, match="cancelled"):
        await asyncio.wait_for(call, 20)


async def test_timeout_sets_cancel_flag(make_queue: MakeQueue, fake: str, redis: Redis) -> None:
    """A wait past `timeout_secs` raises LLMQueueTimeout and the task stops on the worker.

    Both sides end at the same deadline: the caller times out and flags the task cancelled, or
    the worker's deadline cap fires first and its `deadline_exceeded` reply (claim `expired`)
    raises the same LLMQueueTimeout.
    """
    await patch_model(fake, SMALL_KEY, {"latency_ms": {"p50": 4000, "p95": 4000}})
    llmq = await make_queue()
    t0 = time.monotonic()
    with pytest.raises(LLMQueueTimeout):
        await llmq.converse("small", HELLO, timeout_secs=1.0, request_id="slow-1")
    assert time.monotonic() - t0 < 3
    flagged = await redis.exists(llmq.keys.cancel("slow-1"))
    assert flagged or await redis.hget(llmq.keys.job("slow-1"), "state") == b"expired"


async def test_stop_drain_hands_back_running_work(
    make_queue: MakeQueue, fake: str, queues: Any
) -> None:
    """§7.4: stop() with a short drain releases the running task; another pod completes it."""
    await patch_model(fake, SMALL_KEY, {"latency_ms": {"p50": 2500, "p95": 2500}})
    cfg = base_config(fake, queues, worker_slots={"small": 1})
    sender = await make_queue("send", config=cfg)
    first = await make_queue("work", config=cfg)
    calls = [asyncio.create_task(sender.converse("small", HELLO)) for _ in range(2)]
    for _ in range(100):
        if first._state is not None and first._state.runtime.running:
            break
        await asyncio.sleep(0.05)
    assert first._state is not None
    assert first._state.runtime.running
    await first.stop(drain_secs=0.2)
    second = await make_queue("work", config=cfg)
    results = await asyncio.wait_for(asyncio.gather(*calls), 30)
    assert {r.meta.worker for r in results} == {second.pod_id}


# ---------------------------------------------------------------- routing and failover


async def test_kill_switch_routes_direct(make_queue: MakeQueue, fake: str, queues: Any) -> None:
    """With the canary router on, the live kill switch sends calls `direct` (same limiter)."""
    routing = {"models": {"big": {"rt": 100, "bg": 100}}}
    cfg = base_config(fake, queues, routing_enabled=True, routing_default=routing)
    hooks = Recorder()
    llmq = await make_queue(config=cfg, hooks=hooks)
    await llmq.converse("big", HELLO)
    router = llmq._own_router
    assert router is not None
    await router.store.set(RoutingConfig.model_validate(routing).with_kill(True), actor="test")
    result = await llmq.converse("big", HELLO)
    assert result.meta.worker == f"direct:{llmq.pod_id}"
    assert [p for _, p, _ in hooks.paths] == ["sqs", "direct"]


async def test_sqs_send_failure_fails_over_to_direct(make_queue: MakeQueue) -> None:
    """DS17: a failed send runs the call direct (claimed) and opens the model's failover."""
    hooks = Recorder()
    llmq = await make_queue(hooks=hooks)
    state = llmq._state
    assert state is not None
    broker = state.brokers.by_alias["big"]

    async def broken(message: Any) -> None:
        raise SQSSendError("endpoint down", family=ErrorFamily.TRANSIENT, code="Unreachable")

    broker.kick = broken  # type: ignore[method-assign]
    first = await llmq.converse("big", HELLO)
    second = await llmq.converse("big", HELLO)
    assert first.meta.worker == second.meta.worker == f"direct:{llmq.pod_id}"
    assert llmq.failover.path_override("big") == "direct"
    assert [p for _, p, _ in hooks.paths] == ["sqs", "direct"]


# ---------------------------------------------------------------- adapters


async def test_langchain_adapter_through_real_queue(make_queue: MakeQueue) -> None:
    """QueuedChatBedrockConverse.ainvoke runs through LLMQueue and reports how it was served."""
    pytest.importorskip("langchain_aws")
    from taskiq_llm_queue.langchain import QueuedChatBedrockConverse

    llmq = await make_queue()
    model = QueuedChatBedrockConverse(
        llmq=llmq,
        model_id="openai.gpt-oss-120b-1:0",
        model_alias="big",
        region_name=REGION,
    )
    with request_context(lane="rt"):
        reply = await model.ainvoke("Summarise today's market in one line.")
    assert reply.content
    assert reply.response_metadata["served_model"] == "big"


async def test_sync_converse_runs_direct(make_queue: MakeQueue) -> None:
    """converse_sync runs direct under the shared limiter and counts the sync fallback."""
    llmq = await make_queue("send")
    result = await asyncio.to_thread(llmq.converse_sync, "big", HELLO)
    assert result.meta.worker == f"sync:{llmq.pod_id}"
    vectors = await asyncio.to_thread(llmq.embed_sync, "titan", ["a", "b"])
    assert len(vectors) == 2
    assert llmq.sync_fallback_count == 3


async def test_caller_cancel_during_pending_send_sets_cancel_flag(
    make_queue: MakeQueue, redis: Redis
) -> None:
    """Bug #19: a caller cancelled while its (batched) send is still pending sets the cancel
    flag, so a message that still goes out is acked as cancelled instead of run."""
    llmq = await make_queue("send")
    state = llmq._state
    assert state is not None
    sending = asyncio.Event()

    async def hang(message: Any) -> None:
        sending.set()
        await asyncio.sleep(30)

    state.brokers.by_alias["big"].kick = hang  # type: ignore[method-assign]
    call = asyncio.create_task(llmq.converse("big", HELLO, request_id="cancel-in-send"))
    await asyncio.wait_for(sending.wait(), 5)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert await redis.exists(llmq.keys.cancel("cancel-in-send"))


async def test_deterministic_send_error_raises_without_failover(make_queue: MakeQueue) -> None:
    """Bug #20: a send rejected for a deterministic reason (bad label) raises to the caller
    and does not move the model to `direct` for the failover window."""
    llmq = await make_queue()
    state = llmq._state
    assert state is not None

    async def rejected(message: Any) -> None:
        raise InvalidLabelError("delay must be 0..900 seconds")

    state.brokers.by_alias["big"].kick = rejected  # type: ignore[method-assign]
    with pytest.raises(InvalidLabelError):
        await llmq.converse("big", HELLO)
    assert llmq.failover.path_override("big") is None
