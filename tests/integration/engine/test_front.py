"""`LLMQueue` and the LangChain adapters end to end (plan step A14), on fakellm.

Built with `LLMQueue.from_env` exactly as an app builds it: converse with the request context,
embeddings, jobs, sync calls on the direct path, and the four LangChain adapters (Bedrock chat
with tools, Bedrock embeddings, OpenAI-compatible chat and embeddings). No real LLM is called.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import FakeEngine, Scenario, create_app, registry

from taskiq_llm_queue import CallFailed, LLMQueue, request_context
from taskiq_llm_queue.http_transport import _error_response

pytestmark = pytest.mark.integration


def _doc(url: str) -> dict[str, Any]:
    doc: dict[str, Any] = registry()
    doc["accounts"]["vllm"] = {"kind": "openai", "base_url": f"{url}/v1", "api_key_ref": "VLLM_KEY"}
    doc["models"]["local"] = {
        "type": "chat",
        "provider": "openai",
        "model_id": "gpt-oss-120b",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    doc["models"]["local-emb"] = {
        "type": "embedding",
        "provider": "openai",
        "model_id": "text-embed",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    return doc


@pytest.fixture
async def llmq(store: redis.asyncio.Redis, prefix: str) -> AsyncIterator[LLMQueue]:
    fake = FakeEngine(Scenario.from_mapping({}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    url = str(server.make_url("")).rstrip("/")
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(_doc(url)),
        "LLMQ_BEDROCK_ENDPOINT_URL": url,
        "LLMQ_METRICS_SINK": "none",
    }
    queue = LLMQueue.from_env(
        env,
        secrets={"VLLM_KEY": "key-v"}.get,
        credentials=lambda account: {
            "aws_access_key_id": account.name,
            "aws_secret_access_key": "x",
        },
    )
    seen: list[dict[str, Any]] = []

    async def ingest(payload: dict[str, Any]) -> None:
        vectors = await queue.embed("cohere", payload["texts"], input_type="search_query")
        seen.append({"payload": payload, "vectors": len(vectors.vectors)})

    queue.register_job("ingest", ingest)
    queue.seen = seen  # type: ignore[attr-defined]
    await queue.start()
    yield queue
    await queue.stop()
    await server.close()


async def test_converse_with_request_context(llmq: LLMQueue) -> None:
    with request_context(priority=True, user_id="ann", session_id="s-1"):
        reply = await llmq.converse(
            "big", {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
        )
    assert reply.ok
    status = await llmq.request_status(reply.request_id)
    assert status["priority"] == "1"
    assert (await llmq.health())["ready"]


async def test_embed_and_jobs(llmq: LLMQueue) -> None:
    result = await llmq.embed("cohere", ["a", "b", "c"], input_type="search_query")
    assert len(result.vectors) == 3
    await llmq.send_job("ingest", {"texts": ["x", "y"]})
    for _ in range(50):
        if llmq.seen:  # type: ignore[attr-defined]
            break
        await asyncio.sleep(0.1)
    assert llmq.seen == [{"payload": {"texts": ["x", "y"]}, "vectors": 2}]  # type: ignore[attr-defined]


async def test_sync_calls_run_direct(llmq: LLMQueue) -> None:
    request = {"messages": [{"role": "user", "content": [{"text": "sync"}]}]}
    reply = await asyncio.to_thread(llmq.converse_sync, "big", request)
    assert reply.ok
    vectors = await asyncio.to_thread(llmq.embed_sync, "titan", ["one"])
    assert len(vectors.vectors) == 1


async def test_langchain_bedrock_chat_and_tools(llmq: LLMQueue) -> None:
    from langchain_core.messages import AIMessage
    from langchain_core.tools import tool

    from taskiq_llm_queue.langchain import QueuedChatBedrockConverse

    model = QueuedChatBedrockConverse(model_alias="big", llmq=llmq)  # type: ignore[call-arg]
    assert model.model_id == "openai.gpt-oss-120b-1:0"  # looked up from the registry
    message = await model.ainvoke("hello")
    assert str(message.content).startswith("fakellm")
    assert message.response_metadata["deployment"] == "aws-a/r1"

    @tool
    def lookup(query: str) -> str:
        """Look something up."""
        return query

    called = await model.bind_tools([lookup]).ainvoke("find it")
    assert isinstance(called, AIMessage)
    assert called.tool_calls[0]["name"] == "lookup"
    sync_reply = await asyncio.to_thread(model.invoke, "sync hello")
    assert str(sync_reply.content).startswith("fakellm")


async def test_langchain_bedrock_embeddings(llmq: LLMQueue) -> None:
    from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings

    emb = QueuedBedrockEmbeddings(
        model_alias="cohere", model_id="cohere.embed-english-v3", llmq=llmq
    )
    docs = await emb.aembed_documents(["a", "b"])
    query = await emb.aembed_query("q")
    assert len(docs) == 2
    assert len(query) == 1024


async def test_langchain_openai_chat_and_embeddings(llmq: LLMQueue) -> None:
    from taskiq_llm_queue.langchain import QueuedChatOpenAI, QueuedOpenAIEmbeddings

    chat = QueuedChatOpenAI(model="local", llmq=llmq)
    message = await chat.ainvoke("hello")
    assert str(message.content).startswith("fakellm")
    assert message.response_metadata["served_model"] == "local"
    emb = QueuedOpenAIEmbeddings(model="local-emb", llmq=llmq)
    vectors = await emb.aembed_documents(["a", "b", "c"])
    assert len(vectors) == 3


def test_error_mapping() -> None:
    request = httpx.Request("POST", "http://llmq.invalid/v1/chat/completions")
    limited = _error_response(request, CallFailed("rate_limited", "busy", retry_after_secs=2))
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "2"
    assert _error_response(request, CallFailed("deadline_exceeded")).status_code == 504
    assert _error_response(request, CallFailed("model_unavailable")).status_code == 503
    assert _error_response(request, CallFailed("request_too_large")).status_code == 400
