from __future__ import annotations

import httpx
import openai
import pytest
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from lc_fakes import CHAT_TOOL_RESPONSE, FakeQueueClient  # type: ignore[import-not-found]  # bare-name test helper

from taskiq_llm_queue.errors import (
    ConfigError,
    LLMQueueError,
    LLMQueueTimeout,
    ProviderError,
    QueueUnavailable,
)
from taskiq_llm_queue.http_transport import QUEUED_BASE_URL, QueuedHTTPTransport
from taskiq_llm_queue.langchain import (
    QueuedChatOpenAI,
    QueuedOpenAIEmbeddings,
    set_default_queue,
)


@tool
def get_price(symbol: str) -> float:
    """Latest price for a stock symbol."""
    return 1.0


def _chat(fake: FakeQueueClient, **kwargs: object) -> QueuedChatOpenAI:
    return QueuedChatOpenAI(model="llama-3.1-70b", model_alias="local-llama", llmq=fake, **kwargs)


def test_sdk_settings_are_forced() -> None:
    model = QueuedChatOpenAI(model="x", streaming=True, max_retries=5, base_url="http://real")
    assert model.streaming is False
    assert model.disable_streaming is True
    assert model.max_retries == 0
    assert model.openai_api_base == QUEUED_BASE_URL
    assert QueuedOpenAIEmbeddings(model="e").check_embedding_ctx_length is False


async def test_ainvoke_goes_through_chat() -> None:
    fake = FakeQueueClient()
    reply = await _chat(fake, lane="rt", fallback_policy="chain").ainvoke("hi")
    assert isinstance(reply, AIMessage)
    assert reply.content == "hello"
    assert reply.usage_metadata is not None
    assert reply.usage_metadata["total_tokens"] == 4
    op, alias, body, kwargs = fake.calls[0]
    assert (op, alias) == ("chat", "local-llama")
    assert body["messages"] == [{"role": "user", "content": "hi"}]
    assert body["model"] == "llama-3.1-70b"
    assert not body.get("stream")
    assert kwargs == {"lane": "rt", "fallback": "chain", "timeout": None}


async def test_tool_calls_round_trip() -> None:
    fake = FakeQueueClient(chat_response=CHAT_TOOL_RESPONSE)
    reply = await _chat(fake).bind_tools([get_price]).ainvoke("price of INFY?")
    assert isinstance(reply, AIMessage)
    assert fake.calls[0][2]["tools"][0]["function"]["name"] == "get_price"
    assert reply.tool_calls == [
        {"name": "get_price", "args": {"symbol": "INFY"}, "id": "call_1", "type": "tool_call"}
    ]


async def test_astream_yields_one_message() -> None:
    fake = FakeQueueClient()
    chunks = [chunk async for chunk in _chat(fake).astream("hi")]
    assert [c.content for c in chunks] == ["hello"]
    assert fake.ops == ["chat"]


async def test_throttle_maps_to_rate_limit_error() -> None:
    fake = FakeQueueClient(error=ProviderError("throttled", retryable=True, retry_after_secs=2))
    with pytest.raises(openai.RateLimitError) as info:
        await _chat(fake).ainvoke("hi")
    assert info.value.status_code == 429
    assert info.value.response.headers["retry-after"] == "2"
    assert fake.ops == ["chat"]  # max_retries=0: the SDK does not retry


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ProviderError("bad request", retryable=False), openai.BadRequestError),
        (LLMQueueTimeout("waited too long"), openai.InternalServerError),
        (QueueUnavailable("sqs down"), openai.InternalServerError),
        (ConfigError("unknown model alias"), openai.NotFoundError),
        (LLMQueueError("other"), openai.InternalServerError),
    ],
)
async def test_queue_errors_map_to_sdk_errors(
    error: Exception, expected: type[openai.APIStatusError]
) -> None:
    with pytest.raises(expected):
        await _chat(FakeQueueClient(error=error)).ainvoke("hi")


async def test_default_queue_is_resolved_lazily() -> None:
    model = QueuedChatOpenAI(model="local-llama")
    fake = FakeQueueClient()
    set_default_queue(fake)
    await model.ainvoke("hi")
    assert fake.calls[0][:2] == ("chat", "local-llama")  # alias falls back to the body model


async def test_embeddings_async_base64_round_trip() -> None:
    fake = FakeQueueClient()
    emb = QueuedOpenAIEmbeddings(model="bge", llmq=fake, lane="bulk")
    assert await emb.aembed_documents(["a", "bb"]) == [[1.0, 0.5], [2.0, 0.5]]
    assert fake.calls == [("embed", "bge", ["a", "bb"], {"lane": "bulk", "timeout": None})]


def test_embeddings_sync_uses_embed_sync() -> None:
    fake = FakeQueueClient()
    assert QueuedOpenAIEmbeddings(model="bge", llmq=fake).embed_query("abc") == [3.0, 0.5]
    assert fake.ops == ["embed_sync"]


def test_sync_chat_without_chat_sync_is_a_clear_error() -> None:
    with pytest.raises(openai.InternalServerError, match="sync chat"):
        _chat(FakeQueueClient()).invoke("hi")


@pytest.mark.parametrize(
    ("method", "path", "body", "status"),
    [
        ("POST", "/v1/responses", {"model": "m"}, 404),
        ("GET", "/v1/models", None, 405),
        ("POST", "/v1/chat/completions", {"model": "m", "stream": True}, 400),
        ("POST", "/v1/embeddings", {"model": "m", "input": [[1, 2]]}, 400),
    ],
)
async def test_transport_rejects_what_it_cannot_queue(
    method: str, path: str, body: object, status: int
) -> None:
    fake = FakeQueueClient()
    async with httpx.AsyncClient(
        transport=QueuedHTTPTransport(fake), base_url="http://llmq.invalid"
    ) as client:
        response = await client.request(method, path, json=body)
    assert response.status_code == status
    assert response.json()["error"]["type"] == "invalid_request_error"
    assert fake.calls == []
