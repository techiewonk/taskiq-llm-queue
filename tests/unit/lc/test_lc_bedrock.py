from __future__ import annotations

import copy
from typing import Any

import pytest
from langchain_aws import ChatBedrockConverse
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import LLMResult
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from lc_fakes import (  # type: ignore[import-not-found]  # bare-name test helper
    CONVERSE_TEXT_RESPONSE,
    CONVERSE_TOOL_RESPONSE,
    META,
    FakeQueueClient,
)

from taskiq_llm_queue.config import ModelConfig, QueueConfig
from taskiq_llm_queue.errors import LLMQueueError
from taskiq_llm_queue.langchain import (
    QueuedBedrockEmbeddings,
    QueuedChatBedrockConverse,
    set_default_queue,
)
from taskiq_llm_queue.langchain.bedrock import _looks_like_bedrock_id

MODEL_ID = "openai.gpt-oss-120b-1:0"
META_KEYS = set(META.model_dump())


class _StubBoto:
    """Stands in for a boto3 bedrock-runtime client for the plain parent class."""

    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.kwargs: dict[str, Any] = {}

    def converse(self, **kwargs: Any) -> dict[str, Any]:
        self.kwargs = copy.deepcopy(kwargs)
        return copy.deepcopy(self.response)


@tool
def get_price(symbol: str) -> float:
    """Latest price for a stock symbol."""
    return 1.0


def _queued(fake: FakeQueueClient, **kwargs: Any) -> QueuedChatBedrockConverse:
    return QueuedChatBedrockConverse(model_id=MODEL_ID, model_alias="gpt120b", llmq=fake, **kwargs)


def test_no_boto3_client_is_created(monkeypatch: pytest.MonkeyPatch) -> None:
    """Construction must not build an AWS client (import-time models, D85)."""

    def _boom(**_: Any) -> None:
        raise AssertionError("boto3 client created")

    monkeypatch.setattr("langchain_aws.chat_models.bedrock_converse.create_aws_client", _boom)
    monkeypatch.setattr("langchain_aws.embeddings.bedrock.create_aws_client", _boom)
    model = QueuedChatBedrockConverse(model=MODEL_ID, model_alias="gpt120b")  # type: ignore[call-arg]  # parent alias of model_id
    assert (model.model_id, model.model_alias) == (MODEL_ID, "gpt120b")
    QueuedBedrockEmbeddings(model_id="titan-v2")


def test_alias_passed_as_model_is_a_clear_error() -> None:
    """`model="gpt120b"` is a queue alias, not a Bedrock id: say what to pass instead."""
    with pytest.raises(ValueError, match="looks like a queue alias"):
        QueuedChatBedrockConverse(model="gpt120b")  # type: ignore[call-arg]  # parent alias
    with pytest.raises(ValueError, match="needs model_id"):
        QueuedChatBedrockConverse(model_alias="gpt120b")  # type: ignore[call-arg]  # no model_id
    assert _looks_like_bedrock_id("arn:aws:bedrock:ap-south-1:1:application-inference-profile/x")
    assert _looks_like_bedrock_id("apac.openai.gpt-oss-120b-1:0")


def test_model_id_is_looked_up_from_the_queue_config() -> None:
    """`model_alias` + `llmq=` is enough: the Bedrock id comes from `QueueConfig.models`."""
    config = QueueConfig(models={"gpt120b": ModelConfig(provider="bedrock", model_id=MODEL_ID)})
    fake = FakeQueueClient()
    fake.config = config
    model = QueuedChatBedrockConverse(model_alias="gpt120b", llmq=fake)  # type: ignore[call-arg]  # model_id resolved
    assert model.model_id == MODEL_ID


async def test_ainvoke_matches_parent_parsing() -> None:
    """Same Converse response → same message as plain ChatBedrockConverse, plus CallMeta."""
    stub = _StubBoto(CONVERSE_TOOL_RESPONSE)
    # `bedrock_client` is a field only in newer langchain-aws; on 0.2.24 it would be a model kwarg.
    slots = {s: stub for s in ("client", "bedrock_client") if s in ChatBedrockConverse.model_fields}
    plain = ChatBedrockConverse(model_id=MODEL_ID, region_name="us-east-1", **slots)
    fake = FakeQueueClient()
    queued = _queued(fake)

    expected = await plain.ainvoke("price of INFY?")
    got = await queued.ainvoke("price of INFY?")

    assert isinstance(got, AIMessage)
    assert isinstance(expected, AIMessage)
    assert got.content == expected.content
    assert got.tool_calls == expected.tool_calls
    assert got.tool_calls[0]["name"] == "get_price"
    assert got.usage_metadata == expected.usage_metadata
    assert got.usage_metadata is not None
    assert got.usage_metadata["input_token_details"]["cache_read"] == 5
    assert got.usage_metadata["input_token_details"]["cache_creation"] == 2
    meta = {k: v for k, v in got.response_metadata.items() if k not in META_KEYS}
    assert meta == expected.response_metadata
    # The request is the parent's request minus modelId (the queue alias picks the model).
    op, model, request, kwargs = fake.calls[0]
    assert (op, model) == ("converse", "gpt120b")
    assert request == {k: v for k, v in stub.kwargs.items() if k != "modelId"}
    assert kwargs == {"lane": None, "fallback": None, "timeout_secs": None}


async def test_response_metadata_has_call_meta() -> None:
    reply = await _queued(FakeQueueClient()).ainvoke("hi")
    md = reply.response_metadata
    assert md["model_name"] == MODEL_ID
    assert md["served_model"] == "gpt120b"
    assert md["served_target"] == "apac"
    assert md["fallback_used"] is True
    assert md["queue_wait_ms"] == 42
    assert md["attempts"] == 2


async def test_bind_tools_sends_tool_config() -> None:
    fake = FakeQueueClient()
    await _queued(fake).bind_tools([get_price]).ainvoke("price of INFY?")
    tool_config = fake.calls[0][2]["toolConfig"]
    assert tool_config["tools"][0]["toolSpec"]["name"] == "get_price"
    assert "inputSchema" in tool_config["tools"][0]["toolSpec"]


async def test_prompt_pipe_model_chain() -> None:
    fake = FakeQueueClient(converse_response=CONVERSE_TEXT_RESPONSE)
    prompt = ChatPromptTemplate.from_messages([("system", "Be terse."), ("human", "{q}")])
    reply = await (prompt | _queued(fake)).ainvoke({"q": "market today?"})
    assert reply.content == "Markets rose."
    request = fake.calls[0][2]
    assert request["system"] == [{"text": "Be terse."}]
    assert request["messages"] == [{"role": "user", "content": [{"text": "market today?"}]}]


async def test_astream_yields_one_full_message_without_streaming() -> None:
    """astream() must not open a stream: one complete message from one queued converse."""
    fake = FakeQueueClient(converse_response=CONVERSE_TEXT_RESPONSE)
    model = _queued(fake)
    assert model.disable_streaming is True
    chunks = [chunk async for chunk in model.astream("hi")]
    assert len(chunks) == 1
    assert chunks[0].content == "Markets rose."
    assert fake.ops == ["converse"]


async def test_streaming_flag_cannot_be_turned_on() -> None:
    model = QueuedChatBedrockConverse(model_id=MODEL_ID, disable_streaming=False)
    assert model.disable_streaming is True


async def test_callbacks_fire() -> None:
    events: list[str] = []

    class Recorder(AsyncCallbackHandler):
        async def on_llm_start(self, serialized: Any, prompts: Any, **kwargs: Any) -> None:
            events.append("start")

        async def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
            events.append("end")

    await _queued(FakeQueueClient()).ainvoke("hi", config={"callbacks": [Recorder()]})
    assert events == ["start", "end"]


async def test_lane_fallback_and_service_tier_are_passed() -> None:
    fake = FakeQueueClient()
    model = _queued(
        fake, lane="rt", fallback_policy="chain", service_tier="priority", queue_timeout_secs=30
    )
    await model.ainvoke("hi")
    _, _, request, kwargs = fake.calls[0]
    assert request["serviceTier"] == {"type": "priority"}
    assert kwargs == {"lane": "rt", "fallback": "chain", "timeout_secs": 30}


async def test_default_queue_is_resolved_lazily() -> None:
    """A model built before the queue exists (import time) finds it on first call (D85)."""
    model = QueuedChatBedrockConverse(model_id=MODEL_ID)
    with pytest.raises(LLMQueueError, match="no LLM queue"):
        await model.ainvoke("hi")
    fake = FakeQueueClient()
    set_default_queue(fake)
    await model.ainvoke("hi")
    assert fake.calls[0][:2] == ("converse", MODEL_ID)  # alias defaults to model_id


def test_sync_invoke_uses_converse_sync() -> None:
    fake = FakeQueueClient()
    reply = _queued(fake).invoke("hi")
    assert fake.ops == ["converse_sync"]
    assert reply.response_metadata["served_target"] == "apac"


async def test_embeddings_async_one_call_each() -> None:
    fake = FakeQueueClient()
    emb = QueuedBedrockEmbeddings(model_id="titan-v2", llmq=fake, lane="bulk")
    assert await emb.aembed_query("abc") == [3.0, 0.5]
    assert await emb.aembed_documents(["a", "bb"]) == [[1.0, 0.5], [2.0, 0.5]]
    assert fake.calls == [
        (
            "embed",
            "titan-v2",
            ["abc"],
            {"lane": "bulk", "input_type": "search_query", "timeout_secs": None},
        ),
        (
            "embed",
            "titan-v2",
            ["a", "bb"],
            {"lane": "bulk", "input_type": "search_document", "timeout_secs": None},
        ),
    ]


def test_embeddings_sync_and_normalize() -> None:
    fake = FakeQueueClient()
    emb = QueuedBedrockEmbeddings(
        model_id="titan-v2", model_alias="titan", llmq=fake, normalize=True
    )
    vector = emb.embed_query("abc")
    assert vector == pytest.approx([3.0 / (9.25**0.5), 0.5 / (9.25**0.5)])
    emb.embed_documents(["a"])
    assert fake.ops == ["embed_sync", "embed_sync"]
    assert fake.calls[0][1] == "titan"


def test_placeholder_client_only_fills_fields_the_installed_class_has() -> None:
    """Research-assistant bug: on langchain-aws 0.2.24 `bedrock_client` is no field, so the
    placeholder moved into `model_kwargs`, joined the Converse request and broke envelope
    serialization. Only real client fields get the placeholder."""
    from taskiq_llm_queue.langchain import QueuedChatBedrockConverse

    model = QueuedChatBedrockConverse(model_id="openai.gpt-oss-20b-1:0", model_alias="gpt20b")
    assert "bedrock_client" not in (getattr(model, "model_kwargs", None) or {})
    for slot in ("client", "bedrock_client"):
        if slot in type(model).model_fields:
            assert getattr(model, slot) is not None
