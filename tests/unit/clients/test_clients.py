"""Client pool and providers on routes, against an in-process fakellm (plan step A3, B05, B98).

Covers clients per account and region, chat assembled from the streaming API (text, tool calls,
usage), the attempt timers (hung region, idle stream, long healthy answer) and embeddings.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from botocore.exceptions import ClientError
from clients_testkit import StartFake, registry_doc

from taskiq_llm_queue.clients import (
    AttemptTimeout,
    ClientPool,
    Providers,
    Route,
    Timing,
    decode_invoke_body,
    route_for,
)
from taskiq_llm_queue.errors import ConfigError, ErrorKind
from taskiq_llm_queue.registry import Account, ResolvedRegistry, load_registry

MODEL = "openai.gpt-oss-120b-1:0"
CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)


@pytest.fixture
async def setup(start_fake: StartFake) -> AsyncIterator[Any]:
    providers: list[Providers] = []

    async def make(scenario: dict[str, Any] | None = None) -> tuple[ResolvedRegistry, Providers]:
        url, _ = await start_fake(scenario)
        view = load_registry(registry_doc(url))
        pool = ClientPool(
            bedrock_endpoint_url=url,
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
            secrets={"VLLM_KEY": "key-v"}.get,
        )
        built = Providers(pool)
        providers.append(built)
        return view, built

    yield make
    for built in providers:
        await built.aclose()


def _route(view: ResolvedRegistry, model: str, key: str) -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.key == key))


async def test_routes_and_profiles(setup: Any) -> None:
    view, _ = await setup()
    assert _route(view, "gpt120b", "aws-a/ap-south-1").model_id == MODEL
    profile = _route(view, "gpt120b", "aws-a/apac")
    assert (profile.model_id, profile.region) == ("apac.openai.gpt-oss-120b-1:0", "ap-south-1")


async def test_converse_is_assembled_from_the_stream(setup: Any) -> None:
    view, providers = await setup({"defaults": {"output_tokens": 50}})
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    result = await providers.get("bedrock").call("converse", CHAT, route, FAST)
    text = result.response["output"]["message"]["content"][0]["text"]
    assert len(text) == 200
    assert result.response["stopReason"] == "end_turn"
    assert result.usage.output_tokens == 50
    assert result.usage.input_tokens is not None
    direct = await (await providers.pool.bedrock(route)).converse(modelId=MODEL, **CHAT)
    assert direct["output"]["message"]["content"][0]["text"] == text


async def test_tool_use_is_assembled(setup: Any) -> None:
    view, providers = await setup()
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    tools = {
        "tools": [
            {
                "toolSpec": {
                    "name": "lookup",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"q": {"type": "string"}, "n": {"type": "integer"}},
                        }
                    },
                }
            }
        ]
    }
    result = await providers.get("bedrock").call(
        "converse", {**CHAT, "toolConfig": tools}, route, FAST
    )
    block = result.response["output"]["message"]["content"][0]["toolUse"]
    assert block["name"] == "lookup"
    assert block["input"]["n"] == 1
    assert result.response["stopReason"] == "tool_use"


async def test_accounts_have_own_clients_and_quotas(setup: Any) -> None:
    view, providers = await setup({"models": {f"{MODEL}@aws-a/ap-south-1": {"rpm": 1}}})
    bedrock = providers.get("bedrock")
    a = _route(view, "gpt120b", "aws-a/ap-south-1")
    b = _route(view, "gpt120b", "aws-b/ap-south-1")
    assert await providers.pool.bedrock(a) is not await providers.pool.bedrock(b)
    await bedrock.call("converse", CHAT, a, FAST)
    with pytest.raises(ClientError) as info:
        await bedrock.call("converse", CHAT, a, FAST)
    assert bedrock.classify(info.value).kind is ErrorKind.THROTTLE
    await bedrock.call("converse", CHAT, b, FAST)  # the other account is not throttled


async def test_hung_region_gives_up_at_first_response(setup: Any) -> None:
    view, providers = await setup({"defaults": {"hang": True}})
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    started = time.monotonic()
    bedrock = providers.get("bedrock")
    with pytest.raises(AttemptTimeout, match="first response") as info:
        await bedrock.call("converse", CHAT, route, Timing(0.4, 5))
    assert time.monotonic() - started < 2
    assert bedrock.classify(info.value).kind is ErrorKind.RETRYABLE


async def test_long_healthy_answer_is_not_cut(setup: Any) -> None:
    view, providers = await setup({"defaults": {"output_tokens": 200, "tokens_per_s": 400}})
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    started = time.monotonic()
    result = await providers.get("bedrock").call("converse", CHAT, route, Timing(0.3, 0.3))
    assert time.monotonic() - started > 0.4  # longer than the first-response limit
    assert result.usage.output_tokens == 200


async def test_idle_stream_is_given_up(setup: Any) -> None:
    view, providers = await setup({"defaults": {"output_tokens": 40, "tokens_per_s": 4}})
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    with pytest.raises(AttemptTimeout, match="chunk"):
        await providers.get("bedrock").call("converse", CHAT, route, Timing(2, 0.3))


async def test_invoke_usage_from_header(setup: Any) -> None:
    view, providers = await setup()
    route = _route(view, "titan", "aws-a/ap-south-1")
    body = {"inputText": "a b c d e f g h", "dimensions": 256}
    result = await providers.get("bedrock").call("invoke_model", {"body": body}, route, FAST)
    assert len(decode_invoke_body(result.response)["embedding"]) == 256
    assert result.usage.input_tokens == 4
    view, providers = await setup({"defaults": {"usage_missing": True}})
    route = _route(view, "titan", "aws-a/ap-south-1")
    result = await providers.get("bedrock").call("invoke_model", {"body": body}, route, FAST)
    assert result.usage.input_tokens is None


async def test_openai_chat_tools_and_embeddings(setup: Any) -> None:
    view, providers = await setup({"defaults": {"output_tokens": 30}})
    route = _route(view, "local", "vllm/dc1")
    openai = providers.get("openai")
    messages = [{"role": "user", "content": "hi"}]
    result = await openai.call("chat", {"messages": messages}, route, FAST)
    message = result.response["choices"][0]["message"]
    assert len(message["content"]) == 120
    assert result.usage.output_tokens == 30
    tools = [
        {
            "type": "function",
            "function": {
                "name": "lookup",
                "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
            },
        }
    ]
    result = await openai.call("chat", {"messages": messages, "tools": tools}, route, FAST)
    call = result.response["choices"][0]["message"]["tool_calls"][0]
    assert call["function"]["name"] == "lookup"
    assert result.response["choices"][0]["finish_reason"] == "tool_calls"
    result = await openai.call("embeddings", {"input": ["a", "b"]}, route, FAST)
    assert len(result.response["data"]) == 2
    assert result.usage.input_tokens is not None


async def test_openai_key_and_errors(setup: Any) -> None:
    view, providers = await setup({"models": {"gpt-oss-120b@key-v": {"rpm": 1}}})
    route = _route(view, "local", "vllm/dc1")
    openai = providers.get("openai")
    messages = [{"role": "user", "content": "hi"}]
    await openai.call("chat", {"messages": messages}, route, FAST)
    with pytest.raises(Exception, match="429") as info:  # the key names the fake account
        await openai.call("chat", {"messages": messages}, route, FAST)
    assert openai.classify(info.value).kind is ErrorKind.THROTTLE
    missing = Route(
        Account(name="x", kind="openai", base_url="http://fakellm/v1", api_key_ref="NOPE"),
        "dc1",
        "m",
        "x/dc1",
    )
    with pytest.raises(ConfigError, match="NOPE is not set"):
        providers.pool.openai(missing)


async def test_assume_role_is_not_built_yet(setup: Any) -> None:
    _, providers = await setup()
    account = Account(name="dr", kind="aws", role_arn="arn:aws:iam::1:role/x")
    with pytest.raises(ConfigError, match="A15"):
        await providers.pool.bedrock(Route(account, "ap-south-1", MODEL, "dr/ap-south-1"))
    with pytest.raises(ConfigError, match="no provider"):
        providers.get("azure")
