"""fakellm additions for the 0.3 tests (plan step A0), exercised through the real SDK clients.

Accounts by access key / API key, TPM that reserves max_tokens and settles to usage like
Bedrock, response speed, usage-missing, a region that hangs, and streaming on both fronts.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError, ReadTimeoutError
from fakellm_testkit import Fake, StartFake
from openai import AsyncOpenAI
from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

MODEL = "openai.gpt-oss-120b-1:0"
MESSAGES: list[Any] = [{"role": "user", "content": [{"text": "hello"}]}]


async def _client(
    stack: AsyncExitStack, fake: Fake, account: str = "test", read_timeout: float = 60
) -> BedrockRuntimeClient:
    client = get_session().create_client(
        "bedrock-runtime",
        region_name="ap-south-1",
        endpoint_url=fake.url,
        aws_access_key_id=account,
        aws_secret_access_key="test",
        config=AioConfig(
            retries={"total_max_attempts": 1, "mode": "standard"}, read_timeout=read_timeout
        ),
    )
    return await stack.enter_async_context(client)


async def test_accounts_have_their_own_quota(start_fake: StartFake, stack: AsyncExitStack) -> None:
    fake = await start_fake(
        {"models": {f"{MODEL}@acct-a/ap-south-1": {"rpm": 1}, f"{MODEL}@acct-b/ap-south-1": {}}}
    )
    a = await _client(stack, fake, "acct-a")
    b = await _client(stack, fake, "acct-b")
    await a.converse(modelId=MODEL, messages=MESSAGES)
    with pytest.raises(ClientError, match="Throttling"):
        await a.converse(modelId=MODEL, messages=MESSAGES)
    await b.converse(modelId=MODEL, messages=MESSAGES)
    stats = fake.engine.snapshot()["models"]
    assert isinstance(stats, dict)
    assert stats[f"{MODEL}@acct-a/ap-south-1"]["throttled"] == 1
    assert stats[f"{MODEL}@acct-b/ap-south-1"]["ok"] == 1


async def test_tpm_reserves_max_tokens_then_settles(
    start_fake: StartFake, stack: AsyncExitStack
) -> None:
    fake = await start_fake({"models": {f"{MODEL}@ap-south-1": {"tpm": 100}}})
    client = await _client(stack, fake)
    with pytest.raises(ClientError, match="Throttling"):  # input + 200 > 100 at the start
        await client.converse(modelId=MODEL, messages=MESSAGES, inferenceConfig={"maxTokens": 200})
    for _ in range(3):  # 50 reserved each, settled to the small real usage
        reply = await client.converse(
            modelId=MODEL, messages=MESSAGES, inferenceConfig={"maxTokens": 50}
        )
    used = reply["usage"]["totalTokens"]
    stats = fake.engine.snapshot()["models"]
    assert isinstance(stats, dict)
    assert stats[f"{MODEL}@ap-south-1"]["tokens"] == 3 * used


async def test_converse_stream_matches_converse(
    start_fake: StartFake, stack: AsyncExitStack
) -> None:
    fake = await start_fake({"defaults": {"output_tokens": 40}})
    client = await _client(stack, fake)
    whole = await client.converse(modelId=MODEL, messages=MESSAGES)
    stream = await client.converse_stream(modelId=MODEL, messages=MESSAGES)
    text, usage, stop = "", None, None
    async for event in stream["stream"]:
        if "contentBlockDelta" in event:
            text += event["contentBlockDelta"]["delta"]["text"]
        if "messageStop" in event:
            stop = event["messageStop"]["stopReason"]
        if "metadata" in event:
            usage = event["metadata"].get("usage")
    assert text == whole["output"]["message"]["content"][0]["text"]
    assert stop == "end_turn"
    assert usage == whole["usage"]
    assert whole["usage"]["outputTokens"] == 40


async def test_stream_tool_use(start_fake: StartFake, stack: AsyncExitStack) -> None:
    fake = await start_fake()
    client = await _client(stack, fake)
    tools: Any = {
        "tools": [
            {
                "toolSpec": {
                    "name": "lookup",
                    "inputSchema": {"json": {"type": "object", "properties": {"q": {}}}},
                }
            }
        ]
    }
    stream = await client.converse_stream(modelId=MODEL, messages=MESSAGES, toolConfig=tools)
    names, stop = [], None
    async for event in stream["stream"]:
        if "contentBlockStart" in event:
            names.append(event["contentBlockStart"]["start"]["toolUse"]["name"])
        if "messageStop" in event:
            stop = event["messageStop"]["stopReason"]
    assert names == ["lookup"]
    assert stop == "tool_use"


async def test_speed_paces_output(start_fake: StartFake, stack: AsyncExitStack) -> None:
    fake = await start_fake({"defaults": {"output_tokens": 40, "tokens_per_s": 100}})
    client = await _client(stack, fake)
    started = time.monotonic()
    first_chunk = None
    stream = await client.converse_stream(modelId=MODEL, messages=MESSAGES)
    async for event in stream["stream"]:
        if "contentBlockDelta" in event and first_chunk is None:
            first_chunk = time.monotonic() - started
    total = time.monotonic() - started
    assert first_chunk is not None
    assert first_chunk < 0.3  # the first chunk does not wait for the whole answer
    assert total >= 0.35  # 40 tokens at 100 tokens / s


async def test_usage_missing(start_fake: StartFake, stack: AsyncExitStack) -> None:
    fake = await start_fake({"defaults": {"usage_missing": True}})
    client = await _client(stack, fake)
    reply = await client.converse(modelId=MODEL, messages=MESSAGES)
    assert "usage" not in reply
    raw = await client.invoke_model(
        modelId="amazon.titan-embed-text-v2:0", body=b'{"inputText": "x"}'
    )
    headers = raw["ResponseMetadata"]["HTTPHeaders"]
    assert "x-amzn-bedrock-input-token-count" not in headers


async def test_hang_never_answers(start_fake: StartFake, stack: AsyncExitStack) -> None:
    fake = await start_fake({"defaults": {"hang": True}})
    client = await _client(stack, fake, read_timeout=0.5)
    with pytest.raises(ReadTimeoutError):
        await client.converse(modelId=MODEL, messages=MESSAGES)


async def test_openai_account_and_stream(
    start_fake: StartFake, make_openai: Callable[[Fake], AsyncOpenAI]
) -> None:
    fake = await start_fake(
        {"models": {"gpt-oss-120b@key-a": {"rpm": 1}}, "defaults": {"output_tokens": 30}}
    )
    keyed = AsyncOpenAI(base_url=f"{fake.url}/v1", api_key="key-a", max_retries=0)
    messages: list[Any] = [{"role": "user", "content": "hi"}]
    whole = await keyed.chat.completions.create(model="gpt-oss-120b", messages=messages)
    other = make_openai(fake)  # api key "test": a different account, no rpm cap
    parts = []
    stream = await other.chat.completions.create(
        model="gpt-oss-120b",
        messages=messages,
        stream=True,
        stream_options={"include_usage": True},
    )
    usage = None
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            parts.append(chunk.choices[0].delta.content)
        if chunk.usage:
            usage = chunk.usage
    assert "".join(parts) == whole.choices[0].message.content
    assert usage is not None
    assert whole.usage is not None
    assert usage.completion_tokens == whole.usage.completion_tokens == 30
    with pytest.raises(Exception, match="429"):
        await keyed.chat.completions.create(model="gpt-oss-120b", messages=messages)
    await keyed.close()
    await asyncio.sleep(0)
