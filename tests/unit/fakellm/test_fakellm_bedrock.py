"""Bedrock front of fakellm, exercised through a real aiobotocore bedrock-runtime client.

Covers Converse/InvokeModel shapes, SigV4-region targeting, URL-decoded model ids and
the error-fidelity table (botocore must parse each error code by name).
"""

from __future__ import annotations

import json
import math
from typing import Any

import pytest
from botocore.exceptions import ClientError
from fakellm import ERRORS
from fakellm_testkit import MakeBedrock, StartFake

GPT = "openai.gpt-oss-120b-1:0"
MSGS: Any = [{"role": "user", "content": [{"text": "hello there"}]}]


async def test_converse_shape_and_determinism(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Converse returns message, stopReason, usage with cache fields and metrics."""
    fake = await start_fake()
    client = await make_bedrock(fake)
    first = await client.converse(modelId=GPT, messages=MSGS)
    second = await client.converse(modelId=GPT, messages=MSGS)
    other = await client.converse(
        modelId=GPT, messages=[{"role": "user", "content": [{"text": "bye"}]}]
    )
    text = first["output"]["message"]["content"][0]["text"]
    assert text.startswith("fakellm reply ")
    assert first["output"] == second["output"]
    assert first["output"] != other["output"]
    assert first["stopReason"] == "end_turn"
    usage = first["usage"]
    assert usage["totalTokens"] == usage["inputTokens"] + usage["outputTokens"]
    assert usage.get("cacheReadInputTokens") == 0
    assert usage.get("cacheWriteInputTokens") == 0
    assert "latencyMs" in first["metrics"]


async def test_converse_tool_echo_and_tool_result(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """With toolConfig and tool_calls echo the reply is a toolUse; a toolResult gets text."""
    fake = await start_fake({"defaults": {"tool_calls": "echo"}})
    client = await make_bedrock(fake)
    tool_config: Any = {
        "tools": [
            {
                "toolSpec": {
                    "name": "lookup",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"q": {"type": "string"}, "n": {"type": "integer"}},
                            "required": ["q", "n"],
                        }
                    },
                }
            }
        ]
    }
    resp = await client.converse(modelId=GPT, messages=MSGS, toolConfig=tool_config)
    assert resp["stopReason"] == "tool_use"
    tool_use = resp["output"]["message"]["content"][0]["toolUse"]
    assert tool_use["name"] == "lookup"
    assert tool_use["input"] == {"q": "hello there", "n": 1}
    follow_up: Any = [
        *MSGS,
        {"role": "assistant", "content": [{"toolUse": tool_use}]},
        {
            "role": "user",
            "content": [
                {"toolResult": {"toolUseId": tool_use["toolUseId"], "content": [{"text": "ok"}]}}
            ],
        },
    ]
    final = await client.converse(modelId=GPT, messages=follow_up, toolConfig=tool_config)
    assert final["stopReason"] == "end_turn"


async def test_converse_tool_calls_none_and_max_tokens(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    fake = await start_fake({"defaults": {"tool_calls": "none"}})
    client = await make_bedrock(fake)
    tool_config: Any = {"tools": [{"toolSpec": {"name": "t", "inputSchema": {"json": {}}}}]}
    resp = await client.converse(modelId=GPT, messages=MSGS, toolConfig=tool_config)
    assert resp["stopReason"] == "end_turn"
    capped = await client.converse(modelId=GPT, messages=MSGS, inferenceConfig={"maxTokens": 2})
    assert capped["stopReason"] == "max_tokens"
    assert capped["usage"]["outputTokens"] == 2


async def test_invoke_titan_v2(start_fake: StartFake, make_bedrock: MakeBedrock) -> None:
    """Titan v2 vectors are unit length, deterministic and honour `dimensions`."""
    model = "amazon.titan-embed-text-v2:0"
    fake = await start_fake({"models": {f"{model}@ap-south-1": {"dims": 512}}})
    client = await make_bedrock(fake)

    async def embed(body: dict[str, Any]) -> dict[str, Any]:
        resp = await client.invoke_model(modelId=model, body=json.dumps(body))
        result: dict[str, Any] = json.loads(await resp["body"].read())
        return result

    a = await embed({"inputText": "alpha"})
    b = await embed({"inputText": "alpha"})
    small = await embed({"inputText": "alpha", "dimensions": 256})
    assert len(a["embedding"]) == 512
    assert len(small["embedding"]) == 256
    assert a == b
    assert a["inputTextTokenCount"] >= 1
    assert math.isclose(sum(v * v for v in a["embedding"]), 1.0, rel_tol=1e-9)


async def test_invoke_cohere_v3(start_fake: StartFake, make_bedrock: MakeBedrock) -> None:
    fake = await start_fake({"defaults": {"dims": 64}})
    client = await make_bedrock(fake)
    body = {"texts": ["one", "two", "one"], "input_type": "search_document"}
    resp = await client.invoke_model(
        modelId="cohere.embed-english-v3", body=json.dumps(body), contentType="application/json"
    )
    data = json.loads(await resp["body"].read())
    assert data["texts"] == body["texts"]
    assert isinstance(data["id"], str)
    assert [len(v) for v in data["embeddings"]] == [64, 64, 64]
    assert data["embeddings"][0] == data["embeddings"][2] != data["embeddings"][1]


async def test_invoke_rejects_bad_bodies(start_fake: StartFake, make_bedrock: MakeBedrock) -> None:
    fake = await start_fake()
    client = await make_bedrock(fake)
    for body in ({"texts": ["x"]}, {"inputText": "x", "dimensions": 7}, {"prompt": "x"}):
        with pytest.raises(ClientError) as info:
            await client.invoke_model(modelId="m", body=json.dumps(body))
        assert info.value.response["Error"]["Code"] == "ValidationException"
    assert fake.engine.stats["m@ap-south-1"].errors == 3


async def test_region_discrimination(start_fake: StartFake, make_bedrock: MakeBedrock) -> None:
    """Two regions on one endpoint land on separate keys (region read from SigV4 scope)."""
    fake = await start_fake(
        {"models": {f"{GPT}@us-east-1": {"force_error": "AccessDeniedException"}}}
    )
    mumbai = await make_bedrock(fake, "ap-south-1")
    virginia = await make_bedrock(fake, "us-east-1")
    await mumbai.converse(modelId=GPT, messages=MSGS)
    await mumbai.converse(modelId=GPT, messages=MSGS)
    with pytest.raises(ClientError):
        await virginia.converse(modelId=GPT, messages=MSGS)
    stats = fake.engine.snapshot()["models"]
    assert stats == {
        f"{GPT}@ap-south-1": {**_zero(), "calls": 2, "ok": 2, "max_concurrent_seen": 1},
        f"{GPT}@us-east-1": {**_zero(), "calls": 1, "errors": 1},
    }


async def test_profile_ids_and_arns_resolve(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Profile ids (`:`) and ARNs (`/`) are URL-decoded and matched to the profile key."""
    profile = "apac.openai.gpt-oss-120b-1:0"
    fake = await start_fake({"models": {profile: {"max_concurrency": 80}}})
    client = await make_bedrock(fake)
    arn = f"arn:aws:bedrock:ap-south-1:123456789012:inference-profile/{profile}"
    await client.converse(modelId=profile, messages=MSGS)
    await client.converse(modelId=arn, messages=MSGS)
    assert fake.engine.stats[profile].calls == 2


@pytest.mark.parametrize("name", sorted(ERRORS))
async def test_every_error_parses_in_botocore(
    name: str, start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Each error-table row: botocore reads the code from x-amzn-ErrorType, status matches."""
    fake = await start_fake({"defaults": {"force_error": name}})
    client = await make_bedrock(fake)
    with pytest.raises(ClientError) as info:
        await client.converse(modelId=GPT, messages=MSGS)
    error = info.value.response
    assert error["Error"]["Code"] == name
    assert error["Error"]["Message"] == ERRORS[name].message
    assert error["ResponseMetadata"]["HTTPStatusCode"] == ERRORS[name].status


async def test_strict_models_unknown_key_is_not_found(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    fake = await start_fake({"strict_models": True, "models": {f"{GPT}@ap-south-1": {}}})
    client = await make_bedrock(fake)
    await client.converse(modelId=GPT, messages=MSGS)
    with pytest.raises(ClientError) as info:
        await client.converse(modelId="nope", messages=MSGS)
    assert info.value.response["Error"]["Code"] == "ResourceNotFoundException"


def _zero() -> dict[str, int]:
    keys = ("calls", "ok", "concurrent", "max_concurrent_seen", "throttled", "errors")
    return dict.fromkeys(keys, 0)
