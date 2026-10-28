"""OpenAI-compatible front of fakellm, exercised through the real `AsyncOpenAI` client.

Covers chat (text and tool_calls), embeddings (base64 and float) and that rate limits
and 5xx map onto the SDK's exception classes with Retry-After headers intact.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import Any

import openai
import pytest
from fakellm_testkit import Fake, StartFake
from openai import AsyncOpenAI

Client = Callable[[Fake], AsyncOpenAI]
MSGS: Any = [{"role": "user", "content": "hi there"}]


async def test_chat_completion(start_fake: StartFake, make_openai: Client) -> None:
    client = make_openai(await start_fake())
    first = await client.chat.completions.create(model="llama-3.1-70b", messages=MSGS)
    again = await client.chat.completions.create(model="llama-3.1-70b", messages=MSGS)
    assert first.choices[0].finish_reason == "stop"
    assert first.choices[0].message.content == again.choices[0].message.content
    assert (first.choices[0].message.content or "").startswith("fakellm reply ")
    assert first.usage is not None
    assert first.usage.total_tokens == first.usage.prompt_tokens + first.usage.completion_tokens
    assert first.model == "llama-3.1-70b"


async def test_chat_tool_calls_echo(start_fake: StartFake, make_openai: Client) -> None:
    client = make_openai(await start_fake())
    tools: Any = [
        {
            "type": "function",
            "function": {
                "name": "search",
                "parameters": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
                    "required": ["query"],
                },
            },
        }
    ]
    resp = await client.chat.completions.create(model="m", messages=MSGS, tools=tools)
    choice = resp.choices[0]
    assert choice.finish_reason == "tool_calls"
    calls = choice.message.tool_calls or []
    assert len(calls) == 1
    call: Any = calls[0]
    assert call.function.name == "search"
    assert json.loads(call.function.arguments) == {"query": "hi there"}
    follow: Any = [
        *MSGS,
        {"role": "assistant", "content": None, "tool_calls": [call.model_dump()]},
        {"role": "tool", "tool_call_id": call.id, "content": "result"},
    ]
    final = await client.chat.completions.create(model="m", messages=follow, tools=tools)
    assert final.choices[0].finish_reason == "stop"


async def test_embeddings_base64_and_float(start_fake: StartFake, make_openai: Client) -> None:
    """The SDK requests base64 by default and decodes it; floats must match exactly-ish."""
    client = make_openai(await start_fake({"defaults": {"dims": 32}}))
    b64 = await client.embeddings.create(model="e", input=["a", "b", "a"])
    flt = await client.embeddings.create(model="e", input="a", encoding_format="float")
    small = await client.embeddings.create(model="e", input="a", dimensions=8)
    vectors = [d.embedding for d in b64.data]
    assert [len(v) for v in vectors] == [32, 32, 32]
    assert vectors[0] == vectors[2] != vectors[1]
    assert all(
        math.isclose(x, y, abs_tol=1e-6)
        for x, y in zip(vectors[0], flt.data[0].embedding, strict=True)
    )
    assert len(small.data[0].embedding) == 8
    assert b64.usage.total_tokens == 3


async def test_rate_limit_error_with_retry_after(
    start_fake: StartFake, make_openai: Client
) -> None:
    fake = await start_fake({"models": {"m": {"throttle_rate": 1.0, "retry_after_s": 2.5}}})
    client = make_openai(fake)
    with pytest.raises(openai.RateLimitError) as info:
        await client.chat.completions.create(model="m", messages=MSGS)
    err = info.value
    assert err.status_code == 429
    assert err.response.headers["Retry-After"] == "3"
    assert err.response.headers["retry-after-ms"] == "2500"
    assert err.response.headers["x-ratelimit-remaining-requests"] == "0"
    assert err.code == "rate_limit_exceeded"
    assert err.type == "requests"
    assert fake.engine.stats["m"].throttled == 1


@pytest.mark.parametrize(
    ("name", "exc", "status"),
    [
        ("InternalServerException", openai.InternalServerError, 500),
        ("ServiceUnavailableException", openai.InternalServerError, 503),
        ("ResourceNotFoundException", openai.NotFoundError, 404),
        ("AccessDeniedException", openai.PermissionDeniedError, 403),
        ("ValidationException", openai.BadRequestError, 400),
    ],
)
async def test_errors_map_to_sdk_exceptions(
    name: str,
    exc: type[openai.APIStatusError],
    status: int,
    start_fake: StartFake,
    make_openai: Client,
) -> None:
    client = make_openai(await start_fake({"defaults": {"force_error": name}}))
    with pytest.raises(exc) as info:
        await client.chat.completions.create(model="m", messages=MSGS)
    assert info.value.status_code == status
    body: Any = info.value.body
    assert set(body) == {"message", "type", "code", "param"}


async def test_bad_requests_are_validation_errors(
    start_fake: StartFake, make_openai: Client
) -> None:
    fake = await start_fake()
    client = make_openai(fake)
    with pytest.raises(openai.BadRequestError):
        await client.chat.completions.create(model="m", messages=[])
    with pytest.raises(openai.BadRequestError):
        await client.embeddings.create(model="e", input=[])
    assert fake.engine.stats["m"].errors == 1
