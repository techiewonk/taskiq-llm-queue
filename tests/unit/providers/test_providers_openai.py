"""OpenAICompatProvider against a local fake server, through the real openai SDK."""

from __future__ import annotations

import time
from email.utils import formatdate

import openai
import pytest
from providers_fake import FakeLLM, openai_error

from taskiq_llm_queue import ConfigError, ErrorKind, ModelConfig, QueueConfig, Target
from taskiq_llm_queue.providers import OpenAICompatProvider, classify_openai, parse_retry_after

CHAT = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}


async def test_chat_forces_stream_off_and_returns_dict(
    fake: FakeLLM, openai_provider: OpenAICompatProvider
) -> None:
    reply = await openai_provider.call("chat", {**CHAT, "stream": True, "seed": 7}, Target())

    _, sent = fake.requests[-1]
    assert sent["stream"] is False
    assert sent["seed"] == 7
    assert reply["choices"][0]["message"] == {"role": "assistant", "content": "hello"}
    assert reply["usage"]["total_tokens"] == 4


async def test_embeddings_return_float_vectors(
    fake: FakeLLM, openai_provider: OpenAICompatProvider
) -> None:
    reply = await openai_provider.call(
        "embeddings", {"model": "text-embedding-3-small", "input": ["a", "b"]}, Target()
    )

    assert [d["embedding"] for d in reply["data"]] == [[0.0, 0.5, -1.0], [1.0, 0.5, -1.0]]
    assert fake.calls["embeddings"] == 1


async def test_unknown_op_raises(openai_provider: OpenAICompatProvider) -> None:
    with pytest.raises(ValueError, match="does not support"):
        await openai_provider.call("converse", CHAT, Target())


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (429, ErrorKind.THROTTLE),
        (500, ErrorKind.RETRYABLE),
        (503, ErrorKind.RETRYABLE),
        (408, ErrorKind.RETRYABLE),
        (400, ErrorKind.FATAL),
        (401, ErrorKind.FATAL),
        (404, ErrorKind.FATAL),
    ],
)
async def test_status_classification_and_single_attempt(
    fake: FakeLLM, openai_provider: OpenAICompatProvider, status: int, kind: ErrorKind
) -> None:
    """Also pins D88: `max_retries=0`, so the server sees exactly one call per error."""
    fake.fail_next(openai_error(status, code="rate_limit_exceeded" if status == 429 else ""))
    with pytest.raises(openai.APIStatusError) as caught:
        await openai_provider.call("chat", CHAT, Target())

    assert openai_provider.classify(caught.value).kind is kind
    assert fake.calls["chat"] == 1


async def test_retry_after_ms_wins_over_retry_after(
    fake: FakeLLM, openai_provider: OpenAICompatProvider
) -> None:
    fake.fail_next(openai_error(429, {"Retry-After": "9", "retry-after-ms": "1500"}))
    with pytest.raises(openai.APIStatusError) as caught:
        await openai_provider.call("chat", CHAT, Target())
    info = openai_provider.classify(caught.value)
    assert (info.kind, info.retry_after_secs) == (ErrorKind.THROTTLE, 1.5)


async def test_retry_after_seconds(fake: FakeLLM, openai_provider: OpenAICompatProvider) -> None:
    fake.fail_next(openai_error(429, {"Retry-After": "7"}, code="rate_limit_exceeded"))
    with pytest.raises(openai.APIStatusError) as caught:
        await openai_provider.call("chat", CHAT, Target())
    info = openai_provider.classify(caught.value)
    assert (info.code, info.retry_after_secs) == ("rate_limit_exceeded", 7.0)


async def test_retry_after_http_date(fake: FakeLLM, openai_provider: OpenAICompatProvider) -> None:
    fake.fail_next(openai_error(429, {"Retry-After": formatdate(time.time() + 30, usegmt=True)}))
    with pytest.raises(openai.APIStatusError) as caught:
        await openai_provider.call("chat", CHAT, Target())
    wait = openai_provider.classify(caught.value).retry_after_secs
    assert wait is not None
    assert 27 <= wait <= 31


async def test_insufficient_quota_is_fatal_not_throttle(
    fake: FakeLLM, openai_provider: OpenAICompatProvider
) -> None:
    fake.fail_next(openai_error(429, code="insufficient_quota"))
    with pytest.raises(openai.APIStatusError) as caught:
        await openai_provider.call("chat", CHAT, Target())
    assert openai_provider.classify(caught.value).kind is ErrorKind.FATAL


async def test_timeout_and_connection_errors_are_retryable(fake: FakeLLM) -> None:
    slow = OpenAICompatProvider(base_url=f"{fake.url}/v1", api_keys={None: "k"}, timeout_secs=0.2)
    fake.delay_secs = 1.0
    try:
        with pytest.raises(openai.APIConnectionError) as caught:
            await slow.call("chat", CHAT, Target())
    finally:
        await slow.aclose()
    assert slow.classify(caught.value).kind is ErrorKind.RETRYABLE
    assert fake.calls["chat"] == 1

    dead = OpenAICompatProvider(base_url="http://127.0.0.1:9/v1", api_keys={None: "k"})
    try:
        with pytest.raises(openai.APIConnectionError) as caught:
            await dead.call("chat", CHAT, Target())
    finally:
        await dead.aclose()
    assert dead.classify(caught.value).kind is ErrorKind.RETRYABLE


def test_classify_non_http_errors() -> None:
    assert classify_openai(TimeoutError()).kind is ErrorKind.RETRYABLE
    assert classify_openai(ValueError("bad")).kind is ErrorKind.FATAL


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"retry-after-ms": "250"}, 0.25),
        ({"Retry-After": "2"}, 2.0),
        ({"RETRY-AFTER": "1.5"}, 1.5),
        ({"Retry-After": "Wed, 21 Oct 2015 07:28:10 GMT"}, 10.0),
        ({"Retry-After": "Wed, 21 Oct 2015 07:27:00 GMT"}, 0.0),
        ({"Retry-After": "soon"}, None),
        ({"retry-after-ms": "x", "Retry-After": "3"}, 3.0),
        ({}, None),
        (None, None),
    ],
)
def test_parse_retry_after(headers: dict[str, str] | None, expected: float | None) -> None:
    now = 1445412480.0  # 2015-10-21 07:28:00 GMT
    assert parse_retry_after(headers, now=now) == expected


async def test_target_base_url_gets_its_own_client_and_key(fake: FakeLLM) -> None:
    provider = OpenAICompatProvider(api_keys={f"{fake.url}/v1": "sk-target"})
    try:
        await provider.call("chat", CHAT, Target(base_url=f"{fake.url}/v1"))
        client = provider._clients[f"{fake.url}/v1"]
        assert client.api_key == "sk-target"
        assert client.max_retries == 0
    finally:
        await provider.aclose()


async def test_denied_base_url_never_contacted(fake: FakeLLM) -> None:
    provider = OpenAICompatProvider(denied_hosts=["*.openai.azure.com"], api_keys={None: "k"})
    with pytest.raises(ConfigError, match="denied host"):
        await provider.call("chat", CHAT, Target(base_url="https://res.openai.azure.com/v1"))
    assert fake.calls == {}


def test_from_config_reads_keys_and_reports_every_problem() -> None:
    config = QueueConfig(
        models={
            "a": ModelConfig(
                provider="openai",
                model_id="m",
                targets={"p": Target(base_url="http://h1/v1")},
                api_key_env="KEY_A",
            ),
            "b": ModelConfig(
                provider="openai",
                model_id="m",
                targets={"p": Target(base_url="http://h1/v1")},
                api_key_env="KEY_B",
            ),
            "c": ModelConfig(
                provider="openai",
                model_id="m",
                targets={"p": Target(base_url="http://h2/v1")},
                api_key_env="KEY_C",
            ),
        }
    )
    with pytest.raises(ConfigError) as caught:
        OpenAICompatProvider.from_config(config, environ={"KEY_A": "x"})
    message = str(caught.value)
    assert "already uses 'KEY_A'" in message
    assert "'KEY_C'" in message

    ok = OpenAICompatProvider.from_config(
        config.model_copy(update={"models": {"a": config.models["a"]}}), environ={"KEY_A": "secret"}
    )
    assert ok._api_keys == {"http://h1/v1": "secret"}
