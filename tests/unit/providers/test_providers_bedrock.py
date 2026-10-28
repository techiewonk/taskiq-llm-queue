"""BedrockProvider against a local fake runtime, through real aiobotocore parsing."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import BotoCoreError, ClientError
from providers_fake import FakeLLM, bedrock_error

from taskiq_llm_queue import ConfigError, ErrorKind, Target
from taskiq_llm_queue.providers import (
    BedrockProvider,
    classify_bedrock,
    decode_invoke_body,
    embedding_body,
    parse_embeddings,
)

PROFILE = "apac.openai.gpt-oss-120b-1:0"
TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "search",
                "description": "Search the web",
                "inputSchema": {
                    "json": {"type": "object", "properties": {"q": {"type": "string"}}}
                },
            }
        }
    ],
    "toolChoice": {"auto": {}},
}


def _converse_params(**extra: Any) -> dict[str, Any]:
    return {
        "modelId": "openai.gpt-oss-120b-1:0",
        "messages": [{"role": "user", "content": [{"text": "hi"}]}],
        **extra,
    }


async def test_converse_passes_params_through_and_drops_response_metadata(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    params = _converse_params(
        system=[{"text": "be brief"}],
        inferenceConfig={"maxTokens": 64, "temperature": 0.1},
        additionalModelRequestFields={"reasoning_effort": "low"},
    )
    reply = await bedrock.call("converse", params, Target(region="ap-south-1"))

    assert "ResponseMetadata" not in reply
    assert reply["output"]["message"]["content"] == [{"text": "echo from openai.gpt-oss-120b-1:0"}]
    assert reply["usage"]["cacheReadInputTokens"] == 5
    assert reply["usage"]["cacheWriteInputTokens"] == 2
    assert reply["metrics"] == {"latencyMs": 42}
    _, sent = fake.requests[-1]
    assert sent["system"] == [{"text": "be brief"}]
    assert sent["inferenceConfig"] == {"maxTokens": 64, "temperature": 0.1}
    assert sent["additionalModelRequestFields"] == {"reasoning_effort": "low"}


async def test_converse_tool_config_round_trips_a_tool_use_reply(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    reply = await bedrock.call("converse", _converse_params(toolConfig=TOOL_CONFIG), Target())

    assert fake.requests[-1][1]["toolConfig"] == TOOL_CONFIG
    assert reply["stopReason"] == "tool_use"
    block = reply["output"]["message"]["content"][0]["toolUse"]
    assert block == {"toolUseId": "tooluse_1", "name": "search", "input": {"q": "x"}}


async def test_profile_id_overrides_model_id_and_is_url_encoded(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    reply = await bedrock.call("converse", _converse_params(), Target(profile_id=PROFILE))

    assert fake.requests[-1][1]["_model_id"] == PROFILE
    assert reply["output"]["message"]["content"][0]["text"] == f"echo from {PROFILE}"


async def test_missing_model_id_is_fatal(bedrock: BedrockProvider) -> None:
    with pytest.raises(ValueError, match="modelId") as caught:
        await bedrock.call("converse", {"messages": []}, Target())
    assert bedrock.classify(caught.value).kind is ErrorKind.FATAL


async def test_unknown_op_raises(bedrock: BedrockProvider) -> None:
    with pytest.raises(ValueError, match="does not support"):
        await bedrock.call("chat", _converse_params(), Target())


async def test_invoke_model_titan(fake: FakeLLM, bedrock: BedrockProvider) -> None:
    body = embedding_body("amazon.titan-embed-text-v2:0", ["hello"], dimensions=4)
    reply = await bedrock.call(
        "invoke_model", {"modelId": "amazon.titan-embed-text-v2:0", "body": body}, Target()
    )

    assert reply["contentType"].startswith("application/json")
    assert set(reply) == {"body_b64", "contentType"}
    assert fake.requests[-1][1]["inputText"] == "hello"
    decoded = decode_invoke_body(reply)
    assert parse_embeddings(decoded) == [[0.25] * 4]


async def test_invoke_model_cohere_with_string_body(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    body = json.dumps(embedding_body("cohere.embed-english-v3", ["a", "b"]))
    reply = await bedrock.call(
        "invoke_model", {"modelId": "cohere.embed-english-v3", "body": body}, Target()
    )

    assert fake.requests[-1][1]["input_type"] == "search_document"
    assert parse_embeddings(decode_invoke_body(reply)) == [[0.0, 0.5], [1.0, 0.5]]


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        ("ThrottlingException", ErrorKind.THROTTLE),
        ("ModelNotReadyException", ErrorKind.THROTTLE),
        ("ServiceQuotaExceededException", ErrorKind.THROTTLE),
        ("ServiceUnavailableException", ErrorKind.RETRYABLE),
        ("InternalServerException", ErrorKind.RETRYABLE),
        ("ModelTimeoutException", ErrorKind.RETRYABLE),
        ("ModelErrorException", ErrorKind.RETRYABLE),
        ("ValidationException", ErrorKind.FATAL),
        ("AccessDeniedException", ErrorKind.FATAL),
        ("ResourceNotFoundException", ErrorKind.FATAL),
    ],
)
async def test_every_bedrock_error_classifies_and_is_called_once(
    fake: FakeLLM, bedrock: BedrockProvider, code: str, kind: ErrorKind
) -> None:
    """Also pins D32: SDK retries are off, so the server sees exactly one call per error."""
    fake.fail_next(bedrock_error(code))
    with pytest.raises(ClientError) as caught:
        await bedrock.call("converse", _converse_params(), Target())

    info = bedrock.classify(caught.value)
    assert info.kind is kind
    assert info.code == code
    assert fake.calls["converse"] == 1


@pytest.mark.parametrize(
    ("code", "status", "kind"),
    [
        ("Throttling", 400, ErrorKind.THROTTLE),
        ("throttling_exception", 400, ErrorKind.THROTTLE),
        ("TooManyRequestsException", 400, ErrorKind.THROTTLE),
        ("ModelStreamErrorException", 424, ErrorKind.RETRYABLE),
        ("InternalFailure", 500, ErrorKind.RETRYABLE),
        ("ValidationError", 400, ErrorKind.FATAL),
        ("SomethingNewException", 503, ErrorKind.RETRYABLE),
        ("SomethingNewException", 429, ErrorKind.THROTTLE),
        ("SomethingNewException", 400, ErrorKind.FATAL),
    ],
)
async def test_bedrock_classify_tolerates_spelling_and_falls_back_to_status(
    fake: FakeLLM, bedrock: BedrockProvider, code: str, status: int, kind: ErrorKind
) -> None:
    fake.fail_next(bedrock_error(code, status))
    with pytest.raises(ClientError) as caught:
        await bedrock.call("converse", _converse_params(), Target())
    assert bedrock.classify(caught.value).kind is kind


async def test_status_only_error_without_error_type_header(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    fake.fail_next(bedrock_error("ServiceUnavailableException", 503, header=False))
    with pytest.raises(ClientError) as caught:
        await bedrock.call("converse", _converse_params(), Target())
    assert bedrock.classify(caught.value).kind is ErrorKind.RETRYABLE


async def test_read_timeout_is_retryable(fake: FakeLLM) -> None:
    provider = BedrockProvider(
        endpoint_url=fake.url,
        read_timeout_secs=0.2,
        client_kwargs={"aws_access_key_id": "test", "aws_secret_access_key": "test"},
    )
    fake.delay_secs = 1.0
    try:
        with pytest.raises(BotoCoreError) as caught:
            await provider.call("converse", _converse_params(), Target())
    finally:
        await provider.aclose()
    assert provider.classify(caught.value).kind is ErrorKind.RETRYABLE
    assert fake.calls["converse"] == 1


async def test_connection_refused_is_retryable() -> None:
    provider = BedrockProvider(
        endpoint_url="http://127.0.0.1:9",
        connect_timeout_secs=1,
        client_kwargs={"aws_access_key_id": "test", "aws_secret_access_key": "test"},
    )
    try:
        with pytest.raises(BotoCoreError) as caught:
            await provider.call("converse", _converse_params(), Target())
    finally:
        await provider.aclose()
    assert provider.classify(caught.value).kind is ErrorKind.RETRYABLE


def test_classify_botocore_transport_and_generic_errors() -> None:
    from botocore.exceptions import (
        ConnectTimeoutError,
        EndpointConnectionError,
        ReadTimeoutError,
    )

    for exc in (
        ReadTimeoutError(endpoint_url="http://x"),
        ConnectTimeoutError(endpoint_url="http://x"),
        EndpointConnectionError(endpoint_url="http://x"),
        TimeoutError(),
        ConnectionResetError(),
    ):
        assert classify_bedrock(exc).kind is ErrorKind.RETRYABLE, exc
    assert classify_bedrock(KeyError("x")).kind is ErrorKind.FATAL


def test_classify_reads_retry_after_from_response_headers() -> None:
    exc = ClientError(
        {
            "Error": {"Code": "ThrottlingException", "Message": "slow down"},
            "ResponseMetadata": {
                "HTTPStatusCode": 429,
                "HTTPHeaders": {"retry-after": "3"},
                "RequestId": "r",
                "HostId": "h",
                "RetryAttempts": 0,
            },
        },
        "Converse",
    )
    info = classify_bedrock(exc)
    assert (info.kind, info.retry_after_secs) == (ErrorKind.THROTTLE, 3.0)


async def test_clients_are_cached_per_region_and_closed(
    fake: FakeLLM, bedrock: BedrockProvider
) -> None:
    await bedrock.call("converse", _converse_params(), Target(region="ap-south-1"))
    await bedrock.call("converse", _converse_params(), Target(region="ap-south-1"))
    await bedrock.call("converse", _converse_params(), Target(region="ap-southeast-1"))
    assert len(bedrock._clients) == 2
    await bedrock.aclose()
    await bedrock.aclose()
    assert bedrock._clients == {}


def test_denied_endpoint_rejected_before_any_client() -> None:
    with pytest.raises(ConfigError, match="denied host"):
        BedrockProvider(
            endpoint_url="https://myres.openai.azure.com", denied_hosts=["*.openai.azure.com"]
        )


async def test_denied_target_url_rejected_at_call(fake: FakeLLM) -> None:
    provider = BedrockProvider(endpoint_url=fake.url, denied_hosts=["*.openai.azure.com"])
    with pytest.raises(ConfigError):
        await provider.call(
            "converse", _converse_params(), Target(base_url="https://x.openai.azure.com")
        )
    assert fake.calls == {}
    await provider.aclose()
