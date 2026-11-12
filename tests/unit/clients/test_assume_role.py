"""Assume-role accounts (plan step A15): STS AssumeRole per account, refreshed before expiry.

Runs moto's STS server on a local port and fakellm as Bedrock; no AWS call. A failed or
unreachable STS, or a missing external id secret, is a `CredentialsUnavailable` error.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from clients_testkit import StartFake, registry_doc
from moto.server import ThreadedMotoServer

from taskiq_llm_queue.clients import (
    ClientPool,
    CredentialsUnavailable,
    Providers,
    Timing,
    route_for,
)
from taskiq_llm_queue.registry import load_registry

ROLE = "arn:aws:iam::123456789012:role/llmq-bedrock"
CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def sts_url() -> Iterator[str]:
    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
async def make(start_fake: StartFake, sts_url: str) -> AsyncIterator[Any]:
    built: list[Providers] = []

    async def build(
        *, external: str | None = "ext-123", sts: str | None = None
    ) -> tuple[Any, Providers]:
        url, _ = await start_fake()
        doc = registry_doc(url)
        doc["accounts"]["aws-b"] = {
            "kind": "aws",
            "role_arn": ROLE,
            "external_id_ref": "AWS_B_EXTERNAL_ID",
        }
        view = load_registry(doc)
        secrets = {"VLLM_KEY": "k", **({"AWS_B_EXTERNAL_ID": external} if external else {})}
        pool = ClientPool(
            bedrock_endpoint_url=url,
            credentials=lambda account: {
                "aws_access_key_id": "base",
                "aws_secret_access_key": "x",
            },
            secrets=secrets.get,
            sts_endpoint_url=sts or sts_url,
            session_name="llmq-test-pod1",
        )
        providers = Providers(pool)
        built.append(providers)
        route = route_for(
            view, next(d for d in view.deployments("gpt120b") if d.account == "aws-b")
        )
        return route, providers

    yield build
    for providers in built:
        await providers.aclose()


async def test_assumed_role_credentials_sign_the_calls(make: Any) -> None:
    route, providers = await make()
    result = await providers.get("bedrock").call("converse", CHAT, route, FAST)
    assert result.response["stopReason"] == "end_turn"
    client = await providers.pool.bedrock(route)
    creds = await client._request_signer._credentials.get_frozen_credentials()
    assert creds.access_key != "base"  # temporary keys from STS, not the base ones
    assert creds.token
    assert await providers.pool.bedrock(route) is client  # one client per (account, region)


async def test_role_session_is_reused_and_forgotten(make: Any) -> None:
    route, providers = await make()
    pool = providers.pool
    first = await pool._role_session(route)
    assert await pool._role_session(route) is first
    pool.forget(route)
    assert await pool._role_session(route) is not first


async def test_missing_external_id_secret_is_a_credential_failure(make: Any) -> None:
    route, providers = await make(external=None)
    with pytest.raises(CredentialsUnavailable, match="AWS_B_EXTERNAL_ID"):
        await providers.pool.bedrock(route)


async def test_unreachable_sts_is_a_credential_failure(make: Any) -> None:
    route, providers = await make(sts=f"http://127.0.0.1:{_free_port()}")
    with pytest.raises(CredentialsUnavailable, match="AssumeRole failed"):
        await providers.pool.bedrock(route)
