"""In-process moto S3 server on a free port, plus a fresh bucket per test.

No Docker needed: `ThreadedMotoServer` serves the real S3 wire protocol that aiobotocore
speaks, so the middleware and backend run their production code paths.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from aiobotocore.session import get_session
from moto.server import ThreadedMotoServer

REGION = "us-east-1"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def moto_endpoint() -> Iterator[str]:
    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def s3_kwargs(moto_endpoint: str) -> dict[str, Any]:
    """Connection settings for the code under test (dummy credentials, moto endpoint)."""
    return {
        "endpoint_url": moto_endpoint,
        "region_name": REGION,
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
    }


@pytest.fixture
async def s3_client(s3_kwargs: dict[str, Any]) -> AsyncIterator[Any]:
    """A separate client the tests use to inspect the bucket directly."""
    async with get_session().create_client("s3", **s3_kwargs) as client:
        yield client


@pytest.fixture
async def bucket(s3_client: Any) -> str:
    name = f"llmq-{uuid.uuid4().hex[:12]}"
    await s3_client.create_bucket(Bucket=name)
    return name
