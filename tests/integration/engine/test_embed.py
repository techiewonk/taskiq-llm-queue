"""Embeddings over the queue on Valkey and Redis (plan step A13; scenarios B15, B32, B97).

Titan one text per call, Cohere up to 96, a smaller `embed_batch_size`, vectors in input
order (checked against fakellm's deterministic vectors), and large results kept in S3 (moto).
"""

from __future__ import annotations

import pytest
import redis.asyncio
from engine_testkit import Make, stats
from fakellm.synth import embedding

from taskiq_llm_queue.engine.offload import MARKER, S3Offload

pytestmark = pytest.mark.integration
COHERE = "cohere.embed-english-v3"
TITAN = "amazon.titan-embed-text-v2:0"


async def test_titan_one_text_per_call(make: Make) -> None:
    engine, fake = await make()
    texts = [f"text {i}" for i in range(5)]
    vectors = await engine.embed("titan", texts)
    assert len(vectors) == 5
    assert all(len(v) == 256 for v in vectors)  # dimensions from the registry
    assert vectors[3] == pytest.approx(embedding("text 3", 256))
    assert stats(fake, f"{TITAN}@r1")["ok"] == 5


async def test_cohere_batches_keep_order(make: Make) -> None:
    engine, fake = await make()
    texts = [f"doc {i}" for i in range(200)]
    vectors = await engine.embed("cohere", texts)
    assert len(vectors) == 200
    assert vectors[150] == pytest.approx(embedding("doc 150", 1024))
    assert stats(fake, f"{COHERE}@r1")["ok"] == 3  # 96 + 96 + 8


async def test_smaller_batch_size(make: Make) -> None:
    engine, fake = await make({"cohere": {"embed_batch_size": 10}})
    await engine.embed("cohere", [f"d{i}" for i in range(25)])
    assert stats(fake, f"{COHERE}@r1")["ok"] == 3
    assert await engine.embed("cohere", []) == []


async def test_large_results_go_to_object_storage(make: Make, store: redis.asyncio.Redis) -> None:
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    kwargs = {
        "endpoint_url": endpoint,
        "region_name": "us-east-1",
        "aws_access_key_id": "x",
        "aws_secret_access_key": "x",
    }
    offload = S3Offload("llmq-results", client_kwargs=kwargs)
    try:
        s3 = await offload._s3()
        await s3.create_bucket(Bucket="llmq-results")
        engine, _ = await make()
        engine.requests.offload = offload
        engine.requests.offload_bytes = 1000
        reply = await engine.call("cohere", {"body": {"texts": ["a", "b"]}}, request_id="big-one")
        assert reply.response is not None
        raw = await store.get(engine.keys.request("big-one", "res"))
        assert raw is not None
        assert isinstance(raw, bytes)
        assert raw.startswith(MARKER)  # the state store keeps only a reference
        objects = await s3.list_objects_v2(Bucket="llmq-results")
        assert objects["KeyCount"] == 1
    finally:
        await offload.aclose()
        server.stop()
