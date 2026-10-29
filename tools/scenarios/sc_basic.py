"""Functional scenarios: round trips, embeddings, cross-pod, limiter, pause gate, fallback, breaker.

Each scenario starts its own workers and sender against the isolated harness stack; `isolate()`
has already purged queues, flushed the prefix and reloaded the base fakellm scenario.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

from aiobotocore.session import get_session

from tools.scenarios import api
from tools.scenarios.common import converse_shape
from tools.scenarios.env import (
    GPT20B_ID,
    REGION,
)
from tools.scenarios.harness import Ctx, scenario

TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "lookup",
                "description": "Look up a ticker",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {"ticker": {"type": "string"}},
                        "required": ["ticker"],
                    }
                },
            }
        }
    ]
}


def normalize(response: dict[str, Any]) -> dict[str, Any]:
    """Converse response without latency and response metadata (the "identical" rule)."""
    return {k: v for k, v in response.items() if k not in ("ResponseMetadata", "metrics")}


async def direct_converse(ctx: Ctx, model_id: str, request: dict[str, Any]) -> dict[str, Any]:
    """The same Converse call straight to fakellm with aiobotocore (the `direct` reference)."""
    async with get_session().create_client(
        "bedrock-runtime", region_name=REGION, endpoint_url=ctx.settings.fakellm_url
    ) as client:
        response = await client.converse(modelId=model_id, **request)
    return normalize(dict(response))


@scenario("S01", "Chat round trip (Bedrock)", aws_safe="yes")
async def s01(ctx: Ctx) -> None:
    """Rt converse through the queue equals the direct call (text and tool use)."""
    await ctx.workers(1)
    q = await ctx.queue()
    if ctx.aws:
        await _s01_aws(ctx, q)
        return
    for n, tools in ((1, None), (2, TOOL_CONFIG)):
        request = api.converse_request(ctx.text(ctx.rid(n), "price of INFY?"))
        if tools:
            request["toolConfig"] = tools
        reply = await api.converse(q, "gpt20b", request, lane="rt", request_id=ctx.rid(n))
        ctx.expect(reply.ok, f"queued call failed: {reply.error!r}")
        direct = await direct_converse(ctx, GPT20B_ID, request)
        ctx.expect(normalize(reply.response) == direct, f"differs: {reply.response} vs {direct}")
        ctx.expect(reply.meta.served_model == "gpt20b", f"meta: {reply.meta}")
    calls = await ctx.fake.calls(ctx.rid_prefix())
    ctx.metric("provider_calls", sum(len(v) for v in calls.values()))
    ctx.metric("queue_wait_ms", reply.meta.queue_wait_ms)
    ctx.expect(all(len(v) == 2 for v in calls.values()), f"each rid called once+direct: {calls}")
    await ctx.final_checks()


async def _s01_aws(ctx: Ctx, q: Any) -> None:
    """S01 on real Bedrock: reply shape and usage per call, exactly one attempt per call.

    No direct reference call (it would double the cost and replies are not deterministic).
    """
    usage: Counter[str] = Counter()
    provider_ms: list[int] = []
    for n, tools in ((1, None), (2, TOOL_CONFIG)):
        text = ctx.text(ctx.rid(n), "price of INFY?")
        request = api.converse_request(text, max_tokens=ctx.max_tokens)
        if tools:
            request["toolConfig"] = tools
        reply = await api.converse(q, "gpt20b", request, lane="rt", request_id=ctx.rid(n))
        ctx.expect(reply.ok, f"queued call failed: {reply.error!r}")
        problems, tokens = converse_shape(reply.response)
        ctx.expect(not problems, f"call {n} reply shape: {problems}")
        ctx.expect(reply.meta.served_model == "gpt20b", f"meta: {reply.meta}")
        usage.update(tokens)
        provider_ms.append(reply.meta.provider_ms)
    attempts = await ctx.provider_attempts()
    ctx.metric("provider_calls", attempts)
    ctx.metric("provider_ms", provider_ms)
    ctx.metric("queue_wait_ms", reply.meta.queue_wait_ms)
    ctx.metric("tokens", dict(usage))
    ctx.expect(attempts == 2, f"{attempts} provider attempts for 2 calls")
    await ctx.final_checks()


@scenario("S03", "Embeddings (Titan, Cohere, OpenAI-compatible)", aws_safe="yes")
async def s03(ctx: Ctx) -> None:
    """100 texts per model: vectors equal fakellm's deterministic ones, batched per provider."""
    from fakellm.synth import embedding

    await ctx.workers(1)
    q = await ctx.queue()
    if ctx.aws:
        await _s03_aws(ctx, q)
        return
    calls_per_model: dict[str, int] = {}
    for model, tag, dims, expected_calls in (
        ("titan", "t", None, 100),
        ("cohere", "c", 256, 2),
    ):
        texts = [ctx.text(ctx.rid(i, tag), f"doc {i}") for i in range(100)]
        reply = await api.embed(q, model, texts, lane="bg", input_type="search_document")
        ctx.expect(reply.ok, f"{model} embed failed: {reply.error!r}")
        vectors = reply.response
        ctx.expect(len(vectors) == 100, f"{model}: {len(vectors)} vectors")
        want_dims = dims or len(vectors[0])
        for text, vec in zip(texts, vectors, strict=True):
            ref = embedding(text, want_dims)
            err = max(abs(a - b) for a, b in zip(vec, ref, strict=True))
            ctx.expect(err < 1e-5, f"{model} vector differs from direct (max err {err})")
        log = await ctx.fake.calls(ctx.rid_prefix(tag))
        calls_per_model[model] = sum(len(v) for v in log.values())
        ctx.expect(
            calls_per_model[model] == expected_calls,
            f"{model}: {calls_per_model[model]} provider calls, want {expected_calls}",
        )
    ctx.metric("provider_calls", calls_per_model)
    await ctx.final_checks()


@scenario("S05", "Cross-pod execution (1 sender, 3 workers)", aws_safe="low-volume")
async def s05(ctx: Ctx) -> None:
    """Each of 3 workers executes >= 15 % of 300 calls; every result reaches the sender."""
    await ctx.workers(3)
    q = await ctx.queue()
    n = 300
    replies = await asyncio.gather(
        *(
            api.converse(
                q,
                "gpt20b",
                api.converse_request(ctx.text(ctx.rid(i))),
                lane="bg",
                request_id=ctx.rid(i),
            )
            for i in range(n)
        )
    )
    failed = [r.error for r in replies if not r.ok]
    ctx.expect(not failed, f"{len(failed)} failed: {failed[:3]}")
    served = Counter(r.meta.worker for r in replies)
    ctx.metric("served", dict(served))
    shares = {w: c / n for w, c in served.items()}
    ctx.metric("min_share", round(min(shares.values()), 3))
    ctx.expect(len(served) == 3, f"workers used: {dict(served)}")
    ctx.expect(min(shares.values()) >= 0.15, f"a worker served < 15 %: {dict(served)}")
    await ctx.final_checks()


async def _s03_aws(ctx: Ctx, q: Any) -> None:
    """S03 on real Bedrock: 100 vectors per model of the model's default size, finite, non-zero.

    Batching is checked by worker attempts per alias (Titan one text per call, Cohere 96).
    """
    import math

    attempts: dict[str, int] = {}
    secs: dict[str, float] = {}
    for model, tag, dims, expected_calls in (("titan", "t", 1024, 100), ("cohere", "c", 1024, 2)):
        texts = [ctx.text(ctx.rid(i, tag), f"doc {i}") for i in range(100)]
        t0 = asyncio.get_running_loop().time()
        reply = await api.embed(q, model, texts, lane="bg", input_type="search_document")
        secs[model] = round(asyncio.get_running_loop().time() - t0, 2)
        ctx.expect(reply.ok, f"{model} embed failed: {reply.error!r}")
        vectors = reply.response
        ctx.expect(len(vectors) == 100, f"{model}: {len(vectors)} vectors")
        sizes = Counter(len(v) for v in vectors)
        ctx.expect(set(sizes) == {dims}, f"{model}: vector sizes {dict(sizes)}, want {dims}")
        bad = [
            i for i, v in enumerate(vectors) if not all(math.isfinite(x) for x in v) or not any(v)
        ]
        ctx.expect(not bad, f"{model}: non-finite or zero vectors at {bad[:5]}")
        attempts[model] = await ctx.provider_attempts(model)
        ctx.expect(
            attempts[model] == expected_calls,
            f"{model}: {attempts[model]} provider attempts, want {expected_calls}",
        )
    ctx.metric("provider_calls", attempts)
    ctx.metric("embed_secs", secs)
    await ctx.final_checks()
