"""Streaming to the caller (plan step A17, §3.13, F6, R19) against fakellm and the store.

Covers events in order with the first one early, batching by `stream_flush_ms`, the direct
path, a cut mid-stream (error with the partial text, no switch to another deployment), a
failure before any chunk, a stream another worker began (`worker_lost`) and the idle timer.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from engine_testkit import CHAT, SMALL, Make, stats

from taskiq_llm_queue.engine.errors import CallFailed

KEY = f"{SMALL}@r1"


async def _drain(events: AsyncIterator[dict[str, Any]], seen: list[dict[str, Any]]) -> None:
    async for event in events:
        seen.append(event)


def _text(events: list[dict[str, Any]]) -> str:
    return "".join(
        e["contentBlockDelta"]["delta"].get("text", "") for e in events if "contentBlockDelta" in e
    )


async def test_events_arrive_in_order_and_early(make: Make) -> None:
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 80, "tokens_per_s": 100}}},
    )
    await engine.call("small", CHAT)  # warm: the client's start-up is not what is measured
    started = time.monotonic()
    events: list[dict[str, Any]] = []
    first = None
    async for event in engine.stream("small", CHAT, request_id="s-order"):
        if first is None:
            first = time.monotonic() - started
        events.append(event)
    total = time.monotonic() - started
    assert "messageStart" in events[0]
    assert events[-1]["metadata"]["usage"]["outputTokens"] == 80
    assert len(_text(events)) == 320
    assert first is not None
    assert first < total / 2  # the first chunk is never held back
    status = await engine.requests.status("s-order")
    assert status["state"] == "done"


async def test_flush_window_batches_chunks(make: Make) -> None:
    engine, _ = await make(
        {"small": {"stream_flush_ms": 300}},
        {"models": {KEY: {"output_tokens": 80, "tokens_per_s": 200}}},
    )
    events = [e async for e in engine.stream("small", CHAT, request_id="s-batch")]
    entries = await engine.state.xlen(engine.keys.request("s-batch", "chunks"))
    assert len(_text(events)) == 320
    assert entries < len(events) / 2  # ~14 events over 0.4 s land in a few entries


async def test_direct_path_streams_too(make: Make) -> None:
    engine, _ = await make(worker=False)
    events = [e async for e in engine.stream("small", CHAT, force_direct=True)]
    assert "messageStop" in events[-2]


async def test_cut_mid_stream_ends_with_partial_text(make: Make) -> None:
    engine, fake = await make(
        {"small": {"stream_flush_ms": 0, "max_attempts": 3}},
        {"models": {KEY: {"output_tokens": 80, "cut_after_chunks": 2}}},
    )
    seen: list[dict[str, Any]] = []
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT), seen)
    assert info.value.kind == "provider_error"
    assert info.value.partial_text == _text(seen)
    assert len(info.value.partial_text or "") == 64
    assert stats(fake, KEY)["calls"] == 1  # never retried once chunks were sent


async def test_failure_before_any_chunk_raises_the_call_error(make: Make) -> None:
    engine, _ = await make({}, {"models": {KEY: {"force_error": "ValidationException"}}})
    seen: list[dict[str, Any]] = []
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT), seen)
    assert info.value.kind == "provider_error"
    assert not seen


async def test_stream_begun_elsewhere_ends_with_worker_lost(make: Make) -> None:
    engine, fake = await make()
    key = engine.keys.request("s-lost", "chunks")
    await engine.state.xadd(key, {"seq": 1, "att": "old", "t": "chunk", "d": b"[]"})
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT, request_id="s-lost"), [])
    assert info.value.kind == "worker_lost"
    assert not stats(fake, KEY)  # the provider was never called again


async def test_idle_stream_fails_with_partial_text(make: Make) -> None:
    engine, _ = await make(
        {"small": {"stream_idle_secs": 0.3, "stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 80, "tokens_per_s": 10}}},
    )
    seen: list[dict[str, Any]] = []
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT), seen)
    assert _text(seen)  # the first chunk arrived before the stream went quiet
    assert info.value.kind in ("provider_error", "deadline_exceeded")
