"""Small building blocks shared by scenario modules: call batches, steady streams, state probes."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from taskiq_llm_queue import LLMQueue
from tools.scenarios import api, stack
from tools.scenarios.harness import Ctx


async def calls(
    ctx: Ctx,
    q: LLMQueue,
    n: int,
    *,
    model: str = "gpt20b",
    lane: str = "bg",
    tag: str = "",
    start: int = 0,
    **kw: Any,
) -> list[api.Reply]:
    """`n` concurrent converse calls with rids `rid(start..start+n-1, tag)`."""
    return list(
        await asyncio.gather(
            *(
                api.converse(
                    q,
                    model,
                    api.converse_request(ctx.text(ctx.rid(i, tag)), max_tokens=ctx.max_tokens),
                    lane=lane,
                    request_id=ctx.rid(i, tag),
                    **kw,
                )
                for i in range(start, start + n)
            )
        )
    )


async def one(ctx: Ctx, q: LLMQueue, n: int | str, *, tag: str = "", **kw: Any) -> api.Reply:
    """One converse call (gpt20b, bg unless overridden) with rid `rid(n, tag)`."""
    kw.setdefault("lane", "bg")
    model = kw.pop("model", "gpt20b")
    request = api.converse_request(ctx.text(ctx.rid(n, tag)), max_tokens=ctx.max_tokens)
    return await api.converse(q, model, request, request_id=ctx.rid(n, tag), **kw)


@dataclass(slots=True)
class Stream:
    """A steady call stream (`rate` per second) recording (sent_at, reply) until stopped."""

    ctx: Ctx
    q: LLMQueue
    model: str
    lane: str
    rate: float
    tag: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    records: list[tuple[float, float, api.Reply]] = field(default_factory=list)
    _task: asyncio.Task[None] | None = None
    _pending: set[asyncio.Task[None]] = field(default_factory=set)
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

    def start(self) -> Stream:
        """Begin sending."""
        self._task = asyncio.create_task(self._run())
        return self

    async def _one(self, i: int) -> None:
        t0 = time.time()
        reply = await api.converse(
            self.q,
            self.model,
            api.converse_request(self.ctx.text(self.ctx.rid(i, self.tag))),
            lane=self.lane,
            request_id=self.ctx.rid(i, self.tag),
            **self.kwargs,
        )
        self.records.append((t0, time.time(), reply))

    async def _run(self) -> None:
        i = 0
        t_start = time.time()
        while not self._stop.is_set():
            task = asyncio.create_task(self._one(i))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)
            i += 1
            due = t_start + i / self.rate
            await asyncio.sleep(max(0.0, due - time.time()))

    async def stop(self, wait_secs: float = 120.0) -> list[tuple[float, float, api.Reply]]:
        """Stop sending and wait for outstanding calls."""
        self._stop.set()
        if self._task is not None:
            await self._task
        if self._pending:
            await asyncio.wait(set(self._pending), timeout=wait_secs)
        return sorted(self.records, key=lambda r: r[0])

    def between(self, t0: float, t1: float) -> list[api.Reply]:
        """Replies of calls sent in [t0, t1)."""
        return [r for (s, _, r) in self.records if t0 <= s < t1]


def outcome_counts(replies: list[api.Reply]) -> dict[str, int]:
    """Ok / error-kind counts."""
    return dict(Counter(r.error_kind for r in replies))


async def hgetall(ctx: Ctx, key: str) -> dict[str, str]:
    """A Valkey hash as text."""
    r = stack.redis_client(ctx.settings)
    try:
        raw = await r.hgetall(key)
    finally:
        await r.aclose()

    def text(value: bytes | str) -> str:
        return value.decode() if isinstance(value, bytes) else value

    return {text(k): text(v) for k, v in raw.items()}


async def breaker_state(ctx: Ctx, model: str, target: str) -> str:
    """Breaker position stored in Valkey (`closed` when absent)."""
    data = await hgetall(ctx, api.keys(ctx.settings.prefix).breaker(model, target))
    return data.get("state", "closed")


async def limiter_state(ctx: Ctx, model: str, target: str) -> dict[str, str]:
    """Limiter hash (limit, ramp, paused_until, ...)."""
    return await hgetall(ctx, api.keys(ctx.settings.prefix).limiter(model, target))


async def redis_call(ctx: Ctx, fn: Callable[[Any], Any]) -> Any:
    """Run `await fn(redis)` on a fresh harness client."""
    r = stack.redis_client(ctx.settings)
    try:
        return await fn(r)
    finally:
        await r.aclose()


async def wait_until(
    probe: Callable[[], Any], *, timeout_secs: float, every: float = 0.5
) -> float | None:
    """Poll `await probe()` until truthy; seconds taken, or None on timeout."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_secs:
        if await probe():
            return round(time.monotonic() - t0, 2)
        await asyncio.sleep(every)
    return None


def log_events(paths: list[Path]) -> dict[str, int]:
    """Counts of structured log events (`<level> <logger> <event> ...`) in worker logs."""
    found: Counter[str] = Counter()
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) >= 5 and parts[2] in ("WARNING", "ERROR", "CRITICAL"):
                found[parts[4]] += 1
    return dict(found)


def converse_shape(response: Any) -> tuple[list[str], dict[str, int]]:
    """Problems with a real Converse reply's shape, and its token usage.

    Used in aws mode instead of comparing with fakellm's deterministic reply: the message
    must carry a non-empty text or a tool-use block, a stop reason and token counts.
    """
    problems: list[str] = []
    if not isinstance(response, dict):
        return [f"response is {type(response).__name__}, not dict"], {}
    content = response.get("output", {}).get("message", {}).get("content") or []
    has_text = any(isinstance(b, dict) and str(b.get("text", "")).strip() for b in content)
    has_tool = any(isinstance(b, dict) and "toolUse" in b for b in content)
    if not (has_text or has_tool):
        blocks = [list(b) for b in content]
        problems.append(f"no text/toolUse block: {blocks} stopReason={response.get('stopReason')}")
    if not response.get("stopReason"):
        problems.append("no stopReason")
    raw = response.get("usage") or {}
    usage = {k: int(raw.get(k) or 0) for k in ("inputTokens", "outputTokens", "totalTokens")}
    if usage["inputTokens"] <= 0 or usage["outputTokens"] <= 0:
        problems.append(f"token usage missing: {raw}")
    return problems, usage
