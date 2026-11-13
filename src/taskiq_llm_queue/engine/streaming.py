"""Streaming to the caller (plan §3.13, F6, R19): chunk streams in the state store.

The worker serving a streaming call appends the provider's events to the request's chunk
stream (`P:{r:ID}:chunks`) through a `ChunkWriter`: the first event at once, later ones batched
by `stream_flush_ms`, then one `end` or `error` entry. Each caller process runs one
`ChunkReader` that reads all its open chunk streams with one blocking read.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import orjson

from taskiq_llm_queue._logging import get_logger

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)


class StreamTooLarge(Exception):
    """The chunk stream went above `stream_max_bytes` (plan §3.13: fail, never trim)."""


def text_of(event: Mapping[str, Any]) -> str:
    """The answer text an event adds: a Bedrock `contentBlockDelta` or an OpenAI chunk delta."""
    delta = event.get("contentBlockDelta", {}).get("delta", {})
    if "text" in delta:
        return str(delta["text"])
    choices = event.get("choices") or []
    if choices and isinstance(choices[0], Mapping):
        content = (choices[0].get("delta") or {}).get("content")
        return str(content) if content else ""
    return ""


class ChunkWriter:
    """Writes one attempt's events to a chunk stream; `sent` once any entry was written."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        key: str,
        attempt_id: str,
        *,
        flush_ms: int,
        max_bytes: int,
        ttl_secs: int,
    ) -> None:
        """Keep the target; nothing is written until the first event."""
        self._client = client
        self.key = key
        self.attempt_id = attempt_id
        self._flush_secs = flush_ms / 1000
        self._max_bytes = max_bytes
        self._ttl = ttl_secs
        self._buffer: list[Any] = []
        self._seq = 0
        self._bytes = 0
        self._last = 0.0
        self._lock = asyncio.Lock()
        self._timer: asyncio.Task[None] | None = None
        self._failed: Exception | None = None
        self.partial: list[str] = []

    @property
    def sent(self) -> bool:
        """True once the caller may have seen part of the answer."""
        return self._seq > 0 or bool(self._buffer)

    @property
    def partial_text(self) -> str:
        """The answer text written so far."""
        return "".join(self.partial)

    async def add(self, event: Any) -> None:
        """One provider event: written now if it is the first or the flush window has passed."""
        if self._failed is not None:
            raise self._failed
        data = event.model_dump(exclude_none=True) if hasattr(event, "model_dump") else event
        self._buffer.append(data)
        if text := text_of(data):
            self.partial.append(text)
        if (
            self._seq == 0
            or self._flush_secs == 0
            or time.monotonic() - self._last >= self._flush_secs
        ):
            await self._flush()
        elif self._timer is None or self._timer.done():
            self._timer = asyncio.create_task(self._flush_later())

    async def _flush_later(self) -> None:
        await asyncio.sleep(self._flush_secs)
        try:
            await self._flush()
        except Exception as exc:  # StreamTooLarge or a store error: raised by the next add()/end()
            self._failed = exc

    async def _write(self, kind: str, data: Any) -> None:
        payload = orjson.dumps(data, default=str)
        self._bytes += len(payload)
        if self._bytes > self._max_bytes:
            raise StreamTooLarge(f"the stream is above {self._max_bytes} bytes")
        seq = self._seq + 1  # counted only once written: a failed write leaves no gap
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.xadd(self.key, {"seq": seq, "att": self.attempt_id, "t": kind, "d": payload})
            pipe.expire(self.key, self._ttl)  # every write: the first one may have failed
            await pipe.execute()
        self._seq = seq
        self._last = time.monotonic()

    async def _flush(self) -> None:
        async with self._lock:
            if self._buffer:
                events, self._buffer = self._buffer, []
                await self._write("chunk", events)

    async def end(self, usage: Mapping[str, Any]) -> None:
        """Write what is left, then the `end` entry with the usage."""
        await self._stop_timer()
        await self._flush()
        async with self._lock:
            await self._write("end", {"usage": dict(usage)})

    async def error(self, kind: str, message: str) -> None:
        """End the stream with an error that carries the partial text (best effort)."""
        await self._stop_timer()
        with contextlib.suppress(StreamTooLarge):
            await self._flush()
        async with self._lock:
            self._bytes = 0  # the error entry is always written
            await self._write(
                "error", {"kind": kind, "message": message, "partial": self.partial_text}
            )

    async def _stop_timer(self) -> None:
        if self._timer is not None and not self._timer.done():
            self._timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer


class ChunkReader:
    """One per caller process: one blocking read over every open chunk stream."""

    def __init__(self, client: redis.asyncio.Redis, *, block_ms: int = 100) -> None:
        """Keep the client; the read task starts with the first open stream.

        `block_ms`: an opened stream joins the shared read within this time
        (`Tuning.engine.stream_read_block_ms`).
        """
        self._client = client
        self._block_ms = block_ms
        self._open: dict[str, tuple[asyncio.Queue[dict[str, Any]], list[str]]] = {}
        self._task: asyncio.Task[None] | None = None

    def open(self, key: str) -> asyncio.Queue[dict[str, Any]]:
        """Start reading a chunk stream from its beginning; entries arrive on the queue."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._open[key] = (queue, ["0"])
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._read())
        return queue

    def close(self, key: str) -> None:
        """Stop reading a chunk stream."""
        self._open.pop(key, None)

    async def aclose(self) -> None:
        """Stop the read task."""
        self._open.clear()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _read(self) -> None:
        while self._open:
            streams: dict[Any, Any] = {key: last[0] for key, (_, last) in self._open.items()}
            try:
                found: Any = await self._client.xread(streams, block=self._block_ms)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("chunk_reader_error", error=str(exc))
                await asyncio.sleep(0.5)
                continue
            for raw_key, entries in found or []:
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if key not in self._open:
                    continue
                queue, last = self._open[key]
                for entry_id, fields in entries:
                    last[0] = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                    entry = {
                        (k.decode() if isinstance(k, bytes) else k): v for k, v in fields.items()
                    }
                    queue.put_nowait(
                        {
                            "seq": int(entry["seq"]),
                            "att": _text(entry.get("att", b"")),
                            "t": _text(entry["t"]),
                            "d": orjson.loads(entry["d"]),
                        }
                    )


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
