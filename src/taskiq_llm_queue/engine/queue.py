"""A queue on any Redis-protocol store: a stream plus visibility, generation and failure records.

Order is the stream id (the store's clock at send). A received message is held (invisible) for
the visibility time; `return_` makes it visible again in its place without a failure;
a lapsed hold (the worker died) counts one failure, and `max_receives` failures move it to the
dead-letter stream, reported to the receiver so the caller can be told (plan §3.18). Every
operation is one Lua call. A model has two queues: `<name>` and `<name>-priority` (E92).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.engine.keys import Keys

if TYPE_CHECKING:
    import redis.asyncio

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# Keys of one queue, in this order for every script.
_PARTS = ("s", "vis", "gen", "fails", "st", "dlq", "meta")

# ARGV: body, max waiting (0 = no limit), retention ms. Reply: {'ok', id} or {'full', depth}.
SEND = (
    _NOW
    + """
local limit = tonumber(ARGV[2])
local depth = redis.call('XLEN', KEYS[1])
if limit > 0 and depth >= limit then return {'full', depth} end
local id = redis.call('XADD', KEYS[1], 'MINID', '~', tostring(now - tonumber(ARGV[3])), '*',
  'body', ARGV[1])
return {'ok', id}
"""
)

_DLQ = """
local function to_dlq(id, body, fails, keep_ms)
  redis.call('XADD', KEYS[6], 'MINID', '~', tostring(now - keep_ms), '*',
    'body', body, 'src', id, 'fails', fails)
  redis.call('XDEL', KEYS[1], id)
  redis.call('ZREM', KEYS[2], id)
  redis.call('HDEL', KEYS[3], id)
  redis.call('HDEL', KEYS[4], id)
  redis.call('HDEL', KEYS[5], id)
end
local function older(a, b)
  local am, as = string.match(a, '(%d+)-(%d+)')
  local bm, bs = string.match(b, '(%d+)-(%d+)')
  if tonumber(am) ~= tonumber(bm) then return tonumber(am) < tonumber(bm) end
  return tonumber(as) < tonumber(bs)
end
"""

# ARGV: max messages, visibility ms, max receives, dead-letter retention ms.
# Reply: {taken: {id, generation, body, ...}, dead: {id, body, ...}}. Messages come oldest first,
# whether new or visible again; a lapsed hold counts one failure.
RECEIVE = (
    _NOW
    + _DLQ
    + """
local want = tonumber(ARGV[1])
local maxr = tonumber(ARGV[3])
local keep = tonumber(ARGV[4])
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 1000)
table.sort(due, older)
local cursor = redis.call('HGET', KEYS[7], 'cursor')
local start = cursor and ('(' .. cursor) or '-'
local fresh = redis.call('XRANGE', KEYS[1], start, '+', 'COUNT', want)
local taken, gone = {}, {}
local i, j = 1, 1
local last_new = nil
while #taken < want * 3 and (i <= #due or j <= #fresh) do
  local pick_due
  if i > #due then pick_due = false
  elseif j > #fresh then pick_due = true
  else pick_due = older(due[i], fresh[j][1]) end
  local id, body
  if pick_due then
    id = due[i]
    i = i + 1
    local entry = redis.call('XRANGE', KEYS[1], id, id)
    if #entry == 0 then
      redis.call('ZREM', KEYS[2], id)
      redis.call('HDEL', KEYS[3], id)
      redis.call('HDEL', KEYS[4], id)
      redis.call('HDEL', KEYS[5], id)
    else
      body = entry[1][2][2]
      if redis.call('HGET', KEYS[5], id) == 'h' then
        local fails = redis.call('HINCRBY', KEYS[4], id, 1)
        if maxr > 0 and fails >= maxr then
          to_dlq(id, body, fails, keep)
          table.insert(gone, id)
          table.insert(gone, body)
          body = nil
        end
      end
    end
  else
    id = fresh[j][1]
    body = fresh[j][2][2]
    last_new = id
    j = j + 1
  end
  if body then
    local gen = redis.call('HINCRBY', KEYS[3], id, 1)
    redis.call('HSET', KEYS[5], id, 'h')
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), id)
    table.insert(taken, id)
    table.insert(taken, tostring(gen))
    table.insert(taken, body)
    if #taken >= want * 3 then break end
  end
end
if last_new then redis.call('HSET', KEYS[7], 'cursor', last_new) end
return {taken, gone}
"""
)

# ARGV: id, generation. Reply: 1 deleted, 0 the receipt is stale (another holder has it).
ACK = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('XDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
return 1
"""

# ARGV: id, generation, delay ms. Visible again in its place after the delay; no failure.
RETURN = (
    _NOW
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[5], ARGV[1], 'r')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 1
"""
)

# ARGV: id, generation, visibility ms. Extend a hold (the worker's heartbeat).
EXTEND = (
    _NOW
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[2], 'XX', now + tonumber(ARGV[3]), ARGV[1])
return 1
"""
)

# ARGV: id, generation, delay ms, max receives, dead-letter retention ms. The handler failed:
# count one failure; at `max_receives` the message goes to the dead-letter stream.
# Reply: 'retry', 'dead' or 'stale'.
FAIL = (
    _NOW
    + _DLQ
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 'stale' end
local fails = redis.call('HINCRBY', KEYS[4], ARGV[1], 1)
local maxr = tonumber(ARGV[4])
if maxr > 0 and fails >= maxr then
  local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1])
  if #entry > 0 then to_dlq(ARGV[1], entry[1][2][2], fails, tonumber(ARGV[5])) end
  return 'dead'
end
redis.call('HSET', KEYS[5], ARGV[1], 'r')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 'retry'
"""
)

# Reply: {messages in the stream, held, dead letters}.
DEPTH = """
local held = 0
for _, v in ipairs(redis.call('HVALS', KEYS[5])) do if v == 'h' then held = held + 1 end end
return {redis.call('XLEN', KEYS[1]), held, redis.call('XLEN', KEYS[6])}
"""

# ARGV: max count. Dead letters back on the queue (new ids: they go to the back).
REDRIVE = """
local moved = 0
for _, entry in ipairs(redis.call('XRANGE', KEYS[6], '-', '+', 'COUNT', tonumber(ARGV[1]))) do
  local fields = entry[2]
  local body
  for k = 1, #fields, 2 do if fields[k] == 'body' then body = fields[k + 1] end end
  redis.call('XADD', KEYS[1], '*', 'body', body)
  redis.call('XDEL', KEYS[6], entry[1])
  moved = moved + 1
end
return moved
"""


@dataclass(frozen=True, slots=True)
class Message:
    """A received message: its stream id (its place), generation and body."""

    queue: str
    id: str
    generation: int
    body: bytes

    @property
    def position_ms(self) -> int:
        """The store's time at send, in ms (deadlines and ages are measured from it)."""
        return int(self.id.split("-", 1)[0])


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A message moved to the dead-letter stream by this receive."""

    queue: str
    id: str
    body: bytes


@dataclass(frozen=True, slots=True)
class QueueSettings:
    """Per-queue numbers (from the model's `queue` setting and `max_waiting`)."""

    visibility_secs: float = 60.0
    max_receives: int = 10
    retention_secs: int = 345_600
    dlq_retention_secs: int = 86_400
    max_waiting: int = 50_000


class QueueFull(Exception):  # the plan's error kind is `queue_full`
    """`max_waiting` calls already wait on this queue."""

    def __init__(self, queue: str, depth: int) -> None:
        """Keep the queue and its depth."""
        super().__init__(f"queue {queue} is full ({depth} waiting)")
        self.queue = queue
        self.depth = depth


class StreamQueues:
    """Send, receive, return, extend, fail and acknowledge on named queues of one prefix."""

    def __init__(self, client: redis.asyncio.Redis, keys: Keys) -> None:
        """Register the scripts; no I/O until the first call."""
        self._client = client
        self.keys = keys
        self._send = client.register_script(SEND)
        self._receive = client.register_script(RECEIVE)
        self._ack = client.register_script(ACK)
        self._return = client.register_script(RETURN)
        self._extend = client.register_script(EXTEND)
        self._fail = client.register_script(FAIL)
        self._depth = client.register_script(DEPTH)
        self._redrive = client.register_script(REDRIVE)

    def _k(self, queue: str) -> list[str]:
        return [self.keys.queue(queue, part) for part in _PARTS]

    async def send(self, queue: str, body: bytes, settings: QueueSettings) -> str:
        """Append a message; returns its id. `QueueFull` above `max_waiting`."""
        reply = await self._send(
            self._k(queue), [body, settings.max_waiting, settings.retention_secs * 1000]
        )
        if _text(reply[0]) == "full":
            raise QueueFull(queue, int(reply[1]))
        return _text(reply[1])

    async def receive(
        self, queue: str, count: int, settings: QueueSettings
    ) -> tuple[list[Message], list[DeadLetter]]:
        """Up to `count` messages, oldest first, held for the visibility time; and any dead."""
        taken, dead = await self._receive(
            self._k(queue),
            [
                count,
                int(settings.visibility_secs * 1000),
                settings.max_receives,
                settings.dlq_retention_secs * 1000,
            ],
        )
        messages = [
            Message(queue, _text(taken[i]), int(taken[i + 1]), _bytes(taken[i + 2]))
            for i in range(0, len(taken), 3)
        ]
        letters = [
            DeadLetter(queue, _text(dead[i]), _bytes(dead[i + 1])) for i in range(0, len(dead), 2)
        ]
        return messages, letters

    async def receive_wait(
        self, queue: str, count: int, settings: QueueSettings, wait_secs: float
    ) -> tuple[list[Message], list[DeadLetter]]:
        """`receive`, polling with growing pauses (20 ms to 500 ms) for up to `wait_secs`."""
        loop = asyncio.get_running_loop()
        until = loop.time() + wait_secs
        pause = 0.02
        while True:
            messages, dead = await self.receive(queue, count, settings)
            if messages or dead or loop.time() >= until:
                return messages, dead
            await asyncio.sleep(min(pause, max(0.0, until - loop.time())))
            pause = min(pause * 2, 0.5)

    async def ack(self, message: Message) -> bool:
        """Delete a handled message; False when the receipt is stale."""
        return bool(await self._ack(self._k(message.queue), [message.id, message.generation]))

    async def return_(self, message: Message, delay_secs: float = 0.0) -> bool:
        """Make a message visible again in its place (after `delay_secs`), with no failure."""
        return bool(
            await self._return(
                self._k(message.queue), [message.id, message.generation, int(delay_secs * 1000)]
            )
        )

    async def extend(self, message: Message, visibility_secs: float) -> bool:
        """Extend the hold of a message (heartbeat); False when the receipt is stale."""
        return bool(
            await self._extend(
                self._k(message.queue),
                [message.id, message.generation, int(visibility_secs * 1000)],
            )
        )

    async def fail(self, message: Message, settings: QueueSettings, delay_secs: float = 0.0) -> str:
        """Count a failure: `retry` (visible again after the delay), `dead` or `stale`."""
        return _text(
            await self._fail(
                self._k(message.queue),
                [
                    message.id,
                    message.generation,
                    int(delay_secs * 1000),
                    settings.max_receives,
                    settings.dlq_retention_secs * 1000,
                ],
            )
        )

    async def depth(self, queue: str) -> tuple[int, int, int]:
        """(messages in the queue, held by workers, dead letters)."""
        reply = await self._depth(self._k(queue), [])
        return int(reply[0]), int(reply[1]), int(reply[2])

    async def dead_letters(self, queue: str, count: int = 10) -> list[dict[str, Any]]:
        """Dead letters, oldest first: id, source id, failures, body."""
        entries = await self._client.xrange(self.keys.queue(queue, "dlq"), count=count)
        out = []
        for entry_id, fields in entries or ():
            row = {_text(k): v for k, v in (fields or {}).items()}
            out.append(
                {
                    "id": _text(entry_id),
                    "src": _text(row.get("src", b"")),
                    "fails": int(row.get("fails", 0)),
                    "body": _bytes(row.get("body", b"")),
                }
            )
        return out

    async def redrive(self, queue: str, count: int = 1000) -> int:
        """Move up to `count` dead letters back to the queue; returns how many."""
        return int(await self._redrive(self._k(queue), [count]))

    async def purge_dead(self, queue: str) -> None:
        """Delete every dead letter of a queue."""
        await self._client.delete(self.keys.queue(queue, "dlq"))

    async def delete(self, names: Sequence[str]) -> None:
        """Delete queues and everything in them (model removal)."""
        await self._client.delete(*[k for name in names for k in self._k(name)])


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _bytes(value: Any) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode()
