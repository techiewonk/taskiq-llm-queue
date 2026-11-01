"""`ValkeyQueueClient`: the SQS calls `HybridSQSBroker` makes, served by Valkey Streams.

Same request / response shapes and error codes as aiobotocore's SQS client, so the broker, the
Acker and `classify_error` run unchanged (docs/VALKEY_QUEUE_PLAN.md). Each queue is a stream plus
an in-flight zset (visibility), a receive-count hash (receipt-handle generation) and a delay zset,
all under one hash tag; every operation is one Lua script, so it is atomic and cluster-safe.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from botocore.exceptions import ClientError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.broker.queue import MAX_DELAY_SECS, MAX_MESSAGE_BYTES, MAX_VISIBILITY_SECS

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub

logger = get_logger(__name__)

URL_SCHEME = "valkey://"
GROUP = "llmq"
CONSUMER = "c"
DEFAULT_VISIBILITY_SECS = 30
DEFAULT_RETENTION_SECS = 345_600
MAX_BATCH = 10
# Long polls re-check at least this often: in-flight messages whose visibility lapsed and due
# delayed messages have no wake-up event of their own.
RECHECK_SECS = 1.0

_RECEIVE = """
local s, vis, rc, delay, dlq, cfg = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
if redis.call('EXISTS', cfg) == 0 then return redis.error_reply('QueueDoesNotExist') end
local now, n, visms = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local maxr = tonumber(redis.call('HGET', cfg, 'max_receives') or '0')
local has_dlq = redis.call('HGET', cfg, 'dlq') or ''
local due = redis.call('ZRANGEBYSCORE', delay, '-inf', now, 'LIMIT', 0, 100)
for _, p in ipairs(due) do
  local m = cjson.decode(p)
  redis.call('XADD', s, '*', 'mid', m.mid, 'body', m.body, 'attrs', m.attrs, 'sent', m.sent)
  redis.call('ZREM', delay, p)
end
local out = {}
local ids = redis.call('ZRANGEBYSCORE', vis, '-inf', now, 'LIMIT', 0, n)
for _, id in ipairs(ids) do
  local e = redis.call('XRANGE', s, id, id)
  if #e == 0 then
    redis.call('ZREM', vis, id)
    redis.call('HDEL', rc, id)
  else
    local count = redis.call('HINCRBY', rc, id, 1)
    if maxr > 0 and count > maxr then
      if has_dlq ~= '' then
        local f = e[1][2]
        local args = {'XADD', dlq, '*'}
        for i = 1, #f do table.insert(args, f[i]) end
        table.insert(args, 'src') table.insert(args, id)
        redis.call(unpack(args))
      end
      redis.call('XACK', s, ARGV[4], id)
      redis.call('XDEL', s, id)
      redis.call('ZREM', vis, id)
      redis.call('HDEL', rc, id)
    else
      redis.call('ZADD', vis, now + visms, id)
      table.insert(out, {id, count, e[1][2]})
    end
  end
end
local k = n - #out
if k > 0 then
  local r = redis.call('XREADGROUP', 'GROUP', ARGV[4], ARGV[5], 'COUNT', k, 'STREAMS', s, '>')
  if r then
    for _, e in ipairs(r[1][2]) do
      redis.call('HSET', rc, e[1], 1)
      redis.call('ZADD', vis, now + visms, e[1])
      table.insert(out, {e[1], 1, e[2]})
    end
  end
end
return out
"""

_SEND = """
local s, delay, cfg = KEYS[1], KEYS[2], KEYS[3]
if redis.call('EXISTS', cfg) == 0 then return redis.error_reply('QueueDoesNotExist') end
local now, delay_ms = tonumber(ARGV[4]), tonumber(ARGV[5])
if delay_ms > 0 then
  local p = cjson.encode({mid = ARGV[1], body = ARGV[2], attrs = ARGV[3], sent = ARGV[4]})
  redis.call('ZADD', delay, now + delay_ms, p)
else
  local keep = tonumber(redis.call('HGET', cfg, 'retention_ms') or '0')
  if keep > 0 then
    redis.call('XADD', s, 'MINID', '~', tostring(now - keep), '*',
      'mid', ARGV[1], 'body', ARGV[2], 'attrs', ARGV[3], 'sent', ARGV[4])
  else
    redis.call('XADD', s, '*', 'mid', ARGV[1], 'body', ARGV[2], 'attrs', ARGV[3], 'sent', ARGV[4])
  end
  redis.call('PUBLISH', ARGV[6], '1')
end
return ARGV[1]
"""

_DELETE = """
local s, vis, rc = KEYS[1], KEYS[2], KEYS[3]
local cur = redis.call('HGET', rc, ARGV[2])
if not cur then return 1 end
if cur ~= ARGV[3] then return redis.error_reply('ReceiptHandleIsInvalid') end
redis.call('XACK', s, ARGV[1], ARGV[2])
redis.call('XDEL', s, ARGV[2])
redis.call('ZREM', vis, ARGV[2])
redis.call('HDEL', rc, ARGV[2])
return 1
"""

_VISIBILITY = """
local vis, rc = KEYS[1], KEYS[2]
local cur = redis.call('HGET', rc, ARGV[1])
if not cur or cur ~= ARGV[2] then return redis.error_reply('ReceiptHandleIsInvalid') end
local until_ms = redis.call('ZSCORE', vis, ARGV[1])
if not until_ms or tonumber(until_ms) <= tonumber(ARGV[3]) then
  return redis.error_reply('MessageNotInflight')
end
redis.call('ZADD', vis, tonumber(ARGV[3]) + tonumber(ARGV[4]), ARGV[1])
return 1
"""

_STATS = """
local s, vis, delay, dlq = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local total = redis.call('XLEN', s)
local inflight = redis.call('ZCOUNT', vis, '(' .. ARGV[1], '+inf')
return {total - inflight, inflight, redis.call('ZCARD', delay), redis.call('XLEN', dlq)}
"""

_REDRIVE = """
local s, dlq = KEYS[1], KEYS[2]
local moved = 0
for _, e in ipairs(redis.call('XRANGE', dlq, '-', '+', 'COUNT', tonumber(ARGV[1]))) do
  local f, args = e[2], {'XADD', s, '*'}
  for i = 1, #f, 2 do
    if f[i] ~= 'src' then table.insert(args, f[i]) table.insert(args, f[i + 1]) end
  end
  redis.call(unpack(args))
  redis.call('XDEL', dlq, e[1])
  moved = moved + 1
end
return moved
"""


def _error(code: str, message: str, operation: str, status: int = 400) -> ClientError:
    """A botocore `ClientError` shaped like the SQS one, so `classify_error` maps it the same."""
    response = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(cast("Any", response), operation)


def queue_url(name: str) -> str:
    """Queue URL for a Valkey queue name (`valkey://<name>`)."""
    return f"{URL_SCHEME}{name}"


def _name(url: str) -> str:
    return url[len(URL_SCHEME) :] if url.startswith(URL_SCHEME) else url.rsplit("/", 1)[-1]


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class ValkeyQueueClient:
    """Async SQS-client subset over Valkey Streams (see the module docstring).

    `redis` is an async client without `decode_responses`; it is closed by `close()` only when
    `owns_redis`. The pub/sub connection for long-poll wake-ups is opened lazily.
    """

    def __init__(self, redis: Redis, *, prefix: str, owns_redis: bool = False) -> None:
        """Bind to `redis`; keys are `{<prefix>:q:<queue>}:*`. No I/O until the first call."""
        self._redis = redis
        self._prefix = prefix
        self._owns = owns_redis
        self._scripts = {
            name: redis.register_script(src)
            for name, src in (
                ("receive", _RECEIVE),
                ("send", _SEND),
                ("delete", _DELETE),
                ("visibility", _VISIBILITY),
                ("stats", _STATS),
                ("redrive", _REDRIVE),
            )
        }
        self._pubsub: PubSub | None = None
        self._listener: asyncio.Task[None] | None = None
        self._wake: dict[str, asyncio.Event] = {}
        self._subscribe_lock = asyncio.Lock()
        # Last-known attributes per queue, to re-create a queue whose keys vanished (a Valkey
        # restart without persistence, a flush): the service recovers; its messages are lost.
        self._known: dict[str, dict[Any, Any]] = {}

    # ------------------------------------------------------------------ keys

    def _tag(self, name: str) -> str:
        return f"{{{self._prefix}:q:{name}}}"

    def _keys(self, name: str) -> dict[str, str]:
        tag = self._tag(name)
        return {k: f"{tag}:{k}" for k in ("s", "vis", "rc", "delay", "dlq", "cfg", "wake")}

    def _registry(self) -> str:
        return f"{self._prefix}:queues"

    # ------------------------------------------------------------------ admin

    async def create_queue(
        self, QueueName: str, Attributes: Mapping[str, str] | None = None, **_: Any
    ) -> dict[str, Any]:
        """Create (idempotent) a queue: stream, consumer group and attributes.

        Understands `VisibilityTimeout`, `MessageRetentionPeriod` and `RedrivePolicy`
        (`maxReceiveCount`; any `deadLetterTargetArn` turns the queue's DLQ stream on). A
        queue named `<x>-dlq` that is some queue's target reports that queue's DLQ stream.
        """
        attrs = dict(Attributes or {})
        if QueueName.endswith(".fifo") or attrs.get("FifoQueue") == "true":
            raise _error("InvalidParameterValue", "FIFO queues are not supported", "CreateQueue")
        keys = self._keys(QueueName)
        fields: dict[Any, Any] = {
            "visibility_secs": attrs.get("VisibilityTimeout", str(DEFAULT_VISIBILITY_SECS)),
            "retention_ms": str(
                int(attrs.get("MessageRetentionPeriod", str(DEFAULT_RETENTION_SECS))) * 1000
            ),
        }
        if policy := attrs.get("RedrivePolicy"):
            redrive = json.loads(policy)
            fields["max_receives"] = str(int(redrive.get("maxReceiveCount", 0)))
            target = str(redrive.get("deadLetterTargetArn", "")).rsplit(":", 1)[-1]
            fields["dlq"] = target or "1"
            if target:
                dlq_cfg: dict[Any, Any] = {"dlq_of": QueueName}
                await self._redis.hset(self._keys(target)["cfg"], mapping=dlq_cfg)
        existing = await self._redis.hgetall(keys["cfg"])
        if not existing:
            fields["created_ms"] = str(int(time.time() * 1000))
        await self._redis.hset(keys["cfg"], mapping=fields)
        self._known[QueueName] = await self._redis.hgetall(keys["cfg"])
        with contextlib.suppress(Exception):  # BUSYGROUP: already there
            await self._redis.xgroup_create(keys["s"], GROUP, id="0", mkstream=True)
        await self._redis.sadd(self._registry(), QueueName)
        return {"QueueUrl": queue_url(QueueName)}

    async def set_queue_attributes(
        self, QueueUrl: str, Attributes: Mapping[str, str], **_: Any
    ) -> dict[str, Any]:
        """Update attributes of an existing queue (same keys as `create_queue`)."""
        name = _name(QueueUrl)
        if not await self._redis.exists(self._keys(name)["cfg"]):
            raise _error("QueueDoesNotExist", f"queue {name} does not exist", "SetQueueAttributes")
        await self.create_queue(QueueName=name, Attributes=Attributes)
        return {}

    async def get_queue_url(self, QueueName: str, **_: Any) -> dict[str, Any]:
        """`QueueDoesNotExist` unless `create_queue` ran for this name."""
        cfg = await self._redis.hgetall(self._keys(QueueName)["cfg"])
        if not cfg:
            if await self._heal(QueueName):
                return {"QueueUrl": queue_url(QueueName)}
            raise _error("QueueDoesNotExist", f"queue {QueueName} does not exist", "GetQueueUrl")
        self._known[QueueName] = cfg
        return {"QueueUrl": queue_url(QueueName)}

    async def _heal(self, name: str) -> bool:
        """Re-create a queue this client saw before (attributes, stream, group); else False."""
        cfg = self._known.get(name)
        if not cfg:
            return False
        k = self._keys(name)
        await self._redis.hset(k["cfg"], mapping=cfg)
        with contextlib.suppress(Exception):  # BUSYGROUP: the stream and group survived
            await self._redis.xgroup_create(k["s"], GROUP, id="0", mkstream=True)
        await self._redis.sadd(self._registry(), name)
        logger.warning("valkey_queue_recreated", queue=name)
        return True

    async def list_queues(self, QueueNamePrefix: str = "", **_: Any) -> dict[str, Any]:
        """Queue URLs created through this prefix, optionally filtered by name prefix."""
        names = sorted(_text(n) for n in await self._redis.smembers(self._registry()))
        return {"QueueUrls": [queue_url(n) for n in names if n.startswith(QueueNamePrefix)]}

    async def get_queue_attributes(
        self, QueueUrl: str, AttributeNames: Sequence[str] = ("All",), **_: Any
    ) -> dict[str, Any]:
        """Visibility, retention, redrive policy and the approximate message counts."""
        name = _name(QueueUrl)
        cfg = {
            _text(k): _text(v)
            for k, v in (await self._redis.hgetall(self._keys(name)["cfg"])).items()
        }
        if not cfg:
            raise _error("QueueDoesNotExist", f"queue {name} does not exist", "GetQueueAttributes")
        source = cfg.get("dlq_of")
        if source:  # a DLQ name: its messages live in the source queue's DLQ stream
            dead = await self._redis.xlen(self._keys(source)["dlq"])
            visible, inflight, delayed = int(dead), 0, 0
        else:
            visible, inflight, delayed, _dead = await self._stats(name)
        attrs = {
            "QueueArn": f"arn:valkey:{name}",
            "VisibilityTimeout": cfg.get("visibility_secs", str(DEFAULT_VISIBILITY_SECS)),
            "MessageRetentionPeriod": str(int(cfg.get("retention_ms", "0")) // 1000),
            "ApproximateNumberOfMessages": str(visible),
            "ApproximateNumberOfMessagesNotVisible": str(inflight),
            "ApproximateNumberOfMessagesDelayed": str(delayed),
        }
        if cfg.get("max_receives"):
            attrs["RedrivePolicy"] = json.dumps(
                {
                    "maxReceiveCount": int(cfg["max_receives"]),
                    "deadLetterTargetArn": cfg.get("dlq", ""),
                }
            )
        wanted = set(AttributeNames)
        return {
            "Attributes": attrs
            if "All" in wanted
            else {k: v for k, v in attrs.items() if k in wanted}
        }

    async def depth(self, name: str) -> dict[str, float]:
        """Counts by state plus the age of the oldest message on the stream (0 when empty).

        The metrics the SQS backend gets from CloudWatch (`ApproximateAgeOfOldestMessage`).
        """
        visible, inflight, delayed, dead = await self._stats(name)
        oldest = await self._redis.xrange(self._keys(name)["s"], count=1)
        age = 0.0
        if oldest:
            entry_ms = int(_text(oldest[0][0]).split("-", 1)[0])
            age = max(0.0, time.time() - entry_ms / 1000)
        return {
            "visible": float(visible),
            "in_flight": float(inflight),
            "delayed": float(delayed),
            "dead": float(dead),
            "oldest_secs": age,
        }

    async def _stats(self, name: str) -> tuple[int, int, int, int]:
        k = self._keys(name)
        now_ms = int(time.time() * 1000)
        raw = await self._scripts["stats"](
            keys=[k["s"], k["vis"], k["delay"], k["dlq"]], args=[now_ms]
        )
        visible, inflight, delayed, dead = (int(x) for x in raw)
        return visible, inflight, delayed, dead

    async def purge_queue(self, QueueUrl: str, **_: Any) -> dict[str, Any]:
        """Drop every message (stream, in-flight, delayed, DLQ); the queue itself stays."""
        name = _name(QueueUrl)
        k = self._keys(name)
        cfg = await self._redis.hgetall(k["cfg"])
        source = cfg.get(b"dlq_of")
        if source:
            await self._redis.delete(self._keys(_text(source))["dlq"])
            return {}
        await self._redis.delete(k["s"], k["vis"], k["rc"], k["delay"], k["dlq"])
        await self._redis.xgroup_create(k["s"], GROUP, id="0", mkstream=True)
        return {}

    async def delete_queue(self, QueueUrl: str, **_: Any) -> dict[str, Any]:
        """Remove the queue and all its keys."""
        name = _name(QueueUrl)
        k = self._keys(name)
        await self._redis.delete(k["s"], k["vis"], k["rc"], k["delay"], k["dlq"], k["cfg"])
        await self._redis.srem(self._registry(), name)
        return {}

    async def redrive(self, name: str, limit: int = 10_000) -> int:
        """Move up to `limit` dead-lettered messages of `name` back onto `name` (new receives)."""
        k = self._keys(name)
        moved = await self._scripts["redrive"](keys=[k["s"], k["dlq"]], args=[limit])
        if moved:
            await self._redis.publish(k["wake"], "1")
        return int(moved)

    async def dead_letters(self, name: str, count: int = 10) -> list[dict[str, Any]]:
        """Peek at up to `count` dead-lettered messages of `name` (body, attributes, source id)."""
        rows = cast(
            "list[tuple[bytes, dict[bytes, bytes]]]",
            await self._redis.xrange(self._keys(name)["dlq"], count=count),
        )
        return [
            {_text(k): _text(v) for k, v in fields.items()} | {"id": _text(i)} for i, fields in rows
        ]

    # ------------------------------------------------------------------ messages

    async def send_message(
        self,
        QueueUrl: str,
        MessageBody: str,
        DelaySeconds: int = 0,
        MessageAttributes: Mapping[str, Any] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """Append one message (or schedule it after `DelaySeconds`)."""
        return await self._send(
            QueueUrl, MessageBody, DelaySeconds, MessageAttributes, extra, "SendMessage"
        )

    async def _send(
        self,
        url: str,
        body: str,
        delay_secs: int,
        attributes: Mapping[str, Any] | None,
        extra: Mapping[str, Any],
        operation: str,
    ) -> dict[str, Any]:
        if extra.get("MessageGroupId") or extra.get("MessageDeduplicationId"):
            raise _error("InvalidParameterValue", "FIFO parameters are not supported", operation)
        if len(body.encode()) > MAX_MESSAGE_BYTES:
            raise _error("MessageTooLong", "message must be shorter than the size limit", operation)
        if not 0 <= int(delay_secs) <= MAX_DELAY_SECS:
            raise _error("InvalidParameterValue", "DelaySeconds out of range", operation)
        name = _name(url)
        k = self._keys(name)
        mid = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        args: list[str | int] = [
            mid,
            body,
            json.dumps(dict(attributes or {})),
            now_ms,
            int(delay_secs) * 1000,
            k["wake"],
        ]
        for attempt in (0, 1):
            try:
                await self._scripts["send"](keys=[k["s"], k["delay"], k["cfg"]], args=args)
            except Exception as exc:
                error = _translate(exc, operation)
                if attempt == 0 and _missing(error) and await self._heal(name):
                    continue
                raise error from exc
            break
        return {"MessageId": mid}

    async def send_message_batch(
        self, QueueUrl: str, Entries: Sequence[Mapping[str, Any]], **_: Any
    ) -> dict[str, Any]:
        """Up to 10 entries; each succeeds or fails on its own, like SQS."""
        if len(Entries) > MAX_BATCH:
            raise _error("TooManyEntriesInBatchRequest", "at most 10 entries", "SendMessageBatch")
        ok: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for entry in Entries:
            extra = {
                k: v for k, v in entry.items() if k in ("MessageGroupId", "MessageDeduplicationId")
            }
            try:
                sent = await self._send(
                    QueueUrl,
                    entry["MessageBody"],
                    int(entry.get("DelaySeconds", 0)),
                    entry.get("MessageAttributes"),
                    extra,
                    "SendMessageBatch",
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "QueueDoesNotExist":
                    raise
                failed.append(_failed(entry["Id"], exc, sender=True))
                continue
            ok.append({"Id": entry["Id"], "MessageId": sent["MessageId"]})
        return {"Successful": ok, "Failed": failed}

    async def receive_message(
        self,
        QueueUrl: str,
        MaxNumberOfMessages: int = 1,
        WaitTimeSeconds: int = 0,
        VisibilityTimeout: int | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Up to `MaxNumberOfMessages`, long-polling up to `WaitTimeSeconds` (≤ 20)."""
        name = _name(QueueUrl)
        k = self._keys(name)
        n = max(1, min(int(MaxNumberOfMessages), MAX_BATCH))
        if VisibilityTimeout is None:
            raw = await self._redis.hget(k["cfg"], "visibility_secs")
            visibility = int(raw) if raw is not None else DEFAULT_VISIBILITY_SECS
        else:
            visibility = int(VisibilityTimeout)
        deadline = time.monotonic() + max(0, min(int(WaitTimeSeconds), 20))
        event = await self._wake_event(k["wake"]) if WaitTimeSeconds else None
        while True:
            if event is not None:
                event.clear()
            try:
                rows = await self._scripts["receive"](
                    keys=[k["s"], k["vis"], k["rc"], k["delay"], k["dlq"], k["cfg"]],
                    args=[int(time.time() * 1000), n, visibility * 1000, GROUP, CONSUMER],
                )
            except Exception as exc:
                error = _translate(exc, "ReceiveMessage")
                if _missing(error) and await self._heal(name):
                    continue
                raise error from exc
            if rows:
                return {"Messages": [_message(row) for row in rows]}
            remaining = deadline - time.monotonic()
            if remaining <= 0 or event is None:
                return {}
            with contextlib.suppress(TimeoutError):
                # Random re-check phase: no poller is in step with the due times it set (S23).
                pause = random.uniform(RECHECK_SECS / 2, RECHECK_SECS)  # idle: <= 2 checks/s
                await asyncio.wait_for(event.wait(), min(remaining, pause))
            if not event.is_set() and time.monotonic() >= deadline:
                return {}  # the caller's next poll checks at once: no back-to-back re-check

    async def delete_message(self, QueueUrl: str, ReceiptHandle: str, **_: Any) -> dict[str, Any]:
        """Remove a received message; a handle from an older receive is `ReceiptHandleIsInvalid`."""
        await self._delete(_name(QueueUrl), ReceiptHandle, "DeleteMessage")
        return {}

    async def _delete(self, name: str, handle: str, operation: str) -> None:
        entry, gen = _parse_handle(handle, operation)
        k = self._keys(name)
        try:
            await self._scripts["delete"](
                keys=[k["s"], k["vis"], k["rc"]], args=[GROUP, entry, gen]
            )
        except Exception as exc:
            raise _translate(exc, operation) from exc

    async def delete_message_batch(
        self, QueueUrl: str, Entries: Sequence[Mapping[str, Any]], **_: Any
    ) -> dict[str, Any]:
        """Batch delete; each entry succeeds or fails on its own."""
        ok: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for entry in Entries:
            try:
                await self._delete(_name(QueueUrl), entry["ReceiptHandle"], "DeleteMessageBatch")
            except ClientError as exc:
                failed.append(_failed(entry["Id"], exc, sender=True))
                continue
            ok.append({"Id": entry["Id"]})
        return {"Successful": ok, "Failed": failed}

    async def change_message_visibility(
        self, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int, **_: Any
    ) -> dict[str, Any]:
        """Extend (heartbeat) or end (0 = visible now) the visibility of an in-flight message."""
        await self._visibility(
            _name(QueueUrl), ReceiptHandle, VisibilityTimeout, "ChangeMessageVisibility"
        )
        return {}

    async def _visibility(self, name: str, handle: str, secs: int, operation: str) -> None:
        if not 0 <= int(secs) <= MAX_VISIBILITY_SECS:
            raise _error("InvalidParameterValue", "VisibilityTimeout out of range", operation)
        entry, gen = _parse_handle(handle, operation)
        k = self._keys(name)
        try:
            await self._scripts["visibility"](
                keys=[k["vis"], k["rc"]],
                args=[entry, gen, int(time.time() * 1000), int(secs) * 1000 + _jitter_ms(secs)],
            )
        except Exception as exc:
            raise _translate(exc, operation) from exc

    async def change_message_visibility_batch(
        self, QueueUrl: str, Entries: Sequence[Mapping[str, Any]], **_: Any
    ) -> dict[str, Any]:
        """Batch visibility change; each entry succeeds or fails on its own."""
        ok: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for entry in Entries:
            try:
                await self._visibility(
                    _name(QueueUrl),
                    entry["ReceiptHandle"],
                    int(entry["VisibilityTimeout"]),
                    "ChangeMessageVisibilityBatch",
                )
            except ClientError as exc:
                failed.append(_failed(entry["Id"], exc, sender=True))
                continue
            ok.append({"Id": entry["Id"]})
        return {"Successful": ok, "Failed": failed}

    # ------------------------------------------------------------------ wake-ups

    async def _wake_event(self, channel: str) -> asyncio.Event:
        event = self._wake.get(channel)
        if event is not None:
            return event
        async with self._subscribe_lock:
            event = self._wake.setdefault(channel, asyncio.Event())
            try:
                if self._pubsub is None:
                    self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
                    await self._pubsub.subscribe(channel)
                    self._listener = asyncio.create_task(self._listen(), name="llmq-valkey-wake")
                else:
                    await self._pubsub.subscribe(channel)
            except Exception as exc:  # polls still work, only with the re-check delay
                logger.warning("valkey_queue_subscribe_failed", error_type=type(exc).__name__)
        return event

    async def _listen(self) -> None:
        assert self._pubsub is not None
        while True:
            try:
                msg = await self._pubsub.get_message(timeout=1.0)
            except asyncio.CancelledError:
                raise
            except Exception:  # connection hiccup: pollers fall back to the 1 s re-check
                await asyncio.sleep(1.0)
                continue
            if msg is not None:
                event = self._wake.get(_text(msg.get("channel", b"")))
                if event is not None:
                    event.set()

    # ------------------------------------------------------------------ lifecycle

    async def close(self) -> None:
        """Stop the wake listener; close the Valkey client when this client owns it."""
        if self._listener is not None:
            self._listener.cancel()
            with contextlib.suppress(BaseException):
                await self._listener
            self._listener = None
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()  # type: ignore[no-untyped-call]  # redis-py stub
            self._pubsub = None
        if self._owns:
            await self._redis.aclose()

    async def __aenter__(self) -> ValkeyQueueClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


def _jitter_ms(secs: int) -> int:
    """Up to 0.5 s added to a non-zero visibility deadline.

    The pod that releases a message re-checks in step with the deadline it set and would
    always take the message back; SQS hands it to whichever long poll waits. Jitter gives
    every poller a fair chance (S23: a pod without the job kept re-taking it into the DLQ).
    """
    return random.randint(0, 500) if secs > 0 else 0


def _parse_handle(handle: str, operation: str) -> tuple[str, str]:
    entry, sep, gen = handle.rpartition(".")
    if not sep or not entry or not gen.isdigit():
        raise _error("ReceiptHandleIsInvalid", f"invalid receipt handle {handle!r}", operation)
    return entry, gen


def _message(row: Sequence[Any]) -> dict[str, Any]:
    entry, count, flat = _text(row[0]), int(row[1]), row[2]
    fields = {_text(flat[i]): _text(flat[i + 1]) for i in range(0, len(flat), 2)}
    message: dict[str, Any] = {
        "MessageId": fields.get("mid", entry),
        "ReceiptHandle": f"{entry}.{count}",
        "Body": fields.get("body", ""),
        "Attributes": {
            "ApproximateReceiveCount": str(count),
            "SentTimestamp": fields.get("sent", ""),
        },
    }
    attrs = json.loads(fields.get("attrs") or "{}")
    if attrs:
        message["MessageAttributes"] = attrs
    return message


def _failed(entry_id: str, exc: ClientError, *, sender: bool) -> dict[str, Any]:
    error = exc.response["Error"]
    return {
        "Id": entry_id,
        "Code": error["Code"],
        "Message": error.get("Message", ""),
        "SenderFault": sender,
    }


def _missing(exc: BaseException) -> bool:
    return isinstance(exc, ClientError) and exc.response["Error"]["Code"] == "QueueDoesNotExist"


def _translate(exc: BaseException, operation: str) -> BaseException:
    """Script errors carry the SQS code as their message; other Valkey errors are transient."""
    if isinstance(exc, ClientError):
        return exc
    text = str(exc).strip()
    if text.startswith(
        "NOGROUP"
    ):  # stream or consumer group gone (data loss): heal like a missing queue
        return _error("QueueDoesNotExist", text, operation)
    for code in ("QueueDoesNotExist", "ReceiptHandleIsInvalid", "MessageNotInflight"):
        if text.endswith(code) or text.startswith(code):
            return _error(code, text, operation)
    return _error("ServiceUnavailable", f"{type(exc).__name__}: {text}", operation, status=503)
