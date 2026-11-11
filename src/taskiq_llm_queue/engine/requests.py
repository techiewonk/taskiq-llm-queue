"""Per-request records: the duplicate-id claim, the result and the request status.

A call's record is written only if absent when it is sent and confirmed only after the send
succeeded, so a failed send never leaves a phantom (plan §3.18 R23). Results are written once
and read without deleting; a final status is never replaced. Scripts use the store's clock.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.engine.envelope import Reply
from taskiq_llm_queue.engine.keys import Keys

if TYPE_CHECKING:
    import redis.asyncio

FINAL_STATES = ("done", "failed", "expired", "cancelled")

# KEYS: job, callers, status, result. ARGV: user hash, params hash, caller, unconfirmed ttl,
# status ttl, model, priority, takeover grace secs.
# Reply: new | attached | takeover | done | expired | conflict.
REGISTER = """
local job = redis.call('HGETALL', KEYS[1])
local clock = redis.call('TIME')
local now = tonumber(clock[1])
if #job == 0 then
  redis.call('HSET', KEYS[1], 'state', 'queued', 'user', ARGV[1], 'params', ARGV[2],
    'caller', ARGV[3], 'confirmed', '0', 'created', now)
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
  if redis.call('HGET', KEYS[3], 'state') == false then
    local t = redis.call('TIME')
    redis.call('HSET', KEYS[3], 'state', 'waiting', 'model', ARGV[6], 'priority', ARGV[7],
      'enqueued_at', t[1] .. '.' .. string.format('%06d', tonumber(t[2])))
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
  end
  return 'new'
end
local fields = {}
for i = 1, #job, 2 do fields[job[i]] = job[i + 1] end
if fields['user'] ~= ARGV[1] or fields['params'] ~= ARGV[2] then return 'conflict' end
local state = fields['state']
if state == 'done' or state == 'failed' or state == 'expired' or state == 'cancelled' then
  if redis.call('EXISTS', KEYS[4]) == 1 then return 'done' end
  return 'expired'
end
-- An unconfirmed record older than the grace: its send failed or its sender died; take over.
if fields['confirmed'] == '0' and now - tonumber(fields['created'] or '0') >= tonumber(ARGV[8]) then
  redis.call('HSET', KEYS[1], 'caller', ARGV[3], 'created', now)
  redis.call('SADD', KEYS[2], ARGV[3])
  return 'takeover'
end
redis.call('SADD', KEYS[2], ARGV[3])
return 'attached'
"""

# KEYS: job, callers. ARGV: keep secs. The send succeeded: the record lives until the deadline
# plus the status TTL.
CONFIRM = """
redis.call('HSET', KEYS[1], 'confirmed', '1')
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
return 1
"""

# KEYS: job, callers, status. ARGV: caller. The send failed: drop the unconfirmed record.
ABANDON = """
local confirmed = redis.call('HGET', KEYS[1], 'confirmed')
if confirmed == '0' and redis.call('HGET', KEYS[1], 'caller') == ARGV[1] then
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
  return 1
end
return 0
"""

# KEYS: result, job, callers. ARGV: reply JSON, result ttl, final state, keep secs, channel
# prefix (`<prefix>:done:`), request id. Writes the result only if none exists, marks the claim
# final and wakes every caller waiting on this id. Reply: 1 written, 0 a result already existed.
RESULT_PUT = """
local written = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('HSET', KEYS[2], 'state', ARGV[3])
  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
end
for _, caller in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  redis.call('PUBLISH', ARGV[5] .. caller, ARGV[6])
end
if written then return 1 end
return 0
"""

# KEYS: result, job. ARGV: dedupe secs. Read without deleting; after the first read a result
# of a library-made id lives only `dedupe secs` more (an app-supplied id keeps its TTL).
RESULT_TAKE = """
local value = redis.call('GET', KEYS[1])
if not value then return false end
if redis.call('HGET', KEYS[2], 'app') ~= '1' then
  local ttl = redis.call('TTL', KEYS[1])
  if ttl > tonumber(ARGV[1]) then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) end
end
return value
"""

# KEYS: status. ARGV: state, ttl, then field / value pairs. A final state is never replaced; the
# TTL only grows; every write stamps the store's time as `<state>_at`.
STATUS_SET = """
local current = redis.call('HGET', KEYS[1], 'state')
if current == 'done' or current == 'failed' or current == 'expired' or current == 'cancelled' then
  return 0
end
local t = redis.call('TIME')
local now = t[1] .. '.' .. string.format('%06d', tonumber(t[2]))
local args = {'HSET', KEYS[1], 'state', ARGV[1], ARGV[1] .. '_at', now}
for i = 3, #ARGV, 2 do
  table.insert(args, ARGV[i])
  table.insert(args, ARGV[i + 1])
end
redis.call(unpack(args))
local ttl = redis.call('TTL', KEYS[1])
if ttl < tonumber(ARGV[2]) then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])) end
return 1
"""


@dataclass(frozen=True, slots=True)
class Registration:
    """What `register` found: `new`, `attached`, `takeover`, `done`, `expired` or `conflict`."""

    outcome: str

    @property
    def must_send(self) -> bool:
        """True when this caller must put the message on the queue."""
        return self.outcome in ("new", "takeover")


class RequestStore:
    """Claim records, results and request status of one environment."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: Keys,
        *,
        result_ttl_secs: int = 300,
        result_dedupe_secs: int = 30,
        status_ttl_secs: int = 1800,
        unconfirmed_secs: int = 30,
        takeover_grace_secs: int = 5,
    ) -> None:
        """Register the scripts; no I/O until the first call."""
        self._client = client
        self.keys = keys
        self.result_ttl = result_ttl_secs
        self.dedupe = result_dedupe_secs
        self.status_ttl = status_ttl_secs
        self.unconfirmed = unconfirmed_secs
        self.takeover_grace = takeover_grace_secs
        self._register = client.register_script(REGISTER)
        self._confirm = client.register_script(CONFIRM)
        self._abandon = client.register_script(ABANDON)
        self._put = client.register_script(RESULT_PUT)
        self._take = client.register_script(RESULT_TAKE)
        self._status = client.register_script(STATUS_SET)

    def _req(self, request_id: str) -> tuple[str, str, str, str]:
        k = self.keys
        return (
            k.request(request_id, "job"),
            k.request(request_id, "callers"),
            k.status(request_id),
            k.request(request_id, "res"),
        )

    async def register(
        self,
        request_id: str,
        *,
        caller: str,
        user_hash: str,
        params_hash: str,
        model: str,
        priority: bool,
        app_id: bool,
    ) -> Registration:
        """Register a call before it is sent (duplicate ids attach to the first run)."""
        job, callers, status, result = self._req(request_id)
        outcome = await self._register(
            [job, callers, status, result],
            [
                user_hash,
                params_hash,
                caller,
                self.unconfirmed,
                self.status_ttl,
                model,
                "1" if priority else "0",
                self.takeover_grace,
            ],
        )
        text = _text(outcome)
        if text == "new" and app_id:
            await self._client.hset(job, "app", "1")
        return Registration(text)

    async def confirm(self, request_id: str, *, keep_secs: float) -> None:
        """The send succeeded: keep the record until the deadline plus the status TTL."""
        job, callers, _, _ = self._req(request_id)
        await self._confirm([job, callers], [int(keep_secs) + self.status_ttl])

    async def abandon(self, request_id: str, *, caller: str) -> None:
        """The send failed: remove this caller's unconfirmed record."""
        job, callers, status, _ = self._req(request_id)
        await self._abandon([job, callers, status], [caller])

    async def put_result(self, reply: Reply, *, final_state: str) -> bool:
        """Store a result once and wake its callers; False when one was already stored."""
        job, callers, _, result = self._req(reply.request_id)
        written = await self._put(
            [result, job, callers],
            [
                reply.dumps(),
                self.result_ttl,
                final_state,
                self.status_ttl,
                f"{self.keys.prefix}:done:",
                reply.request_id,
            ],
        )
        return bool(written)

    async def take_result(self, request_id: str) -> Reply | None:
        """A stored result (not deleted; a library id keeps it `result_dedupe_secs` more)."""
        job, _, _, result = self._req(request_id)
        raw = await self._take([result, job], [self.dedupe])
        return None if raw is None else Reply.model_validate_json(raw)

    async def set_status(self, request_id: str, state: str, **fields: Any) -> bool:
        """Write a status (False when a final state was already there)."""
        pairs: list[str] = []
        for name, value in fields.items():
            if value is not None:
                pairs += [name, str(value)]
        done = await self._status([self.keys.status(request_id)], [state, self.status_ttl, *pairs])
        return bool(done)

    async def status(self, request_id: str) -> dict[str, str]:
        """The status hash of a request (empty when unknown or expired)."""
        raw = await self._client.hgetall(self.keys.status(request_id))
        return {_text(k): _text(v) for k, v in (raw or {}).items()}

    async def callers(self, request_id: str) -> Sequence[str]:
        """Callers waiting on a request id."""
        return [_text(v) for v in await self._client.smembers(self._req(request_id)[1])]

    async def job(self, request_id: str) -> Mapping[str, str]:
        """The claim record (for tests and status views)."""
        raw = await self._client.hgetall(self._req(request_id)[0])
        return {_text(k): _text(v) for k, v in (raw or {}).items()}


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
