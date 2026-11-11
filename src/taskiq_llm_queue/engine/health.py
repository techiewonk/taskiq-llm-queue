"""Deployment health (plan §3.5, §3.18): auto-disable with safeguards, probes, recovery.

Target errors (model not in region, access denied) are counted per deployment and request in a
window; at `disable_errors` the deployment is disabled, unless it is the model's last usable one
(then only an alert). Errors are counted once the call's outcome is known: a request that hits
target errors on two deployments is the cause and is never counted. A disabled deployment stays
out until a probe succeeds; probes start after `probe_after_secs` and back off up to
`probe_max_secs`. Health lives apart from the registry, so a publish never re-enables one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import orjson

from taskiq_llm_queue.engine.keys import Keys

if TYPE_CHECKING:
    import redis.asyncio

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local req = cjson.decode(ARGV[1])
"""

# KEYS: health hash, the deployment's target-error set. req: dep, request_id, window_ms, needed,
# probe_after_ms, others (keys of the model's other deployments).
# Reply: counted | disabled | last.
RECORD = (
    _NOW
    + """
redis.call('ZADD', KEYS[2], now, req.request_id)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - req.window_ms)
redis.call('PEXPIRE', KEYS[2], req.window_ms)
if redis.call('ZCARD', KEYS[2]) < req.needed then return 'counted' end
local usable_left = 0
for _, other in ipairs(req.others) do
  if not redis.call('HGET', KEYS[1], other .. '|until') then usable_left = usable_left + 1 end
end
if usable_left == 0 then return 'last' end
redis.call('HSET', KEYS[1], req.dep .. '|until', now + req.probe_after_ms,
  req.dep .. '|wait', req.probe_after_ms, req.dep .. '|since', now)
redis.call('DEL', KEYS[2])
return 'disabled'
"""
)

# KEYS: health hash. req: dep, ok (bool), probe_max_ms. Reply: recovered | waiting.
PROBED = (
    _NOW
    + """
if req.ok then
  redis.call('HDEL', KEYS[1], req.dep .. '|until', req.dep .. '|wait', req.dep .. '|since')
  return 'recovered'
end
local last = tonumber(redis.call('HGET', KEYS[1], req.dep .. '|wait') or '0')
local wait = math.min(math.max(last * 2, 60000), req.probe_max_ms)
redis.call('HSET', KEYS[1], req.dep .. '|until', now + wait, req.dep .. '|wait', wait)
return 'waiting'
"""
)


class Health:
    """Health records of every model of one environment."""

    def __init__(self, client: redis.asyncio.Redis, keys: Keys) -> None:
        """Register the scripts; no I/O until the first call."""
        self._client = client
        self.keys = keys
        self._record = client.register_script(RECORD)
        self._probed = client.register_script(PROBED)

    def _terr(self, alias: str, dep: str) -> str:
        return self.keys.model(alias, f"terr:{dep}")

    async def record_target_error(
        self,
        alias: str,
        dep: str,
        request_id: str,
        *,
        others: Sequence[str],
        window_secs: float,
        needed: int,
        probe_after_secs: float,
    ) -> str:
        """Count a target error; `disabled`, `last` (would be the last usable) or `counted`."""
        body = {
            "dep": dep,
            "request_id": request_id,
            "window_ms": int(window_secs * 1000),
            "needed": needed,
            "probe_after_ms": int(probe_after_secs * 1000),
            "others": list(others),
        }
        reply = await self._record(
            [self.keys.model(alias, "health"), self._terr(alias, dep)], [orjson.dumps(body)]
        )
        return reply.decode() if isinstance(reply, bytes) else str(reply)

    async def probed(self, alias: str, dep: str, *, ok: bool, probe_max_secs: float) -> str:
        """A probe's outcome: `recovered` or `waiting` (the wait doubles up to the maximum)."""
        body = {"dep": dep, "ok": ok, "probe_max_ms": int(probe_max_secs * 1000)}
        reply = await self._probed([self.keys.model(alias, "health")], [orjson.dumps(body)])
        return reply.decode() if isinstance(reply, bytes) else str(reply)

    async def disabled(self, alias: str) -> dict[str, float]:
        """Disabled deployments of a model and when each is next probed (store time, secs)."""
        raw = await self._client.hgetall(self.keys.model(alias, "health"))
        out: dict[str, float] = {}
        for field, value in (raw or {}).items():
            name = field.decode() if isinstance(field, bytes) else str(field)
            if name.endswith("|until"):
                out[name[: -len("|until")]] = float(value) / 1000.0
        return out

    async def disable(self, alias: str, dep: str, *, probe_after_secs: float) -> None:
        """Disable a deployment now (tests and operators)."""
        secs, micros = await self._client.time()
        now_ms = int(secs * 1000 + micros / 1000)
        wait = int(probe_after_secs * 1000)
        await self._client.hset(
            self.keys.model(alias, "health"),
            mapping={f"{dep}|until": now_ms + wait, f"{dep}|wait": wait, f"{dep}|since": now_ms},
        )

    async def snapshot(self, alias: str) -> dict[str, Any]:
        """The raw health hash of a model (status view)."""
        raw = await self._client.hgetall(self.keys.model(alias, "health"))
        return {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else str(v)
            )
            for k, v in (raw or {}).items()
        }
