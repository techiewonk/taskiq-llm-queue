"""Lua scripts for the limiter, leases, circuit breaker and claim store.

Every shared decision runs inside one script and reads the clock with Valkey `TIME`, so pods
with skewed clocks still agree. Floats go back to Python as strings: Lua numbers in a Redis
reply are truncated to integers.
"""

from __future__ import annotations

from typing import Any


def as_text(value: Any) -> str:
    """Decode a Redis reply item; the package's own pool runs without `decode_responses`."""
    return value.decode() if isinstance(value, bytes) else str(value)


_NOW = """
local _t = redis.call('TIME')
local now = tonumber(_t[1]) + tonumber(_t[2]) / 1000000
"""

# Shared by acquire and release: create the state on first use (stored value wins) and clamp.
_LIMIT = """
local min_l = tonumber(ARGV[2])
local max_l = tonumber(ARGV[3])
local limit = tonumber(redis.call('HGET', KEYS[1], 'limit') or '')
if not limit then
  limit = tonumber(ARGV[1])
  redis.call('HSET', KEYS[1], 'ramp', 1, 'ok_count', 0, 'paused_until', 0, 'last_decrease', 0)
end
limit = math.max(min_l, math.min(max_l, limit))
redis.call('HSET', KEYS[1], 'limit', limit)
local paused = tonumber(redis.call('HGET', KEYS[1], 'paused_until') or '0') or 0
"""

# KEYS: state hash, leases zset. ARGV: initial, min, max, share, lease_ttl, token.
# Reply: {granted 0|1, inflight, limit, expires_at (granted) or pause remaining (refused)}.
LIMITER_ACQUIRE = (
    _NOW
    + """
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
"""
    + _LIMIT
    + """
local inflight = redis.call('ZCARD', KEYS[2])
if paused > now then
  return {0, inflight, limit, tostring(paused - now)}
end
local allowed = math.max(1, math.floor(limit * tonumber(ARGV[4])))
if inflight >= allowed then
  return {0, inflight, limit, '0'}
end
local expires = now + tonumber(ARGV[5])
redis.call('ZADD', KEYS[2], expires, ARGV[6])
return {1, inflight + 1, limit, tostring(expires)}
"""
)

# KEYS: state hash, leases zset. ARGV: initial, min, max, token, throttled 0|1, ramp_window,
# increase_every, decrease_factor, pause_secs, ramp_cap, utilisation.
# Reply: {limit, removed 0|1, pause remaining}.
LIMITER_RELEASE = (
    _NOW
    + """
local removed = redis.call('ZREM', KEYS[2], ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
-- concurrency this attempt ran at: the others still in flight plus itself
local inflight = redis.call('ZCARD', KEYS[2]) + removed
"""
    + _LIMIT
    + """
if ARGV[5] == '1' then
  redis.call('HSET', KEYS[1], 'ramp', 0, 'ok_count', 0)
  local last = tonumber(redis.call('HGET', KEYS[1], 'last_decrease') or '0') or 0
  if now - last >= 1 then
    limit = math.max(min_l, math.floor(limit * tonumber(ARGV[8])))
    redis.call('HSET', KEYS[1], 'limit', limit, 'last_decrease', tostring(now))
  end
  local until_ts = math.max(paused, now + tonumber(ARGV[9]))
  redis.call('HSET', KEYS[1], 'paused_until', tostring(until_ts))
  return {limit, removed, tostring(until_ts - now)}
end
local ok = (tonumber(redis.call('HGET', KEYS[1], 'ok_count') or '0') or 0) + 1
if redis.call('HGET', KEYS[1], 'ramp') == '1' then
  if ok >= tonumber(ARGV[6]) then
    limit = math.max(limit, math.min(tonumber(ARGV[10]), limit * 2))
    ok = 0
  end
elseif inflight >= tonumber(ARGV[11]) * limit then
  if ok >= tonumber(ARGV[7]) then
    limit = math.min(max_l, limit + 1)
    ok = 0
  end
else
  ok = ok - 1
end
redis.call('HSET', KEYS[1], 'limit', limit, 'ok_count', ok)
local remaining = 0
if paused > now then remaining = paused - now end
return {limit, removed, tostring(remaining)}
"""
)

# KEYS: state hash, leases zset. Read-only. Reply: {limit or -1, live leases, pause remaining}.
LIMITER_PEEK = (
    _NOW
    + """
local limit = tonumber(redis.call('HGET', KEYS[1], 'limit') or '-1') or -1
local inflight = redis.call('ZCOUNT', KEYS[2], '(' .. tostring(now), '+inf')
local paused = tonumber(redis.call('HGET', KEYS[1], 'paused_until') or '0') or 0
local remaining = 0
if paused > now then remaining = paused - now end
return {limit, inflight, tostring(remaining)}
"""
)

# KEYS: leases zset. ARGV: lease_ttl, token... Renews only leases still present (DS11).
# Reply: {new expiry, renewed count}.
LEASE_RENEW = (
    _NOW
    + """
local expires = now + tonumber(ARGV[1])
local renewed = 0
for i = 2, #ARGV do
  renewed = renewed + redis.call('ZADD', KEYS[1], 'XX', 'CH', expires, ARGV[i])
end
return {tostring(expires), renewed}
"""
)

# KEYS: breaker hash. ARGV: op (allow|success|failure|peek), threshold, window, cooldown.
# Reply: {state, allowed 0|1}.
BREAKER = (
    _NOW
    + """
local op = ARGV[1]
local threshold = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local cooldown = tonumber(ARGV[4])
local st = redis.call('HGET', KEYS[1], 'state') or 'closed'
local opened = tonumber(redis.call('HGET', KEYS[1], 'opened_at') or '0') or 0
local function open_now()
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', tostring(now), 'fail_count', 0,
    'window_start', tostring(now))
  return {'open', 0}
end
if op == 'failure' then
  if st == 'half_open' then return open_now() end
  if st == 'open' then return {'open', 0} end
  local ws = tonumber(redis.call('HGET', KEYS[1], 'window_start') or '0') or 0
  local n = tonumber(redis.call('HGET', KEYS[1], 'fail_count') or '0') or 0
  if now - ws > window then ws = now; n = 0 end
  n = n + 1
  if n >= threshold then return open_now() end
  redis.call('HSET', KEYS[1], 'state', 'closed', 'fail_count', n, 'window_start', tostring(ws))
  return {'closed', 1}
end
if op == 'success' then
  if st == 'half_open' then
    redis.call('HSET', KEYS[1], 'state', 'closed', 'fail_count', 0, 'window_start', tostring(now))
    return {'closed', 1}
  end
  if st == 'open' then return {'open', 0} end
  return {'closed', 1}
end
if st == 'open' then
  if now - opened < cooldown then return {'open', 0} end
  if op == 'peek' then return {'half_open', 1} end
  redis.call('HSET', KEYS[1], 'state', 'half_open', 'probe_at', tostring(now))
  return {'half_open', 1}
end
if st == 'half_open' then
  local probe = tonumber(redis.call('HGET', KEYS[1], 'probe_at') or '0') or 0
  if now - probe < cooldown then return {'half_open', 0} end
  if op ~= 'peek' then redis.call('HSET', KEYS[1], 'probe_at', tostring(now)) end
  return {'half_open', 1}
end
return {'closed', 1}
"""
)

_CLAIM_TTL = """
local function ttl_for(deadline)
  return math.max(tonumber(ARGV[3]), math.ceil(deadline - now + tonumber(ARGV[4])))
end
"""

# KEYS: job hash, cancel flag. ARGV: deadline_at, worker, state_ttl, exec_max, stale_after.
# Reply: a ClaimDecision value (DS1-DS3, DS14).
CLAIM = (
    _NOW
    + _CLAIM_TTL
    + """
local deadline = tonumber(ARGV[1])
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'done' then return 'ack_done' end
if state == 'cancelled' then return 'ack_cancelled' end
if state == 'expired' then return 'ack_expired' end
local fresh = false
if state == 'running' then
  local hb = tonumber(redis.call('HGET', KEYS[1], 'heartbeat_at') or '0') or 0
  fresh = now - hb <= tonumber(ARGV[5])
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  -- a live owner sees the flag itself and writes the cancelled result
  if not fresh then
    redis.call('HSET', KEYS[1], 'state', 'cancelled')
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
  end
  return 'ack_cancelled'
end
if fresh then return 'defer' end
if now >= deadline then
  redis.call('HSET', KEYS[1], 'state', 'expired')
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
  return 'ack_expired'
end
redis.call('HSET', KEYS[1], 'state', 'running', 'worker', ARGV[2],
  'heartbeat_at', tostring(now), 'deadline_at', tostring(deadline))
if state == 'running' then redis.call('HINCRBY', KEYS[1], 'reclaims', 1) end
redis.call('EXPIRE', KEYS[1], ttl_for(deadline))
return 'run'
"""
)

# KEYS: job hashes. ARGV: worker, state_ttl, exec_max. Reply: indexes (1-based) not owned.
CLAIM_HEARTBEAT = (
    _NOW
    + """
local lost = {}
for i, key in ipairs(KEYS) do
  local f = redis.call('HMGET', key, 'state', 'worker', 'deadline_at')
  if f[1] == 'running' and f[2] == ARGV[1] then
    local deadline = tonumber(f[3] or '0') or 0
    redis.call('HSET', key, 'heartbeat_at', tostring(now))
    redis.call('EXPIRE', key,
      math.max(tonumber(ARGV[2]), math.ceil(deadline - now + tonumber(ARGV[3]))))
  else
    table.insert(lost, i)
  end
end
return lost
"""
)

# KEYS: job hash. ARGV: terminal state, state_ttl. Keeps the record at least state_ttl.
CLAIM_COMPLETE = """
redis.call('HSET', KEYS[1], 'state', ARGV[1])
if redis.call('TTL', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 1
"""

# KEYS: job hash. Marks a running task released so the next delivery runs at once.
CLAIM_RELEASE = """
if redis.call('HGET', KEYS[1], 'state') == 'running' then
  redis.call('HSET', KEYS[1], 'state', 'released')
  redis.call('HDEL', KEYS[1], 'worker', 'heartbeat_at')
  return 1
end
return 0
"""

# KEYS: config key, audit stream. ARGV: new JSON ('' deletes), actor, retention_ms.
# Writes the value and its audit entry atomically, trims entries older than retention (D89).
# Reply: the previous value ('' if none).
CONFIG_SET = (
    _NOW
    + """
local before = redis.call('GET', KEYS[1]) or ''
if ARGV[1] == '' then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], ARGV[1]) end
local now_ms = math.floor(now * 1000)
redis.call('XADD', KEYS[2], '*', 'key', KEYS[1], 'actor', ARGV[2], 'before', before,
  'after', ARGV[1], 'ts', tostring(now))
redis.call('XTRIM', KEYS[2], 'MINID', tostring(now_ms - tonumber(ARGV[3])))
return before
"""
)
