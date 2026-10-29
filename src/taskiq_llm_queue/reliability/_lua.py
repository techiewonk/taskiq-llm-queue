"""Lua scripts for the limiter, leases, circuit breaker and claim store.

Every shared decision runs inside one script and reads the clock with Valkey `TIME`, so pods
with skewed clocks still agree. Floats go back to Python as strings: Lua numbers in a Redis
reply are truncated to integers.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from redis.exceptions import NoScriptError

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

ScriptCall = tuple["AsyncScript", Sequence[Any], Sequence[Any]]


def as_text(value: Any) -> str:
    """Decode a Redis reply item; the package's own pool runs without `decode_responses`."""
    return value.decode() if isinstance(value, bytes) else str(value)


def queue_script(pipe: Any, script: AsyncScript, keys: Sequence[Any], args: Sequence[Any]) -> None:
    """Queue a registered script on a pipeline as a raw `EVALSHA`.

    `script(..., client=pipe)` would register it on the pipeline, and redis-py then spends an
    extra `SCRIPT EXISTS` round trip on every `execute`; `settle` handles a `NOSCRIPT` reply.
    """
    pipe.evalsha(script.sha, len(keys), *keys, *args)


async def settle(reply: Any, script: AsyncScript, keys: Sequence[Any], args: Sequence[Any]) -> Any:
    """Reply of a queued script: re-run it alone on `NOSCRIPT` (it did not run), raise errors."""
    if isinstance(reply, NoScriptError):
        return await script(list(keys), list(args))
    if isinstance(reply, Exception):
        raise reply
    return reply


async def run_many(client: Any, calls: Sequence[ScriptCall]) -> list[Any]:
    """Run several scripts in one non-transactional pipeline: one round trip, not one each.

    Works on cluster clients too (the pipeline groups commands by node); replies keep order.
    """
    if not calls:
        return []
    async with client.pipeline(transaction=False) as pipe:
        for script, keys, args in calls:
            queue_script(pipe, script, keys, args)
        replies = await pipe.execute(raise_on_error=False)
    return [
        await settle(reply, script, keys, args)
        for reply, (script, keys, args) in zip(replies, calls, strict=True)
    ]


_NOW = """
local _t = redis.call('TIME')
local now = tonumber(_t[1]) + tonumber(_t[2]) / 1000000
"""

# Shared by acquire and release: create the state on first use (stored value wins) and clamp.
# Writes only when the state is new or the stored limit is outside [min, max], so a refused
# acquire stays read-only (no hot-key writes while callers wait).
_LIMIT = """
local min_l = tonumber(ARGV[2])
local max_l = tonumber(ARGV[3])
local stored = tonumber(redis.call('HGET', KEYS[1], 'limit') or '')
local limit = stored
if not limit then
  limit = tonumber(ARGV[1])
  redis.call('HSET', KEYS[1], 'ramp', 1, 'ok_count', 0, 'paused_until', 0, 'last_decrease', 0)
end
limit = math.max(min_l, math.min(max_l, limit))
if limit ~= stored then redis.call('HSET', KEYS[1], 'limit', limit) end
local paused = tonumber(redis.call('HGET', KEYS[1], 'paused_until') or '0') or 0
"""

# Live leases (score > now) without pruning, so the count needs no write.
_LIVE = """
local function live_leases()
  return redis.call('ZCOUNT', KEYS[2], '(' .. tostring(now), '+inf')
end
"""

# KEYS: state hash, leases zset. ARGV: initial, min, max, share, lease_ttl, token.
# Reply: {granted 0|1, inflight, limit, expires_at (granted) or pause remaining (refused)}.
# A refusal writes nothing; expired leases are pruned only when a slot is granted (DS11).
LIMITER_ACQUIRE = (
    _NOW
    + _LIVE
    + _LIMIT
    + """
local inflight = live_leases()
if paused > now then
  return {0, inflight, limit, tostring(paused - now)}
end
local allowed = math.max(1, math.floor(limit * tonumber(ARGV[4])))
if inflight >= allowed then
  return {0, inflight, limit, '0'}
end
if redis.call('ZCARD', KEYS[2]) > inflight then
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
end
local expires = now + tonumber(ARGV[5])
redis.call('ZADD', KEYS[2], expires, ARGV[6])
return {1, inflight + 1, limit, tostring(expires)}
"""
)

# KEYS: state hash, leases zset. ARGV: initial, min, max, token, throttled 0|1, ramp_window,
# increase_every, decrease_factor, pause_secs, ramp_cap, utilisation, peak_halflife, share,
# loss_tolerance, min_throttles, pause_ratio, window_secs, settle_ratio, probe_secs, lease_ttl,
# hold_ratio.
# Reply: {limit, removed 0|1, pause remaining}.
#
# Decrease (S35: a 2 % random 429 rate held limits at 3-5): outcomes are counted in a sliding
# window of `window_secs`; a throttle cuts the limit only when the window holds >= min_throttles
# throttles making up >= loss_tolerance of its outcomes, at most once per window, and only for a
# call started after the last cut (one cut per round trip). The pause is kept for throttle
# storms (95 % lower bound of the ratio >= pause_ratio), not for every 429 or a lucky window.
# Growth: +1 per min(increase_every, limit / 2) busy successes. After a cut the limit it was cut
# from is kept as `w_max`; from `settle_ratio x w_max` on, growth is one step per `probe_secs`
# and only on a success at full use (S07: it saw-toothed 44-64 around a capacity of 50 with
# ~50 leases in use); `w_max` is dropped once the limit passes `w_max / settle_ratio` without
# a cut (capacity grew). A throttle met while probing, `probe_secs` after the last cut, cuts
# back to the plateau (x settle_ratio), not x0.7.
# Futile cuts (S35b: a 30 % random 429 storm kept cutting every second): when the throttle
# ratio is still >= hold_ratio x the ratio at the last cut (within 10 windows of the last cut
# or hold), cutting did not help, so the throttles do not come from our concurrency; the limit
# is then not cut below the start value.
# DS12: a success counts toward growth only when inflight >= utilisation x the lane's allowed
# slots; growth is capped at 2 x the peak inflight (decaying with `peak_halflife` seconds).
LIMITER_RELEASE = (
    _NOW
    + """
local score = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[4]) or '')
local removed = redis.call('ZREM', KEYS[2], ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
-- concurrency this attempt ran at: the others still in flight plus itself
local inflight = redis.call('ZCARD', KEYS[2]) + removed
"""
    + _LIMIT
    + """
local throttled = ARGV[5] == '1'
local window = tonumber(ARGV[17])
local settle = tonumber(ARGV[18])
local h = redis.call('HMGET', KEYS[1], 'win_start', 'win_ok', 'win_thr', 'prev_ok', 'prev_thr',
  'last_decrease', 'w_max', 'last_increase', 'cut_ratio', 'last_hold')
local win_start = tonumber(h[1] or '0') or 0
local win_ok = tonumber(h[2] or '0') or 0
local win_thr = tonumber(h[3] or '0') or 0
local prev_ok = tonumber(h[4] or '0') or 0
local prev_thr = tonumber(h[5] or '0') or 0
local last_decrease = tonumber(h[6] or '0') or 0
local w_max = tonumber(h[7] or '0') or 0
local last_increase = tonumber(h[8] or '0') or 0
local cut_ratio = tonumber(h[9] or '0') or 0
local last_hold = tonumber(h[10] or '0') or 0
if now - win_start >= window then
  if now - win_start < 2 * window then
    prev_ok, prev_thr = win_ok, win_thr
  else
    prev_ok, prev_thr = 0, 0
  end
  win_start, win_ok, win_thr = now, 0, 0
end
if throttled then win_thr = win_thr + 1 else win_ok = win_ok + 1 end
redis.call('HSET', KEYS[1], 'win_start', tostring(win_start), 'win_ok', win_ok,
  'win_thr', win_thr, 'prev_ok', prev_ok, 'prev_thr', prev_thr)
local function remaining()
  if paused > now then return tostring(paused - now) end
  return '0'
end
if throttled then
  local carry = math.max(0, 1 - (now - win_start) / window)
  local e_thr = win_thr + prev_thr * carry
  local ratio = e_thr / math.max(1, e_thr + win_ok + prev_ok * carry)
  local started = now
  if score then started = score - tonumber(ARGV[20]) end
  if e_thr >= tonumber(ARGV[15]) and ratio >= tonumber(ARGV[14])
      and now - last_decrease >= window and started >= last_decrease then
    local factor = tonumber(ARGV[8])
    if w_max > 0 and limit >= math.floor(settle * w_max)
        and now - last_decrease >= tonumber(ARGV[19]) then
      factor = settle  -- probing hit the known ceiling again: back to the plateau only
    end
    local target = math.max(min_l, math.floor(limit * factor))
    if cut_ratio > 0 and now - math.max(last_decrease, last_hold) <= 10 * window
        and ratio >= tonumber(ARGV[21]) * cut_ratio then
      target = math.max(target, math.min(limit, tonumber(ARGV[1])))  -- futile: hold the start
      if target >= limit then redis.call('HSET', KEYS[1], 'last_hold', tostring(now)) end
    end
    if target < limit then
      redis.call('HSET', KEYS[1], 'w_max', limit, 'ramp', 0, 'ok_count', 0,
        'last_decrease', tostring(now), 'cut_ratio', tostring(ratio))
      limit = target
      redis.call('HSET', KEYS[1], 'limit', limit)
    end
  end
  -- Pause only when the window *shows* a storm: the 95 % Wilson lower bound of the throttle
  -- ratio must reach pause_ratio. S35b: 2 of 3 throttled under 30 % random 429s paused a
  -- model ~22 % of windows; fewer outcomes then paused it more (stuck 50-70 % of the time).
  -- 3 of 3 pauses, 5 of 6 does not. Checked on every throttle, not only when a cut is due,
  -- so the evidence that arrives after the window's one cut still counts.
  if e_thr >= tonumber(ARGV[15]) then
    local n = e_thr + win_ok + prev_ok * carry
    local zz = 2.706  -- 1.645^2 (one-sided 95 %)
    local lower = (ratio + zz / (2 * n)
      - math.sqrt(zz) * math.sqrt(ratio * (1 - ratio) / n + zz / (4 * n * n))) / (1 + zz / n)
    if lower >= tonumber(ARGV[16]) then
      paused = math.max(paused, now + tonumber(ARGV[9]))
      redis.call('HSET', KEYS[1], 'paused_until', tostring(paused))
    end
  end
  return {limit, removed, remaining()}
end
local halflife = tonumber(ARGV[12] or '0') or 0
local peak = tonumber(redis.call('HGET', KEYS[1], 'peak') or '0') or 0
local peak_at = tonumber(redis.call('HGET', KEYS[1], 'peak_at') or '0') or 0
if halflife > 0 and now > peak_at then peak = peak * 0.5 ^ ((now - peak_at) / halflife) end
if inflight >= peak then
  peak = inflight
  redis.call('HSET', KEYS[1], 'peak', inflight, 'peak_at', tostring(now))
end
local share = tonumber(ARGV[13] or '1') or 1
local busy = inflight >= tonumber(ARGV[11]) * math.max(1, math.floor(limit * share))
local peak_cap = math.max(min_l, math.floor(2 * peak))
local ok = (tonumber(redis.call('HGET', KEYS[1], 'ok_count') or '0') or 0) + 1
local need = math.max(1, math.min(tonumber(ARGV[7]), math.ceil(limit / 2)))
local grow = false
if not busy then
  ok = ok - 1
elseif redis.call('HGET', KEYS[1], 'ramp') == '1' then
  if ok >= tonumber(ARGV[6]) then
    limit = math.max(limit, math.min(tonumber(ARGV[10]), peak_cap, limit * 2))
    ok = 0
  end
elseif w_max > 0 and limit >= w_max / settle then
  redis.call('HSET', KEYS[1], 'w_max', 0)  -- passed the old ceiling without a cut
  grow = ok >= need
elseif w_max > 0 and limit >= math.floor(settle * w_max) then
  -- probe only while the limit binds: every allowed slot in use, not 0.9 of them
  local full = inflight >= math.max(1, math.floor(limit * share))
  grow = full and ok >= need and now - last_increase >= tonumber(ARGV[19])
else
  grow = ok >= need
end
if grow then
  limit = math.max(limit, math.min(max_l, peak_cap, limit + 1))
  ok = 0
  redis.call('HSET', KEYS[1], 'last_increase', tostring(now))
end
redis.call('HSET', KEYS[1], 'limit', limit, 'ok_count', ok)
return {limit, removed, remaining()}
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

# KEYS: leases zset. ARGV: lease_ttl, token... Renews only leases still live (DS11): an
# expired lease's slot may already be counted free, so it is not brought back.
# Reply: {new expiry, renewed count}.
LEASE_RENEW = (
    _NOW
    + """
local expires = now + tonumber(ARGV[1])
local renewed = 0
for i = 2, #ARGV do
  local score = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[i]) or '')
  if score and score > now then
    renewed = renewed + redis.call('ZADD', KEYS[1], 'XX', 'CH', expires, ARGV[i])
  end
end
return {tostring(expires), renewed}
"""
)

# KEYS: breaker hash. ARGV: op (allow|success|failure|neutral|peek), threshold, window,
# cooldown, probes. Half-open lets up to `probes` calls through at once; a probe slot comes back
# on `neutral` (an outcome that counts neither way) or after `cooldown` (a lost probe).
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
local max_probes = tonumber(ARGV[5] or '1') or 1
local probes = tonumber(redis.call('HGET', KEYS[1], 'probes') or '0') or 0
if op == 'success' then
  if st == 'half_open' then
    redis.call('HSET', KEYS[1], 'state', 'closed', 'fail_count', 0, 'window_start', tostring(now),
      'probes', 0)
    return {'closed', 1}
  end
  if st == 'open' then return {'open', 0} end
  return {'closed', 1}
end
if op == 'neutral' then
  if st == 'half_open' and probes > 0 then
    redis.call('HSET', KEYS[1], 'probes', probes - 1)
    return {'half_open', 1}
  end
  if st == 'open' then return {'open', 0} end
  return {st, 1}
end
if st == 'open' then
  if now - opened < cooldown then return {'open', 0} end
  if op == 'peek' then return {'half_open', 1} end
  redis.call('HSET', KEYS[1], 'state', 'half_open', 'probe_at', tostring(now), 'probes', 1)
  return {'half_open', 1}
end
if st == 'half_open' then
  local probe = tonumber(redis.call('HGET', KEYS[1], 'probe_at') or '0') or 0
  local lost = now - probe >= cooldown
  if probes >= max_probes and not lost then return {'half_open', 0} end
  if op ~= 'peek' then
    if lost then probes = 0 end
    redis.call('HSET', KEYS[1], 'probe_at', tostring(now), 'probes', probes + 1)
  end
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

# KEYS: job hashes. ARGV: worker, state_ttl, exec_max. Reply: indexes (1-based) lost to
# another worker, or whose record is gone. A record this worker already finished (done,
# cancelled, expired) or released is not lost: the task is between its own CAS and leaving the
# running set, and cancelling it there killed its result publish (S35c: 19 spurious losses).
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
  elseif not f[1] or (f[2] and f[2] ~= ARGV[1]) then
    table.insert(lost, i)
  end
end
return lost
"""
)

# KEYS: job hash. ARGV: terminal state, state_ttl, worker ('' skips the owner check).
# Keeps the record at least state_ttl. With a worker, the write is a CAS: a record owned by
# another worker is left alone, so a stale former owner cannot finish the new owner's run.
# Reply: 1 written, -1 not the owner.
CLAIM_COMPLETE = """
local worker = ARGV[3] or ''
if worker ~= '' then
  local owner = redis.call('HGET', KEYS[1], 'worker')
  if owner and owner ~= worker then return -1 end
end
redis.call('HSET', KEYS[1], 'state', ARGV[1])
if redis.call('TTL', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 1
"""

# KEYS: job hash. ARGV: worker ('' skips the owner check). Marks a running task released so the
# next delivery runs at once; only the current owner may do so when a worker is given.
# Reply: 1 released, 0 not running, -1 running under another worker.
CLAIM_RELEASE = """
if redis.call('HGET', KEYS[1], 'state') ~= 'running' then return 0 end
local worker = ARGV[1] or ''
if worker ~= '' and redis.call('HGET', KEYS[1], 'worker') ~= worker then return -1 end
redis.call('HSET', KEYS[1], 'state', 'released')
redis.call('HDEL', KEYS[1], 'worker', 'heartbeat_at')
return 1
"""

# KEYS: config key, audit stream. ARGV: new JSON ('' deletes), actor, retention_ms.
# Writes the value and its audit entry atomically, trims entries older than retention.
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

# F17 rate budget for one scope (a model target or a provider's shared budget). KEYS: rate
# hash. ARGV: op (take|give|peek), mode (smooth|fixed), bg (0|1), bg_share, ttl, then per
# budget (rpm, tpm): limit (0 = off), cost, burst; then (give) calls to take back. `smooth`
# is GCRA per budget (theoretical arrival time `<b>_tat`, burst allowance `burst` units, a
# single cost above it passes when idle); `fixed` counts per calendar minute. bg/bulk also
# pass a second bucket at
# `limit * bg_share`, so rt keeps the rest. `take` is all or nothing: reply {1, '0'} or
# {0, wait_secs}. `give` returns `cost` (negative: charges more, for a TPM correction).
# `seen_*` count calls / tokens taken in the current minute (status view).
# peek reply: {seen_calls, seen_tokens, available rpm units, available tpm units}.
RATE = (
    _NOW
    + """
local op, mode, bg = ARGV[1], ARGV[2], ARGV[3] == '1'
local share, ttl = tonumber(ARGV[4]), tonumber(ARGV[5])
local window = math.floor(now / 60)
local budgets = {}
for i, name in ipairs({'rpm', 'tpm'}) do
  local base = 5 + (i - 1) * 3
  local limit = tonumber(ARGV[base + 1])
  if limit > 0 then
    budgets[#budgets + 1] = {name = name, limit = limit,
      cost = tonumber(ARGV[base + 2]), burst = tonumber(ARGV[base + 3])}
  end
end
local function hnum(field, default)
  return tonumber(redis.call('HGET', KEYS[1], field) or '') or default
end
if mode == 'fixed' and hnum('win', -1) ~= window then
  redis.call('HSET', KEYS[1], 'win', window, 'rpm_n', 0, 'rpm_bgn', 0, 'tpm_n', 0, 'tpm_bgn', 0)
end
if hnum('seen_win', -1) ~= window then
  redis.call('HSET', KEYS[1], 'seen_win', window, 'seen_calls', 0, 'seen_tokens', 0)
end
-- One bucket: returns (wait, new value) for a take of `cost`.
local function check(b, suffix, limit, burst)
  if mode == 'fixed' then
    local n = hnum(b.name .. suffix, 0)
    if n + b.cost <= limit or (n == 0 and b.cost > limit) then return 0, n + b.cost end
    return (window + 1) * 60 - now, n
  end
  local t = 60 / limit
  local tat = math.max(hnum(b.name .. suffix, now), now)
  local new = tat + b.cost * t
  local over = new - now - math.max(burst, b.cost) * t
  if over > 0 then return over, tat end
  return 0, new
end
local function buckets(b)
  local list = {{suffix = (mode == 'fixed') and '_n' or '_tat', limit = b.limit, burst = b.burst}}
  if bg then
    list[2] = {suffix = (mode == 'fixed') and '_bgn' or '_bgtat',
      limit = math.max(b.limit * share, 1e-9), burst = math.max(1, math.floor(b.burst * share))}
  end
  return list
end
if op == 'peek' then
  local avail = {-1, -1}
  for i, b in ipairs(budgets) do
    local idx = (b.name == 'rpm') and 1 or 2
    if mode == 'fixed' then
      avail[idx] = math.max(0, b.limit - hnum(b.name .. '_n', 0))
    else
      local t = 60 / b.limit
      local debt = math.max(hnum(b.name .. '_tat', now), now) - now
      avail[idx] = math.max(0, math.floor((b.burst * t - debt) / t))
    end
  end
  return {hnum('seen_calls', 0), hnum('seen_tokens', 0), avail[1], avail[2]}
end
if op == 'give' then
  for _, b in ipairs(budgets) do
    for _, k in ipairs(buckets(b)) do
      local field = b.name .. k.suffix
      if mode == 'fixed' then
        redis.call('HSET', KEYS[1], field, math.max(0, hnum(field, 0) - b.cost))
      else
        local tat = hnum(field, now)
        if b.cost < 0 or tat > now then
          tat = math.max(math.max(tat, now) - b.cost * 60 / k.limit, now)
          redis.call('HSET', KEYS[1], field, tostring(tat))
        end
      end
    end
    if b.name == 'tpm' then redis.call('HINCRBY', KEYS[1], 'seen_tokens', -b.cost) end
  end
  redis.call('HINCRBY', KEYS[1], 'seen_calls', -tonumber(ARGV[12] or '0'))
  redis.call('EXPIRE', KEYS[1], ttl)
  return {1, '0'}
end
local wait, updates = 0, {}
for _, b in ipairs(budgets) do
  for _, k in ipairs(buckets(b)) do
    local w, value = check(b, k.suffix, k.limit, k.burst)
    if w > wait then wait = w end
    updates[#updates + 1] = {b.name .. k.suffix, value}
  end
end
if wait > 0 then return {0, tostring(wait)} end
for _, u in ipairs(updates) do redis.call('HSET', KEYS[1], u[1], tostring(u[2])) end
redis.call('HINCRBY', KEYS[1], 'seen_calls', 1)
for _, b in ipairs(budgets) do
  if b.name == 'tpm' then redis.call('HINCRBY', KEYS[1], 'seen_tokens', b.cost) end
end
redis.call('EXPIRE', KEYS[1], ttl)
return {1, '0'}
"""
)
