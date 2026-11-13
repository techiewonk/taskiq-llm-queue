"""Lua of the admission scripts: PICK_TAKE, FINISH, UNDO, RENEW and SWEEP (plan §3.4, §3.18).

Each script takes the key prefix and one JSON argument and builds its key names itself, which
needs the single-primary store 0.3 supports. Times are the store's clock in ms. Budgets are GCRA
clocks (`smooth`) or minute counters (`fixed`); class clocks enforce the priority shares.
"""

# ruff: noqa: E501  # Lua source in strings; wrapping its lines hurts reading it

from __future__ import annotations

_COMMON = r"""
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local minute = math.floor(now / 60000)
local P = ARGV[1]
local req = cjson.decode(ARGV[2])
-- Timings and limits: `EngineTimings` (sent as `t` by every call) and the registry's breaker
-- settings (on each row); the numbers after them are the defaults.
local T = req.t or {}
local KEY_MS  -- budget key lifetime: set below, once num() exists
local function mk(alias, name) return P .. ':{m:' .. alias .. '}:' .. name end
local function num(v, d) if v == nil or v == false or v == cjson.null then return d end return tonumber(v) end
local function hnum(key, field, d) return num(redis.call('HGET', key, field), d) end
local function present(v) return v ~= nil and v ~= false and v ~= cjson.null end
KEY_MS = num(T.budget_key_ms, 180000)  -- budget and limiter keys live this long (> 2 minutes)

-- One budget: field prefix f, limit L per minute, burst fraction bf, window mode.
local function avail(key, f, L, bf, mode)
  local burst = math.max(1, math.floor(L * bf))
  if mode == 'fixed' then
    local n = 0
    if hnum(key, f .. '_win', -1) == minute then n = hnum(key, f .. '_n', 0) end
    return L - n, n
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  return (now + burst * per - tat) / per, tat
end

-- Room for cost c behind R reserved units: returns wait ms (0 = fits) and the new state.
local reset_minute = num(redis.call('GET', P .. ':reset_minute'), -1)
local function room(key, f, L, c, R, bf, mode)
  local burst = math.max(1, math.floor(L * bf))
  if mode == 'fixed' then
    -- After a store reset, a fixed window holds until the next minute (plan T10).
    if reset_minute == minute and hnum(key, f .. '_win', -1) ~= minute then
      return (minute + 1) * 60000 - now, nil
    end
    local _, n = avail(key, f, L, bf, mode)
    if n + R + c <= L or (n == 0 and R == 0 and c > L) then return 0, n + c end
    return (minute + 1) * 60000 - now, nil
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  if c > burst and R == 0 then
    if tat <= now then return 0, now + c * per end
    return tat - now, nil
  end
  local over = tat + (R + c) * per - now - burst * per
  if over > 0 then return over, nil end
  return 0, tat + c * per
end

local function write(key, f, mode, value)
  if mode == 'fixed' then
    redis.call('HSET', key, f .. '_win', minute, f .. '_n', value)
  else
    redis.call('HSET', key, f .. '_tat', tostring(value))
  end
  redis.call('PEXPIRE', key, KEY_MS)
end

-- Give back g units (negative g charges more). Fixed counters only inside the grant's minute.
local function give(key, f, L, g, mode, granted_minute)
  if not L or L <= 0 or g == 0 then return end
  if mode == 'fixed' then
    if granted_minute ~= minute and g > 0 then return end
    if hnum(key, f .. '_win', -1) ~= minute then
      -- The minute turned: an overrun (g < 0) is charged to the new minute, not dropped.
      if g < 0 then
        redis.call('HSET', key, f .. '_win', minute, f .. '_n', -g)
        redis.call('PEXPIRE', key, KEY_MS)
      end
      return
    end
    redis.call('HSET', key, f .. '_n', math.max(0, hnum(key, f .. '_n', 0) - g))
    redis.call('PEXPIRE', key, KEY_MS)
    return
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  local value = tat - g * per
  if g > 0 then value = math.max(value, now) end
  redis.call('HSET', key, f .. '_tat', tostring(value))
  redis.call('PEXPIRE', key, KEY_MS)  -- the key may have expired since the grant
end

-- The budgets of a deployment row for class cls: {key, field, limit, cost kind}.
local function budgets(alias, row, model, cls, account)
  local list = {}
  local limits = row.rate_limits
  local rkey = mk(alias, 'rate:' .. row.key)
  local share = 1
  if cls == 'n' then share = 1 - num(model.priority_reserve, 0) else share = 1 - num(model.normal_min_share, 0) end
  local function add(key, f, L, kind)
    if present(L) and tonumber(L) > 0 then table.insert(list, {key, f, tonumber(L), kind}) end
  end
  if limits == 'both' or limits == 'rpm' then
    add(rkey, 'rpm', row.rpm, 'r')
    if share < 1 and present(row.rpm) then add(rkey, cls .. '_rpm', row.rpm * share, 'r') end
  end
  if limits == 'both' or limits == 'tpm' then
    add(rkey, 'tpm', row.tpm, 't')
    if share < 1 and present(row.tpm) then add(rkey, cls .. '_tpm', row.tpm * share, 't') end
  end
  if account then
    local akey = P .. ':{a:' .. row.account .. '}:rate'
    local ashare = 1
    if cls == 'n' then ashare = 1 - num(row.account_priority_reserve, 0) end
    add(akey, 'rpm', row.shared_rpm, 'r')
    add(akey, 'tpm', row.shared_tpm, 't')
    if ashare < 1 then
      if present(row.shared_rpm) then add(akey, 'n_rpm', row.shared_rpm * ashare, 'r') end
      if present(row.shared_tpm) then add(akey, 'n_tpm', row.shared_tpm * ashare, 't') end
    end
  end
  return list
end

local function lease_keys(alias, dep)
  return mk(alias, 'lease:' .. dep .. ':p'), mk(alias, 'lease:' .. dep .. ':n')
end

local function limit_of(alias, row)
  local key = mk(alias, 'ac:' .. row.key)
  local cap = num(row.max_concurrency, 200)
  local limit = hnum(key, 'limit', nil)
  if limit == nil then
    -- A new deployment takes its full share at once (plan E74).
    limit = cap
    redis.call('HSET', key, 'limit', tostring(limit))
  end
  return math.max(1, math.floor(math.min(limit, cap))), cap
end

local function cfg_of(alias)
  local raw = redis.call('HGET', mk(alias, 'cfg'), 'numbers')
  if not raw then return nil end
  local doc = cjson.decode(raw)
  local rows = {}
  for _, row in ipairs(doc.deployments) do rows[row.key] = row end
  return doc.model, rows, hnum(mk(alias, 'cfg'), 'changes', -1)
end
"""

PICK_TAKE = (
    _COMMON
    + r"""
local alias = req.alias
local model, rows, changes = cfg_of(alias)
if model == nil then return cjson.encode({r = 'no_config'}) end
if changes ~= num(req.changes, -2) then return cjson.encode({r = 'stale_config', changes = changes}) end
local cls = req.cls
local rid = req.request_id
local entry_key = mk(alias, 'inprog:' .. rid)

-- Finished or cancelled? Checked for every ask: a fallback ask (claim off) once started a
-- call that was already cancelled or answered elsewhere. Only the claim write needs `claim`.
local job_key = P .. ':{r:' .. rid .. '}:job'
if redis.call('EXISTS', P .. ':{r:' .. rid .. '}:res') == 1 then return cjson.encode({r = 'done'}) end
if redis.call('EXISTS', P .. ':{r:' .. rid .. '}:cancel') == 1 then return cjson.encode({r = 'cancelled'}) end
local state = redis.call('HGET', job_key, 'state')
if state == 'done' or state == 'failed' or state == 'expired' or state == 'cancelled' then
  return cjson.encode({r = 'done'})
end
-- Running under another worker with a fresh heartbeat (on another model, or between its
-- retries when no entry exists): a redelivered message is a duplicate, not a new run.
if req.claim and state == 'running' then
  local runner = redis.call('HGET', job_key, 'owner')
  if runner and runner ~= req.worker
    and now - hnum(job_key, 'hb', 0) < num(req.lease_ms, 60000) then
    return cjson.encode({r = 'duplicate'})
  end
end
local held = redis.call('HGET', entry_key, 'owner')
if held then
  if held == req.owner then
    return cjson.encode({r = 'granted', dep = redis.call('HGET', entry_key, 'dep'),
      minute = hnum(entry_key, 'minute', minute), repeat_ = true})
  end
  local holder = redis.call('HGET', entry_key, 'worker')
  local hb = hnum(job_key, 'hb', 0)
  local fresh = now - hb < num(req.lease_ms, 60000) and redis.call('HGET', entry_key, 'started') ~= false
  if fresh then return cjson.encode({r = 'duplicate'}) end
  -- A dead owner (no heartbeat for a lease): drop its slot and entry (no refund).
  local old_dep = redis.call('HGET', entry_key, 'dep')
  local old_cls = redis.call('HGET', entry_key, 'cls')
  if old_dep then redis.call('ZREM', mk(alias, 'lease:' .. old_dep .. ':' .. old_cls), held) end
  redis.call('ZREM', mk(alias, 'inprog'), held)
  redis.call('DEL', entry_key)
end

-- Foreign call (model fallback): only while this model has no waiters of its own.
local foreign = present(req.foreign)
local wait_keys = {p = mk(alias, 'wait:p'), n = mk(alias, 'wait:n')}
local info_key = mk(alias, 'waitinfo')
for _, c in ipairs({'p', 'n'}) do
  for _, id in ipairs(redis.call('ZRANGE', wait_keys[c], 0, 49)) do
    local raw = redis.call('HGET', info_key, id)
    if not raw or num(cjson.decode(raw).exp, 0) < now then
      redis.call('ZREM', wait_keys[c], id)
      redis.call('HDEL', info_key, id)
    end
  end
end
if foreign and (redis.call('ZCARD', wait_keys.p) + redis.call('ZCARD', wait_keys.n)) > 0 then
  return cjson.encode({r = 'busy', reason = 'waiters'})
end

-- What older waiters set aside, per deployment (plan §3.18 reservation rule).
local reserved = {}
local function reserve_from(list, into)
  into = into or reserved
  for _, id in ipairs(list) do
    if id ~= rid then
      local raw = redis.call('HGET', info_key, id)
      if raw then
        local w = cjson.decode(raw)
        if num(w.exp, 0) >= now then
          for _, dep in ipairs(w.deps) do
            local slot = into[dep] or {r = 0, t = 0, s = 0}
            slot.r = slot.r + num(w.r, 1)
            slot.t = slot.t + num(w.t, 0)
            slot.s = slot.s + 1
            into[dep] = slot
          end
        end
      end
    end
  end
end
local pos = num(req.pos, now * 1000)
if not foreign then
  -- %.17g: plain `..` formats with %.14g and misorders waiters within ~0.1 ms of each other.
  local before = '(' .. string.format('%.17g', pos)
  reserve_from(redis.call('ZRANGEBYSCORE', wait_keys[cls], '-inf', before, 'LIMIT', 0, 200))
end
-- What waiting priority calls need, per deployment: added to a normal call's reservation
-- per candidate (only where that candidate's normal use reached its floor), never to the shared
-- table, so the result no longer depends on the order the candidates are tried in.
local priority_reserved = {}
if cls == 'n' and not foreign then
  reserve_from(redis.call('ZRANGE', wait_keys.p, 0, 199), priority_reserved)
end

local cost_r = num(req.cost_r, 1)
local cost_t = num(req.cost_t, 0)
local best_wait, best_reason = nil, nil
-- A breaker's one probe call is in flight: its answer comes within an attempt, so a call
-- waits for it instead of failing as `model_unavailable` (scenario P7 at 300 %: ten calls
-- that arrived during the probe failed after every region had recovered).
local probe_wait = false
local function note(ms, reason)
  if best_wait == nil or ms < best_wait then best_wait, best_reason = ms, reason end
end

-- Evaluate one candidate: fits (with its new budget states and headroom) or the wait.
local function evaluate(cand)
  local row = rows[cand.key]
  if row == nil then return nil end
  local health = mk(alias, 'health')
  if row.enabled == false or row.account_enabled == false or row.outside_residency or row.blocked
    or redis.call('HEXISTS', health, cand.key .. '|until') == 1 then
    return nil
  end
  local cb = mk(alias, 'cb:' .. cand.key)
  local probe = false
  if redis.call('HGET', cb, 'state') == 'open' then
    if hnum(cb, 'until', 0) > now then return nil end
    local who = redis.call('HGET', cb, 'probe')
    if who and who ~= '' and hnum(cb, 'probe_at', 0) > now - num(row.breaker_probe_secs, 60) * 1000 then
      probe_wait = true
      return nil
    end
    probe = true
  end
  local res = reserved[cand.key] or {r = 0, t = 0, s = 0}
  -- Normal calls also set aside what waiting priority calls need, unless normal use is below
  -- normal_min_share this minute.
  local rkey = mk(alias, 'rate:' .. cand.key)
  local prio = priority_reserved[cand.key]
  if prio then
    local floor = num(model.normal_min_share, 0) * num(row.rpm, 0)
    local used = 0
    if hnum(rkey, 'seen_win', -1) == minute then used = hnum(rkey, 'seen_n', 0) end
    if floor <= 0 or used >= floor then
      res = {r = res.r + prio.r, t = res.t + prio.t, s = res.s + prio.s}
    end
  end
  local states = {}
  local headroom = nil
  local most = nil
  for _, b in ipairs(budgets(alias, row, model, cls, true)) do
    local cost = cost_r
    local R = res.r
    if b[4] == 't' then cost, R = cost_t, res.t end
    local mode = row.rate_window
    local wait, value = room(b[1], b[2], b[3], cost, R, num(row.rate_burst, 0.1), mode)
    if wait > 0 then
      local reason = (b[4] == 'r') and 'rpm' or 'tpm'
      if string.find(b[1], '{a:', 1, true) then reason = 'account'
      elseif string.sub(b[2], 2, 2) == '_' then reason = 'reserve' end
      if R > 0 then reason = 'not_head' end
      return {wait = wait, reason = reason}
    end
    table.insert(states, {b[1], b[2], mode, value})
    local free = avail(b[1], b[2], b[3], num(row.rate_burst, 0.1), mode) - R
    local left = free - cost
    -- Headroom is the share free before this call of what can be taken now: the minute (fixed)
    -- or the bucket (smooth, never more than rate_burst of the minute). Before, not after: a
    -- call larger than the bucket could otherwise never fall back to an idle model.
    local size = b[3]
    if mode ~= 'fixed' then size = math.max(1, math.floor(b[3] * num(row.rate_burst, 0.1))) end
    local frac = free / size
    if headroom == nil or frac < headroom then headroom = frac end
    if b[4] == 'r' and not string.find(b[1], '{a:', 1, true) then most = left end
  end
  -- Concurrency: slots per class, after what older waiters set aside.
  local lp, ln = lease_keys(alias, cand.key)
  redis.call('ZREMRANGEBYSCORE', lp, '-inf', now)
  redis.call('ZREMRANGEBYSCORE', ln, '-inf', now)
  local in_p, in_n = redis.call('ZCARD', lp), redis.call('ZCARD', ln)
  local limit = limit_of(alias, row)
  local share = 1 - num(model.priority_reserve, 0)
  if cls == 'p' then share = 1 - num(model.normal_min_share, 0) end
  local cap = math.floor(share * limit)
  if limit >= 2 and share > 0 then cap = math.max(1, cap) end
  local mine = (cls == 'p') and in_p or in_n
  if in_p + in_n + res.s >= limit or mine >= cap then
    return {wait = num(T.slot_wait_ms, 50) + math.floor(num(req.rand, 0.5) * num(T.slot_jitter_ms, 150)), reason = 'slot'}
  end
  local slot_frac = (limit - in_p - in_n - res.s - 1) / limit
  if headroom == nil or slot_frac < headroom then headroom = slot_frac end
  return {fits = true, states = states, headroom = headroom, most = most or (limit - in_p - in_n),
    probe = probe, limit = limit}
end

-- Sticky session: keep the session's deployment while it fits or would wait only briefly.
local sticky_key = nil
local sticky_dep = nil
if present(req.sticky) then
  sticky_key = mk(alias, 'sticky:' .. req.sticky.hash)
  sticky_dep = redis.call('GET', sticky_key)
end

-- Latency-aware exclusion (plan §3.4): a deployment much slower than the model's median,
-- with enough samples and at least 3 to compare, becomes a last choice (never excluded alone).
local slow = {}
do
  local hs = model.health or {}
  local ratio, min_ms, min_n = num(hs.slow_ratio, 3), num(hs.slow_min_secs, 5) * 1000, num(hs.slow_min_samples, 20)
  local health_key = mk(alias, 'health')
  local values, lat_of = {}, {}
  for _, cand in ipairs(req.candidates) do
    local lat = hnum(health_key, cand.key .. '|lat', nil)
    if lat and hnum(health_key, cand.key .. '|n', 0) >= min_n then
      lat_of[cand.key] = lat
      table.insert(values, lat)
    end
  end
  if #values >= 3 then
    table.sort(values)
    local median = values[math.floor((#values + 1) / 2)]
    for key, lat in pairs(lat_of) do
      if lat > ratio * median and lat > min_ms then slow[key] = true end
    end
  end
end

local fitting = {confirmed = {}, fallback = {}}
local last = {}
local results = {}
local any_usable = false
-- `ordered` (the default) takes the first confirmed candidate that fits, so evaluation stops
-- there: evaluating all of them cost ~12 store reads each (30 deployments: p99 above 2 ms).
local first_fit = req.strategy == 'ordered' and not present(req.sticky)
for _, cand in ipairs(req.candidates) do
  local res = evaluate(cand)
  if res ~= nil then
    any_usable = true
    results[cand.key] = res
    if res.fits then
      if cand.last or slow[cand.key] then table.insert(last, cand) else table.insert(fitting[cand.tier], cand) end
      if first_fit and cand.tier == 'confirmed' and not (cand.last or slow[cand.key]) then break end
    else
      note(res.wait, res.reason)
    end
  end
end
if not any_usable then
  if probe_wait then return cjson.encode({r = 'wait', ms = num(T.probe_wait_ms, 200), reason = 'probe'}) end
  return cjson.encode({r = 'none'})
end

local chosen = nil
if sticky_dep and results[sticky_dep] then
  local s = results[sticky_dep]
  if s.fits then
    for _, cand in ipairs(req.candidates) do if cand.key == sticky_dep then chosen = cand end end
  elseif s.wait <= num(req.sticky.move_ms, 1000) then
    best_wait, best_reason = s.wait, s.reason
    chosen = false
  end
end

local function pick(list)
  if #list == 0 then return nil end
  local strategy = req.strategy
  if strategy == 'round_robin' then
    local rot = mk(alias, 'rot')
    local best, best_seq = nil, nil
    for _, cand in ipairs(list) do
      local seq = hnum(rot, 'rr|' .. cand.key, 0)
      if best_seq == nil or seq < best_seq then best, best_seq = cand, seq end
    end
    return best
  elseif strategy == 'weighted' then
    local rot = mk(alias, 'rot')
    local total, best, best_cw = 0, nil, nil
    for _, cand in ipairs(list) do
      local w = num(rows[cand.key].weight, 1)
      local cw = hnum(rot, 'cw|' .. cand.key, 0) + w
      redis.call('HSET', rot, 'cw|' .. cand.key, tostring(cw))
      total = total + w
      if best_cw == nil or cw > best_cw then best, best_cw = cand, cw end
    end
    redis.call('HSET', rot, 'cw|' .. best.key, tostring(best_cw - total))
    return best
  elseif strategy == 'headroom' then
    local mode = req.pick or 'percent'
    if mode == 'spread' then
      local total = 0
      for _, cand in ipairs(list) do total = total + math.max(0.0001, results[cand.key].headroom) end
      local x = num(req.rand, 0.5) * total
      for _, cand in ipairs(list) do
        x = x - math.max(0.0001, results[cand.key].headroom)
        if x <= 0 then return cand end
      end
      return list[#list]
    end
    local best, best_score = nil, nil
    for _, cand in ipairs(list) do
      local score = (mode == 'max') and results[cand.key].most or results[cand.key].headroom
      if best_score == nil or score > best_score then best, best_score = cand, score end
    end
    return best
  end
  return list[1]
end

if chosen == nil then
  chosen = pick(fitting.confirmed) or pick(fitting.fallback) or pick(last)
end

if foreign and chosen and num(req.foreign.require_pct, 0) > 0 then
  if results[chosen.key].headroom < num(req.foreign.require_pct, 0) / 100 then chosen = nil end
end

if not chosen then
  if foreign then return cjson.encode({r = 'busy', reason = 'headroom'}) end
  -- Refused: register (or refresh) as a waiter, with the deployments it can use.
  local deps = {}
  for _, cand in ipairs(req.candidates) do table.insert(deps, cand.key) end
  redis.call('ZADD', wait_keys[cls], pos, rid)
  redis.call('HSET', info_key, rid, cjson.encode({r = cost_r, t = cost_t, deps = deps,
    exp = now + num(req.waiter_ms, 2000)}))
  return cjson.encode({r = 'wait', ms = math.max(1, math.floor(best_wait or num(T.wait_ms, 100))),
    reason = best_reason or 'slot'})
end

-- Take everything at once.
local row = rows[chosen.key]
local res = results[chosen.key]
for _, s in ipairs(res.states) do write(s[1], s[2], s[3], s[4]) end
local rkey = mk(alias, 'rate:' .. chosen.key)
if hnum(rkey, 'seen_win', -1) ~= minute then
  redis.call('HSET', rkey, 'seen_win', minute, 'seen_calls', 0, 'seen_tokens', 0, 'seen_n', 0)
  redis.call('PEXPIRE', rkey, KEY_MS)
end
redis.call('HINCRBY', rkey, 'seen_calls', cost_r)
redis.call('HINCRBY', rkey, 'seen_tokens', cost_t)
if cls == 'n' then redis.call('HINCRBY', rkey, 'seen_n', cost_r) end
local lp, ln = lease_keys(alias, chosen.key)
local lease_ms = num(req.lease_ms, 60000)
redis.call('ZADD', (cls == 'p') and lp or ln, now + lease_ms, req.owner)
redis.call('ZADD', mk(alias, 'inprog'), now + lease_ms, req.owner)
redis.call('HSET', entry_key, 'owner', req.owner, 'worker', req.worker, 'dep', chosen.key,
  'cls', cls, 'minute', minute, 'cost_r', cost_r, 'cost_t', cost_t, 'account', row.account,
  'started', now, 'request', rid, 'served', alias)
redis.call('PEXPIRE', entry_key, lease_ms * 2)
if res.probe then
  local cb = mk(alias, 'cb:' .. chosen.key)
  redis.call('HSET', cb, 'probe', req.owner, 'probe_at', now)
end
-- Running, by this worker: a fallback grant too, so a redelivery of the call is refused
-- while it runs on the other model (it was granted again and ran twice).
if req.claim or redis.call('EXISTS', job_key) == 1 then
  redis.call('HSET', job_key, 'state', 'running', 'owner', req.worker, 'hb', now)
end
redis.call('ZREM', wait_keys[cls], rid)
redis.call('HDEL', info_key, rid)
if foreign and present(req.foreign.from_alias) then
  redis.call('ZREM', mk(req.foreign.from_alias, 'wait:' .. cls), rid)
  redis.call('HDEL', mk(req.foreign.from_alias, 'waitinfo'), rid)
end
if sticky_key then
  redis.call('SET', sticky_key, chosen.key, 'PX', num(req.sticky.ttl_ms, 1800000))
end
if req.strategy == 'round_robin' then
  local rot = mk(alias, 'rot')
  redis.call('HSET', rot, 'rr|' .. chosen.key, redis.call('HINCRBY', rot, 'rr_seq', 1))
end
return cjson.encode({r = 'granted', dep = chosen.key, minute = minute, limit = res.limit,
  tier = chosen.tier})
"""
)

# Undo or finish one grant: give back budgets, release the slot and the entry.
_SETTLE = r"""
local function settle(alias, owner, rid, refund_r, refund_t, learn, outcome, latency)
  local entry_key = mk(alias, 'inprog:' .. rid)
  if redis.call('HGET', entry_key, 'owner') ~= owner then return 0 end
  local dep = redis.call('HGET', entry_key, 'dep')
  local cls = redis.call('HGET', entry_key, 'cls')
  local granted_minute = hnum(entry_key, 'minute', minute)
  local model, rows = cfg_of(alias)
  local row = rows and rows[dep]
  if row then
    for _, b in ipairs(budgets(alias, row, model, cls, true)) do
      local g = (b[4] == 'r') and refund_r or refund_t
      give(b[1], b[2], b[3], g, row.rate_window, granted_minute)
    end
    local rkey = mk(alias, 'rate:' .. dep)
    if granted_minute == minute and hnum(rkey, 'seen_win', -1) == minute then
      redis.call('HINCRBY', rkey, 'seen_calls', -refund_r)
      redis.call('HINCRBY', rkey, 'seen_tokens', -refund_t)
      if cls == 'n' then redis.call('HINCRBY', rkey, 'seen_n', -refund_r) end
    end
  end
  redis.call('ZREM', mk(alias, 'lease:' .. dep .. ':' .. cls), owner)
  redis.call('ZREM', mk(alias, 'inprog'), owner)
  redis.call('DEL', entry_key)
  if learn and row then
    local ac = mk(alias, 'ac:' .. dep)
    local limit, cap = limit_of(alias, row)
    if outcome == 'throttle' then
      local thr = mk(alias, 'thr:' .. dep .. ':' .. minute)
      redis.call('INCR', thr)
      redis.call('PEXPIRE', thr, KEY_MS)
      if hnum(ac, 'cut_at', 0) < now - num(T.limiter_cut_ms, 1000) then
        redis.call('HSET', ac, 'limit', tostring(math.max(1, math.floor(limit * num(T.limiter_cut_factor, 0.7)))), 'cut_at', now)
      end
    elseif outcome == 'ok' then
      redis.call('HSET', ac, 'limit', tostring(math.min(cap, limit + 1 / math.max(1, limit))))
    end
    local cb = mk(alias, 'cb:' .. dep)
    if outcome == 'ok' then
      redis.call('HSET', cb, 'state', 'closed', 'failures', 0, 'probe', '')
      redis.call('DEL', mk(alias, 'terr:' .. dep))
    elseif outcome == 'fail' then
      -- Recent failures only: one more than 60 s after the last starts the count again (it
      -- counted forever until a success, so 5 failures over hours opened the breaker).
      if hnum(cb, 'fail_at', 0) < now - num(row.breaker_window_secs, 60) * 1000 then redis.call('HSET', cb, 'failures', 0) end
      redis.call('HSET', cb, 'fail_at', now)
      local fails = redis.call('HINCRBY', cb, 'failures', 1)
      local probing = redis.call('HGET', cb, 'probe') == owner
      if fails >= num(row.breaker_threshold, 5) or probing then
        redis.call('HSET', cb, 'state', 'open', 'probe', '',
          'until', now + math.floor(num(row.breaker_cooldown_secs, 15) * 1000))
      end
    end
    if latency and latency >= 0 then
      local health = mk(alias, 'health')
      local old = hnum(health, dep .. '|lat', latency)
      redis.call('HSET', health, dep .. '|lat', tostring(old * 0.8 + latency * 0.2))
      redis.call('HINCRBY', health, dep .. '|n', 1)
    end
  end
  return 1
end
"""

# req: alias, owner, request_id, refund_r, refund_t, outcome (ok|throttle|fail|none), latency_ms.
FINISH = (
    _COMMON
    + _SETTLE
    + r"""
return settle(req.alias, req.owner, req.request_id, num(req.refund_r, 0), num(req.refund_t, 0),
  true, req.outcome, num(req.latency_ms, -1))
"""
)

# req: alias, owner, request_id. Give everything back; no limiter or breaker learning.
UNDO = (
    _COMMON
    + _SETTLE
    + r"""
local entry_key = mk(req.alias, 'inprog:' .. req.request_id)
if redis.call('HGET', entry_key, 'owner') ~= req.owner then return 0 end
return settle(req.alias, req.owner, req.request_id, hnum(entry_key, 'cost_r', 0),
  hnum(entry_key, 'cost_t', 0), false, 'none', -1)
"""
)

# req: {lease_ms, worker, items: [{alias, owner, request_id}]}. Heartbeat of held grants.
RENEW = (
    _COMMON
    + r"""
local renewed = 0
for _, item in ipairs(req.items) do
  local entry_key = mk(item.alias, 'inprog:' .. item.request_id)
  if redis.call('HGET', entry_key, 'owner') == item.owner then
    local until_ = now + num(req.lease_ms, 60000)
    local dep = redis.call('HGET', entry_key, 'dep')
    local cls = redis.call('HGET', entry_key, 'cls')
    -- No XX: a lease that expired during a missed beat comes back (the owner check above
    -- guards it), so a still-running call holds its slot again.
    redis.call('ZADD', mk(item.alias, 'lease:' .. dep .. ':' .. cls), until_, item.owner)
    redis.call('ZADD', mk(item.alias, 'inprog'), 'XX', until_, item.owner)
    redis.call('PEXPIRE', entry_key, num(req.lease_ms, 60000) * 2)
    local job = P .. ':{r:' .. item.request_id .. '}:job'
    if redis.call('EXISTS', job) == 1 then redis.call('HSET', job, 'hb', now) end
    renewed = renewed + 1
  end
end
return renewed
"""
)

# req: {lease_ms, items: [{alias, owner, request_id, dep, cls, cost_r, cost_t}]}. After a
# store reset, put back the slots and entries of calls still running (plan T10).
REREGISTER = (
    _COMMON
    + r"""
local done = 0
for _, item in ipairs(req.items) do
  local until_ = now + num(req.lease_ms, 60000)
  redis.call('ZADD', mk(item.alias, 'lease:' .. item.dep .. ':' .. item.cls), until_, item.owner)
  redis.call('ZADD', mk(item.alias, 'inprog'), until_, item.owner)
  local entry_key = mk(item.alias, 'inprog:' .. item.request_id)
  local worker = string.match(item.owner, '^.*|([^|]*)|[^|]*$') or ''
  redis.call('HSET', entry_key, 'owner', item.owner, 'worker', worker, 'dep', item.dep,
    'cls', item.cls, 'minute', minute, 'cost_r', item.cost_r, 'cost_t', item.cost_t,
    'started', now, 'request', item.request_id, 'served', item.alias)
  redis.call('PEXPIRE', entry_key, num(req.lease_ms, 60000) * 2)
  done = done + 1
end
return done
"""
)

# req: {alias}. Remove in-progress entries whose owner stopped renewing (the dead worker's
# estimate stays charged for that minute; plan §3.18). Reply: how many were removed.
SWEEP = (
    _COMMON
    + r"""
local removed = 0
local inprog = mk(req.alias, 'inprog')
for _, owner in ipairs(redis.call('ZRANGEBYSCORE', inprog, '-inf', now, 'LIMIT', 0, 200)) do
  redis.call('ZREM', inprog, owner)
  local rid = string.match(owner, '^(.*)|[^|]*|[^|]*$')
  if rid then
    local entry_key = mk(req.alias, 'inprog:' .. rid)
    if redis.call('HGET', entry_key, 'owner') == owner then
      local dep = redis.call('HGET', entry_key, 'dep')
      local cls = redis.call('HGET', entry_key, 'cls')
      if dep then redis.call('ZREM', mk(req.alias, 'lease:' .. dep .. ':' .. cls), owner) end
      redis.call('DEL', entry_key)
    end
  end
  removed = removed + 1
end
return removed
"""
)
