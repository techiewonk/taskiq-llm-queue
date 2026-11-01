# Valkey Streams queue backend: plan

Status: **done** (2026-11-01 → 2026-11-02), shipped in 0.2.0. Owner decision: build "Option B", a Valkey Streams
queue usable in place of Amazon SQS, then test it with the same suites as SQS.

## 1. Goal and non-goals

**Goal.** `queue_backend="valkey"` (env `LLMQ_QUEUE_BACKEND=valkey`) runs every queue (model lanes
and jobs) on Valkey Streams instead of SQS, with the same delivery guarantees the library gives on
SQS: at-least-once delivery, visibility timeout with heartbeat extension, release with a delay,
redelivery after a pod dies, a dead-letter queue after `max_receives`, delays, expiry, strict
priority across lanes, capacity-aware receive, batched send / ack. Everything above the transport
(claims, limiter, rate budgets, results, routing, fallback, cancel, drain) is unchanged.

**Non-goals (v1).** FIFO queues (`.fifo`, group / dedup ids): rejected at boot with a clear error.
Cross-region replication. A web console: the CLI covers create / stats / peek / purge / redrive.

## 2. Design: an SQS-compatible client over Valkey, not a second broker

`HybridSQSBroker` is ~940 lines of tested behaviour (priority scheduler, receive budget and gates,
heartbeat, batched ack, release, screening, metadata labels, drain). Everything it needs from SQS
goes through seven client calls:

| SQS call | Used by |
|---|---|
| `get_queue_url`, `get_queue_attributes(VisibilityTimeout)`, `create_queue` | startup |
| `receive_message(MaxNumberOfMessages, WaitTimeSeconds, attributes)` | pollers |
| `send_message`, `send_message_batch` | `kick` (rt single, bg / bulk batched) |
| `delete_message(_batch)` | ack, expired messages |
| `change_message_visibility(_batch)` | heartbeat, release, reject |

So the backend is **`ValkeyQueueClient`**: an async object with exactly these methods, same request
and response shapes, and the same error codes (`QueueDoesNotExist`, `ReceiptHandleIsInvalid`, ...)
raised as botocore `ClientError`, so `classify_error` / the Acker / failover work unchanged. The
broker gets a `client_factory` hook; `build_brokers` passes the Valkey one when the backend is
`valkey`. One code path is tested twice instead of two code paths once.

## 3. Data model (one hash tag per queue: cluster-safe)

For queue `<q>` and prefix `P` (`LLMQ_KEY_PREFIX`), all keys share the tag `{P:q:<q>}`:

| Key | Type | Holds |
|---|---|---|
| `{..}:s` | stream | one entry per message: `body`, `attrs` (JSON message attributes), `sent_ms` |
| `{..}:vis` | zset | in-flight entry id → epoch ms when it becomes visible again |
| `{..}:rc` | hash | entry id → receive count (also the receipt-handle generation) |
| `{..}:delay` | zset | delayed payload (JSON) → epoch ms when it may be delivered |
| `{..}:cfg` | hash | `visibility_secs`, `retention_secs`, `max_receives`, `dlq` (queue name), `created_ms` |
| `{..}:wake` | pub/sub channel | published on send; idle pollers wake without busy polling |

The DLQ is an ordinary queue (`<q>-dlq`); moving a message there is done by the receive script, so
its keys must be in the same slot: the DLQ of `<q>` uses the tag of `<q>` (`{P:q:<q>}:dlq:*`).

Consumer group `g` on every stream; every pod reads as consumer `c` (consumer identity is not used
for ownership: visibility lives in `:vis`, so any pod can reclaim, as with SQS).

**Receipt handle** = `<entry id>.<receive count>`. Every redelivery increments `:rc`, so a handle
from an earlier receive is stale and ack / visibility calls with it fail `ReceiptHandleIsInvalid`,
exactly like SQS. The heartbeat's lost-message detection (DS6) depends on this.

## 4. Operations (each one Lua script, atomic)

| Op | Script |
|---|---|
| send | `XADD :s MINID ~ <now-retention>` (retention trim), `PUBLISH wake`; with delay: `ZADD :delay` |
| receive(n) | 1) move due `:delay` items into `:s`; 2) take up to n entries from `:vis` with score ≤ now (timed-out or released): bump `:rc`; if > `max_receives` move to the DLQ stream (or drop when no DLQ) else re-set score = now + visibility and return; 3) the rest from `XREADGROUP g c COUNT k STREAMS :s >`: `:rc`=1, `:vis` = now + visibility. Returns entries with `MessageId`, `ReceiptHandle`, `Body`, `ApproximateReceiveCount`, `SentTimestamp`, `MessageAttributes` |
| delete(handle) | check generation; `XACK`, `XDEL`, `ZREM :vis`, `HDEL :rc` |
| change_visibility(handle, s) | check generation and that the entry is in flight; `ZADD :vis now+s` (s=0: visible now) |
| stats | `XLEN`, `ZCARD :vis`, count of `:vis` scores ≤ now, `ZCARD :delay` → visible / in flight / delayed |
| purge / redrive | CLI / test helpers: delete keys; move DLQ entries back to the source queue |

**Long poll.** Lua cannot block, so `receive_message(WaitTimeSeconds=w)` runs the receive script;
if empty it waits on the queue's local wake event (one pub/sub connection per client, `PUBLISH` on
send) with timeout `min(remaining, 1 s)`, then retries until `w` elapses. Idle cost per pod:
≤ 1 script call per queue per second, versus SQS's one long poll per queue per 20 s. Timed-out
in-flight messages and due delays are found by that ≤ 1 s re-check.

## 5. Configuration

| Setting | Default | Meaning |
|---|---|---|
| `queue_backend` (`LLMQ_QUEUE_BACKEND`) | `sqs` | `sqs` or `valkey` |
| `queue_redis_url` (`LLMQ_QUEUE_REDIS_URL`) | unset = the state Valkey | a separate Valkey for queues (recommended in prod: limits the blast radius) |
| queue attributes | visibility 30 s, retention 4 days, max receives 10 | set at create time (`llm-queue queues create`), like SQS attributes |

Queue names stay the same (`mofsl-<env>-researcha-llm-gpt20b-rt`, ...), so routing, config and the
runbook do not change. FIFO names are refused when `queue_backend=valkey`.

## 6. Failure behaviour (differs from SQS; document in the runbook)

* Valkey down: queue, limiter and results are down together. Callers run `direct` under the
  per-pod cap (existing Valkey-down path); queued work waits in Valkey until it returns.
* Valkey failover (async replica): messages acked or sent in the last moment before failover can
  reappear or be lost. Handlers are idempotent (claims), so a reappearance is safe; a loss shows as
  a caller timeout. Needs AOF `everysec` + Multi-AZ (OQ6).
* Memory: the backlog lives in RAM. Budget: messages × (body ≤ 200 KB, params over that go by
  reference) + DLQ. Retention trimming keeps it bounded; alarms on `XLEN`.

### 6.1 Deployment decision (2026-11-01, owner): self-hosted Valkey server with persistence

Required server settings for queues (a queue message must never be evicted or lost on restart):

| Setting | Value | Why |
|---|---|---|
| `appendonly` | `yes` | messages survive a restart |
| `appendfsync` | `everysec` | at most ~1 s of sends / acks lost on a crash; `always` if that is not acceptable (2.6× slower receive, see measurements below) |
| `aof-use-rdb-preamble` + `save` snapshots | `yes` + e.g. `save 900 1 300 100` | fast restart, backups |
| `maxmemory-policy` | **`noeviction`** | any eviction policy can silently drop queued messages or claims; with `noeviction` a full server rejects writes (calls fail visibly and run direct) |
| `maxmemory` | ≥ 2 × the sizing below | headroom for AOF rewrite and spikes |
| replica + Sentinel (or Valkey cluster) | recommended | restart / failover without losing the queue; same caveat as §6 for async replication |

Sizing per queued message ≈ body + **~600 B** of stream / zset / hash overhead (measured: 3,619 B
per 3,015 B message over a 10K backlog, so ~36 MB per 10K typical messages). Alarm on
`llmq_queue_depth` and `used_memory`.

Measured (V7, `tools/valkey_queue_bench.py`, Docker on a Windows laptop, one client):

| `appendfsync` | sends / s | receive + delete / s | lost after `stop -t 0` right after 2,000 sends |
|---|---|---|---|
| `always` | 2,020 | 578 | 0 |
| `everysec` | 1,643 | 1,513 | 0 |

`everysec` is 2.6× faster on receive + delete and lost nothing in the kill test (its window is
the last second of writes). Use `everysec` unless losing up to one second of queued messages on a
host crash is unacceptable; `always` then costs throughput (measure on the real server).

## 7. Work plan

| # | Step | Test gate |
|---|---|---|
| V1 | `ValkeyQueueClient` (scripts, handles, long poll, errors) + `create_queue` / stats / purge / redrive helpers | new unit/integration suite against the local Valkey: every op, stale handles, timeouts, DLQ, delay, retention |
| V2 | Contract parity: the existing SQS client contract tests (`tests/integration` broker suite, ElasticMQ) run against both backends | same assertions pass on `sqs` and `valkey` |
| V3 | `client_factory` in `HybridSQSBroker`; `queue_backend` / `queue_redis_url` in `QueueConfig`; `build_brokers` wiring; FIFO refused | broker integration suite parametrised by backend |
| V4 | CLI: `queues` (create / list / stats / peek / purge / redrive) for valkey; metrics (`XLEN` gauges) | CLI tests |
| V5 | Scenario harness `SCN_QUEUE_BACKEND=valkey` (stack helpers: ensure / counts / purge / peek / redrive) | all 69 scenarios on valkey, on a quiet stack |
| V6 | Samples (`_local.py` backend switch), README / ARCHITECTURE / runbook, CHANGELOG | samples 01-17 on valkey |
| V7 | Load and failure: S27, S35 family, Valkey restart mid-run, memory per 10K backlog | numbers recorded here |

## 8. Progress

| Step | State | Notes |
|---|---|---|
| V1 | done 2026-11-01 | `broker/valkey_queue.py`; `tests/integration/broker/test_valkey_queue_int.py` 11/11 |
| V2 | done 2026-11-01 | broker suite parametrised: 18/18 on ElasticMQ, 17/18 on Valkey (FIFO skipped by design) |
| V3 | done 2026-11-01 | `queue_backend`, `queue_redis_url`, `create_queue_redis`, broker `client_factory`, `valkey_client_factory` (FIFO refused at boot) |
| V4 | done 2026-11-01 | CLI `queues create / list / redrive / purge` and `job send` on valkey (`test_cli_int.py`); gauges `llmq_queue_depth`, `llmq_queue_oldest_seconds` (`depth()` tested) |
| V5 | done 2026-11-02 | Full run (2026-11-01): 62/69 pass. Fixed and re-run on `valkey-scn-persist`: R12 ✅ (CLI env lacked the backend), S23 ✅ (the releasing pod re-took the job in step with its own deadline until the DLQ: release deadlines and re-checks are now jittered), S50 ✅ (restart without persistence wiped messages: valkey runs use the persistent twin, AOF `always` + `noeviction`, §6.1), S21 ✅ (back on the queue 0.75 s after Valkey returned: queue failover windows now end with Valkey-down mode when the queues live on that Valkey; the client also re-creates a queue whose keys vanished), R03 ✅ 3/3 (the one failure was CPU contention from checks run alongside), S33v ✅ 3/3 (idle ≈ 2 XREADGROUP per queue per second, rt queue wait 16-50 ms), S20 / S33 skipped (SQS-only) |
| V6 | done 2026-11-02 | README "Queue backend", samples README settings table, 12_cli.md; samples 01-17: 16/16 exit 0 on valkey and 16/16 on sqs |
| V7 | done 2026-11-02 | memory ~600 B overhead per message; `always` vs `everysec` throughput and kill test (§6.1); restart / outage behaviour: S21, S50 |
