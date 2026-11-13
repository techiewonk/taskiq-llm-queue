# Changelog

All notable changes are listed here. The project uses semantic versioning; while it is 0.x,
a minor release may break the public API without notice (see STABILITY.md).

## [0.3.0] - unreleased

A clean break from 0.2 (plan E101): nothing is carried over. Empty the 0.2 queues, deploy 0.3.

### Added

- Docs site (Zensical on GitHub Pages): the README, architecture, samples, an API reference
  from the docstrings, changelog, stability and contributing; built with `--strict` on every
  pull request, deployed on a release or a manual run (`.github/workflows/docs.yml`).
- `Tuning` (`StoreOptions`, `BedrockOptions`, `EngineTimings`, `RegistryOptions`), passed as
  `LLMQueue.from_env(tuning=...)`: store client retries and timeouts, Bedrock client timeouts
  and pool size, the assumed-role session length, idle receive pacing, the result recheck, the
  health probe, the stream read block, registry history size and edit retries. They were
  constants; each keeps its value as the default. Bad values and bad combinations stop
  start-up with every problem listed. `ClientPool` takes `options=BedrockOptions(...)` instead
  of `read_timeout_secs` / `connect_timeout_secs` / `max_pool_connections`. The admission
  scripts' pacing is in `EngineTimings` too (probe, slot and default waits, the limiter's cut,
  budget key lifetime), and the breaker's failure window and probe hold are registry settings
  (`breaker_window_secs`, `breaker_probe_secs`, 60 s each); the scripts had them as numbers.
- One registry document (`LLMQ_REGISTRY`): accounts (AWS by pod identity, OpenAI-compatible
  endpoints), models with deployments per account and region (confirmed and fallback), every
  setting at four levels with types, ranges and defaults; validated with every problem listed.
- Registry in the store: publish by version, compare-and-set live edits with protected
  fields, audit as path diffs, rollback, lenient read, restore after a store reset.
- Admission in one store call: pick a deployment (ordered, round robin, weighted, headroom,
  custom) and take RPM / TPM, the account budget, a slot per class and an in-progress entry;
  waiters keep their order (reservation rule); priority calls go first.
- Queues on Valkey / Redis streams: a pair per model (normal, priority), return in place,
  failures and dead letters, a waiting limit, the store's clock everywhere.
- Fallback regions and fallback models by cause; health with auto-disable, probes and safeguards;
  credentials per process; kill switch, `queue_pct`, `store_down` modes; cancel; caller gone.
- Token accounting (estimate, corrections, refunds by outcome), inference defaults and limits
  per provider, embeddings batched per model, large results in S3.
- Status, alerts, metrics (CloudWatch EMF, Prometheus), one `llmq_call_finished` record per call,
  HTTP routes, the new `llm-queue` CLI, jobs on the stream queue.
- Assume-role accounts (`role_arn`, `external_id_ref`): STS AssumeRole per account, sessions
  named `llmq-<env>-<pod>`, refreshed before expiry; an STS failure is a per-process
  credential failure. `LLMQ_STS_ENDPOINT_URL` for tests.
- Latency-aware exclusion: a deployment much slower than the model's median (`health`
  settings) becomes a last choice.
- Streaming to the caller: `LLMQueue.converse_stream` / `chat_stream` yield provider events
  through a chunk stream in the state store (`stream_flush_ms`, `stream_max_bytes`,
  `stream_idle_secs`); a cut mid-stream raises `CallFailed` with `partial_text`.
- Runnable usage samples (`samples/`, with where to use each) and `scripts/samples.py` to run
  them on fakellm or, with `--live`, on real Bedrock; `check.py samples` runs the fakellm pass.
- `LLMQueue.close_sync()` for processes that only make sync calls.
- `LLMQueue.pause_receiving()` for a preStop hook: readiness fails and no new queued calls or
  jobs start; running work finishes. `stop(drain_secs=)` bounds the whole shutdown with one
  budget (jobs first, then running calls), closes every store and is safe to call twice.
- `http_router(dependencies=, allow_writes=, actor=)`: `POST /registry` and
  `POST /registry/block` behind `allow_writes`; `actor(request)` names who made the edit
  (required with writes). Sample `09_http_router.py`.
- Store failover handling (`engine/store_client.py`): `READONLY` and dropped connections
  reconnect and retry, connect timeout 5 s, a blocking pool (`LLMQ_STORE_MAX_CONNECTIONS`,
  default 200); readiness probes the store with a bounded write and reports `read_only`.
  `scripts/failover.py` drills a Valkey failover, a lagging DNS name and a node restart; the
  `store` step of `check.py` runs it. `valkey://` / `valkeys://` URLs are accepted.
- `scripts/samples.py --store env`: the samples on the env file's store (ElastiCache, TLS).
- `scripts/gates.py`: the plan's load and chaos gates (B17, B19, B22, B23, B33, B34, B56 / B93,
  B91, B92, B94, B95, B96, B97) on fakellm with worker processes as pods, scaled to minutes;
  each prints its numbers and the plan's pass mark.
- `LLMQ_QUEUE_WAIT_REPLICAS` / `LLMQ_QUEUE_WAIT_MS` (a send waits for replicas; a wait that
  fails is a failed send), `LLMQ_QUEUE_FAILOVER_SECS` (a separate queue store's errors are
  retried this long before calls run direct; before, the first error did it) and
  `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` (a model's `debug_capture` keeps prompt text only with it;
  refused in prod): planned settings that were never built.
- `scripts/load.py` (`check.py load`): 2,000 calls over three workers (all succeed, none left
  held), and a burst far over an RPM budget (served within the bucket's bound, the rest
  `rate_limited`). A test kills a worker mid-call and another worker finishes it (B13 / B30 /
  B71).
- `UnknownModel` (a `ConfigError` and a `KeyError`) for a model the registry does not have.
- `MissingExtra` (a `ConfigError` and an `ImportError`) for every optional feature whose extra
  is not installed (`http`, `metrics`, `openai`, LangChain), naming the `pip install` to run.
  The `register_*` hooks all validate their arguments and raise `ConfigError`.
- `from_env(secrets=)` reads store passwords from a secret on every new connection
  (rotation); `LLMQ_MODE`, `LLMQ_ENVIRONMENT`, `LLMQ_ID_HASH_SALT`, `LLMQ_CFG_RELOAD_SECS`.

### Fixed (found by the samples on real Bedrock, the failover drill and the 0.3.0 review)

- `require_headroom` model fallback never fired with the default `smooth` rate window:
  headroom was measured against the minute's limit, so it never exceeded `rate_burst` (10%).
  It is now the free share of the bucket before the call, so a call larger than the bucket
  can still fall back to an idle model.
- After a store failover every call failed with `READONLY` until the process restarted
  (pooled connections stayed on the demoted primary); readiness still said `ok`.
- A store error or refusal (OOM, MISCONF), or a bug, during admission, a hold or a run left
  the call held: the heartbeat hid its message for good and the caller got
  `deadline_exceeded`; a grant could leak a slot for the life of the process. Calls are now
  released (grants back, message returned); a store blip after the provider answered no
  longer throws the answer away. Held calls are tracked per delivery, not per request id.
- Refunds were computed from deployment-level params while admission charged model-level
  ones: a larger account `max_tokens` refunded more than was taken and TPM stopped holding.
- Missing admission numbers (`no_config`) left a call held for good; they are restored or
  the call fails with `queue_unavailable`.
- `stop()` returned waiting messages that the heartbeat then hid again for the whole drain.
- The heartbeat beat slower than a short `visibility_secs` and one bad message skipped the
  whole beat; a job's keep-visible task died on the first store error (the job ran twice).
- A failed chunk write left a sequence gap (`worker_lost`) and a stream key with no TTL.
- An expired lease was never restored by RENEW; waiter positions lost precision in Lua.
- `LLMQ_ID_HASH_SALT` was required but unused: user and session ids are now an HMAC with it
  (status, sticky keys, logs, `routing.allow_users`, which now takes hashes only).
- AssumeRole failed for worker ids with `:` (session names are now cleaned).
- The CLI read queues from the state store when `LLMQ_QUEUE_STORE_URL` was set, and printed
  tracebacks for unknown models.
- `start()` twice doubled every loop; threads making their first sync call together built
  several private engines; a sync call running at `close_sync()` never returned; stores and
  forgotten provider clients were never closed; `embed_sync` reported the raw model id.
- Settings: TLS was enforced only for `LLMQ_ENVIRONMENT=prod` exactly and could be turned off
  with `ssl_cert_reqs=none`; it now applies outside dev / local / test / ci, as does the salt.
  `LLMQ_METRICS_SINK` typos and non-finite numbers are refused; the store URL is no longer in
  the settings' repr; `LLMQ_CFG_RELOAD_SECS` was ignored.
- Live edits under `safe` can no longer turn on `debug_capture` (prompt text in the store).
- Blocking an unknown region or account, or neither, is refused (it blocked nothing).
- `/health/ready` took 41 s with a blackholed store; each probe is now bounded to 3 s (above a
  cold TLS connect, which took 1.2 s to a remote ElastiCache and read as `down` under 1 s).
- An embedding batch that failed left the others running; errors are `CallFailed`.
- A store error after a call was sent (confirm, or reading its result) ran it a second time on
  the `store_down` path (once outside the budget); the caller now waits for the store.
- A fallback ask could start a call that was already cancelled or answered elsewhere.
- Admission: an overrun charged after the minute turned was dropped (fixed window); rate keys
  written by a refund, the per-minute counters and RENEW's heartbeat could lose their TTL; the
  learned concurrency limit, a fraction, let a deployment run one call over.
- Admission: models sharing an account budget with different `rate_window` / `rate_burst`
  enforced it twice (4 shared RPM granted 8); such a registry is now refused. Whether a normal
  call reserved room for waiting priority calls depended on the order of its candidates.
  A redelivered message was granted again while the call ran on a fallback model or between
  its retries (it ran twice); a call running elsewhere is now a duplicate.
- Cancelling one of two waiters on the same request id in one process cancelled the call for
  both. The two also shared one wake-up that the first to leave removed (or each replaced
  with its own), so the other slept until the last read at its deadline.
- After a store outage, workers dropped waiting calls as `caller_gone` because the callers'
  liveness keys had expired while nobody could renew them (B95: 429 of 500 dropped after a
  45 s block). A worker that saw the store fail treats callers as alive for a caller TTL and
  a heartbeat after; a failed heartbeat retries in 1 s. A failed receive now counts as seeing
  the store fail too: a 20 s block that fell between two heartbeats went unseen, and the
  one-hour soak still got 9 `cancelled` calls.
- After a store block, two calls stayed in progress for 50 minutes in the one-hour soak: the
  heartbeat renewed the grants of any call still held, for good. A call held a lease past its
  deadline is now logged (`held_call_stuck`), its task cancelled, its grants given back and
  its message returned. The store client also gets a 15 s read timeout (`socket_timeout`;
  there was none), so a reply that never comes fails the command instead of hanging its
  task (`Tuning.store.read_timeout_secs`; start-up refuses a `LLMQ_QUEUE_WAIT_MS` that
  would not fit under it). A read timeout is not retried: redis-py's `Retry` retries
  `TimeoutError` by default (`retry_on_error` only adds to it), so a half-open store hung each
  command 4 x 15 s and a timed-out Lua write could run twice.
- Found by the fault scenarios (`scripts/scenarios.py`):
  - Half-open store (S5): workers dropped waiting calls as `caller_gone` the moment the store
    came back, because the receive loop swallowed its store error and the callers' liveness
    keys had lapsed. A worker now also judges from its own heartbeat: late, or just back from
    a gap, means the store was out of reach for it too.
  - Credentials refused in one region (P5) marked the whole account bad, in every region.
    They are now judged per account and region (an opt-in region such as ap-south-2 can refuse
    a token the others accept).
  - With one region hanging or missing the model (P3, P4), moving a call to the healthy region
    spent the retry budget, and calls failed as `retry_budget_exhausted`. A move to a
    deployment the call has not tried is now free; retries after every deployment failed
    still spend it.
  - Admin operations (`cancel`, `status`, `request_status`, `send_job`, and the registry's
    `set` / `edit` / `block` / `rollback` / `publish` / `log` / `versions`) let redis-py's
    `ConnectionError` reach the app while the store was down (X2: a live edit during a
    store block). They now raise `StoreUnavailable` (new: an `LLMQueueError` and a
    `ConnectionError`).
  - Every region failing, then recovering (P7 at 300 %): while a breaker's one probe call ran,
    other calls failed at once as `model_unavailable`. They now wait for the probe's answer.
  - A call dropped because its caller looked gone (`caller_gone`) is logged as a warning.
  - A store over `maxmemory` (S9) raised redis-py's `OutOfMemoryError` to the app; it is now
    `CallFailed` `queue_unavailable` ("out of memory"). The old check looked for "OOM" in the
    message, which redis-py strips.
- A call could answer after its own deadline: the `pause` store_down rule waited a whole new
  timeout from the moment the store failed, then re-ran the call with a new margin (the soak:
  135 of 252,000 calls late, all in 20 s store blocks). One deadline, timeout plus
  `caller_margin_secs`, now caps every call whatever path it takes.
- Bulk embeddings held every result (~2 MB for 96 Cohere vectors) for 30 s after it was read:
  587 MB at Cohere's rate (B97). A library id's result is now deleted on its first read
  (`result_dedupe_secs` 0 by default; app ids keep `result_ttl_secs`): 31 MB peak.
- A send that no replica acknowledged (`queue_wait_replicas`) now runs the call direct under
  the same record (B23 had 837 failed calls with the replica gone); a record made by a
  worker's claim no longer reads as a conflicting id.
- A call that waited past `max_wait_secs` unreceived in the queue was granted on arrival with
  no time left; a retry backoff could sleep past the call's deadline. The caller gave up and
  was told nothing (load drill, 1 run in 6). Such calls are now refused as `rate_limited`, and
  a backoff never outlasts the deadline.
- The circuit breaker counted failures until the next success, so failures spread over hours
  opened it; a failure more than 60 s after the last now starts the count again. A throttle
  after a 5xx slept the 5xx's backoff again; a throttle now moves on at once.
- A pipelined send with `queue_wait_replicas` failed with `NOSCRIPT` when a failover moved it
  to a node without the script cached (B23); it is now run once more, which reloads it.
- An attempt cut by the caller's own deadline (late grant under a rate limit) counted as a
  provider failure: five opened the breaker and every other call got `model_unavailable`
  (found by `scripts/load.py`). Such cuts no longer count toward the breaker or health.
- Registry: an account's `api_key_ref` / `external_id_ref` may not name the process's own
  secrets (store password, salt, AWS keys), and `base_url` may not be a link-local address
  (instance metadata). STS errors no longer carry role ARNs into replies and logs.

### Performance (measured 2026-11-13, Python 3.12)

- botocore's local parameter check is off (Bedrock validates): 8.5 ms -> 3.5 ms CPU per
  attempt for a 218 KB request.
- The request is no longer deep-copied per attempt (1.86 ms at 217 KB), and the charged
  estimate is computed once per held call (it was rebuilt three times).
- `hiredis` is a dependency (the `speedups` extra is gone): Lua array replies and stream reads
  parse 39-75x faster.
- Envelope and reply encoding skip `model_dump` (342 -> 57 us per send at 217 KB); results
  decode with orjson + validate (nested replies 2-3x faster).
- Ids are UUIDv7 from `uuid-utils` (time-ordered; 0.49 us vs 2.4 us for `uuid4().hex`).
- EMF metric lines use orjson (0.8 us vs 6.2 us); loggers are cached after first use.
- A caller no longer reads the result right after sending (it cannot be there): one store
  round trip less per call. Waiting callers no longer poll for their result: the wake-up is
  published with the result, a re-subscribe wakes every waiter, and one script per process
  every 5 s finds any missed result. With 2,000 calls waiting, store calls went from 1,976/s
  to 499/s, the same as with a few hundred (B56).
- `status()` reads each model in one script: 20 models x 30 deployments took 6.5 s, now
  47 ms (B91). PICK_TAKE stops at the first fitting candidate for `ordered` (the default):
  script p99 1.0 ms with 30 deployments per model (B91 asks for 2 ms).
- A worker's receive loop is woken when its held call is granted or settled, and a class
  that waits on a held call is not counted as idle (it slept up to 200 ms per grant).
- A send rings its queue's doorbell (a list token, one per call) and one idle worker blocked
  on it wakes at once: the queue path adds 0-15 ms at p95 over a direct call (B34), and the
  idle poll slows to 1 s. (A pub/sub wake-up woke every process: store work per call grew
  with them, B22.)
- Workers build their Bedrock clients at start: a fresh worker's first call took 1,032 ms,
  now ~140 ms (B92). Idle receive loops slow from 50 ms to 200 ms after a second with
  nothing (about 4x less idle store load; a busy loop never waits).
- LangChain chat adapters with only `model_alias` now look the model id up from the started
  queue, as documented; before, they needed `llmq=` or `model_id=`.

### Removed

- `limiter_initial` (published but never read: a new deployment starts at its cap).
- Registry settings `retry_attempts`, `throttle_retry_max_secs`, `hold_max_secs`,
  `output_reserve` and `request_status`, and envelope fields `params_ref` / `est_tokens`:
  validated but never read (a message with `params_ref` would have run with empty params). A
  document that sets them is now refused with the problem named.
- The host deny list (`providers.hosts`, `ClientPool(denied_hosts=)`): never filled in 0.3;
  `LLMQ_BASE_URL_ALLOWED_HOSTS` is the control.
- taskiq, the SQS broker, lanes, targets, `QueueConfig`, `LLMQ_MODELS`, `LLMQ_SETTINGS`,
  `LLMQ_FALLBACK_CHAINS`, `LLMQ_ROUTING_*`, `LLMQ_WORKER_SLOTS`, pod-count logic and every 0.2
  module, test, sample and scenario harness.

### Changed

- Dependencies are bounded (`aiobotocore<4`, `redis<9`, `structlog<27`); the `http` extra
  needs `fastapi>=0.115.3` (Starlette with the multipart DoS fixes).
- The local stack is Valkey, Redis and fakellm (the SQS service and scenario profiles are gone).
- `llm-queue registry publish --from-audit N` is `--from-version N` (it takes a history version).

## [0.2.1] - 2026-11-02

Re-release of 0.2.0 with no code changes, published by `publish.yml` (PyPI trusted publishing)
so the files carry provenance attestations. 0.2.0 was uploaded by hand without them and is yanked;
use 0.2.1.

## [0.2.0] - 2026-11-02

### Changed
- Development status Alpha. `llm-queue queues` takes subcommands (`list` default, `create`, `redrive`, `purge`); `queues`, `status` and `job send` use the configured queue backend. Job messages carry their name in the `llmq_job` label (`send_job` and `llm-queue job send`); older messages still match by args.
- Scenario harness: `SCN_QUEUE_BACKEND=valkey` runs every scenario on Valkey Streams against `valkey-scn-persist` (AOF `always`, `noeviction`); S20 and S33 are SQS-only, S33v Valkey-only.

### Verified
- 869 tests (unit, integration and every scenario on SQS): 866 pass, 2 skip by design; S27 at its full 450K-call default exceeds what one laptop load generator offers (196K in 30 min) and passes at the gate load (6K/min × 3 min, rt queue wait p95 1.1 s). All scenarios pass on Valkey; samples 01-17 exit 0 on both backends; real Bedrock: S01, S03, S04, S05, S16, S17, S19, S22, S26, S28, S29, S40, S41 pass.

### Fixed
- Jobs whose payload was offloaded to S3 were never run: the job filter read the job name from the args, which offload empties, so the message was released every 30 s until the DLQ. Jobs now carry the name in the `llmq_job` label (older messages still match by args). Scenario S24.
- The first sync call stalled the caller's event loop 0.2-0.6 s: every new `ProviderRegistry` re-scanned all installed packages' entry points, holding the GIL. The scan now runs once per process. The sync runner's session also reuses the main session's parsed botocore data. A first burst of sync calls on real AWS still stalls the loop 0.1-1.8 s once per pod while clients and HTTPS connections are built (S22 reports it as `cold_burst_lag_ms`); warm bursts stay under 60 ms.
- Tracing: `QueuedChatBedrockConverse` and `QueuedChatOpenAI` now default `name` to the parent class, so Langfuse / LangSmith generations keep the name `ChatBedrockConverse` / `ChatOpenAI`. After a fallback, `llm_output["model_name"]` is the Bedrock id that served the call, so tracers show and price the right model.

### Added
- Valkey Streams queue backend: `queue_backend="valkey"` (`LLMQ_QUEUE_BACKEND`), optional `queue_redis_url` (`LLMQ_QUEUE_REDIS_URL`). `ValkeyQueueClient` (`broker/valkey_queue.py`) serves the SQS calls `HybridSQSBroker` makes (visibility, receipt-handle generations, delay, DLQ after `maxReceiveCount`, long polls woken by pub/sub), so the broker and everything above it are unchanged; `HybridSQSBroker(client_factory=...)` is the hook. CLI `queues create | list | redrive | purge`; gauges `llmq_queue_depth{queue,kind}` and `llmq_queue_oldest_seconds{queue}`. FIFO queues are refused. A client re-creates a queue it knew when its keys vanish (restart without persistence); release deadlines and idle re-checks are jittered so the releasing pod does not always win the message back; with the queues on the state Valkey, Valkey recovering also ends the queue failover windows (`FailoverSettings.queues_on_state_valkey`). All scenarios pass on Valkey except the SQS-only S20 / S33 (skipped; S33v measures Valkey). Plan and results: docs/VALKEY_QUEUE_PLAN.md.
- Scenarios S04 (LangChain / LangGraph drop-in: `ainvoke`, `abatch`, lanes from `request_context`, `bind_tools`, structured output, a LangGraph tool loop, sync `invoke`, queued embeddings), S24 (S3 offload through an in-process moto S3) and S33 (SQS call counts through a counting proxy: bg sends and acks batched, rt single, idle long polls bounded). They replace the three skipped placeholders.
- `SCN_AWS_LOW_VOLUME=1` lets `SCN_PROVIDER=aws` run scenarios marked `low-volume`. S10, S11, S18, S23, S37, S47 and S49 are now marked not AWS-safe, because they inject faults or read the fakellm call log.
- Sample `17_langgraph_rag_agent.py`: an app-style graph with queued embeddings for retrieval, a structured-output router, a tool loop with `fallback_policy="chain"`, a bg-lane summary node, `stream_mode="updates"` and a sync `.invoke()`.

## [0.1.0a2] - 2026-10-30

### Added
- `ModelConfig.max_concurrency` (env `LLMQ_AC_MAX_BY_MODEL`, JSON `{"alias": N}`): a hard cluster-wide cap on calls in flight per model alias and target. The adaptive limiter still learns and cuts below it but never grows past it. Start limits above the cap are clamped (logged once), and a limit learned before the cap was set is clamped on next use. The cap also bounds the sync `direct` path (same Lua) and the per-pod local caps used while Valkey is down. Scenario S53 checks it: 2,000 queued, peak provider concurrency exactly 10.
- The receive budget subtracts the pod's own callers already waiting for a limiter slot, so a full bucket leaves the backlog in SQS.
- Settings per provider and per model, live on every pod without a restart (`model_settings.py`, README "Settings per provider and model"): `max_concurrency`, `limiter_initial`, `bg_share`, lane timeouts, `attempt_timeout_secs`, `throttle_retry_attempts`, the retry curve and breaker threshold/cooldown resolve through six layers (live model > live provider > env model > env provider > env global > default). Env layers come from the new `LLMQ_SETTINGS` JSON; live layers from Valkey `P:{cfg}:models`, validated, audited and reloaded every `cfg_reload_secs`.
- `shared_max_concurrency` on a provider: one bucket across all its models and pods (limiter key `{m:@<provider>:shared}`); each call holds its model slot and the shared slot. Scenario S55: shared cap 8 over two models, combined peak exactly 8, 600/600 OK.
- `llm-queue settings get [--model M | --provider P] | set | unset | reset`; `get` shows every value with its source. Scenario S54: a live `max_concurrency` cut from 20 to 5 mid-run reached all 3 workers within one reload, 900/900 OK.
- Retry curve knobs `LLMQ_RETRY_MULTIPLIER` (4), `LLMQ_RETRY_SPREAD` (2.5), `LLMQ_RETRY_MAX_SECS` (30) and `LLMQ_THROTTLE_RETRY_MAX_SECS` (10).
- DEBUG trace per call on `taskiq_llm_queue.core`: `call_started`, `slot_acquired` / `slot_unavailable`, `attempt_done`, `backoff`, `rt_capacity_wait`, `call_requeued`, `call_done`, each with the `request_id`.
- `sqs_timeouts_to_failover` (`LLMQ_SQS_TIMEOUTS_TO_FAILOVER`, 3).
- Rate budgets per model target and per provider: `rpm`, `tpm`, `rate_window` (`smooth` token bucket or `fixed` calendar minute), `rate_burst`, `rt_rate_share` (env `LLMQ_RATE_WINDOW`, `LLMQ_RT_RATE_SHARE`), `shared_rpm` / `shared_tpm`; settable in `LLMQ_SETTINGS` and live. Out of budget, rt moves to the next target and then fails `rate_limited` with `retry_after_secs`; queued bg/bulk wait in SQS for their own target. TPM is corrected from the response `usage`. Scenarios R01-R12.
- `lane_max_concurrency.<lane>`: a bucket per lane inside a model's bucket (per target, all pods, env and live). Scenario S57: bulk 3 and bg 5 held exactly under 600 parallel calls.
- `llm-queue status`: waiting / working / started this minute / budget left per model and target; gauges `llmq_rate_used`, `llmq_rate_remaining`.
- Limiter knobs as `LLMQ_AC_*` variables: `UTILISATION`, `WINDOW_SECS`, `LOSS_TOLERANCE`, `MIN_THROTTLES`, `PAUSE_RATIO`, `SETTLE_RATIO`, `PROBE_SECS`, `HOLD_RATIO`, `PEAK_HALFLIFE_SECS`.
- `llmq_inflight` and `llmq_breaker_open` gauges (per model target used by the pod) and SQS counters from every broker hook: `llmq_sqs_received_total`, `_sent_total`, `_acked_total`, `_released_total` (by reason), `_expired_total`, `_errors_total`, and `llmq_sqs_wait_seconds`.
- The Valkey state schema key `<prefix>:schema` is written on start; state from a newer schema refuses to start.
- `cancel()` (and a caller timeout) also stops a provider attempt already running: workers subscribe to `<prefix>:cancel` and cancel that attempt (scenario S56: slot freed 0.03–0.05 s after the cancel).
- Send batching on bg (`default`) and bulk queues (`sqs_batch_wait_secs`, 0.02 s; 0 turns it off); rt still sends at once.
- `s3_offload_bucket` / `s3_offload_prefix` / `s3_endpoint_url`: oversized messages carry their params in S3 (`S3OffloadMiddleware` on every broker) instead of a Valkey `params_ref`.

### Changed
- A received rt call keeps looking for a free slot for `rt_capacity_wait_secs` (30 s, env `LLMQ_RT_CAPACITY_WAIT_SECS`, live per provider/model, never past its deadline) instead of 3 passes; `core.RT_CAPACITY_PASSES` is gone. S35b: 36 of 6,043 rt calls had given up with 33–157 s of deadline left.
- Failed calls raise with `.meta` (attempts, queue wait, last target tried) on every `LLMQueueError`; error replies fill `queue_wait_ms` and the last target.
- Retry backoff: retry n sleeps a random time in `[base * multiplier^n, that * spread]`, i.e. 1-2.5 s, 4-10 s, 16-30 s, then 30 s (was `base * 2^n + uniform(0, base)`, capped at 20 s). After a throttle one sleep is capped at 10 s. `core.MAX_BACKOFF_SECS` is gone; `backoff_secs` takes a `Backoff` curve.

- A model fails over to direct after `sqs_timeouts_to_failover` SQS send timeouts within 60 s, not after one (a refused send still fails over at once); the timed-out call itself still runs direct. One timeout in a 10K burst used to cost the model 300 s of SQS.
- A 5xx that opens a target's breaker ends the call's retries on that target; in-flight calls kept retrying the open target (S12).
- `QueuedChatBedrockConverse` on langchain-aws 0.2.24: the queue-only client placeholder went into `model_kwargs` (no `bedrock_client` field there), reached the Converse request and failed envelope serialization. Only fields the installed class has are filled.
- A pod that lacks a model waits a random fraction of a second before handing the message back, so the message no longer comes back in phase with that pod's own polls (S48: the old pod had won 8 redeliveries in a row).
- `limiter_initial_clamped` warns only for a start you set, not for the default start (20) above a small `max_concurrency`.

### Removed
- The old `_S` / suffix-less env spellings (`LLMQ_CONSUMER_DRAIN_S`, `LLMQ_RETRY_BASE_S`, `LLMQ_SQS_WAIT_TIME_S`, `LLMQ_TIMEOUT_RT`, ...) are no longer read; the README lists each with its `_SECS` name.

## [0.1.0a1] - 2026-10-29

First pre-release. Everything listed below is new.

### Changed before 0.1 (breaking against the earlier dev snapshots)
- Every call returns a result object with `.meta`: `ConverseResult`, `ChatResult`, `EmbedResult`, `InvokeResult`.
- `timeout=` / `deadline=` became `timeout_secs=` / `deadline_at=`; `request_id=` is accepted on every call.
- `invoke_model(model, body, *, content_type=, accept=)` replaces guessing from a `"body"` key.
- Cancellation raises `LLMQueueCancelled`.
- `__all__` lists only the client surface; wire and plumbing names import from their submodules.
- `aws_region` defaults to `None`; `redis_password` is a `SecretStr`; `_SECS` env names (old `_S` names warn once).
- Redis keys are hash-tagged for cluster mode (key schema 2); ids are hashed with keyed BLAKE2b (`LLMQ_ID_HASH_SALT`), and xxhash is no longer a dependency.
- `QueuedChatBedrockConverse` takes `model_alias=`; an alias passed as `model=` raises.
- The empty `s3` extra is gone.

### Added
- Package skeleton (src layout, hatchling, uv, ruff, mypy strict, pytest).
- Frozen contracts for 0.x:
  - `QueueConfig` / `ModelConfig` / `Target`, with `alias[@target]` resolution and `LLMQ_*` env loading
  - envelope v1 and `LLMReply`, with the version rule and hashed-id labels
  - `request_context`
  - `KeySpace` (Redis key schema)
  - error tree and `ErrorInfo`
  - protocols for providers, limiter, claims, results and receive gates
  - metric names
- Contract test suite.
- Docker test stack (Valkey 8.1 with a password, ElasticMQ 1.7.1 with all queues and DLQs).
- SQS capability check script: ElasticMQ 1.7.1 passes 20/20, moto 5.2.5 server passes 19/20 (no `StartMessageMoveTask`).
- `HybridSQSBroker`: strict priority across queues with reserved slots, capacity-aware receive, receive gates, real long polls, batched acks and heartbeats, release on shutdown, byte-aware batched send, delays, FIFO, fair queues, expiry, message metadata; unknown tasks and newer envelopes are released, never dropped.
- Reliability:
  - adaptive concurrency limiter in Lua (baseline start, ramp, AIMD, pause gate, lane share, leases), with a sync twin and a local fallback limiter
  - Lua claim store for exactly-once execution, circuit breaker and fallback resolver
  - live config store with an audit stream
- Results: JSON result backend (no pickle; `GETDEL`; shared-owner safe) and a per-pod dispatcher. Measured: 1,000 ready results take 1 `MGET`.
- Providers:
  - Bedrock with SDK retries off (`total_max_attempts=1`), and OpenAI-compatible with `max_retries=0`
  - error-family classification and `Retry-After` parsing
  - provider registry for built-in, import-path and entry-point providers, with allowed and denied hosts
- `fakellm`: a fake Bedrock + OpenAI server with real error shapes, SigV4 region detection, scenarios, timed phases and an admin API, plus a Docker service.
- `docs/DESIGN.md`: the delivery-safety rules (DS1–DS19) and broker features (P1–P17, A1–A9) that the code refers to.
- Makefile and `tools/llmq_dev.py` task runner.
- S3 offload for large messages (P9) and an S3 result backend (P10).
- LangChain adapters for Bedrock and OpenAI-compatible chat and embeddings; LangChain 0.3 and 1.x; LangGraph compatible.
- Ops: canary router, failover to the direct path, `llm-queue` CLI (routing, fallback, limiter, breaker, queues, jobs), metrics hooks.
- `LLMQueue` client with jobs, cancellation, sync calls and an ordered drain on `stop()`.
- Performance: orjson for JSON and the SQS wire format, one parse per received message, results encoded once, 6 Valkey round trips per worker call, `speedups` extra (hiredis).
- Logging: structlog key-value events over stdlib `logging`; the library never configures logging.
- Reliability fixes from a multi-pod review: infrastructure errors release messages instead of deleting them, owner-checked claims, a pod heartbeat for direct and sync paths, the limiter grows only when busy, throttles never trip the breaker, S3 payloads deleted only after ack.
- Runnable `samples/` (13), including a replay of a real app's flows against real Bedrock.
- 619 tests; CI on Python 3.11–3.14, oldest-dependency and LangChain 1.x jobs, integration on the Docker stack; trusted publishing to PyPI.

### Known gaps
- Valkey restarted without persistence wipes all claims; in-flight calls are then treated as lost and may run a second time (S50: 0–16 re-runs). Keep Valkey persistence on.
- A reply that can't be stored is cached per pod; a redelivery to a different pod calls the provider again.
- Exhausted jobs reach a dead-letter queue only if their queue has one configured.
- Cluster-mode Valkey is covered by key-slot tests, not yet by a real cluster run.
- A call queued during a Valkey outage longer than `valkey_wait_failover_secs` can run twice if a worker takes it before its cancel flag is replayed.

### Fixed before release (found by the scenario harness, `tools/scenarios`)
- Limiter collapse under sporadic throttles: cuts only on clustered throttles (≥2 and ≥5 % in a 1 s window), at most one cut per window, pause only when half the window throttles, growth scales with the limit. 10K burst with 2 % throttles: 22.5 → 76.1 req/s, 6,721 → 10,000 ok (S35).
- Limit overshoot: growth slows near the last cut point; settles at 57–63 instead of swinging 44–64 (S07).
- Spurious claim takeovers under load: the heartbeat treated the worker's own just-finished claim as lost and cancelled the result publish (S35c, S27: 19 and 4 losses → 0). A real claim loss now ends the task quietly with a warning, no ERROR traceback.
- Valkey pools are sized to worker slots + 16; a failed lease release is retried on the next limiter call instead of holding the slot for 240 s.
- Throttle storms: a cut that does not lower the throttle ratio stops at the start value; a throttled step gets `throttle_retry_attempts` (new, default 6) tries; an rt call walks the fallback chain up to 3 times before `no_capacity`; leases live 60 s (was 240 s, renewed every 10 s) so a killed pod's slots return within a minute; a send queued behind a burst no longer counts as SQS down while other sends complete. 10K burst with 30 % random throttles and a worker kill: 16.3 → 49.7-59.3 req/s, 17.5 % → 99.2 % ok (S35b); S35 76.1 → 90.4 req/s.
- SQS down: the send is bounded by `sqs_send_timeout_secs` and half the remaining budget, then the call runs direct; first call served in 5.7 s instead of timing out (S20).
- Valkey down on a send-only pod: result-read errors now trip failover; waiting callers run direct; cancels are replayed on recovery (S21).
- Target-specific errors (ResourceNotFound, AccessDenied, model not in region, 404) move to the next fallback target instead of ending the call (S38).
- Per-attempt timeout `attempt_timeout_secs`; configurable provider read/connect timeouts (S39).
- A worker missing a model alias releases the message unclaimed (jittered delay) instead of acking an `internal_error`; it answers `model_not_configured` only near the delivery budget or deadline (S48).

- Valkey blip: a worker releasing work after a Valkey error is no longer cancelled by the claim heartbeat mid-release; the SQS message is released before the claim, so no call is lost (S50: 32/40 → 40/40 ok).
- Result dispatcher: a new same-lane waiter no longer forces a full read of every pending result (float rounding woke the loop); 1K waiters 43 → 26 MGET/s (S31).

### Added before release
- Scenario harness: 54 scenarios (work, retry, pause, cancel, purge, region change, timeouts, chaos, scale, 10K burst) on an isolated compose profile; `make scenarios`, `make load`, `make chaos`.
- Scenario harness real-Bedrock mode (`SCN_PROVIDER=aws`): queues stay local, provider calls go to Bedrock; only AWS-safe scenarios run. S01, S03, S16, S17, S29, S40, S41 pass against real Bedrock (~108 calls).
- Settings: `attempt_timeout_secs`, `provider_read_timeout_secs`, `provider_connect_timeout_secs`, `sqs_send_timeout_secs`, `valkey_wait_failover_secs`, limiter window settings.
