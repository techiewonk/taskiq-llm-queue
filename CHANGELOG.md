# Changelog

All notable changes are listed here. The project uses semantic versioning; while it is 0.x,
a minor release may break the public API, with a deprecation announced one minor release ahead
(see STABILITY.md).

## [Unreleased]

### Changed

- Queue names (model `queues` and `job_queues`) are checked when the config is built: 1-80 of
  `A-Z a-z 0-9 - _`, `<name>.fifo`, or an SQS queue URL, on both backends. Before, a bad name
  passed and failed at start as "does not exist", and on Valkey `queues create` made it (a `{`
  or `}` broke the key's hash tag).
- The missing-queue error at start names the fix for each backend (`llm-queue queues create`
  on Valkey) instead of only SQS's.

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
