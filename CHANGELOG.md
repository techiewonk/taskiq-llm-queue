# Changelog

All notable changes are listed here. The project uses semantic versioning; while it is 0.x,
a minor release may break the public API, with a deprecation announced one minor release ahead
(see STABILITY.md).

## [Unreleased]

### Fixed (found by the scenario harness, `tools/scenarios`)
- Limiter collapse under sporadic throttles: cuts only on clustered throttles (≥2 and ≥5 % in a 1 s window), at most one cut per window, pause only when half the window throttles, growth scales with the limit. 10K burst with 2 % throttles: 22.5 → 76.1 req/s, 6,721 → 10,000 ok (S35).
- Limit overshoot: growth slows near the last cut point; settles at 57–63 instead of swinging 44–64 (S07).
- Spurious claim takeovers under load: the heartbeat treated the worker's own just-finished claim as lost and cancelled the result publish (S35c, S27: 19 and 4 losses → 0). A real claim loss now ends the task quietly with a warning, no ERROR traceback.
- Valkey pools are sized to worker slots + 16; a failed lease release is retried on the next limiter call instead of holding the slot for 240 s.
- SQS down: the send is bounded by `sqs_send_timeout_secs` and half the remaining budget, then the call runs direct; first call served in 5.7 s instead of timing out (S20).
- Valkey down on a send-only pod: result-read errors now trip failover; waiting callers run direct; cancels are replayed on recovery (S21).
- Target-specific errors (ResourceNotFound, AccessDenied, model not in region, 404) move to the next fallback target instead of ending the call (S38).
- Per-attempt timeout `attempt_timeout_secs`; configurable provider read/connect timeouts (S39).
- A worker missing a model alias releases the message unclaimed (jittered delay) instead of acking an `internal_error`; it answers `model_not_configured` only near the delivery budget or deadline (S48).

### Added
- Scenario harness: 54 scenarios (work, retry, pause, cancel, purge, region change, timeouts, chaos, scale, 10K burst) on an isolated compose profile; `make scenarios`, `make load`, `make chaos`.
- Settings: `attempt_timeout_secs`, `provider_read_timeout_secs`, `provider_connect_timeout_secs`, `sqs_send_timeout_secs`, `valkey_wait_failover_secs`, limiter window settings.

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
- A reply that can't be stored is cached per pod; a redelivery to a different pod calls the provider again.
- Exhausted jobs reach a dead-letter queue only if their queue has one configured.
- Cluster-mode Valkey is covered by key-slot tests, not yet by a real cluster run.
- A 30 % random throttle storm (S35b) still holds limits low: a throttle-counting limiter can't tell it from overload; using Retry-After as a signal is planned.
- A call queued during a Valkey outage longer than `valkey_wait_failover_secs` can run twice if a worker takes it before its cancel flag is replayed.
