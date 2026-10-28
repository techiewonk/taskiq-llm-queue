# Changelog

All notable changes are listed here. The project uses semantic versioning; while it is 0.x,
a minor release may break the public API, with a deprecation announced one minor release ahead
(see STABILITY.md).

## [Unreleased]

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
- 365 tests (unit + integration on Valkey 8.1, ElasticMQ 1.7.1 and fakellm); ruff and mypy strict are clean.

### Not yet built
- `LLMQueue` client API, LangChain adapters, task/receiver wiring and lifecycle (`start`/`stop`/drain).
- Canary routing, failover modes, CLI, metrics exporters.
- S3 offload (P9) and the S3 result backend (P10).
