# Stability policy

- **Public API** = the names in `taskiq_llm_queue.__all__`, the registry document's setting
  names and the `llm-queue` commands. Anything else may change without notice.
- **0.x releases:** a minor release may break the public API; every break is in CHANGELOG.md.
  0.3.0 is a clean break from 0.2 (nothing is carried over).
- **Message format:** queue messages carry `v` (2 in 0.3). A worker returns a message newer
  than it understands to the queue instead of dropping it.
- **Store schema:** key schema 3. A schema change ships with a migration note.
- **Supported versions:** Python 3.11-3.13 (3.14 runs in CI, not yet supported); Valkey 8
  and Redis 7.2+ as one primary with replicas (cluster mode is not supported: one admission
  touches keys of several hash tags). The store must allow Lua scripts, pub/sub and blocking
  list reads (`BLPOP`, the queue doorbell); ElastiCache for Valkey / Redis OSS does.
- **Dependencies:** ranges with upper bounds in `pyproject.toml` (`aiobotocore<4`, `redis<9`,
  ...); a release may raise a bound, never drop one without a test on the new major.
- **Settings:** the `LLMQ_*` names and the registry setting names are public API; a removed
  name is refused at start with its replacement named, not silently ignored.
