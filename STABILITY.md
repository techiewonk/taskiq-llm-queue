# Stability policy

- **Public API** = the names in `taskiq_llm_queue.__all__`, the registry document's setting
  names and the `llm-queue` commands. Anything else may change without notice.
- **0.x releases:** a minor release may break the public API; every break is in CHANGELOG.md.
  0.3.0 is a clean break from 0.2 (nothing is carried over).
- **Message format:** queue messages carry `v` (2 in 0.3). A worker returns a message newer
  than it understands to the queue instead of dropping it.
- **Store schema:** key schema 3. A schema change ships with a migration note.
- **Supported versions:** Python 3.11-3.13; Valkey 8 and Redis 7.2+ as one primary with
  replicas (cluster mode is not supported).
