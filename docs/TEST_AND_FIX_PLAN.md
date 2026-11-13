# 0.3.0 test and fix plan

Every check runs one at a time (no parallel runs), on fakellm unless it says live. A step is
done only when its command passes; a failure is fixed (with a regression test or scenario)
and the step is run again before the next one starts. Logs: `reports/`.

## 1. Where things stand (2026-11-13)

| Area | State |
|---|---|
| Fault scenarios, 50 % load, one by one | 32/32 pass (29 scenarios + 3 Docker failover drills) |
| Fault scenarios, 100 % and 300 % load | running (step T1) |
| `check.py all` (lint, types, tests, drills, gates, samples) | last full pass before this round's fixes; must run again (T2) |
| 10-minute soak | pass (7/7) before this round's fixes |
| 1-hour soak | failed (stuck calls, late answers, a throughput dip); causes fixed, not re-proved (T4) |
| Live Bedrock samples (9) | pass |
| Live Bedrock 3-minute soak | 3 refusals at start (load too close to 10 RPM); offer lowered to 50 %, not re-run (T6) |

Fixed this round (each with a regression test and a CHANGELOG entry): read timeouts not
retried (redis-py `Retry` default); half-open store dropping calls as `caller_gone`; held calls
renewed forever (`held_call_stuck`); one deadline per call; credentials per account and
region; failover to an untried region free of the retry budget; store out of memory as
`CallFailed`; `StoreUnavailable` for admin operations; shared wake-up for two local waiters;
`Tuning` (every timeout and pool size settable in code).

## 2. Gaps and fixes

### Library

| # | Gap | Fix | Proved by |
|---|---|---|---|
| L1 | Other admin paths may still leak redis-py errors: CLI commands, HTTP router handlers, `JobRunner`, LangChain adapters | Audit each public entry; wrap with `store_op` / map to `CallFailed`; CLI prints the error and exits 1 | a test per entry point with a dead store |
| L2 | Streaming under faults is untested (store block / worker kill mid-stream) | Add scenario P6 (stream cut) and S12 (store block mid-stream) | scenarios |
| L3 | Separate queue store (`LLMQ_QUEUE_STORE_URL`) has no fault scenario | Scenario Q1: queue store blocked, state store fine (calls run direct after `queue_failover_secs`) | scenarios |
| L4 | Embeddings (bulk, batched) never run under faults | Scenario E1: Cohere batches with a worker kill and a store block | scenarios |
| L5 | Throughput dip at minutes 55-56 of the 1-hour soak is unexplained (worker logs were lost then) | Re-run the 20-minute soak with worker logs; if it recurs, read `worker-N.log` | T4 |
| L6 | `--profile prod` scenarios never run (production timers) | Run the store and worker groups with prod timers | T5 |

### Test harness

| # | Gap | Fix |
|---|---|---|
| H1 | Gate B96 reads 16/s alone but 26.5/s inside the full gate run: gates are not isolated (a previous gate's processes or keys still count) | Each gate waits for its workers to exit and deletes its prefix before the next starts; B96 counts only its own prefix's scripts |
| H2 | The Windows accept bug (WinError 64 closes the listener) is fixed in the scenario proxy only; `soak.py` and gate B95 use the same proxy | Move `serve()` (keep listening) into `failover.Proxy` and use it everywhere |
| H3 | A flaky test can hide (the fixed `queue_full` race) | Run the unit and integration tests 3 times in a row; any test that fails once is fixed to poll, not sleep |
| H4 | Library logs are files under `reports/`, not `docker logs` | Optional: a worker image and compose service so `docker logs` shows workers (on request) |
| H5 | Leftover `llmq-test-sqs-1` (ElasticMQ) container from 0.2 | `docker rm -f llmq-test-sqs-1` |

### Docs

| # | Gap | Fix |
|---|---|---|
| D1 | README: `StoreUnavailable` | Add to the errors section |
| D2 | ARCHITECTURE failure model: store trouble judged from the worker's own heartbeat; credentials per region; failover free of the retry budget; read timeouts not retried; stuck-call release | Update the failure-model section and measured numbers |
| D3 | STABILITY: new public names `Tuning`, `StoreOptions`, `BedrockOptions`, `EngineTimings`, `RegistryOptions`, `StoreUnavailable` | Add to the public API list |
| D4 | RELEASE_PLAN: release gate = `check.py all` + `scenarios` (all loads) + `soak20` + live samples | Update the checklist |

## 3. Order (one by one)

| Step | Command | Pass |
|---|---|---|
| T1 | `python scripts/scenarios.py <all> --load at --load burst --jobs 1` | every run PASS |
| T2 | fix L1, H2, H1, H5; then `python scripts/check.py all` | ALL PASS |
| T3 | H3: `pytest tests -p no:randomly` three times (via `check.py a0 ... a17` x3) | no test fails once |
| T4 | `python scripts/check.py soak20` | PASS; no `held_call_stuck`; no dip outside chaos |
| T5 | `python scripts/scenarios.py --group store --group worker --profile prod --jobs 1` | every run PASS |
| T6 | `python scripts/soak.py --minutes 3 --live` and `python scripts/samples.py --live --store env` | PASS (needs a valid AWS token in `.env`) |
| T7 | L2-L4 scenarios added, run one by one at all loads | every run PASS |
| T8 | D1-D4 docs | reviewed against the code |
| T9 | `python scripts/check.py all` once more on the final tree | ALL PASS, then one commit (owner) |
