# Release plan: 0.3.0

0.3.0 replaces 0.2 entirely (no taskiq broker, no SQS, no lanes). This is the checklist from
"built" to "running in prod", in order. Each step names who does it and how it is checked.
The mechanics of a release (TestPyPI, tags, trusted publishing) are in
[RELEASING.md](RELEASING.md); this file is what has to be true before and after.

## 1. Library ready (done on branch `feat/accounts-routing`, not committed yet)

| Check | How | State |
|---|---|---|
| Every step passes | `python scripts/check.py all` (lint, format, strict types, unit and integration tests on Valkey and Redis, failover drill, load drill, plan gates, samples) | must be green on the final commit |
| Plan gates | `python scripts/gates.py`: B17, B19, B22, B23, B33, B34, B56/B93, B91, B92, B94, B95, B96, B97 | all pass (plan §13 table) |
| Soak | `python scripts/check.py soak20` (20 min, 8 worker processes, chaos every 2 min: kill, store block, live edit, restart; leak and drift checks) | PASS required |
| Fault scenarios | `python scripts/scenarios.py all --jobs 1` (29 scenarios x 3 loads + 3 failover drills, one at a time) | every run PASS |
| Real Bedrock and the real store | `python scripts/samples.py --live --store env --env-file ../../.env` (9 samples, ElastiCache over TLS) | all pass; no keys left behind |
| Docs match the code | README, ARCHITECTURE, plan §13, walkthrough, samples README, CHANGELOG 0.3.0, STABILITY | reviewed |
| Commit | one commit (or a short series) on `feat/accounts-routing`, no co-author lines; `.env` / `.pypirc` never staged | owner |

## 2. Merge and release (owner)

1. Open a PR `feat/accounts-routing` -> `main` in `techiewonk/taskiq-llm-queue`; CI must pass
   (checks on Python 3.11-3.14, lowest-direct dependencies, LangChain 1.x, integration).
2. Merge. Set the CHANGELOG heading `[0.3.0] - <date>`; `_version.py` is already `0.3.0`.
3. **Actions -> publish -> Run workflow** (TestPyPI); install it in a clean venv and run one
   sample against fakellm.
4. Publish the GitHub release `v0.3.0`; `publish.yml` uploads to PyPI with provenance.
   Never twine or a manual upload.

## 3. Owner decisions (answered 2026-11-13)

| | Decision | Answer | In the app registry |
|---|---|---|---|
| Q1 | Real quotas per model and region (Service Quotas) | Keep the placeholders for now | gpt-oss-120b 500 RPM / 400K TPM, gpt-oss-20b 1,000 / 800K, titan and cohere 2,000 RPM; changed live later with `llm-queue registry set` |
| Q2 | Region lists per model | ap-south-1 confirmed, ap-south-2 as fallback | every model: `confirmed` its region, `fallback` ap-south-2, rule `fallback` on exhausted / broken after 2 s. Whether each model is offered in ap-south-2 is unverified: a missing one is a target error that disables that deployment with an alert |
| Q4 | Residency sign-off | India only | `residency.allowed_regions` = ap-south-1, ap-south-2 |
| Q6 | The Bedrock streaming permission in every account and region | Granted | chat stays on ConverseStream; the live sample run checks it |

## 4. App rollout (research assistant)

The app pins `taskiq-llm-queue[langchain-aws]==0.3.0` and keeps `LLM_QUEUE_ENABLED=false` by
default, so the release changes nothing until it is switched on per environment.

1. **Secrets** in `{env}/researchassistant/secret` for every environment that turns it on:
   `llmq_id_hash_salt` (required outside dev / local / test / ci); the `valkey_*` keys (TLS is
   required outside dev).
2. **Pod settings** (recommended, deployment YAML not changed yet): preStop that calls
   `POST /drain` and then sleeps; `terminationGracePeriodSeconds` >= 75 (25 s producers + 10 s
   background tasks + 20 s queue drain); liveness on its own path (readiness fails while
   draining); `POD_NAME` from the downward API (worker ids and AWS session names).
3. **dev**: `LLM_QUEUE_ENABLED=true`, run a day; watch `llmq_alert_*`, `llmq_calls_total`
   by outcome, store memory. Check: `python scripts/llm_queue_live_e2e.py` (app repo).
4. **uat**: same, with a load test at the expected peak.
5. **prod**: canary with `queue_pct` (the rest stays direct), then 100 %. Rollback in order:
   `routing.kill_switch` (live, every call direct) -> `LLM_QUEUE_ENABLED=false` (deploy) ->
   the previous image.

## 5. After the release

- Record prod's measured numbers (store memory, calls per second, queue overhead) in plan §13.
- The soak runs 20 minutes on one machine with 8 worker processes; repeat it in uat at prod
  volume on the real pods (point `scripts/soak.py` at uat's store with fakellm, or run the
  app's load test) before prod.
- Known scale limits, measured: about 11 store calls per queued call; admission cost grows
  with the number of waiting calls (2.2 ms at 400, measured before the first-fit change) but
  stays far inside the app's rates.
