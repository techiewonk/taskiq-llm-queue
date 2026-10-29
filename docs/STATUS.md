# Status: features, issues, solutions and results

Snapshot **2026-10-30**. PyPI has **`0.1.0a2`** (tag `v0.1.0a2`, `037b6c0`): every feature below marked "main only" is in it. Plan: `docs/BEDROCK_SQS_QUEUE_PLAN.md`
v0.30 in the app repo; rate-limit design: [RATE_LIMIT_PLAN.md](RATE_LIMIT_PLAN.md).

Legend: ✅ built and verified · 🟡 built, partly verified or with a known limit · 🔵 proposed / agreed, not built · ❌ open problem

---

## 1. Features

| # | Feature | Status | Where | Sample | Verified by |
|---|---|---|---|---|---|
| F1 | SQS queue per model and lane (rt / bg / bulk); every request kept until it runs | ✅ 0.1.0a1 | broker | 01, 04 | S01–S06, S53 (2,000 queued) |
| F2 | Exactly-once execution (claims), heartbeats, crash takeover, DLQ | ✅ 0.1.0a1 | reliability | 06 | S20–S22, S50 |
| F3 | Adaptive concurrency limiter per model + target (learns from throttles) | ✅ 0.1.0a1 | limiter | 05 | S07, S08 |
| F4 | Fallback: region → APAC profile → other model; breaker | ✅ 0.1.0a1 | core, fallback | 05 | S38, S39, S35 |
| F5 | Failover to direct when SQS / Valkey are down | ✅ 0.1.0a1 | failover | 06 | S20, S21 |
| F6 | Results via Valkey + per-pod dispatcher; cancel / timeout | ✅ 0.1.0a1 | results | 10 | S16, S17, S31 |
| F7 | LangChain / LangGraph adapters, OpenAI-compatible provider, sync calls, jobs | ✅ 0.1.0a1 | langchain, providers | 02, 03, 07, 08, 09, 11 | samples 01–13, S02, S10 |
| F8 | Canary routing (sqs vs direct), live fallback chains, CLI | ✅ 0.1.0a1 | routing, cli | 12_cli.md | integration tests |
| F9 | **Bucket size** `max_concurrency` per model + target | ✅ 0.1.0a2 | limiter | 14 | S53: peak exactly 10 of 2,000 |
| F10 | **Settings per provider and model, six layers** (live model > live provider > env model > env provider > env global > default), `LLMQ_SETTINGS` | ✅ 0.1.0a2 | model_settings | 14 | unit tests every layer; sample 14 shows sources |
| F11 | **Live changes without restart** (`llm-queue settings get/set/unset/reset`, audited) | ✅ 0.1.0a2 | cli, client | 14, 12_cli.md | S54: cap 20 → 5 live, peak 20 → 5; CLI integration tests |
| F12 | **Provider-wide bucket** `shared_max_concurrency` | ✅ 0.1.0a2 | core, lifecycle | 14 | S55: shared 8, combined peak exactly 8; integration test peak 3 |
| F13 | **Retry curve** 1–2.5 s, 4–10 s, 16–30 s, cap 30 s; throttle cap 10 s; all knobs configurable | 🟡 0.1.0a2 | core | 15 | unit tests; see I4/I5 (throughput cost) |
| F14 | **rt capacity wait** `rt_capacity_wait_secs` (30 s, live per model) | 🟡 0.1.0a2 | core | 15 | unit tests, sample 15; S35b effect unclear (I4) |
| F15 | **`.meta` on every error** (attempts, queue wait, last target) | ✅ 0.1.0a2 | client, core | 15 | unit tests, sample 15 |
| F16 | Old `_S` env names dropped | ✅ 0.1.0a2 | config | — | unit test |
| F17 | **RPM / TPM rate budget** per model target and provider (`smooth` or `fixed` minute, rt reserve, usage-corrected TPM) | ✅ 0.1.0a2 | rate, core | 16 | R01–R09, R11 PASS (below); integration tests on Valkey |
| F18 | **Waiting / working / done view** `llm-queue status` + `llmq_rate_used` / `llmq_rate_remaining` | ✅ 0.1.0a2 | cli, client | 16, 12_cli.md | R12: status = call log (12 started, 18 left) |
| F19 | **A bucket per lane** `lane_max_concurrency.<lane>` | ✅ 0.1.0a2 | core, lifecycle | 16 | S57: bulk 3 / bg 5 held exactly over 600 calls |
| F20 | Cancel stops a running attempt; caller timeout too | ✅ 0.1.0a2 | core, client | 10 | S56: slot freed 0.03–0.05 s after cancel |
| F21 | Debug trace per call (`taskiq_llm_queue.core` at DEBUG) | ✅ 0.1.0a2 | core | — | unit test of the event sequence |
| F22 | Metrics: `llmq_inflight`, `llmq_breaker_open`, SQS counters per queue | ✅ 0.1.0a2 | client, lifecycle | — | unit tests |
| F23 | S3 offload for messages over 200 KB (`s3_offload_bucket`) | ✅ 0.1.0a2 | lifecycle, client | — | integration test (moto S3 + ElasticMQ) |
| F24 | Send batching for bg / bulk (`sqs_batch_wait_secs` 0.02 s) | ✅ 0.1.0a2 | lifecycle | — | S35, S35b, S35c, S23, S27 PASS |

## 2. Issues found and their solutions

| # | Issue | Why it happens | Solution | Status |
|---|---|---|---|---|
| I1 | rt calls failed `no_capacity` with 33–157 s of deadline left (36 of 6,043 in S35b) | rt tried 3 passes × 2 s per target, then gave up | `rt_capacity_wait_secs` (30 s, live, never past the deadline) | ✅ built (`d3c11d6`) |
| I2 | Failed calls showed attempts 0 / queue wait 0 to the caller | `_unwrap` raised without the reply's meta | `.meta` on every `LLMQueueError`; error replies fill queue wait + last target | ✅ built |
| I3 | gpt20b `ThrottlingException` (3–4 per S35b run) | single target: 6 throttles in a row at 30 % = 0.07 % | give gpt20b a fallback, or `throttle_retry_attempts=10` (0.0006 %) | ✅ documented (README, integration guide) |
| I4 | **One model's rt queue missed its 180 s deadline in some S35b runs** | Two causes found with a per-second timeline: (1) false "SQS down" at burst start: the sender's busy loop made its first sends look stalled against `_sent_at = -inf`, so both chat models failed over to direct for 300 s; (2) the limiter paused a model on chance windows (2 of 3 throttled ≈ 22 % of windows at 30 % random), pauses fed on themselves (failing runs paused 43–70 % of the time) | (1) a send wait the loop overran does not count; a send is judged from its own start. (2) pause only when the 95 % Wilson lower bound of the throttle ratio reaches `pause_ratio` (3 of 3 pauses, 2 of 3 / 5 of 6 do not), on every throttle; same rule in the local limiter | ✅ fixed (`ad81729`): pauses ~0 %, no false failovers |
| I5 | New code failed S35b more often than old | Mostly I4 cause (1): the new per-call work tipped the sender's busy start past 5 s | Fixed by I4 (1) | ✅ fixed |
| I6 | S35b failed 2 of 5 runs right after `ad81729` (806 and 1,267 rt timeouts; was up to 4,429) | Not reproduced since. Those runs had no pauses, no failovers and full slots but half the calls/s; per-process CPU (added `f29ed63`) shows workers ~0.55 core, sender ~0.3, fakellm ~0.08 of 8, so CPU is not the limit | 16 runs since the fixes: **14 pass**, the last **11 in a row** (99.90–99.96 % OK) | 🟡 watching |
| I7 | A timed-out call cannot be traced | Library logged nothing per call | DEBUG trace: `call_started`, `slot_acquired`, `attempt_done`, `backoff`, `rate_limited`, `call_done` ... | ✅ built (`1bce171`) |
| I8 | Scenario gate passes on a single run | Flaky scenarios slip through | Repeat gate run: full suite 3 times in a row, S35b 5 times. Found I21–I23 (failed all 3 passes); everything else passed 3 / 3 | ✅ done |
| I9 | `retry_attempts=5` (owner request) | `retry_attempts` covers 5xx; the S35b leftovers are throttle runs on single-target gpt20b (`throttle_retry_attempts`) | Measured: 99.90 % and 99.95 % OK with 5 vs 99.91–99.95 % with 3: no difference. Keep 3 | ✅ measured |
| I10 | `limiter_initial_clamped` warned for the default start | Warning fired for start 20 > a small `max_concurrency` | Warn only for a start someone chose | ✅ fixed (`1bce171`) |
| I11 | First S35b failure was blamed on unscaled harness caps | That run had the same one-model pattern as I4 | Claim withdrawn; caps stay scaled (consistent shape) | ✅ corrected |
| I12 | No per-minute limit | Library limited concurrency only | F17 rate budget (RPM + TPM, smooth / fixed, rt reserve, usage correction, Valkey-down local share) | ✅ built (`8d47946`) |
| I13 | No single view of waiting / working | Counts spread over SQS, limiter, CloudWatch | F18 `llm-queue status`; `llmq_inflight`, `llmq_rate_used`, `llmq_rate_remaining`; waiting = SQS `ApproximateNumberOfMessagesVisible` | ✅ built |
| I14 | `llmq_breaker_open` never emitted; broker hooks not exported | metrics wiring gap | Both gauges per used target; every broker hook except heartbeat reported | ✅ built (`53a5d0c`) |
| I15 | `<prefix>:schema` never written | missing code | Written with NX on start; newer schema refuses to start | ✅ built (`ba5c269`) |
| I16 | S3 offload not wired | missing wiring | `s3_offload_bucket` installs `S3OffloadMiddleware` on every broker | ✅ built (`9611d91`) |
| I17 | Send batching not enabled | `build_brokers` did not turn it on | bg / bulk batch for 0.02 s; rt sends at once | ✅ built (`5ea3159`) |
| I18 | `cancel()` did not stop an attempt in flight | by design so far | Cancel channel + `InflightAttempts`; caller timeouts publish too | ✅ built (`5ea3159`) |
| I19 | Valkey without persistence can re-run in-flight calls (S50: 0–16) | claims are lost on restart | Keep ElastiCache persistence on (ops) | ✅ documented |
| I20 | Limiter cut / hold / probe knobs had no env variables | only `LimiterSettings` fields | `LLMQ_AC_UTILISATION`, `_WINDOW_SECS`, `_LOSS_TOLERANCE`, `_MIN_THROTTLES`, `_PAUSE_RATIO`, `_SETTLE_RATIO`, `_PROBE_SECS`, `_HOLD_RATIO`, `_PEAK_HALFLIFE_SECS` | ✅ built (`388fe3b`) |
| I21 | S12: 3 calls reached a target while its breaker was open (limit 2) | Calls already in flight kept retrying after their own 5xx opened the breaker; the longer retry curve stretched those retries into the open window | A failure that opens the breaker ends the step at once (unit test + S12 3 / 3, `calls_while_open_8s` 2) | ✅ fixed (`1e09f08`) |
| I22 | S11: new fallback chain "not used within 30 s" (took ~34 s) | Scenario timing: `throttle_retry_attempts` (6) overrode its one attempt per step, ~15 s of throttle retries per call | Scenario sets one throttle attempt and a 2 s rt wait; chain change takes effect in ~4.5 s (3 / 3) | ✅ fixed (`1e09f08`) |
| I24 | One SQS send timeout in a burst sent a model direct for 300 s (H17) | A timeout is ambiguous | Fail over after `sqs_timeouts_to_failover` (3) timeouts in 60 s; a refused send still at once | ✅ fixed (`1bce171`); S20 5 calls 15.8 s (was 5.7 s), all direct |
| I25 | S48: a pod lacking a model won 8 redeliveries in a row | Whole-second SQS delays kept the message in phase with that pod's polls | Random sub-second pause before the release | 🟡 improved (`5ea3159`): 6 / 6 pass, the old pod still wins 2–3 times on ElasticMQ |
| I26 | S27 rt queue wait p95 29.8 s once | mypy ran on the same machine during the run | Quiet re-run 0.47 s; never run checks during load scenarios | ✅ explained |
| I23 | S40: timeout after 1.28 s (want 1.0 ± 0.2) | Scenario timer included the fakellm script request (0.28 s); the queue call itself took 1.015 s | Clock starts at the call (3 / 3) | ✅ fixed (`1e09f08`) |

## 3. Results

### Tests and checks (latest, `1e09f08`)

| Check | Result |
|---|---|
| ruff, ruff format, mypy strict | clean (163 files) |
| Unit tests | 583 passed |
| Integration tests (Valkey + ElasticMQ) | 149 passed |
| GitHub CI (3.11–3.14, lowest deps, langchain 1.x, integration) | green on `1af5ebb`; `d3c11d6` pushed |
| Samples 01–15 on the local stack | 15 / 15 exit 0 (13 also passed 12/12 on real Bedrock earlier) |

### Scenarios (fakellm)

| Scenario | What | Result |
|---|---|---|
| S06, S07, S08, S16, S17, S30, S38, S39 | priority, limit learning, fast start, expiry, cancel, Retry-After, region→profile, hang→timeout | PASS |
| S53 | 2,000 queued, bucket 10 | PASS: peak exactly 10, 0 lost, 0 duplicates |
| S54 | live bucket 20 → 5 on 3 workers | PASS: peak 20 before, 5 after one reload, 900/900 |
| S55 | provider bucket 8 over two models | PASS: combined peak 8, 600/600, 0 duplicates |
| S35b | 10K burst, 30 % random throttles, worker killed at 10 s | **14 of 16 pass** since fixes `ad81729`, last 11 in a row (was ~2 of 4), see below |
| Full suite × 3 | all 57 scenarios, 3 passes in a row (S04, S24, S33 skip: need LangChain / upstream / AWS) | 51 pass every pass; S11, S12, S40 failed every pass → I21–I23 fixed, then 3 / 3 each. Re-run after the breaker fix: S10, S12, S36, S39, S42, S44, S50, S35b pass |

### Rate limits and lane buckets (fakellm, 2026-10-30)

| Scenario | What | Result |
|---|---|---|
| R01 (+R04) | RPM 50 `fixed`, 120 bg calls, 3 workers | PASS: 40 / 40 / 40 per minute (bg = 80 %), 120 / 120 |
| R02 | RPM 120 `smooth`, burst 10, `max_concurrency` 3, 1 s calls | PASS: max 19 starts in any 10 s, 60 calls in 33.9 s |
| R03 | rt reserve under a 150-call bg backlog (RPM 60) | PASS: rt 15 / 15, p95 0.47 s; bg ≤ 41 / min |
| R05 | TPM 30,000 `smooth`, 200 calls reserving ~260, using ~14 | PASS: 5.2–5.6 s, budget counted 2,800 = usage (not 52,000) |
| R06 | gpt20b + gpt120b RPM 40 each, bedrock shared 50 | PASS: combined 40 / 40 / 20, each ≤ 40 |
| R07 | rt call with 10 s left, budget spent | PASS: `rate_limited`, `retry_after_secs`, answered at 8 s |
| R08 | RPM 60 → 15 live | PASS: 48 then 12 per minute |
| R09 | Valkey down, RPM 20 | PASS: 16 then 9 (local share), no errors |
| R11 | gpt120b RPM 5 per target | PASS: rt 5 primary + 3 apac; bg never on apac |
| R12 | `status` vs call log | PASS: started 12, `rpm_left` 18, working 0 |
| S56 | cancel during a 20 s provider hang | PASS 3 / 3: slot freed in 0.03–0.05 s |
| S57 | lane buckets bulk 3, bg 5 | PASS: peaks exactly 3 and 5, rt 0.23 s, 600 / 600 |
| Sample 16 | RPM 10 fixed, rt reserve, bulk cap 2, status | 8 now + 4 next minute, rt 0.5 s, bulk peak 2, status matches |

R10 (real quota lower than the configured RPM) is the adaptive limiter's existing behaviour (S07, S35c).

### S35b runs (gate: ≥ 95 % OK, 0 lost, 0 duplicates)

| # | Code / config | Result | OK | Timeouts | Other errors | Clean run? |
|---|---|---|---|---|---|---|
| 1 | old (`23f66e2`) | PASS | 99.7 % | 0 | 29 | yes |
| 2 | old | PASS | 99.7 % | 0 | 30 | yes |
| 3 | old | FAIL | 94.0 % | 539 | 60 | yes |
| 4 | old | FAIL | 63.3 % | 3,605 | 69 | **no** (overlapped samples 14/15) |
| 5 | new, harness caps unscaled | FAIL | 86.9 % | 1,212 (rt gpt120b) | 101 | yes |
| 6 | new (`1af5ebb`) | PASS | 99.6 % | 1 | 40 | yes |
| 7 | new + rt wait 30 s + INFO logs | FAIL | 94.9 % | 448 (rt gpt20b) | 62 | yes (logging overhead) |
| 8 | new + rt wait 30 s | PASS | 99.5 % | 0 | 46 | yes |
| 9 | new + rt wait 6 s | FAIL | 88.1 % | 1,103 (rt gpt20b) | 91 | yes |
| 10 | new (`d3c11d6`) | FAIL | 83.6 % | 1,581 | 62 | yes |
| 11 | new (`d3c11d6`) | FAIL | 85.3 % | 1,417 | 51 | yes |
| 12 | new (`d3c11d6`) | FAIL | 82.9 % | 1,631 | 83 | **no** (overlapped samples 01–13) |

After `ad81729` (5 runs in a row, quiet stack): PASS 99.3 %, 99.96 %, 99.91 %; FAIL 91.9 % (806 rt timeouts), 87.1 % (1,267). Pauses ~0 %, false SQS failovers 0. Then 5 more with the CPU timeline: all pass (99.93–99.96 %). Full suite × 3: pass 99.95 %, 99.92 %, 99.93 %. `retry_attempts=5`: pass 99.90 %, 99.95 %. After `1e09f08`: pass 99.91 %. S06, S07, S08, S35, S35c, S39, S53, S54, S55 re-run and pass.

In every run: **0 lost, 0 duplicate provider calls**; bg and bulk 100 % or close; all timeouts are rt calls of a single model (I4).

### Why S35b is not 100 %

1. **Small, every run (now ~0.05–0.1 %, 5–10 of 10,000)**: throttle runs on single-target gpt20b (I3) and the odd rt `no_capacity` (I1). Clean errors to the caller; `retry_attempts=5` does not change them (I9), a gpt20b fallback or `throttle_retry_attempts=10` does.
2. **Large, some runs (was up to ~16 %)**: I4/I5 (limiter pauses on chance throttle windows, false SQS failover at burst start), fixed in `ad81729`. Not seen in the 11 runs since (I6 watching).
3. Production differs: real Bedrock throttles track load (quota), not a fixed 30 % random rate, and bursts are not 10K in 2 s. S35b is a stress test; it still has to pass reliably (I8).

## 4. Next steps (proposed order)

| # | Step | Size |
|---|---|---|
| N1 | ✅ Record limiter limit/inflight per model every second in S35b; confirm I4 | S |
| N2 | ✅ Fix I4 (limiter behaviour under random throttles) and I5 (backoff without holding a worker for queued bg/bulk) | M |
| N3 | ✅ Repeat gate (I8): S35b 5 runs in a row on a quiet stack, then the full suite 3 times | M (time) |
| N4 | ✅ I9: S35b with `retry_attempts=5`, record the effect | S |
| N5 | ✅ Debug lifecycle logs (I7), `limiter_initial_clamped` fix (I10), metrics I13–I14 | S |
| N6 | ✅ Released `0.1.0a2` to PyPI (CI green, twine check, clean install verified) | S |
| N7 | ✅ Rate budget RPM/TPM + status view (F17–F18), lane buckets (F19), scenarios R01–R12 | L |
| N8 | ✅ Research assistant integration steps 1–8 on app branch `feat/llm-queue-integration` (`609f3aed`, flag off by default); 4 app tests pass incl. a `get_llm_model()` turn through SQS. Steps 9–11 (infra, alarms, canary) are DevOps / rollout | L |

### Final gate (2026-10-30)

| Check | Result |
|---|---|
| Full fakellm suite (S01–S57, R01–R12), one pass | **66 pass, 0 fail**, 3 skip (need LangChain / upstream / AWS) |
| Unit / integration tests | 602 / 159 pass; ruff, format, mypy clean; GitHub CI green |
| Samples 01–16 on the local stack | 15 / 15 exit 0 |
| Adapter tests on langchain-aws 0.2.24 (app pin) and 0.2.35 | 39 / 39 each (found and fixed a 0.2.24 serialization bug) |
| Real Bedrock subset (S01, S03, S16, S17, S29, S40, S41) | **blocked**: the AWS session token in `.env` has expired (`ExpiredTokenException`); S16, S17, S41 (no provider call) pass. Re-run after refreshing credentials: `SCN_PROVIDER=aws python -m tools.scenarios run S01 S03 S16 S17 S29 S40 S41` |

## 5. Using it in the research assistant

| Need in the app | Use | Sample | Guide step |
|---|---|---|---|
| Queue every chat / embedding call | `LLMQueue` + `QueuedChatBedrockConverse` / `QueuedBedrockEmbeddings` | 07, 03, 13 | Steps 3–6 |
| Live chat first | `request_context(lane="rt")` | 04 | Step 6 |
| Bucket per model from Bedrock quotas | `LLMQ_SETTINGS` `models.<alias>.max_concurrency` | 14 | Step 2b |
| Provider defaults (timeouts, retries) | `LLMQ_SETTINGS` `providers.bedrock` | 14 | Step 2c |
| Change limits during an incident | `llm-queue settings set ...` | 12_cli.md, 14 | Step 2c |
| Show "busy, retry in N s" | catch `ProviderError`, read `.meta`, `.retry_after_secs` | 15 | Step 6 |
| gpt20b has no fallback yet | `throttle_retry_attempts=10` for gpt20b | 15 | Step 2c |
| Ingestion jobs | `register_job` / `send_job`, `bulk` lane | 09 | Step 7 |
| Respect Bedrock RPM / TPM | **not available yet** (F17) | — | T4.x |
