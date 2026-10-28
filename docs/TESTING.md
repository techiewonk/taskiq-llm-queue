# taskiq-llm-queue — local testing guide (draft)

Everything runs locally in Docker. No AWS account is needed: **ElasticMQ** stands in for SQS, **fakellm** stands in for Bedrock and OpenAI-compatible APIs, and **Valkey** stands in for ElastiCache. Unit tests use **moto** in server mode.

> Status: pre-alpha. The Docker core stack and the SQS capability check exist today; fakellm, workers, Locust and dashboards are being built.

## 1. The stack

| Service | Image (slim) | Size | Port | Profile | Purpose |
|---|---|---|---|---|---|
| `valkey` | `valkey/valkey:8.1-alpine` | ~18 MB | 6379 | core | Limiter, breaker, claims, results, live config (prod runs Valkey 8.x) |
| `sqs` | `softwaremill/elasticmq-native:1.7.1` | ~33 MB | 9324 (API), 9325 (UI) | core | Fake SQS: queues + DLQs from `elasticmq.conf` |
| `fakellm` | built from `python:3.12-slim` | ~60 MB | 8080 | core | Fake Bedrock (Converse, InvokeModel) + OpenAI (chat, embeddings), scriptable |
| `worker` | built from `python:3.12-slim` | ~120 MB | 8000 | core | Demo app using the package (sends + executes); `--scale worker=3` for cross-pod tests |
| `locust` | `locustio/locust:2.47.0` | ~90 MB | 8089 | load | Load generator (lane mix, 30K calls/min) |
| `prometheus` | `prom/prometheus:v3.15.1` | ~114 MB | 9090 | obs | Scrapes `worker` and `fakellm` `/metrics` |
| `grafana` | `grafana/grafana-oss:13.0.2` | ~350 MB | 3000 | obs | Pre-provisioned dashboards (queues, limiter, fallback, latency) |

Profiles keep the default run small: `make up` starts **core** only (~250 MB of images). `make up-load` adds Locust; `make up-obs` adds Prometheus + Grafana.

```yaml
# libs/taskiq_llm_queue/docker/compose.yaml (planned)
name: llmq-test
services:
  valkey:
    image: valkey/valkey:8.1-alpine
    # Password required and TLS off locally, like a typical managed Redis with auth
    command: ["valkey-server", "--requirepass", "${LLMQ_REDIS_PASSWORD:-localdev}", "--save", "", "--appendonly", "no"]
    ports: ["${VALKEY_PORT:-6379}:6379"]
    healthcheck: {test: ["CMD", "valkey-cli", "-a", "${LLMQ_REDIS_PASSWORD:-localdev}", "ping"], interval: 2s, retries: 15}
  sqs:
    image: softwaremill/elasticmq-native:1.7.1
    volumes: ["./elasticmq.conf:/opt/elasticmq.conf:ro"]
    ports: ["${SQS_PORT:-9324}:9324", "9325:9325"]
    healthcheck: {test: ["CMD", "wget", "-qO-", "http://localhost:9324/?Action=GetQueueUrl&QueueName=llm-gpt20b"], interval: 2s, retries: 30}
  fakellm:
    build: {context: .., dockerfile: docker/fakellm.Dockerfile}
    environment: {FAKELLM_SCENARIO: /scenarios/${SCENARIO:-default}.yaml}
    volumes: ["./scenarios:/scenarios:ro"]
    ports: ["8080:8080"]
  worker:
    build: {context: .., dockerfile: docker/worker.Dockerfile}
    environment:
      LLMQ_REDIS_HOST: valkey
      LLMQ_REDIS_PORT: "6379"
      LLMQ_REDIS_PASSWORD: ${LLMQ_REDIS_PASSWORD:-localdev}
      LLMQ_REDIS_SSL: "false"
      LLMQ_KEY_PREFIX: llmq:local
      FAKELLM_SEED: ${FAKELLM_SEED:-42}
      LLMQ_SQS_ENDPOINT_URL: http://sqs:9324
      LLMQ_BEDROCK_ENDPOINT_URL: http://fakellm:8080
      LLMQ_OPENAI_BASE_URL: http://fakellm:8080/v1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: ap-south-1
    depends_on: {valkey: {condition: service_healthy}, sqs: {condition: service_healthy}, fakellm: {condition: service_started}}
    deploy: {replicas: 3}
    stop_grace_period: 90s          # fits the ordered drain
  locust:
    image: locustio/locust:2.47.0
    profiles: ["load"]
    volumes: ["./locust:/mnt/locust:ro"]
    command: ["-f", "/mnt/locust/locustfile.py", "--host", "http://worker:8000"]
    ports: ["8089:8089"]
  prometheus:
    image: prom/prometheus:v3.15.1
    profiles: ["obs"]
    volumes: ["./prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]
  grafana:
    image: grafana/grafana-oss:13.0.2
    profiles: ["obs"]
    volumes: ["./grafana:/etc/grafana/provisioning:ro"]
    ports: ["3000:3000"]
```

### Fake SQS: ElasticMQ queues

```hocon
# libs/taskiq_llm_queue/docker/elasticmq.conf (planned) — same names/settings as prod §12.1
include classpath("application.conf")
node-address { protocol = http, host = sqs, port = 9324, context-path = "" }
rest-sqs { enabled = true, bind-port = 9324, bind-hostname = "0.0.0.0", sqs-limits = strict }
queues {
  "llm-gpt120b-rt"     { defaultVisibilityTimeout = 60 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-gpt120b-rt-dlq", maxReceiveCount = 10 } }
  "llm-gpt120b"        { defaultVisibilityTimeout = 60 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-gpt120b-dlq",    maxReceiveCount = 10 } }
  "llm-gpt20b-rt"      { defaultVisibilityTimeout = 60 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-gpt20b-rt-dlq",  maxReceiveCount = 10 } }
  "llm-gpt20b"         { defaultVisibilityTimeout = 60 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-gpt20b-dlq",     maxReceiveCount = 10 } }
  "llm-embed-rt"       { defaultVisibilityTimeout = 30 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-embed-rt-dlq",   maxReceiveCount = 10 } }
  "llm-embed"          { defaultVisibilityTimeout = 30 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-embed-dlq",      maxReceiveCount = 10 } }
  "llm-embed-bulk"     { defaultVisibilityTimeout = 30 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "llm-embed-bulk-dlq", maxReceiveCount = 10 } }
  "llm-dup-test"       { defaultVisibilityTimeout = 2 seconds,  receiveMessageWait = 1 second }      # S14 only
  "jobs-ingest"        { defaultVisibilityTimeout = 300 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "jobs-ingest-dlq",   maxReceiveCount = 5 } }
  "jobs-followup"      { defaultVisibilityTimeout = 300 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "jobs-followup-dlq", maxReceiveCount = 5 } }
  "jobs-nudge"         { defaultVisibilityTimeout = 300 seconds, receiveMessageWait = 20 seconds, deadLettersQueue { name = "jobs-nudge-dlq",    maxReceiveCount = 5 } }
  "llm-gpt120b-rt-dlq" {}  "llm-gpt120b-dlq" {}  "llm-gpt20b-rt-dlq" {}  "llm-gpt20b-dlq" {}
  "llm-embed-rt-dlq" {}  "llm-embed-dlq" {}  "llm-embed-bulk-dlq" {}  "jobs-ingest-dlq" {}  "jobs-followup-dlq" {}  "jobs-nudge-dlq" {}
  "parity-fifo.fifo"   { fifo = true, contentBasedDeduplication = true }    # parity tests only
}
```

- Browse the queues at http://localhost:9325, or run `aws --endpoint-url http://localhost:9324 sqs get-queue-attributes --queue-url http://localhost:9324/000000000000/llm-gpt120b --attribute-names All`.
- **Things to verify on ElasticMQ in week 1:** that botocore 1.43's JSON protocol works against ElasticMQ, plus `ChangeMessageVisibilityBatch`, `DeleteMessageBatch`, delays, FIFO, `ApproximateReceiveCount` and DLQ redrive. Anything ElasticMQ lacks is covered by moto server tests and listed in "Known fake-vs-AWS gaps" below.

**Unit tests use moto in server mode.** `aiobotocore` doesn't go through botocore's HTTP layer, so moto's in-process decorator can't intercept it. A session-scoped `ThreadedMotoServer` fixture is used instead, and each test points `endpoint_url` at it.

### Fake LLM: `fakellm`

A small FastAPI app shipped with the package (open source; other users can use it in their own tests).

| Endpoint | Speaks | Notes |
|---|---|---|
| `POST /model/{modelId}/converse` | Bedrock Converse | Text, tool-use replies, `usage` incl. cache fields, `stopReason` |
| `POST /model/{modelId}/invoke` | Bedrock InvokeModel | Titan v2 (`inputText`, `dimensions`) and Cohere v3 (`texts`, `input_type`) bodies; **deterministic** vectors (hash of text), so tests can compare |
| `POST /v1/chat/completions` | OpenAI-compatible | Messages, tools, `usage`; `429` + `Retry-After` |
| `POST /v1/embeddings` | OpenAI-compatible | Deterministic vectors |
| `GET /admin/scenario`, `PUT /admin/scenario` | Admin | Read / replace the whole scenario |
| `PATCH /admin/models/{model}` | Admin | Change one model's behaviour mid-test |
| `POST /admin/reset`, `GET /admin/stats` | Admin | Counters: calls, concurrent, throttled, errors per model/target |
| `GET /metrics` | Prometheus | Same counters |

Errors look like the real thing:
- Bedrock: HTTP 429 with `x-amzn-ErrorType: ThrottlingException`, 503 `ServiceUnavailableException`, 424/429 `ModelNotReadyException`, 400 `ValidationException`.
- OpenAI: 429 + `Retry-After`, 5xx.

**How fakellm tells targets apart:** every Bedrock region points at the same `LLMQ_BEDROCK_ENDPOINT_URL`, so fakellm reads the region from the SigV4 `Authorization` credential scope (`Credential=…/<region>/bedrock/aws4_request`). It accepts but never verifies signatures, and URL-decodes `{modelId:path}` (`%3A`, profile ARNs with `/`). A fakellm unit test sends an aiobotocore-signed request per target.

**Error fidelity** (each one checked by an SDK-parse test: botocore `ClientError.response["Error"]["Code"]`, openai exception class):

| Error | HTTP | Header / body |
|---|---|---|
| `ThrottlingException` | 429 | `x-amzn-ErrorType: ThrottlingException`, `{"message": "..."}` |
| `ModelNotReadyException` | 429 | same pattern |
| `ServiceUnavailableException` | 503 | same pattern |
| `InternalServerException` | 500 | same pattern |
| `ModelTimeoutException` | 408 | same pattern |
| `ModelErrorException` | 424 | same pattern |
| `ValidationException` | 400 | same pattern |
| `AccessDeniedException` | 403 | same pattern |
| `ResourceNotFoundException` | 404 | same pattern |
| OpenAI rate limit | 429 | `{"error": {"message", "type", "code"}}`, `Retry-After`, `retry-after-ms`, `x-ratelimit-*` |
| OpenAI server error | 500/503 | same body shape |

**Converse responses** include `output.message`, `stopReason`, `usage` (with cache fields) and `metrics.latencyMs`. **Fixtures:** real responses recorded once in dev AWS are replayed by fakellm, and a contract test diffs fakellm output against them, so parity is checked against reality rather than the fake. **Determinism:** `seed:` in the scenario file / `FAKELLM_SEED`; replies are a hash of the input; latency is drawn from the seeded generator.

**Scenario file**, one block per model *and target* (region or cross-region profile):

```yaml
# scenarios/default.yaml
seed: 42
defaults: {latency_ms: {p50: 800, p95: 2500}, error_rate_5xx: 0.0}
models:
  "openai.gpt-oss-120b-1:0@ap-south-1":
    max_concurrency: 50          # more in flight -> ThrottlingException (what the limiter must learn)
    rpm: 3000                    # optional request-rate cap
    latency_ms: {p50: 2500, p95: 5000}
    tool_calls: echo             # reply with a toolUse block when tools are sent
  "apac.openai.gpt-oss-120b-1:0":            # cross-region profile target
    max_concurrency: 80
  "openai.gpt-oss-20b-1:0@ap-south-1":
    max_concurrency: 100
    latency_ms: {p50: 900, p95: 2000}
  "amazon.titan-embed-text-v2:0@ap-south-1": {max_concurrency: 200, latency_ms: {p50: 60, p95: 150}, dims: 1024}
  "cohere.embed-english-v3@ap-south-1":      {max_concurrency: 100, latency_ms: {p50: 80, p95: 200}, dims: 1024}
  "llama-3.1-70b":                            # OpenAI-compatible
    max_concurrency: 30
phases:                          # optional timeline: change behaviour during a run
  - {at_s: 180, model: "openai.gpt-oss-120b-1:0@ap-south-1", set: {throttle_rate: 1.0}}
  - {at_s: 300, model: "openai.gpt-oss-120b-1:0@ap-south-1", set: {throttle_rate: 0.0}}
```

### Fake capability table (measured 2026-10-28)

Measured with `libs/taskiq_llm_queue/tests/capability/check_sqs_capabilities.py` (aiobotocore 3.10.0, botocore 1.43.117):

| SQS feature | ElasticMQ 1.7.1 | moto 5.2.5 (server) | Used by |
|---|---|---|---|
| JSON protocol (CreateQueue / GetQueueUrl) | ✅ | ✅ | everything |
| Message attributes; `ApproximateReceiveCount` | ✅ | ✅ | DS1–DS5, S14 |
| `ChangeMessageVisibilityBatch` | ✅ | ✅ | heartbeat (DS6) |
| `DeleteMessageBatch` + partial failure in `Failed[]` (`ReceiptHandleIsInvalid`) | ✅ | ✅ | ack batching (DS7) |
| `DelaySeconds` (standard) | ✅ | ✅ | P5 |
| Long poll returns on arrival (1.0 s) / empty poll waits full time | ✅ | ✅ | A3, S33 |
| 1 MiB message accepted, 1 MiB + 1 rejected | ✅ (`InvalidAttributeValue`) | ✅ (`InvalidParameterValue`) | DS15, S18 |
| `SendMessageBatch` > 1 MiB total rejected | ✅ (`InvalidAttributeValue`) | ✅ (`BatchRequestTooLong`) | DS15 |
| FIFO + dedup; per-message delay rejected on FIFO | ✅ | ✅ | P6, DS19 |
| `MessageGroupId` on standard queues (fair queues) | ✅ | ✅ | P7 |
| Redrive to DLQ after `maxReceiveCount` | ✅ | ✅ | DS5, S26 |
| `StartMessageMoveTask` (DLQ redrive back) | ✅ | ❌ (not implemented) | S26 → ElasticMQ only |
| `PurgeQueue` | ✅ | ✅ | isolation fixture |

**Rules from the results:**
- ElasticMQ is the default fake for integration and scenarios. moto server runs the unit tests, except anything needing `StartMessageMoveTask`.
- **Error codes differ between fakes (and from AWS)**, so the broker classifies errors by family (size limit, invalid handle, throttled, not found), never by exact code string. Unit tests cover all three spellings.

## 2. Make targets (local only for now)

| Command | Does |
|---|---|
| `make up` / `make down` | Start / stop the core stack (`docker compose up -d --wait`) |
| `make up-load`, `make up-obs` | Add Locust / Prometheus + Grafana |
| `make test-unit` | `uv run pytest -m unit` (moto server, fakeredis), no Docker |
| `make test-int` | `uv run pytest -m integration` against the running stack |
| `make scenario S=<id>` | Run one named scenario (below); prints pass/fail + key numbers |
| `make scenarios` | Run all scenarios in order |
| `make load` | Headless Locust run (default: 30K calls/min, 15 min, lane mix 60/30/10) |
| `make chaos C=<kill-worker\|stop-sqs\|stop-valkey\|pause-fakellm>` | Fault injection helpers |
| `make logs`, `make reset` | Tail logs; clear queues + Valkey + fakellm counters |
| `make test` | `make test-unit && make test-int` |
| `make test-lua` | Limiter/claim/breaker Lua tests against the real `valkey` container (fakeredis's Lua differs) |
| `make matrix` | Version matrix: pinned versions + latest langchain-aws, langchain-openai, langgraph, taskiq; runs adapter unit tests + S01, S04 |
| `make package-smoke` | Build the wheel; install core-only and each extra into clean venvs; smoke imports; grep-gate for `src\.` / internal names |

**Windows:** every target also runs as `uv run llmq-dev <target>` (no `make` needed). The repo adds `.gitattributes` (`*.sh *.conf *.lua *.yaml eol=lf`). Give Docker Desktop / WSL2 at least **6 GB**. Override ports through `.env` (`VALKEY_PORT`, `SQS_PORT`, …) if 6379/9324/3000 are taken.

## 3. Scenarios (the release gate)

No coverage gate: **every scenario must pass before a release.** Unit tests are still written for core logic (limiter math, scheduler, envelope, routing).

**Harness rules:**
- **Each scenario is a pytest test.** It reads numbers from `GET fakellm /admin/stats`, worker `/metrics` (`llmq_queue_wait_seconds`, `llmq_limit`, `llmq_inflight`, `llmq_fallback_total`, `llmq_sync_fallback_total`) and `/health`.
- **Timing criteria** have a tolerance band and a retry-once rule.
- **Isolation fixture before every scenario:** purge all queues, `FLUSHDB` the `llmq:local:*` keys, `POST /admin/reset` (which also restarts the phase clock), then `PUT /admin/scenario`. Scenario order must not matter.
- **Tags:** `fast` (target ≤ 15 min for the whole fast set) and `slow` (S13, S23, S27, S31, S32).
- **"Identical to direct"** means equal after `normalize()`, which strips `latencyMs`, run ids, timestamps and `ResponseMetadata`, and keeps callback events in order. Golden outputs are stored.

| ID | Scenario | Setup | Pass when |
|---|---|---|---|
| S01 | Chat round trip (Bedrock) | 1 worker, `rt` | `AIMessage` identical to the `direct` path (content, tool calls, usage, `model_name`) |
| S02 | Chat round trip (OpenAI-compatible) | `llama-3.1-70b` | `QueuedChatOpenAI` result identical to plain `ChatOpenAI` against fakellm |
| S03 | Embeddings (Titan, Cohere, OpenAI) | batch of 100 texts | Same vectors as direct; sends batched (≤ 10/call) |
| S04 | LangChain/LangGraph drop-in | Sample graph: `prompt \| trimmer \| model.bind_tools()`, `astream(stream_mode=["custom"])`, Langfuse callback stub | Graph output + callback events identical to direct; `stream_mode="messages"` gives one full message, no bypass |
| S05 | Cross-pod execution | 1 fixed sender pod, 3 workers | Each worker executes ≥ 15 % of tasks (from `meta.worker`); all results delivered |
| S06 | Priority | 10K `bg` queued, then `rt` at 5/s | `rt` queue wait p95 ≤ 2 s; `bg` still progresses (`min_slots_for_lower`) |
| S07 | Limit learning | gpt120b `max_concurrency: 50`, offered 200 | Limiter settles within 60 s at 40–55; throttle rate < 2 % after that; nothing fails |
| S08 | Fast start | Stored limit wiped, `LLMQ_AC_INITIAL=40` | Reaches 50 ± 5 within 20 s (doubling) |
| S09 | Pause gate | gpt120b `throttle_rate: 1.0` for 30 s | Workers stop receiving `llm-gpt120b*`; gpt20b throughput unchanged; resumes after |
| S10 | **Fallback: profile then model** | region throttled 100 %, profile healthy, then profile throttled too | `served_target=…@apac` first; then `chain` calls → gpt20b, `same_model` calls → retryable error after retries |; `llm-gpt120b` queue keeps draining while the region is throttled (pause is per target, DS13) |
| S11 | **Live fallback change** | `llm-queue fallback set` mid-run | All workers use the new chain within 30 s; an invalid JSON is rejected and the last good chain stays |
| S12 | Circuit breaker | 5xx 100 % on gpt20b | Opens within 30 s, half-open after 15 s, closes when healthy |
| S13 | Worker killed mid-task | `make chaos C=kill-worker` during long calls | Each task produces exactly one result; reclaimed ≤ ~60 s |
| S14 | Duplicate delivery | Queue `llm-dup-test` (visibility 2 s), heartbeat **off**, task 5 s | Claim keeps the copy invisible (DS2); fakellm `calls == 1`; `receive_count ≥ 2` seen |
| S15 | Graceful shutdown | `docker compose stop worker` (30 s) | Unstarted messages released at once (picked up < 5 s by others) |
| S16 | Expiry / deadline | `rt` deadline 2 s, workers paused 5 s | Expired messages deleted unrun; client got `LLMQueueTimeout` |
| S17 | Cancellation | cancel 50 in-flight ids | Not executed if not started; no results stored |
| S18 | Large params | 300 KB prompt | Stored by reference; result OK; `TTL llmq:local:req:<id>` within 1790–1800 s and the result key ≈ 300 s (no waiting); optional run with `LLMQ_STATE_TTL_S=10` shows real expiry |
| S19 | Canary routing | `routing set gpt20b rt 10` | ~10 % of sessions on `sqs`, sticky per session; kill switch → 100 % `direct` in ≤ 30 s |
| S20 | **SQS down** | `make chaos C=stop-sqs` | Calls switch to `direct` for that model within one send failure; alarm event; back to `sqs` after recovery + failover window |
| S21 | **Valkey down** | `make chaos C=stop-valkey` | Calls run `direct` with the local per-pod cap; no crash; back to normal when Valkey returns |
| S22 | Sync callers | `embed_query` (sync) from a thread and from the event loop | Runs `direct` + limiter, logs `sync_fallback`; event loop never blocked |
| S23 | Jobs | `llm-queue job send` ingest job of 5K texts | Embeddings go `bulk`; in-flight ≤ cap; job status `done` |
| S24 | Broker parity | Upstream examples: multi-queue, delay, FIFO + group/dedup, fair queue, batching + flush, expiry, metadata, heartbeat, S3 offload (MinIO, optional) | Same observable behaviour as the upstream libraries' examples |
| S25 | Custom provider | Register a toy provider | Routed, limited, fallback-able like built-ins |
| S26 | DLQ + redrive | Poison message (raises on decode) | Lands in DLQ after 3 receives; redrive script moves it back |
| S27 | Load | `make load` 30K calls/min, 15 min, lane mix + an ingest job | Zero user-visible throttles; `rt` wait p95 ≤ 2 s; Valkey CPU < 60 %; worker CPU noted (R14, R28) | **Locally informational; a gate only in dev AWS** |

| S28 | Malformed input | Raw messages via boto3: bad JSON, unknown task name, envelope `v=0` / `v=2`, missing `params_ref` key, invalid `SQSQueue` config (P16 limits), invalid routing/fallback JSON, model not on the allow-list | Unknown task/version released, not acked (DS9); typed exceptions; `*_config_invalid` metrics; DLQ only for poison |
| S29 | No secrets or content leaks | Peek every message sent during S01–S03; capture logs | No `AKIA`, `aws_secret`, `Authorization`, `api_key`, `sk-` in messages; no prompt/response text in logs; ids hashed in labels |
| S30 | SDK retries off + `Retry-After` | fakellm throttles once (Bedrock and OpenAI) | fakellm `calls == 1` per attempt; backoff ≥ `Retry-After`; limiter saw the throttle |
| S31 | 1K concurrent waiters | 1,000 awaiting calls on one pod | Valkey ops/s from `INFO commandstats` within budget (batched dispatcher `MGET`); no event-loop lag > 100 ms |
| S32 | Rolling deploy under load | `docker compose up -d --force-recreate worker` one by one during S27-lite | Zero DLQ messages; < 1 % rt timeouts; released work picked up < 5 s |
| S33 | Broker call counts | SQS call-counting proxy / moto request log | Idle: ≤ 1 `ReceiveMessage` per queue per 20 s per worker; received ≤ free slots; `DeleteMessageBatch` and `ChangeMessageVisibilityBatch` used |
| S34 | Bulk isolation | 20K `bulk` embeddings on `llm-embed-bulk` + `bg` memory writes | `bg` wait p95 ≤ 60 s while the bulk backlog drains |

## 4. Verify fallback (step by step)

```bash
make up SCENARIO=default
uv run llm-queue fallback get --model gpt120b                         # expect: gpt120b -> gpt120b@apac -> gpt20b
curl -X PATCH localhost:8080/admin/models/openai.gpt-oss-120b-1:0@ap-south-1 -d '{"throttle_rate":1.0}'
uv run python examples/chat_fallback.py --policy chain                # prints served_target=apac.openai.gpt-oss-120b-1:0
curl -X PATCH localhost:8080/admin/models/apac.openai.gpt-oss-120b-1:0 -d '{"throttle_rate":1.0}'
uv run python examples/chat_fallback.py --policy chain                # prints served_model=openai.gpt-oss-20b-1:0, fallback_used=true
uv run python examples/chat_fallback.py --policy same_model           # prints LLMQueueError(retryable=True)
uv run llm-queue fallback set --model gpt120b --chain "gpt20b"        # skip the profile live
curl localhost:8080/admin/stats                                       # per-target call/throttle counts confirm the path
curl -X POST localhost:8080/admin/reset
```

In Grafana (`make up-obs`, http://localhost:3000), the **Fallback** panel shows `llmq_fallback_total{model,step}`, and the **Limiter** panel shows `limit` / `inflight` per target.

## 5. Known fake-vs-AWS gaps (keep in mind)

| Area | Fake | Real AWS | Covered by |
|---|---|---|---|
| IAM / IRSA, SSE-KMS | Not enforced | Enforced | Dev smoke test on real SQS + Bedrock once DevOps creates queues |
| SQS throughput limits, latency | Local, fast | Network, per-API throttling | Load test repeated in dev AWS before prod |
| Bedrock quotas | `max_concurrency` model in fakellm | Real RPM/TPM, cross-region behaviour | Limiter learns in dev; canary in prod |
| ElasticMQ API gaps (if any found) | — | — | moto server tests for those calls |

## 6. Real smoke test (dev AWS, after DevOps delivers queues)

Run `make smoke-aws ENV=dev` with real SQS + Bedrock (small volume): S01, S03, S05, S10 (with real throttling only if quota is low) and S13. This confirms IRSA, encryption and the real error shapes match fakellm.
