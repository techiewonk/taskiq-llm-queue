# taskiq-llm-queue samples

Short, runnable scripts that use the public API against the local Docker stack. No AWS
account and no `.env` file are needed: all settings come from code, with `LLMQ_*` variables
as optional overrides (see `_local.py`). Only `13_research_assistant_flow.py` reads a `.env`
file or AWS Secrets Manager, and only to reach real Bedrock with `--target aws`.

## Prerequisites

From the package root (`libs/taskiq_llm_queue`):

```bash
uv sync --all-extras                                  # library + langchain/openai extras + dev tools
make up                                               # or: docker compose -f docker/compose.yaml up -d --wait
                                                      # Windows without make: uv run python tools/llmq_dev.py up
```

The stack runs:

| Service | Address | Used for |
|---|---|---|
| Valkey | `localhost:6379`, password `localdev` | limiter, breaker, claims, results, live config |
| ElasticMQ (fake SQS) | `http://localhost:9324` | queues from `docker/elasticmq.conf` |
| fakellm | `http://localhost:8080` | fake Bedrock (Converse, InvokeModel) and OpenAI-compatible API, plus `/admin/*` |

`_local.py` builds the `QueueConfig` every sample uses: key prefix `llmq:local`, models
`gpt120b` (targets `primary` and `apac`), `gpt20b`, `titan`, `cohere` (Bedrock) and `llama`
(OpenAI-compatible), and the default chain `gpt120b -> gpt20b`. It also sets dummy AWS
credentials and creates the two `llm-llama*` queues that `elasticmq.conf` lacks. To point at
something else, set the matching `LLMQ_*` variable (e.g. `LLMQ_REDIS_HOST`, `LLMQ_MODELS`) or
`FAKELLM_URL`.

## Samples

| File | Shows |
|---|---|
| `01_quickstart_converse.py` | `start(mode="both")`, one `converse` call, `result.meta`, `stop()` |
| `02_chat_openai_compatible.py` | `llmq.chat` and `QueuedChatOpenAI` against an OpenAI-compatible server |
| `03_embeddings.py` | `embed` with Titan, Cohere and OpenAI batching, `invoke_model`, `QueuedBedrockEmbeddings` |
| `04_lanes_and_request_context.py` | `request_context`, lane precedence, `rt` overtaking a `bg` backlog |
| `05_fallback_on_throttle.py` | fakellm throttles targets; `served_model` / `served_target` / `fallback_used`; `chain` vs `same_model` |
| `06_send_only_and_worker_pods.py` | `mode="send"` and `mode="work"`, in one process or as `send` / `work` processes |
| `07_langchain_bedrock.py` | `QueuedChatBedrockConverse`: `ainvoke`, prompt \| model chain, callbacks, `bind_tools` |
| `08_langgraph_agent.py` | a LangGraph tool-calling agent with a module-level queued model; `stream_mode="messages"` |
| `09_jobs.py` | `job_queues`, `register_job`, `send_job` (an ingest job embedding on the `bulk` lane) |
| `10_cancel_and_timeout.py` | `cancel(request_id)`, `timeout=`, `deadline=`, cancelling the awaiting task |
| `11_sync_calls.py` | `converse_sync` / `chat_sync` / `embed_sync`, sync LangChain calls, `strict_async` |
| `12_cli.md` | `llm-queue` commands: limiter, fallback, routing, job send; queue and DLQ depth |
| `13_research_assistant_flow.py` | the research-assistant app's 12 LLM/embedding flows (agent graph, guardrail, follow-ups, embeddings, nudge/ingest jobs) on the local stack or real Bedrock; see below |
| `14_settings_and_buckets.py` | settings per provider and model (`settings_default` = `LLMQ_SETTINGS`), value + source, bucket size, a live change without restart, a provider-wide bucket shared by two models |
| `15_retries_errors_and_rt_wait.py` | the retry curve, `exc.meta` on a failed call (attempts, queue wait, last target), `rt_capacity_wait_secs` live per model |
| `16_rate_limits_and_lane_caps.py` | RPM budget per minute (`fixed`), the rt reserve inside it, a bucket for the bulk lane (`lane_max_concurrency`), `llm-queue status` |
| `17_langgraph_rag_agent.py` | LangChain + LangGraph app-style graph: queued embeddings for retrieval, `with_structured_output` router, `bind_tools` agent loop with `fallback_policy="chain"`, a bg-lane summary node, `astream(stream_mode="updates")`, sync `.invoke()` from a thread |

## Running

```bash
.venv/Scripts/python samples/01_quickstart_converse.py      # Windows
.venv/bin/python samples/01_quickstart_converse.py          # Linux / macOS
uv run python samples/01_quickstart_converse.py             # either
```

Each script exits 0 and takes roughly 5-30 s. fakellm's `gpt120b` replies take about 2.5 s,
and `stop()` drains workers before returning. Helpers in `_local.py`:

```bash
python samples/_local.py env [--powershell]   # LLMQ_* exports for the llm-queue CLI
python samples/_local.py queues               # message counts per sample queue and DLQ
python samples/_local.py reset                # wipe llmq:local:* keys + reload fakellm's default scenario
```

Lint and type-check (samples import `_local` as a top-level module, so mypy needs the folder on
its path):

```bash
uv run ruff check samples && uv run ruff format --check samples
MYPYPATH=samples uv run mypy samples --strict
```

## Settings: change any sample without editing it

Every sample builds its config with `local_config()` (`_local.py`): the local defaults below,
overlaid with any `LLMQ_*` variable set in the environment, so the same variables as
production (README "Setting names") work for every script. `python samples/_local.py env`
prints the effective values.

| Variable | Local default | Change it to |
|---|---|---|
| `LLMQ_QUEUE_BACKEND` | `sqs` (ElasticMQ :9324) | `valkey`: every queue on Valkey Streams; `ensure_queues()` creates them |
| `LLMQ_QUEUE_REDIS_URL` | unset (queues on the state Valkey) | `redis://:localdev@localhost:6379/0` or a second Valkey |
| `LLMQ_REDIS_HOST` / `_PORT` / `_PASSWORD` / `_SSL` | `localhost` / `6379` / `localdev` / `false` | another Valkey |
| `LLMQ_SQS_ENDPOINT_URL` | `http://localhost:9324` | another SQS endpoint (unset with real AWS credentials: AWS SQS) |
| `LLMQ_BEDROCK_ENDPOINT_URL` | fakellm `http://localhost:8080` | unset for real Bedrock (see 13) |
| `LLMQ_KEY_PREFIX` | `llmq:local` | a private prefix per run |
| `LLMQ_MODELS`, `LLMQ_FALLBACK_CHAINS` | the five sample aliases, `gpt120b -> gpt20b` | your own JSON |
| `LLMQ_SETTINGS` | none (14-16 set theirs in code) | per provider / model JSON (README "Settings per provider and model") |
| `LLMQ_SQS_WAIT_TIME_SECS`, `LLMQ_RETRY_BASE_SECS`, `LLMQ_CONSUMER_DRAIN_SECS`, `LLMQ_RESULT_DISPATCH_MS` | `2`, `0.2`, `5`, `rt:50,bg:100,bulk:200` (fast exits) | production defaults: unset |

### Run every sample on Valkey Streams instead of SQS

```bash
export LLMQ_QUEUE_BACKEND=valkey                       # PowerShell: $env:LLMQ_QUEUE_BACKEND = 'valkey'
for f in samples/[01][0-9]_*.py; do .venv/Scripts/python "$f" > /dev/null && echo "ok   $f" || echo "FAIL $f"; done
python samples/_local.py queues                        # counts from the Valkey queues
python samples/_local.py reset                         # also wipes the Valkey queue keys
```

Without the variable the same loop runs on ElasticMQ. Both are part of the release checks.

## Expected output

fakellm replies are deterministic text such as `fakellm reply 67fe302d948bb63f`. Timings, pod
ids and request ids vary. For example:

```text
# 01_quickstart_converse.py
reply       : fakellm reply 67fe302d948bb63f
stop reason : end_turn
usage       : {'inputTokens': 15, 'outputTokens': 8, 'totalTokens': 23, ...}
served by   : gpt120b@primary (fallback_used=False, attempts=1)
timings     : queue_wait_ms=437 provider_ms=3375
worker      : pod-b1250c42 (this pod: pod-b1250c42)

# 04_lanes_and_request_context.py
rt :  4 calls, mean queue_wait_ms = 757
bg : 24 calls, mean queue_wait_ms = 4889

# 05_fallback_on_throttle.py
healthy                            -> served gpt120b@primary fallback_used=False attempts=1
primary throttled, same_model      -> served gpt120b@apac fallback_used=True attempts=4
both targets throttled, chain      -> served gpt20b@primary fallback_used=True attempts=7
both targets throttled, same_model -> ProviderError(retryable=True): ThrottlingException: ...

# 06_send_only_and_worker_pods.py
sender 'sender' got 12 results; served by: {'worker-1': 11, 'worker-2': 1}

# 10_cancel_and_timeout.py
1. cancel(request_id)   -> LLMQueueError call cancel-demo-... was cancelled
2. timeout=0.5          -> LLMQueueTimeout after 0.7s: no result for task llm-...
```

## 13: research-assistant flows

Replays every LLM / embedding call site of the research-assistant app (`src/`) through the
queue, one function per flow, printing the request summary, reply, `served_model@served_target`,
fallback, attempts, `queue_wait_ms`, `provider_ms`, token usage and latency. Exits 1 if any flow
fails.

```bash
python samples/13_research_assistant_flow.py                      # all flows, Docker stack
python samples/13_research_assistant_flow.py --flow guardrail     # one flow
python samples/13_research_assistant_flow.py --target aws         # real Bedrock, local queue
```

Flows: `agent_graph`, `human_condense`, `guardrail`, `response_fix`, `followup_writer`,
`offline_converse`, `followup_embed`, `query_embed`, `safe_embedding`, `memory_write`,
`nudge_job`, `ingest_job` (the docstring maps each to its app file and line).

`--target aws` calls real Bedrock with the app's model ids; the queue stays on the local
Valkey + ElasticMQ unless `LLMQ_*` variables say otherwise. Settings come from
`--env-source` (values are never printed; only key names and their source):

| `--env-source` | Reads |
|---|---|
| `auto` (default) | `.env`; on `--target aws` also the app secret when `ENV` or `SECRET_NAME_TEMPLATE` is set (a failure is reported and the run continues) |
| `dotenv` | `LLMQ_ENV_FILE`, else `<package root>/.env` |
| `secrets` | the app's Secrets Manager secret `SECRET_NAME_TEMPLATE.format(env=ENV)` (default `{env}/researchassistant/secret`, region `AWS_REGION`, default `ap-south-1`); a failure exits 1 |
| `none` | the process environment only |

Precedence: the process environment always wins; the secret wins over `.env` (as in the
app, where Secrets Manager is the source of truth and environment variables only a local
fallback); `.env` fills the rest. Variables used: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION`,
`AWS_DEFAULT_REGION`, `AWS_MODEL_REGION` (chat region), `AWS_EMBEDDING_REGION`,
`BEDROCK_SERVICE_TIER`, `ENV`, `SECRET_NAME_TEMPLATE`, `LLMQ_ENV_FILE`, any `LLMQ_*`, and with
`LLMQ_SAMPLES_APP_VALKEY=true` the app's `VALKEY_HOST` / `VALKEY_PORT` / `VALKEY_KEY` /
`VALKEY_SSL` (or the secret's `valkey_host` / `valkey_port` / `valkey_key`) for the queue.

```bash
# .env only (default file: libs/taskiq_llm_queue/.env)
python samples/13_research_assistant_flow.py --target aws --env-source dotenv
# Secrets Manager only (credentials from the shell or AWS_PROFILE)
ENV=dev python samples/13_research_assistant_flow.py --target aws --env-source secrets
# both: .env supplies the credentials, the secret the app settings
ENV=dev python samples/13_research_assistant_flow.py --target aws
```

If AWS credentials are missing or expired, the first failing flow prints the error class and
message and the run stops (no retries).

## Resetting fakellm and shared state

fakellm keeps behaviour patches (`PATCH /admin/models/{key}`) until the scenario is replaced;
`POST /admin/reset` clears counters only. `05_fallback_on_throttle.py` saves and restores the
scenario itself. To reset by hand:

```bash
curl -X PUT localhost:8080/admin/scenario -H 'content-type: application/yaml' \
     --data-binary @tools/fakellm/scenarios/default.yaml    # default behaviour + clear counters
curl -X POST localhost:8080/admin/reset                      # counters only
curl localhost:8080/admin/stats                              # calls / throttled / errors per model key
curl -X PATCH localhost:8080/admin/models/openai.gpt-oss-120b-1:0@ap-south-1 \
     -H 'content-type: application/json' -d '{"throttle_rate": 1.0}'   # throttle one target
```

Learned limits and breaker state live in Valkey and survive between runs. After `05`, the
throttled `gpt120b` targets start with a low limit and may serve from `apac` for a short while.
Run `python samples/_local.py reset` to start clean, which also reloads fakellm's default
scenario.
