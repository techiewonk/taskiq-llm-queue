# taskiq-llm-queue

LLM calls through a queue on Valkey or Redis, under RPM / TPM budgets per **deployment**
(model x account x region), balanced across regions and accounts, with fallback regions,
fallback models, health checks and status. Works with Amazon Bedrock and OpenAI-compatible
servers (vLLM), and with LangChain.

Version 0.3 is a clean break from 0.2: no taskiq broker, no SQS, no lanes.

| Document | What it is |
|---|---|
| [docs/ARCHITECTURE.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/ARCHITECTURE.md) | How it works: components, the life of a call, keys, failure model, measured performance |
| [docs/ACCOUNT_ROUTING_PLAN.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/ACCOUNT_ROUTING_PLAN.md) | The design and its decisions; §13 is the implementation status and the gate results |
| [docs/ACCOUNT_ROUTING_WALKTHROUGH.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/ACCOUNT_ROUTING_WALKTHROUGH.md) | A walkthrough with sample data |
| [samples/README.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/samples/README.md) | Runnable samples, with where to use each |
| [CHANGELOG.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/CHANGELOG.md), [STABILITY.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/STABILITY.md) | What changed; what is public and how it may change |
| [CONTRIBUTING.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/CONTRIBUTING.md), [docs/RELEASING.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/RELEASING.md), [docs/RELEASE_PLAN_0.3.0.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/RELEASE_PLAN_0.3.0.md) | How to work on it; how a release goes out; the 0.3.0 release checklist |

## How a call flows

1. The app calls `llmq.converse(...)` (or a LangChain adapter). The call is registered (a
   repeated request id runs once) and put on its model's queue: `<name>-priority` for priority
   calls, `<name>` for the rest. The send rings the queue's doorbell, which wakes one idle
   worker at once.
2. A worker (in the same pods) reads the priority queue first and admits each call with one
   store call: pick a deployment by the model's strategy and take one request from RPM, the
   token estimate from TPM, a concurrency slot and the account budget, all or nothing.
3. A call without budget waits (its class stops being read; older calls keep their place) and
   spills to fallback regions and then fallback models when the registry's rules allow.
4. The provider is called on the deployment's account and region; tokens are corrected to the
   real usage; the result goes back to the caller (written and announced in one store call).

Measured on fakellm with worker processes (plan §13): the queue path adds 0-15 ms at p95 over
a direct call, about 11 store calls per call; admission p99 under 1 ms with 30 deployments per
model; budgets held exactly (1,800 of 1,800 a minute); a 45 s store outage loses no waiting call.

## Install

```
pip install "taskiq-llm-queue[langchain-aws]"        # Bedrock + LangChain
pip install "taskiq-llm-queue[openai,langchain-openai]"  # OpenAI-compatible
pip install "taskiq-llm-queue[http]"                  # status routes for FastAPI
```

## Configure

Two things, both from the environment or a secret (`LLMQ_*`):

| Setting | Meaning |
|---|---|
| `LLMQ_STORE_URL` | The state store. Outside `dev` / `local` / `test` / `ci` it must be `rediss://` with the certificate checked |
| `LLMQ_STORE_PASSWORD`, `LLMQ_QUEUE_STORE_PASSWORD` | Optional: read from `from_env(secrets=)` on every new connection (rotation) |
| `LLMQ_QUEUE_STORE_URL` | Optional separate queue store |
| `LLMQ_ENVIRONMENT` | `dev` (default), `local`, `test`, `ci`, or any other name (then TLS and the salt are required) |
| `LLMQ_ID_HASH_SALT` | Key of the user / session id hash (HMAC); required outside dev |
| `LLMQ_STORE_MAX_CONNECTIONS` | Store pool size (default 200: worker slots + callers + 3) |
| `LLMQ_QUEUE_FAILOVER_SECS` | With a separate queue store: errors this long (default 5) before calls run direct |
| `LLMQ_QUEUE_WAIT_REPLICAS`, `LLMQ_QUEUE_WAIT_MS` | A send waits for this many replicas (default 0: off) for this long (100 ms; below the store read timeout, see Tuning) |
| `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` | Allows a model's `debug_capture` (prompt text in the store); refused in prod |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | Names of this environment's keys and queues |
| `LLMQ_REGISTRY` (or `LLMQ_REGISTRY_FILE`) | The registry document (below) |
| `LLMQ_REGISTRY_LIVE_EDIT` | `safe` (default), `all`, `off` |
| `LLMQ_MODE` | `both` (default), `caller`, `worker` |
| `LLMQ_METRICS_SINK` | `emf` (CloudWatch, default), `prometheus`, `none` |
| `LLMQ_BEDROCK_ENDPOINT_URL`, `LLMQ_STS_ENDPOINT_URL` | Tests only (fakellm, moto) |

Large results go to S3 with `LLMQueue.from_env(offload=..., offload_bytes=...)`.

The registry describes accounts, models and their settings:

```json
{
  "registry_version": 1,
  "residency": {"allowed_regions": ["ap-south-1", "ap-south-2"]},
  "accounts": {"aws-main": {"kind": "aws"}},
  "models": {
    "gpt120b": {
      "type": "chat", "provider": "bedrock", "model_id": "openai.gpt-oss-120b-1:0",
      "rpm": 500, "tpm": 400000, "balance": "headroom", "balance_priority": "ordered",
      "inference": {"temperature": 0.2, "max_tokens": 4096},
      "fallback": {"on": ["exhausted", "broken"], "after_secs": 2},
      "deployments": {"aws-main": {"confirmed": ["ap-south-1", "ap-south-2"],
                                   "fallback": ["us-east-1"]}}
    },
    "embed-cohere": {
      "type": "embedding", "provider": "bedrock", "model_id": "cohere.embed-english-v3",
      "rpm": 2000, "embed_batch_size": 96,
      "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}}
    }
  }
}
```

Every setting, its levels (deployment > model > account > defaults), type, range and default
are in the plan, sections 3.9 and 3.19. `llm-queue settings get --model gpt120b` shows the
effective value of each and where it came from. Invalid documents are refused with every
problem listed at once.

### Tuning (in code)

Timeouts, pool sizes and loop pacing are set in code; every value has a measured default, so
most apps pass nothing. Set only what you change:

```python
from taskiq_llm_queue import LLMQueue, StoreOptions, Tuning

llmq = LLMQueue.from_env(tuning=Tuning(store=StoreOptions(read_timeout_secs=20)))
```

| Part | Value (default) | What it does |
|---|---|---|
| `StoreOptions` | `retries` (3) | Retries of a failed store command, with jittered backoff (~1.5 s in all) |
| | `connect_timeout_secs` (5) | A store node that does not answer in a failover fails fast |
| | `read_timeout_secs` (15) | A reply that never comes fails the command instead of hanging its call |
| | `health_check_secs` (30) | A connection idle this long is pinged before use |
| | `pool_wait_secs` (10) | A burst waits this long for a free connection |
| `BedrockOptions` | `read_timeout_secs` (300), `connect_timeout_secs` (10) | Bedrock / STS client timeouts (each attempt also has `attempt_timeout_secs`) |
| | `max_pool_connections` (100) | Connections per client (one client per account x region) |
| | `role_session_secs` (3600) | Assumed-role session length (900-43200; 3600 is the role-chaining maximum) |
| `EngineTimings` | `idle_poll_secs` (0.05), `idle_slow_secs` (1), `idle_after_empty` (20) | Idle receive pacing; a send's doorbell wakes a worker at once |
| | `result_recheck_secs` (60) | A waiting caller's last-resort read of its result |
| | `health_probe_secs` (3) | A store probe slower than this reports "down" |
| | `stream_read_block_ms` (100) | An opened chunk stream joins the shared read within this |
| | `admission_probe_wait_ms` (200), `admission_slot_wait_ms` (50) + `admission_slot_jitter_ms` (150), `admission_wait_ms` (100) | Admission script pacing: wait for a breaker probe, for a free slot, or by default |
| | `limiter_cut_ms` (1000), `limiter_cut_factor` (0.7) | A throttle cuts the adaptive limit at most once per this, to this fraction |
| | `budget_key_ms` (180000) | Budget and limiter keys live this long (at least 130000: a fixed window spans 2 minutes) |
| `RegistryOptions` | `history_size` (20), `edit_retries` (3) | Versions kept for rollback; retries of a raced live edit |

`from_env` refuses bad tuning at start-up with `ConfigError`, every problem listed at once:
wrong types, zero or negative values, and combinations (the store read timeout must be at
least 1 s above `idle_slow_secs`, `LLMQ_QUEUE_WAIT_MS` and `stream_read_block_ms`, each a
blocking command).

## Use

```python
from taskiq_llm_queue import LLMQueue, request_context, CallFailed

llmq = LLMQueue.from_env()
await llmq.start()                       # publish / load the registry, run the workers

with request_context(priority=True, session_id=sid, user_id=uid):
    reply = await llmq.converse("gpt120b", {"messages": [...]})
reply.response                           # the Bedrock Converse body
reply.meta.deployment                    # "aws-main/ap-south-1"

vectors = (await llmq.embed("embed-cohere", texts, input_type="search_document")).vectors

try:
    await llmq.converse("gpt120b", request, max_wait_secs=5)
except CallFailed as exc:                # exc.kind: rate_limited, model_unavailable, ...
    ...
# Admin operations (cancel, status, registry edits) raise StoreUnavailable when the store
# is unreachable, read-only or full. Everything raised to callers is an LLMQueueError.

llmq.register_job("ingest", ingest)      # jobs run at least once
await llmq.send_job("ingest", {"rows": rows})
await llmq.stop()
```

Runnable samples of each of these (streaming, sync calls, jobs, fallback, status, LangChain)
are in [samples/](https://github.com/techiewonk/taskiq-llm-queue/blob/main/samples/README.md), with where to use each one.

LangChain: `QueuedChatBedrockConverse(model_alias="gpt120b")`,
`QueuedBedrockEmbeddings`, `QueuedChatOpenAI`, `QueuedOpenAIEmbeddings` from
`taskiq_llm_queue.langchain` (they find the started queue on their own).

Extension points: `register_balancer`, `register_fallback_policy`, `register_admission`,
`register_sticky_key`, `register_probe`, `register_token_estimator`, `register_provider`.

## Operate

`llm-queue` (reads the same `LLMQ_*`):

```
llm-queue status [--model M]          llm-queue request ID
llm-queue settings get --model M      llm-queue registry get|set|enable|disable|diff|log|gc
llm-queue registry publish [--from-version N]
llm-queue block --region R            llm-queue unblock --region R
llm-queue drain MODEL --wait          llm-queue dlq list|show|redrive|purge --model M
llm-queue queues list
```

HTTP: `app.include_router(llmq.http_router(dependencies=[Depends(require_admin)]), prefix="/llmq")`
gives `/status`, `/requests/{id}`, `/registry`, and `/health/live` and `/health/ready` without
auth. Readiness never depends on the stores or the providers; it reports each store as `ok`,
`read_only` (a failover left it a replica) or `down`. `allow_writes=True, actor=fn` adds
`POST /registry` and `POST /registry/block`, audited under `actor(request)`. A preStop hook calls
`llmq.pause_receiving()`; shutdown is `await llmq.stop(drain_secs=...)`.

## Develop

```
python scripts/stack.py up        # Valkey, Redis, fakellm (fake Bedrock and OpenAI)
python scripts/check.py --list    # every step's checks
python scripts/check.py all       # lint, format, types and tests of every step
python scripts/samples.py         # the usage samples on fakellm; --live for real Bedrock
python scripts/failover.py        # store failover drill (Docker primary + replica, fakellm)
python scripts/load.py            # load drill: 2,000 calls over 3 workers; a burst over budget
python scripts/gates.py           # the plan's load / chaos gates (B17 ... B97), ~20 min
python scripts/soak.py            # 60 min, 8 worker processes, chaos every 5 min, leak checks
```

No test calls a real LLM. `scripts/samples.py --live` is the only thing that does, and only
when asked.
