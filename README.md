# taskiq-llm-queue

> **Status:** pre-alpha (`0.1.0a2`). Built and tested: the `LLMQueue` client, the SQS broker, adaptive limiter, claims, circuit breaker, retry and fallback, JSON and S3 result backends, Bedrock and OpenAI-compatible providers, LangChain/LangGraph adapters, canary routing, failover and the CLI. 760 tests on Valkey 8.1, ElasticMQ 1.7.1 and a fake LLM server; CI on Python 3.11–3.14. Breaking changes are still possible before 0.1 (see [CHANGELOG](https://github.com/techiewonk/taskiq-llm-queue/blob/main/CHANGELOG.md)).

An LLM request client with its own durable queue, built on [taskiq](https://github.com/taskiq-python/taskiq) and Amazon SQS.

Your code keeps calling LangChain models (`ainvoke`, `aembed_query`) or a small client API. Each call becomes a task on an SQS queue. Workers in your own app's processes run it under a limit learned from the provider's throttling, retry and fall back when needed, and return the result through Redis/Valkey.

```
your code ──> client / LangChain adapter ──> SQS (per model: -rt queue first, base, bulk)
                                               │
              result <── Redis/Valkey <── worker in your pods ──> Bedrock / OpenAI-compatible
            (per-pod dispatcher)          (adaptive limiter, retry, fallback)
```

**How it works:** [docs/ARCHITECTURE.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/ARCHITECTURE.md) walks through the components, every data flow (happy path, fallback, failover, jobs, cancellation, crash recovery), the adaptive limiter, the Valkey key schema and the full configuration reference.

## Why

- **Spikes wait in a queue instead of failing.** Throttled calls aren't lost or retried in a storm.
- **No quota numbers to maintain.** The limit for each model and target is learned from throttle errors and shared by all pods.
- **Chat first.** Separate `-rt` queues and reserved capacity keep live traffic ahead of background and bulk work.
- **No extra servers.** The same app pods send and execute; SQS is only the broker.
- **Drop-in.** LangChain/LangGraph code doesn't change: tool calling, callbacks (Langfuse, LangSmith) and response objects are identical.
- **Safe rollout.** `direct` mode uses the same limiter, and a live canary moves traffic per model and lane without restarts.

## Install

```bash
uv add "taskiq-llm-queue[langchain-aws]"                     # Bedrock + LangChain
uv add "taskiq-llm-queue[openai,langchain-openai]"           # OpenAI-compatible + LangChain
uv add "taskiq-llm-queue[speedups]"                          # hiredis parser for redis-py
```

| | Packages |
|---|---|
| Core | `taskiq`, `aiobotocore`, `redis>=5.0.1`, `pydantic>=2.10`, `orjson`, `structlog` |
| `openai` | `openai`, `httpx` |
| `langchain-aws` / `langchain-openai` | LangChain **0.3 and 1.x** are both supported |
| `metrics` | `prometheus-client` |
| `speedups` | `hiredis` |

The package declares version **ranges**; pin exact versions in your app's lockfile. `aiobotocore` pins a narrow `botocore` window, so let your resolver pick an `aiobotocore` that matches your `boto3`.

## Quickstart

```python
from taskiq_llm_queue import LLMQueue, ModelConfig, QueueConfig, Target, request_context
from taskiq_llm_queue.langchain import QueuedChatBedrockConverse

llmq = LLMQueue(QueueConfig(
    key_prefix="llmq:local",
    redis_host="localhost", redis_port=6379, redis_password="localdev", redis_ssl=False,
    sqs_endpoint_url="http://localhost:9324",                 # ElasticMQ locally; omit on AWS
    models={
        "gpt120b": ModelConfig(
            provider="bedrock", model_id="openai.gpt-oss-120b-1:0",
            targets={"primary": Target(region="ap-south-1"),
                     "apac": Target(profile_id="apac.openai.gpt-oss-120b-1:0")},
            queues={"rt": "llm-gpt120b-rt", "default": "llm-gpt120b"}),
    },
))                                                            # or QueueConfig.from_env()  (LLMQ_* variables)

async def main():
    await llmq.start(mode="both")                             # "send" | "work" | "both"
    model = QueuedChatBedrockConverse(llmq=llmq, model_alias="gpt120b")
    with request_context(lane="rt", session_id="s-1", user_id="u-1"):
        reply = await model.ainvoke("Summarise today's market in one line.")
    print(reply.content, reply.response_metadata["served_model"])
    await llmq.stop()                                         # drains in-flight work
```

In a FastAPI app, call `start()` in the lifespan startup and `stop()` on shutdown. Send-only processes, such as CLIs and scripts, use `mode="send"`.

## Client API

Every call returns a result object with `.meta`: `served_model`, `served_target`, `fallback_used`, `attempts`, `queue_wait_ms`, `provider_ms`, `worker`.

```python
res = await llmq.converse("gpt120b", {"messages": [...], "system": [...], "toolConfig": ...},
                          lane="rt", fallback="chain", timeout_secs=90, request_id="req-1")
res.response                    # exact Bedrock Converse dict

emb = await llmq.embed("titan", ["a", "b"], lane="bulk")
emb.vectors                     # list[list[float]]; `emb` itself also works as a list

chat = await llmq.chat("llama", {"messages": [{"role": "user", "content": "hi"}]})
chat.response                   # OpenAI-compatible chat completion dict

raw = await llmq.invoke_model("titan", {"inputText": "a"}, content_type="application/json")
raw.json(), raw.body, raw.content_type

await llmq.cancel("req-1")      # not run if queued, stopped if running; caller gets LLMQueueCancelled
```

- **Timing:** `timeout_secs` (relative) or `deadline_at` (absolute epoch seconds) on every call.
- **Model ids** are aliases, optionally with a target: `gpt120b`, `gpt120b@apac`. Unknown aliases are rejected when the config is built.
- **Errors:** everything derives from `LLMQueueError`: `LLMQueueTimeout`, `LLMQueueCancelled`, `ProviderError(retryable, retry_after_secs)`, `QueueUnavailable`, `ConfigError`.
- **Sync callers:** `converse_sync`, `chat_sync`, `embed_sync`, `invoke_model_sync` run `direct` under the shared limiter (counted as sync fallbacks). Set `strict_async=True` to raise instead. `close_sync()` (or `stop()`) closes the sync runner.
- **Jobs:** `register_job(name, fn)` on workers, `send_job(name, payload, timeout_secs=...)` anywhere; jobs run on the job queues with the same claims and DLQ rules.

## LangChain and LangGraph

| Class | Based on | How |
|---|---|---|
| `QueuedChatBedrockConverse` | `langchain_aws.ChatBedrockConverse` | Builds the request and parses the reply with the installed langchain-aws's own code, so `bind_tools`, structured output, cache usage and service tier match exactly |
| `QueuedBedrockEmbeddings` | `langchain_aws.BedrockEmbeddings` | `aembed_query` / `aembed_documents` go through the queue |
| `QueuedChatOpenAI` | `langchain_openai.ChatOpenAI` | `http_async_client` with a queued httpx transport: chat completions and embeddings go through the queue; queue meta lands in `response_metadata`. The Responses API (`/responses`) is not supported yet |
| `QueuedOpenAIEmbeddings` | `langchain_openai.OpenAIEmbeddings` | Same transport; `check_embedding_ctx_length=False` by default for non-OpenAI servers |

- **Model names:** pass the queue alias as `model_alias=` (plus `model_id=` or `llmq=`). Passing an alias as `model=` raises a clear error.
- **Streaming:** all queued classes force `disable_streaming=True` / `streaming=False`. `astream()` and LangGraph `stream_mode="messages"` return one complete message, so nothing bypasses the queue.
- **Laziness:** queued classes find the running `LLMQueue` on first use and check routing and the kill switch on every call, so you can create them at import time.

## Configuration

`QueueConfig.from_env()` reads `LLMQ_*` variables (durations end in `_SECS`; older `_S` spellings are not read). Notable settings:

| Setting | Default | Note |
|---|---|---|
| `aws_region` | `None` | Left to the AWS credential chain / IRSA |
| `redis_url` or `redis_host`/`port`/`username`/`password`/`ssl` | localhost | `redis_password` is a `SecretStr` |
| `redis_socket_timeout_secs` / `connect_timeout` / `health_check_interval` | 5 / 2 / 30 | Connection errors retried 3× with backoff |
| `id_hash_salt` (`LLMQ_ID_HASH_SALT`) | library constant | **Set your own in production**; keys the BLAKE2b hash of session/user ids |
| `timeout_secs` per lane | rt 90, bg 300, bulk 900 | |
| `attempt_timeout_secs` | 180 | One provider attempt; a hung target moves to the next fallback step |
| `provider_read_timeout_secs` / `provider_connect_timeout_secs` | 300 / 10 | Bedrock botocore and OpenAI httpx timeouts |
| `sqs_send_timeout_secs` | 5 | SQS counts as down only when no send completed for this long; then the call runs direct |
| `sqs_timeouts_to_failover` | 3 | Send timeouts within 60 s before the model's later calls skip SQS for `sqs_failover_secs` (a refused send: at once) |
| `sqs_batch_wait_secs` | 0.02 | bg and bulk sends within this window share one `SendMessageBatch` (0 = off); rt never batches |
| `s3_offload_bucket` (`LLMQ_S3_OFFLOAD_BUCKET`) | none | Messages over `inline_max_bytes` (200 KB) carry their params in S3 under `s3_offload_prefix` instead of Valkey; add a lifecycle rule to expire leftovers |
| `throttle_retry_attempts` | 6 | Tries per fallback step for throttles (backoff holds no slot) |
| `retry_base_secs` / `retry_multiplier` / `retry_spread` / `retry_max_secs` | 1 / 4 / 2.5 / 30 | Retry n sleeps a random time in [base·4ⁿ, base·4ⁿ·2.5]: **1–2.5 s, 4–10 s, 16–30 s**, then 30 s; see [Retry backoff](#retry-backoff) |
| `throttle_retry_max_secs` | 10 | Cap of one sleep after a throttle, so 6 throttle tries fit an rt deadline |
| `rt_capacity_wait_secs` | 30 | How long a received rt call keeps looking for a free slot before `no_capacity` (never past its deadline) |
| `valkey_wait_failover_secs` | 10 | Result reads failing this long make waiting callers run direct |
| `redis_max_connections` | 32 | Raised automatically to worker slots + 16 |
| `worker_slots` (`LLMQ_WORKER_SLOTS=gpt120b:50,jobs:4`) | 50 per model alias (4 for `jobs`) | Tasks one pod runs at once for that alias's queues |
| `ModelConfig.limiter_initial` (`LLMQ_AC_INITIAL='{"gpt120b": 40}'`) | 20 | Start of the adaptive limit before anything was learned |
| `ModelConfig.max_concurrency` (`LLMQ_AC_MAX_BY_MODEL='{"gpt120b": 30}'`) | `None` (adaptive up to `LLMQ_AC_MAX`) | Hard cap on calls in flight across all pods, per alias **and target**; see below |
| `limiter.max_limit` (`LLMQ_AC_MAX`) | 2000 | Global ceiling of every adaptive limit |
| `limiter.bg_share` (`LLMQ_AC_BG_SHARE`) | 0.8 | Fraction of a limit `bg`/`bulk` may use; `rt` may use all of it |
| `settings_default` (`LLMQ_SETTINGS`, JSON) | `{}` | Per-provider and per-model values of the settings below; see [Settings per provider and model](#settings-per-provider-and-model) |

Redis keys are cluster-safe (hash-tagged per request and per model+target), so cluster-mode Valkey (e.g. ElastiCache Serverless) works.

### Settings per provider and model

A provider (`bedrock`, `openai`, or a custom one) serves several models. These settings can be given once for a provider, overridden for one model, and changed live on every pod without a restart:

`max_concurrency`, `limiter_initial`, `bg_share`, `timeout_secs.<lane>`, `embed_timeout_secs.<lane>`, `attempt_timeout_secs`, `throttle_retry_attempts`, `retry_base_secs`, `retry_multiplier`, `retry_spread`, `retry_max_secs`, `throttle_retry_max_secs`, `breaker_threshold`, `breaker_cooldown_secs`, `lane_max_concurrency.<lane>`, the rate budget `rpm`, `tpm`, `rate_window`, `rate_burst`, `rt_rate_share` ([Rate limits](#rate-limits-rpm--tpm)), and for a provider only `shared_max_concurrency`, `shared_rpm`, `shared_tpm`.

Each one resolves through six layers; the highest that has a value wins:

| # | Layer | Where it comes from |
|---|---|---|
| 6 | Live model | Valkey, `llm-queue settings set --model M ...` |
| 5 | Live provider | Valkey, `llm-queue settings set --provider P ...` |
| 4 | Env model | `LLMQ_SETTINGS` `models`, plus `ModelConfig.max_concurrency` / `limiter_initial` and `LLMQ_AC_MAX_BY_MODEL` / `LLMQ_AC_INITIAL` |
| 3 | Env provider | `LLMQ_SETTINGS` `providers` |
| 2 | Env global | `LLMQ_ATTEMPT_TIMEOUT_SECS`, `LLMQ_TIMEOUT_RT_SECS`, `LLMQ_RETRY_*`, `LLMQ_CB_*`, `LLMQ_AC_BG_SHARE`, ... (code, `.env` or Secrets Manager) |
| 1 | Built-in default | The defaults in the tables above |

```bash
# .env: provider defaults and one model override (layers 3-4), validated at boot
LLMQ_SETTINGS='{"providers": {"openai": {"shared_max_concurrency": 40, "attempt_timeout_secs": 120}},
                "models": {"gpt120b": {"max_concurrency": 30, "timeout_secs": {"rt": 60}}}}'

# live, reaches every pod within LLMQ_CFG_RELOAD_SECS (30 s); every change is audited
llm-queue settings set --model gpt120b max_concurrency=20 timeout_secs.rt=45
llm-queue settings set --provider bedrock attempt_timeout_secs=90 retry_multiplier=3
llm-queue settings get --model gpt120b      # every setting with its value and source
llm-queue settings unset --model gpt120b max_concurrency
llm-queue settings reset                    # drop all live overrides
```

`settings get` shows each value's source (`live:model`, `live:provider`, `env:model`, `env:provider`, `env`, `default`), so a value that differs from your `.env` is easy to explain. A live value wins over `.env` until it is unset. An unknown alias or provider, an unknown setting or a bad value is refused before anything is written. If the stored value is unreadable or Valkey is down, pods keep the last good one.

**Provider-wide cap.** `shared_max_concurrency` on a provider bounds the calls in flight across *all* its models and pods, e.g. one vLLM server hosting three models. Each call then holds two slots, its model's and the provider's. Measured (scenario S55): with a shared cap of 8 on `bedrock` and gpt20b and gpt120b allowed 20 each, 600 calls peaked at exactly 8 at the provider, all OK, none duplicated.

**Live bucket change.** Measured (scenario S54): gpt20b ran at `max_concurrency` 20 on 3 workers; `settings set --model gpt20b max_concurrency=5` mid-run brought the provider peak from 20 to 5 after one reload, with 900/900 calls OK.

**Models with a single target.** A model with only one target (no APAC profile, no `chain` fallback) has nowhere to go when it is throttled: after `throttle_retry_attempts` (6) throttles in a row the call fails. Under a 30 % throttle storm that is 0.3⁶ ≈ 0.07 % of calls (S35b: 4 of 3,020 gpt20b rt calls). Give such a model a fallback target or model, or raise its `throttle_retry_attempts` (10 tries: 0.3¹⁰ ≈ 0.0006 %).

**Errors carry meta.** A failed call raises `ProviderError` / `LLMQueueTimeout` with `.meta` (attempts, queue wait, last target tried), the same fields a successful result has.

Not live (needs a restart): connection settings, the model list (targets, queues, provider), credentials, provider HTTP timeouts, `worker_slots` and `retry_attempts`.

### Retry backoff

Each fallback step retries a failed attempt `retry_attempts` times (3), or `throttle_retry_attempts` times (6) once the provider throttled it. Retry *n* (0-based) sleeps a random time in `[base · multiplierⁿ, base · multiplierⁿ · spread]`, capped at `retry_max_secs`:

| Retry | Defaults (base 1, multiplier 4, spread 2.5, cap 30) | After a throttle (cap 10) |
|---|---|---|
| 1st | 1–2.5 s | 1–2.5 s |
| 2nd | 4–10 s | 4–10 s |
| 3rd | 16–30 s | 10 s |
| 4th and later | 30 s | 10 s |

A provider `Retry-After` larger than the drawn delay wins, up to `retry_max_secs`. A sleep that would pass the call's deadline is skipped and the next fallback step is tried. No limiter slot is held while sleeping. All five knobs are `LLMQ_*` variables and can be set per provider or model like the settings above.

A sleeping call frees its limiter slot but keeps its worker task (one of `worker_slots`). Under a long throttle storm a longer curve therefore lowers per-pod throughput; if queued callers start to time out, raise `worker_slots` or lower `retry_multiplier` / `throttle_retry_max_secs` for that provider or model. The scenario harness scales the whole curve by 0.2 (base 0.2 s, caps 6 s and 2 s) to keep runs short.

### Setting names

Every duration ends in `_SECS` (milliseconds in `_MS`). Old spellings from pre-release builds, such as `LLMQ_CONSUMER_DRAIN_S`, `LLMQ_RETRY_BASE_S`, `LLMQ_SQS_WAIT_TIME_S` or `LLMQ_TIMEOUT_RT`, are **not read**: a variable with one of them is silently ignored and the default applies. Rename them in your `.env`, Helm values and Secrets Manager entries:

| Old spelling (ignored) | Use |
|---|---|
| `LLMQ_CFG_RELOAD_S` | `LLMQ_CFG_RELOAD_SECS` |
| `LLMQ_CONSUMER_DRAIN_S` | `LLMQ_CONSUMER_DRAIN_SECS` |
| `LLMQ_RETRY_BASE_S` | `LLMQ_RETRY_BASE_SECS` |
| `LLMQ_SQS_WAIT_TIME_S` | `LLMQ_SQS_WAIT_TIME_SECS` |
| `LLMQ_SQS_FAILOVER_S` | `LLMQ_SQS_FAILOVER_SECS` |
| `LLMQ_STATE_TTL_S` | `LLMQ_STATE_TTL_SECS` |
| `LLMQ_AC_PAUSE_S` | `LLMQ_AC_PAUSE_SECS` |
| `LLMQ_AC_LEASE_TTL_S` | `LLMQ_AC_LEASE_TTL_SECS` |
| `LLMQ_CB_WINDOW_S` | `LLMQ_CB_WINDOW_SECS` |
| `LLMQ_CB_COOLDOWN_S` | `LLMQ_CB_COOLDOWN_SECS` |
| `LLMQ_TIMEOUT_RT` / `_BG` / `_BULK` | `LLMQ_TIMEOUT_RT_SECS` / `_BG_SECS` / `_BULK_SECS` |
| `LLMQ_TIMEOUT_EMBED_RT` / `_BG` / `_BULK` | `LLMQ_TIMEOUT_EMBED_RT_SECS` / `_BG_SECS` / `_BULK_SECS` |

## Providers

```python
ModelConfig(provider="openai", model_id="llama-3.1-70b",
            targets={"primary": Target(base_url="http://vllm:8000/v1")}, api_key_env="LOCAL_LLM_KEY")
QueueConfig(allowed_providers={"bedrock"}, denied_hosts=["*.openai.azure.com"])   # enforced on sender and worker
QueueConfig(providers={"myprov": "mypkg.providers:MyProvider"})                    # custom provider, loaded by every pod
```

A custom provider implements `async call(op, params, target) -> dict` and `classify(exc) -> ErrorInfo(kind="throttle" | "retryable" | "fatal", retry_after_secs)`. It can also be registered through the `taskiq_llm_queue.providers` entry point. SDK-level retries are off inside workers (botocore `total_max_attempts=1`, openai `max_retries=0`), so throttles reach the limiter.

## Lanes and queues

| Lane | Queue | Use |
|---|---|---|
| `rt` | `llm-<model>-rt` | Live user requests; polled first; may use the whole limit |
| `bg` | `llm-<model>` | Background work; at most 80 % of the limit |
| `bulk` | `llm-<model>-bulk` if configured (e.g. embeddings), else base | Ingestion/batch; lowest priority |

A queued `bg`/`bulk` call waits at most 15 s for a slot, then goes back to the queue with a delay instead of holding a worker.

### Concurrency: queue depth vs bucket size

The queue depth is unbounded: SQS holds every request that is not running. How many run at once is set by three knobs:

- **`worker_slots`** (per pod, per model alias, default 50): tasks one pod runs at once. Total pod capacity is `pods × worker_slots`.
- **The limiter** (cluster-wide, per model alias and target, in Valkey): the number of provider calls in flight across all pods. It starts at `limiter_initial` (20), grows on busy successes, is cut on clustered throttles, and never exceeds `LLMQ_AC_MAX` (2000).
- **`max_concurrency`** (cluster-wide, per model alias **and target**, default unset): a hard cap on that limit. With `max_concurrency=10`, at most 10 calls of that alias run against each of its targets at once, whatever the pod count. A model with two targets can therefore have 2 × 10 in flight. The limiter still adapts below the cap. Set `limiter_initial == max_concurrency` for a fixed bucket that only shrinks under real throttling; a `limiter_initial` above the cap is clamped to it (logged once as `limiter_initial_clamped`). Lowering the cap also clamps a limit learned earlier, on its next use.

`bg` and `bulk` may use `bg_share` (80 %) of the limit, so with a cap of 10, background work runs at most 8 at once and keeps 2 slots for `rt`. Set `LLMQ_AC_BG_SHARE=1` to give all lanes the whole bucket.

**A bucket per lane.** `lane_max_concurrency.<lane>` gives one lane of a model its own cap inside the model's bucket, per target and across all pods, e.g. bulk embeddings at most 5 while bg may use 15 of 20:

```bash
LLMQ_SETTINGS='{"models": {"titan": {"max_concurrency": 20, "lane_max_concurrency": {"bulk": 5, "bg": 15}}}}'
llm-queue settings set --model titan lane_max_concurrency.bulk=3      # live, every pod
```

A call in a capped lane holds a slot of the model's bucket and one of the lane's (`{m:<alias>#<lane>:<target>}`, a fixed bucket that never learns). A full lane leaves its own backlog in SQS while the other lanes keep receiving. Measured (scenario S57): gpt20b bucket 20, bulk 3, bg 5, 300 bulk + 300 bg calls at once: bulk peaked at exactly 3, bg at exactly 5, an rt call in the middle started in 0.23 s, all 600 OK.

A pod receives at most min(free worker slots, limiter headroom − its own calls already waiting for a slot), so a full bucket leaves the backlog in SQS rather than in pod memory. The rest wait in SQS until their lane deadline (`timeout_secs`: rt 90 s, bg 300 s, bulk 900 s). `bg`/`bulk` calls that do get received but find no slot within 15 s go back to SQS with a delay. An `rt` call that is received keeps walking its fallback chain for a free slot (2 s per step) for up to `rt_capacity_wait_secs` (30 s, never past its deadline), then fails with `no_capacity`. Size the bucket so that `cap × calls per second per slot × deadline` covers the backlog you expect. `max_concurrency` and `bg_share` can also be set per provider and changed live ([Settings per provider and model](#settings-per-provider-and-model)), and a provider can have one bucket shared by all its models. While Valkey is down, each pod runs under a local cap of `last limit / live pods`, never above `max_concurrency`. This is best effort: with the pod count unknown, each pod may run up to the cap.

Measured (scenario S53, fakellm at 200 ms p50, 3 workers × 50 slots, `max_concurrency=10`, `bg_share=1`): 2,000 bg calls queued (2,000 visible in SQS at peak), peak provider concurrency exactly 10 (from the call log and fakellm's own counter), limiter never above 10, 2,000/2,000 done, 0 lost, 0 duplicates, 61 s wall time. SQS in-flight peaked at 40, which is 10 running plus finished messages waiting for the 0.5 s batched delete.

### Rate limits (RPM / TPM)

Concurrency caps how many calls run at once; a provider quota is usually per minute. Set `rpm` (calls that may **start** per minute) and / or `tpm` (tokens per minute) per model, and `shared_rpm` / `shared_tpm` per provider for an account quota. Each budget is per model **target** (a model with a primary and an APAC profile has one budget on each), shared by every pod in Valkey. Unset means no rate limit.

```bash
LLMQ_SETTINGS='{"models": {"gpt120b": {"rpm": 500, "tpm": 200000}},
                "providers": {"bedrock": {"shared_rpm": 800}}}'
llm-queue settings set --model gpt120b rpm=300 rate_window=fixed    # live, every pod
llm-queue status --model gpt120b       # waiting / working / started this minute / left
```

A call starts only with a rate token **and** a concurrency slot. How the budget behaves:

| Setting | Default | Meaning |
|---|---|---|
| `rate_window` | `smooth` | `smooth`: a token bucket, `rpm / 60` per second with `rate_burst` back to back. `fixed`: the whole `rpm` from each calendar minute (500 at 10:00:00, the next 500 from 10:01:00) |
| `rate_burst` | 10 % of `rpm` | Calls that may start back to back in `smooth` mode; TPM uses the same fraction |
| `rt_rate_share` | 0.2 | Part of each budget only `rt` may use: `bg`/`bulk` get at most 80 % of the rate, so a backlog never starves live calls |

When a budget is used up:

- **`bg` / `bulk`** wait for **their own target's** budget: the message goes back to SQS with a delay until the next token, and that pod stops receiving the model's `bg`/`bulk` work until then (the backlog stays in SQS). They never spend a fallback target's budget.
- **`rt`** moves to the next target in its chain (each target has its own budget), then keeps trying for `rt_capacity_wait_secs` (30 s, never past its deadline), then fails with `ProviderError("rate_limited ...")`, `retryable=True`, `retry_after_secs` = time to the next token (HTTP 429 through `http_transport`).
- **TPM** reserves `input characters / 4 + max output tokens` (1,000 when the request sets none) before the call and is corrected with the response's `usage` after it, so a generous `max_tokens` does not hold the budget.
- **Valkey down**: each pod applies `limit / live pods` in process (best effort; the total is right while the pod count is right).

`llm-queue status` prints per model: SQS `waiting` / `delayed` / `received` per lane queue, and per target `working` (calls in flight, all pods), `limit`, `started` this minute, `rpm_left`, `tokens`, `tpm_left`, plus the provider's shared budget. Metrics: `llmq_rate_used` and `llmq_rate_remaining` per model target; waiting messages are SQS's own `ApproximateNumberOfMessagesVisible` in CloudWatch.

The adaptive limiter stays on underneath: if the real quota is lower than the `rpm` you set, throttles still cut concurrency.

## Results

Results are stored as JSON (never pickle) for 5 minutes and read once (`GETDEL`). Each pod runs one result dispatcher: a pub/sub wake reads only the announced results, and a periodic batched sweep catches the rest, so 10K waiting calls don't mean 10K polling loops.

## Fallbacks: how to check and update

A fallback chain is tried **inside the worker**. Each step is retried with backoff before moving on:

1. same model, primary target
2. same model, another target (e.g. a cross-region profile `gpt120b@apac`)
3. another model (only when the call's policy is `chain`)

Embeddings never switch model. Throttles shrink the limiter but never trip the circuit breaker; only 5xx and timeouts do.

```bash
uv run llm-queue fallback get                       # whole config (JSON); starts from your app's defaults
uv run llm-queue fallback get --model gpt120b       # resolved chain for one model
uv run llm-queue fallback test --model gpt120b      # dry run: which target would serve now
uv run llm-queue fallback set --model gpt120b --chain "gpt20b"
uv run llm-queue fallback set --file fallback.json  # merges per model; --replace to overwrite all
uv run llm-queue fallback reset                     # back to the startup default
```

Chains are validated and reloaded by every pod within 30 s with no restart. An invalid config is rejected and the last good one stays. Every change is written to an audit stream.

```json
{ "chains": {
    "gpt120b": ["gpt20b"],
    "gpt20b": [{"model": "gpt120b", "require_headroom": true}]
  },
  "skip_targets": { "gpt120b": ["apac"] } }
```

**To see fallbacks happen:** `res.meta` / `response_metadata` show `served_model`, `served_target`, `fallback_used` and `attempts`, and the `llmq_fallback_total{model,step}` metric counts them. `samples/05_fallback_on_throttle.py` shows it end to end.

## Operating it

```bash
uv run llm-queue routing set --model gpt20b --lane rt --pct 10   # canary: 10 % of sessions via SQS
uv run llm-queue routing kill on                                 # everything direct within 30 s
uv run llm-queue routing reset
uv run llm-queue limiter show --model gpt120b                    # learned limit, in-flight, pause
uv run llm-queue limiter reset --model gpt120b [--target apac]   # forget the learned limit
uv run llm-queue breaker show | reset
uv run llm-queue settings get --model gpt120b                   # effective settings + source
uv run llm-queue settings set --model gpt120b max_concurrency=20 # live bucket size, all pods
uv run llm-queue settings set --provider openai shared_max_concurrency=40
uv run llm-queue queues                                          # messages per queue and DLQ
uv run llm-queue job send ingest --payload-file payload.json --queue jobs-ingest
```

## Failure behaviour

| Failure | What happens |
|---|---|
| Provider throttles | Limiter shrinks and pauses that model + target; brokers stop receiving it unless another target in the chain has headroom; work waits in SQS |
| Provider down (5xx, timeouts) | Circuit breaker opens → next fallback step |
| Worker killed mid-task | Message reappears after the visibility timeout; the claim prevents a second provider call |
| Rolling deploy | Pods stop receiving at once, release unstarted work, finish running calls, then exit (90 s budget in the reference deployment) |
| **SQS unreachable** | That model switches to `direct` (+ shared limiter) for 300 s on transient send errors only. An ambiguous send is claimed first, so it can't run twice |
| **Redis/Valkey unreachable** | Workers release messages instead of losing them; senders run `direct` with a local per-pod cap (last known limit ÷ live pods) |

## Logging

The library logs with structlog's key-value API on top of stdlib `logging` and **never configures logging itself**. Records are named `taskiq_llm_queue.*`; the message reads `event key=value ...` and the fields are also on `record.llmq` (a dict). Prompts, responses and params are never logged.

```python
import logging
logging.getLogger("taskiq_llm_queue").setLevel(logging.INFO)   # stdlib apps: that's all

# loguru apps: the usual InterceptHandler picks these up like any stdlib logger.
# structlog apps: use structlog.stdlib.ProcessorFormatter on your handler to render them.
```

To trace one call on a worker, set `taskiq_llm_queue.core` to DEBUG. Each call then logs `call_started` (with `queue_wait_ms`), `slot_acquired` / `slot_unavailable` (`waited_ms`), `attempt_done` (target, outcome, `provider_ms`), `backoff` (`sleep_secs`), `rt_capacity_wait`, `call_requeued` and `call_done` (ok or error code, attempts, `total_ms`), all with the `request_id`. Below DEBUG these cost one level check per call.

The `llm-queue` CLI logs warnings and errors to stderr, so its stdout is always plain JSON.

## Security and data

- Encryption is server-side on the queues (SSE-SQS or SSE-KMS); the library needs no setting for either.
- Prompts and responses are never logged. `user_id` / `session_id` are hashed (keyed BLAKE2b) in task labels; raw ids stay inside the message body.
- Nothing secret goes into messages; credentials come from the AWS chain (IRSA / Pod Identity) or env.
- Unknown task names or newer message versions are released for another pod, never silently dropped.

## Run it locally (no AWS needed)

```bash
make up              # Valkey (password) + ElasticMQ + fakellm
make test            # unit + integration against the stack
make down
# Windows without make:  uv run python tools/llmq_dev.py up | test | down
```

Runnable examples are in [`samples/`](https://github.com/techiewonk/taskiq-llm-queue/tree/main/samples): quickstart, chat, embeddings, lanes, fallback on throttle, send-only and worker pods, LangChain, LangGraph, jobs, cancel and timeouts, sync calls, CLI, and `13_research_assistant_flow.py`, which replays a real app's flows locally or against real Bedrock (`--target aws`, settings from `.env` or AWS Secrets Manager). The stack, capability checks and test layers are described in [docs/TESTING.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/TESTING.md); delivery-safety rules in [docs/DESIGN.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/DESIGN.md).

## Credits

Broker design and feature set build on [taskiq-aio-sqs](https://github.com/vonsteer/taskiq-aio-sqs) (MIT, © 2022-2024 Jesse Constante) and [taskiq-sqs](https://github.com/taskiq-python/taskiq-sqs) (MIT, © 2026 Mike Shultz and the Taskiq team). Licence: MIT.
