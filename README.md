# taskiq-llm-queue (draft)

> **Status:** pre-alpha (0.1.0.dev). The API below is the target design, frozen for 0.x; features land incrementally (see CHANGELOG.md).

An LLM request client with its own durable queue, built on [taskiq](https://github.com/taskiq-python/taskiq) and Amazon SQS.

Your code keeps calling LangChain models (`ainvoke`, `aembed_query`) or a small client API. Each call becomes a task on an SQS queue. Workers in your own app's processes run it under a limit learned from the provider's throttling, retry and fall back when needed, and return the result through Redis/Valkey.

```
your code ──> client / LangChain adapter ──> SQS (per model: -rt queue first, base, bulk)
                                               │
              result <── Redis/Valkey <── worker in your pods ──> Bedrock / OpenAI-compatible
            (per-pod dispatcher)          (adaptive limiter, retry, fallback)
```

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
# core: taskiq, aiobotocore, redis>=5, pydantic, xxhash
# extras: openai, langchain-aws, langchain-openai, s3, metrics
```

The package declares version **ranges**; pin exact versions in your app's lockfile.

## Quickstart

```python
from taskiq_llm_queue import LLMQueue, QueueConfig, ModelConfig, Target, request_context
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
    model = QueuedChatBedrockConverse(llmq=llmq, model="gpt120b")
    with request_context(lane="rt", session_id="s-1", user_id="u-1"):
        reply = await model.ainvoke("Summarise today's market in one line.")
    print(reply.content, reply.response_metadata["served_model"])
    await llmq.stop()                                         # drains in-flight work
```

In a FastAPI app, call `start()` in the lifespan startup and `stop()` on shutdown. Send-only processes, such as CLIs and scripts, use `mode="send"`.

## Client API

```python
res = await llmq.converse("gpt120b", {"messages": [...], "system": [...], "toolConfig": ...},
                          lane="rt", fallback="chain", timeout=90)
res.response            # exact Bedrock Converse dict
res.meta                # served_model, served_target, fallback_used, queue_wait_ms, attempts

vectors = await llmq.embed("titan-v2", ["a", "b"], lane="bulk")         # list[list[float]]
raw = await llmq.invoke_model("titan-v2", {"inputText": "a"})            # raw provider body
chat = await llmq.chat("local-llama", {"messages": [{"role": "user", "content": "hi"}]})   # OpenAI-compatible
```

- **Model ids** are aliases, optionally with a target: `gpt120b`, `gpt120b@apac`. Unknown aliases are rejected when the config is built.
- **Errors:** `LLMQueueError`, with subclasses `LLMQueueTimeout`, `ProviderError(retryable, retry_after)` and `QueueUnavailable`.
- **Sync callers:** sync methods (e.g. `embed_query`) run `direct` under the shared limiter through a small sync Redis client, and count `llmq_sync_fallback_total`. Set `strict_async=True` to raise instead.

## LangChain and LangGraph

| Class | Based on | How |
|---|---|---|
| `QueuedChatBedrockConverse` | `langchain_aws.ChatBedrockConverse` | Native async `_agenerate` using the parent's own helpers; `bind_tools`, structured output via tools, cache usage, serviceTier |
| `QueuedBedrockEmbeddings` | `langchain_aws.BedrockEmbeddings` | `aembed_query` / `aembed_documents` go through the queue |
| `QueuedChatOpenAI` | `langchain_openai.ChatOpenAI` | `http_async_client` with a queued httpx transport, so **every** SDK path (raw response, `parse`, Responses API) is queued and LangChain parses as usual |
| `QueuedOpenAIEmbeddings` | `langchain_openai.OpenAIEmbeddings` | Same transport; `check_embedding_ctx_length=False` by default for non-OpenAI servers |

- **Streaming:** all queued classes force `disable_streaming=True` / `streaming=False`. `astream()` and LangGraph `stream_mode="messages"` return one complete message, so nothing bypasses the queue.
- **Laziness:** queued classes find the running `LLMQueue` on first use and check routing and the kill switch on every call, so you can create them at import time.
- **Callbacks** and `RunnableConfig` behave as before.

## Providers

```python
ModelConfig(provider="openai", model_id="llama-3.1-70b", base_url="http://vllm:8000/v1",
            api_key_env="LOCAL_LLM_KEY")
QueueConfig(allowed_providers={"bedrock"}, denied_hosts=["*.openai.azure.com"])   # enforced on sender and worker
QueueConfig(providers={"myprov": "mypkg.providers:MyProvider"})                    # custom provider, loaded by every pod
```

A custom provider implements `async call(op, params, target) -> dict` and `classify(exc) -> ErrorInfo(kind="throttle" | "retryable" | "fatal", retry_after_s)`. It can also be registered through the `taskiq_llm_queue.providers` entry point. SDK-level retries are off inside workers (botocore `max_attempts=1`, openai `max_retries=0`), so throttles reach the limiter.

## Lanes and queues

| Lane | Queue | Use |
|---|---|---|
| `rt` | `llm-<model>-rt` | Live user requests; polled first; may use the whole limit |
| `bg` | `llm-<model>` | Background work; at most 80 % of the limit |
| `bulk` | `llm-<model>-bulk` if configured (e.g. embeddings), else base | Ingestion/batch; lowest priority |

## Results

Results are stored as JSON (never pickle) under `<key_prefix>:res:<task_id>` for 5 minutes and read once (`GETDEL`). Each pod runs one result dispatcher that resolves all its waiting calls with one batched `MGET`, optionally woken by a pub/sub message, so 10K waiting calls don't mean 10K polling loops.

## Fallbacks: how to check and update

A fallback chain is tried **inside the worker**. Each step is retried with backoff before moving on:

1. same model, primary target
2. same model, another target (e.g. a cross-region profile `gpt120b@apac`)
3. another model (only when the call's policy is `chain`)

Embeddings never switch model. A chat (`rt`) call that can't get capacity within 2 s moves to the next step.

```bash
uv run llm-queue fallback get                       # whole config (JSON)
uv run llm-queue fallback get --model gpt120b       # resolved chain for one model
uv run llm-queue fallback test --model gpt120b      # dry run: which target would serve now (limiter/breaker state)
uv run llm-queue fallback set --model gpt120b --chain "gpt120b@apac,gpt20b"
uv run llm-queue fallback set --file fallback.json
uv run llm-queue fallback reset                     # back to the startup default
```

Chains are stored in `<key_prefix>:cfg:fallback`, validated, and reloaded by every pod within 30 s with no restart. An invalid config is rejected and the last good one stays. Every change is written to the `<key_prefix>:cfg:audit` stream.

```json
{ "gpt120b": {"steps": ["gpt120b@apac", "gpt20b"]},
  "gpt20b":  {"steps": ["gpt20b@apac", "gpt120b"], "require_headroom": ["gpt120b"]},
  "titan-v2": {"steps": ["titan-v2@apac"]} }
```

**To see fallbacks happen:** `res.meta` / `response_metadata` show `served_model`, `served_target`, `fallback_used` and `attempts`, and the `llmq_fallback_total{model,step}` metric counts them. Locally, follow [TESTING.md §4](TESTING.md#4-verify-fallback-step-by-step).

## Canary routing

```bash
uv run llm-queue routing get
uv run llm-queue routing set --model gpt20b --lane rt --pct 10
uv run llm-queue routing kill on                    # everything direct within 30 s
```

Routing is sticky per `session_id` and audited. Every call is tagged `path=sqs|direct`.

## Failure behaviour

| Failure | What happens |
|---|---|
| Provider throttles | Limiter shrinks and pauses that model + target; brokers stop receiving it unless another target in the chain has headroom; work waits in SQS |
| Provider down | Circuit breaker opens → next fallback step |
| Worker killed mid-task | Message reappears after the visibility timeout; the claim prevents a second provider call |
| Rolling deploy | Pods stop receiving at once, release unstarted work, finish running calls, then exit (90 s budget in the reference deployment) |
| **SQS unreachable** | That model switches to `direct` (+ shared limiter) for `sqs_failover_seconds` (300), with an alarm. An ambiguous send is claimed first, so it can't run twice |
| **Redis/Valkey unreachable** | Workers stop taking tasks; senders run `direct` with a local per-pod cap (last known limit ÷ pods); results/claims need Redis, so chat is degraded until it's back |

## Security and data

- Encryption is server-side on the queues (SSE-SQS or SSE-KMS); the library needs no setting for either.
- Prompts and responses are never logged. `user_id` / `session_id` are hashed in task labels (`hash_ids=True`), and raw ids stay inside the message body.
- Nothing secret goes into messages; credentials come from the AWS chain (IRSA / Pod Identity) or env.
- Unknown task names or newer message versions are released for another pod, never silently dropped.

## Run it locally (no AWS needed)

```bash
make up                         # Valkey (password) + ElasticMQ + fakellm + 3 workers
make test                       # unit (moto server) + integration against the stack
make scenario S=S10             # fallback: profile, then model
make down
# Windows without make:  uv run llmq-dev up | test | scenario S10 | down
```

See [TESTING.md](TESTING.md) for the stack, scenarios and dashboards.

## Credits

Broker design and feature set build on [taskiq-aio-sqs](https://github.com/vonsteer/taskiq-aio-sqs) (MIT, © 2022-2024 Jesse Constante) and [taskiq-sqs](https://github.com/taskiq-python/taskiq-sqs) (MIT, © 2026 Mike Shultz and the Taskiq team). Licence: MIT.
