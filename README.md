# taskiq-llm-queue

LLM calls through a queue on Valkey or Redis, under RPM / TPM budgets per **deployment**
(model x account x region), balanced across regions and accounts, with fallback regions,
fallback models, health checks and status. Works with Amazon Bedrock and OpenAI-compatible
servers (vLLM), and with LangChain.

Version 0.3 is a clean break from 0.2: no taskiq broker, no SQS, no lanes. See
[CHANGELOG.md](CHANGELOG.md) and the design in [docs/ACCOUNT_ROUTING_PLAN.md](docs/ACCOUNT_ROUTING_PLAN.md)
(walkthrough with sample data: [docs/ACCOUNT_ROUTING_WALKTHROUGH.md](docs/ACCOUNT_ROUTING_WALKTHROUGH.md)).

## How a call flows

1. The app calls `llmq.converse(...)` (or a LangChain adapter). The call is registered (a
   repeated request id runs once) and put on its model's queue: `<name>-priority` for priority
   calls, `<name>` for the rest.
2. A worker (in the same pods) reads the priority queue first and admits each call with one
   store call: pick a deployment by the model's strategy and take one request from RPM, the
   token estimate from TPM, a concurrency slot and the account budget, all or nothing.
3. A call without budget waits (its class stops being read; older calls keep their place) and
   spills to fallback regions and then fallback models when the registry's rules allow.
4. The provider is called on the deployment's account and region; tokens are corrected to the
   real usage; the result goes back to the caller.

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
| `LLMQ_STORE_URL` | The state store (`rediss://...` in prod) |
| `LLMQ_QUEUE_STORE_URL` | Optional separate queue store |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | Names of this environment's keys and queues |
| `LLMQ_REGISTRY` (or `LLMQ_REGISTRY_FILE`) | The registry document (below) |
| `LLMQ_REGISTRY_LIVE_EDIT` | `safe` (default), `all`, `off` |
| `LLMQ_MODE` | `both` (default), `caller`, `worker` |
| `LLMQ_METRICS_SINK` | `emf` (CloudWatch, default), `prometheus`, `none` |

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

llmq.register_job("ingest", ingest)      # jobs run at least once
await llmq.send_job("ingest", {"rows": rows})
await llmq.stop()
```

Runnable samples of each of these (streaming, sync calls, jobs, fallback, status, LangChain)
are in [samples/](samples/README.md), with where to use each one.

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
llm-queue registry publish [--from-audit N]
llm-queue block --region R            llm-queue unblock --region R
llm-queue drain MODEL --wait          llm-queue dlq list|show|redrive|purge --model M
llm-queue queues list
```

HTTP: `app.include_router(llmq.http_router(dependencies=[Depends(require_admin)]), prefix="/llmq")`
gives `/status`, `/requests/{id}`, `/registry`, and `/health/live` and `/health/ready` without
auth. Readiness never depends on the stores or the providers.

## Develop

```
python scripts/stack.py up        # Valkey, Redis, fakellm (fake Bedrock and OpenAI)
python scripts/check.py --list    # every step's checks
python scripts/check.py all       # lint, format, types and tests of every step
python scripts/samples.py         # the usage samples on fakellm; --live for real Bedrock
```

No test calls a real LLM. `scripts/samples.py --live` is the only thing that does, and only
when asked.
