# Samples

Runnable examples of every way an app uses taskiq-llm-queue. Each file is plain app code:
it builds the queue with `LLMQueue.from_env()` and reads nothing else. The runner only sets
the environment.

| Sample | Shows | Use it in |
|---|---|---|
| `01_converse.py` | `converse` with `request_context` (priority, session, user); reply text and `meta` | Async request handlers (FastAPI routes), agent steps, anything user-facing |
| `02_stream.py` | `converse_stream`: events as they arrive; `CallFailed.partial_text` on a cut | Chat UIs and server-sent events, where the first words should show at once |
| `03_embed.py` | `embed` on Titan (1 text per request) and Cohere (96 per request, `input_type`) | Indexing documents (`search_document`) and search queries (`search_query`) |
| `04_sync.py` | `converse_sync` / `embed_sync` on the direct path, `close_sync()` | Sync code only: scripts, Celery tasks, sync libraries. Async code uses 01/03 |
| `05_errors_and_fallback.py` | Model fallback when a model is off; `CallFailed.kind` to branch on | Every caller: map `kind` to a user message or a retry |
| `06_jobs.py` | `register_job` / `send_job`: work that runs at least once, LLM calls inside | Ingestion, nightly re-embedding, any batch work that should not block a request |
| `07_status_and_registry.py` | `status`, `request_status`, live `registry.set`, audit `log`, `health` | Admin pages, runbooks, readiness checks |
| `08_langchain.py` | `QueuedChatBedrockConverse` (with tools) and `QueuedBedrockEmbeddings` | LangChain / LangGraph chains, agents, RAG: drop-in for `ChatBedrockConverse` / `BedrockEmbeddings` |
| `09_http_router.py` | `http_router` on FastAPI: health without auth, status / registry behind the app's admin check, audited writes (`actor=`), `/drain` with `pause_receiving()` | Admin API and Kubernetes probes; the preStop hook |

`registry.json` defines the four models the samples use: chat `gpt120b` (falls back to `gpt20b`)
and `gpt20b`, and embeddings `titan` and `cohere`, all on one AWS account. The quotas are
small sample numbers. Use your Service Quotas numbers in a real registry.

## Run them

Start the local stack once (Valkey, plus fakellm as a fake Bedrock):

```
python scripts/stack.py up
```

Then run the samples from the library root:

```
python scripts/samples.py                              # all, on fakellm (no real LLM, no cost)
python scripts/samples.py 03_embed 08_langchain        # some
python scripts/samples.py --live                       # all, on real Bedrock
python scripts/samples.py --live --env-file ../../.env # AWS keys from another .env
python scripts/samples.py --live --store env --env-file ../../.env  # and the env file's store
```

- **Stored keys:** each run uses its own key prefix on the local Valkey and deletes its keys at the end.
- **What `--live` reads:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_MODEL_REGION` from the env file. It never prints them.
- **Region:** every model is placed in that region.
- **Cost:** a full live run makes about 20 small calls, well under a cent.
- **`--store env`:** the store is the env file's `LLMQ_STORE_URL`, or `rediss://` built from
  the app's `valkey_host` / `valkey_port` / `valkey_key`. Only the run's own prefix is touched
  and deleted, so a shared store (ElastiCache) is safe.

`python scripts/check.py samples` runs the fakellm pass with lint and type checks, as part of
`check.py all`.

## Use them in an app

An app sets the same environment the runner sets, from its secrets:

| Variable | Runner value | In an app |
|---|---|---|
| `LLMQ_STORE_URL` | `redis://:localdev@127.0.0.1:6379/0` | `rediss://...` of the ElastiCache Valkey |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | per run | per environment, e.g. `llmq:prod` |
| `LLMQ_REGISTRY` | `samples/registry.json`, region set | the app's registry document |
| `LLMQ_METRICS_SINK` | `none` | `emf` (CloudWatch) or `prometheus` |
| AWS credentials | from `.env` (or fake for fakellm) | the pod's role; `role_arn` accounts assume roles |

Start the queue once per process, in the app's startup hook, and stop it on shutdown:

```python
@asynccontextmanager
async def lifespan(app):
    await llmq.start()            # a failure here should fail the pod: queued models need it
    yield
    llmq.pause_receiving()        # not ready; no new queued work (also from a preStop /drain)
    await llmq.stop(drain_secs=20)  # one budget for running jobs and calls together
```

`09_http_router.py` shows the `/drain` endpoint for the preStop hook and the health routes for
the probes. Outside dev, `LLMQ_STORE_URL` must be `rediss://` and `LLMQ_ID_HASH_SALT` set.

## Measured on real Bedrock (us-east-1, 2026-11-12)

- **All four models** answered through the queue.
- **Embeddings:**

  | Model | Similar pair | Unrelated pair | Dimensions |
  |---|---|---|---|
  | Titan v2 | 0.73 | 0.07 | 1024 |
  | Cohere v3 | 0.83 | 0.37 | 1024 |

- **Streaming:** a warm stream of gpt-oss-20b took 0.83 s through the queue; the same prompt took 0.7 s called directly. Most of the time is the model's own.
- **Fallback:** with `gpt120b` off, the call went to `gpt20b` (`meta.fallback == "model"`).
- **Tool calls:** a LangChain tool call came back as `stock_price({"symbol": "RELIANCE"})`.
- **ElastiCache (Valkey 8.2, TLS) as the store:** all nine samples pass with `--live --store
  env` (2026-11-13); the run left no keys behind.
- **ap-south-1:** gpt-oss-120b, gpt-oss-20b, Titan v2 and Cohere v3 answer there too (one call
  each, 2026-11-13); the app's own paths ran end to end there through the queue.
