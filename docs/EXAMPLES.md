# taskiq-llm-queue — runnable examples (draft)

All examples run against the local Docker stack (`make up`) and are executed by `make scenarios` / scenario S24, so they can't silently rot.

| File (`examples/`) | Shows | Scenario |
|---|---|---|
| `quickstart.py` | 20-line converse through the queue | S01 |
| `chat_bedrock_langchain.py` | `QueuedChatBedrockConverse` with `bind_tools`, prompt \| model chain, callbacks | S01, S04 |
| `chat_openai_compatible.py` | `QueuedChatOpenAI` against any `base_url` (fakellm, vLLM, Ollama) | S02 |
| `embeddings.py` | Titan, Cohere and OpenAI embeddings, batched | S03 |
| `langgraph_dropin.py` | Small LangGraph graph with `astream(stream_mode=["custom"])`, unchanged except the model class | S04 |
| `lanes_priority.py` | Flood `bg`, measure `rt` wait | S06 |
| `chat_fallback.py` | `--policy chain\|same_model`; prints `served_model`, `served_target`, `fallback_used` | S10, S11 |
| `canary_routing.py` | Set pct, show sticky sessions, kill switch | S19 |
| `cancel_and_deadline.py` | Cancel in-flight ids; deadline expiry | S16, S17 |
| `large_params.py` | 300 KB prompt by reference | S18 |
| `custom_provider.py` | Register a toy `Provider` | S25 |
| `jobs_ingest.py` | Send a job; job fans out embeddings with `lane=bulk` | S23 |
| `fastapi_app.py` | Lifespan wiring: `start(workers=True)` / `stop()` drain | S13, S15 |
| `parity/multi_queue.py` | Route tasks to named queues (`queue_name` label) | S24 |
| `parity/delayed.py` | `delay` label 0–900 s | S24 |
| `parity/fifo.py` | FIFO `group_id`, `deduplication_id`, task-id dedup | S24 |
| `parity/fair_queue.py` | Fair queue message groups | S24 |
| `parity/batching.py` | Batched send, `skip_batching`, flush on shutdown | S24 |
| `parity/expiry.py` | `expiry` label | S24 |
| `parity/metadata.py` | Receipt handle / message id / receive count in task | S24 |
| `parity/heartbeat.py` | Long task kept invisible by the heartbeat | S24 |
| `parity/s3_offload.py` | Large payload via S3 (MinIO container, optional) | S24 |
| `parity/s3_results.py` | S3 result backend (MinIO, optional) | S24 |
