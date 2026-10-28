# Examples

Runnable examples live in [`samples/`](../samples/README.md). Every script runs against the local
Docker stack (`make up`), and `13_research_assistant_flow.py` can also run against real Bedrock.

| Sample | Shows |
|---|---|
| `01_quickstart_converse.py` | `start()`, `converse()`, result meta, `stop()` |
| `02_chat_openai_compatible.py` | `chat()` and `QueuedChatOpenAI` on an OpenAI-compatible server |
| `03_embeddings.py` | Titan, Cohere and OpenAI embeddings; `invoke_model()` |
| `04_lanes_and_request_context.py` | `rt` vs `bg` lanes with `request_context` |
| `05_fallback_on_throttle.py` | Region throttled → APAC profile → another model |
| `06_send_only_and_worker_pods.py` | Send-only and worker processes |
| `07_langchain_bedrock.py` | `QueuedChatBedrockConverse`: `ainvoke`, chains, `bind_tools` |
| `08_langgraph_agent.py` | A small LangGraph agent through the queue |
| `09_jobs.py` | `register_job` / `send_job` |
| `10_cancel_and_timeout.py` | `cancel()`, `timeout_secs`, `deadline_at` |
| `11_sync_calls.py` | `*_sync` calls and `strict_async` |
| `12_cli.md` | `llm-queue` commands for routing, fallback, limiter, breaker, queues, jobs |
| `13_research_assistant_flow.py` | A real app's 12 LLM/embedding flows; `--target local|aws`, settings from `.env` or Secrets Manager |
