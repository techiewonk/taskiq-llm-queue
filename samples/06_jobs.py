"""Background jobs (ingestion, nightly re-embedding): queued work that runs at least once.

Register the job function in every worker process before `start()`; any process can send a
job. A job that raises is retried, so make it safe to run twice (e.g. upsert by id). LLM calls
made inside a job go through the same queue and budgets as user calls, at normal priority.

    python scripts/samples.py 06_jobs [--live]
"""

from __future__ import annotations

import asyncio
from typing import Any

from taskiq_llm_queue import LLMQueue

done: asyncio.Queue[dict[str, Any]] = asyncio.Queue()


async def main() -> None:
    llmq = LLMQueue.from_env()

    async def index_documents(payload: dict[str, Any]) -> None:
        result = await llmq.embed("cohere", payload["texts"], input_type="search_document")
        # write result.vectors to the vector store here, keyed by payload["ids"]
        await done.put({"ids": payload["ids"], "vectors": len(result.vectors)})

    llmq.register_job("index_documents", index_documents)
    await llmq.start()
    try:
        job_id = await llmq.send_job(
            "index_documents", {"ids": ["d1", "d2"], "texts": ["first report", "second report"]}
        )
        print(f"sent job {job_id}")
        finished = await asyncio.wait_for(done.get(), timeout=60)
        print(f"job finished: {finished}")
        assert finished["vectors"] == 2
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
