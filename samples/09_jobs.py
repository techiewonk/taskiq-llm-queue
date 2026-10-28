"""Background jobs: send an app function by name through SQS; a worker pod runs it once.

1. `QueueConfig.job_queues` maps a job name to its SQS queue (here `ingest` -> `jobs-ingest`).
2. Every worker pod allow-lists the job with `register_job(name, async_fn)`; a pod that
   does not know a job name releases the message for another pod instead of dropping it.
3. Any process (even `mode="send"`, or the `llm-queue job send` CLI) calls
   `send_job(name, payload)`, which returns the job id at once; it is not awaited.

The job here embeds its payload's texts on the `bulk` lane, the usual ingestion pattern.
`send_job` has no result channel, so this sample signals completion with an `asyncio.Event`;
a real job writes its output somewhere (a DB, S3, ...).

Run: .venv/Scripts/python samples/09_jobs.py
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue


async def main() -> None:
    """Register an ingest job, send it, and wait until the worker has run it."""
    config = local_config(job_queues={"ingest": "jobs-ingest"})
    await ensure_queues(config)
    llmq = LLMQueue(config)
    run_tag = f"daily-notes-{uuid.uuid4().hex[:6]}"
    done = asyncio.Event()
    running = 0  # jobs in progress on this pod (older jobs left in the queue may run too)

    async def ingest(payload: dict[str, Any]) -> None:
        """The job: embed every document of the payload on the bulk lane."""
        nonlocal running
        running += 1
        try:
            vectors = await llmq.embed("titan", payload["texts"], lane="bulk")
        finally:
            running -= 1
        print(f"job ran: {payload['source']!r} -> {len(vectors)} vectors")
        if payload["source"] == run_tag:
            done.set()

    llmq.register_job("ingest", ingest)  # allow-list before start() so no message is missed
    await llmq.start()
    try:
        job_id = await llmq.send_job(
            "ingest",
            {"source": run_tag, "texts": [f"note {i}" for i in range(20)]},
            timeout_secs=600,  # drop the job if it has not run within 10 minutes
        )
        print("sent job:", job_id)
        await asyncio.wait_for(done.wait(), timeout=60)
        # stop() hands running jobs back to SQS after drain_secs; let them finish first.
        for _ in range(100):
            if not running:
                break
            await asyncio.sleep(0.1)
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
