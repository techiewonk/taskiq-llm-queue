"""Quickstart: one Bedrock Converse call through the queue, sent and worked by this process.

Shows the minimum lifecycle: build an `LLMQueue`, `start(mode="both")` (this process sends
calls *and* runs the in-process worker), call `converse`, then `stop()` to drain.

The call goes: `converse` -> SQS queue `llm-gpt120b-rt` -> worker -> fakellm (Bedrock) ->
result in Valkey -> back to the awaiting caller. `result.meta` tells you how it was served.

Run (local stack up, see samples/README.md):

    .venv/Scripts/python samples/01_quickstart_converse.py      # Windows
    .venv/bin/python samples/01_quickstart_converse.py          # Linux / macOS
"""

from __future__ import annotations

import asyncio

from _local import ensure_queues, local_config

from taskiq_llm_queue import LLMQueue, request_context


async def main() -> None:
    """Start, make one call, print the reply and how it was served, stop."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start(mode="both")  # "send" | "work" | "both"
    try:
        # request_context sets the lane (rt = live user traffic) and ids for routing/tracing.
        with request_context(lane="rt", session_id="s-1", user_id="u-1"):
            result = await llmq.converse(
                "gpt120b",  # a model alias from QueueConfig.models, optionally "alias@target"
                {
                    "system": [{"text": "You are a terse market analyst."}],
                    "messages": [
                        {"role": "user", "content": [{"text": "Summarise today's market."}]}
                    ],
                    "inferenceConfig": {"maxTokens": 128},
                },
            )
        # `response` is the exact Bedrock Converse dict (minus ResponseMetadata).
        text = result.response["output"]["message"]["content"][0]["text"]
        print("reply       :", text[:80])
        print("stop reason :", result.response["stopReason"])
        print("usage       :", result.response["usage"])
        # `meta` says which model/target served the call and how long it queued.
        meta = result.meta
        print(
            f"served by   : {meta.served_model}@{meta.served_target} "
            f"(fallback_used={meta.fallback_used}, attempts={meta.attempts})"
        )
        print(f"timings     : queue_wait_ms={meta.queue_wait_ms} provider_ms={meta.provider_ms}")
        print(f"worker      : {meta.worker} (this pod: {llmq.pod_id})")
    finally:
        await llmq.stop()  # pause receiving, drain running work, close connections


if __name__ == "__main__":
    asyncio.run(main())
