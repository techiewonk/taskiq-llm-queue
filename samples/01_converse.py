"""Chat with a Bedrock model through the queue (async code: FastAPI handlers, agents, chains).

`LLMQueue.from_env()` reads `LLMQ_*`; `start()` loads the registry and runs workers in this
process. `request_context` sets priority and ids for every call inside it. The reply's
`response` is the Bedrock Converse body; `meta` says where and how the call was served.

    python scripts/samples.py 01_converse            # fakellm
    python scripts/samples.py 01_converse --live     # real Bedrock (AWS keys from .env)
"""

from __future__ import annotations

import asyncio
from typing import Any

from taskiq_llm_queue import LLMQueue, request_context


def answer_text(response: dict[str, Any] | None) -> str:
    """The text blocks of a Converse reply (gpt-oss also returns reasoning blocks)."""
    blocks = (response or {}).get("output", {}).get("message", {}).get("content", [])
    return "".join(b["text"] for b in blocks if "text" in b)


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        request = {
            "messages": [{"role": "user", "content": [{"text": "Name one Indian stock index."}]}],
            "inferenceConfig": {"maxTokens": 300},
        }
        for model in ("gpt120b", "gpt20b"):
            # A user-facing call: priority calls are read before batch ones.
            with request_context(priority=True, session_id="s-1", user_id="u-1"):
                reply = await llmq.converse(model, request)
            text = answer_text(reply.response)
            meta = reply.meta
            print(f"{model}: {text.strip()[:120]!r}")
            print(
                f"  served by {meta.served_model} on {meta.deployment}, "
                f"tokens {meta.tokens_in}/{meta.tokens_out}, queue wait {meta.queue_wait_ms} ms"
            )
            assert reply.ok, reply.error
            assert text, f"{model} returned no text"
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
