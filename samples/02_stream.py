"""Stream an answer as it is written (chat UIs, server-sent events to the browser).

`converse_stream` yields Bedrock ConverseStream events in order. If the stream breaks after
some text was sent, `CallFailed` carries `partial_text` and the call is not retried, so the
user never sees the answer start over.

    python scripts/samples.py 02_stream [--live]
"""

from __future__ import annotations

import asyncio

from taskiq_llm_queue import CallFailed, LLMQueue, request_context


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        request = {
            "messages": [
                {"role": "user", "content": [{"text": "Explain a P/E ratio in two lines."}]}
            ],
            "inferenceConfig": {"maxTokens": 400},
        }
        pieces: list[str] = []
        events = 0
        try:
            with request_context(priority=True):
                async for event in llmq.converse_stream("gpt20b", request):
                    events += 1
                    delta = event.get("contentBlockDelta", {}).get("delta", {})
                    if "text" in delta:
                        pieces.append(delta["text"])
                        print(delta["text"], end="", flush=True)  # send to the client here
                    if "metadata" in event:
                        usage = event["metadata"]["usage"]
                        tokens = f"{usage['inputTokens']}/{usage['outputTokens']}"
                        print(f"\n  [{events} events, tokens {tokens}]")
        except CallFailed as exc:
            print(f"\n  stream failed: {exc.kind}; text so far: {exc.partial_text!r}")
            raise
        assert "".join(pieces).strip(), "the stream had no text"
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
