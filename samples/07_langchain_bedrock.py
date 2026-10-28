"""LangChain `ChatBedrockConverse` code, unchanged, with every call going through the queue.

`QueuedChatBedrockConverse` subclasses `langchain_aws.ChatBedrockConverse`: it builds the
Converse request with LangChain's own helpers and parses the reply the same way, so
`ainvoke`, `bind_tools`, prompt | model chains and callbacks behave as before. The queue adds
`served_model`, `served_target`, `fallback_used`, ... to `response_metadata`.

Pass the real Bedrock id as `model_id` (LangChain uses it to detect the provider) and the queue
alias as `model_alias`. Streaming is always off: `astream()` yields one complete message.

Run: .venv/Scripts/python samples/07_langchain_bedrock.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from _local import ensure_queues, local_config
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.langchain import QueuedChatBedrockConverse


@tool
def get_price(ticker: str) -> str:
    """Return the latest price of a stock ticker."""
    return f"{ticker}: 101.5"


class CountTokens(AsyncCallbackHandler):
    """A callback, like Langfuse or LangSmith would add; it sees queued calls as usual."""

    def __init__(self) -> None:
        """Start with no calls seen."""
        self.calls = 0

    async def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        """Count every finished model call."""
        self.calls += 1


async def main() -> None:
    """Plain call, prompt | model chain with a callback, and tool calling."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        model = QueuedChatBedrockConverse(
            llmq=llmq,  # optional: without it, the running LLMQueue is found on first use
            model_id="openai.gpt-oss-120b-1:0",  # LangChain also accepts it as model=
            model_alias="gpt120b",
            temperature=0.2,
            max_tokens=256,
        )
        with request_context(lane="rt", session_id="s-7", user_id="u-7"):
            reply = await model.ainvoke("Summarise today's market in one line.")
            meta = reply.response_metadata
            print("ainvoke      :", str(reply.content)[:60])
            print(
                "  served by  :",
                f"{meta['served_model']}@{meta['served_target']}",
                f"queue_wait_ms={meta['queue_wait_ms']}",
            )

            prompt = ChatPromptTemplate.from_messages(
                [("system", "You are a {role}."), ("human", "{question}")]
            )
            counter = CountTokens()
            chain = prompt | model
            answer = await chain.ainvoke(
                {"role": "terse analyst", "question": "Is the market up?"},
                config={"callbacks": [counter]},
            )
            print("chain        :", str(answer.content)[:60], f"(callback calls={counter.calls})")

            with_tools = model.bind_tools([get_price])
            tool_reply = await with_tools.ainvoke("What is the price of INFY?")
            assert isinstance(tool_reply, AIMessage)  # ainvoke is typed as BaseMessage
            print("bind_tools   :", tool_reply.tool_calls)
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
