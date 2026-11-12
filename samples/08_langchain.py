"""LangChain: drop-in chat model and embeddings that go through the queue (chains, agents, RAG).

`QueuedChatBedrockConverse` is a `ChatBedrockConverse` whose calls go through the queue (tools,
streaming and structured output work as usual); `QueuedBedrockEmbeddings` is a
`BedrockEmbeddings`. They find the started `LLMQueue` on their own.

    python scripts/samples.py 08_langchain [--live]
"""

from __future__ import annotations

import asyncio

from langchain_core.messages import AIMessage
from langchain_core.tools import tool

from taskiq_llm_queue import LLMQueue
from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse


@tool
def stock_price(symbol: str) -> str:
    """Latest price of an NSE stock symbol."""
    return f"{symbol}: 2,950.10"


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        # model_id comes from the registry of the started queue
        chat = QueuedChatBedrockConverse(model_alias="gpt120b", max_tokens=400)  # type: ignore[call-arg]
        message = await chat.ainvoke("In one line, what is an index fund?")
        print(f"chat: {message.text()[:120]!r} via {message.response_metadata['deployment']}")

        with_tools = chat.bind_tools([stock_price])
        called = await with_tools.ainvoke("Use the stock_price tool to get the price of RELIANCE.")
        assert isinstance(called, AIMessage)
        print(f"tool calls: {[(c['name'], c['args']) for c in called.tool_calls]}")
        assert called.tool_calls
        assert called.tool_calls[0]["name"] == "stock_price"

        emb = QueuedBedrockEmbeddings(model_alias="titan")
        docs = await emb.aembed_documents(["first", "second"])
        query = await emb.aembed_query("which one")
        print(f"embeddings: {len(docs)} docs, query of {len(query)} dims")
        assert len(docs) == 2
        assert query
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
