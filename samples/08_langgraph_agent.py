"""A tiny LangGraph tool-calling agent whose model calls all go through the queue.

The graph is the standard "agent -> tools -> agent" loop; the only change from a direct
Bedrock agent is the model class (`QueuedChatBedrockConverse`). The model is created at
import time without `llmq=`: queued classes find the running `LLMQueue` on first use, so
module-level graphs work as long as `start()` runs before the first call.

`stream_mode="messages"` still works but yields one complete message per model call:
streaming is off so nothing bypasses the queue.

Run: .venv/Scripts/python samples/08_langgraph_agent.py
"""

from __future__ import annotations

import asyncio

from _local import ensure_queues, local_config
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.langchain import QueuedChatBedrockConverse


@tool
def get_price(ticker: str) -> str:
    """Return the latest price of a stock ticker."""
    return f"{ticker} last traded at 101.5"


TOOLS = [get_price]
# Module level, no llmq=...: resolved from the started LLMQueue on the first call.
MODEL = QueuedChatBedrockConverse(
    model_id="openai.gpt-oss-120b-1:0", model_alias="gpt120b"
).bind_tools(TOOLS)


async def agent(state: MessagesState) -> dict[str, list[BaseMessage]]:
    """Call the model with the conversation so far."""
    return {"messages": [await MODEL.ainvoke(state["messages"])]}


def build_graph() -> CompiledStateGraph[MessagesState, None, MessagesState, MessagesState]:
    """Wire the loop: agent -> (tool call?) -> tools -> agent -> ... -> END."""
    graph = StateGraph(MessagesState)
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode(TOOLS))
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")
    graph.add_edge("agent", END)
    return graph.compile()


async def main() -> None:
    """Run the agent once and print each message, then stream it with `messages` mode."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start()
    app = build_graph()
    try:
        with request_context(lane="rt", session_id="s-8", user_id="u-8"):
            question = MessagesState(messages=[HumanMessage("What is the price of INFY?")])
            result = await app.ainvoke(question)
            for message in result["messages"]:
                served = message.response_metadata.get("served_model", "")
                calls = message.tool_calls if isinstance(message, AIMessage) else []
                extra = f" tool_calls={calls}" if calls else ""
                print(f"{message.type:6s}: {str(message.content)[:50]!r} {served}{extra}")

            chunks = 0
            async for _chunk in app.astream(question, stream_mode="messages"):
                chunks += 1
            print("stream_mode='messages' chunks:", chunks, "(whole messages, not tokens)")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
