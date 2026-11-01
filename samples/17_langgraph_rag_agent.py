"""LangChain + LangGraph together, app style: retrieve, route, answer with tools, summarise.

One graph, every model call through the queue:

- `retrieve`: `QueuedBedrockEmbeddings` (Titan) embeds the question and a tiny in-memory corpus.
- `route`: `with_structured_output(Route)` decides whether the question needs a tool.
- `agent` / `tools`: `bind_tools` loop on gpt120b with `fallback_policy="chain"` (falls to gpt20b
  when gpt120b is throttled or down).
- `summarise`: runs in the `bg` lane (`request_context(lane="bg")` inside the node), so a slow
  summary never competes with the live turn.

The graph is streamed with `stream_mode="updates"`, and the same model is then called from a
plain thread with `.invoke()` (sync callers run direct under the shared limiter).

Run: .venv/Scripts/python samples/17_langgraph_rag_agent.py
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Literal

from _local import ensure_queues, local_config
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from pydantic import BaseModel

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

GPT120B, TITAN = "openai.gpt-oss-120b-1:0", "amazon.titan-embed-text-v2:0"
CORPUS = [
    "INFY reported 8 % revenue growth in Q2.",
    "TCS announced a buyback of 18,000 crore.",
    "NIFTY 50 closed at a record high on Friday.",
]


@tool
def get_price(ticker: str) -> str:
    """Return the latest price of a stock ticker."""
    return f"{ticker} last traded at 101.5"


class Route(BaseModel):
    """Whether the question needs live data from a tool."""

    needs_tool: bool
    reason: str


class State(MessagesState):
    """Messages plus the retrieved context and the background summary."""

    context: list[str]
    summary: str


# Built at import time without llmq=: resolved from the started LLMQueue on first use.
EMBED = QueuedBedrockEmbeddings(model_id=TITAN, model_alias="titan")
LLM = QueuedChatBedrockConverse(model_id=GPT120B, model_alias="gpt120b", fallback_policy="chain")
AGENT = LLM.bind_tools([get_price])
ROUTER = LLM.with_structured_output(Route)


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / ((math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))) or 1.0)


async def retrieve(state: State) -> dict[str, Any]:
    """Embed the question and the corpus (one batched call each), keep the best two."""
    question = str(state["messages"][-1].content)
    query, docs = await asyncio.gather(EMBED.aembed_query(question), EMBED.aembed_documents(CORPUS))
    ranked = sorted(zip(CORPUS, docs, strict=True), key=lambda d: -_cosine(query, d[1]))
    return {"context": [text for text, _ in ranked[:2]]}


async def route(state: State) -> dict[str, Any]:
    """Structured output: a `Route` object, not text."""
    decision = await ROUTER.ainvoke(state["messages"])
    print(f"  route      : needs_tool={decision.needs_tool}")  # type: ignore[union-attr]
    return {}


async def agent(state: State) -> dict[str, list[BaseMessage]]:
    """Answer with the retrieved context; may call `get_price`."""
    system = SystemMessage("Context:\n" + "\n".join(state.get("context", [])))
    return {"messages": [await AGENT.ainvoke([system, *state["messages"]])]}


async def summarise(state: State) -> dict[str, Any]:
    """One-line summary on the bg lane: queued behind live traffic, never ahead of it."""
    question, answer = state["messages"][0].content, state["messages"][-1].content
    prompt = f"Summarise in one line.\nQ: {question}\nA: {answer}"  # text only: no tool blocks
    with request_context(lane="bg"):
        reply = await LLM.ainvoke([HumanMessage(prompt)])
    return {"summary": str(reply.content)}


def build_graph() -> Any:
    """Wire retrieve -> route -> agent <-> tools -> summarise -> END."""
    graph = StateGraph(State)
    graph.add_node("retrieve", retrieve)
    graph.add_node("route", route)
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode([get_price]))
    graph.add_node("summarise", summarise)
    graph.add_edge(START, "retrieve")
    graph.add_edge("retrieve", "route")
    graph.add_edge("route", "agent")
    graph.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: "summarise"})
    graph.add_edge("tools", "agent")
    graph.add_edge("summarise", END)
    return graph.compile()


def served(message: BaseMessage) -> str:
    """`served_model@served_target via worker` from the queue's response metadata."""
    meta = message.response_metadata
    return f"{meta.get('served_model')}@{meta.get('served_target')} via {meta.get('worker')}"


async def main() -> None:
    """Stream the graph once (rt turn), then call the model from a thread (sync, direct)."""
    config = local_config()
    await ensure_queues(config)
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        app = build_graph()
        question = {"messages": [HumanMessage("What is the price of INFY and any recent news?")]}
        kind: Literal["updates"] = "updates"
        with request_context(lane="rt", session_id="s-17", user_id="u-17"):
            async for update in app.astream(question, stream_mode=kind):
                for node, value in update.items():
                    for message in (value or {}).get("messages", []):
                        calls = message.tool_calls if isinstance(message, AIMessage) else []
                        what = f"tool_calls={[c['name'] for c in calls]}" if calls else "text"
                        origin = served(message) if isinstance(message, AIMessage) else "tool"
                        print(f"  {node:10s} : {message.type:4s} {what:24s} {origin}")
                    if node == "retrieve":
                        print(f"  retrieve   : {value['context']}")
                    if node == "summarise":
                        print(f"  summarise  : {value['summary'][:60]!r} (bg lane)")

        sync = await asyncio.to_thread(LLM.invoke, "Say hi from a thread")
        print(f"  sync invoke: {served(sync)}")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
