"""LangChain / LangGraph drop-in (S04): queued model classes used the way an app uses them.

Models are built without `llmq=` (as at import time) and find the started queue on first use.
Covers ainvoke, abatch, bind_tools, structured output, a LangGraph tool loop, lane from
`request_context`, sync invoke (direct), and queued embeddings.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any

from tools.scenarios.env import GPT20B_ID, GPT120B_ID, TITAN_ID
from tools.scenarios.harness import Ctx, scenario
from tools.scenarios.metrics import RecordingSink


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / ((math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))) or 1.0)


def _sent(sink: RecordingSink, queue: str) -> float:
    """SQS sends this sender made to `queue` (`llmq_sqs_sent_total{queue=...}`)."""
    return sum(
        v
        for k, v in sink.counters.items()
        if k.startswith("llmq_sqs_sent_total{")
        and f"queue={queue}" in k[:-1].split("{", 1)[1].split(",")  # exact label, not a prefix
    )


@scenario("S04", "LangChain/LangGraph drop-in", aws_safe="low-volume")
async def s04(ctx: Ctx) -> None:
    """Every LangChain / LangGraph call goes through SQS to a worker.

    Lanes follow the context, sync calls run direct, embeddings match async vs sync.
    """
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langchain_core.tools import tool
    from langgraph.graph import START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from pydantic import BaseModel

    from taskiq_llm_queue import request_context
    from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

    await ctx.workers(1)
    sink = RecordingSink()
    await ctx.queue(metrics=sink)  # registers the default queue the models resolve
    limits: dict[str, Any] = {"max_tokens": ctx.max_tokens} if ctx.max_tokens else {}
    chat = QueuedChatBedrockConverse(model_id=GPT20B_ID, model_alias="gpt20b", **limits)
    big = QueuedChatBedrockConverse(model_id=GPT120B_ID, model_alias="gpt120b", **limits)

    def by_worker(message: Any) -> bool:
        meta = message.response_metadata
        return meta.get("worker") == "w1" and meta.get("served_model") in ("gpt20b", "gpt120b")

    with request_context(lane="rt", session_id="s04", user_id="u04"):
        one = await chat.ainvoke([HumanMessage(ctx.text(ctx.rid(1), "say hi"))])
        ctx.expect(isinstance(one, AIMessage) and bool(one.content), f"ainvoke: {one!r}")
        ctx.expect(by_worker(one), f"ainvoke not served by the worker: {one.response_metadata}")
        many = await chat.abatch([[HumanMessage(ctx.text(ctx.rid(f"b{i}")))] for i in range(3)])
        ctx.expect(len(many) == 3 and all(by_worker(m) for m in many), f"abatch: {many}")
    rt_sends = _sent(sink, ctx.settings.queue("gpt20b-rt"))
    with request_context(lane="bg"):
        bg = await chat.ainvoke([HumanMessage(ctx.text(ctx.rid("bg")))])
        ctx.expect(by_worker(bg), f"bg call: {bg.response_metadata}")
    ctx.metric("sends_rt", rt_sends)
    ctx.metric("sends_bg", _sent(sink, ctx.settings.queue("gpt20b")))
    ctx.expect(rt_sends == 4, f"rt calls did not use the rt queue: {dict(sink.counters)}")
    ctx.expect(_sent(sink, ctx.settings.queue("gpt20b")) == 1, "bg call did not use the bg queue")

    @tool
    def get_price(ticker: str) -> str:
        """Return the latest price of a stock ticker."""
        return f"{ticker} last traded at 101.5"

    class Quote(BaseModel):
        """A stock quote."""

        ticker: str
        price: float

    with request_context(lane="rt"):
        tooled = await big.bind_tools([get_price]).ainvoke(
            [HumanMessage(ctx.text(ctx.rid("t"), "What is the price of INFY? Use the tool."))]
        )
        ctx.expect(by_worker(tooled), f"bind_tools: {tooled.response_metadata}")
        if not ctx.aws:  # fakellm echoes a tool call for gpt120b; a real model may answer
            ctx.expect(
                isinstance(tooled, AIMessage)
                and [c["name"] for c in tooled.tool_calls] == ["get_price"],
                f"tools: {tooled}",
            )
            quote = await big.with_structured_output(Quote).ainvoke(
                [HumanMessage(ctx.text(ctx.rid("s"), "INFY quote"))]
            )
            ctx.expect(isinstance(quote, Quote), f"structured output: {quote!r}")

        model = big.bind_tools([get_price])

        async def agent(state: MessagesState) -> dict[str, list[Any]]:
            return {"messages": [await model.ainvoke(state["messages"])]}

        graph = StateGraph(MessagesState)
        graph.add_node("agent", agent)
        graph.add_node("tools", ToolNode([get_price]))
        graph.add_edge(START, "agent")
        graph.add_conditional_edges("agent", tools_condition)
        graph.add_edge("tools", "agent")
        result = await graph.compile().ainvoke(
            {"messages": [HumanMessage(ctx.text(ctx.rid("g"), "Price of INFY? Use the tool."))]},
            {"recursion_limit": 6},
        )
        messages = result["messages"]
        final = messages[-1]
        ctx.metric("graph_messages", [m.type for m in messages])
        ctx.expect(isinstance(final, AIMessage) and not final.tool_calls, f"graph end: {final}")
        ctx.expect(all(by_worker(m) for m in messages if isinstance(m, AIMessage)), "graph meta")
        if not ctx.aws:
            ctx.expect(any(isinstance(m, ToolMessage) for m in messages), "graph ran no tool")

    sync = await asyncio.to_thread(chat.invoke, [HumanMessage(ctx.text(ctx.rid("sync")))])
    worker = str(sync.response_metadata.get("worker", ""))
    ctx.metric("sync_worker", worker)
    ctx.expect(bool(sync.content) and worker.startswith("sync:"), f"sync invoke: {worker!r}")

    embeddings = QueuedBedrockEmbeddings(model_id=TITAN_ID, model_alias="titan")
    texts = [ctx.text(ctx.rid(f"e{i}"), "nifty fifty") for i in range(3)]
    with request_context(lane="rt"):
        vectors = await embeddings.aembed_documents(texts)
        query = await embeddings.aembed_query(texts[0])
    direct = await asyncio.to_thread(embeddings.embed_query, texts[0])
    ctx.metric("dims", len(vectors[0]) if vectors else 0)
    ctx.expect(len(vectors) == 3 and len({len(v) for v in vectors}) == 1, "embed_documents")
    ctx.expect(_cosine(query, direct) > 0.999, "async (queued) and sync (direct) query differ")
    calls, _ = await ctx.provider_calls()
    ctx.metric("provider_calls", calls)
    await ctx.final_checks()
