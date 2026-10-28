from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, TypedDict

from langchain_core.messages import AIMessage, BaseMessage
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from lc_fakes import CONVERSE_TEXT_RESPONSE, FakeQueueClient  # type: ignore[import-not-found]  # bare-name test helper

from taskiq_llm_queue.langchain import QueuedChatBedrockConverse

SRC = Path(__file__).resolve().parents[3] / "src"


class _State(TypedDict, total=False):
    question: str
    answer: str


def _graph(model: QueuedChatBedrockConverse) -> Any:
    async def answer(state: _State) -> _State:
        writer = get_stream_writer()
        writer({"status": "calling"})
        reply = await model.ainvoke(state["question"])
        writer({"status": "done"})
        return {"answer": str(reply.content)}

    graph = StateGraph(_State)
    graph.add_node("answer", answer)
    graph.add_edge(START, "answer")
    graph.add_edge("answer", END)
    return graph.compile()


async def test_graph_custom_stream_mode_unchanged() -> None:
    fake = FakeQueueClient(converse_response=CONVERSE_TEXT_RESPONSE)
    app = _graph(QueuedChatBedrockConverse(model_id="gpt120b", llmq=fake))
    events = [e async for e in app.astream({"question": "hi"}, stream_mode=["custom"])]
    assert events == [("custom", {"status": "calling"}), ("custom", {"status": "done"})]
    assert fake.ops == ["converse"]


async def test_graph_messages_stream_mode_gives_one_full_message() -> None:
    fake = FakeQueueClient(converse_response=CONVERSE_TEXT_RESPONSE)
    app = _graph(QueuedChatBedrockConverse(model_id="gpt120b", llmq=fake))
    messages: list[BaseMessage] = [
        msg async for msg, _meta in app.astream({"question": "hi"}, stream_mode="messages")
    ]
    assert len(messages) == 1
    assert isinstance(messages[0], AIMessage)
    assert messages[0].content == "Markets rose."


def test_core_import_does_not_import_langchain() -> None:
    """Importing the core package (and the adapter package's front door) stays langchain-free."""
    code = (
        "import sys, taskiq_llm_queue, taskiq_llm_queue.langchain\n"
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in "
        "{'langchain_core', 'langchain_aws', 'langchain_openai', 'langgraph'})\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, cwd=SRC)
