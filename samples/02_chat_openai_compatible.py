"""OpenAI-compatible chat through the queue: the raw client API and the LangChain adapter.

`llama` is a model alias with `provider="openai"` and a `base_url` target (fakellm here; vLLM,
Ollama or any OpenAI-compatible server in real life). Two ways to call it:

1. `llmq.chat(alias, body)` returns the chat-completion body as a dict.
2. `QueuedChatOpenAI` is a drop-in `langchain_openai.ChatOpenAI` whose HTTP calls are answered
   by the queue (needs the `openai,langchain-openai` extras).

The API key lives with the workers (`ModelConfig.api_key_env`), never in the message.

Run: .venv/Scripts/python samples/02_chat_openai_compatible.py
"""

from __future__ import annotations

import asyncio

from _local import ensure_queues, local_config
from langchain_core.messages import AIMessage

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.langchain import QueuedChatOpenAI


async def main() -> None:
    """Chat once with the client API and once through `QueuedChatOpenAI`."""
    config = local_config()
    await ensure_queues(config)  # creates llm-llama(-rt), which elasticmq.conf does not have
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        with request_context(lane="rt", session_id="s-2"):
            # 1. Client API: any chat-completions body; the alias picks the real model id.
            result = await llmq.chat(
                "llama",
                {"messages": [{"role": "user", "content": "Say hi in five words."}]},
            )
            print("chat()           :", result.response["choices"][0]["message"]["content"][:60])
            print("  model / usage  :", result.response["model"], result.response["usage"])
            print("  served by      :", result.meta.served_model, result.meta.served_target)

            # 2. LangChain: unchanged ChatOpenAI code, only the class differs.
            model = QueuedChatOpenAI(llmq=llmq, model="llama-3.1-70b", model_alias="llama")
            reply = await model.ainvoke("Say hi in five words.")
            assert isinstance(reply, AIMessage)  # ainvoke is typed as BaseMessage
            print("QueuedChatOpenAI :", str(reply.content)[:60])
            print("  token usage    :", reply.usage_metadata)
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
