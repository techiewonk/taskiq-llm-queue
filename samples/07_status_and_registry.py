"""Operate: status of models and calls, and live registry edits (admin pages, runbooks).

`status(model)` shows waiting calls, calls in progress and each deployment's RPM / TPM used
and left this minute. `request_status(id)` follows one call. `registry.set` changes a safe
setting live in every pod (audited); `registry.log` lists the changes. The CLI `llm-queue`
and the HTTP router (`llmq.http_router()`, extra `[http]`) expose the same.

    python scripts/samples.py 07_status_and_registry [--live]
"""

from __future__ import annotations

import asyncio

from taskiq_llm_queue import LLMQueue

REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "One word: ok"}]}],
    "inferenceConfig": {"maxTokens": 100},
}


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        reply = await llmq.converse("gpt20b", REQUEST)
        call = await llmq.request_status(reply.request_id)
        print(f"request {reply.request_id}: state={call['state']}")

        model = await llmq.status("gpt20b")
        for dep in model["deployments"]:
            print(
                f"{dep['name']}: rpm {dep['rpm']['used']}/{dep['rpm']['limit']} "
                f"tpm {dep['tpm']['used']}/{dep['tpm']['limit']} healthy={dep['healthy']} "
                f"breaker={dep['breaker']}"
            )
        print(f"waiting={model['waiting']} in_progress={model['in_progress']}")

        before = llmq.registry.current.value("gpt20b", "rpm")
        await llmq.registry.set({"rpm": 45}, model="gpt20b", actor="sample")
        print(f"gpt20b rpm {before} -> {llmq.registry.current.value('gpt20b', 'rpm')} (live)")
        for entry in await llmq.registry.log(count=3):
            print(f"  audit: {entry}")
        await llmq.registry.set({"rpm": before}, model="gpt20b", actor="sample")
        assert call["state"] == "done"
        print(f"health: {await llmq.health()}")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
