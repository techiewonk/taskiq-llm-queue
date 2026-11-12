"""Call from sync code (scripts, Celery tasks, threads of a sync library).

`converse_sync` / `embed_sync` run on the direct path in a private event loop thread: same
RPM / TPM budgets and health as queued calls, but no queue wait. Use them only where an event
loop is not available; async code should use `converse` / `embed`.

    python scripts/samples.py 04_sync [--live]
"""

from __future__ import annotations

from taskiq_llm_queue import LLMQueue


def main() -> None:
    llmq = LLMQueue.from_env()  # no start() needed for sync calls; nothing to await
    request = {
        "messages": [{"role": "user", "content": [{"text": "Reply with the word ready."}]}],
        "inferenceConfig": {"maxTokens": 200},
    }
    reply = llmq.converse_sync("gpt20b", request)
    blocks = reply.response["output"]["message"]["content"] if reply.response else []
    text = "".join(b["text"] for b in blocks if "text" in b)
    print(f"gpt20b (sync): {text.strip()[:80]!r} via {reply.meta.deployment}")
    vectors = llmq.embed_sync("titan", ["sync embedding"]).vectors
    print(f"titan (sync): {len(vectors)} vector of {len(vectors[0])} dims")
    assert text
    assert len(vectors) == 1
    llmq.close_sync()


if __name__ == "__main__":
    main()
