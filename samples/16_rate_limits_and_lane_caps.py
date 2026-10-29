"""Rate limits (RPM / TPM), a bucket per lane, and `llm-queue status`.

1. RPM 10 per minute on gpt20b (`fixed` window): 12 bg calls start 8 now (bg keeps 20 % of
   the budget for rt), the other 4 wait in SQS for the next calendar minute.
2. An rt call in the same minute still gets one of the 2 reserved tokens at once.
3. `lane_max_concurrency` bulk 2: 8 parallel bulk calls never run more than 2 at once,
   inside the model's bucket of 6.
4. `llm-queue status` for gpt20b: waiting / working / started this minute / RPM left.

Everything is set in code with `settings_default`, the same JSON as `LLMQ_SETTINGS` in .env /
Secrets Manager; `llm-queue settings set --model gpt20b rpm=500` changes it live.
Runs about 1-2 minutes (step 1 waits for the next minute).

Run: .venv/Scripts/python samples/16_rate_limits_and_lane_caps.py
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import time

from _local import (
    GPT20B_PRIMARY,
    ensure_queues,
    local_config,
    patch_model,
    restore_scenario,
    save_scenario,
)

from taskiq_llm_queue import LLMQueue, request_context
from taskiq_llm_queue.cli import main as cli
from taskiq_llm_queue.config import create_redis
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.model_settings import model_settings_store
from taskiq_llm_queue.reliability import AdaptiveLimiter

HELLO = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
SLOW = {"latency_ms": {"p50": 500, "p95": 600}}


async def timed(llmq: LLMQueue, lane: str, t0: float) -> float:
    """One gpt20b call on `lane`; seconds from `t0` until it finished."""
    with request_context(lane=lane):
        await llmq.converse("gpt20b", HELLO, timeout_secs=150)
    return time.monotonic() - t0


async def peak(limiter: AdaptiveLimiter, scope: str, stop: asyncio.Event) -> int:
    """Highest in-flight count of one limiter scope, sampled every 20 ms."""
    best = 0
    while not stop.is_set():
        best = max(best, (await limiter.state(scope, "primary")).inflight)
        await asyncio.sleep(0.02)
    return best


async def main() -> None:
    """RPM budget, rt reserve, a bulk lane cap and the status view."""
    config = local_config(
        settings_default={
            "models": {
                "gpt20b": {
                    "rpm": 10,
                    "rate_window": "fixed",
                    "max_concurrency": 6,
                    "lane_max_concurrency": {"bulk": 2},
                }
            }
        },
        limiter={"bg_share": 1.0},
    )
    await ensure_queues(config)
    scenario = await save_scenario()
    redis = create_redis(config, client_name="sample-16")
    keys = KeySpace(config.key_prefix)
    await model_settings_store(redis, keys).reset(actor="sample-16")
    await redis.delete(keys.rate("gpt20b", "primary"))  # a fresh minute budget for the demo
    await patch_model(GPT20B_PRIMARY, SLOW)
    llmq = LLMQueue(config)
    await llmq.start()
    try:
        left = 60 - time.time() % 60
        if left < 40:  # start early in a minute so the two windows are easy to see
            print(f"   (waiting {left:.0f}s for a fresh minute)")
            await asyncio.sleep(left + 0.5)
        t0 = time.monotonic()
        bg = [asyncio.create_task(timed(llmq, "bg", t0)) for _ in range(12)]
        await asyncio.sleep(3)
        rt = await timed(llmq, "rt", time.monotonic())
        done = sorted(await asyncio.gather(*bg))
        now = [d for d in done if d < 20]
        print(
            f"1. RPM 10 fixed, 12 bg calls: {len(now)} finished at once, "
            f"{len(done) - len(now)} after the next minute started ({done[-1]:.0f}s)"
        )
        print(f"2. rt call during the full minute finished in {rt:.1f}s (rt reserve)")

        limiter = AdaptiveLimiter(redis, keys)
        stop = asyncio.Event()
        sampler = asyncio.create_task(peak(limiter, "gpt20b#bulk", stop))
        await redis.delete(keys.rate("gpt20b", "primary"))
        with request_context(lane="bulk"):
            await asyncio.gather(*(llmq.converse("gpt20b", HELLO) for _ in range(8)))
        stop.set()
        print(f"3. 8 parallel bulk calls, bulk cap 2 -> peak in flight {await sampler}")

        out = io.StringIO()
        env = {
            "LLMQ_KEY_PREFIX": config.key_prefix,
            "LLMQ_REDIS_HOST": config.redis_host,
            "LLMQ_REDIS_PORT": str(config.redis_port),
            "LLMQ_REDIS_PASSWORD": (
                config.redis_password.get_secret_value() if config.redis_password else ""
            ),
            "LLMQ_SQS_ENDPOINT_URL": str(config.sqs_endpoint_url),
            "LLMQ_AWS_REGION": str(config.aws_region),
            "LLMQ_MODELS": json.dumps(
                {k: v.model_dump(mode="json") for k, v in config.models.items()}
            ),
            "LLMQ_SETTINGS": config.settings_default.model_dump_json(),
        }
        with contextlib.redirect_stdout(out):
            await asyncio.to_thread(cli, ["status", "--model", "gpt20b"], environ=env)
        (row,) = json.loads(out.getvalue())
        (target,) = row["targets"]
        print(
            f"4. status: started this minute {target['started']}, rpm_left "
            f"{target['rpm_left']}, working {target['working']}, waiting "
            f"{row['queues']['default']['waiting']}"
        )
    finally:
        await llmq.stop()
        await redis.aclose()
        await restore_scenario(scenario)


if __name__ == "__main__":
    asyncio.run(main())
