"""`llm-queue` CLI end to end on the Docker stack: routing, fallback, audit, limiter, job send.

The CLI runs in-process (`cli.main`) so each test can pass its own key prefix; checks read
Valkey and ElasticMQ directly. Every key is removed and `jobs-ingest` is purged afterwards.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
import redis.asyncio
from aiobotocore.session import get_session

from taskiq_llm_queue.cli import main
from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.envelope import TASK_JOB
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.routing import CanaryRouter, routing_store

pytestmark = pytest.mark.integration
SQS_ENDPOINT = os.environ.get("LLMQ_TEST_SQS_ENDPOINT", "http://localhost:9324")


def run(env: dict[str, str], capsys: pytest.CaptureFixture[str], *argv: str) -> str:
    code = main(["--actor", "tester", *argv], environ=env)
    out, err = capsys.readouterr()
    assert code == 0, err
    return out


def audit(client: redis.Redis, prefix: str) -> list[dict[str, str]]:
    entries: list[tuple[bytes, dict[bytes, bytes]]] = client.xrange(KeySpace(prefix).audit)  # type: ignore[assignment]  # sync client, no decode_responses
    return [{k.decode(): v.decode() for k, v in fields.items()} for _id, fields in entries]


def test_routing_set_via_cli_then_router_reload_sees_it(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run(cli_env, capsys, "routing", "set", "--model", "gpt20b", "--lane", "rt", "--pct", "100")
    run(cli_env, capsys, "routing", "allow", "add", "vip")

    async def routes() -> tuple[str, str, str]:
        client = redis.asyncio.Redis(
            host=cli_env["LLMQ_REDIS_HOST"],
            port=int(cli_env["LLMQ_REDIS_PORT"]),
            password=cli_env["LLMQ_REDIS_PASSWORD"],
        )
        try:
            router = CanaryRouter(routing_store(client, KeySpace(prefix)))
            before = router.route("gpt20b", Lane.RT, "s1", None)  # default until reload
            await router.reload()
            after = router.route("gpt20b", Lane.RT, "s1", None)
            return before, after, router.route("gpt120b", Lane.RT, "s1", "vip")
        finally:
            await client.aclose()

    assert asyncio.run(routes()) == ("direct", "sqs", "sqs")
    run(cli_env, capsys, "routing", "kill", "on")
    assert asyncio.run(routes())[1:] == ("direct", "direct")
    stored = json.loads(run(cli_env, capsys, "routing", "get"))
    assert stored["kill_switch"] is True
    assert stored["models"] == {"gpt20b": {"rt": 100}}
    rows = audit(sync_valkey, prefix)
    assert len(rows) == 3
    assert {r["actor"] for r in rows} == {"tester"}
    assert all(r["key"] == KeySpace(prefix).routing for r in rows)


def test_routing_rejects_bad_pct_without_writing(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["routing", "set", "--model", "m", "--lane", "rt", "--pct", "150"], environ=cli_env)
    assert code == 1
    assert "error" in capsys.readouterr().err
    assert sync_valkey.get(KeySpace(prefix).routing) is None
    assert audit(sync_valkey, prefix) == []


def test_fallback_set_get_test_reset(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    run(cli_env, capsys, "fallback", "set", "--model", "gpt120b", "--chain", "gpt20b")
    chain = run(cli_env, capsys, "fallback", "get", "--model", "gpt120b").strip()
    assert chain == "gpt120b@primary -> gpt120b@apac -> gpt20b@primary"

    whole = json.loads(run(cli_env, capsys, "fallback", "get"))
    assert whole["chains"] == {"gpt120b": [{"model": "gpt20b", "require_headroom": False}]}

    dry = json.loads(run(cli_env, capsys, "fallback", "test", "--model", "gpt120b"))
    assert dry["would_serve"] == "gpt120b@primary"
    assert [s["step"] for s in dry["steps"]] == [
        "gpt120b@primary",
        "gpt120b@apac",
        "gpt20b@primary",
    ]

    file = tmp_path / "fallback.json"
    file.write_text(json.dumps({"chains": {"gpt20b": ["gpt120b"]}, "skip_targets": {}}))
    run(cli_env, capsys, "fallback", "set", "--file", str(file))
    assert run(cli_env, capsys, "fallback", "get", "--model", "gpt20b").strip() == (
        "gpt20b@primary -> gpt120b@primary -> gpt120b@apac"
    )

    file.write_text('{"chains": {"gpt20b": ["gpt20b"]}}')  # self-reference: invalid
    assert main(["fallback", "set", "--file", str(file)], environ=cli_env) == 1
    capsys.readouterr()

    run(cli_env, capsys, "fallback", "reset")
    assert sync_valkey.get(KeySpace(prefix).fallback) is None
    rows = audit(sync_valkey, prefix)
    assert len(rows) == 3  # set, set --file, reset (the invalid one never reached Valkey)
    assert {r["actor"] for r in rows} == {"tester"}
    assert rows[-1]["after"] == ""


def test_limiter_show_lists_configured_targets(
    cli_env: dict[str, str], capsys: pytest.CaptureFixture[str]
) -> None:
    rows = json.loads(run(cli_env, capsys, "limiter", "show"))
    assert [(r["model"], r["target"]) for r in rows] == [
        ("gpt120b", "primary"),
        ("gpt120b", "apac"),
        ("gpt20b", "primary"),
    ]
    assert all(r["breaker"] == "closed" for r in rows)
    one = json.loads(run(cli_env, capsys, "limiter", "show", "--model", "gpt20b@primary"))
    assert len(one) == 1


@pytest.fixture
def jobs_queue(cli_env: dict[str, str]) -> Iterator[str]:
    """`jobs-ingest` on ElasticMQ, purged after the test; skipped when ElasticMQ is down."""

    async def call(op: str) -> Any:
        async with get_session().create_client(
            "sqs", region_name="ap-south-1", endpoint_url=SQS_ENDPOINT
        ) as sqs:
            url = (await sqs.get_queue_url(QueueName="jobs-ingest"))["QueueUrl"]
            if op == "purge":
                await sqs.purge_queue(QueueUrl=url)
            return url

    try:
        url = asyncio.run(call("url"))
    except Exception as exc:
        pytest.skip(f"ElasticMQ not reachable: {exc}")
    yield url
    asyncio.run(call("purge"))


def test_job_send_puts_taskiq_message_on_jobs_ingest(
    cli_env: dict[str, str], jobs_queue: str, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    payload = tmp_path / "payload.json"
    payload.write_text(json.dumps({"source": "s3://bucket/x.csv", "rows": 5}))
    env = {k: v for k, v in cli_env.items() if "REDIS" not in k}  # needs SQS only
    cli_env["LLMQ_REDIS_PORT"] = "1"  # an unusable Valkey proves the command never touches it
    sent = json.loads(run(env, capsys, "job", "send", "ingest", "--payload-file", str(payload)))

    async def receive() -> list[dict[str, Any]]:
        async with get_session().create_client(
            "sqs", region_name="ap-south-1", endpoint_url=SQS_ENDPOINT
        ) as sqs:
            out = await sqs.receive_message(
                QueueUrl=jobs_queue, MaxNumberOfMessages=10, WaitTimeSeconds=2
            )
            return [json.loads(m["Body"]) for m in out.get("Messages", [])]

    bodies = [b for b in asyncio.run(receive()) if b.get("task_id") == sent["task_id"]]
    assert len(bodies) == 1
    body = bodies[0]
    assert body["task_name"] == TASK_JOB
    (job,) = body["args"]
    assert body["kwargs"] == {}
    assert job["v"] == 1
    assert job["request_id"] == sent["task_id"]
    assert (job["job"], job["payload"]) == ("ingest", {"source": "s3://bucket/x.csv", "rows": 5})
    assert job["deadline_at"] > time.time()
    assert body["labels"] == {
        "lane": "bulk",
        "job": "ingest",
        "sender": "tester",
        "queue_name": "jobs-ingest",
    }
