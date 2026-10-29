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


def test_routing_and_fallback_start_from_app_defaults_and_merge(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With nothing stored, get/set start from LLMQ_ROUTING_DEFAULT / LLMQ_FALLBACK_CHAINS."""
    env = {
        **cli_env,
        "LLMQ_ROUTING_DEFAULT": json.dumps({"models": {"gpt120b": {"bg": 25}}}),
        "LLMQ_FALLBACK_CHAINS": json.dumps({"chains": {"gpt120b": ["gpt20b"]}}),
    }
    assert json.loads(run(env, capsys, "routing", "get"))["models"] == {"gpt120b": {"bg": 25}}
    run(env, capsys, "routing", "set", "--model", "gpt20b", "--lane", "rt", "--pct", "10")
    stored = json.loads(run(env, capsys, "routing", "get"))
    assert stored["models"] == {"gpt120b": {"bg": 25}, "gpt20b": {"rt": 10}}
    run(env, capsys, "routing", "reset")
    assert sync_valkey.get(KeySpace(prefix).routing) is None

    run(env, capsys, "fallback", "set", "--model", "gpt20b", "--chain", "gpt120b")
    chains = json.loads(run(env, capsys, "fallback", "get"))["chains"]
    assert set(chains) == {"gpt120b", "gpt20b"}  # the default's gpt120b chain was kept


def test_fallback_set_file_merges_unless_replace(
    cli_env: dict[str, str], capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    run(cli_env, capsys, "fallback", "set", "--model", "gpt120b", "--chain", "gpt20b")
    file = tmp_path / "fallback.json"
    file.write_text(json.dumps({"chains": {"gpt20b": ["gpt120b"]}}))
    run(cli_env, capsys, "fallback", "set", "--file", str(file))
    assert set(json.loads(run(cli_env, capsys, "fallback", "get"))["chains"]) == {
        "gpt120b",
        "gpt20b",
    }
    run(cli_env, capsys, "fallback", "set", "--file", str(file), "--replace")
    assert set(json.loads(run(cli_env, capsys, "fallback", "get"))["chains"]) == {"gpt20b"}


def test_limiter_and_breaker_reset(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`limiter reset` forgets the learned limit; `breaker reset` closes an open breaker."""
    keys = KeySpace(prefix)
    sync_valkey.hset(keys.limiter("gpt20b", "primary"), mapping={"limit": 3, "ramp": 0})
    sync_valkey.hset(keys.breaker("gpt20b", "primary"), mapping={"state": "open"})
    rows = json.loads(run(cli_env, capsys, "limiter", "reset", "--model", "gpt20b"))
    assert rows[0]["before"]["limit"] == 3
    assert not sync_valkey.exists(keys.limiter("gpt20b", "primary"))
    shown = json.loads(run(cli_env, capsys, "breaker", "show", "--model", "gpt20b@primary"))
    assert len(shown) == 1
    reset = json.loads(
        run(cli_env, capsys, "breaker", "reset", "--model", "gpt20b", "--target", "primary")
    )
    assert reset[0]["breaker"] == "closed"
    assert not sync_valkey.exists(keys.breaker("gpt20b", "primary"))
    assert [r["key"] for r in audit(sync_valkey, prefix)] == [
        keys.limiter("gpt20b", "primary"),
        keys.breaker("gpt20b", "primary"),
    ]


def test_queues_lists_counts(
    cli_env: dict[str, str], jobs_queue: str, capsys: pytest.CaptureFixture[str]
) -> None:
    env = {**cli_env, "LLMQ_JOB_QUEUES": json.dumps({"ingest": "jobs-ingest"})}
    rows = json.loads(run(env, capsys, "queues"))
    (row,) = [r for r in rows if r["queue"] == "jobs-ingest"]
    assert {"visible", "in_flight", "delayed"} <= set(row)


def test_settings_set_get_unset_reset_with_sources_and_audit(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`settings` edits the live layer: values resolve with their source, every change is
    audited, `unset` falls back to env / defaults, and `reset` deletes the key."""
    env = {**cli_env, "LLMQ_ATTEMPT_TIMEOUT_SECS": "100"}
    run(env, capsys, "settings", "set", "--provider", "openai", "attempt_timeout_secs=60")
    run(
        env,
        capsys,
        "settings",
        "set",
        "--model",
        "gpt20b",
        "max_concurrency=5",
        "timeout_secs.rt=45",
    )
    run(env, capsys, "settings", "set", "--provider", "openai", "shared_max_concurrency=8")
    got = json.loads(run(env, capsys, "settings", "get", "--model", "gpt20b"))["gpt20b"]
    assert got["max_concurrency"] == {"value": 5, "source": "live:model"}
    assert got["timeout_secs.rt"] == {"value": 45.0, "source": "live:model"}
    assert got["timeout_secs.bg"] == {"value": 300.0, "source": "default"}
    assert got["attempt_timeout_secs"] == {"value": 60.0, "source": "live:provider"}
    provider = json.loads(run(env, capsys, "settings", "get", "--provider", "openai"))["openai"]
    assert provider["shared_max_concurrency"] == 8
    assert provider["models"] == ["gpt120b", "gpt20b"]
    run(env, capsys, "settings", "unset", "--provider", "openai", "attempt_timeout_secs")
    got = json.loads(run(env, capsys, "settings", "get", "--model", "gpt20b"))["gpt20b"]
    assert got["attempt_timeout_secs"] == {"value": 100.0, "source": "env"}
    whole = json.loads(run(env, capsys, "settings", "get"))
    assert set(whole) == {"live", "providers", "models"}
    rows = audit(sync_valkey, prefix)
    assert len(rows) == 4
    assert {r["key"] for r in rows} == {KeySpace(prefix).model_settings}
    run(env, capsys, "settings", "reset")
    assert sync_valkey.get(KeySpace(prefix).model_settings) is None


@pytest.mark.parametrize(
    "argv",
    [
        ("settings", "set", "--model", "nope", "max_concurrency=5"),
        ("settings", "set", "--provider", "bedrock", "max_concurrency=5"),
        ("settings", "set", "--model", "gpt20b", "max_concurrency=0"),
        ("settings", "set", "--model", "gpt20b", "shared_max_concurrency=3"),
        ("settings", "set", "--model", "gpt20b", "timeout_secs.fast=3"),
    ],
)
def test_settings_rejects_bad_input_without_writing(
    cli_env: dict[str, str],
    prefix: str,
    sync_valkey: redis.Redis,
    capsys: pytest.CaptureFixture[str],
    argv: tuple[str, ...],
) -> None:
    """Unknown alias / provider, invalid value or name: exit 1 and nothing stored or audited."""
    assert main(["--actor", "tester", *argv], environ=cli_env) == 1
    assert "error:" in capsys.readouterr().err
    assert sync_valkey.get(KeySpace(prefix).model_settings) is None
    assert audit(sync_valkey, prefix) == []
