"""Infrastructure helpers: Valkey inspection, SQS queues, fakellm admin and docker chaos.

Scenarios use these for isolation (reset prefix, purge queues, reload the fakellm scenario)
and for the end-of-run checks (DLQ empty, nothing in flight, no leaked leases or claims).
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import aiohttp
import yaml  # type: ignore[import-untyped,unused-ignore]  # no types-PyYAML in the venv
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from redis.asyncio import Redis

from tools.scenarios import api
from tools.scenarios.env import COMPOSE, REGION, SCENARIO_YAML_DIR, Settings

# ------------------------------------------------------------------ Valkey


def redis_client(settings: Settings) -> Redis:
    """A fresh harness Valkey client (short timeouts so chaos checks fail fast)."""
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        socket_timeout=5,
        socket_connect_timeout=3,
    )


async def scan_keys(r: Redis, prefix: str) -> list[str]:
    """Every key under `prefix:` as text."""
    return [
        k.decode() if isinstance(k, bytes) else str(k)
        async for k in r.scan_iter(match=f"{prefix}:*", count=2000)
    ]


async def flush_prefix(settings: Settings) -> int:
    """Delete every key under the harness prefix; returns how many were deleted."""
    r = redis_client(settings)
    try:
        found = await scan_keys(r, settings.prefix)
        for i in range(0, len(found), 1000):
            await r.delete(*found[i : i + 1000])
        return len(found)
    finally:
        await r.aclose()


async def commandstats(settings: Settings) -> dict[str, int]:
    """`INFO commandstats` as command -> calls (server-wide, so diff two snapshots)."""
    r = redis_client(settings)
    try:
        info = await r.info("commandstats")
    finally:
        await r.aclose()
    out: dict[str, int] = {}
    for name, value in info.items():
        if isinstance(value, dict) and "calls" in value:
            out[name.removeprefix("cmdstat_")] = int(value["calls"])
    return out


def ops_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    """Per-command call deltas, largest first (commands with no calls dropped)."""
    delta = {k: after.get(k, 0) - before.get(k, 0) for k in after}
    return dict(sorted(((k, v) for k, v in delta.items() if v > 0), key=lambda kv: -kv[1]))


async def leak_report(settings: Settings) -> dict[str, Any]:
    """Live limiter leases and claims still `running` under the prefix (both key schemas)."""
    r = redis_client(settings)
    now = time.time()
    try:
        found = await scan_keys(r, settings.prefix)
        live_leases = 0
        for key in (k for k in found if api.is_lease_key(k)):
            live_leases += int(await r.zcount(key, f"({now}", "+inf"))
        running: list[str] = []
        claim_keys = [(k, rid) for k in found if (rid := api.claim_id(k)) is not None]
        for i in range(0, len(claim_keys), 500):
            chunk = claim_keys[i : i + 500]
            async with r.pipeline(transaction=False) as pipe:
                for key, _ in chunk:
                    pipe.hget(key, "state")
                states = await pipe.execute()
            running += [rid for (_, rid), s in zip(chunk, states, strict=True) if s == b"running"]
        results = sum(1 for k in found if api.result_id(k) is not None)
        return {
            "keys": len(found),
            "live_leases": live_leases,
            "running_claims": len(running),
            "running_ids": running[:20],
            "results_unread": results,
        }
    finally:
        await r.aclose()


# ------------------------------------------------------------------ SQS


def _sqs(settings: Settings) -> Any:
    return get_session().create_client("sqs", region_name=REGION, endpoint_url=settings.sqs_url)


async def ensure_queues(
    settings: Settings,
    names: Iterable[str],
    *,
    visibility_secs: int = 60,
    max_receive: int = 10,
    wait_secs: int = 0,
) -> None:
    """Create each queue (and `<name>-dlq` with a redrive policy) if missing; set attributes."""
    async with _sqs(settings) as sqs:
        for name in names:
            dlq = await sqs.create_queue(QueueName=f"{name}-dlq")
            arn = (
                await sqs.get_queue_attributes(
                    QueueUrl=dlq["QueueUrl"], AttributeNames=["QueueArn"]
                )
            )["Attributes"]["QueueArn"]
            attrs = {
                "VisibilityTimeout": str(visibility_secs),
                "ReceiveMessageWaitTimeSeconds": str(wait_secs),
                "RedrivePolicy": (
                    f'{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"{max_receive}"}}'
                ),
            }
            try:
                url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
                await sqs.set_queue_attributes(QueueUrl=url, Attributes=attrs)
            except ClientError:
                await sqs.create_queue(QueueName=name, Attributes=attrs)


async def purge(settings: Settings, names: Iterable[str]) -> None:
    """Purge each queue and its DLQ (missing queues are ignored)."""
    async with _sqs(settings) as sqs:
        for name in names:
            for queue in (name, f"{name}-dlq"):
                try:
                    url = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
                    await sqs.purge_queue(QueueUrl=url)
                except ClientError:
                    continue


async def queue_counts(settings: Settings, names: Iterable[str]) -> dict[str, dict[str, int]]:
    """Visible / in-flight / delayed counts per queue and its DLQ."""
    out: dict[str, dict[str, int]] = {}
    async with _sqs(settings) as sqs:
        for name in names:
            for queue in (name, f"{name}-dlq"):
                try:
                    url = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
                except ClientError:
                    continue
                attrs = (
                    await sqs.get_queue_attributes(
                        QueueUrl=url,
                        AttributeNames=[
                            "ApproximateNumberOfMessages",
                            "ApproximateNumberOfMessagesNotVisible",
                            "ApproximateNumberOfMessagesDelayed",
                        ],
                    )
                )["Attributes"]
                out[queue] = {
                    "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
                    "inflight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
                    "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
                }
    return out


async def settle_queues(
    settings: Settings, names: Sequence[str], *, timeout_secs: float = 15.0
) -> dict[str, dict[str, int]]:
    """Wait until no main queue has visible/in-flight messages (acks are batched); last counts."""
    deadline = time.monotonic() + timeout_secs
    while True:
        counts = await queue_counts(settings, names)
        busy = sum(c["visible"] + c["inflight"] for q, c in counts.items() if q in names)
        if busy == 0 or time.monotonic() > deadline:
            return counts
        await asyncio.sleep(0.5)


def dlq_total(counts: dict[str, dict[str, int]]) -> int:
    """Messages sitting in any DLQ."""
    return sum(sum(c.values()) for q, c in counts.items() if q.endswith("-dlq"))


async def send_raw(settings: Settings, queue: str, body: str, **extra: Any) -> str:
    """Send a raw SQS message (malformed-input scenarios); returns the message id."""
    async with _sqs(settings) as sqs:
        url = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
        reply = await sqs.send_message(QueueUrl=url, MessageBody=body, **extra)
        return str(reply["MessageId"])


async def peek(settings: Settings, queue: str, max_messages: int = 10) -> list[dict[str, Any]]:
    """Receive up to N messages with visibility 0 (does not consume them)."""
    async with _sqs(settings) as sqs:
        url = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
        reply = await sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=max_messages,
            VisibilityTimeout=0,
            WaitTimeSeconds=1,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        return list(reply.get("Messages", []))


async def redrive(settings: Settings, queue: str) -> int:
    """Move every DLQ message of `queue` back (receive + send + delete); returns how many."""
    moved = 0
    async with _sqs(settings) as sqs:
        dlq = (await sqs.get_queue_url(QueueName=f"{queue}-dlq"))["QueueUrl"]
        src = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
        while True:
            reply = await sqs.receive_message(
                QueueUrl=dlq,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
                MessageAttributeNames=["All"],
            )
            messages = reply.get("Messages", [])
            if not messages:
                return moved
            for m in messages:
                await sqs.send_message(
                    QueueUrl=src,
                    MessageBody=m["Body"],
                    MessageAttributes=m.get("MessageAttributes") or {},
                )
                await sqs.delete_message(QueueUrl=dlq, ReceiptHandle=m["ReceiptHandle"])
                moved += 1


# ------------------------------------------------------------------ fakellm admin


def load_yaml_scenario(name: str) -> dict[str, Any]:
    """A fakellm scenario file from tools/fakellm/scenarios/<name>.yaml."""
    data = yaml.safe_load(Path(SCENARIO_YAML_DIR / f"{name}.yaml").read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


class FakeLLM:
    """Admin client for fakellm (and the probe's `/scn/*` routes when present)."""

    def __init__(self, url: str) -> None:
        """Talk to the fakellm at `url`."""
        self.url = url.rstrip("/")

    async def _req(self, method: str, path: str, body: Any = None) -> Any:
        timeout = aiohttp.ClientTimeout(total=60)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.request(method, f"{self.url}{path}", json=body) as resp,
        ):
            resp.raise_for_status()
            return await resp.json()

    async def health(self) -> bool:
        """True if fakellm answers."""
        try:
            await self._req("GET", "/health")
        except (aiohttp.ClientError, OSError, TimeoutError):
            return False
        return True

    async def load(self, scenario: str | dict[str, Any]) -> None:
        """Replace the scenario (name of a YAML file or a mapping); clears counters and log."""
        body = load_yaml_scenario(scenario) if isinstance(scenario, str) else scenario
        await self._req("PUT", "/admin/scenario", body)
        await self._req("POST", "/scn/reset")

    async def patch(self, key: str, patch: dict[str, Any]) -> None:
        """Change one behaviour key mid-run (None values drop overrides)."""
        from urllib.parse import quote

        await self._req("PATCH", f"/admin/models/{quote(key, safe='')}", patch)

    async def stats(self) -> dict[str, dict[str, int]]:
        """Per-key counters from `/admin/stats`."""
        data = await self._req("GET", "/admin/stats")
        return dict(data.get("models", {}))

    async def calls(self, prefix: str = "") -> dict[str, list[list[Any]]]:
        """Probe call log: rid -> [[t_start, t_end, status, model], ...]."""
        from urllib.parse import quote

        data = await self._req("GET", f"/scn/calls?prefix={quote(prefix)}")
        return dict(data["calls"])

    async def script(
        self, rid_prefix: str, actions: list[str], *, model: str = "", repeat: bool = False
    ) -> None:
        """Scripted faults for calls whose rid starts with `rid_prefix`."""
        body = {"rid_prefix": rid_prefix, "actions": actions, "model": model, "repeat": repeat}
        await self._req("POST", "/scn/script", body)


def stat_sum(
    stats: dict[str, dict[str, int]], field: str, keys: Iterable[str] | None = None
) -> int:
    """Sum one counter over all (or the given) fakellm keys."""
    wanted = set(keys) if keys is not None else None
    return sum(v.get(field, 0) for k, v in stats.items() if wanted is None or k in wanted)


def ok_calls(log: dict[str, list[list[Any]]]) -> dict[str, int]:
    """Rid -> number of provider calls answered 200."""
    return {rid: sum(1 for c in calls if c[2] == 200) for rid, calls in log.items()}


# ------------------------------------------------------------------ docker chaos


def compose(
    *args: str, check: bool = True, timeout: float = 120
) -> subprocess.CompletedProcess[str]:
    """Run `docker compose -f docker/compose.yaml <args>`."""
    return subprocess.run(
        [*COMPOSE, *args], capture_output=True, text=True, check=check, timeout=timeout
    )


async def stop_service(service: str) -> None:
    """`docker compose stop -t 0 <service>` (chaos)."""
    await asyncio.to_thread(compose, "--profile", "scn", "stop", "-t", "0", service)


async def start_service(service: str) -> None:
    """`docker compose up -d --wait <service>`."""
    await asyncio.to_thread(compose, "--profile", "scn", "up", "-d", "--wait", service)


async def pause_service(service: str) -> None:
    """`docker compose pause <service>` (hang, connections stay open)."""
    await asyncio.to_thread(compose, "--profile", "scn", "pause", service)


async def unpause_service(service: str) -> None:
    """`docker compose unpause <service>`."""
    await asyncio.to_thread(compose, "--profile", "scn", "unpause", service)
