"""Broker scenarios: S3 offload parity (S24) and SQS call counts through a counting proxy (S33).

S24 runs an in-process moto S3 server (no extra container); S33 puts an aiohttp proxy in front
of ElasticMQ that counts SQS actions, so batching and long polling are measured on the wire.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import socket
import time
from collections import Counter
from typing import Any

import aiohttp
from aiohttp import web

from tools.scenarios import api, stack
from tools.scenarios.harness import Ctx, ScenarioSkipped, scenario

_HOP = {"host", "content-length", "transfer-encoding", "connection", "content-encoding"}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class CountingProxy:
    """HTTP proxy to the harness SQS that counts SQS actions on the wire.

    Reads the JSON `X-Amz-Target` or the query `Action=`; forwards bodies as-is (ElasticMQ
    does not check signatures).
    """

    def __init__(self, upstream: str) -> None:
        """Proxy to `upstream` on a free local port (started by `start`)."""
        self.upstream = upstream.rstrip("/")
        self.counts: Counter[str] = Counter()
        self.port = _free_port()
        self._runner: web.AppRunner | None = None
        self._session: aiohttp.ClientSession | None = None

    @property
    def url(self) -> str:
        """Endpoint URL to give the queue config."""
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        body = await request.read()
        target = request.headers.get("X-Amz-Target", "")
        action = target.rpartition(".")[2] if target else ""
        if not action:
            form = dict(request.query)
            if body and b"Action=" in body:
                form.update(dict(p.split("=", 1) for p in body.decode().split("&") if "=" in p))
            action = form.get("Action", "?")
        self.counts[action] += 1
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP}
        assert self._session is not None
        async with self._session.request(
            request.method, f"{self.upstream}{request.path_qs}", data=body, headers=headers
        ) as upstream:
            data = await upstream.read()
            out = {k: v for k, v in upstream.headers.items() if k.lower() not in _HOP}
            return web.Response(status=upstream.status, body=data, headers=out)

    async def start(self) -> None:
        """Listen on 127.0.0.1:`port`."""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
        app = web.Application(client_max_size=4 * 1024 * 1024)
        app.router.add_route("*", "/{tail:.*}", self._handle)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        await web.TCPSite(self._runner, "127.0.0.1", self.port).start()

    async def stop(self) -> None:
        """Stop listening and close the upstream session."""
        if self._runner is not None:
            await self._runner.cleanup()
        if self._session is not None:
            await self._session.close()

    def take(self) -> Counter[str]:
        """Counts since the last `take()`, then reset."""
        counts, self.counts = self.counts, Counter()
        return counts


@scenario("S33", "Broker call counts")
async def s33(ctx: Ctx) -> None:
    """On the wire: bg sends and acks are batched, rt sends are single, idle workers long-poll.

    The idle bound is one receive per queue per `sqs_wait_time_secs` (no busy loop).
    """
    if ctx.settings.queue_backend == "valkey":
        raise ScenarioSkipped("SQS wire counts; S33v measures the Valkey backend")
    proxy = CountingProxy(ctx.settings.sqs_url)
    await proxy.start()
    ctx.on_cleanup(proxy.stop)
    cfg = ctx.config(sqs_endpoint_url=proxy.url)
    await ctx.ensure_queues(cfg)
    await ctx.workers(1, cfg)
    q = await ctx.queue(cfg)
    await asyncio.sleep(1.0)
    proxy.take()

    n_bg = 30
    replies = await asyncio.gather(
        *(
            api.converse(
                q,
                "gpt20b",
                api.converse_request(ctx.text(ctx.rid(i))),
                lane="bg",
                request_id=ctx.rid(i),
            )
            for i in range(n_bg)
        )
    )
    ctx.expect(all(r.ok for r in replies), f"bg calls failed: {[r.error for r in replies][:3]}")
    await asyncio.sleep(2.0)  # let batched acks flush
    bg = proxy.take()
    bg_sends = bg["SendMessage"] + bg["SendMessageBatch"]
    deletes = bg["DeleteMessage"] + bg["DeleteMessageBatch"]
    ctx.metric("bg_counts", {k: bg[k] for k in sorted(bg)})
    ctx.expect(bg["SendMessageBatch"] >= math.ceil(n_bg / 10), f"bg sends not batched: {bg}")
    ctx.expect(bg_sends <= n_bg // 2, f"{bg_sends} send calls for {n_bg} bg messages: {bg}")
    ctx.expect(0 < deletes < n_bg, f"acks not batched ({deletes} delete calls): {bg}")

    for i in range(5):
        r = await api.converse(
            q,
            "gpt20b",
            api.converse_request(ctx.text(ctx.rid(f"rt{i}"))),
            lane="rt",
            request_id=ctx.rid(f"rt{i}"),
        )
        ctx.expect(r.ok, f"rt call failed: {r.error!r}")
    rt = proxy.take()
    ctx.metric("rt_counts", {k: rt[k] for k in sorted(rt)})
    ctx.expect(rt["SendMessage"] == 5 and rt["SendMessageBatch"] == 0, f"rt sends: {rt}")

    await asyncio.sleep(3.0)  # drain in-flight acks, then measure an idle window
    proxy.take()
    idle_secs = 8.0
    t0 = time.monotonic()
    await asyncio.sleep(idle_secs)
    idle = proxy.take()
    elapsed = time.monotonic() - t0
    queues = len(api.all_queue_names(cfg))
    wait = float(cfg.sqs_wait_time_secs)
    bound = queues * (math.ceil(elapsed / wait) + 1)  # one long poll per queue per wait time
    ctx.metric("idle_receives", idle["ReceiveMessage"])
    ctx.metric("idle_bound", bound)
    ctx.metric("idle_other", {k: v for k, v in idle.items() if k != "ReceiveMessage"})
    ctx.expect(idle["ReceiveMessage"] <= bound, f"busy polling: {idle} over {elapsed:.1f}s")
    await ctx.final_checks(cfg)


@scenario("S24", "Broker parity: S3 offload of large job payloads")
async def s24(ctx: Ctx) -> None:
    """Large job payloads travel through S3 and are deleted after the ack.

    A small payload stays inline; a large LLM prompt still goes by Valkey reference, not S3.
    """
    import boto3  # type: ignore[import-untyped]
    from moto.server import ThreadedMotoServer

    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    ctx.on_cleanup(lambda: asyncio.to_thread(server.stop))
    endpoint, bucket = f"http://127.0.0.1:{port}", f"llmq-scn-{ctx.run}"
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="ap-south-1")
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})

    def objects() -> int:
        return int(s3.list_objects_v2(Bucket=bucket).get("KeyCount", 0))

    cfg = ctx.config(s3_offload_bucket=bucket, s3_endpoint_url=endpoint)
    await ctx.ensure_queues(cfg)
    seen: dict[str, tuple[int, int]] = {}  # job tag -> (rows received, S3 objects meanwhile)
    done = {tag: asyncio.Event() for tag in ("big", "small")}

    async def ingest(payload: dict[str, Any]) -> None:
        seen[payload["tag"]] = (len(payload["rows"]), await asyncio.to_thread(objects))
        done[payload["tag"]].set()

    q = api.new_queue(cfg, f"{ctx.sid.lower()}-both")
    q.register_job("ingest", ingest)
    await q.start(mode="both")
    ctx._queues.append(q)

    row = "x" * 1000
    big = {"tag": "big", "rows": [f"{i}:{row}" for i in range(300)]}  # ~300 KB
    await api.send_job(q, "ingest", big, job_id=ctx.rid("big"))
    await asyncio.wait_for(done["big"].wait(), 60)
    for _ in range(40):  # the big job's object goes once SQS confirmed its ack
        if await asyncio.to_thread(objects) == 0:
            break
        await asyncio.sleep(0.25)
    await api.send_job(q, "ingest", {"tag": "small", "rows": ["a", "b"]}, job_id=ctx.rid("s"))
    await asyncio.wait_for(done["small"].wait(), 60)

    prompt = ctx.text(ctx.rid("llm"), "y" * 300_000)
    reply = await api.converse(
        q, "gpt20b", api.converse_request(prompt), lane="bg", request_id=ctx.rid("llm")
    )
    ctx.expect(reply.ok, f"300 KB prompt failed: {reply.error!r}")
    left = 0
    for _ in range(20):  # the offloaded object goes once SQS confirmed the ack
        left = await asyncio.to_thread(objects)
        if left == 0:
            break
        await asyncio.sleep(0.5)
    ctx.metric("big_job", seen.get("big"))
    ctx.metric("small_job", seen.get("small"))
    ctx.metric("objects_after", left)
    ctx.expect(seen.get("big") == (300, 1), f"big payload not via S3: {seen.get('big')}")
    ctx.expect(seen.get("small") == (2, 0), f"small payload offloaded: {seen.get('small')}")
    ctx.expect(left == 0, f"{left} offloaded objects left after the ack")
    with contextlib.suppress(Exception):
        await q.stop(drain_secs=2)
    await ctx.final_checks(cfg)


@scenario("S33v", "Valkey queue backend: no busy polling, wake-up on send")
async def s33v(ctx: Ctx) -> None:
    """Idle workers do not busy-poll, and a send wakes a waiting poller at once.

    Idle: measured ~2 XREADGROUP per queue per second (jittered 0.5-1 s re-checks plus the
    check that opens each long poll); gated at 3/s, far from a busy loop (hundreds/s).
    Wake: rt queue waits stay far below the 1 s re-check.
    """
    if ctx.settings.queue_backend != "valkey":
        raise ScenarioSkipped("Valkey backend only (SCN_QUEUE_BACKEND=valkey); S33 covers SQS")
    from taskiq_llm_queue.broker.valkey_queue import RECHECK_SECS

    await ctx.workers(1)
    q = await ctx.queue()
    await asyncio.sleep(3.0)  # pollers settle into idle long polls
    before = await stack.commandstats(ctx.settings)
    t0 = time.monotonic()
    await asyncio.sleep(8.0)
    after = await stack.commandstats(ctx.settings)
    elapsed = time.monotonic() - t0
    reads = after.get("xreadgroup", 0) - before.get("xreadgroup", 0)
    queues = len(api.all_queue_names(ctx.base))
    bound = math.ceil(queues * elapsed * 3 / RECHECK_SECS)
    ctx.metric("idle_xreadgroup", reads)
    ctx.metric("idle_bound", bound)
    ctx.expect(reads <= bound, f"busy polling: {reads} XREADGROUP in {elapsed:.1f}s > {bound}")

    waits = []
    for i in range(5):
        await asyncio.sleep(1.5)  # every poller idle again
        r = await api.converse(
            q,
            "gpt20b",
            api.converse_request(ctx.text(ctx.rid(i))),
            lane="rt",
            request_id=ctx.rid(i),
        )
        ctx.expect(r.ok, f"rt call failed: {r.error!r}")
        waits.append(r.meta.queue_wait_ms)
    ctx.metric("rt_queue_wait_ms", waits)
    ctx.expect(max(waits) < 400, f"send did not wake the poller: queue waits {waits} ms")
    await ctx.final_checks()
