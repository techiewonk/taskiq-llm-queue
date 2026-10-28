"""Acker and heartbeat against an in-memory fake client: partial failures, lost handles."""

from __future__ import annotations

import asyncio
from typing import Any

from botocore.exceptions import ClientError

from taskiq_llm_queue.broker import BrokerHooks, Delivery, Heartbeat, SQSQueue, VisibilityResult
from taskiq_llm_queue.broker.acker import Acker
from taskiq_llm_queue.broker.heartbeat import HeartbeatManager


def _err(code: str, status: int = 400) -> ClientError:
    response: Any = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return ClientError(response, "op")


class FakeSQS:
    """Records calls; handles in `invalid` fail as ReceiptHandleIsInvalid, `flaky` as 500."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.invalid: set[str] = set()
        self.flaky: set[str] = set()
        self.throttle_next = 0

    def _failed(self, entries: list[dict[str, Any]]) -> dict[str, Any]:
        failed = []
        for e in entries:
            if e["ReceiptHandle"] in self.invalid:
                failed.append(
                    {"Id": e["Id"], "Code": "ReceiptHandleIsInvalid", "SenderFault": True}
                )
            elif e["ReceiptHandle"] in self.flaky:
                failed.append({"Id": e["Id"], "Code": "InternalError", "SenderFault": False})
        ids = {f["Id"] for f in failed}
        return {
            "Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in ids],
            "Failed": failed,
        }

    async def delete_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        if self.throttle_next:
            self.throttle_next -= 1
            raise _err("RequestThrottled")
        self.calls.append(("delete_batch", [e["ReceiptHandle"] for e in Entries]))
        return self._failed(Entries)

    async def delete_message(self, QueueUrl: str, ReceiptHandle: str) -> Any:
        self.calls.append(("delete", ReceiptHandle))
        self.flaky.discard(ReceiptHandle)
        return {}

    async def change_message_visibility_batch(
        self,
        QueueUrl: str,
        Entries: list[dict[str, Any]],
    ) -> Any:
        self.calls.append(
            ("visibility_batch", [(e["ReceiptHandle"], e["VisibilityTimeout"]) for e in Entries])
        )
        return self._failed(Entries)

    async def change_message_visibility(
        self,
        QueueUrl: str,
        ReceiptHandle: str,
        VisibilityTimeout: int,
    ) -> Any:
        self.calls.append(("visibility", ReceiptHandle))
        if ReceiptHandle in self.invalid:
            raise _err("ReceiptHandleIsInvalid")
        return {}


class CountingHooks(BrokerHooks):
    def __init__(self) -> None:
        self.acks = 0
        self.extended = 0
        self.lost = 0
        self.errors: list[str] = []

    def on_ack(self, queue: str, count: int) -> None:
        self.acks += count

    def on_heartbeat(self, queue: str, extended: int, lost: int) -> None:
        self.extended += extended
        self.lost += lost

    def on_error(self, queue: str, operation: str, family: str) -> None:
        self.errors.append(family)


def _acker(fake: FakeSQS, hooks: BrokerHooks) -> Acker:
    client: Any = fake
    return Acker(
        client,
        hooks=hooks,
        ack_flush_secs=0.02,
        visibility_flush_secs=0.02,
        backoff_base_secs=0.001,
    )


async def test_delete_batch_tolerates_invalid_and_retries_failed_individually() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    fake.invalid.add("h1")
    fake.flaky.add("h2")
    acker = _acker(fake, hooks)
    results = await asyncio.gather(*(acker.delete("q", "url", h) for h in ("h0", "h1", "h2")))
    assert results == [True, True, True]
    assert fake.calls[0] == ("delete_batch", ["h0", "h1", "h2"])
    assert ("delete", "h2") in fake.calls
    assert ("delete", "h1") not in fake.calls
    assert hooks.acks == 3
    await acker.close()


async def test_delete_batch_retries_throttle_with_backoff() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    fake.throttle_next = 2
    acker = _acker(fake, hooks)
    assert await acker.delete("q", "url", "h") is True
    assert hooks.errors == ["throttled", "throttled"]
    await acker.close()


async def test_visibility_batch_reports_lost_handles() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    fake.invalid.add("gone")
    acker = _acker(fake, hooks)
    ok, lost = await asyncio.gather(
        acker.change_visibility("q", "url", "live", 30),
        acker.change_visibility("q", "url", "gone", 30),
    )
    assert (ok, lost) == (VisibilityResult.OK, VisibilityResult.LOST)
    await acker.close()


def _delivery(handle: str, received_at: float, visibility: int = 3) -> Delivery:
    return Delivery(
        queue=SQSQueue("q"),
        queue_url="url",
        message_id=handle,
        receipt_handle=handle,
        body="{}",
        receive_count=1,
        received_at=received_at,
        visibility_secs=visibility,
    )


async def test_heartbeat_beats_at_interval_and_respects_max_extensions() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    acker = _acker(fake, hooks)
    hb = HeartbeatManager(acker, Heartbeat(max_extensions=2), hooks=hooks)
    d = _delivery("h", received_at=100.0, visibility=3)
    hb.track(d)
    assert hb.beat(100.5) == 0  # interval = 1 s (visibility / 3)
    assert hb.beat(101.0) == 1
    assert hb.beat(101.5) == 0
    assert hb.beat(102.0) == 1
    assert hb.beat(103.0) == 0  # max_extensions reached -> untracked
    assert hb.tracked == 0
    await acker.flush()
    assert hooks.extended == 2
    await acker.close()


async def test_heartbeat_lost_handle_sets_event_and_calls_back() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    fake.invalid.add("h")
    acker = _acker(fake, hooks)
    lost: list[Delivery] = []
    hb = HeartbeatManager(acker, Heartbeat(), hooks=hooks, on_lost=lost.append)
    d = _delivery("h", received_at=0.0)
    hb.track(d)
    hb.beat(5.0)
    await acker.flush()
    assert d.lost.is_set()
    assert lost == [d]
    assert hooks.lost == 1
    assert hb.tracked == 0
    await acker.close()


async def test_heartbeat_skips_done_deliveries() -> None:
    fake, hooks = FakeSQS(), CountingHooks()
    acker = _acker(fake, hooks)
    hb = HeartbeatManager(acker, Heartbeat(), hooks=hooks)
    d = _delivery("h", received_at=0.0)
    hb.track(d)
    d.done = True
    assert hb.beat(10.0) == 0
    assert hb.tracked == 0
    await acker.close()
