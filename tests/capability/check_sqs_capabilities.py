"""Check which SQS features a fake SQS endpoint supports (plan T0.1b).

Runs the SQS calls our broker depends on against any endpoint (ElasticMQ, moto server, real
SQS) using aiobotocore, and prints a capability table. The script creates its own ``cap-*``
queues, so it doesn't depend on the endpoint's preconfigured queues.

Usage:
    python check_sqs_capabilities.py --endpoint http://localhost:9324 --name elasticmq
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

RESULTS: list[tuple[str, str, str]] = []


def record(feature: str, ok: bool | None, detail: str = "") -> None:
    status = "yes" if ok else ("no" if ok is False else "n/a")
    RESULTS.append((feature, status, detail))


async def make_queue(sqs: Any, name: str, attributes: dict[str, str] | None = None) -> str:
    resp = await sqs.create_queue(QueueName=name, Attributes=attributes or {})
    return str(resp["QueueUrl"])


async def drain(sqs: Any, url: str) -> None:
    while True:
        resp = await sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        msgs = resp.get("Messages", [])
        if not msgs:
            return
        await sqs.delete_message_batch(
            QueueUrl=url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)
            ],
        )


async def check(endpoint: str) -> None:
    suffix = uuid.uuid4().hex[:6]
    session = get_session()
    async with session.create_client(
        "sqs",
        region_name="ap-south-1",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=AioConfig(retries={"max_attempts": 1}),
    ) as client:
        sqs: Any = client  # probe script: SDK stubs add noise, not safety
        # 1. JSON protocol: any call works at all with botocore >= 1.31
        try:
            std = await make_queue(sqs, f"cap-std-{suffix}", {"VisibilityTimeout": "30"})
            record("JSON protocol (CreateQueue/GetQueueUrl)", True)
        except Exception as exc:
            record("JSON protocol (CreateQueue/GetQueueUrl)", False, type(exc).__name__)
            return

        # 2. Send/receive with message attributes and ApproximateReceiveCount
        await sqs.send_message(
            QueueUrl=std,
            MessageBody="hello",
            MessageAttributes={"lane": {"DataType": "String", "StringValue": "rt"}},
        )
        resp = await sqs.receive_message(
            QueueUrl=std,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        msg = resp.get("Messages", [{}])[0]
        record(
            "Message attributes round trip",
            msg.get("MessageAttributes", {}).get("lane", {}).get("StringValue") == "rt",
        )
        record(
            "ApproximateReceiveCount attribute",
            "ApproximateReceiveCount" in msg.get("Attributes", {}),
            msg.get("Attributes", {}).get("ApproximateReceiveCount", ""),
        )

        # 3. ChangeMessageVisibilityBatch (heartbeat batching)
        try:
            r = await sqs.change_message_visibility_batch(
                QueueUrl=std,
                Entries=[
                    {"Id": "a", "ReceiptHandle": msg["ReceiptHandle"], "VisibilityTimeout": 45}
                ],
            )
            record("ChangeMessageVisibilityBatch", len(r.get("Successful", [])) == 1)
        except ClientError as exc:
            record("ChangeMessageVisibilityBatch", False, exc.response["Error"]["Code"])

        # 4. DeleteMessageBatch with one bad receipt handle -> partial failure reported
        try:
            r = await sqs.delete_message_batch(
                QueueUrl=std,
                Entries=[
                    {"Id": "ok", "ReceiptHandle": msg["ReceiptHandle"]},
                    {"Id": "bad", "ReceiptHandle": "not-a-real-handle"},
                ],
            )
            ok = [e["Id"] for e in r.get("Successful", [])]
            failed = [(e["Id"], e.get("Code", "")) for e in r.get("Failed", [])]
            record("DeleteMessageBatch", "ok" in ok, "")
            record(
                "DeleteMessageBatch partial failure reported in Failed[]",
                any(f[0] == "bad" for f in failed),
                json.dumps(failed),
            )
        except ClientError as exc:
            record("DeleteMessageBatch", False, exc.response["Error"]["Code"])

        # 5. Per-message delay on a standard queue
        await sqs.send_message(QueueUrl=std, MessageBody="delayed", DelaySeconds=2)
        early = await sqs.receive_message(QueueUrl=std, WaitTimeSeconds=0)
        await asyncio.sleep(2.5)
        late = await sqs.receive_message(QueueUrl=std, WaitTimeSeconds=1)
        record(
            "DelaySeconds (standard queue)",
            not early.get("Messages") and bool(late.get("Messages")),
        )
        await drain(sqs, std)

        # 6. Long poll returns early when a message arrives
        async def delayed_send() -> None:
            await asyncio.sleep(1.0)
            await sqs.send_message(QueueUrl=std, MessageBody="lp")

        t0 = time.monotonic()
        sender = asyncio.create_task(delayed_send())
        r = await sqs.receive_message(QueueUrl=std, WaitTimeSeconds=20)
        took = time.monotonic() - t0
        await sender
        record(
            "Long poll (WaitTimeSeconds=20) returns on arrival",
            bool(r.get("Messages")) and took < 5,
            f"{took:.1f}s",
        )
        await drain(sqs, std)

        # 7. Long poll on an empty queue actually waits (not 1 s slices server-side)
        t0 = time.monotonic()
        await sqs.receive_message(QueueUrl=std, WaitTimeSeconds=3)
        took = time.monotonic() - t0
        record("Empty long poll waits the full time", took >= 2.5, f"{took:.1f}s")

        # 8. Message size limits
        for size, label in [
            (262_144, "256 KiB"),
            (262_145, "256 KiB + 1"),
            (1_048_576, "1 MiB"),
            (1_048_577, "1 MiB + 1"),
        ]:
            try:
                await sqs.send_message(QueueUrl=std, MessageBody="x" * size)
                record(f"Message size {label}", True, "accepted")
            except ClientError as exc:
                record(f"Message size {label}", False, exc.response["Error"]["Code"])
        await drain(sqs, std)

        # 9. SendMessageBatch total size over 1 MiB rejected (DS15)
        try:
            await sqs.send_message_batch(
                QueueUrl=std,
                Entries=[{"Id": str(i), "MessageBody": "y" * 200_000} for i in range(6)],
            )
            record("SendMessageBatch > 1 MiB total rejected", False, "accepted 1.2 MB batch")
        except ClientError as exc:
            record("SendMessageBatch > 1 MiB total rejected", True, exc.response["Error"]["Code"])
        await drain(sqs, std)

        # 10. FIFO: group + dedup id; DelaySeconds per message rejected
        try:
            fifo = await make_queue(sqs, f"cap-fifo-{suffix}.fifo", {"FifoQueue": "true"})
            await sqs.send_message(
                QueueUrl=fifo, MessageBody="f1", MessageGroupId="g1", MessageDeduplicationId="d1"
            )
            await sqs.send_message(
                QueueUrl=fifo, MessageBody="f1", MessageGroupId="g1", MessageDeduplicationId="d1"
            )
            r = await sqs.receive_message(QueueUrl=fifo, MaxNumberOfMessages=10, WaitTimeSeconds=1)
            record(
                "FIFO queue + deduplication",
                len(r.get("Messages", [])) == 1,
                f"{len(r.get('Messages', []))} received",
            )
            try:
                await sqs.send_message(
                    QueueUrl=fifo,
                    MessageBody="f2",
                    MessageGroupId="g1",
                    MessageDeduplicationId="d2",
                    DelaySeconds=5,
                )
                record("FIFO rejects per-message DelaySeconds", False, "accepted")
            except ClientError as exc:
                record("FIFO rejects per-message DelaySeconds", True, exc.response["Error"]["Code"])
        except ClientError as exc:
            record("FIFO queue + deduplication", False, exc.response["Error"]["Code"])

        # 11. Fair queue: MessageGroupId on a standard queue
        try:
            await sqs.send_message(QueueUrl=std, MessageBody="fair", MessageGroupId="tenant-a")
            record("MessageGroupId on standard queue (fair queues)", True, "accepted")
        except ClientError as exc:
            record(
                "MessageGroupId on standard queue (fair queues)",
                False,
                exc.response["Error"]["Code"],
            )
        await drain(sqs, std)

        # 12. Redrive to DLQ after maxReceiveCount, then StartMessageMoveTask back
        dlq = await make_queue(sqs, f"cap-dlq-{suffix}")
        dlq_arn = (await sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"]))[
            "Attributes"
        ]["QueueArn"]
        src = await make_queue(
            sqs,
            f"cap-src-{suffix}",
            {
                "VisibilityTimeout": "1",
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}
                ),
            },
        )
        src_arn = (await sqs.get_queue_attributes(QueueUrl=src, AttributeNames=["QueueArn"]))[
            "Attributes"
        ]["QueueArn"]
        await sqs.send_message(QueueUrl=src, MessageBody="poison")
        for _ in range(4):
            await sqs.receive_message(QueueUrl=src, WaitTimeSeconds=1)
            await asyncio.sleep(1.2)
        r = await sqs.receive_message(QueueUrl=dlq, WaitTimeSeconds=2)
        moved = bool(r.get("Messages"))
        record("Redrive to DLQ after maxReceiveCount", moved)
        if moved:
            await sqs.change_message_visibility(
                QueueUrl=dlq, ReceiptHandle=r["Messages"][0]["ReceiptHandle"], VisibilityTimeout=0
            )
        try:
            await sqs.start_message_move_task(SourceArn=dlq_arn, DestinationArn=src_arn)
            await asyncio.sleep(2)
            back = await sqs.receive_message(QueueUrl=src, WaitTimeSeconds=2)
            record("StartMessageMoveTask (DLQ redrive)", bool(back.get("Messages")))
        except ClientError as exc:
            record("StartMessageMoveTask (DLQ redrive)", False, exc.response["Error"]["Code"])
        except Exception as exc:
            record("StartMessageMoveTask (DLQ redrive)", False, type(exc).__name__)

        # 13. Purge (test isolation)
        try:
            await sqs.purge_queue(QueueUrl=std)
            record("PurgeQueue", True)
        except ClientError as exc:
            record("PurgeQueue", False, exc.response["Error"]["Code"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()
    asyncio.run(check(args.endpoint))
    print(f"\n{args.name} @ {args.endpoint}")
    width = max(len(f) for f, _, _ in RESULTS)
    for feature, status, detail in RESULTS:
        print(f"  {feature:<{width}}  {status:<4} {detail}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"name": args.name, "results": RESULTS}, fh, indent=2)


if __name__ == "__main__":
    main()
