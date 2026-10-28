"""Taskiq result backend on S3: JSON results at `s3://bucket/<prefix>/<task_id>.json` (P10).

For results that are large or must outlive Redis. S3 has no per-object TTL, so expiry is an
S3 lifecycle rule on `prefix` (e.g. expire after 1 day), set up outside this library.
Adapted from taskiq-sqs `S3ResultBackend` (MIT, see NOTICE). Results are JSON, never pickle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError
from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.exceptions import ResultGetError

from taskiq_llm_queue.broker.s3_offload import S3Connection, is_missing, object_key

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from aiobotocore.session import AioSession
    from types_aiobotocore_s3 import S3Client


class S3ResultBackend(AsyncResultBackend[Any]):
    """Stores each `TaskiqResult` as one JSON object; `get_result` may delete it after reading.

    Several brokers may share one instance: `startup`/`shutdown` are refcounted and an injected
    `client` is never closed. Expire old results with an S3 lifecycle rule on `prefix`; with
    `delete_on_read=False` nothing here ever deletes them.
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "taskiq-results",
        delete_on_read: bool = False,
        declare: bool = False,
        client: S3Client | None = None,
        session: AioSession | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        client_config: AioConfig | None = None,
    ) -> None:
        """Keep settings only; the S3 client is created in `startup` (no I/O here)."""
        self.prefix = prefix
        self.delete_on_read = delete_on_read
        self.s3 = S3Connection(
            bucket,
            client=client,
            session=session,
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            client_config=client_config,
            declare=declare,
        )

    async def startup(self) -> None:
        """Create the S3 client and check (or declare) the bucket."""
        await self.s3.startup()

    async def shutdown(self) -> None:
        """Close the owned S3 client once its last user shuts down."""
        await self.s3.shutdown()

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        """Write the result as JSON (overwrites an earlier result of the same task)."""
        await self.s3.client.put_object(
            Bucket=self.s3.bucket,
            Key=object_key(self.prefix, task_id),
            Body=result.model_dump_json().encode("utf-8"),
            ContentType="application/json",
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """True when a result object exists for `task_id` (one HEAD request)."""
        try:
            await self.s3.client.head_object(
                Bucket=self.s3.bucket, Key=object_key(self.prefix, task_id)
            )
        except ClientError as exc:
            if is_missing(exc):
                return False
            raise
        return True

    async def get_result(self, task_id: str, with_logs: bool = False) -> TaskiqResult[Any]:
        """Read one result (then delete it when `delete_on_read`); `ResultGetError` if absent.

        Read-then-delete is two requests, so two concurrent readers may both get the result.
        """
        key = object_key(self.prefix, task_id)
        raw = await self.s3.get_bytes(key)
        if raw is None:
            raise ResultGetError
        result = TaskiqResult[Any].model_validate_json(raw)
        if self.delete_on_read:
            await self.s3.client.delete_object(Bucket=self.s3.bucket, Key=key)
        return result
