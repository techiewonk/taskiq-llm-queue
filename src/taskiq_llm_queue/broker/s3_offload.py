"""Large task payloads through S3: SQS carries a small reference, S3 carries the args (P9).

`S3OffloadMiddleware` uploads a task's args/kwargs as JSON when the serialized message is over
a threshold and restores them on the worker, so the broker's byte-aware send batching (DS15)
only ever sees small messages. Adapted from taskiq-sqs `S3OffloadMiddleware` (MIT, see NOTICE).
"""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

import orjson
from aiobotocore.config import AioConfig
from aiobotocore.session import AioSession, get_session
from botocore.exceptions import ClientError
from taskiq import TaskiqMiddleware

from taskiq_llm_queue.errors import LLMQueueError

if TYPE_CHECKING:
    from taskiq import TaskiqMessage, TaskiqResult
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)

OFFLOAD_LABEL = "s3_offload_key"
DEFAULT_THRESHOLD_BYTES = 200_000  # DS15: 200 KB inline limit on the serialized taskiq message
_MISSING_CODES = frozenset({"NoSuchKey", "404", "NotFound"})
_BUCKET_EXISTS_CODES = frozenset({"BucketAlreadyOwnedByYou", "BucketAlreadyExists"})


class S3OffloadError(LLMQueueError):
    """S3 offload failed: bucket missing, payload unreadable or not JSON-serializable."""


class OffloadedPayloadMissingError(S3OffloadError):
    """The S3 object a message refers to is gone (deleted, expired or never written).

    Raised from `pre_execute`; the message is then not acked and ends in the DLQ after
    `maxReceiveCount`, instead of running the task with empty arguments.
    """


def is_missing(exc: ClientError) -> bool:
    """True when a botocore error means "no such object/bucket" (spellings differ per call)."""
    return str(exc.response.get("Error", {}).get("Code", "")) in _MISSING_CODES


class S3Connection:
    """Owns one aiobotocore S3 client for a bucket, shared by `startup` users and refcounted.

    Used by `S3OffloadMiddleware` and `S3ResultBackend`. An injected `client` is never closed;
    an owned one is created in the first `startup` and closed when the last user shuts down.
    """

    def __init__(
        self,
        bucket: str,
        *,
        client: S3Client | None = None,
        session: AioSession | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        client_config: AioConfig | None = None,
        declare: bool = False,
    ) -> None:
        """Keep settings only (no I/O); `declare` creates the bucket in `startup` if absent."""
        if not bucket:
            raise ValueError("bucket must be a non-empty name")
        self.bucket = bucket
        self.declare = declare
        self._client = client
        self._owns_client = client is None
        self._session = session
        self._client_config = client_config
        self._client_kwargs: dict[str, Any] = {
            "endpoint_url": endpoint_url,
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
        }
        self._stack: contextlib.AsyncExitStack | None = None
        self._users = 0

    @property
    def client(self) -> S3Client:
        """The client in use; raises `S3OffloadError` before `startup`."""
        if self._client is None:
            raise S3OffloadError("S3 client is not started; call startup() first")
        return self._client

    async def startup(self) -> None:
        """Register one user; the first creates the client and checks (or creates) the bucket."""
        self._users += 1
        if self._users > 1:
            return
        try:
            if self._client is None:
                stack = contextlib.AsyncExitStack()
                session = self._session or get_session()
                try:
                    self._client = await stack.enter_async_context(
                        session.create_client(
                            "s3", config=self._client_config, **self._client_kwargs
                        )
                    )
                except BaseException:
                    await stack.aclose()
                    raise
                self._stack = stack
            await self._ensure_bucket()
        except BaseException:
            self._users -= 1
            await self.aclose()
            raise

    async def shutdown(self) -> None:
        """Drop one user; close the owned client once none is left. Safe to call repeatedly."""
        self._users = max(0, self._users - 1)
        if self._users == 0:
            await self.aclose()

    async def aclose(self) -> None:
        """Close the owned client now, whoever still uses it; idempotent."""
        if self._owns_client and self._stack is not None:
            stack, self._stack, self._client = self._stack, None, None
            await stack.aclose()

    async def _ensure_bucket(self) -> None:
        try:
            await self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError as exc:
            if not is_missing(exc):
                raise
            if not self.declare:
                raise S3OffloadError(f"S3 bucket {self.bucket!r} does not exist") from None
        region = self.client.meta.region_name
        kwargs: dict[str, Any] = {"Bucket": self.bucket}
        if region and region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        try:
            await self.client.create_bucket(**kwargs)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code", "")) not in _BUCKET_EXISTS_CODES:
                raise

    async def get_bytes(self, key: str) -> bytes | None:
        """Read one object, or None when it does not exist."""
        try:
            response = await self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if is_missing(exc):
                return None
            raise
        async with response["Body"] as stream:
            return await stream.read()


def object_key(prefix: str, task_id: str) -> str:
    """`<prefix>/<task_id>.json`, or `<task_id>.json` for an empty prefix."""
    if not task_id or "/" in task_id or task_id in {".", ".."}:
        raise ValueError(f"task_id {task_id!r} cannot be used as an S3 key segment")
    prefix = prefix.strip("/")
    return f"{prefix}/{task_id}.json" if prefix else f"{task_id}.json"


class S3OffloadMiddleware(TaskiqMiddleware):
    """Moves args/kwargs of oversized messages to `s3://bucket/<base_path>/<task_id>.json`.

    `pre_send` (client) offloads when the formatted message exceeds `threshold_bytes` and puts
    the object key in the `s3_offload_key` label; `pre_execute` (worker) restores them;
    `post_execute` deletes the object after a successful run when `delete_after_execute`.
    A failed run keeps the object so a redelivery can still read it; add an S3 lifecycle
    rule on the prefix to expire leftovers. Payloads are JSON only, never pickle.
    """

    def __init__(
        self,
        bucket: str,
        *,
        threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
        base_path: str = "taskiq-offload",
        delete_after_execute: bool = True,
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
        """Keep settings only; the S3 client is created in `startup` (no I/O here).

        `threshold_bytes` is compared with the UTF-8 size of the whole formatted message
        (broker formatter when attached, else JSON of args/kwargs).
        """
        super().__init__()
        if threshold_bytes < 1:
            raise ValueError("threshold_bytes must be >= 1")
        self.threshold_bytes = threshold_bytes
        self.base_path = base_path
        self.delete_after_execute = delete_after_execute
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
        """Create the S3 client and check (or declare) the bucket; runs in broker startup."""
        await self.s3.startup()

    async def shutdown(self) -> None:
        """Close the owned S3 client; runs in broker shutdown."""
        await self.s3.shutdown()

    def message_size(self, message: TaskiqMessage) -> int:
        """Bytes the message would take on the wire, as the threshold check sees it."""
        broker = getattr(self, "broker", None)
        if broker is not None:
            return len(broker.formatter.dumps(message).message)
        return len(encode_payload(message.args, message.kwargs))

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Upload args/kwargs and send a reference when the message is over the threshold."""
        message.labels.pop(OFFLOAD_LABEL, None)  # a re-kicked message must not reuse an old key
        if self.message_size(message) <= self.threshold_bytes:
            return message
        payload = encode_payload(message.args, message.kwargs)
        key = object_key(self.base_path, message.task_id)
        await self.s3.client.put_object(
            Bucket=self.s3.bucket, Key=key, Body=payload, ContentType="application/json"
        )
        logger.debug("offloaded task %s (%d bytes) to S3", message.task_id, len(payload))
        message.args = []
        message.kwargs = {}
        message.labels[OFFLOAD_LABEL] = key
        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """Restore offloaded args/kwargs; raises `OffloadedPayloadMissingError` if gone."""
        key = message.labels.get(OFFLOAD_LABEL)
        if not key:
            return message
        body = await self.s3.get_bytes(str(key))
        if body is None:
            raise OffloadedPayloadMissingError(
                f"offloaded payload of task {message.task_id} not found at "
                f"s3://{self.s3.bucket}/{key}"
            )
        message.args, message.kwargs = decode_payload(body)
        return message

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> None:
        """Delete the offloaded object after a successful run when `delete_after_execute`."""
        key = message.labels.get(OFFLOAD_LABEL)
        if not key or not self.delete_after_execute or result.is_err:
            return
        try:
            await self.s3.client.delete_object(Bucket=self.s3.bucket, Key=str(key))
        except ClientError as exc:  # the result is already computed; a lifecycle rule cleans up
            logger.warning("could not delete offloaded payload %s: %s", key, exc)


def encode_payload(args: list[Any], kwargs: dict[str, Any]) -> bytes:
    """JSON bytes of `{"args", "kwargs"}`; raises `S3OffloadError` for non-JSON values."""
    try:
        return orjson.dumps({"args": args, "kwargs": kwargs})
    except (TypeError, ValueError) as exc:
        raise S3OffloadError(f"task arguments are not JSON-serializable: {exc}") from exc


def decode_payload(body: bytes) -> tuple[list[Any], dict[str, Any]]:
    """Inverse of `encode_payload`; raises `S3OffloadError` for anything else."""
    try:
        data = orjson.loads(body)
    except ValueError as exc:
        raise S3OffloadError("offloaded payload is not valid JSON") from exc
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("args"), list)
        or not isinstance(data.get("kwargs"), dict)
    ):
        raise S3OffloadError("offloaded payload is not an {args, kwargs} object")
    return data["args"], data["kwargs"]
