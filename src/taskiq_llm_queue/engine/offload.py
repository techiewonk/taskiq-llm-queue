"""Large results in object storage (plan E137): the state store keeps only a reference.

A result above `result_offload_bytes` is written to an S3 bucket (encrypted, with a one-day
lifecycle rule set on the bucket by the owner) and the stored reply is a small marker. Readers
fetch it transparently. Used for bulk embedding batches, whose vectors are large.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from aiobotocore.session import AioSession

MARKER = b"llmq-offload:"


class ResultOffload(Protocol):
    """Somewhere to keep a large result body."""

    async def put(self, key: str, data: bytes) -> str:
        """Store `data`; return a reference."""
        ...

    async def get(self, ref: str) -> bytes | None:
        """The data of a reference (None when it expired)."""
        ...


class S3Offload:
    """`ResultOffload` on an S3 bucket (server-side encryption; expiry by the bucket's rule)."""

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "llmq-results/",
        session: AioSession | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Keep settings; the client is created on first use."""
        self.bucket = bucket
        self.prefix = prefix
        self._session = session
        self._kwargs = dict(client_kwargs or {})
        self._client: Any = None
        self._context: Any = None

    async def _s3(self) -> Any:
        if self._client is None:
            from aiobotocore.session import get_session

            session = self._session or get_session()
            self._context = session.create_client("s3", **self._kwargs)
            self._client = await self._context.__aenter__()
        return self._client

    async def put(self, key: str, data: bytes) -> str:
        """Write the object (AES256 server-side encryption)."""
        s3 = await self._s3()
        name = f"{self.prefix}{key}"
        await s3.put_object(Bucket=self.bucket, Key=name, Body=data, ServerSideEncryption="AES256")
        return name

    async def get(self, ref: str) -> bytes | None:
        """Read the object; None when it is gone."""
        s3 = await self._s3()
        try:
            reply = await s3.get_object(Bucket=self.bucket, Key=ref)
        except Exception as exc:
            if "NoSuchKey" in str(exc) or "404" in str(exc):
                return None
            raise
        async with reply["Body"] as body:
            data: bytes = await body.read()
        return data

    async def aclose(self) -> None:
        """Close the client."""
        if self._context is not None:
            await self._context.__aexit__(None, None, None)
            self._client = self._context = None
