"""JSON result backend on Redis/Valkey: taskiq's `AsyncResultBackend` plus `ResultStore`.

Workers store results through taskiq (`set_result`) or `put`; callers read them back through
taskiq (`get_result`, deleted on read) or in batches through `take_many`, which the per-pod
`ResultDispatcher` calls once per interval. Results are JSON only, never pickle.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.connection import Connection, SSLConnection
from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.exceptions import ResultGetError

from taskiq_llm_queue.envelope import LLMReply

if TYPE_CHECKING:
    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.keys import KeySpace

logger = logging.getLogger(__name__)

DEFAULT_TAKE_BATCH = 1000  # keys per MGET; 10K waiters on one pod -> 10 MGETs per interval


class LLMQResultBackend(AsyncResultBackend[Any]):
    """Stores task results as JSON under `KeySpace.result(task_id)` with a TTL.

    Several brokers may share one instance (finding A6): `startup` counts users and
    `shutdown` closes the pool only when the last of them shuts down. An injected client is
    never closed here; its creator owns it. `aclose` force-closes an owned pool.
    """

    def __init__(
        self,
        keys: KeySpace,
        *,
        redis: Redis | None = None,
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        ssl: bool = False,
        max_connections: int = 32,
        pool_timeout_secs: float = 5.0,
        result_ttl_secs: int = 300,
        delete_on_read: bool = True,
        take_batch: int = DEFAULT_TAKE_BATCH,
    ) -> None:
        """Keep settings only; the owned client is created lazily on first use (no I/O here).

        `delete_on_read` makes taskiq's `get_result` use `GETDEL`. `take_batch`
        caps the keys sent in one `MGET` by `take_many`.
        """
        if take_batch < 1:
            raise ValueError("take_batch must be >= 1")
        self.keys = keys
        self.result_ttl_secs = result_ttl_secs
        self.delete_on_read = delete_on_read
        self.take_batch = take_batch
        self._redis = redis
        self._owns_client = redis is None
        self._conn_kwargs: dict[str, Any] = {
            "connection_class": SSLConnection if ssl else Connection,
            "host": host,
            "port": port,
            "password": password,
            "max_connections": max_connections,
            "timeout": pool_timeout_secs,
        }
        self._users = 0

    @classmethod
    def from_config(cls, config: QueueConfig, *, redis: Redis | None = None) -> LLMQResultBackend:
        """Build a backend from `QueueConfig` (prefix, Redis endpoint, pool size, result TTL)."""
        from taskiq_llm_queue.keys import KeySpace

        return cls(
            KeySpace(config.key_prefix),
            redis=redis,
            host=config.redis_host,
            port=config.redis_port,
            password=config.redis_password,
            ssl=config.redis_ssl,
            max_connections=config.redis_max_connections,
            result_ttl_secs=config.result_ttl_secs,
        )

    @property
    def redis(self) -> Redis:
        """The client in use, created on first access with a bounded blocking pool."""
        if self._redis is None:
            pool = BlockingConnectionPool(**self._conn_kwargs)
            self._redis = Redis(connection_pool=pool)
        return self._redis

    async def startup(self) -> None:
        """Register one user (e.g. one broker) and make sure the client exists."""
        self._users += 1
        _ = self.redis

    async def shutdown(self) -> None:
        """Drop one user; close the owned pool once no user is left. Safe to call repeatedly."""
        self._users = max(0, self._users - 1)
        if self._users == 0:
            await self.aclose()

    async def aclose(self) -> None:
        """Close the owned client and pool now, whoever still uses them; idempotent."""
        if self._owns_client and self._redis is not None:
            client, self._redis = self._redis, None
            await client.aclose()
            await client.connection_pool.disconnect()

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        """Store a taskiq result as JSON with the configured TTL."""
        await self.redis.set(
            self.keys.result(task_id), result.model_dump_json().encode(), ex=self.result_ttl_secs
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """True when a result for `task_id` is stored and not yet taken."""
        return bool(await self.redis.exists(self.keys.result(task_id)))

    async def get_result(self, task_id: str, with_logs: bool = False) -> TaskiqResult[Any]:
        """Read one result (`GETDEL` when `delete_on_read`); raises `ResultGetError` if absent."""
        key = self.keys.result(task_id)
        raw = await (self.redis.getdel(key) if self.delete_on_read else self.redis.get(key))
        if raw is None:
            raise ResultGetError
        return TaskiqResult[Any].model_validate_json(raw)

    async def put(self, reply: LLMReply, *, ttl_secs: int) -> None:
        """Store an `LLMReply` under its `request_id` (the task id) in taskiq's result format."""
        result = TaskiqResult[Any](
            is_err=False, return_value=reply.model_dump(mode="json"), execution_time=0.0
        )
        await self.redis.set(
            self.keys.result(reply.request_id), result.model_dump_json().encode(), ex=ttl_secs
        )

    async def take_many(self, task_ids: Sequence[str]) -> dict[str, LLMReply]:
        """Fetch and delete whichever results are ready: one atomic `MGET`+`DEL` per batch.

        MULTI/EXEC makes the delete cover exactly what the read saw, so a result written in
        between is never lost. Needs non-cluster Redis: the keys span hash slots.
        """
        ready: dict[str, LLMReply] = {}
        for start in range(0, len(task_ids), self.take_batch):
            ids = list(task_ids[start : start + self.take_batch])
            keys = [self.keys.result(task_id) for task_id in ids]
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.mget(keys)
                pipe.delete(*keys)
                values, _ = await pipe.execute()
            for task_id, raw in zip(ids, values, strict=True):
                if raw is not None:
                    ready[task_id] = decode_reply(task_id, raw)
        return ready


def decode_reply(task_id: str, raw: bytes | str) -> LLMReply:
    """Turn stored taskiq-result JSON into an `LLMReply`, never raising for bad content.

    A task that raised, or a value that is not an `LLMReply`, becomes `ok=False` with an
    `error` dict, so the waiting caller is always released with a value.
    """
    try:
        result = TaskiqResult[Any].model_validate_json(raw)
        if result.is_err:
            error = result.error
            return LLMReply(
                request_id=task_id,
                ok=False,
                error={
                    "type": type(error).__name__ if error else "task_error",
                    "message": str(error) if error else "",
                    "retryable": False,
                },
            )
        return LLMReply.model_validate(result.return_value)
    except Exception as exc:  # any bad payload must still release the waiter
        logger.warning("undecodable result for task %s: %s", task_id, type(exc).__name__)
        return LLMReply(
            request_id=task_id,
            ok=False,
            error={"type": "invalid_result", "message": type(exc).__name__, "retryable": False},
        )
