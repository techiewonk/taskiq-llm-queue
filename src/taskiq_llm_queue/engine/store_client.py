"""The store client (Valkey / Redis) every process builds: one place for its failover options.

A managed store (ElastiCache) fails over by promoting the replica and moving the endpoint's DNS
name. Pooled connections still reach the old primary, now a replica, and every write gets
`READONLY`; redis-py keeps such a connection, so without these options the queue stayed down
after a failover until the process restarted (measured, CHANGELOG 0.3.0). Here `READONLY`, like
a dropped connection, closes the connection and retries on a new one, which resolves the name
again. Used by `LLMQueue.from_env` and the `llm-queue` CLI.
"""

from __future__ import annotations

from typing import Any

import redis.asyncio
from redis.asyncio.retry import Retry
from redis.backoff import EqualJitterBackoff  # in every redis-py the bounds allow (>=5)
from redis.credentials import CredentialProvider
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError

from taskiq_llm_queue.tuning import StoreOptions

MAX_CONNECTIONS = 200  # LLMQ_STORE_MAX_CONNECTIONS; about worker slots + callers + 3 readers


def store_client(
    url: str,
    credential_provider: CredentialProvider | None = None,
    *,
    max_connections: int = MAX_CONNECTIONS,
    options: StoreOptions | None = None,
) -> redis.asyncio.Redis:
    """An async client (bytes replies) for `url` that reconnects on drops and on `READONLY`.

    The pool blocks when every connection is in use: redis-py's default pool raised at once,
    and 150 concurrent calls on 100 connections were reported as the store being down
    (measured in the 0.3.0 review). `valkey://` / `valkeys://` are read as `redis(s)://`.
    """
    opts = options or StoreOptions()  # timeouts, retries: `Tuning.store`
    if url.startswith("valkey"):
        url = "redis" + url[len("valkey") :]
    kwargs: dict[str, Any] = {
        # Not on a read timeout: the command may have run (a Lua write would run twice), and
        # each retry waited the whole timeout again: 4 x 15 s, a 61 s call in the slow-DNS
        # failover drill. redis-py's Retry retries TimeoutError by default and
        # `retry_on_error` only adds to that list, so the list is given to Retry itself
        # (measured: a half-open store hung a command 60 s with the default, 15 s without).
        "retry": Retry(
            EqualJitterBackoff(cap=1.0, base=0.05),
            opts.retries,
            supported_errors=(StoreConnectionError, ReadOnlyError),
        ),
        "retry_on_error": [StoreConnectionError, ReadOnlyError],
        "socket_connect_timeout": opts.connect_timeout_secs,
        "socket_timeout": opts.read_timeout_secs,
        "socket_keepalive": True,
        "health_check_interval": opts.health_check_secs,
    }
    if credential_provider is not None:
        kwargs["credential_provider"] = credential_provider
    pool = redis.asyncio.BlockingConnectionPool.from_url(
        url, max_connections=max_connections, timeout=opts.pool_wait_secs, **kwargs
    )
    return redis.asyncio.Redis.from_pool(pool)  # owns the pool: aclose() closes it
