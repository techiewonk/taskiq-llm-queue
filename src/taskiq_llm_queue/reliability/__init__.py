"""Reliability layer: adaptive limiter, leases, circuit breaker, claims, fallback, live config.

All shared state lives in Valkey and changes through Lua scripts that read Valkey `TIME`; the
local limiter covers Valkey outages. Clients are injected, so importing this does no I/O.
"""

from __future__ import annotations

from taskiq_llm_queue.reliability.breaker import BreakerSettings, BreakerState, CircuitBreaker
from taskiq_llm_queue.reliability.claims import ClaimSettings, FailureCount, RedisClaimStore
from taskiq_llm_queue.reliability.config_store import (
    AUDIT_RETENTION_SECS,
    LiveConfigStore,
    fallback_store,
)
from taskiq_llm_queue.reliability.fallback import (
    ChainStep,
    FallbackConfig,
    FallbackStep,
    resolve_fallback,
)
from taskiq_llm_queue.reliability.limiter import (
    AdaptiveLimiter,
    LimiterSettings,
    LimiterState,
    PauseGate,
    SyncAdaptiveLimiter,
    local_cap,
)
from taskiq_llm_queue.reliability.local_limiter import LocalLimiter

__all__ = [
    "AUDIT_RETENTION_SECS",
    "AdaptiveLimiter",
    "BreakerSettings",
    "BreakerState",
    "ChainStep",
    "CircuitBreaker",
    "ClaimSettings",
    "FailureCount",
    "FallbackConfig",
    "FallbackStep",
    "LimiterSettings",
    "LimiterState",
    "LiveConfigStore",
    "LocalLimiter",
    "PauseGate",
    "RedisClaimStore",
    "SyncAdaptiveLimiter",
    "fallback_store",
    "local_cap",
    "resolve_fallback",
]
