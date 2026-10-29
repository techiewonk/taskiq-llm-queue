"""Typed configuration: models and their targets, queues, Redis/SQS endpoints, timeouts.

`QueueConfig` is the single object an app builds (directly or with `from_env`) and passes to
`LLMQueue`. Validation runs once at construction, so bad config fails at boot, not mid-request.
`create_redis` / `create_sync_redis` build the package's Redis/Valkey clients from it.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.errors import ConfigError

if TYPE_CHECKING:
    import redis
    import redis.asyncio

logger = get_logger(__name__)

# Built-in provider names; custom providers (QueueConfig.providers / entry points) add their own.
BUILTIN_PROVIDERS = frozenset({"bedrock", "openai"})
ProviderKind = str
ENV_PREFIX = "LLMQ_"
_ALIAS_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
# Key of the BLAKE2b hash behind hashed ids and canary buckets. Public and fixed, so only good
# for local use: set your own `LLMQ_ID_HASH_SALT` in production (all pods must share it).
DEFAULT_ID_HASH_SALT = "taskiq-llm-queue/id-hash/v1"
_MAX_SALT_BYTES = 64  # BLAKE2b key limit


class Lane(StrEnum):
    """Request priority: live chat, background work, bulk ingestion (highest first)."""

    RT = "rt"
    BG = "bg"
    BULK = "bulk"


class Target(BaseModel):
    """One place a model can be served from: a region, a cross-region profile or a URL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    region: str | None = None
    profile_id: str | None = None
    base_url: str | None = None


class ModelConfig(BaseModel):
    """One model alias: provider, provider model id, named targets and its queues.

    `queues` maps `rt` / `default` / optional `bulk` to SQS queue names or URLs. The first entry
    in `targets` is the primary target. `max_concurrency` is a hard cluster-wide cap on calls in
    flight per (alias, target); None leaves the adaptive limit free up to `LimiterTuning.max_limit`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: ProviderKind
    model_id: str
    targets: dict[str, Target] = Field(default_factory=lambda: {"primary": Target()})
    queues: dict[Literal["rt", "default", "bulk"], str] = Field(default_factory=dict)
    limiter_initial: int = Field(default=20, ge=1)
    max_concurrency: int | None = Field(
        default=None,
        ge=1,
        description="Hard cap on this alias's calls in flight across all pods, per target "
        "(the limiter key is per alias and target). The adaptive limit still learns and cuts "
        "below it but never grows past it; `limiter_initial` above it is clamped to it. "
        "`limiter_initial == max_concurrency` gives a fixed bucket that only shrinks on "
        "clustered throttles.",
    )
    api_key_env: str | None = None

    @field_validator("targets")
    @classmethod
    def _targets_not_empty(cls, value: dict[str, Target]) -> dict[str, Target]:
        if not value:
            raise ValueError("a model needs at least one target")
        return value


class LimiterTuning(BaseModel):
    """Adaptive limiter tuning (`LLMQ_AC_*`), mapped onto `reliability.LimiterSettings`.

    `initial` overrides a model's `limiter_initial` by alias (`LLMQ_AC_INITIAL`, JSON);
    `max_by_model` overrides its `max_concurrency` (`LLMQ_AC_MAX_BY_MODEL`, JSON, same shape).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    initial: dict[str, int] = Field(default_factory=dict)
    max_by_model: dict[str, int] = Field(default_factory=dict)
    min_limit: int = Field(default=2, ge=1)
    max_limit: int = Field(default=2000, ge=1)
    ramp_window: int = Field(default=50, ge=1)
    increase_every: int = Field(default=20, ge=1)
    decrease_factor: float = Field(default=0.7, gt=0, lt=1)
    pause_secs: float = Field(default=2.0, ge=0)
    bg_share: float = Field(default=0.8, gt=0, le=1)
    utilisation: float = Field(default=0.9, gt=0, le=1)
    ramp_cap: int | None = Field(default=None, ge=1)
    # Renewed every pod heartbeat (10 s); a killed pod's slots return within it (S35b: 240 s
    # left a killed worker's leases blocking most of the limit for 4 minutes).
    lease_ttl_secs: float = Field(default=60.0, gt=0)
    # I20: the cut / pause / hold / probe knobs (meaning: `reliability.LimiterSettings`).
    peak_halflife_secs: float = Field(default=300.0, gt=0)
    loss_tolerance: float = Field(default=0.05, ge=0, le=1)
    min_throttles: int = Field(default=2, ge=1)
    pause_ratio: float = Field(default=0.5, ge=0, le=1)
    window_secs: float = Field(default=1.0, gt=0)
    settle_ratio: float = Field(default=0.9, gt=0, le=1)
    probe_secs: float = Field(default=5.0, ge=0)
    hold_ratio: float = Field(default=0.6, ge=0)

    @model_validator(mode="after")
    def _min_le_max(self) -> LimiterTuning:
        if self.min_limit > self.max_limit:
            raise ValueError("limiter.min_limit must be <= limiter.max_limit")
        bad = {k: v for k, v in self.max_by_model.items() if v < 1}
        if bad:
            raise ValueError(f"limiter.max_by_model values must be >= 1: {bad}")
        return self


class BreakerTuning(BaseModel):
    """Circuit breaker tuning (`LLMQ_CB_*`), mapped onto `reliability.BreakerSettings`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    threshold: int = Field(default=5, ge=1)
    window_secs: float = Field(default=30.0, gt=0)
    cooldown_secs: float = Field(default=15.0, gt=0)


LANE_SETTINGS = frozenset({"timeout_secs", "embed_timeout_secs", "lane_max_concurrency"})


class Overrides(BaseModel):
    """Settings one model alias (or every model of one provider) may override; None = inherit."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_concurrency: int | None = Field(default=None, ge=1)
    limiter_initial: int | None = Field(default=None, ge=1)
    bg_share: float | None = Field(default=None, gt=0, le=1)
    timeout_secs: dict[Lane, float] = Field(default_factory=dict)
    embed_timeout_secs: dict[Lane, float] = Field(default_factory=dict)
    # F19: cap on one lane's calls in flight per target, inside `max_concurrency`.
    lane_max_concurrency: dict[Lane, int] = Field(default_factory=dict)
    attempt_timeout_secs: float | None = Field(default=None, gt=0)
    throttle_retry_attempts: int | None = Field(default=None, ge=1, le=20)
    retry_base_secs: float | None = Field(default=None, ge=0)
    retry_multiplier: float | None = Field(default=None, ge=1)
    retry_spread: float | None = Field(default=None, ge=1)
    retry_max_secs: float | None = Field(default=None, gt=0)
    throttle_retry_max_secs: float | None = Field(default=None, gt=0)
    rt_capacity_wait_secs: float | None = Field(default=None, ge=0)
    breaker_threshold: int | None = Field(default=None, ge=1)
    breaker_cooldown_secs: float | None = Field(default=None, gt=0)
    # F17 rate budget per model target (docs/RATE_LIMIT_PLAN.md); None = no rate limit.
    rpm: int | None = Field(default=None, ge=1)
    tpm: int | None = Field(default=None, ge=1)
    rate_window: Literal["smooth", "fixed"] | None = None
    rate_burst: int | None = Field(default=None, ge=1)
    rt_rate_share: float | None = Field(default=None, ge=0, lt=1)

    @field_validator("timeout_secs", "embed_timeout_secs", "lane_max_concurrency")
    @classmethod
    def _positive(cls, value: dict[Lane, Any]) -> dict[Lane, Any]:
        if any(v <= 0 for v in value.values()):
            raise ValueError(f"lane values must be > 0: {value}")
        return value

    def get(self, name: str, lane: Lane | None = None) -> Any:
        """The override of `name` (a lane's entry for lane settings), None when not set."""
        value = getattr(self, name, None)
        if name in LANE_SETTINGS and isinstance(value, dict):
            return value.get(lane)
        return value


class ProviderOverrides(Overrides):
    """Provider defaults for its models, plus a cap shared by all of them.

    `shared_max_concurrency` bounds the calls in flight across every model alias of the provider
    and all pods (e.g. one vLLM server hosting three models); each call then holds two slots.
    """

    shared_max_concurrency: int | None = Field(default=None, ge=1)
    # One RPM / TPM budget across all models of the provider (an account quota).
    shared_rpm: int | None = Field(default=None, ge=1)
    shared_tpm: int | None = Field(default=None, ge=1)


class ModelSettings(BaseModel):
    """`{"providers": {name: ProviderOverrides}, "models": {alias: Overrides}}` (live or env)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    providers: dict[str, ProviderOverrides] = Field(default_factory=dict)
    models: dict[str, Overrides] = Field(default_factory=dict)

    def with_values(
        self, scope: Literal["providers", "models"], key: str, values: Mapping[str, Any]
    ) -> ModelSettings:
        """Copy with `values` merged into one entry; a None value removes that setting."""
        entries = {k: v.model_dump(exclude_defaults=True) for k, v in getattr(self, scope).items()}
        entry = dict(entries.get(key, {}))
        for name, value in values.items():
            if name in LANE_SETTINGS and isinstance(value, Mapping):
                lanes = {**entry.get(name, {}), **value}
                entry[name] = {lane: v for lane, v in lanes.items() if v is not None}
            elif value is None:
                entry.pop(name, None)
            else:
                entry[name] = value
        entries[key] = entry
        return ModelSettings.model_validate(
            {
                **self.model_dump(exclude_defaults=True),
                scope: {k: v for k, v in entries.items() if v},
            }
        )

    def without(
        self, scope: Literal["providers", "models"], key: str, names: list[str] | None = None
    ) -> ModelSettings:
        """Copy without the named settings of one entry, or without the whole entry."""
        if not names:
            entries = {k: v for k, v in getattr(self, scope).items() if k != key}
            return self.model_copy(update={scope: entries})
        return self.with_values(scope, key, dict.fromkeys(names))


def _lane_map(default: Mapping[Lane, Any]) -> Any:
    """Field factory for per-lane settings (a fresh dict per instance)."""
    return Field(default_factory=lambda: dict(default))


class QueueConfig(BaseModel):
    """Everything `LLMQueue` needs; immutable after construction.

    Timeouts are per lane; TTLs follow one rule (state keys outlive the longest lane
    deadline plus worst-case execution; results only until the caller reads them).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    key_prefix: str = "llmq:local"
    # Redis/Valkey: `redis_url` (redis:// or rediss://) wins over host/port/ssl when set; its
    # credentials win over `redis_username` / `redis_password`.
    redis_url: SecretStr | None = None
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_username: str | None = None
    redis_password: SecretStr | None = None
    redis_ssl: bool = False
    redis_max_connections: int = Field(default=32, ge=1)
    redis_socket_timeout_secs: float = Field(default=5.0, gt=0)
    redis_socket_connect_timeout_secs: float = Field(default=2.0, gt=0)
    redis_health_check_interval_secs: int = Field(default=30, ge=0)
    redis_retry_attempts: int = Field(default=3, ge=0, le=10)
    redis_client_name: str | None = "taskiq-llm-queue"
    sqs_endpoint_url: str | None = None
    bedrock_endpoint_url: str | None = None
    # None lets the AWS chain decide (AWS_REGION, profile, IRSA / Pod Identity).
    aws_region: str | None = None
    models: dict[str, ModelConfig] = Field(default_factory=dict)
    allowed_providers: frozenset[ProviderKind] = BUILTIN_PROVIDERS
    denied_hosts: tuple[str, ...] = ()
    providers: dict[str, str] = Field(default_factory=dict)
    timeout_secs: dict[Lane, float] = Field(
        default_factory=lambda: {Lane.RT: 90.0, Lane.BG: 300.0, Lane.BULK: 900.0}
    )
    state_ttl_secs: int = Field(default=1800, ge=60)
    result_ttl_secs: int = Field(default=300, ge=10)
    inline_max_bytes: int = Field(default=200_000, ge=1024, le=1_000_000)
    # I16: with a bucket, messages over `inline_max_bytes` carry their args in S3 instead of
    # a Valkey `params_ref` (a lifecycle rule on the prefix should expire leftovers).
    s3_offload_bucket: str | None = None
    s3_offload_prefix: str = "llmq-offload"
    s3_endpoint_url: str | None = None
    hash_ids: bool = True
    id_hash_salt: SecretStr = SecretStr(DEFAULT_ID_HASH_SALT)
    strict_async: bool = False
    # --- runtime tuning (all optional; defaults are the recommended production starts)
    limiter: LimiterTuning = Field(default_factory=LimiterTuning)
    breaker: BreakerTuning = Field(default_factory=BreakerTuning)
    max_failures: int = Field(default=3, ge=1)
    local_limit_cap: int = Field(default=10, ge=1)
    cfg_reload_secs: float = Field(default=30.0, gt=0)
    # Receiver `max_async_tasks` per model alias (`jobs` for the job broker); unset -> default.
    worker_slots: dict[str, int] = Field(default_factory=dict)
    drain_secs: float = Field(default=45.0, ge=0)
    result_dispatch_ms: dict[Lane, int] = _lane_map({Lane.RT: 100, Lane.BG: 500, Lane.BULK: 2000})
    embed_timeout_secs: dict[Lane, float] = _lane_map(
        {Lane.RT: 10.0, Lane.BG: 60.0, Lane.BULK: 300.0}
    )
    # Startup default of the live fallback chains (`reliability.FallbackConfig` JSON shape).
    fallback_default: dict[str, Any] = Field(default_factory=dict)
    routing_enabled: bool = False  # build a CanaryRouter on the live routing config
    routing_default: dict[str, Any] = Field(default_factory=dict)
    # Env layer of per-provider / per-model settings (`LLMQ_SETTINGS`, `ModelSettings` JSON).
    settings_default: ModelSettings = Field(default_factory=lambda: ModelSettings())
    job_queues: dict[str, str] = Field(default_factory=dict)
    retry_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_secs: float = Field(default=1.0, ge=0)
    retry_multiplier: float = Field(
        default=4.0,
        ge=1,
        description="Retry n (0-based) sleeps a random time in [base * multiplier^n, that * "
        "retry_spread]: 1-2.5 s, 4-10 s, 16-30 s with the defaults.",
    )
    retry_spread: float = Field(default=2.5, ge=1)
    retry_max_secs: float = Field(default=30.0, gt=0)
    throttle_retry_max_secs: float = Field(
        default=10.0,
        gt=0,
        description="Cap of one sleep after a throttle, so `throttle_retry_attempts` tries still "
        "fit an rt deadline (6 tries sleep at most 1-2.5 + 4-10 + 3 x 10 s).",
    )
    throttle_retry_attempts: int = Field(
        default=6,
        ge=1,
        le=20,
        description="Attempts per fallback step once the provider throttled it (instead of "
        "`retry_attempts` when larger), each after a backoff that holds no limiter slot. "
        "At S35b's 30 % random 429 rate a step fails 0.3^3 = 2.7 % with 3 tries, 0.07 % with 6.",
    )
    rt_capacity_wait_secs: float = Field(
        default=30.0,
        ge=0,
        description="How long a received rt call keeps walking its fallback chain for a free "
        "slot (2 s per step per pass) before `no_capacity`; never past its deadline. S35b: 36 of "
        "6,043 rt calls gave up after 3 passes with 33-157 s of deadline left.",
    )
    sqs_wait_time_secs: int = Field(default=20, ge=0, le=20)
    sqs_failover_secs: float = Field(default=300.0, gt=0)
    sqs_timeouts_to_failover: int = Field(
        default=3,
        ge=1,
        description="SQS send timeouts within a minute before a model fails over to direct "
        "(a refused send fails over at once). H17: one timeout in a burst cost 300 s of SQS.",
    )
    attempt_timeout_secs: float = Field(
        default=180.0,
        gt=0,
        description="Longest one provider attempt may run (capped by the call's remaining "
        "deadline); on expiry the worker tries the next fallback target. Unverified default: "
        "long enough for an ~8k-token completion.",
    )
    provider_read_timeout_secs: float = Field(
        default=300.0,
        gt=0,
        description="Socket read timeout of the built-in providers (botocore `read_timeout`, "
        "httpx read for OpenAI-compatible servers).",
    )
    provider_connect_timeout_secs: float = Field(
        default=10.0,
        gt=0,
        description="Connect timeout of the built-in providers (botocore `connect_timeout`, "
        "httpx connect for OpenAI-compatible servers).",
    )
    rate_window: Literal["smooth", "fixed"] = Field(
        default="smooth",
        description="Shape of `rpm` / `tpm` budgets: `smooth` spreads starts evenly (token "
        "bucket with `rate_burst`), `fixed` allows the whole budget from each calendar minute.",
    )
    rt_rate_share: float = Field(
        default=0.2,
        ge=0,
        lt=1,
        description="Part of each rate budget only rt calls may use, so a bg backlog never "
        "starves live calls (bg/bulk get at most 1 - share of the rate).",
    )
    sqs_batch_wait_secs: float = Field(
        default=0.02,
        ge=0,
        le=10,
        description="Send batching on bg (`default`) and bulk queues: sends within this window "
        "go in one SendMessageBatch (up to 10 / 256 KB). 0 turns batching off; rt never batches.",
    )
    sqs_send_timeout_secs: float = Field(
        default=5.0,
        gt=0,
        description="Upper bound on one SQS send before the call fails over to direct; the "
        "send also gets at most half the call's remaining budget. A send to an unreachable "
        "SQS otherwise hangs on a dead pooled connection until the 60 s read timeout.",
    )
    valkey_wait_failover_secs: float = Field(
        default=10.0,
        gt=0,
        description="How long result reads may fail before callers waiting on a queued call "
        "run it direct under the local cap instead of waiting to their deadline. Longer than a "
        "Valkey restart blip, so in-flight worker results are not run twice.",
    )

    @field_validator("id_hash_salt")
    @classmethod
    def _salt_fits(cls, value: SecretStr) -> SecretStr:
        size = len(value.get_secret_value().encode("utf-8"))
        if not 1 <= size <= _MAX_SALT_BYTES:
            raise ValueError(f"id_hash_salt must be 1-{_MAX_SALT_BYTES} bytes, got {size}")
        return value

    @field_validator("models")
    @classmethod
    def _aliases_valid(cls, value: dict[str, ModelConfig]) -> dict[str, ModelConfig]:
        bad = [alias for alias in value if not _ALIAS_RE.match(alias)]
        if bad:
            raise ValueError(f"invalid model aliases (lowercase, no '@'): {bad}")
        return value

    @model_validator(mode="after")
    def _providers_allowed(self) -> QueueConfig:
        blocked = {
            a: m.provider
            for a, m in self.models.items()
            if m.provider not in self.allowed_providers
        }
        if blocked:
            raise ValueError(f"models use providers outside allowed_providers: {blocked}")
        if self.fallback_default:
            from taskiq_llm_queue.reliability.fallback import FallbackConfig

            FallbackConfig.model_validate(self.fallback_default)  # fail at boot, not per call
        bad_slots = {k: v for k, v in self.worker_slots.items() if v < 1}
        if bad_slots:
            raise ValueError(f"worker_slots must be >= 1: {bad_slots}")
        return self

    def resolve(self, model: str) -> tuple[str, ModelConfig, str]:
        """Split `alias[@target]` and return (alias, model config, target name).

        Raises `ConfigError` for an unknown alias or target, so typos surface on first use.
        """
        alias, _, target = model.partition("@")
        cfg = self.models.get(alias)
        if cfg is None:
            raise ConfigError(f"unknown model alias {alias!r}; known: {sorted(self.models)}")
        target = target or next(iter(cfg.targets))
        if target not in cfg.targets:
            raise ConfigError(
                f"unknown target {target!r} for {alias!r}; known: {sorted(cfg.targets)}"
            )
        return alias, cfg, target

    @property
    def id_hash_key(self) -> bytes:
        """`id_hash_salt` as the BLAKE2b key for hashed ids and canary buckets."""
        return self.id_hash_salt.get_secret_value().encode("utf-8")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> QueueConfig:
        """Build a config from `LLMQ_*` variables; `LLMQ_MODELS` holds the models as JSON.

        Every bad value is collected and reported in one `ConfigError`.
        """
        env = os.environ if environ is None else environ

        def get(name: str) -> str | None:
            return env.get(ENV_PREFIX + name)

        raw: dict[str, object] = {}
        errors: list[str] = []
        for name, key, conv in (
            ("KEY_PREFIX", "key_prefix", str),
            ("REDIS_URL", "redis_url", str),
            ("REDIS_HOST", "redis_host", str),
            ("REDIS_PORT", "redis_port", int),
            ("REDIS_USERNAME", "redis_username", str),
            ("REDIS_PASSWORD", "redis_password", str),
            ("REDIS_SSL", "redis_ssl", _to_bool),
            ("REDIS_MAX_CONNECTIONS", "redis_max_connections", int),
            ("REDIS_SOCKET_TIMEOUT_SECS", "redis_socket_timeout_secs", float),
            ("REDIS_SOCKET_CONNECT_TIMEOUT_SECS", "redis_socket_connect_timeout_secs", float),
            ("REDIS_HEALTH_CHECK_INTERVAL_SECS", "redis_health_check_interval_secs", int),
            ("REDIS_RETRY_ATTEMPTS", "redis_retry_attempts", int),
            ("REDIS_CLIENT_NAME", "redis_client_name", str),
            ("SQS_ENDPOINT_URL", "sqs_endpoint_url", str),
            ("S3_OFFLOAD_BUCKET", "s3_offload_bucket", str),
            ("S3_OFFLOAD_PREFIX", "s3_offload_prefix", str),
            ("S3_ENDPOINT_URL", "s3_endpoint_url", str),
            ("BEDROCK_ENDPOINT_URL", "bedrock_endpoint_url", str),
            ("AWS_REGION", "aws_region", str),
            ("STATE_TTL_SECS", "state_ttl_secs", int),
            ("RESULT_TTL_SECS", "result_ttl_secs", int),
            ("INLINE_MAX_BYTES", "inline_max_bytes", int),
            ("HASH_IDS", "hash_ids", _to_bool),
            ("ID_HASH_SALT", "id_hash_salt", str),
            ("STRICT_ASYNC", "strict_async", _to_bool),
            ("ROUTING_ENABLED", "routing_enabled", _to_bool),
            ("MAX_FAILURES", "max_failures", int),
            ("LOCAL_LIMIT_CAP", "local_limit_cap", int),
            ("CFG_RELOAD_SECS", "cfg_reload_secs", float),
            ("CONSUMER_DRAIN_SECS", "drain_secs", float),
            ("RETRY_ATTEMPTS", "retry_attempts", int),
            ("RETRY_BASE_SECS", "retry_base_secs", float),
            ("RETRY_MULTIPLIER", "retry_multiplier", float),
            ("RETRY_SPREAD", "retry_spread", float),
            ("RETRY_MAX_SECS", "retry_max_secs", float),
            ("THROTTLE_RETRY_MAX_SECS", "throttle_retry_max_secs", float),
            ("RT_CAPACITY_WAIT_SECS", "rt_capacity_wait_secs", float),
            ("THROTTLE_RETRY_ATTEMPTS", "throttle_retry_attempts", int),
            ("SQS_WAIT_TIME_SECS", "sqs_wait_time_secs", int),
            ("SQS_FAILOVER_SECS", "sqs_failover_secs", float),
            ("SQS_TIMEOUTS_TO_FAILOVER", "sqs_timeouts_to_failover", int),
            ("ATTEMPT_TIMEOUT_SECS", "attempt_timeout_secs", float),
            ("PROVIDER_READ_TIMEOUT_SECS", "provider_read_timeout_secs", float),
            ("PROVIDER_CONNECT_TIMEOUT_SECS", "provider_connect_timeout_secs", float),
            ("SQS_SEND_TIMEOUT_SECS", "sqs_send_timeout_secs", float),
            ("SQS_BATCH_WAIT_SECS", "sqs_batch_wait_secs", float),
            ("RATE_WINDOW", "rate_window", str),
            ("RT_RATE_SHARE", "rt_rate_share", float),
            ("VALKEY_WAIT_FAILOVER_SECS", "valkey_wait_failover_secs", float),
            ("WORKER_SLOTS", "worker_slots", _int_map),
        ):
            value = get(name)
            if value is None:
                continue
            try:
                raw[key] = conv(value)
            except ValueError as exc:
                errors.append(f"{ENV_PREFIX}{name}: {exc}")
        for name, key in (
            ("MODELS", "models"),
            ("PROVIDERS", "providers"),
            ("FALLBACK_CHAINS", "fallback_default"),
            ("ROUTING_DEFAULT", "routing_default"),
            ("SETTINGS", "settings_default"),
            ("JOB_QUEUES", "job_queues"),
        ):
            value = get(name)
            if value:
                try:
                    raw[key] = orjson.loads(value)
                except orjson.JSONDecodeError as exc:
                    errors.append(f"{ENV_PREFIX}{name}: invalid JSON ({exc})")
        _lanes_from_env(get, raw, errors)
        _tuning_from_env(get, raw, errors)
        if (allowed := get("ALLOWED_PROVIDERS")) is not None:
            raw["allowed_providers"] = frozenset(p.strip() for p in allowed.split(",") if p.strip())
        if (denied := get("DENIED_HOSTS")) is not None:
            raw["denied_hosts"] = tuple(h.strip() for h in denied.split(",") if h.strip())
        if errors:
            raise ConfigError("; ".join(errors))
        try:
            return cls.model_validate(raw)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc


_Getter = Callable[[str], "str | None"]

# (env suffix, LimiterTuning field, converter) for `LLMQ_AC_*`; `CB_*` likewise for the breaker.
_LIMITER_ENV: tuple[tuple[str, str, Callable[[str], Any]], ...] = (
    ("AC_MIN", "min_limit", int),
    ("AC_MAX", "max_limit", int),
    ("AC_RAMP_WINDOW", "ramp_window", int),
    ("AC_INCREASE_EVERY", "increase_every", int),
    ("AC_DECREASE_FACTOR", "decrease_factor", float),
    ("AC_PAUSE_SECS", "pause_secs", float),
    ("AC_BG_SHARE", "bg_share", float),
    ("AC_RAMP_CAP", "ramp_cap", int),
    ("AC_LEASE_TTL_SECS", "lease_ttl_secs", float),
    ("AC_UTILISATION", "utilisation", float),
    ("AC_PEAK_HALFLIFE_SECS", "peak_halflife_secs", float),
    ("AC_LOSS_TOLERANCE", "loss_tolerance", float),
    ("AC_MIN_THROTTLES", "min_throttles", int),
    ("AC_PAUSE_RATIO", "pause_ratio", float),
    ("AC_WINDOW_SECS", "window_secs", float),
    ("AC_SETTLE_RATIO", "settle_ratio", float),
    ("AC_PROBE_SECS", "probe_secs", float),
    ("AC_HOLD_RATIO", "hold_ratio", float),
)
_BREAKER_ENV: tuple[tuple[str, str, Callable[[str], Any]], ...] = (
    ("CB_THRESHOLD", "threshold", int),
    ("CB_WINDOW_SECS", "window_secs", float),
    ("CB_COOLDOWN_SECS", "cooldown_secs", float),
)


def _int_map(value: str) -> dict[str, int]:
    """Parse `a:1,b:2` (the `LLMQ_WORKER_SLOTS` / `LLMQ_RESULT_DISPATCH_MS` shape)."""
    out: dict[str, int] = {}
    for part in value.split(","):
        if not part.strip():
            continue
        name, sep, number = part.partition(":")
        if not sep or not name.strip():
            raise ValueError(f"expected name:number pairs, got {part!r}")
        out[name.strip()] = int(number)
    return out


def _lanes_from_env(get: _Getter, raw: dict[str, object], errors: list[str]) -> None:
    """`LLMQ_TIMEOUT_<LANE>_SECS`, `LLMQ_TIMEOUT_EMBED_<LANE>_SECS`, `LLMQ_RESULT_DISPATCH_MS`."""
    for prefix, key in (("TIMEOUT_", "timeout_secs"), ("TIMEOUT_EMBED_", "embed_timeout_secs")):
        lanes: dict[Lane, float] = {}
        for lane in Lane:
            name = f"{prefix}{lane.value.upper()}_SECS"
            value = get(name)
            if value is None:
                continue
            try:
                lanes[lane] = float(value)
            except ValueError as exc:
                errors.append(f"{ENV_PREFIX}{name}: {exc}")
        if lanes:
            default = QueueConfig.model_fields[key].get_default(call_default_factory=True)
            raw[key] = {**default, **lanes}
    if (value := get("RESULT_DISPATCH_MS")) is not None:
        try:
            raw["result_dispatch_ms"] = {Lane(k): v for k, v in _int_map(value).items()}
        except ValueError as exc:
            errors.append(f"{ENV_PREFIX}RESULT_DISPATCH_MS: {exc}")


def _tuning_from_env(get: _Getter, raw: dict[str, object], errors: list[str]) -> None:
    """Nested `limiter` (`LLMQ_AC_*`, JSON `LLMQ_AC_INITIAL` / `_MAX_BY_MODEL`) and `breaker`."""
    for field, table in (("limiter", _LIMITER_ENV), ("breaker", _BREAKER_ENV)):
        values: dict[str, Any] = {}
        for name, key, conv in table:
            value = get(name)
            if value is None:
                continue
            try:
                values[key] = conv(value)
            except ValueError as exc:
                errors.append(f"{ENV_PREFIX}{name}: {exc}")
        for name, key in (("AC_INITIAL", "initial"), ("AC_MAX_BY_MODEL", "max_by_model")):
            if field == "limiter" and (raw_json := get(name)):
                try:
                    values[key] = orjson.loads(raw_json)
                except orjson.JSONDecodeError as exc:
                    errors.append(f"{ENV_PREFIX}{name}: invalid JSON ({exc})")
        if values:
            raw[field] = values


def _to_bool(value: str) -> bool:
    """Parse the usual env-var spellings of a boolean; anything else is an error."""
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"not a boolean: {value!r}")


def _redis_kwargs(config: QueueConfig, retry: Any, client_name: str | None) -> dict[str, Any]:
    """Pool settings shared by the async and sync clients (everything but the address)."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    password = config.redis_password
    return {
        "username": config.redis_username,
        "password": password.get_secret_value() if password is not None else None,
        "max_connections": redis_pool_size(config),
        "socket_timeout": config.redis_socket_timeout_secs,
        "socket_connect_timeout": config.redis_socket_connect_timeout_secs,
        "health_check_interval": config.redis_health_check_interval_secs,
        "retry": retry,
        "retry_on_error": [RedisConnectionError],
        "client_name": client_name or config.redis_client_name,
        "timeout": _POOL_WAIT_SECS,
    }


_POOL_WAIT_SECS = 5.0  # BlockingConnectionPool: wait this long for a free connection
_POOL_SPARE = 16  # connections beyond the worker slots: heartbeat, dispatcher, callers


def redis_pool_size(config: QueueConfig) -> int:
    """Connection cap of the package's Valkey pools: `redis_max_connections`, raised to fit.

    Every worker slot may hold a connection at once (claim, acquire poll, result store), so
    with queued models the cap is at least their worker slots plus `_POOL_SPARE`. Bug: 32
    connections under 154 slots waited out `_POOL_WAIT_SECS`, read as "Valkey down" and leaked
    a limiter lease.
    """
    from taskiq_llm_queue.lifecycle import DEFAULT_JOB_SLOTS, DEFAULT_WORKER_SLOTS, JOBS_BROKER

    brokers: dict[tuple[str, ...], int] = {}  # aliases with the same queues share one broker
    for alias, model in config.models.items():
        if model.queues:
            key = tuple(sorted(model.queues.values()))
            brokers.setdefault(key, config.worker_slots.get(alias, DEFAULT_WORKER_SLOTS))
    slots = sum(brokers.values())
    if config.job_queues:
        slots += config.worker_slots.get(JOBS_BROKER, DEFAULT_JOB_SLOTS)
    return (
        max(config.redis_max_connections, slots + _POOL_SPARE)
        if slots
        else config.redis_max_connections
    )


def _backoff() -> Any:
    from redis.backoff import ExponentialBackoff

    return ExponentialBackoff(cap=1.0, base=0.1)


def create_redis(config: QueueConfig, *, client_name: str | None = None) -> redis.asyncio.Redis:
    """The package's own async client: bounded blocking pool, socket timeouts and retries.

    The one factory for every async Valkey client the package builds (no `decode_responses`).
    Connection errors are retried `redis_retry_attempts` times with exponential backoff.
    """
    from redis.asyncio import BlockingConnectionPool, Redis
    from redis.asyncio.connection import Connection, SSLConnection
    from redis.asyncio.retry import Retry

    kwargs = _redis_kwargs(config, Retry(_backoff(), config.redis_retry_attempts), client_name)
    if config.redis_url is not None:
        pool = BlockingConnectionPool.from_url(config.redis_url.get_secret_value(), **kwargs)
    else:
        pool = BlockingConnectionPool(
            connection_class=SSLConnection if config.redis_ssl else Connection,
            host=config.redis_host,
            port=config.redis_port,
            **kwargs,
        )
    return Redis(connection_pool=pool)


def create_sync_redis(config: QueueConfig, *, client_name: str | None = None) -> redis.Redis:
    """Blocking twin of `create_redis` (same settings) for sync callers and tools."""
    from redis import BlockingConnectionPool, Redis
    from redis.connection import Connection, SSLConnection
    from redis.retry import Retry

    kwargs = _redis_kwargs(config, Retry(_backoff(), config.redis_retry_attempts), client_name)
    if config.redis_url is not None:
        pool = BlockingConnectionPool.from_url(config.redis_url.get_secret_value(), **kwargs)
    else:
        pool = BlockingConnectionPool(  # type: ignore[no-untyped-call]  # redis stub gap
            connection_class=SSLConnection if config.redis_ssl else Connection,
            host=config.redis_host,
            port=config.redis_port,
            **kwargs,
        )
    return Redis(connection_pool=pool)
