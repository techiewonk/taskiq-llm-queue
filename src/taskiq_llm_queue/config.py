"""Typed configuration: models and their targets, queues, Redis/SQS endpoints, timeouts.

`QueueConfig` is the single object an app builds (directly or with `from_env`) and passes to
`LLMQueue`. Validation runs once at construction, so bad config fails at boot, not mid-request.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import Any, Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from taskiq_llm_queue.errors import ConfigError

# Built-in provider names; custom providers (QueueConfig.providers / entry points) add their own.
BUILTIN_PROVIDERS = frozenset({"bedrock", "openai"})
ProviderKind = str
ENV_PREFIX = "LLMQ_"
_ALIAS_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")


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
    in `targets` is the primary target.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: ProviderKind
    model_id: str
    targets: dict[str, Target] = Field(default_factory=lambda: {"primary": Target()})
    queues: dict[Literal["rt", "default", "bulk"], str] = Field(default_factory=dict)
    limiter_initial: int = Field(default=20, ge=1)
    api_key_env: str | None = None

    @field_validator("targets")
    @classmethod
    def _targets_not_empty(cls, value: dict[str, Target]) -> dict[str, Target]:
        if not value:
            raise ValueError("a model needs at least one target")
        return value


class LimiterTuning(BaseModel):
    """Adaptive limiter tuning (`LLMQ_AC_*`), mapped onto `reliability.LimiterSettings`.

    `initial` overrides a model's `limiter_initial` by alias (`LLMQ_AC_INITIAL`, JSON).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    initial: dict[str, int] = Field(default_factory=dict)
    min_limit: int = Field(default=2, ge=1)
    max_limit: int = Field(default=2000, ge=1)
    ramp_window: int = Field(default=50, ge=1)
    increase_every: int = Field(default=20, ge=1)
    decrease_factor: float = Field(default=0.7, gt=0, lt=1)
    pause_secs: float = Field(default=2.0, ge=0)
    bg_share: float = Field(default=0.8, gt=0, le=1)
    utilisation: float = Field(default=0.9, gt=0, le=1)
    ramp_cap: int | None = Field(default=None, ge=1)
    lease_ttl_secs: float = Field(default=240.0, gt=0)

    @model_validator(mode="after")
    def _min_le_max(self) -> LimiterTuning:
        if self.min_limit > self.max_limit:
            raise ValueError("limiter.min_limit must be <= limiter.max_limit")
        return self


class BreakerTuning(BaseModel):
    """Circuit breaker tuning (`LLMQ_CB_*`), mapped onto `reliability.BreakerSettings`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    threshold: int = Field(default=5, ge=1)
    window_secs: float = Field(default=30.0, gt=0)
    cooldown_secs: float = Field(default=15.0, gt=0)


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
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str | None = None
    redis_ssl: bool = False
    redis_max_connections: int = Field(default=32, ge=1)
    sqs_endpoint_url: str | None = None
    bedrock_endpoint_url: str | None = None
    aws_region: str = "us-east-1"
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
    hash_ids: bool = True
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
    job_queues: dict[str, str] = Field(default_factory=dict)
    retry_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_secs: float = Field(default=1.0, ge=0)
    sqs_wait_time_secs: int = Field(default=20, ge=0, le=20)
    sqs_failover_secs: float = Field(default=300.0, gt=0)

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
            ("REDIS_HOST", "redis_host", str),
            ("REDIS_PORT", "redis_port", int),
            ("REDIS_PASSWORD", "redis_password", str),
            ("REDIS_SSL", "redis_ssl", _to_bool),
            ("REDIS_MAX_CONNECTIONS", "redis_max_connections", int),
            ("SQS_ENDPOINT_URL", "sqs_endpoint_url", str),
            ("BEDROCK_ENDPOINT_URL", "bedrock_endpoint_url", str),
            ("AWS_REGION", "aws_region", str),
            ("STATE_TTL_SECS", "state_ttl_secs", int),
            ("RESULT_TTL_SECS", "result_ttl_secs", int),
            ("INLINE_MAX_BYTES", "inline_max_bytes", int),
            ("HASH_IDS", "hash_ids", _to_bool),
            ("STRICT_ASYNC", "strict_async", _to_bool),
            ("ROUTING_ENABLED", "routing_enabled", _to_bool),
            ("MAX_FAILURES", "max_failures", int),
            ("LOCAL_LIMIT_CAP", "local_limit_cap", int),
            ("CFG_RELOAD_S", "cfg_reload_secs", float),
            ("CONSUMER_DRAIN_S", "drain_secs", float),
            ("RETRY_ATTEMPTS", "retry_attempts", int),
            ("RETRY_BASE_S", "retry_base_secs", float),
            ("SQS_WAIT_TIME_S", "sqs_wait_time_secs", int),
            ("SQS_FAILOVER_S", "sqs_failover_secs", float),
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
    ("AC_PAUSE_S", "pause_secs", float),
    ("AC_BG_SHARE", "bg_share", float),
    ("AC_RAMP_CAP", "ramp_cap", int),
    ("AC_LEASE_TTL_S", "lease_ttl_secs", float),
)
_BREAKER_ENV: tuple[tuple[str, str, Callable[[str], Any]], ...] = (
    ("CB_THRESHOLD", "threshold", int),
    ("CB_WINDOW_S", "window_secs", float),
    ("CB_COOLDOWN_S", "cooldown_secs", float),
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
    """`LLMQ_TIMEOUT_<LANE>`, `LLMQ_TIMEOUT_EMBED_<LANE>` and `LLMQ_RESULT_DISPATCH_MS`."""
    for prefix, key in (("TIMEOUT_", "timeout_secs"), ("TIMEOUT_EMBED_", "embed_timeout_secs")):
        lanes: dict[Lane, float] = {}
        for lane in Lane:
            value = get(prefix + lane.value.upper())
            if value is None:
                continue
            try:
                lanes[lane] = float(value)
            except ValueError as exc:
                errors.append(f"{ENV_PREFIX}{prefix}{lane.value.upper()}: {exc}")
        if lanes:
            default = QueueConfig.model_fields[key].get_default(call_default_factory=True)
            raw[key] = {**default, **lanes}
    if (value := get("RESULT_DISPATCH_MS")) is not None:
        try:
            raw["result_dispatch_ms"] = {Lane(k): v for k, v in _int_map(value).items()}
        except ValueError as exc:
            errors.append(f"{ENV_PREFIX}RESULT_DISPATCH_MS: {exc}")


def _tuning_from_env(get: _Getter, raw: dict[str, object], errors: list[str]) -> None:
    """Nested `limiter` (`LLMQ_AC_*`, `LLMQ_AC_INITIAL` JSON) and `breaker` (`LLMQ_CB_*`)."""
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
        if field == "limiter" and (initial := get("AC_INITIAL")):
            try:
                values["initial"] = orjson.loads(initial)
            except orjson.JSONDecodeError as exc:
                errors.append(f"{ENV_PREFIX}AC_INITIAL: invalid JSON ({exc})")
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
