"""Typed configuration: models and their targets, queues, Redis/SQS endpoints, timeouts.

`QueueConfig` is the single object an app builds (directly or with `from_env`) and passes to
`LLMQueue`. Validation runs once at construction, so bad config fails at boot, not mid-request.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from enum import StrEnum
from typing import Literal

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


class QueueConfig(BaseModel):
    """Everything `LLMQueue` needs; immutable after construction.

    Timeouts are per lane; TTLs follow the plan's rule (state keys outlive the longest lane
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
        ):
            value = get(name)
            if value is None:
                continue
            try:
                raw[key] = conv(value)
            except ValueError as exc:
                errors.append(f"{ENV_PREFIX}{name}: {exc}")
        for name, key in (("MODELS", "models"), ("PROVIDERS", "providers")):
            value = get(name)
            if value:
                try:
                    raw[key] = json.loads(value)
                except json.JSONDecodeError as exc:
                    errors.append(f"{ENV_PREFIX}{name}: invalid JSON ({exc})")
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


def _to_bool(value: str) -> bool:
    """Parse the usual env-var spellings of a boolean; anything else is an error."""
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"not a boolean: {value!r}")
