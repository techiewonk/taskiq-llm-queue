"""Per-provider and per-model settings: bucket size, limiter start, timeouts, retries, breaker.

Each setting resolves through six layers, highest first: live model, live provider (both in
Valkey at `KeySpace.model_settings`, edited with `llm-queue settings`), env model, env provider
(`LLMQ_SETTINGS` JSON, plus `ModelConfig` / `LLMQ_AC_*` per-model values), env global
(`LLMQ_*`) and the built-in default. Pods reload the live layers every `cfg_reload_secs`.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from taskiq_llm_queue.config import (
    LANE_SETTINGS,
    Lane,
    ModelConfig,
    ModelSettings,
    Overrides,
    ProviderOverrides,
    QueueConfig,
)
from taskiq_llm_queue.reliability.config_store import LiveConfigStore
from taskiq_llm_queue.reliability.rate import RateLimits

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.keys import KeySpace

Source = Literal["live:model", "live:provider", "env:model", "env:provider", "env", "default"]
# Provider prefix of the shared-cap limiter key `{m:@<provider>:shared}`; aliases never hold "@".
SHARED_PREFIX = "@"
SHARED_TARGET = "shared"
# F19 lane cap limiter alias `<alias>#<lane>` (per target, like the model's own key).
LANE_SEP = "#"


SETTING_NAMES: tuple[str, ...] = tuple(Overrides.model_fields)
PROVIDER_SETTING_NAMES: tuple[str, ...] = tuple(ProviderOverrides.model_fields)
_EMPTY = Overrides()
_EMPTY_PROVIDER = ProviderOverrides()


@dataclass(frozen=True, slots=True)
class Backoff:
    """Retry sleep curve: retry n (0-based) waits a random time in [low, low * spread].

    `low = base_secs * multiplier^n`; one sleep is capped at `max_secs`, or at
    `throttle_max_secs` after a throttle. Defaults give 1-2.5 s, 4-10 s, 16-30 s.
    """

    base_secs: float = 1.0
    multiplier: float = 4.0
    spread: float = 2.5
    max_secs: float = 30.0
    throttle_max_secs: float = 10.0


@functools.cache
def _builtin() -> QueueConfig:
    return QueueConfig()


def lane_scope(alias: str, lane: Lane) -> str:
    """Limiter alias of one model's lane cap, e.g. `gpt20b#bulk` (F19)."""
    return f"{alias}{LANE_SEP}{lane.value}"


def _global(config: QueueConfig, name: str, lane: Lane | None) -> Any:
    """Env-global (or built-in) value of `name`; per-model-only settings have none."""
    if name == "lane_max_concurrency":
        return None
    if name in LANE_SETTINGS:
        return getattr(config, name)[lane]
    if name == "bg_share":
        return config.limiter.bg_share
    if name in ("breaker_threshold", "breaker_cooldown_secs"):
        return getattr(config.breaker, name.removeprefix("breaker_"))
    if name == "limiter_initial":
        return ModelConfig.model_fields["limiter_initial"].default
    if name in ("max_concurrency", "rpm", "tpm", "rate_burst"):
        return None  # per model / provider only
    return getattr(config, name)


def _env_model(config: QueueConfig, alias: str, name: str, lane: Lane | None) -> Any:
    """Env model layer: `LLMQ_SETTINGS.models`, then `LLMQ_AC_*` JSON, then `ModelConfig`."""
    value = config.settings_default.models.get(alias, _EMPTY).get(name, lane)
    if value is not None:
        return value
    model = config.models.get(alias)
    if name == "max_concurrency":
        cap = config.limiter.max_by_model.get(alias)
        return cap if cap is not None else (model.max_concurrency if model else None)
    if name == "limiter_initial":
        start = config.limiter.initial.get(alias)
        if start is None and model is not None and "limiter_initial" in model.model_fields_set:
            start = model.limiter_initial
        return start
    return None


@dataclass(frozen=True, slots=True)
class SettingsResolver:
    """Effective settings of every model alias for one `QueueConfig` and one live value.

    Immutable: a reload builds a new resolver, so a call reads one consistent snapshot.
    """

    config: QueueConfig
    live: ModelSettings = field(default_factory=ModelSettings)

    def provider_of(self, alias: str) -> str | None:
        """Provider name of a configured alias (None for an unknown alias)."""
        model = self.config.models.get(alias.partition("@")[0])
        return model.provider if model else None

    def resolve(self, alias: str, name: str, lane: Lane | None = None) -> tuple[Any, Source]:
        """`(value, source)` of one setting for `alias`, walking the six layers."""
        alias = alias.partition("@")[0]
        provider = self.provider_of(alias)
        live_provider = self.live.providers.get(provider, _EMPTY_PROVIDER) if provider else None
        env_provider = (
            self.config.settings_default.providers.get(provider, _EMPTY_PROVIDER)
            if provider
            else None
        )
        layers: tuple[tuple[Source, Callable[[], Any]], ...] = (
            ("live:model", lambda: self.live.models.get(alias, _EMPTY).get(name, lane)),
            ("live:provider", lambda: live_provider.get(name, lane) if live_provider else None),
            ("env:model", lambda: _env_model(self.config, alias, name, lane)),
            ("env:provider", lambda: env_provider.get(name, lane) if env_provider else None),
        )
        for source, read in layers:
            value = read()
            if value is not None:
                return value, source
        value = _global(self.config, name, lane)
        default = _global(_builtin(), name, lane)
        return value, "default" if value == default else "env"

    def value(self, alias: str, name: str, lane: Lane | None = None) -> Any:
        """Effective value of one setting for `alias` (see `resolve`)."""
        return self.resolve(alias, name, lane)[0]

    def backoff(self, alias: str) -> Backoff:
        """Retry sleep curve of `alias` (each knob resolved on its own)."""
        return Backoff(
            base_secs=self.value(alias, "retry_base_secs"),
            multiplier=self.value(alias, "retry_multiplier"),
            spread=self.value(alias, "retry_spread"),
            max_secs=self.value(alias, "retry_max_secs"),
            throttle_max_secs=self.value(alias, "throttle_retry_max_secs"),
        )

    def shared_cap(self, provider: str) -> int | None:
        """Provider-wide cap shared by all its models: live, else `LLMQ_SETTINGS`, else None."""
        for layer in (self.live.providers, self.config.settings_default.providers):
            cap = layer.get(provider, _EMPTY_PROVIDER).shared_max_concurrency
            if cap is not None:
                return cap
        return None

    def rate_limits(self, alias: str) -> RateLimits:
        """Effective RPM / TPM budget of one alias's targets (F17)."""
        return RateLimits(
            rpm=self.value(alias, "rpm"),
            tpm=self.value(alias, "tpm"),
            window=self.value(alias, "rate_window"),
            burst=self.value(alias, "rate_burst"),
            rt_share=self.value(alias, "rt_rate_share"),
        )

    def shared_rate(self, provider: str) -> tuple[int | None, int | None]:
        """Provider-wide `(shared_rpm, shared_tpm)`: live, else `LLMQ_SETTINGS`, each on its own."""
        out: list[int | None] = []
        for name in ("shared_rpm", "shared_tpm"):
            found = None
            for layer in (self.live.providers, self.config.settings_default.providers):
                found = getattr(layer.get(provider, _EMPTY_PROVIDER), name)
                if found is not None:
                    break
            out.append(found)
        return out[0], out[1]

    def lane_caps(self) -> dict[str, int]:
        """`<alias>#<lane>` limiter alias -> cap, for every model lane that has one (F19)."""
        out: dict[str, int] = {}
        for alias in self.config.models:
            for lane in Lane:
                cap = self.value(alias, "lane_max_concurrency", lane)
                if cap is not None:
                    out[lane_scope(alias, lane)] = cap
        return out

    def shared_caps(self) -> dict[str, int]:
        """`@<provider>` limiter alias -> shared cap, for every provider that has one."""
        providers = {m.provider for m in self.config.models.values()}
        caps = {p: self.shared_cap(p) for p in providers}
        return {f"{SHARED_PREFIX}{p}": cap for p, cap in caps.items() if cap is not None}

    def model_caps(self) -> dict[str, int]:
        """Alias -> effective `max_concurrency` for every alias that has one."""
        caps = {alias: self.value(alias, "max_concurrency") for alias in self.config.models}
        return {alias: cap for alias, cap in caps.items() if cap is not None}

    def providers(self) -> list[str]:
        """Providers used by at least one configured model, sorted."""
        return sorted({m.provider for m in self.config.models.values()})

    def describe_provider(self, provider: str) -> dict[str, Any]:
        """Provider view for `settings get --provider`: its overrides, shared cap and models."""
        live = self.live.providers.get(provider)
        env = self.config.settings_default.providers.get(provider)
        return {
            "models": sorted(a for a, m in self.config.models.items() if m.provider == provider),
            "shared_max_concurrency": self.shared_cap(provider),
            "shared_rpm": self.shared_rate(provider)[0],
            "shared_tpm": self.shared_rate(provider)[1],
            "live": live.model_dump(mode="json", exclude_defaults=True) if live else {},
            "env": env.model_dump(mode="json", exclude_defaults=True) if env else {},
        }

    def describe(self, alias: str) -> dict[str, dict[str, Any]]:
        """Every setting of `alias` as `{name[.lane]: {"value", "source"}}` (CLI `settings get`)."""
        out: dict[str, dict[str, Any]] = {}
        for name in SETTING_NAMES:
            lanes: tuple[Lane | None, ...] = tuple(Lane) if name in LANE_SETTINGS else (None,)
            for lane in lanes:
                value, source = self.resolve(alias, name, lane)
                key = f"{name}.{lane.value}" if lane is not None else name
                out[key] = {"value": value, "source": source}
        return out


def validate_scope(config: QueueConfig, scope: Literal["providers", "models"], key: str) -> None:
    """Raise `ValueError` unless `key` is a configured alias (models) or a used provider."""
    known = (
        set(config.models) if scope == "models" else {m.provider for m in config.models.values()}
    )
    if key not in known:
        kind = "model alias" if scope == "models" else "provider"
        raise ValueError(f"{key!r} is not a configured {kind}; known: {sorted(known)}")


def model_settings_store(
    client: redis.asyncio.Redis, keys: KeySpace, default: ModelSettings | None = None
) -> LiveConfigStore[ModelSettings]:
    """Live store for per-provider / per-model overrides at `KeySpace.model_settings`."""
    return LiveConfigStore(
        client, keys.model_settings, ModelSettings, default or ModelSettings(), audit_key=keys.audit
    )


__all__ = [
    "LANE_SEP",
    "LANE_SETTINGS",
    "PROVIDER_SETTING_NAMES",
    "SETTING_NAMES",
    "Backoff",
    "ModelSettings",
    "Overrides",
    "ProviderOverrides",
    "SettingsResolver",
    "lane_scope",
    "model_settings_store",
    "validate_scope",
]
