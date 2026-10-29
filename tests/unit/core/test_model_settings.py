"""Unit tests for per-provider / per-model settings: layers, validation, limiter and CLI parsing.

The live store and pod reload run against a real Valkey in the integration suite.
"""

from __future__ import annotations

from typing import Any

import orjson
import pytest
from pydantic import ValidationError

from taskiq_llm_queue.cli import _setting_values
from taskiq_llm_queue.config import (
    Lane,
    ModelConfig,
    ModelSettings,
    Overrides,
    ProviderOverrides,
    QueueConfig,
)
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.lifecycle import breaker_by_model, limiter_settings
from taskiq_llm_queue.model_settings import (
    SETTING_NAMES,
    Backoff,
    SettingsResolver,
    validate_scope,
)

MODELS = {
    "gpt": {"provider": "openai", "model_id": "gpt-oss-120b", "max_concurrency": 30},
    "llama": {"provider": "openai", "model_id": "llama", "limiter_initial": 8},
    "titan": {"provider": "bedrock", "model_id": "amazon.titan-embed-text-v2:0"},
}


def make(env: dict[str, Any] | None = None, live: dict[str, Any] | None = None) -> SettingsResolver:
    raw = {"LLMQ_MODELS": orjson.dumps(MODELS).decode()}
    for key, value in (env or {}).items():
        raw[key] = value if isinstance(value, str) else orjson.dumps(value).decode()
    config = QueueConfig.from_env(raw)
    return SettingsResolver(config, ModelSettings.model_validate(live or {}))


def test_six_layers_highest_wins_with_source() -> None:
    """Live model > live provider > env model > env provider > env global > built-in default."""
    env = {
        "LLMQ_ATTEMPT_TIMEOUT_SECS": "100",
        "LLMQ_SETTINGS": {
            "providers": {"openai": {"attempt_timeout_secs": 60}},
            "models": {"gpt": {"attempt_timeout_secs": 50}},
        },
    }
    live = {
        "providers": {"openai": {"attempt_timeout_secs": 40}},
        "models": {"gpt": {"attempt_timeout_secs": 30}},
    }
    assert make().resolve("gpt", "attempt_timeout_secs") == (180.0, "default")
    assert make({"LLMQ_ATTEMPT_TIMEOUT_SECS": "100"}).resolve("gpt", "attempt_timeout_secs") == (
        100.0,
        "env",
    )
    env_only = make(env)
    assert env_only.resolve("gpt", "attempt_timeout_secs") == (50.0, "env:model")
    assert env_only.resolve("llama", "attempt_timeout_secs") == (60.0, "env:provider")
    assert env_only.resolve("titan", "attempt_timeout_secs") == (100.0, "env")
    both = make(env, live)
    assert both.resolve("gpt", "attempt_timeout_secs") == (30.0, "live:model")
    assert both.resolve("llama", "attempt_timeout_secs") == (40.0, "live:provider")
    assert both.resolve("gpt@primary", "attempt_timeout_secs") == (30.0, "live:model")


def test_lane_settings_resolve_per_lane() -> None:
    """A lane override changes only that lane; the other lanes keep their own layer."""
    r = make(live={"models": {"gpt": {"timeout_secs": {"rt": 45}}}})
    assert r.resolve("gpt", "timeout_secs", Lane.RT) == (45.0, "live:model")
    assert r.resolve("gpt", "timeout_secs", Lane.BG) == (300.0, "default")
    described = r.describe("gpt")
    assert described["timeout_secs.rt"] == {"value": 45.0, "source": "live:model"}
    assert set(described) >= {"max_concurrency", "embed_timeout_secs.bulk", "retry_spread"}


def test_model_config_and_ac_json_count_as_env_model_layer() -> None:
    """`max_concurrency`, an explicit `limiter_initial` and `LLMQ_AC_*` JSON are env model."""
    r = make({"LLMQ_AC_MAX_BY_MODEL": {"llama": 12}})
    assert r.resolve("gpt", "max_concurrency") == (30, "env:model")
    assert r.resolve("llama", "max_concurrency") == (12, "env:model")
    assert r.resolve("titan", "max_concurrency") == (None, "default")
    assert r.resolve("llama", "limiter_initial") == (8, "env:model")
    assert r.resolve("gpt", "limiter_initial") == (20, "default")
    assert r.model_caps() == {"gpt": 30, "llama": 12}


def test_shared_provider_cap_and_limiter_settings() -> None:
    """A provider's shared cap becomes a fixed `@provider` limit; caps and bg_share per model."""
    r = make(
        {"LLMQ_SETTINGS": {"providers": {"openai": {"shared_max_concurrency": 40}}}},
        {"models": {"llama": {"bg_share": 0.5, "max_concurrency": 6}}},
    )
    assert r.shared_caps() == {"@openai": 40}
    assert r.shared_cap("bedrock") is None
    limits = limiter_settings(r.config, r)
    assert limits.max_for("@openai") == 40
    assert limits.start_for("@openai") == 40
    assert limits.max_for("llama") == 6
    assert limits.start_for("llama") == 6  # explicit start 8 clamped to the live cap
    assert limits.share_for(Lane.BG, "llama") == 0.5
    assert limits.share_for(Lane.BG, "gpt") == 0.8
    assert limits.share_for(Lane.RT, "llama") == 1.0
    live = make(live={"providers": {"openai": {"shared_max_concurrency": 9}}})
    assert live.shared_caps() == {"@openai": 9}


def test_breaker_by_model_lists_only_differing_aliases() -> None:
    """Only aliases whose threshold or cooldown differ from the global breaker get an entry."""
    r = make(live={"providers": {"bedrock": {"breaker_threshold": 2}}})
    by_model = breaker_by_model(r)
    assert set(by_model) == {"titan"}
    assert (by_model["titan"].threshold, by_model["titan"].cooldown_secs) == (2, 15.0)


def test_backoff_resolves_each_knob() -> None:
    """The retry curve mixes layers knob by knob (provider multiplier, global spread)."""
    r = make(
        {"LLMQ_RETRY_SPREAD": "2"},
        {"providers": {"openai": {"retry_multiplier": 3, "throttle_retry_max_secs": 5}}},
    )
    assert r.backoff("gpt") == Backoff(1.0, 3.0, 2.0, 30.0, 5.0)
    assert r.backoff("titan") == Backoff(1.0, 4.0, 2.0, 30.0, 10.0)


@pytest.mark.parametrize(
    "bad",
    [
        {"models": {"gpt": {"max_concurrency": 0}}},
        {"models": {"gpt": {"bg_share": 1.5}}},
        {"models": {"gpt": {"timeout_secs": {"rt": 0}}}},
        {"models": {"gpt": {"timeout_secs": {"fast": 3}}}},
        {"models": {"gpt": {"shared_max_concurrency": 3}}},
        {"models": {"gpt": {"nope": 1}}},
        {"providers": {"openai": {"retry_multiplier": 0.5}}},
    ],
)
def test_invalid_settings_are_rejected(bad: dict[str, Any]) -> None:
    """Every bad value fails validation, so it never reaches Valkey or a pod."""
    with pytest.raises(ValidationError):
        ModelSettings.model_validate(bad)


def test_bad_llmq_settings_env_is_a_boot_error() -> None:
    """`LLMQ_SETTINGS` is validated at boot like every other `LLMQ_*` value."""
    with pytest.raises(ConfigError, match="LLMQ_SETTINGS"):
        QueueConfig.from_env({"LLMQ_SETTINGS": "{not json"})
    with pytest.raises(ConfigError, match="max_concurrency"):
        QueueConfig.from_env({"LLMQ_SETTINGS": '{"models": {"a": {"max_concurrency": -1}}}'})


def test_with_values_merges_and_unsets() -> None:
    """`set` merges into an entry (lanes too); None unsets; an empty entry disappears."""
    s = ModelSettings().with_values("models", "gpt", {"max_concurrency": 10})
    s = s.with_values("models", "gpt", {"timeout_secs": {"rt": 60}})
    s = s.with_values("models", "gpt", {"timeout_secs": {"bg": 200}})
    assert s.models["gpt"] == Overrides(
        max_concurrency=10, timeout_secs={Lane.RT: 60, Lane.BG: 200}
    )
    s = s.with_values("models", "gpt", {"timeout_secs": {"rt": None}, "max_concurrency": None})
    assert s.models["gpt"] == Overrides(timeout_secs={Lane.BG: 200})
    s = s.without("models", "gpt", ["timeout_secs"])
    assert "gpt" not in s.models
    p = ModelSettings().with_values("providers", "openai", {"shared_max_concurrency": 5})
    assert p.providers["openai"] == ProviderOverrides(shared_max_concurrency=5)
    assert p.without("providers", "openai").providers == {}


def test_cli_setting_values_parse_names_lanes_and_types() -> None:
    """`NAME=VALUE` parses JSON scalars and lane names; unknown names or lanes are refused."""
    values = _setting_values(
        ["max_concurrency=30", "timeout_secs.rt=60", "bg_share=0.5"], SETTING_NAMES
    )
    assert values == {"max_concurrency": 30, "timeout_secs": {Lane.RT: 60}, "bg_share": 0.5}
    assert _setting_values(["timeout_secs.rt"], SETTING_NAMES, unset=True) == {
        "timeout_secs": {Lane.RT: None}
    }
    for bad in (["nope=1"], ["timeout_secs=5"], ["timeout_secs.fast=5"], ["max_concurrency"]):
        with pytest.raises(ConfigError):
            _setting_values(bad, SETTING_NAMES)
    with pytest.raises(ConfigError):
        _setting_values(["shared_max_concurrency=3"], SETTING_NAMES)


def test_validate_scope_knows_aliases_and_providers() -> None:
    """The CLI only edits configured aliases and providers that serve one of them."""
    config = make().config
    validate_scope(config, "models", "gpt")
    validate_scope(config, "providers", "bedrock")
    with pytest.raises(ValueError, match="model alias"):
        validate_scope(config, "models", "nope")
    with pytest.raises(ValueError, match="provider"):
        validate_scope(config, "providers", "anthropic")


def test_lane_caps_resolve_and_become_fixed_limiter_buckets() -> None:
    """F19: `lane_max_concurrency.<lane>` per provider or model -> `<alias>#<lane>` fixed caps."""
    from taskiq_llm_queue.lifecycle import limiter_settings

    config = QueueConfig(
        models={
            "a": ModelConfig(provider="bedrock", model_id="a"),
            "b": ModelConfig(provider="openai", model_id="b"),
        },
        settings_default=ModelSettings.model_validate(
            {
                "providers": {"openai": {"lane_max_concurrency": {"bulk": 4}}},
                "models": {"a": {"lane_max_concurrency": {"bulk": 2, "bg": 5}}},
            }
        ),
    )
    resolver = SettingsResolver(config)
    assert resolver.lane_caps() == {"a#bg": 5, "a#bulk": 2, "b#bulk": 4}
    assert resolver.resolve("a", "lane_max_concurrency", Lane.RT) == (None, "default")
    limits = limiter_settings(config, resolver)
    assert (limits.start_for("a#bulk"), limits.max_for("a#bulk")) == (2, 2)
    assert limits.max_for("b#bulk") == 4
    with pytest.raises(ValueError, match="lane values"):
        ModelSettings.model_validate({"models": {"a": {"lane_max_concurrency": {"bulk": 0}}}})
