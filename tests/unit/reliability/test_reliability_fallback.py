"""Fallback resolution matrix: targets first, other models only on `chain`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from taskiq_llm_queue.config import ModelConfig, QueueConfig, Target
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.reliability import FallbackConfig, FallbackStep, resolve_fallback


def _model(*targets: str) -> ModelConfig:
    return ModelConfig(
        provider="bedrock",
        model_id="x",
        targets={t: Target(region=t) for t in targets},
    )


CONFIG = QueueConfig(
    models={
        "gpt120b": _model("primary", "apac"),
        "gpt20b": _model("primary", "apac"),
        "titan": _model("primary", "apac"),
        "solo": _model("primary"),
    }
)
LIVE = FallbackConfig.model_validate(
    {
        "chains": {
            "gpt120b": ["gpt20b"],
            "gpt20b": [{"model": "gpt120b", "require_headroom": True}],
            "titan": ["gpt20b"],
            "solo": ["unknown", "gpt20b@apac", "gpt20b"],
        }
    }
)


def pairs(steps: tuple[FallbackStep, ...]) -> list[tuple[str, str]]:
    return [(s.alias, s.target) for s in steps]


@pytest.mark.parametrize(
    ("model", "policy", "op", "expected"),
    [
        ("gpt120b", "same_model", "converse", [("gpt120b", "primary"), ("gpt120b", "apac")]),
        (
            "gpt120b",
            "chain",
            "converse",
            [
                ("gpt120b", "primary"),
                ("gpt120b", "apac"),
                ("gpt20b", "primary"),
                ("gpt20b", "apac"),
            ],
        ),
        ("gpt120b@apac", "same_model", "chat", [("gpt120b", "apac"), ("gpt120b", "primary")]),
        ("titan", "chain", "embeddings", [("titan", "primary"), ("titan", "apac")]),
        ("solo", "same_model", "converse", [("solo", "primary")]),
        ("solo", "chain", "converse", [("solo", "primary"), ("gpt20b", "apac")]),
    ],
)
def test_matrix(model: str, policy: str, op: str, expected: list[tuple[str, str]]) -> None:
    steps = resolve_fallback(CONFIG, model, policy=policy, op=op, live=LIVE)  # type: ignore[arg-type]
    assert pairs(steps) == expected


def test_switched_model_and_headroom_flags() -> None:
    steps = resolve_fallback(CONFIG, "gpt20b", policy="chain", live=LIVE)
    assert [s.switched_model for s in steps] == [False, False, True, True]
    assert [s.require_headroom for s in steps] == [False, False, True, True]


def test_skip_targets_drops_unapproved_targets() -> None:
    """Targets listed in skip_targets (e.g. not yet approved) are skipped, for primary and chain."""
    live = LIVE.model_copy(update={"skip_targets": {"gpt120b": {"apac"}, "gpt20b": {"apac"}}})
    steps = resolve_fallback(CONFIG, "gpt120b", policy="chain", live=live)
    assert pairs(steps) == [("gpt120b", "primary"), ("gpt20b", "primary")]


def test_explicitly_requested_target_is_kept_even_if_skipped() -> None:
    live = FallbackConfig.model_validate({"skip_targets": {"gpt120b": ["apac"]}})
    steps = pairs(resolve_fallback(CONFIG, "gpt120b@apac", live=live))
    assert steps == [("gpt120b", "apac"), ("gpt120b", "primary")]


def test_without_live_config_only_same_model() -> None:
    steps = resolve_fallback(CONFIG, "gpt120b", policy="chain")
    assert pairs(steps) == [("gpt120b", "primary"), ("gpt120b", "apac")]


def test_unknown_requested_model_raises() -> None:
    with pytest.raises(ConfigError):
        resolve_fallback(CONFIG, "nope")


def test_chain_must_not_reference_itself() -> None:
    with pytest.raises(ValidationError, match="primary model itself"):
        FallbackConfig.model_validate({"chains": {"a": ["a@apac"]}})


def test_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        FallbackConfig.model_validate({"chain": {}})
