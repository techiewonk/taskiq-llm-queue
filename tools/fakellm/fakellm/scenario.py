"""Scenario model: per model x target behaviour, defaults and timed phases.

A scenario is loaded from YAML or JSON at boot (bad keys raise `ScenarioError` there),
replaced through `PUT /admin/scenario` and patched through `PATCH /admin/models/{key}`.
`Scenario.behaviour(key, elapsed_s)` resolves the effective `Behaviour` for one call.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fakellm.errors import ERRORS

_TOOL_MODES = frozenset({"echo", "none"})
_RATE_FIELDS = ("throttle_rate", "error_rate_5xx", "timeout_rate")
_FIELDS = frozenset(
    {
        "latency_ms",
        "max_concurrency",
        "rpm",
        *_RATE_FIELDS,
        "tool_calls",
        "dims",
        "force_error",
        "retry_after_s",
    }
)


class ScenarioError(ValueError):
    """Raised for an invalid scenario file, scenario body or behaviour patch."""


@dataclass(frozen=True, slots=True)
class Behaviour:
    """Effective behaviour of one model x target key at one instant.

    `max_concurrency` / `rpm` of None mean unlimited; `force_error` names an `ERRORS`
    entry returned on every call (handy for SDK error-parse tests and outage drills).
    """

    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    max_concurrency: int | None = None
    rpm: int | None = None
    throttle_rate: float = 0.0
    error_rate_5xx: float = 0.0
    timeout_rate: float = 0.0
    tool_calls: str = "echo"
    dims: int = 1024
    force_error: str | None = None
    retry_after_s: float = 1.0


def _positive_int(name: str, value: Any, *, nullable: bool) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ScenarioError(f"{name} must be a positive integer, got {value!r}")
    return value


def _number(name: str, value: Any, *, upper: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ScenarioError(f"{name} must be a non-negative number, got {value!r}")
    if upper is not None and value > upper:
        raise ScenarioError(f"{name} must be <= {upper}, got {value!r}")
    return float(value)


def build_behaviour(block: dict[str, Any]) -> Behaviour:
    """Validate a merged behaviour mapping and turn it into a `Behaviour`.

    Used at scenario load (fail fast at boot) and on every call after phases apply.
    """
    unknown = set(block) - _FIELDS
    if unknown:
        raise ScenarioError(f"unknown behaviour field(s): {sorted(unknown)}")
    latency = block.get("latency_ms", {}) or {}
    if not isinstance(latency, dict) or set(latency) - {"p50", "p95"}:
        raise ScenarioError(f"latency_ms must be a mapping of p50/p95, got {latency!r}")
    p50 = _number("latency_ms.p50", latency.get("p50", 0))
    p95 = _number("latency_ms.p95", latency.get("p95", p50))
    tool_calls = block.get("tool_calls", "echo")
    if tool_calls not in _TOOL_MODES:
        raise ScenarioError(f"tool_calls must be one of {sorted(_TOOL_MODES)}")
    force_error = block.get("force_error")
    if force_error is not None and force_error not in ERRORS:
        raise ScenarioError(f"force_error must be one of {sorted(ERRORS)}, got {force_error!r}")
    rates = {name: _number(name, block.get(name, 0.0), upper=1.0) for name in _RATE_FIELDS}
    return Behaviour(
        latency_p50_ms=p50,
        latency_p95_ms=max(p50, p95),
        max_concurrency=_positive_int(
            "max_concurrency", block.get("max_concurrency"), nullable=True
        ),
        rpm=_positive_int("rpm", block.get("rpm"), nullable=True),
        tool_calls=str(tool_calls),
        dims=_positive_int("dims", block.get("dims", 1024), nullable=False) or 1024,
        force_error=force_error,
        retry_after_s=_number("retry_after_s", block.get("retry_after_s", 1.0)),
        **rates,
    )


@dataclass(frozen=True, slots=True)
class Phase:
    """A timed patch: from `at_s` seconds after the last reset, `patch` applies to `model`."""

    at_s: float
    model: str
    patch: dict[str, Any]


@dataclass(slots=True)
class Scenario:
    """Whole scenario: seed, defaults, per-key blocks and phases (mutable via the admin API)."""

    seed: int | None = None
    defaults: dict[str, Any] = field(default_factory=dict)
    models: dict[str, dict[str, Any]] = field(default_factory=dict)
    phases: list[Phase] = field(default_factory=list)
    strict_models: bool = False

    @classmethod
    def from_mapping(cls, raw: Any) -> Scenario:
        """Parse and validate a scenario mapping; every block is checked against defaults."""
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise ScenarioError("scenario must be a mapping")
        unknown = set(raw) - {"seed", "defaults", "models", "phases", "strict_models"}
        if unknown:
            raise ScenarioError(f"unknown scenario field(s): {sorted(unknown)}")
        seed = raw.get("seed")
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise ScenarioError(f"seed must be an integer, got {seed!r}")
        defaults = dict(raw.get("defaults") or {})
        models_raw = raw.get("models") or {}
        if not isinstance(models_raw, dict):
            raise ScenarioError("models must be a mapping of key -> behaviour")
        models = {str(k): dict(v or {}) for k, v in models_raw.items()}
        phases: list[Phase] = []
        for item in raw.get("phases") or []:
            if not isinstance(item, dict) or set(item) != {"at_s", "model", "set"}:
                raise ScenarioError(f"phase needs exactly at_s, model, set: {item!r}")
            phases.append(
                Phase(_number("at_s", item["at_s"]), str(item["model"]), dict(item["set"]))
            )
        phases.sort(key=lambda p: p.at_s)
        scenario = cls(seed, defaults, models, phases, bool(raw.get("strict_models", False)))
        build_behaviour(defaults)
        for key in {*models, *(p.model for p in phases)}:
            scenario.merged(key, float("inf"))
        return scenario

    def to_mapping(self) -> dict[str, Any]:
        """Render back to the file shape (what `GET /admin/scenario` returns)."""
        return {
            "seed": self.seed,
            "strict_models": self.strict_models,
            "defaults": self.defaults,
            "models": self.models,
            "phases": [{"at_s": p.at_s, "model": p.model, "set": p.patch} for p in self.phases],
        }

    def merged(self, key: str, elapsed_s: float) -> Behaviour:
        """Behaviour for `key`: defaults, then its block, then every phase already due."""
        block = {**self.defaults, **self.models.get(key, {})}
        for phase in self.phases:
            if phase.at_s > elapsed_s:
                break
            if phase.model == key:
                block.update(phase.patch)
        return build_behaviour(block)

    def patch_model(self, key: str, patch: dict[str, Any]) -> Behaviour:
        """Merge `patch` into the block for `key` (a None value drops that override)."""
        block = {**self.models.get(key, {}), **patch}
        block = {k: v for k, v in block.items() if v is not None}
        behaviour = build_behaviour({**self.defaults, **block})
        self.models[key] = block
        return behaviour


def load_scenario(path: str | Path) -> Scenario:
    """Load a scenario file: `.json` with the stdlib, anything else as YAML (needs PyYAML)."""
    text = Path(path).read_text(encoding="utf-8")
    if str(path).endswith(".json"):
        return Scenario.from_mapping(json.loads(text))
    try:
        import yaml  # type: ignore[import-untyped]  # no types-PyYAML in the venv
    except ImportError as exc:  # pragma: no cover - the image and venv both ship PyYAML
        raise ScenarioError("YAML scenarios need PyYAML: pip install pyyaml") from exc
    return Scenario.from_mapping(yaml.safe_load(text))
