"""Fallback order for one call: same model on other targets first, other models only on `chain`.

Pure logic over `QueueConfig` and the live `FallbackConfig` (§8, D50, D72). Embeddings never
switch model (vectors from another model are incompatible). The worker walks the steps in order
and checks limiter headroom itself where a step says `require_headroom`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from taskiq_llm_queue.errors import ConfigError

if TYPE_CHECKING:
    from taskiq_llm_queue.config import QueueConfig
    from taskiq_llm_queue.envelope import FallbackPolicy, Op


class ChainStep(BaseModel):
    """One other model in a chain (`alias` or `alias@target`); a plain string is accepted."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    model: str = Field(min_length=1)
    require_headroom: bool = False


class FallbackConfig(BaseModel):
    """Live fallback chains stored as JSON at `KeySpace.fallback` (D72).

    `chains` maps a primary alias to the other models tried under policy `chain`;
    `skip_targets` lists targets not usable yet per alias (e.g. APAC before sign-off, T1.0b).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    chains: dict[str, tuple[ChainStep, ...]] = Field(default_factory=dict)
    skip_targets: dict[str, frozenset[str]] = Field(default_factory=dict)

    @field_validator("chains", mode="before")
    @classmethod
    def _strings_to_steps(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        return {
            alias: [{"model": s} if isinstance(s, str) else s for s in steps]
            if isinstance(steps, list | tuple)
            else steps
            for alias, steps in value.items()
        }

    @model_validator(mode="after")
    def _no_self_reference(self) -> FallbackConfig:
        loops = [a for a, steps in self.chains.items() if any(_alias(s.model) == a for s in steps)]
        if loops:
            raise ValueError(f"chains must not list the primary model itself: {loops}")
        return self


@dataclass(frozen=True, slots=True)
class FallbackStep:
    """One place to try: model alias and target, whether it changes model, headroom rule."""

    alias: str
    target: str
    switched_model: bool = False
    require_headroom: bool = False


def _alias(model: str) -> str:
    return model.partition("@")[0]


def resolve_fallback(
    config: QueueConfig,
    model: str,
    *,
    policy: FallbackPolicy = "same_model",
    op: Op = "converse",
    live: FallbackConfig | None = None,
) -> tuple[FallbackStep, ...]:
    """Ordered steps for `model` (`alias[@target]`): the requested target, then its other targets.

    With policy `chain` (and never for embeddings) the chain's models follow, each primary
    target first. Unknown aliases or targets in the live chain are skipped, not raised, because
    live config may lag the deployed `QueueConfig`; the requested model itself must exist.
    """
    live = live or FallbackConfig()
    alias, cfg, first = config.resolve(model)
    skip = live.skip_targets.get(alias, frozenset())
    steps = [FallbackStep(alias, first)]
    steps += [FallbackStep(alias, t) for t in cfg.targets if t != first and t not in skip]
    if policy != "chain" or op == "embeddings":
        return tuple(steps)
    seen = {alias}
    for step in live.chains.get(alias, ()):
        other, _, pinned = step.model.partition("@")
        if other in seen:
            continue
        try:
            _, other_cfg, _ = config.resolve(step.model)
        except ConfigError:
            continue
        seen.add(other)
        other_skip = live.skip_targets.get(other, frozenset())
        targets = [pinned] if pinned else [t for t in other_cfg.targets if t not in other_skip]
        steps += [FallbackStep(other, t, True, step.require_headroom) for t in targets]
    return tuple(steps)
