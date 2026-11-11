"""Resolve a `Registry`: the deployments of each model and every setting's effective value.

A setting is taken from the most specific level that sets it: deployment → model → account →
`defaults` → library default. Object settings marked `merge` combine key by key. Used by the
validator, the admission script inputs and `llm-queue settings get`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from taskiq_llm_queue.registry.schema import ModelEntry, Registry
from taskiq_llm_queue.registry.settings import (
    CHAT_INFERENCE,
    EMBED_INFERENCE,
    FALLBACK_AFTER_SECS,
    MODEL_FALLBACK_AFTER_SECS,
    SETTINGS,
    A,
    D,
    M,
    R,
)

LIBRARY = "library"
_LEVEL_LABEL = {R: "deployment", M: "model", A: "account", D: "defaults"}


@dataclass(frozen=True, slots=True)
class Deployment:
    """One model in one account and region (or inference profile): what has a budget.

    `key` (`account/name`) names it in keys, metrics and status.
    """

    model: str
    account: str
    name: str
    group: str
    region: str
    profile_id: str | None = None
    routes_to: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """`account/name`, unique inside a model."""
        return f"{self.account}/{self.name}"


@dataclass(frozen=True, slots=True)
class Effective:
    """A setting's value and the level it came from (`deployment` … `defaults`, `library`)."""

    value: Any
    level: str


class ResolvedRegistry:
    """Read view over a `Registry`: deployments per model and effective settings.

    Pure and cheap to build; rebuilt on every registry reload.
    """

    def __init__(self, registry: Registry) -> None:
        """Index the registry's models and deployments."""
        self.registry = registry
        self._deployments: dict[str, tuple[Deployment, ...]] = {}
        self._entries: dict[tuple[str, str], Mapping[str, Any]] = {}
        for alias, model in registry.models.items():
            found: list[Deployment] = []
            for group in ("confirmed", "fallback"):
                for account, entries in model.deployments.items():
                    for entry in entries:
                        if entry.group != group:
                            continue
                        deployment = Deployment(
                            model=alias,
                            account=account,
                            name=entry.name,
                            group=entry.group,
                            region=entry.region,
                            profile_id=entry.profile_id,
                            routes_to=entry.routes_to,
                        )
                        found.append(deployment)
                        self._entries[(alias, deployment.key)] = entry.settings
            self._deployments[alias] = tuple(found)

    def model(self, model: str) -> ModelEntry:
        """The entry of a model named by alias or provider model id."""
        return self.registry.models[self.registry.alias_of(model)]

    def deployments(self, model: str, group: str | None = None) -> tuple[Deployment, ...]:
        """Deployments of a model, confirmed first, in config order; optionally one group."""
        found = self._deployments[self.registry.alias_of(model)]
        return found if group is None else tuple(d for d in found if d.group == group)

    def _layers(self, alias: str, deployment: str | None) -> list[tuple[str, Mapping[str, Any]]]:
        """The settings of each level, most specific first."""
        model = self.registry.models[alias]
        layers: list[tuple[str, Mapping[str, Any]]] = []
        account = None
        if deployment is not None:
            if (alias, deployment) not in self._entries:
                raise KeyError(f"{alias} has no deployment {deployment}")
            layers.append((R, self._entries[(alias, deployment)]))
            account = deployment.split("/", 1)[0]
        layers.append((M, model.settings))
        if account is not None:
            layers.append((A, self.registry.accounts[account].settings))
        layers.append((D, self.registry.defaults))
        return layers

    def effective(self, model: str, name: str, deployment: str | None = None) -> Effective:
        """The value of setting `name` for a model (and one deployment), with its level.

        Without `deployment`, the deployment and account levels are skipped.
        """
        spec = SETTINGS[name]
        alias = self.registry.alias_of(model)
        entry = self.registry.models[alias]
        layers = self._layers(alias, deployment)
        if spec.merge:
            merged: dict[str, Any] = dict(spec.default or {})
            level = LIBRARY
            for code, values in reversed(layers):
                if name in values:
                    part = values[name]
                    if name == "inference" and code in (D, A):
                        # Shared defaults apply only to models of the matching type.
                        own = CHAT_INFERENCE if entry.type == "chat" else EMBED_INFERENCE
                        part = {k: v for k, v in part.items() if k in own}
                        if not part:
                            continue
                    merged.update(part)
                    level = _LEVEL_LABEL[code]
            return Effective(_complete(name, merged), level)
        for code, values in layers:
            if name in values:
                return Effective(_complete(name, values[name]), _LEVEL_LABEL[code])
        return Effective(self._library_default(entry, name, deployment), LIBRARY)

    def value(self, model: str, name: str, deployment: str | None = None) -> Any:
        """Shortcut for `effective(...).value`."""
        return self.effective(model, name, deployment).value

    def _library_default(self, entry: ModelEntry, name: str, deployment: str | None) -> Any:
        spec = SETTINGS[name]
        if name == "rate_limits" and entry.type == "embedding":
            # A `tpm` on an embedding model turns token limiting on (plan §3.19).
            has_tpm = self.effective(entry.alias, "tpm", deployment).value is not None
            return "both" if has_tpm else "rpm"
        if spec.by_type is not None:
            return spec.by_type(entry.type)
        return spec.default

    def table(self, model: str, deployment: str | None = None) -> dict[str, Effective]:
        """Every setting of a model (and deployment) with its value and level (CLI view)."""
        alias = self.registry.alias_of(model)
        return {name: self.effective(alias, name, deployment) for name in sorted(SETTINGS)}

    def queue_name(self, model: str) -> str:
        """The model's normal queue name, without the environment prefix."""
        alias = self.registry.alias_of(model)
        named = self.registry.models[alias].settings.get("queue", {}).get("name")
        return str(named) if named else f"llm-{alias}"

    def residency(self, model: str) -> tuple[str, ...] | None:
        """The regions a model may reach (its own list, else the top-level one), None = any."""
        entry = self.model(model)
        return entry.residency if entry.residency is not None else self.registry.residency

    def outside_residency(self, deployment: Deployment) -> bool:
        """True when a deployment may not be used under the residency list."""
        allowed = self.residency(deployment.model)
        if allowed is None:
            return False
        if deployment.profile_id is not None:
            return not deployment.routes_to or not set(deployment.routes_to) <= set(allowed)
        return deployment.region not in allowed


def _complete(name: str, value: Any) -> Any:
    """Fill the optional fields of the two fallback rules and `sticky`."""
    if name == "fallback" and isinstance(value, dict):
        return {"after_secs": FALLBACK_AFTER_SECS, "regions": "account", **value}
    if name == "model_fallback" and isinstance(value, dict):
        models = [
            {"model": m, "require_headroom": False}
            if isinstance(m, str)
            else {"require_headroom": False, **m}
            for m in value.get("models", [])
        ]
        return {"after_secs": MODEL_FALLBACK_AFTER_SECS, **value, "models": models}
    if name == "sticky" and isinstance(value, dict):
        return {"idle_ttl_secs": 1800, **value}
    return value
