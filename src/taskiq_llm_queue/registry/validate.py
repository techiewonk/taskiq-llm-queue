"""Cross-field checks of a registry and the one entry point that loads it.

`load_registry` parses (`schema.build`), resolves and checks the document and raises one
`RegistryError` listing every problem. The checks here need effective values, so they run on
the resolved registry (plan §3.9 and §3.19).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import orjson

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.registry.resolve import Deployment, ResolvedRegistry
from taskiq_llm_queue.registry.schema import Registry, build
from taskiq_llm_queue.registry.settings import CHAT_INFERENCE, EMBED_INFERENCE


class RegistryError(ConfigError):
    """The registry document is invalid; `problems` lists every problem with its path."""

    def __init__(self, problems: list[str], warnings: list[str] | None = None) -> None:
        """Keep the full lists; the message shows them one per line."""
        self.problems = list(problems)
        self.warnings = list(warnings or [])
        lines = "\n  ".join(self.problems)
        super().__init__(f"invalid registry ({len(self.problems)} problems):\n  {lines}")


def _budget_numbers(view: ResolvedRegistry, alias: str, dep: Deployment) -> tuple[Any, Any]:
    return view.value(alias, "rpm", dep.key), view.value(alias, "tpm", dep.key)


def _check_model(
    view: ResolvedRegistry, alias: str, errors: list[str], warnings: list[str]
) -> None:
    entry = view.registry.models[alias]
    path = f"models.{alias}"
    deployments = view.deployments(alias)
    confirmed = [d for d in deployments if d.group == "confirmed"]

    # Accounts: their kind must serve the model's provider; enabled flags only.
    for dep in deployments:
        account = view.registry.accounts.get(dep.account)
        if account is not None and account.provider != entry.provider:
            errors.append(
                f"{path}.deployments.{dep.account}: a `{account.kind}` account serves "
                f"`{account.provider}`, not `{entry.provider}`"
            )

    # Timeouts.
    timeout = view.value(alias, "timeout_secs")
    if timeout > view.value(alias, "timeout_max_secs"):
        errors.append(f"{path}: `timeout_secs` is above `timeout_max_secs`")
    for dep in deployments:
        if view.value(alias, "attempt_timeout_secs", dep.key) >= view.value(
            alias, "timeout_secs", dep.key
        ):
            errors.append(
                f"{path} @ {dep.key}: `attempt_timeout_secs` must be below `timeout_secs`"
            )
    max_wait = view.value(alias, "max_wait_secs")
    for rule in ("fallback", "model_fallback"):
        value = view.value(alias, rule)
        if value and max_wait is not None and value["after_secs"] >= max_wait:
            errors.append(f"{path}.{rule}.after_secs: must be below `max_wait_secs`")

    # Shares.
    if view.value(alias, "priority_reserve") + view.value(alias, "normal_min_share") >= 1:
        errors.append(f"{path}: `priority_reserve` + `normal_min_share` must be below 1")

    # Budgets named by rate_limits must have a number.
    for dep in deployments:
        limits = view.effective(alias, "rate_limits", dep.key)
        rpm, tpm = _budget_numbers(view, alias, dep)
        if limits.level != "library":
            if limits.value == "rpm" and rpm is None:
                errors.append(f"{path} @ {dep.key}: `rate_limits` is rpm but no `rpm` is set")
            if limits.value == "tpm" and tpm is None:
                errors.append(f"{path} @ {dep.key}: `rate_limits` is tpm but no `tpm` is set")

    # Strategies that need quota numbers.
    for name in ("balance", "balance_priority"):
        if view.value(alias, name) == "headroom":
            bare = [d.key for d in confirmed if _budget_numbers(view, alias, d) == (None, None)]
            if bare:
                errors.append(f"{path}.{name}: headroom needs `rpm` or `tpm` on {', '.join(bare)}")
    if view.value(alias, "balance") == "weighted":
        weights = [view.value(alias, "weight", d.key) for d in confirmed]
        if weights and not any(weights):
            errors.append(f"{path}: every confirmed deployment has weight 0")

    # Output reservation from observed usage: only where TPM is enforced, never on Bedrock.
    if view.value(alias, "output_reserve") == "observed":
        if entry.provider == "bedrock":
            errors.append(
                f"{path}.output_reserve: Bedrock reserves max_tokens itself; use max_tokens"
            )
        for dep in deployments:
            limits = view.value(alias, "rate_limits", dep.key)
            if limits not in ("both", "tpm") or _budget_numbers(view, alias, dep)[1] is None:
                errors.append(f"{path}.output_reserve @ {dep.key}: needs TPM to be enforced")

    # Inference parameters by model type and provider; defaults within their limits.
    wrong = EMBED_INFERENCE if entry.type == "chat" else CHAT_INFERENCE
    for dep_key in (None, *(d.key for d in deployments)):
        inference = view.value(alias, "inference", dep_key) or {}
        where = path if dep_key is None else f"{path} @ {dep_key}"
        for name in inference:
            if name in wrong:
                errors.append(f"{where}.inference.{name}: not a parameter of a {entry.type} model")
        if "seed" in inference and entry.provider == "bedrock":
            errors.append(f"{where}.inference.seed: not supported by Bedrock")
        caps = view.value(alias, "inference_limits", dep_key) or {}
        if "max_tokens" in caps and inference.get("max_tokens", 0) > caps["max_tokens"]:
            errors.append(f"{where}.inference.max_tokens: above `inference_limits.max_tokens`")

    # Rules that depend on the model type.
    if entry.type == "chat" and "embed_batch_size" in entry.settings:
        errors.append(f"{path}.embed_batch_size: only for embedding models")
    rule = view.value(alias, "model_fallback")
    if rule and entry.type == "embedding":
        errors.append(f"{path}.model_fallback: an embedding model cannot change model")
    elif rule:
        for item in rule["models"]:
            other = item["model"]
            if other == alias:
                errors.append(f"{path}.model_fallback: a model cannot list itself")
                continue
            if other not in view.registry.models:
                errors.append(f"{path}.model_fallback: unknown model `{other}`")
                continue
            target = view.registry.models[other]
            if target.type != "chat":
                errors.append(f"{path}.model_fallback: `{other}` is not a chat model")
            back = view.value(other, "model_fallback")
            if back:
                mutual = [m for m in back["models"] if m["model"] == alias]
                if mutual and not (item["require_headroom"] and mutual[0]["require_headroom"]):
                    errors.append(
                        f"{path}.model_fallback: `{alias}` and `{other}` list each other; "
                        "both must use require_headroom"
                    )
            if item["require_headroom"]:
                bare = [
                    d.key
                    for d in view.deployments(other, "confirmed")
                    if _budget_numbers(view, other, d) == (None, None)
                ]
                if bare:
                    errors.append(
                        f"{path}.model_fallback: require_headroom on `{other}` needs `rpm` or "
                        f"`tpm` on {', '.join(bare)}"
                    )

    # Warnings (not fatal).
    fallback_deps = [d for d in deployments if d.group == "fallback"]
    if fallback_deps and not view.value(alias, "fallback"):
        warnings.append(f"{path}: fallback regions are listed but there is no `fallback` rule")
    for dep in deployments:
        if view.outside_residency(dep):
            reason = "routes_to is missing" if dep.profile_id and not dep.routes_to else "region"
            warnings.append(f"{path} @ {dep.key}: outside the residency list ({reason}); unused")
    for dep in deployments:
        rpm = view.value(alias, "rpm", dep.key)
        shared = view.registry.accounts[dep.account].settings.get("shared_rpm")
        if shared is not None and rpm is not None and shared < rpm:
            warnings.append(f"accounts.{dep.account}: `shared_rpm` is below {alias} @ {dep.key}")


def _check_registry(view: ResolvedRegistry) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    registry = view.registry
    ids: dict[str, str] = {}
    queues: dict[str, str] = {}
    for alias, entry in registry.models.items():
        if entry.model_id in ids:
            errors.append(
                f"models.{alias}.model_id: `{entry.model_id}` already belongs to "
                f"`{ids[entry.model_id]}` (one alias per model id)"
            )
        ids.setdefault(entry.model_id, alias)
        name = view.queue_name(alias)
        for queue in (name, f"{name}-priority"):
            if queue in queues:
                errors.append(f"models.{alias}: queue `{queue}` is also used by {queues[queue]}")
            queues[queue] = alias
    for alias in registry.models:
        if alias in ids and ids[alias] != alias:
            errors.append(f"models.{alias}: the alias equals the model id of `{ids[alias]}`")
        _check_model(view, alias, errors, warnings)
    for account in registry.routing.blocked_accounts:
        if account not in registry.accounts:
            warnings.append(f"routing.blocked_accounts: `{account}` is not an account")
    return errors, warnings


def check(
    raw: Mapping[str, Any] | str | bytes,
    *,
    strict: bool = True,
    allowed_hosts: tuple[str, ...] = (),
) -> tuple[Registry | None, list[str], list[str]]:
    """Parse, resolve and check a document; return (registry or None, errors, warnings)."""
    if isinstance(raw, str | bytes):
        try:
            raw = orjson.loads(raw)
        except orjson.JSONDecodeError as exc:
            return None, [f"registry is not valid JSON: {exc}"], []
    if not isinstance(raw, Mapping):
        return None, ["registry must be a JSON object"], []
    registry, errors, warnings = build(raw, strict=strict, allowed_hosts=allowed_hosts)
    if registry is None:
        return None, errors, warnings
    more_errors, more_warnings = _check_registry(ResolvedRegistry(registry))
    warnings = warnings + more_warnings
    if more_errors:
        return None, more_errors, warnings
    return _with_warnings(registry, warnings), [], warnings


def _with_warnings(registry: Registry, warnings: list[str]) -> Registry:
    return Registry(
        version=registry.version,
        defaults=registry.defaults,
        routing=registry.routing,
        accounts=registry.accounts,
        models=registry.models,
        residency=registry.residency,
        min_lib_version=registry.min_lib_version,
        warnings=tuple(warnings),
        raw=registry.raw,
    )


def load_registry(
    raw: Mapping[str, Any] | str | bytes,
    *,
    strict: bool = True,
    allowed_hosts: tuple[str, ...] = (),
) -> ResolvedRegistry:
    """Load and check a registry document; raise `RegistryError` with every problem.

    `strict=False` is for a document read back from the store: unknown names become warnings.
    """
    registry, errors, warnings = check(raw, strict=strict, allowed_hosts=allowed_hosts)
    if registry is None:
        raise RegistryError(errors, warnings)
    return ResolvedRegistry(registry)
