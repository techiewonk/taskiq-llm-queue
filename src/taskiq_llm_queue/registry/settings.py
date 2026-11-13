"""Every registry setting: where it may be set, its type and range, and its library default.

One table drives validation (unknown names, levels, types, ranges) and resolution (defaults), so
a new setting is one row here. Used by `registry.schema` (checks) and `registry.resolve`.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

# Levels, most general first: `defaults`, an account's `settings`, the model, a deployment.
D, A, M, R = "D", "A", "M", "R"
LEVEL_NAMES = {D: "defaults", A: "account", M: "model", R: "deployment"}

Check = Callable[[Any], "str | None"]

STRATEGIES = ("ordered", "round_robin", "weighted", "headroom")
_CUSTOM_RE = re.compile(r"^custom:[A-Za-z0-9_.-]+$")
_HOOK_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def number(
    lo: float | None = None,
    hi: float | None = None,
    *,
    integer: bool = False,
    lo_open: bool = False,
) -> Check:
    """A number (an integer when `integer`) inside [lo, hi]; `lo_open` makes the low end strict."""

    def check(value: Any) -> str | None:
        if integer and not (isinstance(value, int) and not isinstance(value, bool)):
            return "must be an integer"
        if not _is_number(value):
            return "must be a number"
        if lo is not None and (value <= lo if lo_open else value < lo):
            return f"must be {'above' if lo_open else 'at least'} {lo:g}"
        if hi is not None and value > hi:
            return f"must be at most {hi:g}"
        return None

    return check


def one_of(*values: str) -> Check:
    """One of the given words."""

    def check(value: Any) -> str | None:
        return None if value in values else f"must be one of {', '.join(values)}"

    return check


def boolean(value: Any) -> str | None:
    """True or false."""
    return None if isinstance(value, bool) else "must be true or false"


def text(value: Any) -> str | None:
    """A non-empty string."""
    return None if isinstance(value, str) and value else "must be a non-empty string"


def strategy(value: Any) -> str | None:
    """A built-in balancing strategy or `custom:<name>`."""
    if value in STRATEGIES or (isinstance(value, str) and _CUSTOM_RE.match(value)):
        return None
    return f"must be one of {', '.join(STRATEGIES)} or custom:<name>"


def hook_name(value: Any) -> str | None:
    """The name of a registered hook."""
    return None if isinstance(value, str) and _HOOK_RE.match(value) else "must be a hook name"


def text_list(value: Any) -> str | None:
    """A list of non-empty strings."""
    if isinstance(value, list) and all(isinstance(v, str) and v for v in value):
        return None
    return "must be a list of non-empty strings"


def any_object(value: Any) -> str | None:
    """Any JSON object (passed through unchecked)."""
    return None if isinstance(value, dict) else "must be an object"


def obj(fields: Mapping[str, Check], *, required: tuple[str, ...] = ()) -> Check:
    """An object whose keys are among `fields`, each checked; `required` keys must be present."""

    def check(value: Any) -> str | None:
        if not isinstance(value, dict):
            return "must be an object"
        problems = [f"needs `{key}`" for key in required if key not in value]
        for key, item in value.items():
            sub = fields.get(key)
            if sub is None:
                problems.append(f"unknown field `{key}` (known: {', '.join(sorted(fields))})")
                continue
            problem = sub(item)
            if problem:
                problems.append(f"`{key}` {problem}")
        return "; ".join(problems) or None

    return check


def _tags(value: Any) -> str | None:
    if isinstance(value, dict) and all(isinstance(v, str) or _is_number(v) for v in value.values()):
        return None
    return "must be an object of text or number values"


def _causes(value: Any) -> str | None:
    allowed = ("exhausted", "broken", "error")
    if isinstance(value, list) and value and all(v in allowed for v in value):
        return None
    return f"must be a non-empty list of {', '.join(allowed)}"


def _fallback_models(value: Any) -> str | None:
    if not isinstance(value, list) or not value:
        return "must be a non-empty list"
    for item in value:
        if isinstance(item, str) and item:
            continue
        entry = obj({"model": text, "require_headroom": boolean}, required=("model",))(item)
        if entry:
            return f"each entry is an alias or {{model, require_headroom}}: {entry}"
    return None


def _sticky_key(value: Any) -> str | None:
    if value == "session" or (isinstance(value, str) and _CUSTOM_RE.match(value)):
        return None
    return "must be session or custom:<name>"


def _stop(value: Any) -> str | None:
    return None if isinstance(value, str) or text_list(value) is None else "must be text or a list"


# Inference parameters and the model type they belong to (rules of §3.9).
CHAT_INFERENCE: Mapping[str, Check] = {
    "max_tokens": number(1, integer=True),
    "temperature": number(0, 2),
    "top_p": number(0, 1),
    "stop": _stop,
    "top_k": number(1, integer=True),
    "seed": number(integer=True),
    "frequency_penalty": number(-2, 2),
    "presence_penalty": number(-2, 2),
    "extra": any_object,
}
EMBED_INFERENCE: Mapping[str, Check] = {
    "dimensions": number(1, integer=True),
    "normalize": boolean,
    "input_type": text,
    "truncate": text,
    "embedding_types": text_list,
}
INFERENCE_NAMES = frozenset(CHAT_INFERENCE) | frozenset(EMBED_INFERENCE)


def _locked(value: Any) -> str | None:
    if text_list(value) is None and all(v in INFERENCE_NAMES for v in value):
        return None
    return "must be a list of inference parameter names"


HEALTH_DEFAULTS = {
    "disable_errors": 3,
    "disable_window_secs": 300,
    "probe_after_secs": 900,
    "probe_max_secs": 7200,
    "slow_ratio": 3.0,
    "slow_min_secs": 5,
    "slow_min_samples": 20,
}
ALERT_DEFAULTS = {
    "quota_used_pct": 90,
    "quota_used_mins": 5,
    "spill_pct": 20,
    "spill_window_secs": 300,
    "waiting_age_secs": 60,
    "dlq_messages": 1,
}
QUEUE_DEFAULTS = {
    "visibility_secs": 60,
    "max_receives": 10,
    "retention_secs": 345_600,
    "dlq_retention_secs": 86_400,
}

_PCT = number(0, 100, integer=True)
_POS = number(0, lo_open=True)


@dataclass(frozen=True, slots=True)
class Spec:
    """One setting: the levels it may be set at, its check, its default and how levels combine.

    `default` may be a callable taking the model type (`chat` / `embedding`); `merge` objects
    combine key by key across levels instead of the most specific one replacing the rest.
    """

    levels: frozenset[str]
    check: Check
    default: Any = None
    merge: bool = False
    deploy_only: bool = False
    protected: bool = False
    by_type: Callable[[str], Any] | None = field(default=None)


def _spec(levels: str, check: Check, default: Any = None, **kw: Any) -> Spec:
    return Spec(frozenset(levels.split(",")), check, default, **kw)


SETTINGS: Mapping[str, Spec] = {
    "enabled": _spec("A,M,R", boolean, True),
    "rpm": _spec("D,M,R", number(1, integer=True)),
    "tpm": _spec("D,M,R", number(1, integer=True)),
    "rate_limits": _spec(
        "D,A,M,R",
        one_of("both", "rpm", "tpm", "off"),
        by_type=lambda kind: "both" if kind == "chat" else "rpm",
    ),
    "tpm_output_weight": _spec("D,A,M,R", number(1), 1),
    "tpm_count_cached": _spec("D,A,M,R", boolean, False),
    "default_output_tokens": _spec("D,A,M,R", number(1, integer=True), 1000),
    "shared_rpm": _spec("A", number(1, integer=True)),
    "shared_tpm": _spec("A", number(1, integer=True)),
    "rate_window": _spec("D,A,M,R", one_of("smooth", "fixed"), "smooth"),
    "rate_burst": _spec("D,A,M,R", number(0, 1), 0.1),
    "stream_flush_ms": _spec("M,R", number(0, 1000, integer=True), 30),
    "max_concurrency": _spec("D,A,M,R", number(1, integer=True)),
    "priority_reserve": _spec(
        "D,A,M", number(0, 1), by_type=lambda kind: 0.2 if kind == "chat" else 0.0
    ),
    "normal_min_share": _spec("D,M", number(0, 1), 0.1),
    "timeout_secs": _spec("D,A,M", _POS, 120),
    "timeout_max_secs": _spec("D,M", _POS, 900),
    "max_wait_secs": _spec("D,M", _POS),
    "attempt_timeout_secs": _spec("D,A,M,R", _POS, 60),
    "throttle_retry_attempts": _spec("D,A,M,R", number(1, 20, integer=True), 6),
    "retry_base_secs": _spec("D,A,M,R", number(0), 1.0),
    "retry_multiplier": _spec("D,A,M,R", number(1), 4.0),
    "retry_spread": _spec("D,A,M,R", number(1), 2.5),
    "retry_max_secs": _spec("D,A,M,R", _POS, 30.0),
    "breaker_threshold": _spec("D,A,M,R", number(1, integer=True), 5),
    "breaker_cooldown_secs": _spec("D,A,M,R", _POS, 15.0),
    # Failures count toward the threshold only within this window (one after it restarts it).
    "breaker_window_secs": _spec("D,A,M,R", _POS, 60.0),
    # A probe call that never reported back stops blocking others after this.
    "breaker_probe_secs": _spec("D,A,M,R", _POS, 60.0),
    "balance": _spec("D,M", strategy, "ordered"),
    "balance_opts": _spec(
        "D,M", obj({"pick": one_of("percent", "max", "spread")}), {"pick": "percent"}, merge=True
    ),
    "balance_priority": _spec("M", strategy),
    "max_attempts": _spec("D,M", number(1, integer=True), 4),
    "retry_budget_pct": _spec("D,M", _PCT, 20),
    "retry_budget_min": _spec("D,M", number(0, integer=True), 10),
    "stream_idle_secs": _spec("D,M", _POS, 30),
    "stream_max_bytes": _spec("M", number(1024, integer=True), 8_388_608),
    "max_waiting": _spec("D,M", number(0, integer=True), 50_000),
    "sticky_move_after_secs": _spec("D,M", _POS, 1),
    "require_headroom_pct": _spec("D,M", _PCT, 20),
    "tags": _spec("R", _tags),
    "weight": _spec("R", number(0), 1),
    "sticky": _spec(
        "D,M",
        obj({"key": _sticky_key, "idle_ttl_secs": _POS}, required=("key",)),
    ),
    "fallback": _spec(
        "D,M",
        obj(
            {"on": _causes, "after_secs": number(0), "regions": one_of("account", "any")},
            required=("on",),
        ),
    ),
    "model_fallback": _spec(
        "M",
        obj(
            {"on": _causes, "after_secs": number(0), "models": _fallback_models},
            required=("on", "models"),
        ),
    ),
    "store_down": _spec("D,M", one_of("pause", "static_cap", "fail_fast"), "pause"),
    "queue_pct": _spec("D,M", _PCT, 100),
    "worker_slots": _spec("D,M", number(1, 1000, integer=True), 20, deploy_only=True),
    "queue": _spec(
        "D,M",
        obj(
            {
                "name": text,
                "visibility_secs": number(1, 43_200, integer=True),
                "max_receives": number(1, integer=True),
                "retention_secs": number(60, integer=True),
                "dlq_retention_secs": number(60, integer=True),
            }
        ),
        QUEUE_DEFAULTS,
        merge=True,
    ),
    "embed_batch_size": _spec("M", number(1, integer=True), 1),
    "inference": _spec("D,A,M,R", obj({**CHAT_INFERENCE, **EMBED_INFERENCE}), merge=True),
    "inference_limits": _spec(
        "D,A,M,R",
        obj(
            {
                "max_tokens": number(1, integer=True),
                "locked": _locked,
                "on_exceed": one_of("clamp", "reject"),
            }
        ),
        merge=True,
    ),
    "params": _spec("R", any_object, protected=True),
    "health": _spec(
        "D,A,M,R",
        obj(
            {
                "disable_errors": number(1, integer=True),
                "disable_window_secs": _POS,
                "probe_after_secs": _POS,
                "probe_max_secs": _POS,
                "slow_ratio": number(1, lo_open=True),
                "slow_min_secs": number(0),
                "slow_min_samples": number(1, integer=True),
            }
        ),
        HEALTH_DEFAULTS,
        merge=True,
    ),
    "alerts": _spec(
        "D,M",
        obj({key: number(0) for key in ALERT_DEFAULTS}),
        ALERT_DEFAULTS,
        merge=True,
    ),
    "admission": _spec("D,M", hook_name),
    "fallback_policy": _spec("D,M", hook_name),
    "token_estimator": _spec("D,M", hook_name),
    "probe": _spec("D,M", hook_name),
    "debug_capture": _spec("M", boolean, False),
}

# Defaults of the optional fields inside the two fallback rules.
FALLBACK_AFTER_SECS = 2.0
MODEL_FALLBACK_AFTER_SECS = 5.0


def settings_at(level: str) -> frozenset[str]:
    """Names of the settings that may be set at `level`."""
    return frozenset(name for name, spec in SETTINGS.items() if level in spec.levels)
