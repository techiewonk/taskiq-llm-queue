"""Named hooks chosen by name in the registry: balancer, fallback policy, admission, sticky key.

Plan §3.10. Hooks are plain functions registered in code; the registry names them (`balance:
"custom:cheapest"`, `fallback_policy: "office_hours"`). A hook that raises is logged once per
name and the built-in behaviour is used, so a bug in a hook never fails a call.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from taskiq_llm_queue._logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class DeploymentView:
    """What a custom balancer sees of one candidate deployment."""

    key: str
    account: str
    region: str
    group: str
    rpm: int | None
    tpm: int | None
    weight: float
    tags: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CallContext:
    """What hooks see of a call (no prompt text)."""

    model: str
    request_id: str
    priority: bool
    session_id: str | None
    user_id: str | None
    tenant_id: str | None
    waited_secs: float = 0.0


@dataclass(frozen=True, slots=True)
class Reject:
    """Returned by an admission hook to refuse a call before it is sent."""

    reason: str


Balancer = Callable[[Sequence[DeploymentView], CallContext], Sequence[str]]
FallbackPolicy = Callable[[str, float, Mapping[str, Any], CallContext], bool]
AdmissionHook = Callable[[CallContext], "Reject | None"]
StickyKey = Callable[[CallContext], "str | None"]

_HOOKS: dict[str, dict[str, Callable[..., Any]]] = {
    "balancer": {},
    "fallback_policy": {},
    "admission": {},
    "sticky_key": {},
    "probe": {},
}
_WARNED: set[tuple[str, str]] = set()


def _register(kind: str, name: str, fn: Callable[..., Any]) -> None:
    if not name or not callable(fn):
        raise ValueError(f"{kind} hook needs a name and a callable")
    _HOOKS[kind][name] = fn


def register_balancer(name: str, fn: Balancer) -> None:
    """`balance: "custom:<name>"`: fn(candidates, ctx) -> deployment keys, most preferred first."""
    _register("balancer", name, fn)


def register_fallback_policy(name: str, fn: FallbackPolicy) -> None:
    """`fallback_policy: <name>`: fn(cause, waited_secs, rule, ctx) -> may this call spill now."""
    _register("fallback_policy", name, fn)


def register_admission(name: str, fn: AdmissionHook) -> None:
    """`admission: <name>`: fn(ctx) -> Reject(reason) to refuse the call, None to admit it."""
    _register("admission", name, fn)


def register_sticky_key(name: str, fn: StickyKey) -> None:
    """`sticky.key: "custom:<name>"`: fn(ctx) -> the key a call sticks by (None: no sticking)."""
    _register("sticky_key", name, fn)


def register_probe(name: str, fn: Callable[..., Any]) -> None:
    """`probe: <name>`: the request a health probe sends (step A11)."""
    _register("probe", name, fn)


def get(kind: str, name: str | None) -> Callable[..., Any] | None:
    """A registered hook, or None (unknown names fall back to the built-in behaviour)."""
    if not name:
        return None
    fn = _HOOKS[kind].get(name)
    if fn is None and (kind, name) not in _WARNED:
        _WARNED.add((kind, name))
        logger.warning("hook_not_registered", kind=kind, name=name)
    return fn


def call_safely(kind: str, name: str, fn: Callable[..., Any], *args: Any, default: Any) -> Any:
    """Run a hook; on an exception log once per (kind, name) and return `default`."""
    try:
        return fn(*args)
    except Exception as exc:  # a hook bug must never fail a call
        if (kind, name) not in _WARNED:
            _WARNED.add((kind, name))
            logger.warning("hook_failed", kind=kind, name=name, error=str(exc))
        return default
