"""The registry document (`LLMQ_REGISTRY`) as frozen data, built with every problem collected.

`build()` turns the JSON document into a `Registry` and lists all problems at once, each with
its path, so one fix-and-rerun is enough. Cross-field checks that need resolved values live in
`registry.validate`. Unknown names are errors for the process's own config (`strict`) and
warnings for a stored document (read leniently, so an older pod still starts).
"""

from __future__ import annotations

import difflib
import ipaddress
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any
from urllib.parse import urlparse

from taskiq_llm_queue.errors import ConfigError, UnknownModel
from taskiq_llm_queue.registry.settings import SETTINGS, A, D, M, R, settings_at

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
# Queue (stream) names: short and plain, so key names and metric labels stay readable.
QUEUE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,71}$")  # leaves room for "-priority"
# Account kind -> the provider it serves. `register_account_kind` adds more.
ACCOUNT_KINDS: dict[str, str] = {"aws": "bedrock", "openai": "openai"}
MODEL_TYPES = ("chat", "embedding")
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "host.docker.internal"})

_TOP = ("registry_version", "min_lib_version", "defaults", "routing", "residency")
_TOP_KEYS = frozenset((*_TOP, "accounts", "models"))
_ACCOUNT_FIELDS = frozenset(
    {"kind", "enabled", "role_arn", "external_id_ref", "base_url", "api_key_ref", "settings"}
)
_MODEL_FIELDS = frozenset({"type", "provider", "model_id", "deployments", "residency"})
_DEPLOYMENT_FIELDS = frozenset({"confirmed", "fallback", "overrides"})
_ROUTING_FIELDS = frozenset({"kill_switch", "allow_users", "blocked_regions", "blocked_accounts"})
_PROFILE_FIELDS = frozenset({"name", "region", "profile_id", "routes_to"})


def register_account_kind(kind: str, provider: str) -> None:
    """Allow accounts of `kind` to serve models of `provider` (for a custom provider)."""
    if not NAME_RE.match(kind) or not NAME_RE.match(provider):
        raise ConfigError("kind and provider must be lowercase names")
    ACCOUNT_KINDS[kind] = provider


def _frozen(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class Account:
    """Where calls are made from: an AWS account (pod identity or assumed role) or an endpoint."""

    name: str
    kind: str
    enabled: bool = True
    role_arn: str | None = None
    external_id_ref: str | None = None
    base_url: str | None = None
    api_key_ref: str | None = None
    settings: Mapping[str, Any] = field(default_factory=dict)

    @property
    def provider(self) -> str:
        """The provider this account serves."""
        return ACCOUNT_KINDS[self.kind]


@dataclass(frozen=True, slots=True)
class RegionEntry:
    """One region (or inference profile) of a model in one account: a deployment before resolve.

    `name` is the entry's name (the region, or the profile's own name); `region` is where the
    client calls; `settings` are its deployment-level values (profile fields and `overrides`).
    """

    name: str
    region: str
    group: str
    profile_id: str | None = None
    routes_to: tuple[str, ...] = ()
    settings: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelEntry:
    """One model (alias): its provider model id, model-level settings and region entries."""

    alias: str
    type: str
    provider: str
    model_id: str
    settings: Mapping[str, Any]
    deployments: Mapping[str, tuple[RegionEntry, ...]]
    residency: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class Routing:
    """Top-level routing switches: kill switch, users always queued, live blocks."""

    kill_switch: bool = False
    allow_users: tuple[str, ...] = ()
    blocked_regions: tuple[str, ...] = ()
    blocked_accounts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Registry:
    """The whole registry: version, defaults, routing, residency, accounts and models.

    `warnings` lists non-fatal findings (unknown names in a stored document, unused fallback
    regions, deployments outside the residency list).
    """

    version: int
    defaults: Mapping[str, Any]
    routing: Routing
    accounts: Mapping[str, Account]
    models: Mapping[str, ModelEntry]
    residency: tuple[str, ...] | None = None
    min_lib_version: str | None = None
    warnings: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def alias_of(self, model: str) -> str:
        """The alias for `model`, given as an alias or as a provider model id (`UnknownModel`)."""
        if model in self.models:
            return model
        for entry in self.models.values():
            if entry.model_id == model:
                return entry.alias
        raise UnknownModel(model)


class _Collector:
    """Gathers errors and warnings with their paths while the document is walked."""

    def __init__(self, strict: bool) -> None:
        self.strict = strict
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, path: str, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def warn(self, path: str, message: str) -> None:
        self.warnings.append(f"{path}: {message}")

    def unknown(self, path: str, name: str, known: frozenset[str], where: str) -> None:
        """An unknown name: error in strict mode, warning (value ignored) otherwise."""
        close = difflib.get_close_matches(name, sorted(known), n=1)
        hint = f"; did you mean `{close[0]}`?" if close else ""
        if name in SETTINGS:
            message = f"`{name}` cannot be set {where}"
            self.error(path, message)
            return
        message = f"unknown name `{name}`{hint}"
        if self.strict:
            self.error(path, message)
        else:
            self.warn(path, message + " (ignored)")

    def mapping(self, path: str, value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            return value
        self.error(path, "must be an object")
        return None

    def names(self, path: str, value: Any) -> tuple[str, ...]:
        if isinstance(value, list) and all(isinstance(v, str) for v in value):
            return tuple(value)
        self.error(path, "must be a list of strings")
        return ()


def _settings(
    out: _Collector,
    path: str,
    raw: Mapping[str, Any],
    level: str,
    skip: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Check the settings of one level; return the valid ones."""
    allowed = settings_at(level)
    where = {D: "in defaults", A: "in account settings", M: "on a model", R: "on a deployment"}
    kept: dict[str, Any] = {}
    for name, value in raw.items():
        if name in skip:
            continue
        if name not in allowed:
            out.unknown(f"{path}.{name}", name, allowed | skip, where[level])
            continue
        problem = SETTINGS[name].check(value)
        if problem:
            out.error(f"{path}.{name}", problem)
            continue
        if name == "queue" and level != M and "name" in value:
            out.error(f"{path}.queue.name", "a queue name can only be set on a model")
            continue
        kept[name] = value
    return kept


# Secrets of the process itself: never an account's key (a registry edit must not exfiltrate).
_PROCESS_SECRETS = frozenset(
    {
        "LLMQ_STORE_PASSWORD",
        "LLMQ_QUEUE_STORE_PASSWORD",
        "LLMQ_ID_HASH_SALT",
        "LLMQ_STORE_URL",
        "LLMQ_QUEUE_STORE_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    }
)


def _is_link_local(host: str) -> bool:
    """169.254.0.0/16 or fe80::/10: instance metadata endpoints live there."""
    try:
        return ipaddress.ip_address(host.strip("[]")).is_link_local
    except ValueError:
        return False


def _is_local(host: str) -> bool:
    return host in _LOCAL_HOSTS or host.endswith(".localhost") or "." not in host


def _account(
    out: _Collector, name: str, raw: Any, allowed_hosts: tuple[str, ...]
) -> Account | None:
    path = f"accounts.{name}"
    if not NAME_RE.match(name):
        out.error(path, "account names are lowercase letters, digits, `_`, `.`, `-`")
    body = out.mapping(path, raw)
    if body is None:
        return None
    for key in body:
        if key not in _ACCOUNT_FIELDS:
            out.unknown(f"{path}.{key}", key, _ACCOUNT_FIELDS, "on an account")
    kind = body.get("kind")
    if kind not in ACCOUNT_KINDS:
        out.error(f"{path}.kind", f"must be one of {', '.join(sorted(ACCOUNT_KINDS))}")
        return None
    texts: dict[str, str | None] = {}
    for key in ("role_arn", "external_id_ref", "base_url", "api_key_ref"):
        value = body.get(key)
        if value is not None and not (isinstance(value, str) and value):
            out.error(f"{path}.{key}", "must be a non-empty string")
            value = None
        texts[key] = value
    if kind != "aws":
        for key in ("role_arn", "external_id_ref"):
            if texts[key]:
                out.error(f"{path}.{key}", f"only an `aws` account takes `{key}`")
    if texts["external_id_ref"] and not texts["role_arn"]:
        out.error(f"{path}.external_id_ref", "needs `role_arn`")
    for key in ("external_id_ref", "api_key_ref"):
        ref = texts[key]
        if ref and ref in _PROCESS_SECRETS:
            # A registry author could otherwise send the store password or the pod's AWS
            # keys as a Bearer token to a base_url they choose.
            out.error(f"{path}.{key}", f"`{ref}` is a process secret, not an account's")
    if kind == "aws":
        for key in ("base_url", "api_key_ref"):
            if texts[key]:
                out.error(f"{path}.{key}", f"an `aws` account does not take `{key}`")
    if kind == "openai":
        url = texts["base_url"]
        if not url:
            out.error(f"{path}.base_url", "an `openai` account needs `base_url`")
        else:
            parsed = urlparse(url)
            host = parsed.hostname or ""
            if parsed.scheme not in ("https", "http") or not host:
                out.error(f"{path}.base_url", "must be an http(s) URL")
            elif _is_link_local(host):
                out.error(f"{path}.base_url", "a link-local host (instance metadata) is refused")
            elif parsed.scheme == "http" and not _is_local(host):
                out.error(f"{path}.base_url", "must use https unless the host is local")
            elif allowed_hosts and host not in allowed_hosts:
                out.error(f"{path}.base_url", f"host `{host}` is not in the allowed hosts")
    enabled = body.get("enabled", True)
    if not isinstance(enabled, bool):
        out.error(f"{path}.enabled", "must be true or false")
        enabled = True
    settings_raw = out.mapping(f"{path}.settings", body.get("settings", {})) or {}
    settings = _settings(out, f"{path}.settings", settings_raw, A)
    return Account(
        name=name,
        kind=kind,
        enabled=enabled,
        role_arn=texts["role_arn"],
        external_id_ref=texts["external_id_ref"],
        base_url=texts["base_url"],
        api_key_ref=texts["api_key_ref"],
        settings=_frozen(settings),
    )


def _region_entry(out: _Collector, path: str, raw: Any, group: str) -> RegionEntry | None:
    if isinstance(raw, str):
        if not NAME_RE.match(raw):
            out.error(path, f"`{raw}` is not a valid region name")
            return None
        return RegionEntry(name=raw, region=raw, group=group)
    body = out.mapping(path, raw)
    if body is None:
        return None
    name, region = body.get("name"), body.get("region")
    for key, value in (("name", name), ("region", region)):
        if not (isinstance(value, str) and NAME_RE.match(value)):
            out.error(f"{path}.{key}", "needs a valid name")
    profile = body.get("profile_id")
    if not (isinstance(profile, str) and profile):
        out.error(f"{path}.profile_id", "an inference profile needs `profile_id`")
    routes = out.names(f"{path}.routes_to", body.get("routes_to", []))
    extra = {k: v for k, v in body.items() if k not in _PROFILE_FIELDS}
    settings = _settings(out, path, extra, R)
    if not (isinstance(name, str) and isinstance(region, str) and isinstance(profile, str)):
        return None
    return RegionEntry(
        name=name,
        region=region,
        group=group,
        profile_id=profile,
        routes_to=routes,
        settings=_frozen(settings),
    )


def _deployments(
    out: _Collector, path: str, raw: Any, accounts: Mapping[str, Account]
) -> dict[str, tuple[RegionEntry, ...]]:
    result: dict[str, tuple[RegionEntry, ...]] = {}
    body = out.mapping(path, raw)
    if not body:
        if body is not None:
            out.error(path, "needs at least one account")
        return result
    for account_name, spec_raw in body.items():
        apath = f"{path}.{account_name}"
        if account_name not in accounts:
            close = difflib.get_close_matches(account_name, sorted(accounts), n=1)
            hint = f"; did you mean `{close[0]}`?" if close else ""
            out.error(apath, f"unknown account `{account_name}`{hint}")
        spec = out.mapping(apath, spec_raw)
        if spec is None:
            continue
        for key in spec:
            if key not in _DEPLOYMENT_FIELDS:
                out.unknown(f"{apath}.{key}", key, _DEPLOYMENT_FIELDS, "in deployments")
        entries: list[RegionEntry] = []
        for group in ("confirmed", "fallback"):
            listed = spec.get(group, [])
            if not isinstance(listed, list):
                out.error(f"{apath}.{group}", "must be a list")
                continue
            for i, item in enumerate(listed):
                entry = _region_entry(out, f"{apath}.{group}[{i}]", item, group)
                if entry:
                    entries.append(entry)
        seen: dict[str, str] = {}
        regions = {e.region for e in entries if e.profile_id is None}
        for entry in entries:
            if entry.name in seen:
                where = (
                    "both confirmed and fallback" if seen[entry.name] != entry.group else "twice"
                )
                out.error(f"{apath}", f"`{entry.name}` is listed {where}")
            seen[entry.name] = entry.group
            if entry.profile_id is not None and entry.name in regions:
                out.error(f"{apath}", f"profile name `{entry.name}` equals a region name")
        overrides = out.mapping(f"{apath}.overrides", spec.get("overrides", {})) or {}
        by_name = {e.name: e for e in entries}
        for target, values_raw in overrides.items():
            opath = f"{apath}.overrides.{target}"
            if target not in by_name:
                out.error(opath, f"`{target}` is not a region or profile of this account")
                continue
            values = out.mapping(opath, values_raw)
            if values is None:
                continue
            entry = by_name[target]
            merged = {**entry.settings, **_settings(out, opath, values, R)}
            by_name[target] = RegionEntry(
                name=entry.name,
                region=entry.region,
                group=entry.group,
                profile_id=entry.profile_id,
                routes_to=entry.routes_to,
                settings=_frozen(merged),
            )
        result[account_name] = tuple(by_name[e.name] for e in entries if e.name in by_name)
    return result


def _residency(out: _Collector, path: str, raw: Any) -> tuple[str, ...] | None:
    if raw is None:
        return None
    body = out.mapping(path, raw)
    if body is None:
        return None
    for key in body:
        if key != "allowed_regions":
            out.unknown(f"{path}.{key}", key, frozenset({"allowed_regions"}), "in residency")
    return out.names(f"{path}.allowed_regions", body.get("allowed_regions", []))


def _model(
    out: _Collector, alias: str, raw: Any, accounts: Mapping[str, Account]
) -> ModelEntry | None:
    path = f"models.{alias}"
    if not NAME_RE.match(alias):
        out.error(path, "an alias is lowercase letters, digits, `_`, `.`, `-`")
    body = out.mapping(path, raw)
    if body is None:
        return None
    kind = body.get("type")
    if kind not in MODEL_TYPES:
        out.error(f"{path}.type", "must be chat or embedding")
    provider = body.get("provider")
    if provider not in ACCOUNT_KINDS.values():
        known = ", ".join(sorted(set(ACCOUNT_KINDS.values())))
        out.error(f"{path}.provider", f"must be one of {known}")
    model_id = body.get("model_id")
    if not (isinstance(model_id, str) and model_id):
        out.error(f"{path}.model_id", "is required")
    settings = _settings(out, path, body, M, skip=_MODEL_FIELDS)
    deployments = _deployments(out, f"{path}.deployments", body.get("deployments"), accounts)
    if not any(e.group == "confirmed" for entries in deployments.values() for e in entries):
        out.error(f"{path}.deployments", "needs at least one confirmed region")
    residency = _residency(out, f"{path}.residency", body.get("residency"))
    if kind not in MODEL_TYPES or not isinstance(provider, str) or not isinstance(model_id, str):
        return None
    return ModelEntry(
        alias=alias,
        type=kind,
        provider=provider,
        model_id=model_id,
        settings=_frozen(settings),
        deployments=MappingProxyType(deployments),
        residency=residency,
    )


def _routing(out: _Collector, raw: Any) -> Routing:
    body = out.mapping("routing", raw if raw is not None else {})
    if body is None:
        return Routing()
    for key in body:
        if key not in _ROUTING_FIELDS:
            out.unknown(f"routing.{key}", key, _ROUTING_FIELDS, "in routing")
    kill = body.get("kill_switch", False)
    if not isinstance(kill, bool):
        out.error("routing.kill_switch", "must be true or false")
        kill = False
    return Routing(
        kill_switch=kill,
        allow_users=out.names("routing.allow_users", body.get("allow_users", [])),
        blocked_regions=out.names("routing.blocked_regions", body.get("blocked_regions", [])),
        blocked_accounts=out.names("routing.blocked_accounts", body.get("blocked_accounts", [])),
    )


def build(
    raw: Mapping[str, Any], *, strict: bool = True, allowed_hosts: tuple[str, ...] = ()
) -> tuple[Registry | None, list[str], list[str]]:
    """Build a `Registry` from the document; return it (None on errors), errors and warnings.

    Only structural checks run here; `registry.validate.check` adds the cross-field ones.
    """
    out = _Collector(strict)
    for key in raw:
        if key not in _TOP_KEYS:
            out.unknown(key, key, _TOP_KEYS, "at the top level")
    version = raw.get("registry_version")
    if not (isinstance(version, int) and not isinstance(version, bool) and version >= 1):
        out.error("registry_version", "is required: an integer of at least 1")
        version = 0
    min_lib = raw.get("min_lib_version")
    if min_lib is not None and not (isinstance(min_lib, str) and min_lib):
        out.error("min_lib_version", "must be a version string")
        min_lib = None
    defaults_raw = out.mapping("defaults", raw.get("defaults", {})) or {}
    defaults = _settings(out, "defaults", defaults_raw, D)
    accounts_raw = out.mapping("accounts", raw.get("accounts")) or {}
    if not accounts_raw:
        out.error("accounts", "needs at least one account")
    accounts = {
        name: account
        for name, body in accounts_raw.items()
        if (account := _account(out, name, body, allowed_hosts)) is not None
    }
    models_raw = out.mapping("models", raw.get("models")) or {}
    if not models_raw:
        out.error("models", "needs at least one model")
    models = {
        alias: model
        for alias, body in models_raw.items()
        if (model := _model(out, alias, body, accounts)) is not None
    }
    registry = Registry(
        version=version,
        defaults=_frozen(defaults),
        routing=_routing(out, raw.get("routing")),
        accounts=MappingProxyType(accounts),
        models=MappingProxyType(models),
        residency=_residency(out, "residency", raw.get("residency")),
        min_lib_version=min_lib,
        raw=_frozen(raw),
    )
    if out.errors:
        return None, out.errors, out.warnings
    return registry, out.errors, out.warnings
