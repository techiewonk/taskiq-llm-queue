"""The registry in the state store: publish by version, live edits, audit, rollback, reload.

Every write is one Lua call that stores the document, its version record, an audit entry (path
diffs, secrets redacted) and each model's resolved numbers together; a live edit is a
compare-and-set on the change counter. Readers keep the last good registry when the stored one is
unreadable, and the reload loop republishes the config when the stored registry was lost.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import orjson
from redis.exceptions import RedisError

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.registry import _lua
from taskiq_llm_queue.registry.resolve import ResolvedRegistry
from taskiq_llm_queue.registry.validate import RegistryError, check

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)

AUDIT_RETENTION_DAYS = 90
HISTORY_SIZE = 20
EDIT_RETRIES = 3
LIVE_EDIT_MODES = ("safe", "all", "off")
_SECRET_FIELDS = frozenset({"role_arn", "external_id_ref", "api_key_ref"})
# Settings whose value goes into a model's resolved numbers (read by the admission script).
ADMISSION_SETTINGS = (
    "enabled",
    "rpm",
    "tpm",
    "rate_limits",
    "rate_window",
    "rate_burst",
    "tpm_output_weight",
    "max_concurrency",
    "limiter_initial",
    "weight",
    "attempt_timeout_secs",
    "breaker_threshold",
    "breaker_cooldown_secs",
)
MODEL_ADMISSION_SETTINGS = (
    "balance",
    "balance_priority",
    "balance_opts",
    "priority_reserve",
    "normal_min_share",
    "require_headroom_pct",
    "output_reserve",
    "max_waiting",
)


class RegistryConflict(ConfigError):
    """A live edit lost a race with another edit three times in a row."""


class LiveEditRefused(ConfigError):
    """A live edit touched fields that change only by deploy (or live edits are off)."""


Path = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishResult:
    """What a publish did: `published`, `kept`, `not_bumped` or `ignored`, and the version live."""

    action: str
    version: int


def content_hash(doc: Mapping[str, Any]) -> str:
    """Stable hash of a document (sorted keys), to spot a config change without a version bump."""
    return hashlib.sha256(orjson.dumps(doc, option=orjson.OPT_SORT_KEYS)).hexdigest()


def diff(before: Any, after: Any, path: Path = ()) -> list[tuple[Path, Any, Any]]:
    """Leaf changes between two documents as (path, before, after); lists compare whole."""
    if isinstance(before, dict) and after is None:
        after = {}
    if isinstance(after, dict) and before is None:
        before = {}
    if isinstance(before, dict) and isinstance(after, dict):
        out: list[tuple[Path, Any, Any]] = []
        for key in sorted(set(before) | set(after)):
            out += diff(before.get(key), after.get(key), (*path, key))
        return out
    return [] if before == after else [(path, before, after)]


def _redact(path: Path, value: Any) -> Any:
    if value is not None and path and path[-1] in _SECRET_FIELDS:
        return "[redacted]"
    if isinstance(value, dict):
        return {k: _redact((*path, k), v) for k, v in value.items()}
    return value


def audit_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The audit form of a change: one entry per changed path, secrets redacted."""
    return [
        {"path": ".".join(p), "before": _redact(p, b), "after": _redact(p, a)}
        for p, b, a in diff(dict(before), dict(after))
    ]


def _deploy_only(path: Path) -> str | None:
    """Why a changed path needs a deploy (a new `registry_version`), or None."""
    if path[:1] in (("registry_version",), ("min_lib_version",)):
        return "changes with a deploy"
    if path[:1] == ("models",) and len(path) == 2:
        return "adding or removing a model needs a new registry_version"
    if path[:1] == ("models",) and len(path) >= 3:
        if path[2] in ("type", "provider", "model_id", "worker_slots"):
            return f"`{path[2]}` changes only with a deploy"
        if path[2:4] == ("queue", "name"):
            return "a queue name changes only with a deploy"
    if path[:2] == ("defaults", "worker_slots"):
        return "`worker_slots` changes only with a deploy"
    return None


def _protected(path: Path) -> str | None:
    """Why a changed path is protected under `registry_live_edit=safe`, or None."""
    if "residency" in path:
        return "residency changes only with a deploy"
    if path[:1] == ("accounts",):
        if len(path) == 2:
            return "adding or removing an account changes only with a deploy"
        if len(path) >= 3 and path[2] in ("kind", "base_url", *_SECRET_FIELDS):
            return f"account `{path[2]}` changes only with a deploy"
    if path[:1] == ("models",) and len(path) >= 4 and path[2] == "deployments":
        if len(path) == 4 or path[4] in ("confirmed", "fallback"):
            return "region lists change only with a deploy"
        if "params" in path:
            return "deployment params change only with a deploy"
    return None


def refused_edits(before: Mapping[str, Any], after: Mapping[str, Any], mode: str) -> list[str]:
    """Reasons a live edit from `before` to `after` is refused under `registry_live_edit=mode`."""
    if mode not in LIVE_EDIT_MODES:
        raise ValueError(f"registry_live_edit must be one of {', '.join(LIVE_EDIT_MODES)}")
    changes = diff(dict(before), dict(after))
    if mode == "off" and changes:
        return ["live edits are off (registry_live_edit=off)"]
    reasons = []
    whole = []
    for section, what in (("models", "a model"), ("accounts", "an account")):
        old, new = set(before.get(section, {})), set(after.get(section, {}))
        for name in sorted(old ^ new):
            whole.append((section, name))
            reasons.append(
                f"{section}.{name}: adding or removing {what} needs a new registry_version"
            )
    for path, _, _ in changes:
        if path[:2] in whole:
            continue
        why = _deploy_only(path) or (_protected(path) if mode == "safe" else None)
        if why:
            reasons.append(f"{'.'.join(path)}: {why}")
    return reasons


def _set_path(doc: dict[str, Any], path: Sequence[str], value: Any) -> None:
    """Set (or delete, value None) one path, creating objects on the way."""
    node = doc
    for key in path[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    if value is None:
        node.pop(path[-1], None)
    else:
        node[path[-1]] = value


def split_path(doc: Mapping[str, Any], dotted: str) -> list[str]:
    """Split a dotted path, keeping names that contain dots (`deployments.aws-main.x.y`) whole.

    At each level the longest run of segments that names an existing key wins.
    """
    parts = dotted.split(".")
    out: list[str] = []
    node: Any = doc
    i = 0
    while i < len(parts):
        chosen = 1
        if isinstance(node, Mapping):
            for j in range(len(parts), i, -1):
                if ".".join(parts[i:j]) in node:
                    chosen = j - i
                    break
        key = ".".join(parts[i : i + chosen])
        out.append(key)
        node = node.get(key) if isinstance(node, Mapping) else None
        i += chosen
    return out


def admission_numbers(view: ResolvedRegistry, alias: str) -> dict[str, Any]:
    """A model's resolved numbers for the admission script: model values, one row per deployment."""
    model = {name: view.value(alias, name) for name in MODEL_ADMISSION_SETTINGS}
    deployments = []
    for dep in view.deployments(alias):
        row = {name: view.value(alias, name, dep.key) for name in ADMISSION_SETTINGS}
        account = view.registry.accounts[dep.account]
        routing = view.registry.routing
        row.update(
            key=dep.key,
            account=dep.account,
            group=dep.group,
            region=dep.region,
            outside_residency=view.outside_residency(dep),
            account_enabled=account.enabled,
            blocked=dep.region in routing.blocked_regions
            or dep.account in routing.blocked_accounts,
            shared_rpm=account.settings.get("shared_rpm"),
            shared_tpm=account.settings.get("shared_tpm"),
            account_priority_reserve=account.settings.get("priority_reserve", 0),
        )
        deployments.append(row)
    return {"model": model, "deployments": deployments}


class RegistryStore:
    """The registry of one environment (key prefix) in the state store.

    `current` is the last good `ResolvedRegistry`; hot paths read it without I/O. `config` is
    this process's own document, used by `boot()` and to restore a lost registry.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        prefix: str,
        *,
        config: Mapping[str, Any] | None = None,
        live_edit: str = "safe",
        allowed_hosts: tuple[str, ...] = (),
        audit_retention_days: int = AUDIT_RETENTION_DAYS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Keep the client and settings; nothing is read or written until `boot` or `load`."""
        if live_edit not in LIVE_EDIT_MODES:
            raise ConfigError(f"registry_live_edit must be one of {', '.join(LIVE_EDIT_MODES)}")
        self._client = client
        self.prefix = prefix
        self.config = dict(config) if config is not None else None
        self.live_edit = live_edit
        self._hosts = allowed_hosts
        self._retention_ms = audit_retention_days * 86_400_000
        self._sleep = sleep
        self._publish = client.register_script(_lua.PUBLISH)
        self._edit = client.register_script(_lua.EDIT)
        self._current: ResolvedRegistry | None = None
        self._changes = -1
        self._version = 0
        self.invalid_count = 0
        self._warned: set[str] = set()

    # ---------------------------------------------------------------- keys

    @property
    def doc_key(self) -> str:
        """The stored document (JSON string)."""
        return f"{self.prefix}:{{reg}}:doc"

    @property
    def meta_key(self) -> str:
        """Version record: `version`, `config_hash`, `changes`, `published`."""
        return f"{self.prefix}:{{reg}}:meta"

    @property
    def audit_key(self) -> str:
        """Audit stream: one entry per publish or edit, path diffs with secrets redacted."""
        return f"{self.prefix}:{{reg}}:audit"

    @property
    def history_key(self) -> str:
        """Published versions kept for rollback (hash version -> document)."""
        return f"{self.prefix}:{{reg}}:history"

    def numbers_key(self, alias: str) -> str:
        """A model's resolved numbers and the change counter they belong to."""
        return f"{self.prefix}:{{m:{alias}}}:cfg"

    # ---------------------------------------------------------------- reading

    @property
    def current(self) -> ResolvedRegistry:
        """Last good registry; raises `ConfigError` before the first successful load."""
        if self._current is None:
            raise ConfigError("the registry has not been loaded yet")
        return self._current

    @property
    def changes(self) -> int:
        """Change counter of the loaded registry (-1 before the first load)."""
        return self._changes

    async def _read(self) -> tuple[dict[str, Any] | None, dict[str, str]]:
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.get(self.doc_key)
            pipe.hgetall(self.meta_key)
            raw, meta = await pipe.execute()
        meta_text = {_text(k): _text(v) for k, v in (meta or {}).items()}
        if raw is None:
            return None, meta_text
        try:
            doc = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return {"__invalid__": _text(raw)[:200]}, meta_text
        return doc, meta_text

    def _accept(self, doc: Mapping[str, Any], meta: Mapping[str, str]) -> bool:
        registry, errors, _ = check(doc, strict=False, allowed_hosts=self._hosts)
        if registry is None:
            self.invalid_count += 1
            key = "|".join(errors)
            if key not in self._warned:
                self._warned.add(key)
                logger.warning("registry_invalid", problems=errors[:20], count=len(errors))
            return False
        for warning in registry.warnings:
            if warning not in self._warned:
                self._warned.add(warning)
                logger.warning("registry_warning", warning=warning)
        self._current = ResolvedRegistry(registry)
        self._changes = int(meta.get("changes", "0"))
        self._version = registry.version
        return True

    async def load(self) -> ResolvedRegistry | None:
        """Read the stored registry; keep the last good one when it is unreadable or invalid.

        Returns None when there is no stored registry (nothing published yet, or it was lost).
        """
        try:
            doc, meta = await self._read()
        except (RedisError, OSError) as exc:
            logger.warning("registry_unreachable", error=str(exc))
            return self._current
        if doc is None:
            return None
        self._accept(doc, meta)
        return self._current

    # ---------------------------------------------------------------- publishing

    def _numbers(self, view: ResolvedRegistry) -> tuple[list[str], list[str]]:
        aliases = sorted(view.registry.models)
        keys = [self.numbers_key(a) for a in aliases]
        values = [orjson.dumps(admission_numbers(view, a)).decode() for a in aliases]
        return keys, values

    async def publish(self, doc: Mapping[str, Any], *, actor: str) -> PublishResult:
        """Publish a document by the version rule; raise `RegistryError` when it is invalid."""
        registry, errors, warnings = check(doc, strict=True, allowed_hosts=self._hosts)
        if registry is None:
            raise RegistryError(errors, warnings)
        view = ResolvedRegistry(registry)
        stored, _ = await self._read()
        changes = orjson.dumps(audit_diff(stored or {}, doc)).decode()
        keys, values = self._numbers(view)
        reply = await self._publish(
            [self.doc_key, self.meta_key, self.audit_key, self.history_key, *keys],
            [
                orjson.dumps(doc).decode(),
                registry.version,
                content_hash(doc),
                actor,
                changes,
                self._retention_ms,
                HISTORY_SIZE,
                *values,
            ],
        )
        action, version = _text(reply[0]), int(reply[1])
        if action == "not_bumped":
            logger.warning("registry_version_not_bumped", version=version)
        elif action == "ignored":
            logger.info("registry_older_ignored", stored=version, config=registry.version)
        elif action == "published":
            logger.info("registry_published", version=version, actor=actor)
            logger.info("llmq_registry_audit", actor=actor, op="publish", diff=changes)
        await self.load()
        return PublishResult(action, version)

    async def boot(self, *, actor: str = "boot") -> ResolvedRegistry:
        """Publish this process's config by the version rule, then load what is live.

        When the store cannot be reached the process starts on its own config (plan §3.18).
        """
        if self.config is None:
            raise ConfigError("RegistryStore.boot needs the process's own registry (config)")
        try:
            await self.publish(self.config, actor=actor)
        except (RedisError, OSError) as exc:
            logger.warning("registry_store_unreachable_at_boot", error=str(exc))
            registry, errors, warnings = check(self.config, allowed_hosts=self._hosts)
            if registry is None:
                raise RegistryError(errors, warnings) from exc
            self._current = ResolvedRegistry(registry)
        return self.current

    # ---------------------------------------------------------------- live edits

    async def edit(
        self, change: Callable[[dict[str, Any]], None], *, actor: str
    ) -> ResolvedRegistry:
        """Apply `change` to the stored document as a live edit: validated, checked, audited.

        Compare-and-set on the change counter; on a race the document is read again and the
        change re-applied (up to three times).
        """
        for _ in range(EDIT_RETRIES):
            doc, meta = await self._read()
            if doc is None:
                raise ConfigError("no registry is stored; publish one first")
            after = orjson.loads(orjson.dumps(doc))
            change(after)
            reasons = refused_edits(doc, after, self.live_edit)
            if reasons:
                logger.warning("registry_edit_refused", actor=actor, reasons=reasons)
                raise LiveEditRefused("; ".join(reasons))
            registry, errors, warnings = check(after, strict=False, allowed_hosts=self._hosts)
            if registry is None:
                raise RegistryError(errors, warnings)
            changes = orjson.dumps(audit_diff(doc, after)).decode()
            keys, values = self._numbers(ResolvedRegistry(registry))
            reply = await self._edit(
                [self.doc_key, self.meta_key, self.audit_key, self.history_key, *keys],
                [
                    orjson.dumps(after).decode(),
                    int(meta.get("changes", "0")),
                    actor,
                    changes,
                    self._retention_ms,
                    *values,
                ],
            )
            if _text(reply[0]) == "ok":
                logger.info("llmq_registry_audit", actor=actor, op="edit", diff=changes)
                loaded = await self.load()
                assert loaded is not None
                return loaded
        raise RegistryConflict("the registry kept changing; try again")

    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> ResolvedRegistry:
        """Set dotted paths (`{"rpm": 400}` with `model=`, or from the root); None deletes."""

        def change(doc: dict[str, Any]) -> None:
            base = doc
            prefix: list[str] = []
            if model is not None:
                alias = self.current.registry.alias_of(model) if self._current else model
                prefix = ["models", alias]
                base = doc.get("models", {}).get(alias, {})
            for dotted, value in values.items():
                _set_path(doc, [*prefix, *split_path(base, dotted)], value)

        return await self.edit(change, actor=actor)

    async def set_enabled(
        self, model: str, deployment: str, enabled: bool, *, actor: str
    ) -> ResolvedRegistry:
        """Enable or disable one deployment (`account/name`) of a model."""
        account, _, name = deployment.partition("/")
        alias = self.current.registry.alias_of(model)
        path = ["models", alias, "deployments", account, "overrides", name, "enabled"]

        def change(doc: dict[str, Any]) -> None:
            _set_path(doc, path, None if enabled else False)

        return await self.edit(change, actor=actor)

    async def block(
        self, *, region: str | None = None, account: str | None = None, on: bool, actor: str
    ) -> ResolvedRegistry:
        """Block (or unblock) a region or an account for every model."""

        def change(doc: dict[str, Any]) -> None:
            routing = doc.setdefault("routing", {})
            for field, value in (("blocked_regions", region), ("blocked_accounts", account)):
                if value is None:
                    continue
                listed = [v for v in routing.get(field, []) if v != value]
                routing[field] = [*listed, value] if on else listed

        return await self.edit(change, actor=actor)

    # ---------------------------------------------------------------- history, rollback, diff

    async def log(self, count: int = 20, path_prefix: str = "") -> list[dict[str, Any]]:
        """Latest audit entries, newest first; `path_prefix` keeps entries that touch it."""
        entries = await self._client.xrevrange(self.audit_key, count=max(count * 5, count))
        out: list[dict[str, Any]] = []
        for entry_id, fields in entries or ():
            row = {_text(k): _text(v) for k, v in (fields or {}).items()}
            changes = orjson.loads(row.get("diff", "[]"))
            if path_prefix and not any(c["path"].startswith(path_prefix) for c in changes):
                continue
            out.append({"id": _text(entry_id), **row, "diff": changes})
            if len(out) == count:
                break
        return out

    async def versions(self) -> list[int]:
        """Published versions still kept for rollback, oldest first."""
        return sorted(int(_text(v)) for v in await self._client.hkeys(self.history_key))

    async def rollback(self, version: int, *, actor: str) -> PublishResult:
        """Publish the content of an earlier version as a new, higher version."""
        raw = await self._client.hget(self.history_key, str(version))
        if raw is None:
            raise ConfigError(f"version {version} is not kept any more")
        doc = orjson.loads(raw)
        _, meta = await self._read()
        doc["registry_version"] = int(meta.get("version", "0")) + 1
        return await self.publish(doc, actor=f"{actor} (rollback to {version})")

    async def diff_with_config(self) -> list[dict[str, Any]]:
        """What is live but not in this process's config (live edits a deploy would drop)."""
        doc, _ = await self._read()
        if self.config is None or doc is None:
            return []
        return [
            change
            for change in audit_diff(self.config, doc)
            if change["path"] != "registry_version"
        ]

    # ---------------------------------------------------------------- reload loop

    async def refresh(self) -> ResolvedRegistry | None:
        """One reload: load, and restore the config when the stored registry was lost.

        A missing or older stored registry while this process holds a newer one means the store
        was flushed or failed over: republish by the boot rule and alert (live edits are lost).
        """
        held = self._version
        loaded = await self.load()
        if self.config is None:
            return loaded
        try:
            doc, meta = await self._read()
        except (RedisError, OSError):
            return self._current
        stored = int(meta.get("version", "0")) if doc is not None else 0
        if doc is None or stored < held:
            logger.error("registry_reset_live_edits_lost", stored=stored, held=held)
            await self.publish(self.config, actor="restore")
        return self._current

    async def run(self, interval_secs: float) -> None:
        """Reload every `interval_secs` until cancelled; errors never end the loop."""
        while True:
            try:
                await self.refresh()
            except (RedisError, OSError, ConfigError) as exc:
                logger.warning("registry_reload_failed", error=str(exc))
            await self._sleep(interval_secs)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
