"""Process-level settings (plan §3.18 infrastructure table), read from the environment.

Everything about models lives in the registry; these are the stores, prefixes, the registry's
source and a few process limits. Every problem is reported at once; a removed 0.2 name (e.g.
`LLMQ_REDIS_HOST`) is an error that names its replacement.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from taskiq_llm_queue.errors import ConfigError

PREFIX = "LLMQ_"
# 0.2 names that are gone, with what replaces them (plan R44, review 3.10).
REMOVED = {
    "LLMQ_REDIS_HOST": "LLMQ_STORE_URL",
    "LLMQ_REDIS_PORT": "LLMQ_STORE_URL",
    "LLMQ_REDIS_PASSWORD": "LLMQ_STORE_URL",
    "LLMQ_REDIS_SSL": "LLMQ_STORE_URL",
    "LLMQ_QUEUE_REDIS_URL": "LLMQ_QUEUE_STORE_URL",
    "LLMQ_MODELS": "LLMQ_REGISTRY",
    "LLMQ_SETTINGS": "LLMQ_REGISTRY",
    "LLMQ_FALLBACK_CHAINS": "LLMQ_REGISTRY (model_fallback)",
    "LLMQ_ROUTING_DEFAULT": "LLMQ_REGISTRY (routing)",
    "LLMQ_WORKER_SLOTS": "LLMQ_REGISTRY (worker_slots)",
    "LLMQ_ROUTING_ENABLED": "LLMQ_REGISTRY (routing.kill_switch / queue_pct)",
}
LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")


@dataclass(frozen=True, slots=True)
class InfraSettings:
    """The process settings of one environment."""

    store_url: str
    queue_store_url: str | None = None
    key_prefix: str = "llmq"
    queue_prefix: str = ""
    registry: str | None = None
    registry_file: str | None = None
    registry_live_edit: str = "safe"
    base_url_allowed_hosts: tuple[str, ...] = ()
    cfg_reload_secs: float = 30.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 30
    status_ttl_secs: int = 1800
    audit_retention_days: int = 90
    lease_secs: float = 60.0
    caller_ttl_secs: int = 30
    drain_secs: float = 45.0
    local_limit_cap: int = 10
    metrics_sink: str = "emf"
    mode: str = "both"
    bedrock_endpoint_url: str | None = None
    sts_endpoint_url: str | None = None
    id_hash_salt: str | None = None
    environment: str = "dev"
    extra: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
    ) -> InfraSettings:
        """Read `LLMQ_*` from `environ` (default `os.environ`), secrets first when given."""
        env = dict(os.environ if environ is None else environ)

        def get(name: str, default: Any = None) -> Any:
            if secrets is not None:
                value = secrets(PREFIX + name)
                if value:
                    return value
            return env.get(PREFIX + name, default)

        problems = [
            f"{old} is removed in 0.3; use {new}" for old, new in REMOVED.items() if old in env
        ]
        store = get("STORE_URL")
        if not store:
            problems.append("LLMQ_STORE_URL is required (redis:// or rediss://)")
        elif not str(store).startswith(("redis://", "rediss://", "valkey://", "valkeys://")):
            problems.append("LLMQ_STORE_URL must be a redis:// or rediss:// URL")
        environment = str(get("ENVIRONMENT", "dev"))
        urls = (("LLMQ_STORE_URL", store), ("LLMQ_QUEUE_STORE_URL", get("QUEUE_STORE_URL")))
        for name, url in urls:
            if url and environment == "prod" and str(url).startswith(("redis://", "valkey://")):
                host = str(url).split("@")[-1].split(":")[0].split("/")[0].strip("/")
                if host not in LOCAL_HOSTS:
                    problems.append(f"{name} must use TLS (rediss://) in prod")
        registry, registry_file = get("REGISTRY"), get("REGISTRY_FILE")
        if registry and registry_file:
            problems.append("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        salt = get("ID_HASH_SALT")
        if environment == "prod" and not salt:
            problems.append("LLMQ_ID_HASH_SALT is required in prod")
        live_edit = str(get("REGISTRY_LIVE_EDIT", "safe"))
        if live_edit not in ("safe", "all", "off"):
            problems.append("LLMQ_REGISTRY_LIVE_EDIT must be safe, all or off")
        mode = str(get("MODE", "both"))
        if mode not in ("both", "caller", "worker"):
            problems.append("LLMQ_MODE must be both, caller or worker")

        def number(name: str, default: float, kind: type = float) -> Any:
            raw = get(name, default)
            try:
                value = kind(raw)
            except (TypeError, ValueError):
                problems.append(f"LLMQ_{name} must be a number")
                return default
            if value <= 0:
                problems.append(f"LLMQ_{name} must be above 0")
            return value

        settings = cls(
            store_url=str(store or ""),
            queue_store_url=get("QUEUE_STORE_URL"),
            key_prefix=str(get("KEY_PREFIX", "llmq")),
            queue_prefix=str(get("QUEUE_PREFIX", "")),
            registry=registry,
            registry_file=registry_file,
            registry_live_edit=live_edit,
            base_url_allowed_hosts=tuple(
                h.strip() for h in str(get("BASE_URL_ALLOWED_HOSTS", "")).split(",") if h.strip()
            ),
            cfg_reload_secs=number("CFG_RELOAD_SECS", 30.0),
            result_ttl_secs=number("RESULT_TTL_SECS", 300, int),
            result_dedupe_secs=number("RESULT_DEDUPE_SECS", 30, int),
            status_ttl_secs=number("STATUS_TTL_SECS", 1800, int),
            audit_retention_days=number("AUDIT_RETENTION_DAYS", 90, int),
            lease_secs=number("LEASE_SECS", 60.0),
            caller_ttl_secs=number("CALLER_TTL_SECS", 30, int),
            drain_secs=number("DRAIN_SECS", 45.0),
            local_limit_cap=number("LOCAL_LIMIT_CAP", 10, int),
            metrics_sink=str(get("METRICS_SINK", "emf")),
            mode=mode,
            bedrock_endpoint_url=get("BEDROCK_ENDPOINT_URL"),
            sts_endpoint_url=get("STS_ENDPOINT_URL"),
            id_hash_salt=salt,
            environment=environment,
        )
        if problems:
            raise ConfigError("invalid LLMQ settings:\n  " + "\n  ".join(problems))
        return settings

    def registry_document(self) -> str:
        """The registry JSON from `LLMQ_REGISTRY` or the file `LLMQ_REGISTRY_FILE`."""
        if self.registry:
            return self.registry
        if self.registry_file:
            return Path(self.registry_file).read_text(encoding="utf-8")
        raise ConfigError("no registry: set LLMQ_REGISTRY or LLMQ_REGISTRY_FILE")
