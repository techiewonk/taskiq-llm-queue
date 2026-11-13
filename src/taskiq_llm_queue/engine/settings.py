"""Process-level settings (plan §3.18 infrastructure table), read from the environment.

Everything about models lives in the registry; these are the stores, prefixes, the registry's
source and a few process limits. Every problem is reported at once; a removed 0.2 name (e.g.
`LLMQ_REDIS_HOST`) is an error that names its replacement.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

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
STORE_SCHEMES = ("redis", "rediss", "valkey", "valkeys")
TLS_SCHEMES = ("rediss", "valkeys")
METRICS_SINKS = ("emf", "prometheus", "none")
# Environments where the store must use verified TLS and ids a keyed hash (fail closed: any
# name that is not clearly a developer's own is treated as holding real data).
DEV_ENVIRONMENTS = ("dev", "local", "test", "ci")


def _store_url_problems(name: str, url: str, strict: bool) -> list[str]:
    """What is wrong with one store URL; `strict` (not a dev environment) requires verified TLS."""
    parts = urlsplit(url)
    if parts.scheme not in STORE_SCHEMES:
        return [f"{name} must be a redis:// or rediss:// URL"]
    if not strict:
        return []
    query = {k.lower(): v for k, v in parse_qs(parts.query).items()}
    if parts.scheme not in TLS_SCHEMES and (parts.hostname or "") not in LOCAL_HOSTS:
        return [f"{name} must use TLS (rediss://) outside dev"]
    if query.get("ssl_cert_reqs", ["required"])[-1].lower() in ("none", "optional"):
        return [f"{name} must verify the store's certificate outside dev (ssl_cert_reqs)"]
    if query.get("ssl_check_hostname", ["true"])[-1].lower() in ("false", "0", "no"):
        return [f"{name} must check the store's host name outside dev (ssl_check_hostname)"]
    return []


@dataclass(frozen=True, slots=True)
class InfraSettings:
    """The process settings of one environment."""

    store_url: str = field(repr=False)  # holds the password
    queue_store_url: str | None = field(default=None, repr=False)
    store_max_connections: int = 200
    key_prefix: str = "llmq"
    queue_prefix: str = ""
    registry: str | None = None
    registry_file: str | None = None
    registry_live_edit: str = "safe"
    base_url_allowed_hosts: tuple[str, ...] = ()
    cfg_reload_secs: float = 30.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 0
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
    queue_failover_secs: float = 5.0
    queue_wait_replicas: int = 0
    queue_wait_ms: int = 100
    debug_capture_from_config: bool = False

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
        environment = str(get("ENVIRONMENT", "dev")).strip().lower()
        if environment == "production":
            environment = "prod"
        strict = environment not in DEV_ENVIRONMENTS
        if not store:
            problems.append("LLMQ_STORE_URL is required (redis:// or rediss://)")
        urls = (("LLMQ_STORE_URL", store), ("LLMQ_QUEUE_STORE_URL", get("QUEUE_STORE_URL")))
        for name, url in urls:
            if url:
                problems += _store_url_problems(name, str(url), strict)
        registry, registry_file = get("REGISTRY"), get("REGISTRY_FILE")
        if registry and registry_file:
            problems.append("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        salt = get("ID_HASH_SALT")
        if strict and not salt:
            problems.append(f"LLMQ_ID_HASH_SALT is required outside dev ({environment})")
        debug_capture = str(get("DEBUG_CAPTURE_FROM_CONFIG", "false")).strip().lower()
        if debug_capture not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            problems.append("LLMQ_DEBUG_CAPTURE_FROM_CONFIG must be true or false")
        capture = debug_capture in ("true", "1", "yes", "on")
        if capture and environment == "prod":
            problems.append("LLMQ_DEBUG_CAPTURE_FROM_CONFIG is refused in prod (prompt text)")
        sink = str(get("METRICS_SINK", "emf"))
        if sink not in METRICS_SINKS:
            problems.append("LLMQ_METRICS_SINK must be emf, prometheus or none")
        live_edit = str(get("REGISTRY_LIVE_EDIT", "safe"))
        if live_edit not in ("safe", "all", "off"):
            problems.append("LLMQ_REGISTRY_LIVE_EDIT must be safe, all or off")
        mode = str(get("MODE", "both"))
        if mode not in ("both", "caller", "worker"):
            problems.append("LLMQ_MODE must be both, caller or worker")

        def number(name: str, default: float, kind: type = float, *, zero: bool = False) -> Any:
            raw = get(name, default)
            try:
                value = kind(raw)
            except (TypeError, ValueError):
                problems.append(f"LLMQ_{name} must be a number")
                return default
            if not math.isfinite(value):
                problems.append(f"LLMQ_{name} must be a finite number")
                return default
            if value < 0 or (value == 0 and not zero):
                problems.append(f"LLMQ_{name} must be {'0 or more' if zero else 'above 0'}")
            return value

        settings = cls(
            store_url=str(store or ""),
            queue_store_url=get("QUEUE_STORE_URL"),
            store_max_connections=number("STORE_MAX_CONNECTIONS", 200, int),
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
            result_dedupe_secs=number("RESULT_DEDUPE_SECS", 0, int, zero=True),
            status_ttl_secs=number("STATUS_TTL_SECS", 1800, int),
            audit_retention_days=number("AUDIT_RETENTION_DAYS", 90, int),
            lease_secs=number("LEASE_SECS", 60.0),
            caller_ttl_secs=number("CALLER_TTL_SECS", 30, int),
            drain_secs=number("DRAIN_SECS", 45.0),
            local_limit_cap=number("LOCAL_LIMIT_CAP", 10, int),
            metrics_sink=sink,
            mode=mode,
            bedrock_endpoint_url=get("BEDROCK_ENDPOINT_URL"),
            sts_endpoint_url=get("STS_ENDPOINT_URL"),
            id_hash_salt=salt,
            environment=environment,
            queue_failover_secs=number("QUEUE_FAILOVER_SECS", 5.0, zero=True),
            queue_wait_replicas=number("QUEUE_WAIT_REPLICAS", 0, int, zero=True),
            queue_wait_ms=number("QUEUE_WAIT_MS", 100, int),
            debug_capture_from_config=capture,
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
