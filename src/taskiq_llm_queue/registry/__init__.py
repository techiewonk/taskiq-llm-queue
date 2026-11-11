"""The model registry: one document (`LLMQ_REGISTRY`) for accounts, models and their settings.

`load_registry()` parses and checks it and returns a `ResolvedRegistry` that answers "which
deployments does this model have" and "what is this setting here, and where did it come from".
"""

from __future__ import annotations

from taskiq_llm_queue.registry.resolve import Deployment, Effective, ResolvedRegistry
from taskiq_llm_queue.registry.schema import (
    Account,
    ModelEntry,
    RegionEntry,
    Registry,
    Routing,
    register_account_kind,
)
from taskiq_llm_queue.registry.settings import SETTINGS, Spec
from taskiq_llm_queue.registry.validate import RegistryError, check, load_registry

__all__ = [
    "SETTINGS",
    "Account",
    "Deployment",
    "Effective",
    "ModelEntry",
    "RegionEntry",
    "Registry",
    "RegistryError",
    "ResolvedRegistry",
    "Routing",
    "Spec",
    "check",
    "load_registry",
    "register_account_kind",
]
