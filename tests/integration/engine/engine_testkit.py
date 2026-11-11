"""Shared helpers of the engine tests: fakellm on sys.path, the test registry, constants."""

from __future__ import annotations

import copy
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

TOOLS = Path(__file__).resolve().parents[3] / "tools" / "fakellm"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))
from fakellm import Engine as FakeEngine  # noqa: E402  # needs the sys.path entry
from fakellm import Scenario, create_app  # noqa: E402

from taskiq_llm_queue.engine.runtime import Engine  # noqa: E402

GPT = "openai.gpt-oss-120b-1:0"
SMALL = "openai.gpt-oss-20b-1:0"
TITAN = "amazon.titan-embed-text-v2:0"
CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}


def registry(models: dict[str, Any] | None = None) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "registry_version": 1,
        "defaults": {"rate_window": "fixed", "timeout_secs": 10, "attempt_timeout_secs": 2},
        "accounts": {"aws-a": {"kind": "aws"}},
        "models": {
            "big": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": GPT,
                "retry_base_secs": 0.05,
                "deployments": {"aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"]}},
            },
            "small": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": SMALL,
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            },
            "titan": {
                "type": "embedding",
                "provider": "bedrock",
                "model_id": TITAN,
                "inference": {"dimensions": 256},
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            },
        },
    }
    for alias, extra in (models or {}).items():
        doc["models"][alias].update(copy.deepcopy(extra))
    return doc


Make = Callable[..., Awaitable[tuple[Engine, FakeEngine]]]


def stats(fake: FakeEngine, key: str) -> dict[str, int]:
    """fakellm counters of one key (empty when it was never called)."""
    models = fake.snapshot()["models"]
    assert isinstance(models, dict)
    return dict(models.get(key, {}))


__all__ = [
    "CHAT",
    "GPT",
    "SMALL",
    "TITAN",
    "FakeEngine",
    "Make",
    "Scenario",
    "create_app",
    "registry",
    "stats",
]
