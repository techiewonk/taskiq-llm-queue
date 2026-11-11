"""Helpers for the client and provider tests: fakellm on sys.path and the test registry.

Each AWS account of the registry signs with its own access key id (the account name), which
fakellm reads as the account, so per-account quotas are testable on one endpoint.
"""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

TOOLS = Path(__file__).resolve().parents[3] / "tools" / "fakellm"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from fakellm import Engine, Scenario, create_app  # noqa: E402  # needs the sys.path entry

__all__ = ["Engine", "Scenario", "StartFake", "create_app", "registry_doc"]

StartFake = Callable[..., Awaitable[tuple[str, Engine]]]


def registry_doc(fake_url: str) -> dict[str, Any]:
    """Two AWS accounts and one OpenAI-compatible account, a chat and an embedding model."""
    return {
        "registry_version": 1,
        "accounts": {
            "aws-a": {"kind": "aws"},
            "aws-b": {"kind": "aws"},
            "vllm": {"kind": "openai", "base_url": f"{fake_url}/v1", "api_key_ref": "VLLM_KEY"},
        },
        "models": {
            "gpt120b": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "openai.gpt-oss-120b-1:0",
                "deployments": {
                    "aws-a": {
                        "confirmed": ["ap-south-1"],
                        "fallback": [
                            {
                                "name": "apac",
                                "region": "ap-south-1",
                                "profile_id": "apac.openai.gpt-oss-120b-1:0",
                                "routes_to": ["ap-south-1"],
                            }
                        ],
                    },
                    "aws-b": {"confirmed": ["ap-south-1"]},
                },
            },
            "titan": {
                "type": "embedding",
                "provider": "bedrock",
                "model_id": "amazon.titan-embed-text-v2:0",
                "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
            },
            "local": {
                "type": "chat",
                "provider": "openai",
                "model_id": "gpt-oss-120b",
                "deployments": {"vllm": {"confirmed": ["dc1"]}},
            },
        },
    }
