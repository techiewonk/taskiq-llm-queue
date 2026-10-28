"""Shared test types for fakellm tests; importing it puts tools/fakellm on sys.path.

fakellm lives in tools/fakellm (not in the wheel). Test modules import their types from
here rather than from conftest, whose module name can clash with other test folders.
"""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

TOOLS_DIR = Path(__file__).resolve().parents[3] / "tools" / "fakellm"
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from fakellm import Engine, Scenario, create_app  # noqa: E402  # needs the sys.path entry above


@dataclass(slots=True)
class Fake:
    """A running fakellm: its base URL and engine (for direct stats/clock assertions)."""

    url: str
    engine: Engine


__all__ = ["Engine", "Fake", "MakeBedrock", "Scenario", "StartFake", "create_app"]

StartFake = Callable[..., Awaitable[Fake]]
MakeBedrock = Callable[..., Awaitable[BedrockRuntimeClient]]
