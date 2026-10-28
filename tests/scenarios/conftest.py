"""Scenario tests (release gate): need the `scn` compose profile; never part of default runs.

Run: `uv run pytest tests/scenarios -m scenario` (or `make test-scenarios`). Each test runs one
scenario through the same runner as `python -m tools.scenarios run`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "scenario: release-gate scenario (needs the scn stack)")
    config.addinivalue_line("markers", "slow: scenario in the slow set")
