"""One pytest test per registered scenario; xfail/skip follow the scenario's own markers."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from tools.scenarios import harness
from tools.scenarios.env import Settings, ensure_import_paths

ensure_import_paths()
harness.load_all()
_TAG = os.environ.get("SCN_TAG", "all")
_SPECS = harness.select([], _TAG)


def _marks(spec: harness.Scenario) -> list[pytest.MarkDecorator]:
    marks = [pytest.mark.scenario]
    if "slow" in spec.tags:
        marks.append(pytest.mark.slow)
    if spec.skip:
        marks.append(pytest.mark.skip(reason=spec.skip))
    return marks


@pytest.fixture(scope="session")
def run_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("scenario-runs")


@pytest.mark.parametrize("spec", [pytest.param(s, id=s.id, marks=_marks(s)) for s in _SPECS])
def test_scenario(spec: harness.Scenario, run_dir: Path) -> None:
    result = asyncio.run(harness.run_many([spec], Settings(), run_dir, echo=False))[0]
    if result.status == "skip":
        pytest.skip(result.message)
    if result.status == "xfail":
        pytest.xfail(result.message)
    assert result.status in ("pass", "xpass"), f"{result.status}: {result.message}"
