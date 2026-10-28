"""fakellm: a scriptable fake Bedrock runtime + OpenAI-compatible server for tests.

Run with `python -m fakellm`; embed in tests with `create_app(Scenario.from_mapping(...))`.
Not part of the taskiq-llm-queue wheel; it backs the Docker stack and the scenario suite.
"""

from __future__ import annotations

from fakellm.app import create_app
from fakellm.behaviour import Engine, KeyStats, Outcome
from fakellm.errors import ERRORS, ErrorSpec
from fakellm.scenario import Behaviour, Phase, Scenario, ScenarioError, load_scenario

__all__ = [
    "ERRORS",
    "Behaviour",
    "Engine",
    "ErrorSpec",
    "KeyStats",
    "Outcome",
    "Phase",
    "Scenario",
    "ScenarioError",
    "create_app",
    "load_scenario",
]
