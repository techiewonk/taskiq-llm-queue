"""Scenario harness (release gate) for taskiq-llm-queue: S01-S52 against a local or remote stack.

Run `python -m tools.scenarios run --tag fast` from the package root. Everything the scenarios
need from the library goes through `tools.scenarios.api`, so API changes are edited in one place.
"""

from __future__ import annotations
