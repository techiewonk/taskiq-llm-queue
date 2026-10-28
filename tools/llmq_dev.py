"""Cross-platform dev task runner, so Windows users don't need `make`.

Run `uv run python tools/llmq_dev.py <task>`; the Makefile calls the same tasks. Each task is a
list of commands run in order from the package root, stopping at the first failure.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "docker/compose.yaml"]
PY = [sys.executable, "-m"]

TASKS: dict[str, list[list[str]]] = {
    "up": [[*COMPOSE, "up", "-d", "--wait"]],
    "down": [[*COMPOSE, "down"]],
    "logs": [[*COMPOSE, "logs", "--tail", "100"]],
    "lint": [[*PY, "ruff", "check", "."], [*PY, "ruff", "format", "--check", "."]],
    "format": [[*PY, "ruff", "format", "."], [*PY, "ruff", "check", "--fix", "."]],
    "typecheck": [[*PY, "mypy"]],
    "test-unit": [[*PY, "pytest", "tests/unit", "-q"]],
    "test-int": [[*PY, "pytest", "tests/integration", "-q", "-m", "integration"]],
    "capability": [
        [
            sys.executable,
            "tests/capability/check_sqs_capabilities.py",
            "--endpoint",
            "http://localhost:9324",
            "--name",
            "ElasticMQ",
        ],
    ],
}
# Scenario harness (docs/TESTING.md section 3); these tasks take extra arguments.
SCN = [sys.executable, "-m", "tools.scenarios"]
TASKS["up-scn"] = [[*COMPOSE, "--profile", "scn", "up", "-d", "--wait", "valkey-scn", "sqs-scn"]]
TASKS["down-scn"] = [[*COMPOSE, "--profile", "scn", "stop", "valkey-scn", "sqs-scn"]]
TASKS["scenarios"] = TASKS["up-scn"] + [[*SCN, "run"]]
TASKS["scenario"] = TASKS["scenarios"]
TASKS["load"] = TASKS["up-scn"] + [[*SCN, "load"]]
TASKS["chaos"] = [[*SCN, "chaos"]]
TASKS["test-scenarios"] = TASKS["up-scn"] + [[*PY, "pytest", "tests/scenarios", "-m", "scenario"]]
# Tasks whose last command receives the remaining command-line arguments.
ARG_TASKS = frozenset({"scenarios", "scenario", "load", "chaos", "test-scenarios"})
TASKS["test"] = TASKS["test-unit"] + TASKS["test-int"]
TASKS["check"] = TASKS["lint"] + TASKS["typecheck"] + TASKS["test-unit"]


def main(argv: list[str]) -> int:
    """Run the named tasks in order; print usage and return 2 for an unknown task.

    A task in `ARG_TASKS` must come first and takes every following argument, e.g.
    `llmq_dev.py scenarios S01 S35 --report out.json`.
    """
    if argv and argv[0] in ARG_TASKS:
        cmds = [list(c) for c in TASKS[argv[0]]]
        cmds[-1] += argv[1:]
        return _run(cmds)
    unknown = [name for name in argv if name not in TASKS]
    if not argv or unknown:
        print(f"usage: llmq_dev.py <{'|'.join(sorted(TASKS))}> ...  unknown: {unknown}")
        return 2
    return _run([cmd for name in argv for cmd in TASKS[name]])


def _run(cmds: list[list[str]]) -> int:
    """Run commands in order from the package root; stop at the first failure."""
    for cmd in cmds:
        print(f"$ {' '.join(cmd)}", flush=True)
        code = subprocess.call(cmd, cwd=ROOT)
        if code:
            return code
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
