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
TASKS["test"] = TASKS["test-unit"] + TASKS["test-int"]
TASKS["check"] = TASKS["lint"] + TASKS["typecheck"] + TASKS["test-unit"]


def main(argv: list[str]) -> int:
    """Run the named tasks in order; print usage and return 2 for an unknown task."""
    unknown = [name for name in argv if name not in TASKS]
    if not argv or unknown:
        print(f"usage: llmq_dev.py <{'|'.join(sorted(TASKS))}> ...  unknown: {unknown}")
        return 2
    for name in argv:
        for cmd in TASKS[name]:
            print(f"$ {' '.join(cmd)}", flush=True)
            code = subprocess.call(cmd, cwd=ROOT)
            if code:
                return code
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
