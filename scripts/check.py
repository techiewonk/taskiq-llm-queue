"""Run the checks of one work step (plan §6) or of everything: lint, format, types, tests.

Every test run of the 0.3 work goes through this script, so a step is "done" only when
`python scripts/check.py <step>` passes. No step calls a real LLM: stack tests use fakellm.

    python scripts/check.py a1          # one step
    python scripts/check.py a1 a2       # several
    python scripts/check.py all         # every step built so far
    python scripts/check.py --list      # what each step runs
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / "python")


@dataclass(frozen=True)
class Step:
    """What one step checks: source paths (lint and types) and test paths (pytest)."""

    title: str
    sources: tuple[str, ...]
    tests: tuple[str, ...]
    needs_stack: bool = False
    extra: tuple[tuple[str, ...], ...] = field(default=())


STEPS: dict[str, Step] = {
    "a0": Step(
        "Test prerequisites: fakellm accounts, TPM, speed, usage-missing, hang, streaming",
        sources=("tools/fakellm/fakellm", "scripts"),
        tests=("tests/unit/fakellm",),
    ),
    "a1": Step(
        "Registry schema, settings, validation, resolve",
        sources=("src/taskiq_llm_queue/registry",),
        tests=("tests/unit/registry",),
    ),
    "a2": Step(
        "Registry store: version rule, live edits, audit, rollback, reload",
        sources=("src/taskiq_llm_queue/registry",),
        tests=("tests/integration/registry",),
        needs_stack=True,
    ),
    "a3": Step(
        "Clients per account and region; providers on routes; attempt timers",
        sources=("src/taskiq_llm_queue/clients",),
        tests=("tests/unit/clients",),
    ),
    "a4": Step(
        "Engine keys (schema 3), message format v2, claim / result / status records",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_requests.py",),
        needs_stack=True,
    ),
    "a5": Step(
        "Stream queue: order, return in place, failures, dead letters, waiting limit",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_queue.py",),
        needs_stack=True,
    ),
    "a6": Step(
        "Admission: PICK_TAKE, FINISH, UNDO, RENEW, SWEEP (budgets, slots, waiters, strategies)",
        sources=("src/taskiq_llm_queue/engine", "src/taskiq_llm_queue/registry"),
        tests=("tests/integration/engine/test_admission.py",),
        needs_stack=True,
    ),
    "a7": Step(
        "Token estimate and refund rules; inference defaults and limits per provider",
        sources=(
            "src/taskiq_llm_queue/engine/tokens.py",
            "src/taskiq_llm_queue/engine/inference.py",
        ),
        tests=("tests/unit/engine",),
    ),
    "a8": Step(
        "Engine end to end: caller, worker loop, held calls, retries, fallback (fakellm)",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a9": Step(
        "Hooks (balancer, fallback policy, admission, sticky key), fallback by cause, params",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/unit/engine", "tests/integration/engine/test_runtime.py"),
        needs_stack=True,
    ),
    "a10": Step(
        "Operations: direct path, store_down, cancel, caller gone, shutdown, store reset",
        sources=("src/taskiq_llm_queue/engine", "src/taskiq_llm_queue/clients"),
        tests=("tests/integration/engine",),
        needs_stack=True,
    ),
}


def stack_up() -> str | None:
    """None when the Docker stack's Valkey answers; otherwise why not."""
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=1):
            return None
    except OSError as exc:
        return f"Valkey on 127.0.0.1:6379 is not reachable ({exc}); run `make up`"


def _run(label: str, cmd: list[str]) -> bool:
    started = time.monotonic()
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
    ok = proc.returncode == 0
    secs = time.monotonic() - started
    print(f"  {'PASS' if ok else 'FAIL'}  {label} ({secs:.1f}s)")
    if not ok:
        print((proc.stdout + proc.stderr).rstrip()[-4000:])
    return ok


def run_step(name: str, step: Step) -> bool:
    """Run every check of one step; print one line per check."""
    print(f"[{name}] {step.title}")
    if step.needs_stack and (why := stack_up()):
        print(f"  FAIL  stack: {why}")
        return False
    paths = [*step.sources, *step.tests]
    results = [
        _run("ruff check", [PY, "-m", "ruff", "check", *paths]),
        _run("ruff format --check", [PY, "-m", "ruff", "format", "--check", *paths]),
        _run("mypy", [PY, "-m", "mypy", "--no-incremental", *paths]),
        _run(
            "pytest",
            [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rs", *step.tests],
        ),
    ]
    results += [_run(" ".join(cmd), [PY, *cmd]) for cmd in step.extra]
    return all(results)


def main() -> int:
    """Parse the step names and run them; exit 1 if any check fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("steps", nargs="*", help="step names (a1, a2, ...) or all")
    parser.add_argument("--list", action="store_true", help="show the steps and exit")
    args = parser.parse_args()
    if args.list or not args.steps:
        for name, step in STEPS.items():
            print(f"{name:5} {step.title}  ({', '.join(step.tests)})")
        return 0
    names = list(STEPS) if args.steps == ["all"] else args.steps
    unknown = [n for n in names if n not in STEPS]
    if unknown:
        print(f"unknown step(s): {', '.join(unknown)}; see --list")
        return 2
    results = [run_step(n, STEPS[n]) for n in names]  # run every step, then judge
    ok = all(results)
    print("ALL PASS" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
