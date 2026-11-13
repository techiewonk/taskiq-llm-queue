"""Run the checks of one work step (plan §6) or of everything: lint, format, types, tests.

Every test run of the 0.3 work goes through this script, so a step is "done" only when
`python scripts/check.py <step>` passes. No step calls a real LLM: stack tests and samples
use fakellm (`python scripts/samples.py --live` runs the samples on real Bedrock;
`--store env` on the env file's store). `store` also runs a Docker failover drill.

    python scripts/check.py a1          # one step
    python scripts/check.py a1 a2       # several
    python scripts/check.py all         # every step built so far
    python scripts/check.py all --quick # the same, drills shortened (gates subset)
    python scripts/check.py soak3       # soaks (not in `all`): 3 min smoke, soak10, soak20
    python scripts/check.py soak20      # before a release
    python scripts/check.py --list      # what each step runs

Every check writes its whole output, as it runs, to `reports/check-<time>/<step>.<check>.log`
(git-ignored); the console shows one PASS / FAIL line per check with its log, and
`summary.log` there keeps the console. Follow a long drill with `tail -f` on its log.
"""

from __future__ import annotations

import argparse
import datetime
import os
import re
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
    quick: tuple[str, ...] = ()  # arguments added to `extra` commands with --quick
    in_all: bool = True


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
    "a11": Step(
        "Health: auto-disable with safeguards, probes, credentials per process",
        sources=("src/taskiq_llm_queue/engine", "tools/fakellm/fakellm"),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a12": Step(
        "Status, health, alerts, metrics, call record, drain, HTTP handlers, CLI, settings",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_ops.py",),
        needs_stack=True,
    ),
    "a13": Step(
        "Embeddings: batching, provider bodies, vectors in order, large results in S3",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_embed.py",),
        needs_stack=True,
    ),
    "a14": Step(
        "Front door (LLMQueue.from_env), jobs, sync calls, LangChain adapters",
        sources=(
            "src/taskiq_llm_queue/llm_queue.py",
            "src/taskiq_llm_queue/langchain",
            "src/taskiq_llm_queue/http_transport.py",
            "src/taskiq_llm_queue/engine/jobs.py",
            "src/taskiq_llm_queue/tuning.py",
        ),
        tests=("tests/integration/engine/test_front.py", "tests/unit/test_tuning.py"),
        needs_stack=True,
    ),
    "a15": Step(
        "Assume-role accounts: STS AssumeRole per account, refresh, credential failures",
        sources=("src/taskiq_llm_queue/clients", "src/taskiq_llm_queue/engine"),
        tests=(
            "tests/unit/clients/test_assume_role.py",
            "tests/integration/engine/test_runtime.py::test_assume_role_failure_is_a_credential_failure",
            "tests/integration/engine/test_runtime.py::test_credential_failure_is_per_process",
        ),
        needs_stack=True,
    ),
    "a16": Step(
        "Latency-aware exclusion: slow deployments become last choices",
        sources=("src/taskiq_llm_queue/engine",),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a17": Step(
        "Streaming to the caller: chunk streams, flush window, cuts, worker lost, idle",
        sources=(
            "src/taskiq_llm_queue/engine",
            "src/taskiq_llm_queue/clients",
            "src/taskiq_llm_queue/llm_queue.py",
            "tools/fakellm/fakellm",
        ),
        tests=("tests/integration/engine/test_stream.py",),
        needs_stack=True,
    ),
    "store": Step(
        "Store failover: READONLY reconnect, read-only readiness, preStop pause, HTTP router",
        sources=(
            "src/taskiq_llm_queue/engine",
            "src/taskiq_llm_queue/llm_queue.py",
            "scripts/failover.py",
        ),
        tests=("tests/integration/engine/test_store_and_http.py",),
        needs_stack=True,
        extra=(("scripts/failover.py",),),
    ),
    "load": Step(
        "Load drill: 2,000 calls over three workers; a burst far over the RPM budget",
        sources=("scripts/load.py",),
        tests=(),
        needs_stack=True,
        extra=(("scripts/load.py",),),
    ),
    "gates": Step(
        "Plan gates B17-B97 on worker processes: budgets, backlog, kills, store outages (~20 min)",
        sources=("scripts/gates.py", "scripts/gates_fleet.py"),
        tests=(),
        needs_stack=True,
        extra=(("scripts/gates.py",),),
        quick=("--quick",),
    ),
    "scenarios": Step(
        "Fault scenarios (store, worker, provider, registry, caller, pairs) x loads, by calls",
        sources=("scripts/scenarios.py",),
        tests=(),
        needs_stack=True,
        extra=(("scripts/scenarios.py", "all", "--jobs", "1"),),  # one at a time
        quick=("--load", "under"),  # one load per scenario
    ),
    "samples": Step(
        "Usage samples (samples/*.py) run as app code against fakellm",
        sources=("samples", "scripts/samples.py"),
        tests=(),
        needs_stack=True,
        extra=(("scripts/samples.py",),),
    ),
    **{
        f"soak{minutes}": Step(
            f"Soak {minutes} min ({use}): 8 worker processes, chaos (kill, store block, "
            "live edit, restart); leaks, drift",
            sources=("scripts/soak.py",),
            tests=(),
            needs_stack=True,
            extra=(("scripts/soak.py", "--minutes", str(minutes)),),
            in_all=False,  # long; run on purpose
        )
        for minutes, use in ((3, "smoke"), (10, "after an engine change"), (20, "release"))
    },
}

LOG_DIR = ROOT / "reports" / f"check-{datetime.datetime.now():%Y%m%d-%H%M%S}"


def say(line: str) -> None:
    """Print a console line and keep it in the run's `summary.log`."""
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "summary.log").open("a", encoding="utf-8") as summary:
        summary.write(line + "\n")


def stack_up() -> str | None:
    """None when the Docker stack answers (Valkey, Redis, fakellm); otherwise why not."""
    import socket

    for name, port in (("Valkey", 6379), ("Redis", 6380), ("fakellm", 8080)):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
        except OSError as exc:
            return f"{name} on 127.0.0.1:{port} is not reachable ({exc}); run `make up`"
    return None


def _run(step: str, label: str, cmd: list[str]) -> bool:
    """Run one check with its output written live to its own log; print PASS / FAIL."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"{step}.{re.sub(r'[^A-Za-z0-9_.-]+', '_', label).strip('_')}.log"
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",  # the log fills as the check runs
        "LLMQ_DRILL_LOG_DIR": str(LOG_DIR / f"{step}-processes"),  # workers' own logs
    }
    started = time.monotonic()
    with log.open("w", encoding="utf-8") as out_file:
        out_file.write(f"$ {' '.join(cmd)}\n\n")
        out_file.flush()
        proc = subprocess.run(
            cmd, cwd=ROOT, stdout=out_file, stderr=subprocess.STDOUT, env=env, check=False
        )
    ok = proc.returncode == 0
    secs = time.monotonic() - started
    say(f"  {'PASS' if ok else 'FAIL'}  {label} ({secs:.1f}s)  {log.relative_to(ROOT)}")
    if not ok:
        out = log.read_text(encoding="utf-8", errors="replace").rstrip()
        # Verdict lines first (a drill's log noise must not push them out of the tail).
        verdicts = [
            line
            for line in out.splitlines()
            if line.startswith(("[", "PASS", "FAIL", "ALL PASS"))
            or (line.startswith("  ") and not line.startswith("   ") and "File " not in line)
        ]
        if verdicts:
            say("\n".join(verdicts[-80:]))
        say(out[-3000:])
    return ok


def run_step(name: str, step: Step, *, quick: bool = False) -> bool:
    """Run every check of one step; print one line per check."""
    say(f"[{name}] {step.title}")
    if step.needs_stack and (why := stack_up()):
        say(f"  FAIL  stack: {why}")
        return False
    paths = list(dict.fromkeys([*step.sources, *(t.split("::")[0] for t in step.tests)]))
    results = [
        _run(name, "ruff check", [PY, "-m", "ruff", "check", *paths]),
        _run(name, "ruff format --check", [PY, "-m", "ruff", "format", "--check", *paths]),
        _run(name, "mypy", [PY, "-m", "mypy", "--no-incremental", *paths]),
    ]
    if step.tests:
        # -rA: a line per test with its outcome, so the log shows what ran, not only failures.
        pytest = [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rA", *step.tests]
        results.append(_run(name, "pytest", pytest))
    for cmd in step.extra:
        full = [*cmd, *(step.quick if quick else ())]
        results.append(_run(name, " ".join(full), [PY, *full]))
    return all(results)


def main() -> int:
    """Parse the step names and run them; exit 1 if any check fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("steps", nargs="*", help="step names (a1, a2, ...) or all")
    parser.add_argument("--list", action="store_true", help="show the steps and exit")
    parser.add_argument("--quick", action="store_true", help="gates without the slow three")
    args = parser.parse_args()
    if args.list or not args.steps:
        for name, step in STEPS.items():
            note = "" if step.in_all else "  [not in all]"
            print(f"{name:5} {step.title}  ({', '.join(step.tests)}){note}")
        return 0
    names = [n for n, s in STEPS.items() if s.in_all] if args.steps == ["all"] else args.steps
    unknown = [n for n in names if n not in STEPS]
    if unknown:
        print(f"unknown step(s): {', '.join(unknown)}; see --list")
        return 2
    say(f"logs: {LOG_DIR.relative_to(ROOT)}")
    results = {n: run_step(n, STEPS[n], quick=args.quick) for n in names}  # all, then judge
    failed = [n for n, ok in results.items() if not ok]
    say("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    say(f"logs: {LOG_DIR.relative_to(ROOT)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
