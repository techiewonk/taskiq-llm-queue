"""Child processes of the harness: scenario workers and the managed fakellm probe.

Workers are separate OS processes so "kill a pod" is a real hard kill and peak RSS is per pod.
Each one writes its stats file and log into the run directory.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import orjson

from taskiq_llm_queue import QueueConfig
from tools.scenarios import api
from tools.scenarios.env import ROOT, Settings


class WorkerProc:
    """One `tools.scenarios.worker` process with its stats, control and log files."""

    def __init__(
        self,
        settings: Settings,
        config: QueueConfig,
        name: str,
        run_dir: Path,
        *,
        log_level: str = "WARNING",
        drain_secs: float | None = None,
    ) -> None:
        """Prepare paths; `start()` launches the process."""
        self.settings = settings
        self.name = name
        self.dir = run_dir
        self.config_path = run_dir / f"{name}.config.json"
        self.stats_path = run_dir / f"{name}.stats.json"
        self.control_path = run_dir / f"{name}.control"
        self.log_path = run_dir / f"{name}.log"
        self.config_path.write_bytes(api.config_to_json(config))
        self.log_level = log_level
        self.drain_secs = drain_secs
        self.proc: subprocess.Popen[bytes] | None = None
        self.last_stats: dict[str, Any] = {}

    def start(self) -> None:
        """Launch the worker process."""
        for path in (self.stats_path, self.control_path):
            path.unlink(missing_ok=True)
        cmd = [
            self.settings.python,
            "-m",
            "tools.scenarios.worker",
            "--name",
            self.name,
            "--config",
            str(self.config_path),
            "--stats",
            str(self.stats_path),
            "--control",
            str(self.control_path),
            "--log-level",
            self.log_level,
        ]
        if self.drain_secs is not None:
            cmd += ["--drain", str(self.drain_secs)]
        log = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            cmd, cwd=ROOT, env=self.settings.worker_env(), stdout=log, stderr=log
        )
        log.close()

    async def wait_ready(self, timeout_secs: float = 30.0) -> None:
        """Wait for the first stats file (the queue started)."""
        deadline = time.monotonic() + timeout_secs
        while not self.stats_path.exists():
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f"worker {self.name} exited: {self.tail_log()}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"worker {self.name} not ready: {self.tail_log()}")
            await asyncio.sleep(0.2)

    def stats(self) -> dict[str, Any]:
        """Latest stats the worker wrote (kept after it exits)."""
        with contextlib.suppress(OSError, ValueError):
            self.last_stats = orjson.loads(self.stats_path.read_bytes())
        return self.last_stats

    def command(self, word: str) -> None:
        """Send `stop`, `pause` or `resume`."""
        self.control_path.write_text(word, encoding="utf-8")

    @property
    def alive(self) -> bool:
        """True while the process runs."""
        return self.proc is not None and self.proc.poll() is None

    async def stop(self, timeout_secs: float = 60.0) -> float:
        """Graceful stop (drain); hard kill after the timeout. Returns seconds taken."""
        t0 = time.monotonic()
        if not self.alive:
            return 0.0
        self.command("stop")
        deadline = t0 + timeout_secs
        while self.alive and time.monotonic() < deadline:  # noqa: ASYNC110  # polls a child process
            await asyncio.sleep(0.1)
        if self.alive:
            self.kill()
        self.stats()
        return time.monotonic() - t0

    def kill(self) -> None:
        """Hard kill (no drain, no release): the pod-crash chaos."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)

    def tail_log(self, lines: int = 20) -> str:
        """Last lines of the worker log."""
        with contextlib.suppress(OSError):
            return "\n".join(
                self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
            )
        return ""


class FakellmProc:
    """The managed fakellm probe (`tools.scenarios.fakellm_probe`) on the harness port."""

    def __init__(self, settings: Settings, run_dir: Path) -> None:
        """Prepare the log path; `start()` launches it."""
        self.settings = settings
        self.log_path = run_dir / "fakellm.log"
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        """Launch fakellm with the probe middleware."""
        log = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            [
                self.settings.python,
                "-m",
                "tools.scenarios.fakellm_probe",
                "--host",
                os.environ.get("SCN_FAKELLM_BIND", "127.0.0.1"),  # 0.0.0.0 for container workers
                "--port",
                str(self.settings.fakellm_port),
            ],
            cwd=ROOT,
            env=self.settings.worker_env(),
            stdout=log,
            stderr=log,
        )
        log.close()

    def stop(self) -> None:
        """Kill the process."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)
