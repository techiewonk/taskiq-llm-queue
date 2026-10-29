"""Scenario worker process: one work-only `LLMQueue` with a recording metrics sink.

The harness talks to it through files: it writes stats JSON (metrics, peak RSS) every second
to `--stats`, and obeys one-word commands written to `--control` (`stop`, `pause`, `resume`).
A hard kill is the harness killing the process.

Run: `python -m tools.scenarios.worker --name w1 --config cfg.json --stats w1.json`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import orjson

from tools.scenarios import api
from tools.scenarios.env import Settings, ensure_aws_env
from tools.scenarios.metrics import RecordingSink, peak_rss_mb


def _write(path: Path | None, data: dict[str, Any]) -> None:
    if path is None:
        return
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(orjson.dumps(data))
    with contextlib.suppress(OSError):
        os.replace(tmp, path)


async def run(args: argparse.Namespace) -> None:
    """Start the worker, report stats, obey control commands until `stop`."""
    ensure_aws_env()
    if args.config:
        config = api.config_from_json(Path(args.config).read_bytes())  # noqa: ASYNC240  # small local file, startup/teardown only
    else:
        config = api.build_config(Settings())
    sink = RecordingSink()
    queue = api.new_queue(config, args.name, metrics=sink)
    stats = Path(args.stats) if args.stats else None
    control = Path(args.control) if args.control else None
    started = time.time()
    await queue.start(mode="work")
    state = "running"

    def report(final: bool = False) -> None:
        _write(
            stats,
            {
                "name": args.name,
                "pid": os.getpid(),
                "state": "stopped" if final else state,
                "uptime_s": round(time.time() - started, 2),
                "peak_rss_mb": peak_rss_mb(),
                "cpu_s": round(time.process_time(), 3),  # CPU used so far
                "metrics": sink.snapshot(),
            },
        )

    try:
        while True:
            report()
            command = ""
            if control is not None and control.exists():
                with contextlib.suppress(OSError):
                    command = control.read_text(encoding="utf-8").strip()
                    control.unlink()
            if command == "stop":
                break
            if command == "pause":
                await queue.pause_receiving()
                state = "paused"
            elif command == "resume":
                queue.resume_receiving()
                state = "running"
            await asyncio.sleep(0.25)
    finally:
        t0 = time.monotonic()
        await queue.stop(drain_secs=args.drain)
        sink.gauge("scn_stop_seconds", time.monotonic() - t0)
        report(final=True)


def main(argv: list[str] | None = None) -> None:
    """Parse flags and run the worker."""
    parser = argparse.ArgumentParser(prog="scenario-worker")
    parser.add_argument("--name", default=os.environ.get("HOSTNAME", "worker"))
    parser.add_argument("--config", help="QueueConfig JSON (default: harness settings)")
    parser.add_argument("--stats")
    parser.add_argument("--control")
    parser.add_argument("--drain", type=float, default=None)
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
