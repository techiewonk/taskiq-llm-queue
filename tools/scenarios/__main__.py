"""CLI: `python -m tools.scenarios run|list|load|chaos`.

run [S01 S35 ...] [--tag fast|slow|all] [--report out.json]   run scenarios, print a table
list [--tag ...]                                               show ids, tags and status hints
load [--rate 500] [--minutes 15] [--workers 3]                 S27-style load (pure asyncio)
chaos kill-worker|stop-sqs|start-sqs|stop-valkey|start-valkey|pause-fakellm

SCN_PROVIDER=aws runs only `aws_safe="yes"` scenarios against real Bedrock (queue stays local);
credentials come from `.env` / Secrets Manager per SCN_ENV_SOURCE (auto|dotenv|secrets|none).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from tools.scenarios.env import Settings, ensure_import_paths


def _run(args: argparse.Namespace) -> int:
    from tools.scenarios import harness

    specs = harness.select(args.ids, args.tag)
    settings = Settings()
    if settings.aws:
        unsafe = [s.id for s in specs if s.aws_safe != "yes"]
        if unsafe and args.ids:
            raise SystemExit(f"SCN_PROVIDER=aws refuses scenarios that are not aws-safe: {unsafe}")
        specs = [s for s in specs if s.aws_safe == "yes"]
    run_dir = Path(args.run_dir) if args.run_dir else harness.default_run_dir()
    print(f"run dir: {run_dir}  ({len(specs)} scenarios)", flush=True)
    results = asyncio.run(harness.run_many(specs, settings, run_dir))
    print()
    print(harness.table(results))
    report = {
        "settings": {
            "provider": settings.provider,
            "prefix": settings.prefix,
            "sqs": settings.sqs_url,
            "fakellm": None if settings.aws else settings.fakellm_url,
            "redis": f"{settings.redis_host}:{settings.redis_port}",
        },
        "run_dir": str(run_dir),
        "results": [
            {
                "id": r.id,
                "title": r.title,
                "status": r.status,
                "secs": r.secs,
                "metrics": r.metrics,
                "message": r.message,
            }
            for r in results
        ],
    }
    out = Path(args.report) if args.report else run_dir / "report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"report: {out}")
    return 1 if any(r.status in ("fail", "error") for r in results) else 0


def _list(args: argparse.Namespace) -> int:
    from tools.scenarios import harness

    for spec in harness.select([], args.tag or "all"):
        flag = "skip" if spec.skip else "xfail" if spec.xfail else ""
        print(
            f"{spec.id:6s} {','.join(spec.tags):10s} {flag:5s} aws={spec.aws_safe:10s} {spec.title}"
        )
    return 0


def _load(args: argparse.Namespace) -> int:
    from tools.scenarios import loadgen

    result = asyncio.run(
        loadgen.standalone(
            Settings(), rate_per_min=args.rate * 60, minutes=args.minutes, workers=args.workers
        )
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("lost", 1) == 0 else 1


def _chaos(args: argparse.Namespace) -> int:
    from tools.scenarios import stack

    settings = Settings()
    action = args.action
    services = {"sqs": settings.sqs_service, "valkey": settings.valkey_service}
    if action.startswith(("stop-", "start-")):
        verb, _, what = action.partition("-")
        cmd = (
            ["stop", "-t", "0", services[what]] if verb == "stop" else ["up", "-d", services[what]]
        )
        print(stack.compose("--profile", "scn", *cmd).stdout)
        return 0
    if action == "pause-fakellm":
        asyncio.run(
            stack.FakeLLM(settings.fakellm_url).patch(
                "openai.gpt-oss-120b-1:0@ap-south-1", {"force_error": "ServiceUnavailableException"}
            )
        )
        return 0
    if action == "kill-worker":
        out = stack.compose("--profile", "scn-docker", "ps", "-q", "worker-scn", check=False)
        ids = out.stdout.split()
        if not ids:
            print("no worker-scn container running (scenarios kill their own local workers)")
            return 1
        print(stack.compose("kill", "-s", "SIGKILL", "worker-scn", check=False).stdout)
        return 0
    print(f"unknown chaos action {action!r}")
    return 2


def main(argv: list[str] | None = None) -> int:
    """Parse and dispatch."""
    parser = argparse.ArgumentParser(prog="python -m tools.scenarios")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("ids", nargs="*")
    run.add_argument("--tag", choices=["fast", "slow", "all"])
    run.add_argument("--report")
    run.add_argument("--run-dir")
    lst = sub.add_parser("list")
    lst.add_argument("--tag", choices=["fast", "slow", "all"])
    load = sub.add_parser("load")
    load.add_argument("--rate", type=float, default=500.0, help="requests per second")
    load.add_argument("--minutes", type=float, default=15.0)
    load.add_argument("--workers", type=int, default=3)
    chaos = sub.add_parser("chaos")
    chaos.add_argument("action")
    args = parser.parse_args(argv)
    ensure_import_paths()
    handler = {"run": _run, "list": _list, "load": _load, "chaos": _chaos}[args.cmd]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
