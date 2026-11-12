"""Run the usage samples in `samples/`, each as its own process, on fakellm or real Bedrock.

Each sample is plain app code (`LLMQueue.from_env()`); this script only sets the `LLMQ_*`
environment: the local Valkey of `scripts/stack.py`, a fresh key prefix per run (deleted after),
and `samples/registry.json` with its region set.

    python scripts/samples.py                    # every sample on fakellm (no real LLM)
    python scripts/samples.py 03_embed           # one sample
    python scripts/samples.py --live             # real Bedrock: AWS keys from --env-file
    python scripts/samples.py --live --env-file ../../.env

`--live` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and
`AWS_MODEL_REGION` from the env file (default `.env`) and never prints them. Live runs make a
few small calls per sample (well under a cent in total).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
PY = str(ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / "python")
STORE_URL = "redis://:localdev@127.0.0.1:6379/0"
FAKELLM_URL = "http://127.0.0.1:8080"
AWS_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def registry(region: str) -> str:
    """`samples/registry.json` with every deployment and the residency list on `region`."""
    doc = json.loads((SAMPLES / "registry.json").read_text(encoding="utf-8"))
    doc["residency"]["allowed_regions"] = [region]
    for model in doc["models"].values():
        for dep in model["deployments"].values():
            dep["confirmed"] = [region]
    return json.dumps(doc)


def environment(live: bool, env_file: Path, prefix: str) -> dict[str, str]:
    """The process environment of one run (no AWS keys from the shell leak into fake runs)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "LLMQ_"))}
    if live:
        from dotenv import dotenv_values

        values = dotenv_values(env_file)
        missing = [k for k in AWS_KEYS[:2] if not values.get(k)]
        if missing:
            raise SystemExit(f"{env_file}: {', '.join(missing)} not set")
        env.update({k: str(values[k]) for k in AWS_KEYS if values.get(k)})
        region = str(values.get("AWS_MODEL_REGION") or "us-east-1")
    else:
        env.update({"AWS_ACCESS_KEY_ID": "aws-main", "AWS_SECRET_ACCESS_KEY": "fake"})
        env["LLMQ_BEDROCK_ENDPOINT_URL"] = FAKELLM_URL
        region = "us-east-1"
    env.update(
        {
            "LLMQ_STORE_URL": STORE_URL,
            "LLMQ_KEY_PREFIX": prefix,
            "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
            "LLMQ_REGISTRY": registry(region),
            "LLMQ_METRICS_SINK": "none",
            "PYTHONIOENCODING": "utf-8",
        }
    )
    print(f"target: {'real Bedrock' if live else 'fakellm'} in {region}; key prefix {prefix}")
    return env


def cleanup(prefix: str) -> None:
    """Delete every key and queue the run made."""
    import redis

    client = redis.Redis.from_url(STORE_URL)
    queue_prefix = prefix.replace(":", "-") + "-"
    for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
        keys = list(client.scan_iter(match=pattern, count=500))
        if keys:
            client.delete(*keys)
    client.close()


def main() -> int:
    """Run the chosen samples; exit 1 if any fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("names", nargs="*", help="sample names (default: all)")
    parser.add_argument("--live", action="store_true", help="call real Bedrock")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    found = sorted(p.stem for p in SAMPLES.glob("[0-9]*.py"))
    names = args.names or found
    unknown = [n for n in names if n not in found]
    if unknown:
        print(f"unknown sample(s): {', '.join(unknown)}; have {', '.join(found)}")
        return 2
    prefix = f"llmqsample:{uuid.uuid4().hex[:8]}"
    env = environment(args.live, args.env_file, prefix)
    failed = []
    try:
        for name in names:
            started = time.monotonic()
            with tempfile.TemporaryDirectory() as cwd:  # samples never depend on the cwd
                proc = subprocess.run(
                    [PY, str(SAMPLES / f"{name}.py")],
                    cwd=cwd,
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=args.timeout,
                    check=False,
                )
            ok = proc.returncode == 0
            print(f"{'PASS' if ok else 'FAIL'}  {name} ({time.monotonic() - started:.1f}s)")
            out = proc.stdout.rstrip() if ok else (proc.stdout + proc.stderr).rstrip()[-4000:]
            if out:
                print("    " + out.replace("\n", "\n    "))
            if not ok:
                failed.append(name)
    finally:
        cleanup(prefix)
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
