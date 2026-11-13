"""Soak: a run on worker processes with chaos, checking for leaks, drift and lost calls.

8 worker processes (pods) and 2 caller queues against one store through a proxy, fakellm as the
provider. Mixed traffic at 70 calls/s: chat (20 % priority) and embeddings at about 80 % of the
budget, so admission, holds and reservations stay busy. Chaos events in turn: a worker killed
and replaced, the store blocked, a live registry edit (rpm halved, then back), a rolling
restart of one worker.

Three lengths. Each scales its chaos spacing, status expiry and sampling to its length, so even
the short ones see every event and several expiry cycles:

    minutes  use                        calls    chaos            block / edit  status  sample
    3        smoke, while fixing        ~12,600  every 30 s (5)   10 s / 12 s   30 s    15 s
    10       default; engine changes    ~42,000  every 60 s (9)   20 s / 30 s   60 s    30 s
    20       before a release           ~84,000  every 120 s (9)  20 s / 30 s   120 s   60 s

Time is what finds slow leaks and drift (more calls, more growth above the noise), so a release
runs the 20-minute soak; the 3-minute one is a smoke test only.

Pass: every call answered by its deadline (ok, or `rate_limited` / `deadline_exceeded` /
`queue_unavailable` while a chaos event is on); nothing left in progress once the calls drain;
store memory and key count flat (the last sixth of the run within 25 % of the second sixth,
after the ramp-up); store calls per second flat.

    python scripts/soak.py                 # 10 minutes
    python scripts/soak.py --minutes 3
    python scripts/soak.py --minutes 20
    python scripts/soak.py --minutes 3 --live   # real Bedrock at a small budget

`--live` calls real Bedrock (keys and `AWS_MODEL_REGION` from `--env-file`, never printed) at a
budget a test can afford: chat (gpt-oss-20b) 10 RPM / 5,000 TPM, titan 10 RPM, offered at about
50 % (about 5 chat and 2 embedding calls a minute; at 80 % a random burst of chat calls waited
out max_wait_secs). A test call charges about 150 tokens (a short
prompt, 50 output tokens at most), so RPM binds first and TPM has room. The store stays the
local Valkey: the store-block chaos needs the proxy in front of it.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import random
import statistics
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import failover  # needs the sys.path entry
from gates import Gate, Run, chat, registry
from gates_fleet import TITAN_ID, log_dir, setup_logging

PROXY_PORT = 6387


@dataclasses.dataclass(frozen=True)
class Profile:
    """One soak length and what scales with it (see the module docstring's table)."""

    minutes: int
    chaos_secs: float  # between chaos events
    block_secs: float  # store unreachable
    edit_secs: float  # rpm halved before it goes back
    status_ttl_secs: int  # status records expire: the key count levels off within the run
    sample_secs: float


PROFILES = {
    3: Profile(3, chaos_secs=30, block_secs=10, edit_secs=12, status_ttl_secs=30, sample_secs=15),
    10: Profile(10, chaos_secs=60, block_secs=20, edit_secs=30, status_ttl_secs=60, sample_secs=30),
    20: Profile(
        20, chaos_secs=120, block_secs=20, edit_secs=30, status_ttl_secs=120, sample_secs=60
    ),
}


class StoreProxy(failover.Proxy):  # type: ignore[misc]  # failover is a script module
    """In front of the stack's Valkey, so the soak can block the store."""

    async def handle(self, client_r: Any, client_w: Any) -> None:
        """Forward one connection to the stack's Valkey (or refuse while blocked)."""
        if self.refuse:
            client_w.close()
            return
        try:
            node_r, node_w = await asyncio.open_connection("127.0.0.1", 6379)
        except OSError:
            client_w.close()
            return
        self.open += [client_w, node_w]
        await asyncio.gather(self._pipe(client_r, node_w), self._pipe(node_r, client_w))


LIVE_RPM = 10  # chat and titan each, on real Bedrock
LIVE_TPM = 5000  # chat; ~150 tokens a call, so RPM binds first
LIVE_CALLS_PER_SEC = 7 / 60  # ~50 % of the chat budget: a burst of chat calls still fits


def live_env(env_file: Path) -> tuple[dict[str, str], str]:
    """AWS keys and the region from the env file (never printed), for real Bedrock."""
    from dotenv import dotenv_values

    values = dotenv_values(env_file)
    keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    missing = [k for k in keys[:2] if not values.get(k)]
    if missing:
        raise SystemExit(f"--live: {', '.join(missing)} not set in {env_file}")
    region = str(values.get("AWS_MODEL_REGION") or "ap-south-1")
    env = {k: str(values[k]) for k in keys if values.get(k)}
    return {**env, "AWS_DEFAULT_REGION": region}, region


async def soak(profile: Profile, live: Path | None = None) -> bool:
    """Run the soak; print a sample every `sample_secs` and the verdict.

    With `live` (an env file), the workers call real Bedrock at a small budget (`LIVE_*`).
    """
    minutes, chaos_secs = profile.minutes, profile.chaos_secs
    timeouts = {"timeout_secs": 30, "attempt_timeout_secs": 10, "max_wait_secs": 20}
    rate = 70.0  # calls per second
    if live is None:
        rpm = 1200  # per deployment (one account, three regions): 60 chat/s of budget
        doc = registry(
            {
                "chat": {"rpm": rpm, "worker_slots": 50},
                "titan": {"type": "embedding", "model_id": TITAN_ID, "rpm": 6000},
            },
            accounts=1,
            regions=3,
            defaults=timeouts,
        )
        aws: dict[str, str] = {}
    else:
        rpm, rate = LIVE_RPM, LIVE_CALLS_PER_SEC
        aws, region = live_env(live)
        doc = registry(
            {
                "chat": {"rpm": rpm, "tpm": LIVE_TPM, "worker_slots": 4},
                "titan": {"type": "embedding", "model_id": TITAN_ID, "rpm": LIVE_RPM},
            },
            accounts=1,
            regions=1,
            defaults=timeouts,
        )
        doc["residency"]["allowed_regions"] = [region]
        for entry in doc["models"].values():
            entry["deployments"] = {"aws-0": {"confirmed": [region]}}
        print(f"  live: real Bedrock in {region}; chat {rpm} RPM / {LIVE_TPM} TPM, titan {rpm} RPM")
    proxy = StoreProxy()
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", PROXY_PORT)
    chaos_windows: list[tuple[float, float]] = []
    samples: list[dict[str, float]] = []
    late: list[Any] = []
    try:
        # Status records expire within the run (default 30 min), so the key count levels off
        # and growth after that is a leak, not retention.
        store = f"redis://:localdev@127.0.0.1:{PROXY_PORT}/0"
        ttl = str(profile.status_ttl_secs)
        async with Gate(doc, LLMQ_STORE_URL=store, LLMQ_STATUS_TTL_SECS=ttl) as gate:
            if live is not None:  # real Bedrock: no fakellm endpoint, the env file's keys
                gate.env.pop("LLMQ_BEDROCK_ENDPOINT_URL", None)
                gate.env.update(aws)
            await gate.workers(8)
            queues = await gate.callers(2)
            run = Run()
            stop_at = time.monotonic() + minutes * 60

            async def one(i: int) -> None:
                llmq = queues[i % 2]
                if random.random() < 0.3:
                    await run.call(llmq, "titan", {"body": {"inputText": "soak text " * 20}})
                else:
                    await run.call(llmq, "chat", chat(), priority=random.random() < 0.2)
                last = run.results[-1]
                if last.total_secs > 30 + 6:  # deadline + caller margin + slack
                    late.append(last)

            async def traffic() -> None:
                tasks: set[asyncio.Task[None]] = set()
                i = 0
                t0 = time.monotonic()
                while time.monotonic() < stop_at:
                    task = asyncio.create_task(one(i))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                    i += 1
                    await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
                await asyncio.gather(*tasks)

            async def chaos() -> None:
                events = ["kill", "block", "edit", "restart"]
                n = 0
                while time.monotonic() + chaos_secs < stop_at:
                    await asyncio.sleep(chaos_secs)
                    event = events[n % len(events)]
                    n += 1
                    start = time.monotonic() - run.t0
                    if event == "kill":
                        victim = random.choice([w for w in gate.fleet if w.proc.poll() is None])
                        victim.kill()
                        await gate.workers(1)
                    elif event == "block":
                        proxy.refuse = True
                        proxy.drop_all()
                        await asyncio.sleep(profile.block_secs)
                        proxy.refuse = False
                    elif event == "edit":
                        await queues[0].registry.set({"rpm": rpm // 2}, model="chat", actor="soak")
                        await asyncio.sleep(profile.edit_secs)
                        await queues[0].registry.set({"rpm": rpm}, model="chat", actor="soak")
                    else:
                        victim = random.choice([w for w in gate.fleet if w.proc.poll() is None])
                        await gate.workers(1)
                        await victim.stop()
                    chaos_windows.append((start - 1, time.monotonic() - run.t0 + 45))
                    print(f"  [{start / 60:5.1f} min] chaos: {event}", flush=True)

            async def sampler() -> None:
                while time.monotonic() < stop_at:
                    c0, t0 = gate.store.script_calls(), time.monotonic()
                    await asyncio.sleep(profile.sample_secs)
                    info = gate.store.client.info("memory")
                    keys = sum(
                        1 for _ in gate.store.client.scan_iter(match=f"{gate.prefix}:*", count=2000)
                    )
                    rate = (gate.store.script_calls() - c0) / (time.monotonic() - t0)
                    since = time.monotonic() - run.t0 - profile.sample_secs
                    recent = [r for r in run.results if r.sent > since]
                    ok = sum(r.ok for r in recent)
                    sample = {
                        "minute": (time.monotonic() - run.t0) / 60,
                        "memory_mb": int(info["used_memory"]) / 1e6,
                        "keys": keys,
                        "scripts_per_s": rate,
                        "ok": ok,
                        "failed": len(recent) - ok,
                    }
                    samples.append(sample)
                    print(
                        f"  [{sample['minute']:5.1f} min] ok {ok}, failed {len(recent) - ok}, "
                        f"memory {sample['memory_mb']:.0f} MB, keys {keys}, "
                        f"store scripts/s {rate:.0f}",
                        flush=True,
                    )

            await asyncio.gather(traffic(), chaos(), sampler())
            # Every caller has its answer; a worker may still be finishing an attempt, and a
            # killed worker's entry goes when its lease does (60 s) and the sweep runs.
            inprog_key = f"{gate.prefix}:{{m:chat}}:inprog"
            drain_by = time.monotonic() + 90
            for _ in range(90):  # polls the store: no event to wait on
                inprog = gate.store.client.zcard(inprog_key)
                if not inprog or time.monotonic() > drain_by:
                    break
                await asyncio.sleep(1)
            if inprog:
                print(f"  still in progress: {gate.store.client.zrange(inprog_key, 0, -1)}")
            held = sum(1 for w in gate.fleet if w.proc.poll() is None)  # live workers
    finally:
        server.close()

    def in_chaos(r: Any) -> bool:
        return any(a <= r.sent <= b for a, b in chaos_windows)

    bad = [r for r in run.results if not r.ok and not in_chaos(r)]
    chaos_kinds = collections.Counter(r.kind for r in run.results if not r.ok and in_chaos(r))
    # Baseline: the second sixth, never the first two samples (workers start, traffic ramps
    # up). Compared with the last sixth of the run.
    sixth = minutes / 6
    step = profile.sample_secs / 60
    start, end = max(2 * step, sixth), max(4 * step, 2 * sixth)
    warm = [s for s in samples if start <= s["minute"] < end] or samples[:3]
    tail = max(2 * step, sixth)
    last = [s for s in samples if s["minute"] >= samples[-1]["minute"] - tail] or samples[-3:]

    def flat(field: str) -> bool:
        # Medians: in the short soaks every window holds a chaos event, and one store-block dip
        # (a third of the rate) pulled a mean baseline down until a normal end read as drift.
        a = statistics.median(s[field] for s in warm)
        b = statistics.median(s[field] for s in last)
        return b <= a * 1.25 + 5

    print(
        f"  {len(run.results)} calls over {minutes:.0f} min; failed outside chaos: "
        f"{collections.Counter(r.kind for r in bad) or 0}; during chaos: {dict(chaos_kinds) or 0}"
    )
    print(f"  late answers: {len(late)}; live workers {held}; chat in progress at the end {inprog}")
    for r in [*bad[:10], *late[:10]]:  # enough to tell which event or path it was
        print(f"    sent at {r.sent / 60:5.1f} min: ok={r.ok} kind={r.kind} {r.total_secs:.1f}s")
    checks = {
        "every call outside chaos served": not bad,
        "chaos failures only rate_limited / deadline_exceeded / queue_unavailable": set(chaos_kinds)
        <= {"rate_limited", "deadline_exceeded", "queue_unavailable"},
        "no answer after its deadline": not late,
        "nothing in progress at the end": inprog == 0,
        "store memory flat": flat("memory_mb"),
        "store keys flat": flat("keys"),
        "store calls per second flat": flat("scripts_per_s"),
    }
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    passed = all(checks.values())
    print("PASS  soak" if passed else "FAIL  soak")
    return passed


def main() -> int:
    """Run the soak; exit 1 when it fails."""
    setup_logging()
    print(f"  logs of worker processes: {log_dir()}", flush=True)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--minutes", type=int, choices=sorted(PROFILES), default=10, help="soak length"
    )
    parser.add_argument("--live", action="store_true", help="real Bedrock, small budget")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).parents[3] / ".env")
    args = parser.parse_args()
    live = args.env_file if args.live else None
    return 0 if asyncio.run(soak(PROFILES[args.minutes], live)) else 1


if __name__ == "__main__":
    sys.exit(main())
