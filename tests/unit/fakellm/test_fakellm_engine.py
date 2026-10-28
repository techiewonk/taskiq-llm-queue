"""Engine and scenario behaviour: seeds, concurrency/rpm throttling, phases, validation.

Driven directly against `Engine` with an injected clock and sleep so timing tests are
exact and fast; the HTTP fronts are covered in the bedrock/openai/admin test modules.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from fakellm import Engine, Scenario, ScenarioError, load_scenario
from fakellm_testkit import TOOLS_DIR


class FakeClock:
    """Manual clock: `sleep` advances time instantly so latency costs no wall time."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        self.slept.append(secs)
        await asyncio.sleep(0)


def make_engine(raw: dict[str, Any], seed: int | None = None) -> tuple[Engine, FakeClock]:
    clock = FakeClock()
    engine = Engine(Scenario.from_mapping(raw), seed=seed, clock=clock, sleep=clock.sleep)
    return engine, clock


async def outcomes(engine: Engine, key: str, n: int) -> list[str | None]:
    return [(await engine.call(key)).error for _ in range(n)]


async def test_same_seed_same_sequence() -> None:
    """Rolls and latency come from a per-key RNG seeded by (seed, key): runs reproduce."""
    raw = {"defaults": {"throttle_rate": 0.5, "latency_ms": {"p50": 100, "p95": 400}}}
    a, clock_a = make_engine(raw, seed=7)
    b, clock_b = make_engine(raw, seed=7)
    c, _ = make_engine(raw, seed=8)
    seq_a = await outcomes(a, "k", 40)
    assert seq_a == await outcomes(b, "k", 40)
    assert seq_a != await outcomes(c, "k", 40)
    assert clock_a.slept == clock_b.slept
    assert 0 < seq_a.count("ThrottlingException") < 40
    a.reset()
    assert await outcomes(a, "k", 40) == seq_a


async def test_seed_precedence() -> None:
    assert make_engine({"seed": 3})[0].seed == 3
    assert make_engine({"seed": 3}, seed=9)[0].seed == 9
    assert make_engine({})[0].seed == 0


async def test_latency_follows_p50_p95() -> None:
    engine, clock = make_engine({"defaults": {"latency_ms": {"p50": 100, "p95": 300}}}, seed=1)
    await outcomes(engine, "k", 2000)
    ms = sorted(s * 1000 for s in clock.slept)
    assert 90 < ms[1000] < 110
    assert 260 < ms[1900] < 340
    assert max(ms) <= 1200


async def test_max_concurrency_throttles_excess() -> None:
    """More in flight than max_concurrency -> ThrottlingException, peak recorded."""
    gate = asyncio.Event()

    async def blocked_sleep(_: float) -> None:
        await gate.wait()

    engine = Engine(
        Scenario.from_mapping({"models": {"k": {"max_concurrency": 3}}}), sleep=blocked_sleep
    )
    tasks = [asyncio.create_task(engine.call("k")) for _ in range(10)]
    await asyncio.sleep(0)
    assert engine.stats["k"].concurrent == 3
    gate.set()
    errors = [r.error for r in await asyncio.gather(*tasks)]
    assert errors.count(None) == 3
    assert errors.count("ThrottlingException") == 7
    stats = engine.stats["k"].public()
    assert stats == {
        "calls": 10,
        "ok": 3,
        "concurrent": 0,
        "max_concurrent_seen": 3,
        "throttled": 7,
        "errors": 0,
    }


async def test_rpm_window() -> None:
    engine, clock = make_engine({"models": {"k": {"rpm": 5}}})
    assert (await outcomes(engine, "k", 7)).count("ThrottlingException") == 2
    clock.now += 60
    assert await outcomes(engine, "k", 5) == [None] * 5


async def test_phases_patch_behaviour_relative_to_reset() -> None:
    raw = {
        "models": {"k": {"max_concurrency": 10}},
        "phases": [
            {"at_s": 300, "model": "k", "set": {"throttle_rate": 0.0}},
            {"at_s": 180, "model": "k", "set": {"throttle_rate": 1.0}},
        ],
    }
    engine, clock = make_engine(raw)
    assert await outcomes(engine, "k", 3) == [None] * 3
    clock.now += 180
    assert engine.behaviour("k").throttle_rate == 1.0
    assert await outcomes(engine, "k", 3) == ["ThrottlingException"] * 3
    clock.now += 120
    assert await outcomes(engine, "k", 2) == [None] * 2
    engine.reset()
    clock.now += 200
    assert engine.behaviour("k").throttle_rate == 1.0
    assert engine.behaviour("k").max_concurrency == 10


async def test_timeout_and_5xx_rates() -> None:
    engine, _ = make_engine({"models": {"t": {"timeout_rate": 1.0}, "e": {"error_rate_5xx": 1.0}}})
    assert await outcomes(engine, "t", 2) == ["ModelTimeoutException"] * 2
    five = set(await outcomes(engine, "e", 40))
    assert five == {"InternalServerException", "ServiceUnavailableException"}
    assert engine.stats["e"].errors == 40


def test_bedrock_key_resolution() -> None:
    engine, _ = make_engine({"models": {"m@eu-west-1": {}, "apac.m": {}}})
    assert engine.resolve_bedrock_key("m", "eu-west-1") == "m@eu-west-1"
    assert engine.resolve_bedrock_key("m", "us-east-1") == "m@us-east-1"
    assert engine.resolve_bedrock_key("apac.m", "ap-south-1") == "apac.m"
    arn = "arn:aws:bedrock:ap-south-1:1:inference-profile/apac.m"
    assert engine.resolve_bedrock_key(arn, "ap-south-1") == "apac.m"


@pytest.mark.parametrize(
    "raw",
    [
        {"bogus": 1},
        {"defaults": {"nope": 1}},
        {"models": {"k": {"throttle_rate": 1.5}}},
        {"models": {"k": {"max_concurrency": 0}}},
        {"models": {"k": {"tool_calls": "maybe"}}},
        {"models": {"k": {"force_error": "TeapotException"}}},
        {"models": {"k": {"latency_ms": {"p99": 1}}}},
        {"phases": [{"at_s": 1, "model": "k"}]},
        {"phases": [{"at_s": 1, "model": "k", "set": {"dims": -1}}]},
        {"seed": "x"},
    ],
)
def test_invalid_scenarios_raise_at_load(raw: dict[str, Any]) -> None:
    with pytest.raises(ScenarioError):
        Scenario.from_mapping(raw)


def test_load_default_yaml_and_json(tmp_path: Path) -> None:
    scenario = load_scenario(TOOLS_DIR / "scenarios" / "default.yaml")
    assert scenario.seed == 42
    assert scenario.merged("apac.openai.gpt-oss-120b-1:0", 0).max_concurrency == 80
    assert scenario.merged("llama-3.1-70b", 0).latency_p50_ms == 800
    path = tmp_path / "s.json"
    path.write_text(json.dumps(scenario.to_mapping()), encoding="utf-8")
    assert load_scenario(path) == scenario
