"""Canary router: stickiness, percentage boundaries, allowlist, kill switch, last-good reload."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from collections.abc import Callable, Mapping
from typing import Literal, Protocol

import fakeredis
import pytest
from pydantic import ValidationError

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.metrics_names import CONFIG_INVALID_TOTAL
from taskiq_llm_queue.routing import CanaryRouter, RoutingConfig, bucket, routing_store

KEYS = KeySpace("llmq:unit-routing")


class PathRouter(Protocol):
    """Local copy of the client's routing hook (routing_hooks.py), checked structurally."""

    def route(
        self, model: str, lane: Lane, session_id: str | None, user_id: str | None
    ) -> Literal["sqs", "direct"]: ...


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, float, dict[str, str]]] = []

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        self.calls.append(("incr", name, value, dict(labels or {})))

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self.calls.append(("observe", name, value, dict(labels or {})))

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self.calls.append(("gauge", name, value, dict(labels or {})))


def make_router(
    cfg: RoutingConfig | None = None, rand: Callable[[], float] = random.random
) -> CanaryRouter:
    return CanaryRouter(routing_store(fakeredis.FakeAsyncRedis(), KEYS, cfg), rand=rand)


def cfg_with(pct: int, model: str = "gpt20b", lane: Lane = Lane.RT) -> RoutingConfig:
    return RoutingConfig().with_pct(model, lane, pct)


def test_satisfies_path_router_protocol() -> None:
    router: PathRouter = make_router()
    assert router.route("gpt20b", Lane.RT, "s", "u") == "direct"


def test_same_session_same_path_10k_calls_and_across_instances() -> None:
    """Stickiness: one session never flips path, and every pod (instance) agrees."""
    cfg = cfg_with(50)
    a, b = make_router(cfg), make_router(cfg)
    sessions = [uuid.uuid4().hex for _ in range(200)]
    first = {s: a.route("gpt20b", Lane.RT, s, None) for s in sessions}
    for i in range(10_000):
        s = sessions[i % len(sessions)]
        assert a.route("gpt20b", Lane.RT, s, "someone") == first[s]
        assert b.route("gpt20b", Lane.RT, s, None) == first[s]
    assert set(first.values()) == {"sqs", "direct"}


def test_bucket_is_stable_known_value() -> None:
    """The bucket must not change between releases (keyed BLAKE2b of the UTF-8 id)."""
    assert bucket("session-1") == 91  # default salt; pinned
    assert bucket("session-1", b"x") == 27  # the salt (LLMQ_ID_HASH_SALT) moves buckets
    assert 0 <= bucket("x") < 100


def test_router_uses_its_salt() -> None:
    """Two routers with different salts bucket the same session differently (sticky per salt)."""
    cfg = cfg_with(50)
    sessions = [f"s-{i}" for i in range(200)]
    plain = make_router(cfg)
    salted = CanaryRouter(routing_store(fakeredis.FakeAsyncRedis(), KEYS, cfg), salt=b"other")
    routes_a = [plain.route("gpt20b", Lane.RT, s, None) for s in sessions]
    routes_b = [salted.route("gpt20b", Lane.RT, s, None) for s in sessions]
    assert routes_a != routes_b
    assert routes_b == [salted.route("gpt20b", Lane.RT, s, None) for s in sessions]


@pytest.mark.parametrize("lane", list(Lane))
@pytest.mark.parametrize("model", ["gpt20b", "embed"])
def test_pct_boundaries_per_model_and_lane(model: str, lane: Lane) -> None:
    sessions = [f"s{i}" for i in range(4000)]
    for pct, low, high in ((0, 0.0, 0.0), (1, 0.003, 0.02), (50, 0.46, 0.54), (100, 1.0, 1.0)):
        router = make_router(cfg_with(pct, model, lane))
        share = sum(router.route(model, lane, s, None) == "sqs" for s in sessions) / len(sessions)
        assert low <= share <= high, (pct, share)
        other = next(x for x in Lane if x is not lane)
        assert router.route(model, other, "s1", None) == "direct"  # other lane stays 0 %
        assert router.route("unlisted", lane, "s1", None) == "direct"


def test_pct_is_monotonic_promoting_never_moves_sqs_sessions_back() -> None:
    sessions = [f"s{i}" for i in range(2000)]
    r10, r50 = make_router(cfg_with(10)), make_router(cfg_with(50))
    at10 = {s for s in sessions if r10.route("gpt20b", Lane.RT, s, None) == "sqs"}
    at50 = {s for s in sessions if r50.route("gpt20b", Lane.RT, s, None) == "sqs"}
    assert at10 < at50


def test_target_suffix_uses_alias_and_user_id_is_fallback_key() -> None:
    router = make_router(cfg_with(50))
    assert router.route("gpt20b@apac", Lane.RT, "abc", None) == router.route(
        "gpt20b", Lane.RT, "abc", None
    )
    assert router.route("gpt20b", Lane.RT, None, "abc") == router.route(
        "gpt20b", Lane.RT, "abc", None
    )


def test_no_ids_uses_random_draw() -> None:
    router = make_router(cfg_with(50), rand=lambda: 0.1)
    assert router.route("gpt20b", Lane.RT, None, None) == "sqs"
    router = make_router(cfg_with(50), rand=lambda: 0.9)
    assert router.route("gpt20b", Lane.RT, None, None) == "direct"


def test_allowlist_goes_sqs_even_at_zero_pct() -> None:
    cfg = RoutingConfig().with_user("vip", allowed=True)
    router = make_router(cfg)
    assert router.route("gpt120b", Lane.RT, "s", "vip") == "sqs"
    assert router.route("gpt120b", Lane.RT, "s", "other") == "direct"
    assert "vip" not in cfg.with_user("vip", allowed=False).allow_users


def test_kill_switch_beats_allowlist_and_100_pct() -> None:
    cfg = cfg_with(100).with_user("vip", allowed=True).with_kill(True)
    router = make_router(cfg)
    assert router.route("gpt20b", Lane.RT, "s", "vip") == "direct"
    assert make_router(cfg.with_kill(False)).route("gpt20b", Lane.RT, "s", None) == "sqs"


@pytest.mark.parametrize(
    "raw",
    [
        {"models": {"gpt20b": {"rt": 101}}},
        {"models": {"gpt20b": {"rt": -1}}},
        {"models": {"gpt20b": {"fast": 10}}},
        {"kill": True},
    ],
)
def test_config_validation_rejects(raw: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        RoutingConfig.model_validate(raw)


def test_plan_json_shape_is_accepted() -> None:
    cfg = RoutingConfig.model_validate_json(
        '{"kill_switch": false, "models": {"gpt20b": {"rt": 10, "bg": 100, "bulk": 100}},'
        ' "allow_users": ["u1"]}'
    )
    assert cfg.pct("gpt20b", Lane.RT) == 10
    assert cfg.pct("gpt20b", Lane.BULK) == 100


async def test_reload_keeps_last_good_and_reports_invalid(caplog: pytest.LogCaptureFixture) -> None:
    client = fakeredis.FakeAsyncRedis()
    metrics = Recorder()
    router = CanaryRouter(routing_store(client, KEYS), metrics=metrics)
    await client.set(KEYS.routing, cfg_with(100).model_dump_json())
    await router.reload()
    assert router.route("gpt20b", Lane.RT, "s", None) == "sqs"

    await client.set(KEYS.routing, b'{"models": {"gpt20b": {"rt": 500}}}')
    with caplog.at_level(logging.ERROR, logger="taskiq_llm_queue.routing"):
        await router.reload()
        await router.reload()
    assert router.route("gpt20b", Lane.RT, "s", None) == "sqs"  # last good kept
    invalid = [r for r in caplog.records if r.__dict__["llmq"]["event"] == "routing_config_invalid"]
    assert [(r.levelname, r.__dict__["llmq"]["key"]) for r in invalid] == [
        ("ERROR", KEYS.routing)
    ] * 2
    assert [c for c in metrics.calls if c[1] == CONFIG_INVALID_TOTAL] == [
        ("incr", CONFIG_INVALID_TOTAL, 1, {"kind": "routing"}),
        ("incr", CONFIG_INVALID_TOTAL, 1, {"kind": "routing"}),
    ]

    await client.delete(KEYS.routing)
    await router.reload()
    assert router.route("gpt20b", Lane.RT, "s", None) == "direct"  # default: all direct
    await client.aclose()


async def test_start_and_aclose_run_the_reload_loop() -> None:
    client = fakeredis.FakeAsyncRedis()
    await client.set(KEYS.routing, cfg_with(100).model_dump_json())
    router = CanaryRouter(routing_store(client, KEYS), reload_secs=0.01)
    task = router.start()
    assert router.start() is task
    for _ in range(100):
        if router.config.pct("gpt20b", Lane.RT) == 100:
            break
        await asyncio.sleep(0.01)
    assert router.route("gpt20b", Lane.RT, "s", None) == "sqs"
    await router.aclose()
    await router.aclose()
    assert task.cancelled()
    await client.aclose()
