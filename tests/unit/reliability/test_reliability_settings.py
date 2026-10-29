"""Reliability settings: validation, per-model caps, acquire backoff and the Valkey pool size."""

from __future__ import annotations

import random

import orjson
import pytest

from taskiq_llm_queue.config import Lane, ModelConfig, QueueConfig, redis_pool_size
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.lifecycle import limiter_settings, model_caps
from taskiq_llm_queue.protocols import Lease
from taskiq_llm_queue.reliability import (
    BreakerSettings,
    ClaimSettings,
    LimiterSettings,
    LocalLimiter,
)
from taskiq_llm_queue.reliability.limiter import _acquire_args, _release_args


def test_claim_staleness_is_at_least_three_beats() -> None:
    """Finding 12: staleness of 2 x interval let one missed heartbeat hand a live task over."""
    settings = ClaimSettings(heartbeat_interval_secs=10.0)
    assert settings.stale_after_secs == 30.0
    with pytest.raises(ValueError, match="stale_after_beats"):
        ClaimSettings(stale_after_beats=2)


def test_breaker_settings_reject_zero_probes() -> None:
    assert BreakerSettings().half_open_probes == 1
    with pytest.raises(ValueError, match="half_open_probes"):
        BreakerSettings(half_open_probes=0)


def test_limiter_settings_validate_utilisation_and_poll() -> None:
    with pytest.raises(ValueError, match="utilisation"):
        LimiterSettings(utilisation=0)
    with pytest.raises(ValueError, match="poll_secs"):
        LimiterSettings(poll_secs=0.5, max_poll_secs=0.1)


def test_retry_delay_sleeps_out_pause() -> None:
    """Finding 7: a paused target was polled every 50 ms instead of after the pause."""
    settings = LimiterSettings(poll_secs=0.05)
    delay = settings.retry_delay(0, 1.5, random.Random(1))
    assert 1.5 <= delay <= 1.55


def test_retry_delay_backs_off_with_jitter_to_cap() -> None:
    """Finding 7: waiting at the limit used a fixed 50 ms poll on a hot key."""
    settings = LimiterSettings(poll_secs=0.05, max_poll_secs=0.25)
    rng = random.Random(7)
    first = [settings.retry_delay(0, 0.0, rng) for _ in range(50)]
    late = [settings.retry_delay(30, 0.0, rng) for _ in range(50)]
    assert all(0.025 <= d <= 0.05 for d in first)
    assert all(0.125 <= d <= 0.25 for d in late)
    assert len(set(late)) > 1, "jittered"


def test_limiter_settings_validate_loss_and_settle_knobs() -> None:
    """Knobs of the clustered-throttle cut and the settle plateau are checked at boot."""
    with pytest.raises(ValueError, match="loss_tolerance"):
        LimiterSettings(loss_tolerance=1.5)
    with pytest.raises(ValueError, match="min_throttles"):
        LimiterSettings(min_throttles=0)
    with pytest.raises(ValueError, match="settle_ratio"):
        LimiterSettings(settle_ratio=0)


def test_redis_pool_covers_worker_slots() -> None:
    """Bug: a 32-connection pool under 154 worker slots timed out and read as "Valkey down".

    The pool cap is raised to the pod's worker slots (shared-queue aliases counted once) plus
    spare; a larger configured `redis_max_connections` still wins.
    """
    queues = {"rt": "q-rt", "default": "q"}
    models = {
        "a": ModelConfig(provider="bedrock", model_id="a", queues=queues),
        "b": ModelConfig(provider="bedrock", model_id="b", queues=queues),  # shares a's broker
        "c": ModelConfig(provider="bedrock", model_id="c", queues={"default": "qc"}),
    }
    config = QueueConfig(models=models, worker_slots={"c": 100})
    assert redis_pool_size(config) == 50 + 100 + 16
    assert redis_pool_size(QueueConfig()) == 32
    assert redis_pool_size(QueueConfig(models=models, redis_max_connections=500)) == 500


def test_max_by_model_clamps_start_max_and_min() -> None:
    """A per-model cap bounds the start, the max and the floor sent to Lua; others keep defaults.

    A start above the cap is clamped (logged once), never raised: the cap is the stronger rule.
    """
    settings = LimiterSettings(initial={"a": 40, "b": 5}, max_by_model={"a": 10, "c": 1})
    assert (settings.start_for("a"), settings.max_for("a")) == (10, 10)
    assert (settings.start_for("b"), settings.max_for("b")) == (5, 2000)
    assert _acquire_args(settings, "c", Lane.RT, "t")[:3] == [1, 1, 1]  # floor drops to the cap
    lease = Lease("a", "primary", "t", 0.0)
    args = _release_args(settings, lease, throttled=False)
    assert args[:3] == [10, 2, 10]
    assert args[9] == 10, "ramp doubling is capped at the model cap too"
    with pytest.raises(ValueError, match="max_by_model"):
        LimiterSettings(max_by_model={"a": 0})


def test_clamp_warning_only_for_a_chosen_start(caplog: pytest.LogCaptureFixture) -> None:
    """I10: only `max_concurrency` set (default start 20 > cap 8) is quiet; a chosen 40 warns."""
    import logging

    with caplog.at_level(logging.WARNING):
        LimiterSettings(initial={"quiet": 20}, max_by_model={"quiet": 8})
        LimiterSettings(initial={"loud": 40}, max_by_model={"loud": 8})
    clamped = [m for r in caplog.records if "limiter_initial_clamped" in (m := r.getMessage())]
    assert len(clamped) == 1
    assert "loud" in clamped[0]


def test_model_caps_from_config_and_env() -> None:
    """`max_concurrency` per model, overridden per alias by `LLMQ_AC_MAX_BY_MODEL` JSON."""
    models = {
        "a": ModelConfig(provider="bedrock", model_id="a", max_concurrency=30),
        "b": ModelConfig(provider="bedrock", model_id="b", max_concurrency=8),
        "c": ModelConfig(provider="bedrock", model_id="c"),
    }
    cfg = QueueConfig.from_env(
        {
            "LLMQ_MODELS": orjson.dumps({k: v.model_dump() for k, v in models.items()}).decode(),
            "LLMQ_AC_MAX_BY_MODEL": '{"a": 12}',
        }
    )
    assert cfg.models["c"].max_concurrency is None
    assert model_caps(cfg) == {"a": 12, "b": 8}
    settings = limiter_settings(cfg)
    assert [settings.max_for(m) for m in "abc"] == [12, 8, 2000]
    assert settings.start_for("b") == 8  # limiter_initial 20 clamped to the cap
    with pytest.raises(ConfigError, match="AC_MAX_BY_MODEL"):
        QueueConfig.from_env({"LLMQ_AC_MAX_BY_MODEL": "{nope"})
    with pytest.raises(ConfigError, match="max_by_model"):
        QueueConfig.from_env({"LLMQ_AC_MAX_BY_MODEL": '{"a": 0}'})
    with pytest.raises(ValueError, match="max_concurrency"):
        ModelConfig(provider="bedrock", model_id="a", max_concurrency=0)


def test_local_caps_respect_model_caps() -> None:
    """Valkey-down mode: the per-pod local cap never exceeds the alias's `max_concurrency`."""
    state = FailoverState(FailoverSettings(local_cap_fallback=10, model_caps={"a": 3}))
    assert (state.local_cap("a", "primary"), state.local_cap("b", "primary")) == (3, 10)
    shared = LocalLimiter(10, model_caps={"a": 2})
    held = [shared.try_acquire("a", "primary", Lane.RT) for _ in range(3)]
    assert [h is not None for h in held] == [True, True, False]
    assert shared.try_acquire("b", "primary", Lane.RT) is not None


def test_every_limiter_knob_reads_from_env() -> None:
    """I20: the cut, pause, hold and probe knobs have `LLMQ_AC_*` variables too."""
    env = {
        "LLMQ_AC_UTILISATION": "0.8",
        "LLMQ_AC_PEAK_HALFLIFE_SECS": "120",
        "LLMQ_AC_LOSS_TOLERANCE": "0.1",
        "LLMQ_AC_MIN_THROTTLES": "3",
        "LLMQ_AC_PAUSE_RATIO": "0.6",
        "LLMQ_AC_WINDOW_SECS": "2",
        "LLMQ_AC_SETTLE_RATIO": "0.8",
        "LLMQ_AC_PROBE_SECS": "10",
        "LLMQ_AC_HOLD_RATIO": "0.5",
    }
    s = limiter_settings(QueueConfig.from_env(env))
    assert (s.utilisation, s.peak_halflife_secs, s.loss_tolerance, s.min_throttles) == (
        0.8,
        120.0,
        0.1,
        3,
    )
    assert (s.pause_ratio, s.window_secs, s.settle_ratio, s.probe_secs, s.hold_ratio) == (
        0.6,
        2.0,
        0.8,
        10.0,
        0.5,
    )
    assert limiter_settings(QueueConfig()).min_throttles == LimiterSettings().min_throttles
    with pytest.raises(ConfigError):
        QueueConfig.from_env({"LLMQ_AC_PAUSE_RATIO": "2"})
