"""Reliability settings: validation, the limiter's acquire backoff and the Valkey pool size."""

from __future__ import annotations

import random

import pytest

from taskiq_llm_queue.config import ModelConfig, QueueConfig, redis_pool_size
from taskiq_llm_queue.reliability import BreakerSettings, ClaimSettings, LimiterSettings


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
