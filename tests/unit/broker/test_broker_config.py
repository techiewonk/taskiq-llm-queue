"""Typed validation of queue, batching, heartbeat and broker settings (P16), plus DS9 filter."""

from __future__ import annotations

import pytest
from taskiq.message import TaskiqMessage

from taskiq_llm_queue.broker import (
    Batching,
    BrokerConfigError,
    Heartbeat,
    HybridSQSBroker,
    SQSQueue,
    envelope_supported,
)
from taskiq_llm_queue.envelope import ENVELOPE_VERSION
from taskiq_llm_queue.errors import ConfigError


def test_fifo_derived_from_name_and_url() -> None:
    assert SQSQueue("jobs.fifo").fifo
    assert SQSQueue("q", url="http://x/000/jobs.fifo").fifo
    assert not SQSQueue("jobs").fifo


@pytest.mark.parametrize(
    "kwargs",
    [
        {"priority": -1},
        {"wait_time_secs": 21},
        {"max_messages": 0},
        {"max_messages": 11},
        {"visibility_timeout_secs": 43_201},
        {"default_delay_secs": 901},
        {"is_fifo": True},
        {"is_fair": True, "name": "a.fifo"},
        {"default_delay_secs": 5, "name": "a.fifo"},
        {"name": ""},
    ],
)
def test_queue_limits_rejected(kwargs: dict[str, object]) -> None:
    params: dict[str, object] = {"name": "q", **kwargs}
    with pytest.raises(BrokerConfigError):
        SQSQueue(**params)  # type: ignore[arg-type]


def test_queue_errors_collected_in_one_message() -> None:
    with pytest.raises(BrokerConfigError) as info:
        SQSQueue("q", wait_time_secs=99, max_messages=99)
    assert "wait_time_secs" in str(info.value)
    assert "max_messages" in str(info.value)
    assert isinstance(info.value, ConfigError)


@pytest.mark.parametrize(
    "kwargs", [{"size": 11}, {"size": 0}, {"max_bytes": 2_000_000}, {"timeout_secs": -1}]
)
def test_batching_limits(kwargs: dict[str, float]) -> None:
    with pytest.raises(BrokerConfigError):
        Batching(**kwargs)  # type: ignore[arg-type]


def test_heartbeat_interval_capped_at_third_of_visibility() -> None:
    assert Heartbeat().interval_for(60) == 20
    assert Heartbeat(interval_secs=5).interval_for(60) == 5
    assert Heartbeat(interval_secs=50).interval_for(60) == 20
    with pytest.raises(BrokerConfigError):
        Heartbeat(interval_secs=0)
    with pytest.raises(BrokerConfigError):
        Heartbeat(flush_secs=2)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"queues": []},
        {"queues": [SQSQueue("a"), SQSQueue("a")]},
        {"queues": [SQSQueue("a")], "default_queue": "b"},
        {"queues": [SQSQueue("a")], "max_inflight": 0},
        {"queues": [SQSQueue("a")], "max_inflight": 2, "reserve_for_priority0": 3},
        {"queues": [SQSQueue("a")], "ack_flush_secs": 5},
        {"queues": [SQSQueue("a")], "unknown_release_secs": -1},
    ],
)
def test_broker_settings_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(BrokerConfigError):
        HybridSQSBroker(**kwargs)  # type: ignore[arg-type]


def test_broker_construction_has_no_side_effects() -> None:
    broker = HybridSQSBroker([SQSQueue("a"), SQSQueue("b", priority=1)])
    assert not broker.is_started
    assert set(broker.queues) == {"a", "b"}


def _msg(*args: object, **kwargs: object) -> TaskiqMessage:
    return TaskiqMessage(task_id="t", task_name="n", labels={}, args=list(args), kwargs=kwargs)


def test_envelope_version_filter() -> None:
    current = {"v": ENVELOPE_VERSION, "request_id": "r"}
    newer = {"v": ENVELOPE_VERSION + 1, "request_id": "r"}
    assert envelope_supported(_msg(current))
    assert envelope_supported(_msg({"v": 99}))  # not an envelope: no request_id
    assert envelope_supported(_msg("plain", 3))
    assert not envelope_supported(_msg(newer))
    assert not envelope_supported(_msg(envelope=newer))
