"""Metric sinks: EMF line format, label filtering, logger output, optional Prometheus import."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import subprocess
import sys
from collections.abc import Mapping

import pytest

from taskiq_llm_queue.metrics import EmfMetrics, MetricsSink, NullMetrics, PrometheusMetrics
from taskiq_llm_queue.metrics_names import CALLS_TOTAL, PROVIDER_SECONDS


def test_emf_line_is_valid_cloudwatch_json() -> None:
    out = io.StringIO()
    sink = EmfMetrics("LLMQueue", stream=out, static_dimensions={"env": "dev"}, clock=lambda: 12.5)
    sink.incr(CALLS_TOTAL, labels={"model": "gpt20b", "lane": "rt", "path": "sqs"})
    sink.observe(PROVIDER_SECONDS, 0.25, {"model": "gpt20b"})
    sink.gauge("llmq_limit", 40)
    lines = out.getvalue().splitlines()
    assert len(lines) == 3
    doc = json.loads(lines[0])
    meta = doc["_aws"]
    assert meta["Timestamp"] == 12500
    (directive,) = meta["CloudWatchMetrics"]
    assert directive["Namespace"] == "LLMQueue"
    assert directive["Dimensions"] == [["env", "lane", "model", "path"]]
    assert directive["Metrics"] == [{"Name": CALLS_TOTAL, "Unit": "Count"}]
    assert doc[CALLS_TOTAL] == 1.0
    assert (doc["model"], doc["lane"], doc["env"]) == ("gpt20b", "rt", "dev")
    second = json.loads(lines[1])
    assert second["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"] == "Seconds"
    assert json.loads(lines[2])["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["env"]]


def test_emf_drops_unknown_labels_with_one_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Raw session/user ids must never become dimensions (cardinality, privacy)."""
    out = io.StringIO()
    sink = EmfMetrics(stream=out)
    with caplog.at_level(logging.WARNING, logger="taskiq_llm_queue.metrics.emf"):
        sink.incr(CALLS_TOTAL, labels={"model": "m", "session_id": "secret"})
        sink.incr(CALLS_TOTAL, labels={"model": "m", "session_id": "secret2"})
    assert "secret" not in out.getvalue()
    assert [(r.levelname, r.__dict__["llmq"]) for r in caplog.records] == [
        ("WARNING", {"event": "emf_label_dropped", "label": "session_id"})
    ]


def test_emf_defaults_to_logger(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="taskiq_llm_queue.metrics.emf.lines"):
        EmfMetrics().incr(CALLS_TOTAL)
    (record,) = caplog.records
    assert "_aws" in json.loads(record.getMessage())


def test_emf_never_raises_on_broken_stream() -> None:
    class Broken(io.StringIO):
        def write(self, s: str) -> int:
            raise OSError("disk full")

    EmfMetrics(stream=Broken()).incr(CALLS_TOTAL)


def test_sinks_satisfy_protocol() -> None:
    assert isinstance(EmfMetrics(stream=io.StringIO()), MetricsSink)
    assert isinstance(NullMetrics(), MetricsSink)
    NullMetrics().incr("x")


def test_prometheus_missing_raises_clear_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    with pytest.raises(ImportError, match=r"taskiq-llm-queue\[metrics\]"):
        PrometheusMetrics()


def test_prometheus_sink_when_installed() -> None:
    prom = pytest.importorskip("prometheus_client")
    registry = prom.CollectorRegistry()
    sink = PrometheusMetrics(registry)
    sink.incr(CALLS_TOTAL, labels={"model": "m", "path": "sqs"})
    sink.incr(CALLS_TOTAL, 2, {"model": "m", "path": "sqs", "user_id": "dropped"})
    sink.gauge("llmq_limit", 5, {"model": "m"})
    sink.observe(PROVIDER_SECONDS, 0.1)
    assert registry.get_sample_value(CALLS_TOTAL, {"model": "m", "path": "sqs"}) == 3.0


def test_importing_package_does_not_import_prometheus() -> None:
    code = (
        "import sys, taskiq_llm_queue.metrics, taskiq_llm_queue.routing, "
        "taskiq_llm_queue.failover, taskiq_llm_queue.cli; "
        "assert 'prometheus_client' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


class ListSink:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, float, dict[str, str]]] = []

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        self.events.append(("incr", name, value, dict(labels or {})))

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self.events.append(("observe", name, value, dict(labels or {})))

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self.events.append(("gauge", name, value, dict(labels or {})))


def test_metrics_hooks_report_every_broker_event() -> None:
    """I14: every broker hook reaches the sink; empty polls and reason details add nothing."""
    from taskiq_llm_queue import metrics_names as mn
    from taskiq_llm_queue.lifecycle import MetricsHooks

    sink = ListSink()
    hooks = MetricsHooks(sink)
    hooks.on_receive("q", 0)
    hooks.on_receive("q", 3)
    hooks.on_queue_wait("q", 1.5)
    hooks.on_send("q", 2)
    hooks.on_ack("q", 3)
    hooks.on_expired("q", 1)
    hooks.on_error("q", "receive", "throttled")
    hooks.on_release("q", 1, "unknown_task:llmq.call")
    names = [(e[1], e[2], e[3]) for e in sink.events]
    assert names == [
        (mn.SQS_RECEIVED_TOTAL, 3.0, {"queue": "q"}),
        (mn.SQS_WAIT_SECONDS, 1.5, {"queue": "q"}),
        (mn.SQS_SENT_TOTAL, 2.0, {"queue": "q"}),
        (mn.SQS_ACKED_TOTAL, 3.0, {"queue": "q"}),
        (mn.SQS_EXPIRED_TOTAL, 1.0, {"queue": "q"}),
        (mn.SQS_ERRORS_TOTAL, 1.0, {"queue": "q", "step": "receive", "kind": "throttled"}),
        (mn.SQS_RELEASED_TOTAL, 1.0, {"queue": "q", "kind": "unknown_task"}),
        (mn.RELEASED_UNKNOWN_TOTAL, 1.0, {"queue": "q"}),
    ]


async def test_limit_watch_reports_inflight_and_breaker_per_used_target() -> None:
    """I14: `llmq_inflight` and `llmq_breaker_open` per model target this pod used; the
    provider's shared bucket only reports its limit."""
    from types import SimpleNamespace
    from typing import Any, cast

    from taskiq_llm_queue import LLMQueue, QueueConfig
    from taskiq_llm_queue import metrics_names as mn
    from taskiq_llm_queue.reliability.breaker import BreakerState

    class Limiter:
        def __init__(self) -> None:
            self.last_limits = {("big", "primary"): 12, ("@bedrock", "shared"): 8}

        async def state(self, model: str, target: str) -> Any:
            return SimpleNamespace(inflight=5)

    class Breaker:
        async def state(self, model: str, target: str) -> BreakerState:
            return BreakerState.HALF_OPEN

    sink = ListSink()
    q = LLMQueue(QueueConfig(cfg_reload_secs=0.01), metrics=sink)
    task = asyncio.create_task(q._watch_limits(cast(Any, Limiter()), cast(Any, Breaker())))
    await asyncio.sleep(0.05)
    task.cancel()
    gauges = {(e[1], e[3]["model"]): e[2] for e in sink.events if e[0] == "gauge"}
    assert gauges == {
        (mn.LIMIT, "big"): 12.0,
        (mn.INFLIGHT, "big"): 5.0,
        (mn.BREAKER_OPEN, "big"): 0.5,
        (mn.LIMIT, "@bedrock"): 8.0,
    }
