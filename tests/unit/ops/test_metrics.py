"""Metric sinks: EMF line format, label filtering, logger output, optional Prometheus import."""

from __future__ import annotations

import io
import json
import logging
import subprocess
import sys

import pytest
import structlog

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


def test_emf_drops_unknown_labels_with_one_warning() -> None:
    """Raw session/user ids must never become dimensions (cardinality, privacy)."""
    out = io.StringIO()
    sink = EmfMetrics(stream=out)
    with structlog.testing.capture_logs() as logs:
        sink.incr(CALLS_TOTAL, labels={"model": "m", "session_id": "secret"})
        sink.incr(CALLS_TOTAL, labels={"model": "m", "session_id": "secret2"})
    assert "secret" not in out.getvalue()
    assert logs == [{"event": "emf_label_dropped", "label": "session_id", "log_level": "warning"}]


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
