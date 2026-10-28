"""Metric sinks: the `MetricsSink` protocol, CloudWatch EMF lines and an optional Prometheus one.

Production emits EMF through the log pipeline (D89); Prometheus needs the `[metrics]` extra and
is imported only when `PrometheusMetrics` is built. Names and labels come from `metrics_names`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from taskiq_llm_queue.metrics.emf import EmfMetrics
from taskiq_llm_queue.metrics.prometheus import PrometheusMetrics

Labels = Mapping[str, str]


@runtime_checkable
class MetricsSink(Protocol):
    """Where the package reports counters, observations and gauges; labels from `LABELS` only."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        """Add `value` to a counter."""

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Record one observation (latency in seconds, sizes) for a distribution."""

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Set a gauge to `value`."""


class NullMetrics:
    """`MetricsSink` that drops everything; the default when an app wires no sink."""

    def incr(self, name: str, value: float = 1.0, labels: Labels | None = None) -> None:
        """Drop the counter update."""

    def observe(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Drop the observation."""

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        """Drop the gauge update."""


__all__ = ["EmfMetrics", "Labels", "MetricsSink", "NullMetrics", "PrometheusMetrics"]
