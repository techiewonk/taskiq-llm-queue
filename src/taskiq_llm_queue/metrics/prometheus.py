"""Optional Prometheus `MetricsSink` for local runs and the Docker stack (`[metrics]` extra).

`prometheus_client` is imported when the sink is built, never at package import, and a missing
install raises an `ImportError` naming the extra. Label names of a metric are fixed by its first
use; later calls fill missing labels with "" and drop unknown ones.
"""

from __future__ import annotations

import importlib
import threading
from collections.abc import Mapping
from typing import Any

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.metrics_names import LABELS

logger = get_logger(__name__)
INSTALL_HINT = "PrometheusMetrics needs prometheus-client: pip install 'taskiq-llm-queue[metrics]'"


class PrometheusMetrics:
    """`MetricsSink` backed by prometheus_client Counter / Histogram / Gauge objects.

    Pass a `registry` for isolation in tests; the default is prometheus_client's global one.
    `serve(port)` starts the client's HTTP exporter for `/metrics`.
    """

    def __init__(self, registry: Any = None) -> None:
        """Import prometheus_client now; raises `ImportError` with the extra to install."""
        try:
            self._prom: Any = importlib.import_module("prometheus_client")
        except ImportError as exc:
            raise ImportError(INSTALL_HINT) from exc
        self._registry = registry if registry is not None else self._prom.REGISTRY
        self._lock = threading.Lock()
        self._metrics: dict[str, tuple[Any, tuple[str, ...]]] = {}

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        """Increase a Counter."""
        self._child("Counter", name, labels).inc(value)

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Observe into a Histogram (default buckets)."""
        self._child("Histogram", name, labels).observe(value)

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Set a Gauge."""
        self._child("Gauge", name, labels).set(value)

    def serve(self, port: int, addr: str = "0.0.0.0") -> None:
        """Expose `/metrics` on `addr:port` from a daemon thread."""
        self._prom.start_http_server(port, addr=addr, registry=self._registry)

    def _child(self, kind: str, name: str, labels: Mapping[str, str] | None) -> Any:
        given = {k: str(v) for k, v in (labels or {}).items() if k in LABELS}
        with self._lock:
            found = self._metrics.get(name)
            if found is None:
                names = tuple(sorted(given))
                metric = getattr(self._prom, kind)(
                    name, name, labelnames=names, registry=self._registry
                )
                found = self._metrics[name] = (metric, names)
        metric, names = found
        return metric.labels(**{n: given.get(n, "") for n in names}) if names else metric
