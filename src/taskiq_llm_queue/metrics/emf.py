"""CloudWatch Embedded Metric Format (EMF) emitter: one JSON line per metric update (D89).

The lines go to a logger (default) or a text stream; the existing log shipper forwards them and
CloudWatch extracts the metrics, so no in-cluster Prometheus is needed. Labels outside
`metrics_names.LABELS` are dropped (cardinality and privacy), with one warning per label name.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import TextIO

import orjson

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.metrics_names import LABELS

logger = get_logger(__name__)
DEFAULT_NAMESPACE = "LLMQueue"
_EMF_LOGGER = "taskiq_llm_queue.metrics.emf.lines"


class EmfMetrics:
    """`MetricsSink` writing EMF JSON lines with namespace, dimensions and a millisecond stamp.

    `static_dimensions` (e.g. `{"env": "prod"}`) are added to every line and are not limited
    to `LABELS`, since the app controls them. Thread-safe; never raises on a call path.
    """

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        *,
        stream: TextIO | None = None,
        logger: logging.Logger | None = None,
        static_dimensions: Mapping[str, str] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Pick the output: `stream` if given, else `logger` (default a package logger at INFO)."""
        self.namespace = namespace
        self._stream = stream
        self._logger = logger or logging.getLogger(_EMF_LOGGER)
        self._static = dict(static_dimensions or {})
        self._clock = clock
        self._lock = threading.Lock()
        self._warned: set[str] = set()

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        """Emit a counter increment (unit `Count`)."""
        self._emit(name, value, "Count", labels)

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Emit one observation; `*_seconds` names get unit `Seconds`."""
        self._emit(name, value, "Seconds" if name.endswith("_seconds") else "None", labels)

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Emit a gauge value (unit `None`)."""
        self._emit(name, value, "None", labels)

    def render(
        self, name: str, value: float, unit: str, labels: Mapping[str, str] | None = None
    ) -> str:
        """The EMF JSON line for one metric value (exposed for tests and custom shippers)."""
        dims = dict(self._static)
        for key, val in (labels or {}).items():
            if key in LABELS:
                dims[key] = str(val)
            elif key not in self._warned:
                self._warned.add(key)
                logger.warning("emf_label_dropped", label=key)
        doc: dict[str, object] = {
            "_aws": {
                "Timestamp": int(self._clock() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.namespace,
                        "Dimensions": [sorted(dims)],
                        "Metrics": [{"Name": name, "Unit": unit}],
                    }
                ],
            },
            **dims,
            name: value,
        }
        return orjson.dumps(doc).decode()

    def _emit(self, name: str, value: float, unit: str, labels: Mapping[str, str] | None) -> None:
        try:
            line = self.render(name, value, unit, labels)
            if self._stream is None:
                self._logger.info(line)
                return
            with self._lock:
                self._stream.write(line + "\n")
                self._stream.flush()
        except Exception:  # metrics must never break a call
            logger.exception("emf_emit_failed", metric=name)
