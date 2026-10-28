"""Measurement helpers: an in-memory `MetricsSink`, percentiles and peak RSS without psutil.

`RecordingSink` is passed as `metrics=` to worker and sender `LLMQueue`s; its `snapshot()` is
what worker processes write to their stats file.
"""

from __future__ import annotations

import math
import sys
import threading
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any


def _label_key(name: str, labels: Mapping[str, str] | None) -> str:
    if not labels:
        return name
    inner = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return f"{name}{{{inner}}}"


class RecordingSink:
    """`MetricsSink` keeping counters, gauges and raw observations (capped per series)."""

    def __init__(self, max_observations: int = 50_000) -> None:
        """Keep at most `max_observations` values per observed series."""
        self._lock = threading.Lock()
        self._max = max_observations
        self.counters: dict[str, float] = defaultdict(float)
        self.gauges: dict[str, float] = {}
        self.observations: dict[str, list[float]] = defaultdict(list)

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        """Add to a counter."""
        with self._lock:
            self.counters[_label_key(name, labels)] += value

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Record one observation."""
        with self._lock:
            series = self.observations[_label_key(name, labels)]
            if len(series) < self._max:
                series.append(value)

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Set a gauge."""
        with self._lock:
            self.gauges[_label_key(name, labels)] = value

    def total(self, name: str) -> float:
        """Sum of a counter over all label sets."""
        with self._lock:
            return sum(v for k, v in self.counters.items() if k.split("{", 1)[0] == name)

    def snapshot(self) -> dict[str, Any]:
        """JSON-able copy: counters, gauges and per-series summaries (not raw values)."""
        with self._lock:
            return {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "observations": {k: summary(v) for k, v in self.observations.items()},
            }


def percentile(values: Iterable[float], pct: float) -> float:
    """Nearest-rank percentile (0 for an empty input)."""
    data = sorted(values)
    if not data:
        return 0.0
    rank = max(1, math.ceil(pct / 100 * len(data)))
    return data[min(rank, len(data)) - 1]


def summary(values: Iterable[float]) -> dict[str, float]:
    """Count, mean, p50/p95/p99 and max of a series, rounded for reports."""
    data = sorted(values)
    if not data:
        return {"n": 0}
    return {
        "n": len(data),
        "mean": round(sum(data) / len(data), 4),
        "p50": round(percentile(data, 50), 4),
        "p95": round(percentile(data, 95), 4),
        "p99": round(percentile(data, 99), 4),
        "max": round(data[-1], 4),
    }


def peak_rss_mb() -> float:
    """Peak resident set size of this process in MiB (Windows working set / Linux maxrss)."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = [  # ctypes needs a mutable class attribute
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = _Counters()
        counters.cb = ctypes.sizeof(_Counters)
        psapi = ctypes.WinDLL("psapi")  # type: ignore[attr-defined,unused-ignore]
        kernel = ctypes.WinDLL("kernel32")  # type: ignore[attr-defined,unused-ignore]
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
        handle = kernel.GetCurrentProcess()
        psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
        return round(float(counters.PeakWorkingSetSize) / 2**20, 1)
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)
