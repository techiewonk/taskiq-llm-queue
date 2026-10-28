"""Structured loggers that always go through stdlib `logging` (no global structlog config).

Unconfigured structlog prints to stdout and ignores the app's levels and handlers, so every
module logs via `get_logger(__name__)`: structlog's key-value API on top of a stdlib logger.
Apps keep full control (levels, handlers, loguru's InterceptHandler, structlog's
ProcessorFormatter); the fields ride on `record.llmq` as a dict.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping
from typing import Any

import structlog

_PASSTHROUGH = ("exc_info", "stack_info")


def _to_stdlib(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> tuple[tuple[str], dict[str, Any]]:
    # Message is readable on its own ("event k=v ..."); the structured copy lives on one
    # `llmq` attribute so field names can never clash with LogRecord's reserved attributes.
    event = str(event_dict.pop("event", ""))
    kwargs: dict[str, Any] = {k: event_dict.pop(k) for k in _PASSTHROUGH if k in event_dict}
    fields: Mapping[str, Any] = dict(event_dict)
    message = " ".join([event, *(f"{k}={v}" for k, v in fields.items())])
    kwargs["extra"] = {"llmq": {"event": event, **fields}}
    kwargs["stacklevel"] = 4  # point %(funcName)s/%(lineno)d at the library call site
    return (message,), kwargs


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Key-value logger bound to `logging.getLogger(name)`; used by every library module."""
    return structlog.wrap_logger(  # type: ignore[no-any-return]
        logging.getLogger(name),
        processors=[_to_stdlib],
        wrapper_class=structlog.stdlib.BoundLogger,
    )
