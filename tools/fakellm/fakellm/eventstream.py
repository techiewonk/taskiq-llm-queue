"""AWS event-stream framing for the fake ConverseStream (`application/vnd.amazon.eventstream`).

Message layout: total length (4 bytes), headers length (4), prelude CRC32 (4), headers, payload,
message CRC32 (4). Every header here is a string header (type 7). botocore decodes these exactly
as it decodes the real service's stream.
"""

from __future__ import annotations

import json
import struct
import zlib
from typing import Any

CONTENT_TYPE = "application/vnd.amazon.eventstream"


def _headers(values: dict[str, str]) -> bytes:
    out = b""
    for name, value in values.items():
        raw_name, raw_value = name.encode(), value.encode()
        out += struct.pack(">B", len(raw_name)) + raw_name
        out += struct.pack(">BH", 7, len(raw_value)) + raw_value
    return out


def message(headers: dict[str, str], payload: bytes) -> bytes:
    """One framed event-stream message."""
    head = _headers(headers)
    total = 12 + len(head) + len(payload) + 4
    prelude = struct.pack(">II", total, len(head))
    prelude += struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
    body = prelude + head + payload
    return body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


def event(event_type: str, body: dict[str, Any]) -> bytes:
    """A Converse stream event (`messageStart`, `contentBlockDelta`, `metadata`, ...)."""
    return message(
        {
            ":event-type": event_type,
            ":content-type": "application/json",
            ":message-type": "event",
        },
        json.dumps(body, separators=(",", ":")).encode(),
    )


def exception(name: str, text: str) -> bytes:
    """A modelled exception inside the stream (e.g. `throttlingException` mid-stream)."""
    return message(
        {
            ":exception-type": name,
            ":content-type": "application/json",
            ":message-type": "exception",
        },
        json.dumps({"message": text}).encode(),
    )
