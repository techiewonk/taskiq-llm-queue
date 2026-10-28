"""SQS message serializer: taskiq's `ORJSONSerializer` with a stdlib fallback for odd values.

taskiq's default `JSONSerializer` escapes non-ASCII (`ensure_ascii`); measured on a 4 KB Hindi /
Chinese prompt it was ~45x slower to dump and the body 1.5-3x larger than orjson's UTF-8. Both
write plain JSON, so old and new pods read each other's messages during a rolling deploy.
"""

from __future__ import annotations

import json
from typing import Any

import orjson
from taskiq.serializers import ORJSONSerializer


class WireSerializer(ORJSONSerializer):
    """orjson for SQS bodies; values orjson refuses (ints over 64 bits) fall back to stdlib.

    Output is UTF-8 JSON either way, readable by taskiq's stdlib `JSONSerializer` too.
    """

    def dumpb(self, value: Any) -> bytes:
        """Serialize with orjson, or stdlib `json` (no ASCII escaping) when orjson refuses."""
        try:
            return orjson.dumps(value, default=self.default, option=self.option)
        except TypeError:  # orjson.JSONEncodeError: e.g. an int beyond 64 bits
            return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()

    def loadb(self, value: bytes) -> Any:
        """Parse JSON bytes (a `bytes` subclass is copied first; orjson wants exact types)."""
        return orjson.loads(value if type(value) is bytes else bytes(value))
