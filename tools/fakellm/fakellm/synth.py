"""Deterministic fake content: reply text, token counts, tool arguments and vectors.

Everything is a pure function of the request (never the seed or the clock), so the same
input always yields the same reply and embedding and tests can compare results.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from typing import Any


def digest(obj: Any) -> str:
    """Stable SHA-256 hex digest of any JSON-able request fragment."""
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def reply_text(obj: Any) -> str:
    """Assistant reply derived from the input hash (same input, same reply)."""
    return f"fakellm reply {digest(obj)[:16]}"


def count_tokens(text: str) -> int:
    """Rough token estimate (4 chars per token, at least 1) used for every `usage` block."""
    return max(1, math.ceil(len(text) / 4))


def collect_text(obj: Any) -> str:
    """Concatenate every string found under a `text` or `content` key (for token counts)."""
    parts: list[str] = []

    def walk(node: Any, key: str | None) -> None:
        if isinstance(node, str):
            if key in {"text", "content"}:
                parts.append(node)
        elif isinstance(node, dict):
            for k, v in node.items():
                walk(v, k)
        elif isinstance(node, list):
            for v in node:
                walk(v, key)

    walk(obj, None)
    return " ".join(parts)


def tool_arguments(schema: Any, seed_text: str) -> dict[str, Any]:
    """Fill a JSON-schema object's required properties with typed placeholder values.

    Used for "tool_calls: echo": the reply calls the tool with arguments that satisfy
    its schema, string fields carrying `seed_text` so callers can see what was echoed.
    """
    if not isinstance(schema, dict):
        return {}
    props = schema.get("properties") or {}
    required = schema.get("required") or list(props)
    placeholder = {"integer": 1, "number": 1.0, "boolean": True, "array": [], "object": {}}
    args: dict[str, Any] = {}
    for name in required:
        kind = (props.get(name) or {}).get("type", "string")
        args[name] = placeholder.get(kind, seed_text) if isinstance(kind, str) else seed_text
    return args


def embedding(text: str, dims: int) -> list[float]:
    """Unit-length vector seeded by the text hash, so equal texts embed identically."""
    rng = random.Random(hashlib.sha256(text.encode()).digest())
    vec = [rng.gauss(0.0, 1.0) for _ in range(dims)]
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]
