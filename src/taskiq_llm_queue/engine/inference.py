"""Inference defaults and limits merged into a request, per provider (plan §3.9 rules 1-8).

The registry's `inference` values fill what the call did not set; `inference_limits.locked`
names parameters the call may not change; `inference_limits.max_tokens` caps the output
(`clamp` or `reject`). Values are written in the provider's own request shape.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from taskiq_llm_queue.errors import LLMQueueError

# Registry name -> (Converse path, OpenAI field).
_CHAT_BEDROCK = {
    "max_tokens": ("inferenceConfig", "maxTokens"),
    "temperature": ("inferenceConfig", "temperature"),
    "top_p": ("inferenceConfig", "topP"),
    "stop": ("inferenceConfig", "stopSequences"),
    "top_k": ("additionalModelRequestFields", "top_k"),
    "frequency_penalty": ("additionalModelRequestFields", "frequency_penalty"),
    "presence_penalty": ("additionalModelRequestFields", "presence_penalty"),
}
_CHAT_OPENAI = (
    "max_tokens",
    "temperature",
    "top_p",
    "stop",
    "seed",
    "frequency_penalty",
    "presence_penalty",
)


class InferenceRejected(LLMQueueError):
    """The call asked for more than `inference_limits` allow and `on_exceed` is `reject`."""


def _get(params: Mapping[str, Any], path: tuple[str, str]) -> Any:
    return (params.get(path[0]) or {}).get(path[1])


def _set(params: dict[str, Any], path: tuple[str, str], value: Any) -> None:
    params.setdefault(path[0], {})[path[1]] = value


def apply(
    provider: str,
    op: str,
    params: Mapping[str, Any],
    defaults: Mapping[str, Any],
    limits: Mapping[str, Any],
) -> dict[str, Any]:
    """The request with defaults filled, locked values enforced and `max_tokens` capped."""
    out = copy.deepcopy(dict(params))
    locked = set(limits.get("locked", ()))
    if provider == "bedrock" and op == "converse":
        for name, path in _CHAT_BEDROCK.items():
            if name in defaults and (name in locked or _get(out, path) is None):
                value = defaults[name]
                if name == "stop" and isinstance(value, str):
                    value = [value]
                _set(out, path, value)
        extra = defaults.get("extra")
        if extra:
            fields = out.setdefault("additionalModelRequestFields", {})
            for key, value in extra.items():
                fields.setdefault(key, value)
        cap = limits.get("max_tokens")
        current = _get(out, _CHAT_BEDROCK["max_tokens"])
        if cap is not None and isinstance(current, int) and current > cap:
            if limits.get("on_exceed") == "reject":
                raise InferenceRejected(f"max_tokens {current} is above the limit {cap}")
            _set(out, _CHAT_BEDROCK["max_tokens"], cap)
        return out
    if provider == "openai" and op == "chat":
        for name in _CHAT_OPENAI:
            if name in defaults and (name in locked or out.get(name) is None):
                out[name] = defaults[name]
        for key, value in (defaults.get("extra") or {}).items():
            out.setdefault(key, value)
        cap = limits.get("max_tokens")
        current = out.get("max_tokens")
        if cap is not None and isinstance(current, int) and current > cap:
            if limits.get("on_exceed") == "reject":
                raise InferenceRejected(f"max_tokens {current} is above the limit {cap}")
            out["max_tokens"] = cap
        return out
    if op == "embeddings" and "dimensions" in defaults:
        out.setdefault("dimensions", defaults["dimensions"])
    if op == "invoke_model" and isinstance(out.get("body"), Mapping):
        body = dict(out["body"])
        names = ("dimensions", "normalize", "input_type", "truncate", "embedding_types")
        for name in names:
            if name in defaults and (name in locked or name not in body):
                body[name] = defaults[name]
        if "inputText" in body:  # Titan takes dimensions / normalize only
            body = {k: v for k, v in body.items() if k not in ("input_type", "truncate")}
        else:  # Cohere takes input_type / truncate / embedding_types only
            body = {k: v for k, v in body.items() if k not in ("dimensions", "normalize")}
        out["body"] = body
    return out


def merge_params(request: Mapping[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    """Merge a deployment's `params` (guardrail, model fields) into a request, one level deep."""
    out = dict(request)
    for key, value in extra.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = {**out[key], **value}
        else:
            out[key] = copy.deepcopy(value)
    return out
