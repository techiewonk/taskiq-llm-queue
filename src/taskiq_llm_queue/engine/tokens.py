"""Token accounting (plan §3.7a): the estimate before a call and what goes back after it.

The estimate is characters / 4 of every text in the request (tool definitions included) plus
the output reservation (`max_tokens`, as Bedrock reserves it). After the call the budget is
corrected to the real usage; a throttled attempt gives everything back; a failed one keeps its
input. `register_token_estimator` replaces the character count with a real tokenizer.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from taskiq_llm_queue.clients.providers import Usage

Estimator = Callable[[str, Mapping[str, Any]], int]
_ESTIMATORS: dict[str, Estimator] = {}
# Request fields that carry numbers or switches, not text the model reads.
_SKIP = frozenset({"inferenceConfig", "modelId", "max_tokens", "temperature", "top_p", "seed"})


def register_token_estimator(name: str, fn: Estimator) -> None:
    """Add an estimator chosen by the `token_estimator` setting: fn(op, params) -> input tokens."""
    _ESTIMATORS[name] = fn


def _texts(node: Any, out: list[str]) -> None:
    if isinstance(node, str):
        out.append(node)
    elif isinstance(node, Mapping):
        for key, value in node.items():
            if key not in _SKIP:
                _texts(value, out)
    elif isinstance(node, list | tuple):
        for value in node:
            _texts(value, out)


def input_tokens(op: str, params: Mapping[str, Any], estimator: str | None = None) -> int:
    """Estimated input tokens of a request (every text, tool schemas included)."""
    if estimator and estimator in _ESTIMATORS:
        return max(1, int(_ESTIMATORS[estimator](op, params)))
    found: list[str] = []
    _texts(params, found)
    return max(1, math.ceil(sum(len(t) for t in found) / 4))


def max_tokens_of(op: str, params: Mapping[str, Any]) -> int | None:
    """The `max_tokens` a request carries, if any (Converse or OpenAI shape)."""
    if op == "converse":
        value = (params.get("inferenceConfig") or {}).get("maxTokens")
    else:
        value = params.get("max_tokens") or params.get("max_completion_tokens")
    return int(value) if isinstance(value, int) else None


@dataclass(frozen=True, slots=True)
class Estimate:
    """What a call reserves: input tokens and the output reservation (already weighted)."""

    input: int
    output: int

    @property
    def total(self) -> int:
        """Tokens taken from TPM at admission."""
        return self.input + self.output


def estimate(
    op: str,
    params: Mapping[str, Any],
    *,
    embedding: bool,
    default_output_tokens: int,
    output_weight: float = 1.0,
    estimator: str | None = None,
) -> Estimate:
    """The reservation of one call: input, plus `max_tokens` x the output weight for chat."""
    inputs = input_tokens(op, params, estimator)
    if embedding:
        return Estimate(inputs, 0)
    output = max_tokens_of(op, params) or default_output_tokens
    return Estimate(inputs, math.ceil(output * output_weight))


def used_tokens(
    usage: Usage, *, output_weight: float = 1.0, count_cached: bool = False
) -> int | None:
    """Tokens a finished call cost against TPM, or None when the provider reported nothing.

    Bedrock: input + cache writes + output x the burndown rate; cache reads do not count.
    """
    if usage.input_tokens is None and usage.output_tokens is None:
        return None
    total = (usage.input_tokens or 0) + usage.cache_write_tokens
    total += math.ceil((usage.output_tokens or 0) * output_weight)
    if count_cached:
        total += usage.cache_read_tokens
    return total


@dataclass(frozen=True, slots=True)
class Refund:
    """What goes back to the budgets after an attempt (negative tokens: charge more)."""

    requests: int
    tokens: int


def refund(reserved: Estimate, outcome: str, used: int | None = None) -> Refund:
    """The refund rule of plan §3.7a.

    `ok`: correct to the real usage (nothing back when usage is missing); `throttle`: the
    request and every token back; `fail` (timeout, 5xx, cut stream): the output part back, the
    input kept; `cut` (the client gave up mid-answer): nothing back.
    """
    if outcome == "ok":
        return Refund(0, 0 if used is None else reserved.total - used)
    if outcome == "throttle":
        return Refund(1, reserved.total)
    if outcome == "fail":
        return Refund(0, reserved.output)
    return Refund(0, 0)
