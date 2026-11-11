"""Token estimate and refund rules (plan §3.7a) and inference merging (§3.9 rules 1-8)."""

from __future__ import annotations

from typing import Any

import pytest

from taskiq_llm_queue.clients import Usage
from taskiq_llm_queue.engine import inference, tokens

CHAT: dict[str, Any] = {
    "messages": [{"role": "user", "content": [{"text": "x" * 400}]}],
    "toolConfig": {"tools": [{"toolSpec": {"name": "lookup", "description": "y" * 100}}]},
    "inferenceConfig": {"maxTokens": 500},
}


def test_estimate_counts_texts_and_tools() -> None:
    est = tokens.estimate("converse", CHAT, embedding=False, default_output_tokens=1000)
    assert est.input == (400 + len("lookup") + 100 + len("user")) // 4 + 1 or est.input > 120
    assert est.output == 500
    assert est.total == est.input + 500
    without = {k: v for k, v in CHAT.items() if k != "inferenceConfig"}
    assert (
        tokens.estimate("converse", without, embedding=False, default_output_tokens=900).output
        == 900
    )
    weighted = tokens.estimate(
        "converse", CHAT, embedding=False, default_output_tokens=1, output_weight=5
    )
    assert weighted.output == 2500
    emb = tokens.estimate(
        "invoke_model",
        {"body": {"inputText": "abcd" * 10}},
        embedding=True,
        default_output_tokens=1000,
    )
    assert (emb.input, emb.output) == (10, 0)


def test_custom_estimator() -> None:
    tokens.register_token_estimator("fixed", lambda op, params: 7)
    assert tokens.input_tokens("converse", CHAT, "fixed") == 7


def test_used_tokens() -> None:
    usage = Usage(input_tokens=100, output_tokens=20, cache_read_tokens=50, cache_write_tokens=10)
    assert tokens.used_tokens(usage) == 130  # cache reads not counted (Bedrock, verified)
    assert tokens.used_tokens(usage, output_weight=5) == 210
    assert tokens.used_tokens(usage, count_cached=True) == 180
    assert tokens.used_tokens(Usage()) is None


@pytest.mark.parametrize(
    ("outcome", "used", "expected"),
    [
        ("ok", 300, (0, 400)),  # reserved 700, used 300
        ("ok", 900, (0, -200)),  # used more: charged
        ("ok", None, (0, 0)),  # no usage: the estimate stays
        ("throttle", None, (1, 700)),  # everything back
        ("fail", None, (0, 500)),  # the output part back, the input kept
        ("cut", None, (0, 0)),  # the client gave up mid-answer: nothing back
    ],
)
def test_refund_rules(outcome: str, used: int | None, expected: tuple[int, int]) -> None:
    got = tokens.refund(tokens.Estimate(200, 500), outcome, used)
    assert (got.requests, got.tokens) == expected


def test_bedrock_inference_defaults_and_limits() -> None:
    params = {"messages": [], "inferenceConfig": {"temperature": 0.9}}
    out = inference.apply(
        "bedrock",
        "converse",
        params,
        {"temperature": 0.2, "top_p": 0.8, "max_tokens": 4096, "stop": "END", "extra": {"x": 1}},
        {"max_tokens": 2048, "locked": ["top_p"]},
    )
    assert out["inferenceConfig"] == {
        "temperature": 0.9,  # the call wins
        "topP": 0.8,
        "maxTokens": 2048,  # capped by the limit
        "stopSequences": ["END"],
    }
    assert out["additionalModelRequestFields"] == {"x": 1}
    assert params["inferenceConfig"] == {"temperature": 0.9}  # input not changed
    locked = inference.apply(
        "bedrock",
        "converse",
        {"inferenceConfig": {"topP": 0.1}},
        {"top_p": 0.8},
        {"locked": ["top_p"]},
    )
    assert locked["inferenceConfig"]["topP"] == 0.8
    with pytest.raises(inference.InferenceRejected):
        inference.apply(
            "bedrock",
            "converse",
            {"inferenceConfig": {"maxTokens": 9000}},
            {},
            {"max_tokens": 100, "on_exceed": "reject"},
        )


def test_openai_and_embedding_defaults() -> None:
    out = inference.apply("openai", "chat", {"messages": []}, {"temperature": 0.1, "seed": 7}, {})
    assert (out["temperature"], out["seed"]) == (0.1, 7)
    titan = inference.apply(
        "bedrock",
        "invoke_model",
        {"body": {"inputText": "a"}},
        {"dimensions": 256, "normalize": True, "input_type": "x"},
        {},
    )
    assert titan["body"] == {"inputText": "a", "dimensions": 256, "normalize": True}
    cohere = inference.apply(
        "bedrock",
        "invoke_model",
        {"body": {"texts": ["a"]}},
        {"input_type": "search_document", "truncate": "END", "dimensions": 256},
        {},
    )
    assert cohere["body"] == {"texts": ["a"], "input_type": "search_document", "truncate": "END"}
    assert (
        inference.apply("openai", "embeddings", {"input": "a"}, {"dimensions": 64}, {})[
            "dimensions"
        ]
        == 64
    )


def test_deployment_params_merge() -> None:
    request = {"messages": [], "additionalModelRequestFields": {"a": 1}}
    merged = inference.merge_params(
        request,
        {
            "guardrailConfig": {"guardrailIdentifier": "gr", "guardrailVersion": "1"},
            "additionalModelRequestFields": {"b": 2},
        },
    )
    assert merged["guardrailConfig"]["guardrailIdentifier"] == "gr"
    assert merged["additionalModelRequestFields"] == {"a": 1, "b": 2}
    assert request["additionalModelRequestFields"] == {"a": 1}


def test_hook_failure_falls_back_once() -> None:
    from taskiq_llm_queue.engine import hooks

    def broken(*_: object) -> None:
        raise RuntimeError("bug")

    assert hooks.call_safely("balancer", "b", broken, default="built-in") == "built-in"
    assert hooks.get("balancer", "missing") is None
    with pytest.raises(ValueError, match="name"):
        hooks.register_balancer("", lambda c, x: [])
