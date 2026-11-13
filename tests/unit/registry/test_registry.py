"""Registry schema, validation and resolve (plan step A1, scenario B12 config half).

Every rule of plan §3.9 and §3.19 that a document can break has one case here; the plan's own
example registries are the fixtures, so the plan and the code cannot drift apart.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.registry import (
    RegistryError,
    ResolvedRegistry,
    check,
    load_registry,
    register_account_kind,
    schema,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    return data


@pytest.fixture
def full() -> dict[str, Any]:
    return _load("full")


@pytest.fixture
def small() -> dict[str, Any]:
    return _load("smallest")


def _errors(doc: dict[str, Any], **kw: Any) -> list[str]:
    registry, errors, _ = check(doc, **kw)
    assert registry is None, "expected the document to be refused"
    return errors


def _has(errors: list[str], *parts: str) -> bool:
    return any(all(p in e for p in parts) for e in errors)


# ---------------------------------------------------------------- the plan's examples


def test_plan_examples_load(full: dict[str, Any], small: dict[str, Any]) -> None:
    view = load_registry(full)
    assert set(view.registry.models) == {
        "gpt120b",
        "gpt20b",
        "gpt120b-vllm",
        "embed-titan",
        "embed-cohere",
    }
    assert load_registry(small).registry.version == 1


def test_deployments_confirmed_first_in_config_order(full: dict[str, Any]) -> None:
    view = load_registry(full)
    keys = [d.key for d in view.deployments("gpt120b")]
    assert keys == [
        "aws-main/ap-south-1",
        "aws-main/ap-south-2",
        "aws-dr/ap-south-2",
        "aws-main/us-east-1",
        "aws-main/us-west-2",
        "aws-main/apac",
    ]
    profile = view.deployments("gpt120b", "fallback")[-1]
    assert (profile.region, profile.profile_id) == ("ap-south-1", "apac.openai.gpt-oss-120b-1:0")


def test_alias_or_model_id(full: dict[str, Any]) -> None:
    view = load_registry(full)
    assert view.registry.alias_of("openai.gpt-oss-120b-1:0") == "gpt120b"
    assert view.model("gpt120b").model_id == "openai.gpt-oss-120b-1:0"
    with pytest.raises(KeyError):
        view.registry.alias_of("nope")


# ---------------------------------------------------------------- resolve


def test_levels_most_specific_wins(full: dict[str, Any]) -> None:
    view = load_registry(full)
    rpm = view.effective("gpt120b", "rpm", "aws-dr/ap-south-2")
    assert (rpm.value, rpm.level) == (200, "deployment")
    assert view.effective("gpt120b", "rpm", "aws-main/ap-south-1").level == "model"
    attempt = view.effective("gpt120b", "attempt_timeout_secs", "aws-dr/ap-south-2")
    assert (attempt.value, attempt.level) == (45, "account")
    assert view.effective("gpt20b", "store_down").level == "defaults"
    assert view.effective("gpt20b", "max_attempts").level == "library"
    assert view.value("gpt20b", "max_attempts") == 4


def test_defaults_by_model_type(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["emb"] = {
        "type": "embedding",
        "provider": "bedrock",
        "model_id": "amazon.titan-embed-text-v2:0",
        "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}},
    }
    view = load_registry(doc)
    assert view.value("gpt20b", "priority_reserve") == 0.2
    assert view.value("emb", "priority_reserve") == 0.0
    assert view.value("gpt20b", "rate_limits") == "both"
    assert view.value("emb", "rate_limits") == "rpm"
    doc["models"]["emb"]["tpm"] = 300_000
    assert load_registry(doc).value("emb", "rate_limits") == "both"


def test_object_settings_merge_key_by_key(full: dict[str, Any]) -> None:
    doc = copy.deepcopy(full)
    doc["models"]["gpt20b"]["health"] = {"disable_errors": 5}
    view = load_registry(doc)
    health = view.effective("gpt20b", "health")
    assert health.value["disable_errors"] == 5
    assert health.value["probe_after_secs"] == 900  # from defaults
    assert health.level == "model"


def test_inference_defaults_apply_by_type(full: dict[str, Any]) -> None:
    view = load_registry(full)
    assert view.value("embed-titan", "inference") == {"dimensions": 1024, "normalize": True}
    chat = view.value("gpt120b", "inference")
    assert chat["temperature"] == 0.2
    assert chat["max_tokens"] == 4096
    assert chat["top_p"] == 0.9


def test_fallback_rules_get_their_defaults(full: dict[str, Any]) -> None:
    view = load_registry(full)
    rule = view.value("gpt120b", "model_fallback")
    assert rule["models"][0] == {"model": "gpt120b-vllm", "require_headroom": False}
    assert view.value("gpt120b", "fallback")["regions"] == "account"


def test_queue_names(full: dict[str, Any]) -> None:
    view = load_registry(full)
    assert view.queue_name("gpt120b") == "llm-gpt120b"
    assert view.queue_name("embed-titan") == "llm-embed-titan"


def test_residency_filters_and_warns(full: dict[str, Any]) -> None:
    registry, _, warnings = check(full)
    assert registry is not None
    view = ResolvedRegistry(registry)
    outside = [d.key for d in view.deployments("gpt120b") if view.outside_residency(d)]
    assert outside == ["aws-main/us-east-1", "aws-main/us-west-2"]
    assert not view.outside_residency(view.deployments("gpt120b-vllm")[0])  # own list
    assert _has(warnings, "gpt120b @ aws-main/us-east-1", "residency")


def test_profile_without_routes_to_is_outside_residency(full: dict[str, Any]) -> None:
    doc = copy.deepcopy(full)
    del doc["models"]["gpt120b"]["deployments"]["aws-main"]["fallback"][2]["routes_to"]
    registry, _, warnings = check(doc)
    assert registry is not None
    assert _has(warnings, "aws-main/apac", "routes_to is missing")


def test_settings_table_lists_every_setting(full: dict[str, Any]) -> None:
    table = load_registry(full).table("gpt120b", "aws-dr/ap-south-2")
    assert table["rpm"].level == "deployment"
    assert table["timeout_secs"].value == 120


# ---------------------------------------------------------------- structural errors


def test_all_problems_reported_at_once(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    model = doc["models"]["gpt20b"]
    model["rpmm"] = 5
    model["timeout_secs"] = -1
    model["deployments"]["nope"] = {"confirmed": ["x"]}
    with pytest.raises(RegistryError) as info:
        load_registry(doc)
    problems = info.value.problems
    assert len(problems) == 3
    assert isinstance(info.value, ConfigError)
    assert _has(problems, "rpmm", "did you mean `rpm`")
    assert _has(problems, "timeout_secs", "above 0")
    assert _has(problems, "unknown account `nope`")


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda d: d.update(registry_version=0), ["registry_version"]),
        (lambda d: d["models"]["gpt20b"].update(type="vision"), ["type", "chat or embedding"]),
        (lambda d: d["models"]["gpt20b"].update(provider="azure"), ["provider"]),
        (lambda d: d["models"]["gpt20b"].pop("model_id"), ["model_id", "required"]),
        (lambda d: d.update(defaults={"shared_rpm": 5}), ["cannot be set in defaults"]),
        (lambda d: d["models"]["gpt20b"].update(weight=2), ["cannot be set on a model"]),
        (lambda d: d["models"]["gpt20b"].update(balance="fastest"), ["balance", "one of"]),
        (lambda d: d["models"]["gpt20b"].update(store_down="ignore"), ["store_down"]),
        (lambda d: d["models"]["gpt20b"].update(queue_pct=150), ["queue_pct", "at most 100"]),
        (lambda d: d["models"]["gpt20b"].update(rpm=1.5), ["rpm", "integer"]),
        (
            lambda d: d["models"]["gpt20b"].update(fallback={"after_secs": 1}),
            ["fallback", "needs `on`"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(inference={"temperature": 3}),
            ["temperature", "at most 2"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["deployments"]["aws-main"].update(
                fallback=["ap-south-1"]
            ),
            ["listed both confirmed and fallback"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["deployments"]["aws-main"].update(
                confirmed=[], fallback=["ap-south-1"]
            ),
            ["needs at least one confirmed region"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["deployments"]["aws-main"].update(
                overrides={"us-east-1": {"rpm": 5}}
            ),
            ["us-east-1", "not a region or profile"],
        ),
        (lambda d: d.update(colour="blue"), ["unknown name `colour`"]),
        (lambda d: d["accounts"]["aws-main"].update(kind="gcp"), ["kind", "one of"]),
        (
            lambda d: d["accounts"]["aws-main"].update(external_id_ref="X"),
            ["external_id_ref", "needs `role_arn`"],
        ),
        (
            lambda d: d["accounts"]["aws-main"].update(base_url="https://x"),
            ["does not take `base_url`"],
        ),
        (lambda d: d["models"].update({"Bad Alias": d["models"]["gpt20b"]}), ["alias"]),
        (
            lambda d: d.update(defaults={"queue": {"name": "shared"}}),
            ["queue name can only be set on a model"],
        ),
    ],
)
def test_structural_errors(small: dict[str, Any], change: Any, expected: list[str]) -> None:
    doc = copy.deepcopy(small)
    change(doc)
    assert _has(_errors(doc), *expected)


def test_openai_account_rules(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["accounts"]["v"] = {"kind": "openai", "base_url": "http://vllm.example.com/v1"}
    assert _has(_errors(doc), "https unless the host is local")
    doc["accounts"]["v"]["base_url"] = "http://fakellm:8080/v1"  # one-label host: local
    doc["accounts"]["v"]["role_arn"] = "arn:aws:iam::1:role/x"
    assert _has(_errors(doc), "only an `aws` account takes `role_arn`")
    doc["accounts"]["v"].pop("role_arn")
    doc["accounts"]["v"]["base_url"] = "https://other.example.com/v1"
    errors = _errors(doc, allowed_hosts=("vllm.internal",))
    assert _has(errors, "not in the allowed hosts")


def test_profile_name_cannot_equal_a_region(full: dict[str, Any]) -> None:
    doc = copy.deepcopy(full)
    profile = doc["models"]["gpt120b"]["deployments"]["aws-main"]["fallback"][2]
    profile["name"] = "ap-south-1"
    assert _has(_errors(doc), "profile name `ap-south-1` equals a region name")


# ---------------------------------------------------------------- cross-field errors


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (
            lambda d: d["models"]["gpt20b"].update(attempt_timeout_secs=200),
            ["attempt_timeout_secs` must be below `timeout_secs"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(timeout_secs=1000),
            ["above `timeout_max_secs`"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(max_wait_secs=1),
            ["fallback.after_secs: must be below `max_wait_secs`"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(priority_reserve=0.6, normal_min_share=0.5),
            ["must be below 1"],
        ),
        (
            lambda d: (
                d["models"]["gpt20b"].update(rate_limits="tpm", tpm=None)
                or d["models"]["gpt20b"].pop("tpm")
            ),
            ["`rate_limits` is tpm but no `tpm`"],
        ),
        (
            lambda d: (
                d["models"]["gpt120b-vllm"].update(balance="headroom", rpm=None)
                or d["models"]["gpt120b-vllm"].pop("rpm")
            ),
            ["headroom needs `rpm` or `tpm`"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(inference={"dimensions": 256}),
            ["inference.dimensions", "not a parameter of a chat model"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(inference={"seed": 3}),
            ["inference.seed", "not supported by Bedrock"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(inference_limits={"max_tokens": 512}),
            ["above `inference_limits.max_tokens`"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(embed_batch_size=8),
            ["embed_batch_size: only for embedding models"],
        ),
        (
            lambda d: d["models"]["embed-titan"].update(
                model_fallback={"on": ["exhausted"], "models": ["embed-cohere"]}
            ),
            ["embedding model cannot change model"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["model_fallback"].update(models=["gpt20b"]),
            ["cannot list itself"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["model_fallback"].update(models=["gpt999"]),
            ["unknown model `gpt999`"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["model_fallback"].update(models=["gpt120b"]),
            ["list each other; both must use require_headroom"],
        ),
        (
            lambda d: d["models"]["gpt20b"]["model_fallback"].update(models=["embed-titan"]),
            ["`embed-titan` is not a chat model"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(model_id="openai.gpt-oss-120b-1:0"),
            ["one alias per model id"],
        ),
        (
            lambda d: d["models"]["gpt20b"].update(queue={"name": "llm-gpt120b"}),
            ["queue `llm-gpt120b` is also used"],
        ),
        (
            lambda d: d["models"]["gpt120b-vllm"]["deployments"].update(
                {"aws-main": {"confirmed": ["ap-south-1"]}}
            ),
            ["a `aws` account serves `bedrock`, not `openai`"],
        ),
        (
            lambda d: d["models"]["embed-titan"]["deployments"]["aws-main"]["overrides"].update(
                {"ap-south-1": {"weight": 0}, "ap-south-2": {"weight": 0}}
            ),
            ["every confirmed deployment has weight 0"],
        ),
    ],
)
def test_cross_field_errors(full: dict[str, Any], change: Any, expected: list[str]) -> None:
    doc = copy.deepcopy(full)
    change(doc)
    assert _has(_errors(doc), *expected)


def test_require_headroom_needs_numbers_on_the_target(full: dict[str, Any]) -> None:
    doc = copy.deepcopy(full)
    del doc["models"]["gpt120b-vllm"]["rpm"]
    doc["models"]["gpt120b"]["model_fallback"]["models"][0] = {
        "model": "gpt120b-vllm",
        "require_headroom": True,
    }
    assert _has(_errors(doc), "require_headroom on `gpt120b-vllm` needs `rpm` or `tpm`")


# ---------------------------------------------------------------- lenient read, input forms


def test_stored_document_is_read_leniently(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["future_setting"] = 1
    assert _has(_errors(doc), "unknown name `future_setting`")
    view = load_registry(doc, strict=False)
    assert _has(list(view.registry.warnings), "future_setting", "ignored")
    doc["models"]["gpt20b"]["rpm"] = "fast"  # a known name with a bad value stays an error
    assert _has(_errors(doc, strict=False), "rpm", "integer")


def test_json_text_and_bad_json(small: dict[str, Any]) -> None:
    assert load_registry(json.dumps(small)).registry.version == 1
    assert _has(_errors({"x": 1}), "registry_version")
    registry, errors, _ = check("{not json")
    assert registry is None
    assert _has(errors, "not valid JSON")


def test_warnings_for_unused_fallback_and_blocked_account(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["deployments"]["aws-main"]["fallback"] = ["us-east-1"]
    doc["routing"] = {"blocked_accounts": ["ghost"]}
    registry, errors, warnings = check(doc)
    assert registry is not None
    assert not errors
    assert _has(warnings, "fallback regions are listed but there is no `fallback` rule")
    assert _has(warnings, "`ghost` is not an account")


def test_custom_account_kind(small: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(schema, "ACCOUNT_KINDS", dict(schema.ACCOUNT_KINDS))
    register_account_kind("azure", "azure_openai")
    doc = copy.deepcopy(small)
    doc["accounts"]["az"] = {"kind": "azure"}
    doc["models"]["az-gpt"] = {
        "type": "chat",
        "provider": "azure_openai",
        "model_id": "gpt-4o",
        "deployments": {"az": {"confirmed": ["eastus"]}},
    }
    assert load_registry(doc).deployments("az-gpt")[0].key == "az/eastus"
    with pytest.raises(ConfigError, match="lowercase"):
        register_account_kind("Bad Kind", "x")


def test_one_window_per_shared_account_budget(small: dict[str, Any]) -> None:
    """Bug (0.3.0 review): fixed + smooth windows on one shared counter let 4 RPM grant 8."""
    doc = copy.deepcopy(small)
    account = next(iter(doc["accounts"]))
    doc["accounts"][account].setdefault("settings", {})["shared_rpm"] = 4
    model = next(iter(doc["models"]))
    other = copy.deepcopy(doc["models"][model])
    other["model_id"] = other["model_id"] + "-other"
    other["rate_window"] = "fixed"
    doc["models"]["other"] = other
    assert _has(_errors(doc), "must use one `rate_window`")
    doc["models"]["other"]["rate_window"] = "smooth"
    registry, errors, _ = check(doc)
    assert registry is not None, errors


def test_accounts_cannot_point_at_metadata_or_process_secrets(small: dict[str, Any]) -> None:
    """0.3.0 review: a registry edit could aim a key ref at the store password, or base_url at
    the instance metadata endpoint."""
    doc = copy.deepcopy(small)
    doc["accounts"]["vllm"] = {
        "kind": "openai",
        "base_url": "http://169.254.169.254/v1",
        "api_key_ref": "LLMQ_STORE_PASSWORD",
    }
    errors = _errors(doc)
    assert _has(errors, "link-local")
    assert _has(errors, "process secret")
    doc["accounts"]["vllm"].update(
        base_url="https://llm.example.com/v1", api_key_ref="AWS_SECRET_ACCESS_KEY"
    )
    assert _has(_errors(doc), "process secret")
