"""ProviderRegistry policy (allowed providers, denied hosts), plugins, embeddings, lazy import."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from taskiq_llm_queue import ConfigError, ErrorInfo, ErrorKind, ModelConfig, QueueConfig, Target
from taskiq_llm_queue.providers import (
    BedrockProvider,
    OpenAICompatProvider,
    ProviderRegistry,
    batches,
    cohere_body,
    embedding_body,
    error_family,
    load_object,
    max_batch_size,
    parse_embeddings,
    titan_body,
)
from taskiq_llm_queue.providers import registry as registry_module

CUSTOM_PROVIDER = textwrap.dedent(
    '''
    from taskiq_llm_queue.errors import ErrorInfo, ErrorKind

    class EchoProvider:
        """Test provider loaded by import path."""

        name = "bedrock"

        def __init__(self, config):
            self.config = config
            self.closed = False

        async def call(self, op, params, target):
            return {"op": op, "params": dict(params), "region": target.region}

        def classify(self, exc):
            return ErrorInfo(kind=ErrorKind.FATAL, code="echo")

        async def aclose(self):
            self.closed = True

    NOT_A_PROVIDER = object
    '''
)


def _config(**overrides: Any) -> QueueConfig:
    models = {
        "gpt": ModelConfig(provider="bedrock", model_id="openai.gpt-oss-120b-1:0"),
        "oai": ModelConfig(provider="openai", model_id="gpt-4o-mini"),
    }
    return QueueConfig(**{"models": models, **overrides})


@pytest.fixture
def custom_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    (tmp_path / "llmq_custom_provider.py").write_text(CUSTOM_PROVIDER, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    return "llmq_custom_provider"


def test_builtins_resolve_per_model() -> None:
    registry = ProviderRegistry(_config(), use_entry_points=False)
    assert isinstance(registry.for_model("gpt"), BedrockProvider)
    assert isinstance(registry.for_model("oai@primary"), OpenAICompatProvider)
    assert registry.get("bedrock") is registry.get("bedrock")


def test_allowed_providers_enforced() -> None:
    config = QueueConfig(
        models={"gpt": ModelConfig(provider="bedrock", model_id="m")},
        allowed_providers=frozenset({"bedrock"}),
        providers={"openai": "taskiq_llm_queue.providers:OpenAICompatProvider"},
    )
    with pytest.raises(ConfigError, match="'openai' is not in allowed_providers"):
        ProviderRegistry(config, use_entry_points=False)

    registry = ProviderRegistry(config.model_copy(update={"providers": {}}))
    with pytest.raises(ConfigError, match="not configured or not allowed"):
        registry.get("openai")


def test_denied_hosts_rejected_at_boot_with_every_offender() -> None:
    config = _config(
        models={
            "az": ModelConfig(
                provider="openai",
                model_id="gpt-4o",
                targets={
                    "main": Target(base_url="https://res.openai.azure.com/v1"),
                    "other": Target(base_url="https://OTHER.openai.azure.com"),
                    "ok": Target(base_url="https://api.example.com/v1"),
                },
            )
        },
        denied_hosts=("*.openai.azure.com",),
        bedrock_endpoint_url="https://bedrock.openai.azure.com",
    )
    with pytest.raises(ConfigError) as caught:
        ProviderRegistry(config, use_entry_points=False)
    message = str(caught.value)
    assert "az@main" in message
    assert "az@other" in message
    assert "bedrock_endpoint_url" in message
    assert "az@ok" not in message


async def test_custom_provider_loaded_by_import_path(custom_module: str) -> None:
    config = _config(providers={"bedrock": f"{custom_module}:EchoProvider"})
    registry = ProviderRegistry(config, use_entry_points=False)
    provider = registry.for_model("gpt")

    assert type(provider).__name__ == "EchoProvider"
    reply = await provider.call("converse", {"x": 1}, Target(region="r1"))
    assert reply == {"op": "converse", "params": {"x": 1}, "region": "r1"}
    await registry.aclose()
    assert provider.closed  # type: ignore[attr-defined]


def test_bad_import_paths_collected_into_one_error(custom_module: str) -> None:
    config = _config(
        providers={"bedrock": "no_such_module_xyz:Thing", "openai": f"{custom_module}:Missing"}
    )
    with pytest.raises(ConfigError) as caught:
        ProviderRegistry(config, use_entry_points=False)
    assert "no_such_module_xyz" in str(caught.value)
    assert "Missing" in str(caught.value)


def test_non_provider_object_rejected_on_build(custom_module: str) -> None:
    registry = ProviderRegistry(
        _config(providers={"bedrock": f"{custom_module}:NOT_A_PROVIDER"}), use_entry_points=False
    )
    with pytest.raises(ConfigError, match="failed to build"):
        registry.get("bedrock")


def test_factory_returning_non_provider_rejected() -> None:
    def factory(config: QueueConfig) -> Any:
        return object()

    registry = ProviderRegistry(_config(), factories={"bedrock": factory}, use_entry_points=False)
    with pytest.raises(ConfigError, match="does not implement"):
        registry.get("bedrock")


class _EntryPoint:
    def __init__(self, name: str, target: Any) -> None:
        self.name = name
        self._target = target

    def load(self) -> Any:
        return self._target


class _PluginProvider:
    name = "openai"

    def __init__(self, config: QueueConfig) -> None:
        self.config = config

    async def call(self, op: Any, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        return {}

    def classify(self, exc: BaseException) -> ErrorInfo:
        return ErrorInfo(kind=ErrorKind.FATAL)


def test_entry_point_plugin_used_and_import_path_wins(
    monkeypatch: pytest.MonkeyPatch, custom_module: str
) -> None:
    def fake_entry_points(*, group: str) -> list[_EntryPoint]:
        assert group == "taskiq_llm_queue.providers"
        return [_EntryPoint("openai", _PluginProvider), _EntryPoint("bedrock", _PluginProvider)]

    monkeypatch.setattr(registry_module, "entry_points", fake_entry_points)
    registry = ProviderRegistry(_config(providers={"bedrock": f"{custom_module}:EchoProvider"}))
    assert isinstance(registry.get("openai"), _PluginProvider)
    assert type(registry.get("bedrock")).__name__ == "EchoProvider"


def test_injected_factory_wins(custom_module: str) -> None:
    made: list[QueueConfig] = []

    def factory(config: QueueConfig) -> BedrockProvider:
        made.append(config)
        return BedrockProvider(region="eu-west-1")

    config = _config(providers={"bedrock": f"{custom_module}:EchoProvider"})
    registry = ProviderRegistry(config, factories={"bedrock": factory}, use_entry_points=False)
    assert isinstance(registry.get("bedrock"), BedrockProvider)
    assert made == [config]


def test_load_object_accepts_dotted_form() -> None:
    assert load_object("taskiq_llm_queue.providers.BedrockProvider") is BedrockProvider
    with pytest.raises(ConfigError, match="invalid import path"):
        load_object("nodots")


def test_importing_providers_does_not_import_sdks() -> None:
    code = (
        "import sys, taskiq_llm_queue, taskiq_llm_queue.providers;"
        "print(sorted(m for m in ('aiobotocore', 'openai', 'botocore') if m in sys.modules))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "[]"


@pytest.mark.parametrize(
    ("code", "family"),
    [
        ("ThrottlingException", "throttling"),
        ("throttling_exception", "throttling"),
        ("ModelErrorException", "modelerror"),
        ("Throttling", "throttling"),
        ("", ""),
    ],
)
def test_error_family(code: str, family: str) -> None:
    assert error_family(code) == family


def test_embedding_batch_limits_and_bodies() -> None:
    assert max_batch_size("bedrock", "amazon.titan-embed-text-v2:0") == 1
    assert max_batch_size("bedrock", "cohere.embed-multilingual-v3") == 96
    assert max_batch_size("openai", "text-embedding-3-small") == 2048
    assert max_batch_size("openai", "text-embedding-3-small", chunk_size=16) == 16
    assert list(batches(list(range(5)), 2)) == [[0, 1], [2, 3], [4]]
    with pytest.raises(ValueError, match=">= 1"):
        list(batches([1], 0))

    assert titan_body("hi", dimensions=256, normalize=True) == {
        "inputText": "hi",
        "dimensions": 256,
        "normalize": True,
    }
    assert cohere_body(["a"], input_type="search_query", truncate="END") == {
        "texts": ["a"],
        "input_type": "search_query",
        "truncate": "END",
    }
    with pytest.raises(ValueError, match="at most 96"):
        cohere_body(["x"] * 97)
    with pytest.raises(ValueError, match="exactly one"):
        embedding_body("amazon.titan-embed-text-v2:0", ["a", "b"])

    assert parse_embeddings({"embedding": [1.0]}) == [[1.0]]
    assert parse_embeddings({"embeddings": {"float": [[2.0]]}}) == [[2.0]]
    with pytest.raises(ValueError, match="no 'embedding'"):
        parse_embeddings({})
