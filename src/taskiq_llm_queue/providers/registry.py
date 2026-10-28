"""Provider registry: built-ins, import-path overrides and entry-point plugins.

Resolution order per provider name: `QueueConfig.providers` (`"pkg.mod:Class"`), then the
`taskiq_llm_queue.providers` entry-point group, then the built-ins. Every check
(`allowed_providers`, `denied_hosts`, importability) runs at construction, i.e. at boot.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.protocols import Provider
from taskiq_llm_queue.providers.hosts import denied_pattern

if TYPE_CHECKING:
    from taskiq_llm_queue.config import QueueConfig

ENTRY_POINT_GROUP = "taskiq_llm_queue.providers"
BUILTIN_PROVIDERS: Mapping[str, str] = {
    "bedrock": "taskiq_llm_queue.providers.bedrock:BedrockProvider",
    "openai": "taskiq_llm_queue.providers.openai_compat:OpenAICompatProvider",
}

ProviderFactory = Callable[["QueueConfig"], Provider]


def load_object(path: str) -> Any:
    """Import `"pkg.mod:attr.sub"` (or `"pkg.mod.attr"`); failures raise `ConfigError`."""
    module_name, sep, attr_path = path.partition(":")
    if not sep:
        module_name, _, attr_path = path.rpartition(".")
    if not module_name or not attr_path:
        raise ConfigError(f"invalid import path {path!r}; expected 'pkg.module:Name'")
    try:
        obj: Any = importlib.import_module(module_name)
        for attr in attr_path.split("."):
            obj = getattr(obj, attr)
    except (ImportError, AttributeError) as exc:
        raise ConfigError(f"cannot load provider {path!r}: {exc}") from exc
    return obj


def as_factory(obj: Any) -> ProviderFactory:
    """Turn a provider class or callable into a factory taking `QueueConfig`.

    Classes with a `from_config` classmethod use it; anything else is called with the config.
    """
    from_config = getattr(obj, "from_config", None)
    factory = from_config if callable(from_config) else obj
    if not callable(factory):
        raise ConfigError(f"provider {obj!r} is not callable")
    return factory  # type: ignore[no-any-return]


class ProviderRegistry:
    """Builds and caches one `Provider` per name for a `QueueConfig`, enforcing policy.

    Used by workers to find the provider for a model alias; `aclose()` closes them all.
    `factories` injects ready factories (e.g. a Bedrock provider with a shared session).
    """

    def __init__(
        self,
        config: QueueConfig,
        *,
        factories: Mapping[str, ProviderFactory] | None = None,
        use_entry_points: bool = True,
    ) -> None:
        """Validate every provider the config needs and resolve its factory; collect errors."""
        self._config = config
        self._providers: dict[str, Provider] = {}
        self._factories: dict[str, ProviderFactory] = {}
        injected = dict(factories or {})
        plugins = (
            {ep.name: ep for ep in entry_points(group=ENTRY_POINT_GROUP)}
            if use_entry_points
            else {}
        )
        errors: list[str] = []
        names = {m.provider for m in config.models.values()} | set(config.providers)
        for name in sorted(names):
            if name not in config.allowed_providers:
                errors.append(f"provider {name!r} is not in allowed_providers")
                continue
            try:
                if name in injected:
                    self._factories[name] = injected[name]
                elif name in config.providers:
                    self._factories[name] = as_factory(load_object(config.providers[name]))
                elif name in plugins:
                    self._factories[name] = as_factory(plugins[name].load())
                elif name in BUILTIN_PROVIDERS:
                    self._factories[name] = as_factory(load_object(BUILTIN_PROVIDERS[name]))
                else:
                    errors.append(f"no provider registered under {name!r}")
            except ConfigError as exc:
                errors.append(str(exc))
            except Exception as exc:  # a broken plugin must fail boot with its name
                errors.append(f"cannot load provider {name!r}: {exc!r}")
        errors.extend(_host_errors(config))
        if errors:
            raise ConfigError("; ".join(errors))

    def get(self, name: str) -> Provider:
        """Return the provider registered under `name`, building it on first use."""
        if (provider := self._providers.get(name)) is not None:
            return provider
        factory = self._factories.get(name)
        if factory is None:
            raise ConfigError(f"provider {name!r} is not configured or not allowed")
        try:
            provider = factory(self._config)
        except ConfigError:
            raise
        except Exception as exc:  # name the provider: a bare TypeError from a plugin is opaque
            raise ConfigError(f"provider {name!r} failed to build: {exc!r}") from exc
        if not isinstance(provider, Provider):
            raise ConfigError(f"provider {name!r} does not implement the Provider protocol")
        self._providers[name] = provider
        return provider

    def for_model(self, model: str) -> Provider:
        """Return the provider serving `alias[@target]`."""
        _, cfg, _ = self._config.resolve(model)
        return self.get(cfg.provider)

    async def aclose(self) -> None:
        """Close every built provider that has an `aclose()`; safe to call more than once."""
        providers = list(self._providers.values())
        self._providers.clear()
        for provider in providers:
            close = getattr(provider, "aclose", None)
            if callable(close):
                await close()


def _host_errors(config: QueueConfig) -> list[str]:
    """Every configured endpoint (model targets, Bedrock endpoint) that hits `denied_hosts`."""
    if not config.denied_hosts:
        return []
    urls = [
        (f"{alias}@{name}", t.base_url)
        for alias, m in config.models.items()
        for name, t in m.targets.items()
    ]
    urls.append(("bedrock_endpoint_url", config.bedrock_endpoint_url))
    errors = []
    for where, url in urls:
        if url and (pattern := denied_pattern(url, config.denied_hosts)) is not None:
            errors.append(f"{where}: endpoint {url!r} matches denied host {pattern!r}")
    return errors
