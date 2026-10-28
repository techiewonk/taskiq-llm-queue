"""OpenAI-compatible provider: chat completions and embeddings against any base URL.

Request bodies are OpenAI-native and pass through unchanged (streaming forced off). SDK
retries are off (`max_retries=0`) so a 429 and its `Retry-After` reach the limiter.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.errors import ConfigError, ErrorInfo
from taskiq_llm_queue.providers.classify import classify_openai
from taskiq_llm_queue.providers.hosts import check_url

if TYPE_CHECKING:
    from openai import AsyncOpenAI

    from taskiq_llm_queue.config import QueueConfig, Target
    from taskiq_llm_queue.envelope import Op

DEFAULT_TIMEOUT_SECS = 300.0  # unverified; long completions on slow self-hosted servers


class OpenAICompatProvider:
    """`Provider` for OpenAI-compatible servers: ops `chat` and `embeddings`.

    One `AsyncOpenAI` client per base URL (`Target.base_url`, else the provider default),
    each with the API key configured for that URL. Responses are `model_dump()` dicts.
    """

    name = "openai"

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_keys: Mapping[str | None, str | None] | None = None,
        denied_hosts: Iterable[str] = (),
        timeout_secs: float = DEFAULT_TIMEOUT_SECS,
        client_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Keep settings only; the openai SDK is imported on the first call.

        `api_keys` maps a base URL (None = the default URL) to its key; a missing key lets
        the SDK fall back to `OPENAI_API_KEY`.
        """
        self._denied_hosts = tuple(denied_hosts)
        check_url(base_url, self._denied_hosts)
        self._base_url = base_url
        self._api_keys = dict(api_keys or {})
        self._timeout_secs = timeout_secs
        self._client_kwargs = dict(client_kwargs or {})
        self._clients: dict[str | None, AsyncOpenAI] = {}

    @classmethod
    def from_config(
        cls, config: QueueConfig, *, environ: Mapping[str, str] | None = None, **kwargs: Any
    ) -> OpenAICompatProvider:
        """Build from `QueueConfig`: deny-list every OpenAI target and read keys at boot.

        Each target's `base_url` gets its model's `api_key_env`. A missing variable or two
        models sharing a URL with different key variables is a `ConfigError` (all reported).
        """
        env = os.environ if environ is None else environ
        key_envs: dict[str | None, str | None] = {}
        errors: list[str] = []
        for alias, model in config.models.items():
            if model.provider != "openai":
                continue
            for target in model.targets.values():
                check_url(target.base_url, config.denied_hosts)
                url = target.base_url
                if url in key_envs and key_envs[url] != model.api_key_env:
                    errors.append(f"{alias}: base_url {url!r} already uses {key_envs[url]!r}")
                key_envs.setdefault(url, model.api_key_env)
        api_keys: dict[str | None, str | None] = {}
        for url, name in key_envs.items():
            if name is None:
                continue
            if not env.get(name):
                errors.append(f"api_key_env {name!r} (for {url or 'default URL'}) is not set")
            api_keys[url] = env.get(name)
        if errors:
            raise ConfigError("; ".join(errors))
        return cls(api_keys=api_keys, denied_hosts=config.denied_hosts, **kwargs)

    async def call(self, op: Op, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        """Run one chat-completions or embeddings attempt and return the response as a dict."""
        client = self._client(target.base_url)
        if op == "chat":
            response: Any = await client.chat.completions.create(**{**params, "stream": False})
        elif op == "embeddings":
            response = await client.embeddings.create(**params)
        else:
            raise ValueError(f"OpenAI-compatible provider does not support op {op!r}")
        result: dict[str, Any] = response.model_dump(mode="json", exclude_unset=True)
        return result

    def classify(self, exc: BaseException) -> ErrorInfo:
        """Throttle (with Retry-After) / retryable / fatal by status (see `classify_openai`)."""
        return classify_openai(exc)

    async def aclose(self) -> None:
        """Close every client this provider created; safe to call more than once."""
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            await client.close()

    def _client(self, target_url: str | None) -> AsyncOpenAI:
        """Return the cached client for a target URL (None = default), deny-listing it first.

        Keys are looked up by the target URL only, so one host's key never goes to another.
        """
        if (client := self._clients.get(target_url)) is not None:
            return client
        base_url = target_url or self._base_url
        check_url(base_url, self._denied_hosts)
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise ConfigError(
                "OpenAICompatProvider needs the openai SDK: pip install 'taskiq-llm-queue[openai]'"
            ) from exc
        client = AsyncOpenAI(
            api_key=self._api_keys.get(target_url),
            base_url=base_url,
            max_retries=0,
            timeout=self._timeout_secs,
            **self._client_kwargs,
        )
        self._clients[target_url] = client
        return client
