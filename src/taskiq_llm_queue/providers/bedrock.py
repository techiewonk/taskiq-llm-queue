"""Amazon Bedrock runtime provider: Converse and InvokeModel through aiobotocore.

Payloads are provider-native and pass through unchanged, so responses are exactly what
`ChatBedrockConverse` / `BedrockEmbeddings` parse. SDK retries are off (one attempt per call)
so throttles reach the adaptive limiter instead of being hidden by botocore.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Iterable, Mapping
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.errors import ConfigError, ErrorInfo
from taskiq_llm_queue.providers.classify import classify_bedrock
from taskiq_llm_queue.providers.hosts import check_url

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

    from taskiq_llm_queue.config import QueueConfig, Target
    from taskiq_llm_queue.envelope import Op

# Unverified starting values: Bedrock replies can run minutes, and the pool must not be the
# bottleneck below the limiter (botocore's default pool is 10 connections per client).
DEFAULT_READ_TIMEOUT_SECS = 300.0
DEFAULT_CONNECT_TIMEOUT_SECS = 10.0
DEFAULT_MAX_POOL_CONNECTIONS = 100


class BedrockProvider:
    """`Provider` for the Bedrock runtime: ops `converse` and `invoke_model`.

    One aiobotocore client per (region, endpoint), created on first use and closed by
    `aclose()`. The model id is `Target.profile_id` when set, else `params["modelId"]`.
    """

    name = "bedrock"

    def __init__(
        self,
        *,
        session: AioSession | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        denied_hosts: Iterable[str] = (),
        read_timeout_secs: float = DEFAULT_READ_TIMEOUT_SECS,
        connect_timeout_secs: float = DEFAULT_CONNECT_TIMEOUT_SECS,
        max_pool_connections: int = DEFAULT_MAX_POOL_CONNECTIONS,
        client_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Keep settings only; no SDK import and no client until the first call.

        `client_kwargs` go to `create_client` (e.g. explicit credentials for a fake endpoint).
        """
        self._denied_hosts = tuple(denied_hosts)
        check_url(endpoint_url, self._denied_hosts)
        self._session = session
        self._region = region
        self._endpoint_url = endpoint_url
        self._config_kwargs: dict[str, Any] = {
            "read_timeout": read_timeout_secs,
            "connect_timeout": connect_timeout_secs,
            "max_pool_connections": max_pool_connections,
            # `max_attempts: 1` would mean one *retry* (botocore adds 1); total is exact.
            "retries": {"total_max_attempts": 1, "mode": "standard"},
        }
        self._client_kwargs = dict(client_kwargs or {})
        self._clients: dict[tuple[str, str | None], BedrockRuntimeClient] = {}
        self._stack = AsyncExitStack()
        self._lock = asyncio.Lock()

    @classmethod
    def from_config(cls, config: QueueConfig, **kwargs: Any) -> BedrockProvider:
        """Build from `QueueConfig` (region, endpoint, deny list); checks every Bedrock target URL.

        Extra keyword arguments go to the constructor (e.g. `session`, `client_kwargs`).
        """
        for model in config.models.values():
            if model.provider == "bedrock":
                for target in model.targets.values():
                    check_url(target.base_url, config.denied_hosts)
        return cls(
            region=config.aws_region,
            endpoint_url=config.bedrock_endpoint_url,
            denied_hosts=config.denied_hosts,
            **kwargs,
        )

    async def call(self, op: Op, params: Mapping[str, Any], target: Target) -> dict[str, Any]:
        """Run one Converse or InvokeModel attempt and return a JSON-safe response dict.

        `converse` returns the Converse body without `ResponseMetadata`; `invoke_model` returns
        `{"body_b64", "contentType"}` (see `decode_invoke_body`).
        """
        request = dict(params)
        model_id = target.profile_id or request.get("modelId")
        if not model_id:
            raise ValueError("Bedrock call needs Target.profile_id or params['modelId']")
        request["modelId"] = model_id
        if op == "converse":
            client = await self._client(target)
            response: dict[str, Any] = dict(await client.converse(**request))
            response.pop("ResponseMetadata", None)
            return response
        if op == "invoke_model":
            body = request.get("body")
            if isinstance(body, Mapping | list):
                request["body"] = json.dumps(body)
            request.setdefault("contentType", "application/json")
            request.setdefault("accept", "application/json")
            client = await self._client(target)
            raw = await client.invoke_model(**request)
            async with raw["body"] as stream:
                data = await stream.read()
            return {
                "body_b64": base64.b64encode(data).decode("ascii"),
                "contentType": raw.get("contentType", ""),
            }
        raise ValueError(f"Bedrock provider does not support op {op!r}")

    def classify(self, exc: BaseException) -> ErrorInfo:
        """Throttle / retryable / fatal by Bedrock error family (see `classify_bedrock`)."""
        return classify_bedrock(exc)

    async def aclose(self) -> None:
        """Close every client this provider created; safe to call more than once."""
        async with self._lock:
            self._clients.clear()
            stack, self._stack = self._stack, AsyncExitStack()
            await stack.aclose()

    async def _client(self, target: Target) -> BedrockRuntimeClient:
        """Return the cached client for this target's region and endpoint, creating it once."""
        endpoint = target.base_url or self._endpoint_url
        region = target.region or self._region
        key = (region, endpoint)
        if (client := self._clients.get(key)) is not None:
            return client
        check_url(endpoint, self._denied_hosts)
        async with self._lock:
            if (client := self._clients.get(key)) is not None:
                return client
            try:
                from aiobotocore.config import AioConfig
                from aiobotocore.session import get_session
            except ImportError as exc:  # pragma: no cover - aiobotocore is a core dependency
                raise ConfigError(
                    "BedrockProvider needs aiobotocore: pip install 'taskiq-llm-queue'"
                ) from exc
            if self._session is None:
                self._session = get_session()
            context = self._session.create_client(
                "bedrock-runtime",
                region_name=region,
                endpoint_url=endpoint,
                config=AioConfig(**self._config_kwargs),
                **self._client_kwargs,
            )
            client = await self._stack.enter_async_context(context)
            self._clients[key] = client
            return client


def decode_invoke_body(response: Mapping[str, Any]) -> Any:
    """Decode an `invoke_model` result: parsed JSON for JSON content types, else raw bytes.

    Used by embedding callers to turn `{"body_b64", "contentType"}` back into the model body.
    """
    data = base64.b64decode(response["body_b64"])
    if "json" in str(response.get("contentType", "")).lower():
        return json.loads(data)
    return data
