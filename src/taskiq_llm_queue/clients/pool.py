"""One SDK client per (account, region), built on first use with that account's credentials.

A `Route` says where one attempt goes: the account, the region the client calls and the model
id (an inference profile's id for a profile). `ClientPool` hands out Bedrock and
OpenAI-compatible clients for routes and closes them all at `aclose()`.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue.errors import ConfigError, LLMQueueError
from taskiq_llm_queue.providers.hosts import check_url

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from openai import AsyncOpenAI
    from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

    from taskiq_llm_queue.registry import Account, Deployment, ResolvedRegistry

# Unverified starting values. The SDK read timeout only guards a dead socket: the library's own
# first-response and idle timers decide when an attempt is given up.
DEFAULT_READ_TIMEOUT_SECS = 300.0
DEFAULT_CONNECT_TIMEOUT_SECS = 10.0
DEFAULT_MAX_POOL_CONNECTIONS = 100

# Extra `create_client` arguments for an AWS account (None = the pod's own credential chain).
CredentialSource = Callable[["Account"], Mapping[str, Any] | None]
# The value of a secret by name (`api_key_ref`, `external_id_ref`); default: the environment.
SecretSource = Callable[[str], str | None]


@dataclass(frozen=True, slots=True)
class Route:
    """Where one attempt goes: account, region to call, model id, and the deployment's name."""

    account: Account
    region: str
    model_id: str
    deployment: str


def route_for(view: ResolvedRegistry, deployment: Deployment) -> Route:
    """The route of a deployment: its account and region, its profile id or the model id."""
    model = view.registry.models[deployment.model]
    return Route(
        account=view.registry.accounts[deployment.account],
        region=deployment.region,
        model_id=deployment.profile_id or model.model_id,
        deployment=deployment.key,
    )


def _pod_chain(account: Account) -> Mapping[str, Any] | None:
    return None


class CredentialsUnavailable(LLMQueueError):
    """An account's credentials could not be obtained (STS refused or unreachable)."""


ROLE_SESSION_SECS = 3600  # the role-chaining maximum (plan §3.6)


class ClientPool:
    """Clients per (account, region) for Bedrock and per account for OpenAI-compatible servers.

    `credentials` gives an AWS account's client arguments (tests pass static keys per account);
    `secrets` reads `api_key_ref` values. Clients are created once and closed by `aclose()`.
    """

    def __init__(
        self,
        *,
        bedrock_endpoint_url: str | None = None,
        credentials: CredentialSource = _pod_chain,
        secrets: SecretSource = os.environ.get,
        session: AioSession | None = None,
        denied_hosts: tuple[str, ...] = (),
        read_timeout_secs: float = DEFAULT_READ_TIMEOUT_SECS,
        connect_timeout_secs: float = DEFAULT_CONNECT_TIMEOUT_SECS,
        max_pool_connections: int = DEFAULT_MAX_POOL_CONNECTIONS,
        sts_endpoint_url: str | None = None,
        session_name: str = "llmq",
    ) -> None:
        """Keep settings only; no SDK import and no client until the first call.

        `session_name` names assumed-role sessions (`llmq-<env>-<pod>` in CloudTrail).
        """
        check_url(bedrock_endpoint_url, denied_hosts)
        self._endpoint = bedrock_endpoint_url
        self._credentials = credentials
        self._secrets = secrets
        self._session = session
        self._denied = denied_hosts
        self._read = read_timeout_secs
        self._connect = connect_timeout_secs
        self._pool_size = max_pool_connections
        self._bedrock: dict[tuple[str, str], BedrockRuntimeClient] = {}
        self._sts_endpoint = sts_endpoint_url
        self._session_name = session_name
        self._roles: dict[str, AioSession] = {}
        self._openai: dict[str, AsyncOpenAI] = {}
        self._stack = AsyncExitStack()
        self._lock = asyncio.Lock()

    async def bedrock(self, route: Route) -> BedrockRuntimeClient:
        """The Bedrock runtime client of the route's account and region."""
        key = (route.account.name, route.region)
        if (client := self._bedrock.get(key)) is not None:
            return client
        async with self._lock:
            if (client := self._bedrock.get(key)) is not None:
                return client
            from aiobotocore.config import AioConfig
            from aiobotocore.session import get_session

            if self._session is None:
                self._session = get_session()
            session = self._session
            extra = dict(self._credentials(route.account) or {})
            if route.account.role_arn:
                session = await self._role_session(route)
                extra = {}
            config = AioConfig(
                read_timeout=self._read,
                connect_timeout=self._connect,
                max_pool_connections=self._pool_size,
                # One attempt per call: throttles reach the library, not botocore's retries.
                retries={"total_max_attempts": 1, "mode": "standard"},
            )
            context = session.create_client(
                "bedrock-runtime",
                region_name=route.region,
                endpoint_url=self._endpoint,
                config=config,
                **extra,
            )
            client = await self._stack.enter_async_context(context)
            self._bedrock[key] = client
            return client

    async def _role_session(self, route: Route) -> AioSession:
        """A session whose credentials come from STS AssumeRole and refresh before expiry."""
        account = route.account
        if account.name in self._roles:
            return self._roles[account.name]
        from aiobotocore.credentials import AioRefreshableCredentials
        from aiobotocore.session import AioSession, get_session

        base = self._session or get_session()
        base_kwargs = dict(self._credentials(account) or {})
        external_id = self._secrets(account.external_id_ref) if account.external_id_ref else None
        if account.external_id_ref and not external_id:
            raise CredentialsUnavailable(
                f"account {account.name}: secret {account.external_id_ref} is not set"
            )

        async def assume() -> dict[str, Any]:
            request: dict[str, Any] = {
                "RoleArn": account.role_arn,
                "RoleSessionName": self._session_name[:64],
                "DurationSeconds": ROLE_SESSION_SECS,
            }
            if external_id:
                request["ExternalId"] = external_id
            try:
                async with base.create_client(
                    "sts", region_name=route.region, endpoint_url=self._sts_endpoint, **base_kwargs
                ) as sts:
                    reply = await sts.assume_role(**request)
            except Exception as exc:
                raise CredentialsUnavailable(
                    f"account {account.name}: AssumeRole failed: {exc}"
                ) from exc
            creds = reply["Credentials"]
            return {
                "access_key": creds["AccessKeyId"],
                "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"],
                "expiry_time": creds["Expiration"].isoformat(),
            }

        credentials = AioRefreshableCredentials.create_from_metadata(
            metadata=await assume(), refresh_using=assume, method="sts-assume-role"
        )
        session = AioSession()
        # aiobotocore has no public setter for refreshable credentials.
        session._credentials = credentials  # type: ignore[attr-defined]
        self._roles[account.name] = session
        return session

    def openai(self, route: Route) -> AsyncOpenAI:
        """The OpenAI-compatible client of the route's account (its `base_url` and key)."""
        account = route.account
        if (client := self._openai.get(account.name)) is not None:
            return client
        check_url(account.base_url, self._denied)
        try:
            import httpx
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise ConfigError(
                "openai accounts need the openai SDK: pip install 'taskiq-llm-queue[openai]'"
            ) from exc
        key = self._secrets(account.api_key_ref) if account.api_key_ref else None
        if account.api_key_ref and not key:
            raise ConfigError(f"account {account.name}: secret {account.api_key_ref} is not set")
        client = AsyncOpenAI(
            api_key=key or "unused",
            base_url=account.base_url,
            max_retries=0,
            timeout=httpx.Timeout(self._read, connect=self._connect),
        )
        self._openai[account.name] = client
        return client

    def forget(self, route: Route) -> None:
        """Drop an account's cached clients so the next call reads its credentials again."""
        for key in [k for k in self._bedrock if k[0] == route.account.name]:
            del self._bedrock[key]
        self._roles.pop(route.account.name, None)
        self._openai.pop(route.account.name, None)

    async def aclose(self) -> None:
        """Close every client; safe to call more than once."""
        async with self._lock:
            self._bedrock.clear()
            openai_clients = list(self._openai.values())
            self._openai.clear()
            stack, self._stack = self._stack, AsyncExitStack()
            await stack.aclose()
        for client in openai_clients:
            await client.close()
