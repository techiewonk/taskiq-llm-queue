"""The only module that touches the taskiq-llm-queue API; scenarios call these helpers.

It works with the 0.x API at the last commit (`timeout=`, `deadline=`, plain results) and the
reworked one (`timeout_secs=`, `deadline_at=`, `request_id=` everywhere, `*Result.meta`), so
API changes land here and nowhere else in the harness.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import io
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import orjson

import taskiq_llm_queue as llmq
from taskiq_llm_queue import KeySpace, LLMQueue, ModelConfig, QueueConfig, Target
from tools.scenarios.env import (
    COHERE_ID,
    GPT20B_ID,
    GPT120B_APAC,
    GPT120B_ID,
    LLAMA_ID,
    REGION,
    TITAN_ID,
    Settings,
    aws_regions,
)

LLMQueueError = llmq.LLMQueueError
LLMQueueTimeout = llmq.LLMQueueTimeout
ProviderError = llmq.ProviderError
QueueUnavailable = llmq.QueueUnavailable
ConfigError = llmq.ConfigError
# Newer API: a dedicated cancellation error; older: plain LLMQueueError.
LLMQueueCancelled: type[Exception] = getattr(llmq, "LLMQueueCancelled", llmq.LLMQueueError)

_NEW_API = "timeout_secs" in inspect.signature(LLMQueue.converse).parameters
_EMBED_HAS_ID = "request_id" in inspect.signature(LLMQueue.embed).parameters
OPENAI_KEY_ENV = "SCN_OPENAI_KEY"

# Fast-test defaults (production keeps the library defaults).
HARNESS_ENV: dict[str, Any] = {
    "sqs_wait_time_secs": 2,
    "retry_base_secs": 0.2,
    # The retry caps scale with the base (x0.2 of 30 s / 10 s), so the curve keeps its shape.
    "retry_max_secs": 6.0,
    "throttle_retry_max_secs": 2.0,
    "drain_secs": 5.0,
    "cfg_reload_secs": 2.0,
    "result_dispatch_ms": {"rt": 50, "bg": 100, "bulk": 200},
}


def api_flavour() -> str:
    """`new` (timeout_secs / deadline_at) or `old` (timeout / deadline) client API."""
    return "new" if _NEW_API else "old"


def models(settings: Settings, *, llama_url: str | None = None) -> dict[str, ModelConfig]:
    """Model aliases of the harness, on its own queue names (created by `stack.ensure_queues`).

    On real Bedrock (`settings.aws`) every alias has one regional target and there is no
    OpenAI-compatible `llama` (it only exists in fakellm).
    """
    q = settings.queue
    embed_queues: dict[Any, str] = {
        "rt": q("embed-rt"),
        "default": q("embed"),
        "bulk": q("embed-bulk"),
    }
    if settings.aws:
        chat, embed = aws_regions()
        ids = {"gpt120b": GPT120B_ID, "gpt20b": GPT20B_ID, "titan": TITAN_ID, "cohere": COHERE_ID}
        return {
            alias: ModelConfig(
                provider="bedrock",
                model_id=model_id,
                targets={"primary": Target(region=embed if alias in ("titan", "cohere") else chat)},
                queues=(
                    embed_queues
                    if alias in ("titan", "cohere")
                    else {"rt": q(f"{alias}-rt"), "default": q(alias)}
                ),
            )
            for alias, model_id in ids.items()
        }
    return {
        "gpt120b": ModelConfig(
            provider="bedrock",
            model_id=GPT120B_ID,
            targets={"primary": Target(region=REGION), "apac": Target(profile_id=GPT120B_APAC)},
            queues={"rt": q("gpt120b-rt"), "default": q("gpt120b")},
        ),
        "gpt20b": ModelConfig(
            provider="bedrock",
            model_id=GPT20B_ID,
            targets={"primary": Target(region=REGION)},
            queues={"rt": q("gpt20b-rt"), "default": q("gpt20b")},
        ),
        "titan": ModelConfig(
            provider="bedrock",
            model_id=TITAN_ID,
            targets={"primary": Target(region=REGION)},
            queues=embed_queues,
        ),
        "cohere": ModelConfig(
            provider="bedrock",
            model_id=COHERE_ID,
            targets={"primary": Target(region=REGION)},
            queues=embed_queues,
        ),
        "llama": ModelConfig(
            provider="openai",
            model_id=LLAMA_ID,
            targets={"local": Target(base_url=llama_url or f"{settings.fakellm_url}/v1")},
            queues={"rt": q("llama-rt"), "default": q("llama")},
            api_key_env=OPENAI_KEY_ENV,
        ),
    }


def all_queue_names(config: QueueConfig) -> list[str]:
    """Every queue the config names (models and jobs), without DLQs."""
    names = {v for m in config.models.values() for v in m.queues.values()}
    names |= set(config.job_queues.values())
    return sorted(names)


def build_config(settings: Settings, **overrides: Any) -> QueueConfig:
    """`QueueConfig` for the harness stack; keyword arguments override any field.

    With `settings.aws` there is no Bedrock endpoint override, so calls reach real AWS.
    """
    fields: dict[str, Any] = {
        "key_prefix": settings.prefix,
        "redis_host": settings.redis_host,
        "redis_port": settings.redis_port,
        "redis_password": settings.redis_password,
        "sqs_endpoint_url": settings.sqs_url,
        "bedrock_endpoint_url": None if settings.aws else settings.fakellm_url,
        "aws_region": aws_regions()[0] if settings.aws else REGION,
        "models": models(settings),
        "fallback_default": {"chains": {"gpt120b": ["gpt20b"]}},
        "job_queues": {"ingest": settings.queue("jobs-ingest")},
        **HARNESS_ENV,
    }
    fields.update(overrides)
    return QueueConfig.model_validate(fields)


def config_to_json(config: QueueConfig) -> bytes:
    """Serialise a config for a child process (secrets included: local stack only)."""
    data = config.model_dump(mode="json")
    secret = getattr(config.redis_password, "get_secret_value", None)
    data["redis_password"] = secret() if callable(secret) else config.redis_password
    return orjson.dumps(data)


def config_from_json(raw: bytes | str) -> QueueConfig:
    """Inverse of `config_to_json`."""
    return QueueConfig.model_validate(orjson.loads(raw))


def new_queue(
    config: QueueConfig,
    pod_id: str,
    *,
    metrics: Any = None,
    provider_factories: Mapping[str, Any] | None = None,
) -> LLMQueue:
    """Build (not start) an `LLMQueue`."""
    return LLMQueue(config, pod_id=pod_id, metrics=metrics, provider_factories=provider_factories)


def keys(prefix: str) -> KeySpace:
    """The library's key builder for `prefix` (both key schemas)."""
    return KeySpace(prefix)


# Key shapes of schema 1 (`<p>:lease:<m>:<t>`) and schema 2 (`<p>:{m:<m>:<t>}:lease`).
_LEASE_RE = re.compile(r":lease:|\}:lease$")
_CLAIM_RE = re.compile(r":job:(?P<a>.+)$|:\{r:(?P<b>.+)\}:job$")
_RESULT_RE = re.compile(r":res:(?P<a>.+)$|:\{r:(?P<b>.+)\}:res$")
_PARAMS_RE = re.compile(r":req:(?P<a>.+)$|:\{r:(?P<b>.+)\}:params$")


def _match_id(pattern: re.Pattern[str], key: str) -> str | None:
    match = pattern.search(key)
    if match is None:
        return None
    return match.group("a") or match.group("b")


def is_lease_key(key: str) -> bool:
    """True for a limiter lease sorted set."""
    return bool(_LEASE_RE.search(key))


def claim_id(key: str) -> str | None:
    """Request id of a claim key, else None."""
    return _match_id(_CLAIM_RE, key)


def result_id(key: str) -> str | None:
    """Request id of a stored result key, else None."""
    return _match_id(_RESULT_RE, key)


def params_id(key: str) -> str | None:
    """Request id of a by-reference params key, else None."""
    return _match_id(_PARAMS_RE, key)


# ------------------------------------------------------------------ calls


@dataclass(slots=True)
class Meta:
    """How a call was served (subset of the library's `CallMeta`)."""

    served_model: str = ""
    served_target: str = ""
    fallback_used: bool = False
    queue_wait_ms: int = 0
    provider_ms: int = 0
    attempts: int = 0
    worker: str = ""

    @classmethod
    def of(cls, raw: Any) -> Meta:
        """Copy the known fields of any `CallMeta`-like object."""
        if raw is None:
            return cls()
        return cls(**{name: getattr(raw, name, getattr(cls(), name)) for name in cls.__slots__})


@dataclass(slots=True)
class Reply:
    """Uniform call outcome: `ok`, the response or vectors, meta, or the exception."""

    ok: bool
    response: Any = None
    meta: Meta = field(default_factory=Meta)
    error: BaseException | None = None

    @property
    def error_kind(self) -> str:
        """`ok`, `timeout`, `cancelled`, `provider`, `unavailable`, `config` or `error`."""
        exc = self.error
        if exc is None:
            return "ok"
        if isinstance(exc, LLMQueueTimeout):
            return "timeout"
        if isinstance(exc, ProviderError):
            return "provider"
        if isinstance(exc, QueueUnavailable):
            return "unavailable"
        if isinstance(exc, ConfigError):
            return "config"
        if LLMQueueCancelled is not LLMQueueError and isinstance(exc, LLMQueueCancelled):
            return "cancelled"
        if isinstance(exc, LLMQueueError) and "cancel" in str(exc):
            return "cancelled"
        return "error"


def _wait_kwargs(timeout_secs: float | None, deadline_at: float | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if timeout_secs is not None:
        out["timeout_secs" if _NEW_API else "timeout"] = timeout_secs
    if deadline_at is not None:
        out["deadline_at" if _NEW_API else "deadline"] = deadline_at
    return out


def converse_request(text: str, *, max_tokens: int | None = None) -> dict[str, Any]:
    """A Bedrock Converse request with one user message."""
    request: dict[str, Any] = {"messages": [{"role": "user", "content": [{"text": text}]}]}
    if max_tokens is not None:
        request["inferenceConfig"] = {"maxTokens": max_tokens}
    return request


async def converse(
    q: LLMQueue,
    model: str,
    request: dict[str, Any],
    *,
    lane: str | None = None,
    fallback: str | None = None,
    timeout_secs: float | None = None,
    deadline_at: float | None = None,
    request_id: str | None = None,
) -> Reply:
    """`LLMQueue.converse`, never raising an `LLMQueueError` (it is returned in the reply)."""
    try:
        result = await q.converse(
            model,
            request,
            lane=lane,
            fallback=fallback,  # type: ignore[arg-type]  # literal checked by the library
            request_id=request_id,
            **_wait_kwargs(timeout_secs, deadline_at),
        )
    except LLMQueueError as exc:
        return Reply(False, error=exc)
    return Reply(True, result.response, Meta.of(result.meta))


async def chat(
    q: LLMQueue,
    model: str,
    request: dict[str, Any],
    *,
    lane: str | None = None,
    timeout_secs: float | None = None,
    request_id: str | None = None,
) -> Reply:
    """`LLMQueue.chat` (OpenAI-compatible); the response is the completion dict."""
    try:
        result: Any = await q.chat(
            model,
            request,
            lane=lane,
            request_id=request_id,
            **_wait_kwargs(timeout_secs, None),
        )
    except LLMQueueError as exc:
        return Reply(False, error=exc)
    if isinstance(result, dict):
        return Reply(True, result)
    return Reply(True, result.response, Meta.of(getattr(result, "meta", None)))


async def embed(
    q: LLMQueue,
    model: str,
    texts: list[str],
    *,
    lane: str | None = None,
    input_type: str | None = None,
    timeout_secs: float | None = None,
    request_id: str | None = None,
) -> Reply:
    """`LLMQueue.embed`; the response is the list of vectors in input order."""
    extra: dict[str, Any] = {"request_id": request_id} if _EMBED_HAS_ID and request_id else {}
    try:
        result: Any = await q.embed(
            model,
            texts,
            lane=lane,
            input_type=input_type,
            **_wait_kwargs(timeout_secs, None),
            **extra,
        )
    except LLMQueueError as exc:
        return Reply(False, error=exc)
    vectors = list(getattr(result, "vectors", result))
    return Reply(True, vectors, Meta.of(getattr(result, "meta", None)))


def converse_sync(q: LLMQueue, model: str, request: dict[str, Any], **kw: Any) -> Reply:
    """Blocking `converse_sync` (runs `direct` under the shared limiter)."""
    timeout = kw.pop("timeout_secs", None)
    try:
        result = q.converse_sync(model, request, **kw, **_wait_kwargs(timeout, None))
    except LLMQueueError as exc:
        return Reply(False, error=exc)
    return Reply(True, result.response, Meta.of(result.meta))


def embed_sync(q: LLMQueue, model: str, texts: list[str]) -> Reply:
    """Blocking `embed_sync`."""
    try:
        result: Any = q.embed_sync(model, texts)
    except LLMQueueError as exc:
        return Reply(False, error=exc)
    return Reply(True, list(getattr(result, "vectors", result)))


async def send_job(q: LLMQueue, name: str, payload: dict[str, Any], *, job_id: str) -> str:
    """`LLMQueue.send_job` with the harness defaults."""
    return await q.send_job(name, payload, job_id=job_id)


# ------------------------------------------------------------------ operator CLI


def cli_environ(config: QueueConfig) -> dict[str, str]:
    """`LLMQ_*` variables describing `config`, for `llm-queue` CLI calls."""
    data = orjson.loads(config_to_json(config))
    return {
        "LLMQ_KEY_PREFIX": config.key_prefix,
        "LLMQ_REDIS_HOST": config.redis_host,
        "LLMQ_REDIS_PORT": str(config.redis_port),
        "LLMQ_REDIS_PASSWORD": str(data["redis_password"] or ""),
        "LLMQ_REDIS_SSL": "false",
        "LLMQ_SQS_ENDPOINT_URL": str(config.sqs_endpoint_url),
        "LLMQ_AWS_REGION": str(config.aws_region),
        "LLMQ_MODELS": orjson.dumps(data["models"]).decode(),
        "LLMQ_FALLBACK_CHAINS": orjson.dumps(data["fallback_default"]).decode(),
    }


async def cli(config: QueueConfig, *argv: str) -> tuple[int, str]:
    """Run `llm-queue <argv>` in a thread (it owns its event loop); returns (code, stdout)."""
    from taskiq_llm_queue.cli import main

    def run() -> tuple[int, str]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = main(list(argv), environ=cli_environ(config))
        return code, out.getvalue()

    return await asyncio.to_thread(run)


async def fallback_set(config: QueueConfig, model: str, chain: Sequence[str]) -> tuple[int, str]:
    """`llm-queue fallback set --model <model> --chain a,b`."""
    return await cli(config, "fallback", "set", "--model", model, "--chain", ",".join(chain))


async def routing(config: QueueConfig, *argv: str) -> tuple[int, str]:
    """`llm-queue routing ...` (set / kill / get)."""
    return await cli(config, "routing", *argv)


def custom_provider_factory(provider: Any) -> Any:
    """Provider factory for `provider_factories=` (signature differs between versions)."""

    def factory(*_args: Any, **_kwargs: Any) -> Any:
        return provider

    return factory


def error_kind_of(info_kind: Any) -> Any:
    """`ErrorInfo` for a custom provider's `classify` (kind is `throttle|retryable|fatal`)."""
    return llmq.ErrorInfo(llmq.ErrorKind(str(info_kind)))
