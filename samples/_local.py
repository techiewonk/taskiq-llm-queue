"""Shared settings for the samples: a `QueueConfig` pointing at the local Docker stack.

The stack (`make up`, or `docker compose -f docker/compose.yaml up -d --wait`) runs:

- Valkey on localhost:6379 (password `localdev`): limiter, claims, results, live config.
- ElasticMQ on http://localhost:9324: fake SQS with the queues from `docker/elasticmq.conf`.
- fakellm on http://localhost:8080: fake Bedrock + OpenAI-compatible API, plus an admin API.

Every connection setting can be overridden with the usual `LLMQ_*` variables (the same names
`QueueConfig.from_env()` reads in production); the defaults below only fill what is not set.
Models are declared in code here unless `LLMQ_MODELS` is set.

Print the environment for the `llm-queue` CLI (see `12_cli.md`):

    python samples/_local.py env              # bash / zsh `export` lines
    python samples/_local.py env --powershell # PowerShell `$env:` lines
"""

from __future__ import annotations

import os
import sys
from typing import Any

import aiohttp
import orjson
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from taskiq_llm_queue import ModelConfig, QueueConfig, Target

FAKELLM_URL = os.environ.get("FAKELLM_URL", "http://localhost:8080")
REGION = "ap-south-1"  # fakellm keys Bedrock behaviour by region ("<modelId>@ap-south-1")
OPENAI_KEY_ENV = "LLMQ_SAMPLES_OPENAI_KEY"  # fakellm accepts any key

# fakellm scenario keys (see tools/fakellm/scenarios/default.yaml), used by the admin API.
GPT120B_PRIMARY = "openai.gpt-oss-120b-1:0@ap-south-1"
GPT120B_APAC = "apac.openai.gpt-oss-120b-1:0"
GPT20B_PRIMARY = "openai.gpt-oss-20b-1:0@ap-south-1"

# Local defaults; anything already set in the environment wins.
LOCAL_ENV: dict[str, str] = {
    "LLMQ_KEY_PREFIX": "llmq:local",
    "LLMQ_REDIS_HOST": "localhost",
    "LLMQ_REDIS_PORT": "6379",
    "LLMQ_REDIS_PASSWORD": "localdev",
    "LLMQ_REDIS_SSL": "false",
    "LLMQ_SQS_ENDPOINT_URL": "http://localhost:9324",
    "LLMQ_BEDROCK_ENDPOINT_URL": FAKELLM_URL,
    "LLMQ_AWS_REGION": REGION,
    # Sample-friendly timings: short long-polls and quick retries so scripts exit fast.
    # Production keeps the defaults (20 s long-poll, 1 s retry base, 45 s drain).
    "LLMQ_SQS_WAIT_TIME_S": "2",
    "LLMQ_RETRY_BASE_S": "0.2",
    "LLMQ_CONSUMER_DRAIN_S": "5",
    "LLMQ_RESULT_DISPATCH_MS": "rt:50,bg:100,bulk:200",
}

# ElasticMQ and fakellm accept any credentials, but botocore needs some to sign requests.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", REGION)
os.environ.setdefault(OPENAI_KEY_ENV, "sk-local")


def local_models() -> dict[str, ModelConfig]:
    """The model aliases every sample uses, mapped to the queues in `docker/elasticmq.conf`."""
    return {
        # Chat model with two targets: the region first, then a cross-region profile.
        "gpt120b": ModelConfig(
            provider="bedrock",
            model_id="openai.gpt-oss-120b-1:0",
            targets={
                "primary": Target(region=REGION),
                "apac": Target(profile_id=GPT120B_APAC),
            },
            queues={"rt": "llm-gpt120b-rt", "default": "llm-gpt120b"},
        ),
        "gpt20b": ModelConfig(
            provider="bedrock",
            model_id="openai.gpt-oss-20b-1:0",
            targets={"primary": Target(region=REGION)},
            queues={"rt": "llm-gpt20b-rt", "default": "llm-gpt20b"},
        ),
        # Embedding models share one queue set (and so one broker per pod).
        "titan": ModelConfig(
            provider="bedrock",
            model_id="amazon.titan-embed-text-v2:0",
            targets={"primary": Target(region=REGION)},
            queues={"rt": "llm-embed-rt", "default": "llm-embed", "bulk": "llm-embed-bulk"},
        ),
        "cohere": ModelConfig(
            provider="bedrock",
            model_id="cohere.embed-english-v3",
            targets={"primary": Target(region=REGION)},
            queues={"rt": "llm-embed-rt", "default": "llm-embed", "bulk": "llm-embed-bulk"},
        ),
        # OpenAI-compatible server (fakellm here; vLLM, Ollama, ... in real life).
        # These two queues are not in elasticmq.conf; `ensure_queues` creates them.
        "llama": ModelConfig(
            provider="openai",
            model_id="llama-3.1-70b",
            targets={"local": Target(base_url=f"{FAKELLM_URL}/v1")},
            queues={"rt": "llm-llama-rt", "default": "llm-llama"},
            api_key_env=OPENAI_KEY_ENV,
        ),
    }


def local_env() -> dict[str, str]:
    """`LLMQ_*` settings: local defaults overlaid with whatever the environment sets."""
    env = dict(LOCAL_ENV)
    env.update({k: v for k, v in os.environ.items() if k.startswith("LLMQ_")})
    return env


def local_config(**overrides: Any) -> QueueConfig:
    """Build the sample `QueueConfig`; keyword arguments override any field.

    The default fallback chain lets `gpt120b` fall back to `gpt20b` under policy `chain`.
    """
    env = local_env()
    fields = QueueConfig.from_env(env).model_dump()
    if "LLMQ_MODELS" not in env:
        fields["models"] = local_models()
    if "LLMQ_FALLBACK_CHAINS" not in env:
        fields["fallback_default"] = {"chains": {"gpt120b": ["gpt20b"]}}
    fields.update(overrides)
    return QueueConfig.model_validate(fields)


async def ensure_queues(config: QueueConfig) -> None:
    """Create any SQS queue the config names that does not exist yet (ElasticMQ only).

    The library never creates queues: on AWS they come from your infrastructure code.
    """
    names = {q for m in config.models.values() for q in m.queues.values()}
    names |= set(config.job_queues.values())
    async with get_session().create_client(
        "sqs", region_name=config.aws_region, endpoint_url=config.sqs_endpoint_url
    ) as sqs:
        for name in sorted(names):
            try:
                await sqs.get_queue_url(QueueName=name)
            except ClientError:
                await sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "60"})


# ---------------------------------------------------------------- fakellm admin API


async def fakellm(method: str, path: str, body: Any = None) -> Any:
    """Call the fakellm admin API (`/admin/scenario`, `/admin/models/{key}`, ...)."""
    async with (
        aiohttp.ClientSession() as session,
        session.request(method, f"{FAKELLM_URL}{path}", json=body) as resp,
    ):
        resp.raise_for_status()
        return await resp.json()


async def patch_model(key: str, patch: dict[str, Any]) -> None:
    """Change one fakellm model key's behaviour, e.g. `{"force_error": "ThrottlingException"}`."""
    await fakellm("PATCH", f"/admin/models/{key}", patch)


async def save_scenario() -> Any:
    """The current fakellm scenario, to put back later with `restore_scenario`."""
    return await fakellm("GET", "/admin/scenario")


async def restore_scenario(scenario: Any) -> None:
    """Replace the fakellm scenario (this also clears its counters)."""
    await fakellm("PUT", "/admin/scenario", scenario)


# ---------------------------------------------------------------- command line helpers


def _print_env(powershell: bool) -> None:
    """Print the local settings plus models and chains as shell assignments for the CLI."""
    env = local_env()
    config = local_config()
    env.setdefault("LLMQ_MODELS", orjson.dumps(config.model_dump()["models"]).decode())
    env.setdefault("LLMQ_FALLBACK_CHAINS", orjson.dumps(config.fallback_default).decode())
    env.update({k: os.environ[k] for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")})
    for name, value in env.items():
        if powershell:
            print(f"$env:{name} = '{value}'")
        else:
            print(f"export {name}='{value}'")


async def _print_queues() -> None:
    """Print visible / in-flight message counts of every sample queue and its DLQ."""
    config = local_config(job_queues={"ingest": "jobs-ingest"})
    names = {q for m in config.models.values() for q in m.queues.values()}
    names |= set(config.job_queues.values())
    async with get_session().create_client(
        "sqs", region_name=config.aws_region, endpoint_url=config.sqs_endpoint_url
    ) as sqs:
        print(f"{'queue':24s} {'visible':>8s} {'in flight':>9s}")
        for name in sorted(names | {f"{n}-dlq" for n in names}):
            try:
                url = (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
            except ClientError:
                continue  # e.g. no DLQ for queues ensure_queues created
            reply = await sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )
            found = reply["Attributes"]
            visible = found["ApproximateNumberOfMessages"]
            in_flight = found["ApproximateNumberOfMessagesNotVisible"]
            print(f"{name:24s} {visible:>8s} {in_flight:>9s}")


def _default_scenario() -> Any:
    """Load the default fakellm scenario (tools/fakellm/scenarios/default.yaml)."""
    import yaml  # type: ignore[import-untyped]  # PyYAML comes with langchain-core

    path = os.path.join(
        os.path.dirname(__file__), "..", "tools", "fakellm", "scenarios", "default.yaml"
    )
    with open(path, encoding="utf-8") as file:
        return yaml.safe_load(file)


async def _reset(scenario: Any) -> None:
    """Delete every Valkey key under the sample prefix and reload fakellm's default scenario.

    Clears learned limits, breakers, live fallback/routing config and leftover results, so
    the samples start from scratch (e.g. after `05_fallback_on_throttle.py`).
    """
    from redis.asyncio import Redis

    config = local_config()
    client = Redis(host=config.redis_host, port=config.redis_port, password=config.redis_password)
    try:
        keys = [k async for k in client.scan_iter(match=f"{config.key_prefix}:*", count=500)]
        if keys:
            await client.delete(*keys)
    finally:
        await client.aclose()
    await restore_scenario(scenario)
    print(f"deleted {len(keys)} keys under {config.key_prefix}:*; fakellm scenario reset")


if __name__ == "__main__":
    import asyncio

    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "env":
        _print_env("--powershell" in sys.argv)
    elif command == "queues":
        asyncio.run(_print_queues())
    elif command == "reset":
        asyncio.run(_reset(_default_scenario()))
    else:
        sys.exit("usage: python samples/_local.py env [--powershell] | queues | reset")
