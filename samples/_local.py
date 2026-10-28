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

# `13_research_assistant_flow.py` sets this before importing `_local`: it loads `.env` /
# Secrets Manager first and calls `use_dummy_credentials()` itself (local target only), so
# dummy keys never shadow or mix with real ones.
TARGET_ENV = "LLMQ_SAMPLES_TARGET"


def use_dummy_credentials() -> None:
    """ElasticMQ and fakellm accept any credentials, but botocore needs some to sign requests.

    Real credentials already in the environment are kept (setdefault, all-or-nothing).
    """
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        os.environ.pop("AWS_SESSION_TOKEN", None)  # a real token with dummy keys fails
    os.environ.setdefault("AWS_DEFAULT_REGION", REGION)


if TARGET_ENV not in os.environ:
    use_dummy_credentials()
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


# ------------------------------------------------- app settings (.env, Secrets Manager)
#
# Used by `13_research_assistant_flow.py`; the other samples never read a file or a secret.
# Values are only ever put into `os.environ`; nothing here prints or logs a value.

ENV_FILE_ENV = "LLMQ_ENV_FILE"  # path of the env file; default: <package root>/.env
APP_VALKEY_ENV = "LLMQ_SAMPLES_APP_VALKEY"  # "true": queue on the app's Valkey (VALKEY_*)
PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The research-assistant app's model ids (src/configs/llm_config.py, src/constants/llm.py,
# src/core/query_planning/retrieve_query_plan.py). The app reads none of them from env.
APP_MODEL_IDS = {
    "gpt120b": "openai.gpt-oss-120b-1:0",
    "gpt20b": "openai.gpt-oss-20b-1:0",
    "titan": "amazon.titan-embed-text-v2:0",
    "cohere": "cohere.embed-english-v3",
}
APP_DEFAULT_REGION = "ap-south-1"  # bedrock_client.py DEFAULT_AWS_*_REGION


def env_file_path() -> str:
    """`LLMQ_ENV_FILE` if set, else `<package root>/.env`."""
    return os.environ.get(ENV_FILE_ENV) or os.path.join(PACKAGE_ROOT, ".env")


def parse_env_file(path: str) -> dict[str, str]:
    """Parse `KEY=VALUE` lines (optional `export `, quotes, `#` comments); no expansion."""
    found: dict[str, str] = {}
    with open(path, encoding="utf-8-sig") as file:
        for raw in file:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export ") :].lstrip()
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if not key.replace("_", "").isalnum():
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            elif " #" in value:
                value = value.split(" #", 1)[0].rstrip()
            found[key] = value
    return found


def load_env_file(path: str | None = None) -> list[str]:
    """`os.environ.setdefault` every key of the env file; returns the key names it set."""
    path = path or env_file_path()
    if not os.path.isfile(path):
        return []
    loaded = []
    for key, value in parse_env_file(path).items():
        if key not in os.environ:
            os.environ[key] = value
            loaded.append(key)
    return loaded


def app_secret_name() -> str | None:
    """The app's secret id: `SECRET_NAME_TEMPLATE.format(env=ENV)` (`secrets_loader.py`).

    None unless `ENV` or `SECRET_NAME_TEMPLATE` is set: the samples do not guess an env.
    """
    if "ENV" not in os.environ and "SECRET_NAME_TEMPLATE" not in os.environ:
        return None
    template = os.environ.get("SECRET_NAME_TEMPLATE", "{env}/researchassistant/secret")
    return template.format(env=os.environ.get("ENV", "dev"))


async def load_app_secret(protected: set[str]) -> tuple[str, list[str]]:
    """Fetch the app's Secrets Manager JSON and put its keys into `os.environ`.

    Mirrors the app: the secret wins over `.env` values, but a key in `protected` (the real
    process environment, captured before `.env` was loaded) is never overwritten. Region is
    `AWS_REGION` (default `ap-south-1`), as in `secrets_loader.py`.

    Returns:
        The secret id and the key names that were set.

    Raises:
        LookupError: If neither `ENV` nor `SECRET_NAME_TEMPLATE` is set.
        botocore errors: If the secret cannot be read (no credentials, access denied, ...).
    """
    name = app_secret_name()
    if name is None:
        raise LookupError("set ENV (and optionally SECRET_NAME_TEMPLATE) to read the secret")
    region = os.environ.get("AWS_REGION", APP_DEFAULT_REGION)
    async with get_session().create_client("secretsmanager", region_name=region) as client:
        reply = await client.get_secret_value(SecretId=name)
    values = orjson.loads(reply["SecretString"])
    loaded = []
    for key, value in values.items():
        if key in protected:
            continue
        os.environ[key] = value if isinstance(value, str) else orjson.dumps(value).decode()
        loaded.append(key)
    return name, loaded


def _first_env(*names: str) -> str | None:
    """The first non-empty variable among `names` (app secrets use both cases)."""
    for name in names:
        if value := os.environ.get(name):
            return value
    return None


def app_regions() -> tuple[str, str]:
    """Chat and embedding regions, like `bedrock_client.py` (they can differ)."""
    base = _first_env("AWS_REGION", "AWS_DEFAULT_REGION") or APP_DEFAULT_REGION
    chat = _first_env("AWS_MODEL_REGION") or base
    embed = _first_env("AWS_EMBEDDING_REGION") or base
    return chat, embed


def app_service_tier() -> str | None:
    """`BEDROCK_SERVICE_TIER` if it is one the app accepts (`llm_config.py`), else None."""
    tier = (os.environ.get("BEDROCK_SERVICE_TIER") or "").strip().lower()
    return tier if tier in {"priority", "default", "flex", "reserved"} else None


def app_valkey_env() -> dict[str, str]:
    """`LLMQ_REDIS_*` from the app's Valkey settings, only when `LLMQ_SAMPLES_APP_VALKEY=true`.

    Names: `VALKEY_HOST` / `VALKEY_PORT` / `VALKEY_KEY` / `VALKEY_SSL` (env) or the secret's
    `valkey_host` / `valkey_port` / `valkey_key`. Off by default: the samples queue on the
    local Docker Valkey even when the app's `.env` points at ElastiCache.
    """
    if os.environ.get(APP_VALKEY_ENV, "").lower() not in {"1", "true", "yes"}:
        return {}
    mapping = {
        "LLMQ_REDIS_HOST": _first_env("VALKEY_HOST", "valkey_host"),
        "LLMQ_REDIS_PORT": _first_env("VALKEY_PORT", "valkey_port"),
        "LLMQ_REDIS_PASSWORD": _first_env("VALKEY_KEY", "valkey_key"),
        "LLMQ_REDIS_SSL": os.environ.get("VALKEY_SSL", "true"),  # app default: TLS on
    }
    return {k: v for k, v in mapping.items() if v}


def aws_models() -> dict[str, ModelConfig]:
    """The app's Bedrock models on real AWS, on the same queue names as `local_models`."""
    chat_region, embed_region = app_regions()
    models = {}
    for alias in ("gpt120b", "gpt20b"):
        models[alias] = ModelConfig(
            provider="bedrock",
            model_id=APP_MODEL_IDS[alias],
            targets={"primary": Target(region=chat_region)},
            queues={"rt": f"llm-{alias}-rt", "default": f"llm-{alias}"},
        )
    for alias in ("titan", "cohere"):
        models[alias] = ModelConfig(
            provider="bedrock",
            model_id=APP_MODEL_IDS[alias],
            targets={"primary": Target(region=embed_region)},
            queues={"rt": "llm-embed-rt", "default": "llm-embed", "bulk": "llm-embed-bulk"},
        )
    return models


def aws_config(**overrides: Any) -> QueueConfig:
    """Queue on the local stack (unless `LLMQ_*` says otherwise), models on real Bedrock.

    `LLMQ_BEDROCK_ENDPOINT_URL` is dropped unless it is really set, so calls reach AWS.
    """
    env = dict(LOCAL_ENV)
    env.pop("LLMQ_BEDROCK_ENDPOINT_URL")
    env.update(app_valkey_env())
    env.update({k: v for k, v in os.environ.items() if k.startswith("LLMQ_")})
    fields = QueueConfig.from_env(env).model_dump()
    if "LLMQ_MODELS" not in env:
        fields["models"] = aws_models()
    if "LLMQ_FALLBACK_CHAINS" not in env:
        fields["fallback_default"] = {"chains": {"gpt120b": ["gpt20b"], "gpt20b": ["gpt120b"]}}
    fields.update(overrides)
    return QueueConfig.model_validate(fields)


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
    password = config.redis_password.get_secret_value() if config.redis_password else None
    client = Redis(host=config.redis_host, port=config.redis_port, password=password)
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
