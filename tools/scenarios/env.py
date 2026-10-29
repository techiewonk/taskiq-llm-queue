"""Harness settings: endpoints, key prefix and queue names, all overridable with `SCN_*` variables.

Defaults target the isolated `scn` compose profile (Valkey :16379, ElasticMQ :19324) and a
fakellm probe the harness starts on :18080, so chaos never touches the shared stack.
`SCN_PROVIDER=aws` keeps the queue local but sends provider calls to real Bedrock, with
credentials loaded like `samples/13_research_assistant_flow.py` (`SCN_ENV_SOURCE`).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
FAKELLM_DIR = ROOT / "tools" / "fakellm"
SCENARIO_YAML_DIR = FAKELLM_DIR / "scenarios"
# `SCN_COMPOSE_FILE` points chaos at a private stack (same service names, other ports/project).
COMPOSE = [
    "docker",
    "compose",
    "-f",
    os.environ.get("SCN_COMPOSE_FILE", str(ROOT / "docker" / "compose.yaml")),
]
REGION = "ap-south-1"

# fakellm behaviour keys for the model ids below (Bedrock keys are `<modelId>@<region>`).
GPT120B_ID = "openai.gpt-oss-120b-1:0"
GPT120B_PRIMARY = f"{GPT120B_ID}@{REGION}"
GPT120B_APAC = "apac.openai.gpt-oss-120b-1:0"
GPT20B_ID = "openai.gpt-oss-20b-1:0"
GPT20B_PRIMARY = f"{GPT20B_ID}@{REGION}"
TITAN_ID = "amazon.titan-embed-text-v2:0"
TITAN_PRIMARY = f"{TITAN_ID}@{REGION}"
COHERE_ID = "cohere.embed-english-v3"
COHERE_PRIMARY = f"{COHERE_ID}@{REGION}"
LLAMA_ID = "llama-3.1-70b"

PROVIDERS = ("fakellm", "aws")
ENV_SOURCES = ("auto", "dotenv", "secrets", "none")
# Reply cap for chat calls on real Bedrock, as in sample 13: at 256 gpt-oss-20b spent the whole
# budget on reasoning and returned no text block (S01, measured 2026-10-29).
AWS_MAX_TOKENS = 512
SAMPLES_DIR = ROOT / "samples"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _provider() -> str:
    provider = _env("SCN_PROVIDER", "fakellm").strip().lower()
    if provider not in PROVIDERS:
        raise SystemExit(f"SCN_PROVIDER must be one of {PROVIDERS}, got {provider!r}")
    return provider


@dataclass(frozen=True, slots=True)
class Settings:
    """Where the harness points: Valkey, SQS, fakellm, key prefix, queue-name prefix.

    `manage_fakellm` False means fakellm is external (a server run); `chaos` False skips
    scenarios that stop infrastructure (never run them against shared or real AWS services).
    `provider` `aws` sends model calls to real Bedrock; chaos then defaults to off.
    """

    provider: str = field(default_factory=_provider)
    env_source: str = field(default_factory=lambda: _env("SCN_ENV_SOURCE", "auto"))

    redis_host: str = field(default_factory=lambda: _env("SCN_REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(_env("SCN_REDIS_PORT", "16379")))
    redis_password: str = field(default_factory=lambda: _env("SCN_REDIS_PASSWORD", "localdev"))
    sqs_url: str = field(default_factory=lambda: _env("SCN_SQS_URL", "http://localhost:19324"))
    fakellm_url: str = field(
        default_factory=lambda: _env("SCN_FAKELLM_URL", "http://localhost:18080")
    )
    prefix: str = field(default_factory=lambda: _env("SCN_PREFIX", "llmq:scn"))
    queue_prefix: str = field(default_factory=lambda: _env("SCN_QUEUE_PREFIX", "scn"))
    manage_fakellm: bool = field(
        default_factory=lambda: _env("SCN_MANAGE_FAKELLM", "1") not in ("0", "false")
    )
    chaos: bool = field(
        default_factory=lambda: (
            _env("SCN_CHAOS", "0" if _provider() == "aws" else "1") not in ("0", "false")
        )
    )
    valkey_service: str = field(default_factory=lambda: _env("SCN_VALKEY_SERVICE", "valkey-scn"))
    sqs_service: str = field(default_factory=lambda: _env("SCN_SQS_SERVICE", "sqs-scn"))
    python: str = sys.executable

    @property
    def aws(self) -> bool:
        """True when provider calls go to real Bedrock (`SCN_PROVIDER=aws`)."""
        return self.provider == "aws"

    @property
    def fakellm_port(self) -> int:
        """Port of the managed fakellm probe (from `fakellm_url`)."""
        return int(self.fakellm_url.rsplit(":", 1)[1].split("/", 1)[0])

    def queue(self, name: str) -> str:
        """Queue name for a logical queue, e.g. `gpt20b-rt` -> `scn-gpt20b-rt`."""
        return f"{self.queue_prefix}-{name}"

    def worker_env(self) -> dict[str, str]:
        """Environment for child processes: the same settings plus a clean PYTHONPATH."""
        import taskiq_llm_queue

        src = str(Path(taskiq_llm_queue.__file__).resolve().parents[1])
        env = {k: v for k, v in os.environ.items() if not k.startswith("LLMQ_")}
        env.update(
            {
                "SCN_REDIS_HOST": self.redis_host,
                "SCN_REDIS_PORT": str(self.redis_port),
                "SCN_REDIS_PASSWORD": self.redis_password,
                "SCN_SQS_URL": self.sqs_url,
                "SCN_FAKELLM_URL": self.fakellm_url,
                "SCN_PREFIX": self.prefix,
                "SCN_QUEUE_PREFIX": self.queue_prefix,
                "PYTHONPATH": os.pathsep.join([src, str(ROOT), str(FAKELLM_DIR)]),
                "PYTHONUNBUFFERED": "1",
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", REGION),
            }
        )
        return env


def ensure_import_paths() -> None:
    """Make `fakellm` importable (scenarios compare against its deterministic vectors)."""
    if str(FAKELLM_DIR) not in sys.path:
        sys.path.append(str(FAKELLM_DIR))


def ensure_aws_env() -> None:
    """ElasticMQ and fakellm accept any credentials, but botocore needs some to sign."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", REGION)
    os.environ.setdefault("SCN_OPENAI_KEY", "sk-local")


def _samples_local() -> Any:
    """`samples/_local.py`, imported in its AWS mode so it never installs dummy credentials."""
    os.environ["LLMQ_SAMPLES_TARGET"] = "aws"  # `_local` reads this at import time
    if str(SAMPLES_DIR) not in sys.path:
        sys.path.append(str(SAMPLES_DIR))
    import _local

    return _local


async def load_aws_settings(source: str) -> list[str]:
    """Fill `os.environ` from `.env` and/or Secrets Manager exactly like sample 13.

    Precedence: real environment > Secrets Manager > `.env`. Returns report lines with key
    names and sources only, never values. Raises SystemExit when no AWS credentials result.
    """
    if source not in ENV_SOURCES:
        raise SystemExit(f"SCN_ENV_SOURCE must be one of {ENV_SOURCES}, got {source!r}")
    real_env = set(os.environ)  # captured before `.env` / secrets touch os.environ
    local = _samples_local()
    lines: list[str] = []
    if source in ("auto", "dotenv"):
        keys = local.load_env_file()
        lines.append(f".env ({local.env_file_path()}): {len(keys)} keys set")
    if source == "secrets" or (source == "auto" and local.app_secret_name() is not None):
        try:
            name, keys = await local.load_app_secret(real_env)
        except Exception as exc:
            if source == "secrets":
                raise SystemExit(f"secretsmanager failed: {type(exc).__name__}") from None
            lines.append(f"secretsmanager failed: {type(exc).__name__} (continuing)")
        else:
            lines.append(f"secretsmanager:{name}: {len(keys)} keys set")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise SystemExit("SCN_PROVIDER=aws: no AWS credentials in the environment/.env/secret")
    chat, embed = aws_regions()
    lines.append(f"regions: chat={chat} embed={embed}")
    return lines


def aws_regions() -> tuple[str, str]:
    """Chat and embedding regions for real Bedrock, resolved like the app (`_local`)."""
    regions: tuple[str, str] = _samples_local().app_regions()
    return regions
