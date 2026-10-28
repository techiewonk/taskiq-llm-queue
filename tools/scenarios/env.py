"""Harness settings: endpoints, key prefix and queue names, all overridable with `SCN_*` variables.

Defaults target the isolated `scn` compose profile (Valkey :16379, ElasticMQ :19324) and a
fakellm probe the harness starts on :18080, so chaos never touches the shared stack.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FAKELLM_DIR = ROOT / "tools" / "fakellm"
SCENARIO_YAML_DIR = FAKELLM_DIR / "scenarios"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker" / "compose.yaml")]
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


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True, slots=True)
class Settings:
    """Where the harness points: Valkey, SQS, fakellm, key prefix, queue-name prefix.

    `manage_fakellm` False means fakellm is external (a server run); `chaos` False skips
    scenarios that stop infrastructure (never run them against shared or real AWS services).
    """

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
    chaos: bool = field(default_factory=lambda: _env("SCN_CHAOS", "1") not in ("0", "false"))
    valkey_service: str = field(default_factory=lambda: _env("SCN_VALKEY_SERVICE", "valkey-scn"))
    sqs_service: str = field(default_factory=lambda: _env("SCN_SQS_SERVICE", "sqs-scn"))
    python: str = sys.executable

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
