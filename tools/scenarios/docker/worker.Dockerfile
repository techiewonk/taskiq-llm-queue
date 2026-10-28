# Scenario worker: the package plus tools/scenarios, slim. Build context is the package root;
# worker.Dockerfile.dockerignore keeps .venv, caches and secrets out of the context.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE NOTICE CHANGELOG.md STABILITY.md /app/
COPY src/ /app/src/
RUN pip install "/app" "pyyaml>=6,<7" "aiohttp>=3.10,<4"
COPY tools/scenarios/ /app/tools/scenarios/

RUN useradd --system --uid 10001 llmq
USER llmq
ENTRYPOINT ["python", "-m", "tools.scenarios.worker"]
CMD ["--name", "docker-worker"]
