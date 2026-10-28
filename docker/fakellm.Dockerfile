# fakellm: fake Bedrock runtime + OpenAI-compatible server for the local test stack.
# Build context is tools/fakellm (see compose.yaml), so the image holds only the fake.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install "aiohttp>=3.10,<4" "pyyaml>=6,<7"

WORKDIR /app
COPY fakellm/ /app/fakellm/
COPY scenarios/ /scenarios/

RUN useradd --system --uid 10001 fakellm
USER fakellm

ENV FAKELLM_SCENARIO=/scenarios/default.yaml
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=10 \
    CMD ["python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:8080/health', timeout=2)"]
CMD ["python", "-m", "fakellm", "--host", "0.0.0.0", "--port", "8080"]
