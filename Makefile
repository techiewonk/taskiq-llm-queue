# Thin wrapper over tools/llmq_dev.py (works without make: `uv run python tools/llmq_dev.py <task>`).
RUN = uv run python tools/llmq_dev.py

.PHONY: up down logs lint format typecheck test-unit test-int test check capability

up down logs lint format typecheck test-unit test-int test check capability:
	$(RUN) $@
