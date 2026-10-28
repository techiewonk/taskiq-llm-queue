# Thin wrapper over tools/llmq_dev.py (works without make: `uv run python tools/llmq_dev.py <task>`).
RUN = uv run python tools/llmq_dev.py

.PHONY: up down logs lint format typecheck test-unit test-int test check capability \
	up-scn down-scn scenarios scenario load chaos test-scenarios

up down logs lint format typecheck test-unit test-int test check capability up-scn down-scn:
	$(RUN) $@

# Scenario harness (docs/TESTING.md section 3):
#   make scenarios                 fast set        make scenarios TAG=slow|all
#   make scenarios S="S35 S35b"    chosen ids      make scenario S=S01
#   make load RATE=500 MINUTES=15  S27-style load  make chaos C=stop-sqs|start-sqs|stop-valkey|...
scenarios scenario:
	$(RUN) scenarios $(if $(TAG),--tag $(TAG)) $(S) $(if $(REPORT),--report $(REPORT))

load:
	$(RUN) load --rate $(or $(RATE),500) --minutes $(or $(MINUTES),15) --workers $(or $(WORKERS),3)

chaos:
	$(RUN) chaos $(C)

test-scenarios:
	$(RUN) test-scenarios $(S)
