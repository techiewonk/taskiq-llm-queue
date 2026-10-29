"""Metric names and label sets, fixed here so dashboards and alarms don't drift from code."""

from __future__ import annotations

QUEUE_WAIT_SECONDS = "llmq_queue_wait_seconds"
PROVIDER_SECONDS = "llmq_provider_seconds"
CALLS_TOTAL = "llmq_calls_total"
THROTTLES_TOTAL = "llmq_throttles_total"
FALLBACK_TOTAL = "llmq_fallback_total"
LIMIT = "llmq_limit"
INFLIGHT = "llmq_inflight"
BREAKER_OPEN = "llmq_breaker_open"
SYNC_FALLBACK_TOTAL = "llmq_sync_fallback_total"
RELEASED_UNKNOWN_TOTAL = "llmq_released_unknown_total"
CONFIG_INVALID_TOTAL = "llmq_config_invalid_total"
FAILOVER_ACTIVE = "llmq_failover_active"
# Broker (SQS) events, per queue (I14).
SQS_RECEIVED_TOTAL = "llmq_sqs_received_total"
SQS_SENT_TOTAL = "llmq_sqs_sent_total"
SQS_ACKED_TOTAL = "llmq_sqs_acked_total"
SQS_RELEASED_TOTAL = "llmq_sqs_released_total"  # kind = reason (shutdown, drain, unknown_task..)
SQS_EXPIRED_TOTAL = "llmq_sqs_expired_total"
SQS_ERRORS_TOTAL = "llmq_sqs_errors_total"  # step = SQS operation, kind = error family
SQS_WAIT_SECONDS = "llmq_sqs_wait_seconds"  # time a message sat in SQS before receipt

# Allowed labels; raw user/session ids are never metric labels (cardinality, privacy).
LABELS = ("model", "target", "lane", "queue", "path", "kind", "step")
