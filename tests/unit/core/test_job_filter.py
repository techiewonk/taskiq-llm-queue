"""Job message filter: unknown jobs are released, known ones pass, also after S3 offload."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from taskiq.message import TaskiqMessage

from taskiq_llm_queue.envelope import JOB_NAME_LABEL, TASK_CALL, TASK_JOB
from taskiq_llm_queue.tasks import job_filter


def _message(task: str, args: list[Any], labels: dict[str, Any] | None = None) -> TaskiqMessage:
    return TaskiqMessage(task_id="t-1", task_name=task, labels=labels or {}, args=args, kwargs={})


def test_job_filter_reads_the_label_when_args_were_offloaded() -> None:
    """S24: S3 offload empties the args; the job name label still lets the job through."""
    accept = job_filter(SimpleNamespace(jobs={"ingest": object()}))  # type: ignore[arg-type]  # only .jobs is read
    assert accept(_message(TASK_JOB, [], {JOB_NAME_LABEL: "ingest"}))
    assert not accept(_message(TASK_JOB, [], {JOB_NAME_LABEL: "other"}))
    assert accept(_message(TASK_JOB, [{"job": "ingest"}]))  # older messages: no label
    assert not accept(_message(TASK_JOB, [{"job": "other"}]))
    assert accept(_message(TASK_CALL, []))  # LLM calls are never filtered here
