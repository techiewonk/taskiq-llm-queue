"""Jobs on the stream queue (plan E136): `register_job` / `send_job`, run at least once.

A job is any registered async function taking one JSON payload. `send_job` puts it on
`<queue_prefix>jobs-<name>`; workers of processes that registered the name run it. A failed
job is retried after a backoff and goes to the dead-letter stream after `max_receives`; a held
job's visibility is extended while it runs. Jobs take no LLM budget (their own LLM calls do).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import orjson

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.engine.queue import Message, QueueSettings, StreamQueues
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.ids import new_id

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)
JobFn = Callable[[dict[str, Any]], Awaitable[Any]]


class JobRunner:
    """Registered jobs of one process, their senders and their worker loops."""

    def __init__(
        self,
        queues: StreamQueues,
        *,
        queue_prefix: str = "",
        settings: QueueSettings | None = None,
        concurrency: int = 4,
    ) -> None:
        """Nothing runs until `start`."""
        self.queues = queues
        self.queue_prefix = queue_prefix
        self.settings = settings or QueueSettings(visibility_secs=60, max_receives=5, max_waiting=0)
        self.concurrency = concurrency
        self._jobs: dict[str, JobFn] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._stopping = False
        self._paused = False

    def queue(self, name: str) -> str:
        """The queue of one job name."""
        return f"{self.queue_prefix}jobs-{name}"

    def register(self, name: str, fn: JobFn) -> None:
        """Allow this process to run job `name` (before `start`)."""
        if not name or not callable(fn):
            raise ConfigError("a job needs a name and an async function")
        self._jobs[name] = fn

    async def send(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once: make them idempotent)."""
        ident = job_id or new_id()
        body = orjson.dumps({"id": ident, "name": name, "payload": payload})
        await self.queues.send(self.queue(name), body, self.settings)
        return ident

    async def start(self) -> None:
        """One worker loop per registered job."""
        for name in self._jobs:
            self._tasks.append(asyncio.create_task(self._loop(name)))

    def pause(self) -> None:
        """Take no new jobs; running ones go on (`stop()` follows)."""
        self._paused = True

    async def stop(self, drain_secs: float = 10.0) -> None:
        """Stop receiving, let running jobs finish (up to `drain_secs`), then stop."""
        self._stopping = True
        if self._running:
            await asyncio.wait(set(self._running), timeout=drain_secs)
        for task in [*self._tasks, *self._running]:
            task.cancel()
        for task in [*self._tasks, *self._running]:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _loop(self, name: str) -> None:
        gate = asyncio.Semaphore(self.concurrency)
        while not self._stopping:
            if self._paused:
                await asyncio.sleep(0.2)
                continue
            try:
                await gate.acquire()
                messages, dead = await self.queues.receive_wait(
                    self.queue(name), 1, self.settings, wait_secs=1.0
                )
                for letter in dead:
                    logger.error("job_dead_lettered", job=name, message=letter.id)
                if not messages:
                    gate.release()
                    continue
                task = asyncio.create_task(self._run(name, messages[0], gate))
                self._running.add(task)
                task.add_done_callback(self._running.discard)
            except Exception as exc:  # a store blip must not end the loop
                gate.release()
                logger.warning("job_receive_failed", job=name, error=str(exc))
                await asyncio.sleep(1)

    async def _run(self, name: str, message: Message, gate: asyncio.Semaphore) -> None:
        keep = asyncio.create_task(self._keep_visible(message))
        try:
            body = orjson.loads(message.body)
            await self._jobs[name](body["payload"])
            await self.queues.ack(message)
            logger.info("job_done", job=name, id=body.get("id"))
        except Exception as exc:
            outcome = await self.queues.fail(message, self.settings, delay_secs=5.0)
            logger.warning("job_failed", job=name, error=str(exc), outcome=outcome)
        finally:
            keep.cancel()
            gate.release()

    async def _keep_visible(self, message: Message) -> None:
        """Keep a running job hidden; a store blip must not end it (the job would run twice)."""
        while True:
            await asyncio.sleep(max(1.0, self.settings.visibility_secs / 3))
            try:
                if not await self.queues.extend(message, self.settings.visibility_secs):
                    logger.warning("job_lost_visibility", queue=message.queue, message=message.id)
            except Exception as exc:
                logger.warning("job_extend_failed", message=message.id, error=str(exc))
