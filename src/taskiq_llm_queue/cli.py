"""`llm-queue` operator CLI: routing, fallback chains, limiter/breaker state, queues and jobs.

Connection settings come from `QueueConfig.from_env()` (`LLMQ_*`). Live config starts from the
app's defaults (`LLMQ_FALLBACK_CHAINS`, `LLMQ_ROUTING_DEFAULT`) while nothing is stored, and
`set` edits merge into it. Every change is validated and audited with `actor` (`--actor`,
default the OS user). `job send` and `queues` touch only SQS.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import getpass
import logging
import sys
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orjson
from pydantic import BaseModel
from redis.exceptions import RedisError

from taskiq_llm_queue.config import Lane, QueueConfig, create_redis
from taskiq_llm_queue.envelope import TASK_JOB
from taskiq_llm_queue.errors import ConfigError, LLMQueueError
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability.breaker import BreakerState, CircuitBreaker
from taskiq_llm_queue.reliability.config_store import LiveConfigStore, fallback_store
from taskiq_llm_queue.reliability.fallback import FallbackConfig, resolve_fallback
from taskiq_llm_queue.reliability.limiter import AdaptiveLimiter, LimiterSettings
from taskiq_llm_queue.routing import RoutingConfig, routing_store

if TYPE_CHECKING:
    import redis.asyncio

DEFAULT_JOB_QUEUE = "jobs-ingest"
JOB_VERSION = 1  # job body version; must equal tasks.JOB_VERSION (not imported: CLI stays light)
_QUEUE_COUNTS = (
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
)


def build_parser() -> argparse.ArgumentParser:
    """The argparse tree for every `llm-queue` sub-command."""
    parser = argparse.ArgumentParser(prog="llm-queue", description="taskiq-llm-queue operator CLI")
    parser.add_argument("--actor", help="name written to the audit stream (default: OS user)")
    groups = parser.add_subparsers(dest="group", required=True)

    routing = groups.add_parser("routing", help="canary routing sqs vs direct")
    r_cmd = routing.add_subparsers(dest="cmd", required=True)
    r_cmd.add_parser("get", help="print the live routing config (app default if none stored)")
    r_set = r_cmd.add_parser("set", help="set the sqs percentage of one model and lane")
    r_set.add_argument("--model", required=True)
    r_set.add_argument("--lane", required=True, choices=[lane.value for lane in Lane])
    r_set.add_argument("--pct", required=True, type=int)
    r_kill = r_cmd.add_parser("kill", help="kill switch: on routes everything direct")
    r_kill.add_argument("state", choices=["on", "off"])
    r_allow = r_cmd.add_parser("allow", help="allowlist users that always go sqs")
    r_allow.add_argument("action", choices=["add", "remove"])
    r_allow.add_argument("user_id")
    r_cmd.add_parser("reset", help="delete the stored config (back to the app default)")

    fallback = groups.add_parser("fallback", help="live fallback chains")
    f_cmd = fallback.add_subparsers(dest="cmd", required=True)
    f_get = f_cmd.add_parser("get", help="whole config, or the resolved chain of one model")
    f_get.add_argument("--model")
    f_set = f_cmd.add_parser(
        "set", help="set one model's chain, or merge a config file (per model) into the live one"
    )
    source = f_set.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", type=Path)
    source.add_argument("--chain", help='comma separated, e.g. "gpt120b@apac,gpt20b"')
    f_set.add_argument("--model", help="required with --chain")
    f_set.add_argument(
        "--replace", action="store_true", help="with --file: replace the whole config, no merge"
    )
    f_test = f_cmd.add_parser("test", help="dry run: which step would serve now")
    f_test.add_argument("--model", required=True)
    f_cmd.add_parser("reset", help="delete the stored config (back to the app default)")

    job = groups.add_parser("job", help="background jobs")
    j_cmd = job.add_subparsers(dest="cmd", required=True)
    j_send = j_cmd.add_parser("send", help="send one job message to SQS (send permission only)")
    j_send.add_argument("name")
    j_send.add_argument("--payload-file", required=True, type=Path)
    j_send.add_argument("--queue", default=DEFAULT_JOB_QUEUE, help="queue name or URL")
    j_send.add_argument("--lane", default=Lane.BULK.value, choices=[lane.value for lane in Lane])

    limiter = groups.add_parser("limiter", help="adaptive limiter state")
    l_cmd = limiter.add_subparsers(dest="cmd", required=True)
    l_show = l_cmd.add_parser("show", help="limit, in-flight and pause per model target")
    l_show.add_argument("--model", help="alias or alias@target (default: every configured model)")
    l_reset = l_cmd.add_parser("reset", help="forget the learned limit and pause (back to start)")
    l_reset.add_argument("--model", required=True, help="alias or alias@target")
    l_reset.add_argument("--target", help="one target (default: every target of the model)")

    breaker = groups.add_parser("breaker", help="circuit breaker state")
    b_cmd = breaker.add_subparsers(dest="cmd", required=True)
    b_show = b_cmd.add_parser("show", help="breaker position per model target")
    b_show.add_argument("--model", help="alias or alias@target (default: every configured model)")
    b_reset = b_cmd.add_parser("reset", help="close the breaker (forget recorded failures)")
    b_reset.add_argument("--model", required=True, help="alias or alias@target")
    b_reset.add_argument("--target", help="one target (default: every target of the model)")

    groups.add_parser("queues", help="message counts per configured SQS queue and its DLQ")
    return parser


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    """Run one command; returns the exit code (0 ok, 1 failed, 2 usage error from argparse)."""
    args = build_parser().parse_args(argv)
    try:
        config = QueueConfig.from_env(environ)
        return asyncio.run(_dispatch(args, config))
    except (LLMQueueError, RedisError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def console_main() -> None:
    """`llm-queue` console script: warnings and errors to stderr, so stdout stays pure JSON.

    The library never configures logging; this entry point is an app, so it does.
    """
    logging.basicConfig(
        level=logging.WARNING, stream=sys.stderr, format="%(levelname)s %(message)s"
    )
    sys.exit(main())


def _actor(args: argparse.Namespace) -> str:
    if args.actor:
        return str(args.actor)
    try:
        return getpass.getuser()
    except (OSError, KeyError):  # no login name (some containers)
        return "unknown"


def _print_json(value: Any) -> None:
    print(orjson.dumps(value, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS).decode())


def _dump(model: BaseModel) -> Any:
    return model.model_dump(mode="json")


@contextlib.asynccontextmanager
async def _valkey(config: QueueConfig) -> AsyncIterator[redis.asyncio.Redis]:
    client = create_redis(config, client_name="llm-queue-cli")
    try:
        yield client
    finally:
        await client.aclose()


async def _dispatch(args: argparse.Namespace, config: QueueConfig) -> int:
    if args.group == "job":
        return await _job_send(args, config)
    if args.group == "queues":
        return await _queues(config)
    keys = KeySpace(config.key_prefix)
    async with _valkey(config) as client:
        if args.group == "routing":
            default = RoutingConfig.model_validate(config.routing_default)
            return await _routing(args, routing_store(client, keys, default))
        if args.group == "fallback":
            return await _fallback(args, config, client, keys)
        if args.group == "breaker":
            return await _breaker(args, config, client, keys)
        if args.cmd == "reset":
            return await _limiter_reset(args, config, client, keys)
        return await _limiter_show(args, config, client, keys)


async def _load_checked(store: LiveConfigStore[Any]) -> Any:
    """Load for a read-modify-write; warn when the stored value was invalid and is replaced."""
    before = store.invalid_count
    current = await store.load()
    if store.invalid_count > before:
        print(
            f"warning: stored {store.key} is invalid; editing the default instead "
            "(the old value stays in the audit stream)",
            file=sys.stderr,
        )
    return current


async def _routing(args: argparse.Namespace, store: LiveConfigStore[RoutingConfig]) -> int:
    if args.cmd == "get":
        _print_json(_dump(await store.load()))
        return 0
    if args.cmd == "reset":
        _print_json(_dump(await store.reset(actor=_actor(args))))
        return 0
    current: RoutingConfig = await _load_checked(store)
    if args.cmd == "set":
        updated = current.with_pct(args.model, Lane(args.lane), args.pct)
    elif args.cmd == "kill":
        updated = current.with_kill(args.state == "on")
    else:
        updated = current.with_user(args.user_id, allowed=args.action == "add")
    stored = await store.set(updated, actor=_actor(args))
    _print_json(_dump(stored))
    return 0


def _merge_fallback(current: Mapping[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    """Per-model merge: models in `update` replace theirs, the others keep their entries."""
    merged = {k: (dict(v) if isinstance(v, Mapping) else v) for k, v in current.items()}
    for key, value in update.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


async def _fallback(
    args: argparse.Namespace, config: QueueConfig, client: redis.asyncio.Redis, keys: KeySpace
) -> int:
    default = FallbackConfig.model_validate(config.fallback_default)
    store = fallback_store(client, keys, default)
    actor = _actor(args)
    if args.cmd == "reset":
        _print_json(_dump(await store.reset(actor=actor)))
        return 0
    if args.cmd == "set":
        if args.file is not None:
            update = orjson.loads(args.file.read_bytes())
            if not isinstance(update, dict):
                raise ConfigError("the fallback file must hold a JSON object")
            if not args.replace:
                current: FallbackConfig = await _load_checked(store)
                update = _merge_fallback(_dump(current), update)
            stored = await store.set(update, actor=actor)
        else:
            if not args.model:
                raise ConfigError("--chain needs --model")
            chain = [s.strip() for s in args.chain.split(",") if s.strip()]
            current = await _load_checked(store)
            raw = _dump(current)
            raw["chains"][args.model] = chain
            stored = await store.set(raw, actor=actor)
        _print_json(_dump(stored))
        return 0
    live = await store.load()
    if args.cmd == "get" and not args.model:
        _print_json(_dump(live))
        return 0
    if args.model.partition("@")[0] not in config.models:
        if args.cmd == "test":
            raise ConfigError(f"{args.model!r} is not in LLMQ_MODELS; cannot resolve targets")
        print(orjson.dumps([_dump(s) for s in live.chains.get(args.model, ())]).decode())
        return 0
    steps = resolve_fallback(config, args.model, policy="chain", live=live)
    if args.cmd == "get":
        print(" -> ".join(f"{s.alias}@{s.target}" for s in steps))
        return 0
    limiter = AdaptiveLimiter(client, keys, LimiterSettings.from_config(config))
    breaker = CircuitBreaker(client, keys)
    serving = None
    rows = []
    for step in steps:
        state = await limiter.state(step.alias, step.target)
        position = await breaker.state(step.alias, step.target)
        usable = position is not BreakerState.OPEN and state.headroom() > 0
        if usable and serving is None:
            serving = f"{step.alias}@{step.target}"
        rows.append(
            {
                "step": f"{step.alias}@{step.target}",
                "switched_model": step.switched_model,
                "limit": state.limit,
                "inflight": state.inflight,
                "paused_secs": round(state.paused_secs, 3),
                "breaker": position.value,
                "usable": usable,
            }
        )
    _print_json({"model": args.model, "would_serve": serving, "steps": rows})
    return 0


def _pairs(
    config: QueueConfig, model: str | None, target: str | None = None
) -> list[tuple[str, str]]:
    """(alias, target) pairs for `--model alias[@target]` / `--target`, or every configured one."""
    if not model:
        return [(a, t) for a, m in config.models.items() for t in m.targets]
    alias, _, at = model.partition("@")
    target = target or at
    if target:
        return [(alias, target)]
    if alias in config.models:
        return [(alias, t) for t in config.models[alias].targets]
    raise ConfigError(f"{alias!r} is not in LLMQ_MODELS; pass alias@target or --target")


async def _audit(
    client: redis.asyncio.Redis, keys: KeySpace, key: str, actor: str, before: Any
) -> None:
    """Append a reset to the audit stream (same fields as live-config changes)."""
    fields = {
        "key": key,
        "actor": actor,
        "before": orjson.dumps(before).decode(),
        "after": "",
        "ts": repr(time.time()),
    }
    await client.xadd(keys.audit, fields)  # type: ignore[arg-type]  # redis stub: str fields


async def _limiter_show(
    args: argparse.Namespace, config: QueueConfig, client: redis.asyncio.Redis, keys: KeySpace
) -> int:
    limiter = AdaptiveLimiter(client, keys, LimiterSettings.from_config(config))
    breaker = CircuitBreaker(client, keys)
    rows = []
    for alias, target in _pairs(config, args.model):
        state = await limiter.state(alias, target)
        position = await breaker.state(alias, target)
        rows.append(
            {
                "model": alias,
                "target": target,
                "limit": state.limit,
                "inflight": state.inflight,
                "paused_secs": round(state.paused_secs, 3),
                "breaker": position.value,
            }
        )
    _print_json(rows)
    return 0


async def _limiter_reset(
    args: argparse.Namespace, config: QueueConfig, client: redis.asyncio.Redis, keys: KeySpace
) -> int:
    """Delete the learned state (limit, ramp, pause); live leases are kept (real in-flight)."""
    limiter = AdaptiveLimiter(client, keys, LimiterSettings.from_config(config))
    rows = []
    for alias, target in _pairs(config, args.model, args.target):
        before = await limiter.state(alias, target)
        key = keys.limiter(alias, target)
        await client.delete(key)
        snapshot = {"limit": before.limit, "paused_secs": round(before.paused_secs, 3)}
        await _audit(client, keys, key, _actor(args), snapshot)
        rows.append({"model": alias, "target": target, "reset": True, "before": snapshot})
    _print_json(rows)
    return 0


async def _breaker(
    args: argparse.Namespace, config: QueueConfig, client: redis.asyncio.Redis, keys: KeySpace
) -> int:
    breaker = CircuitBreaker(client, keys)
    rows = []
    for alias, target in _pairs(config, args.model, getattr(args, "target", None)):
        position = await breaker.state(alias, target)
        row: dict[str, Any] = {"model": alias, "target": target, "breaker": position.value}
        if args.cmd == "reset":
            key = keys.breaker(alias, target)
            await client.delete(key)
            await _audit(client, keys, key, _actor(args), {"breaker": position.value})
            row = {**row, "before": position.value, "breaker": BreakerState.CLOSED.value}
        rows.append(row)
    _print_json(rows)
    return 0


def _configured_queues(config: QueueConfig) -> list[str]:
    """Every queue name or URL in the config (model lane queues, then job queues), deduped."""
    seen: dict[str, None] = {}
    for model in config.models.values():
        for value in model.queues.values():
            seen.setdefault(value, None)
    for value in config.job_queues.values():
        seen.setdefault(value, None)
    return list(seen)


async def _queue_row(sqs: Any, queue: str) -> dict[str, Any]:
    """Counts of one queue plus its DLQ (from `RedrivePolicy`); errors become an `error` field."""
    name = queue.rstrip("/").rsplit("/", 1)[-1]
    row: dict[str, Any] = {"queue": name}
    try:
        url = queue if "://" in queue else (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
        attrs = (
            await sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=[*_QUEUE_COUNTS, "RedrivePolicy"]
            )
        )["Attributes"]
    except Exception as exc:  # one bad queue must not hide the others
        return {**row, "error": f"{type(exc).__name__}: {exc}"}
    row.update(_counts(attrs))
    policy = attrs.get("RedrivePolicy")
    if policy:
        dlq_name = str(orjson.loads(policy).get("deadLetterTargetArn", "")).rsplit(":", 1)[-1]
        dlq: dict[str, Any] = {"queue": dlq_name}
        try:
            dlq_url = (await sqs.get_queue_url(QueueName=dlq_name))["QueueUrl"]
            dlq_attrs = (
                await sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=[*_QUEUE_COUNTS])
            )["Attributes"]
            dlq.update(_counts(dlq_attrs))
        except Exception as exc:
            dlq["error"] = f"{type(exc).__name__}: {exc}"
        row["dlq"] = dlq
    return row


def _counts(attrs: Mapping[str, Any]) -> dict[str, int]:
    visible, inflight, delayed = (int(attrs.get(name, 0)) for name in _QUEUE_COUNTS)
    return {"visible": visible, "in_flight": inflight, "delayed": delayed}


async def _queues(config: QueueConfig) -> int:
    from aiobotocore.session import get_session  # SQS client only for this command

    queues = _configured_queues(config)
    async with get_session().create_client(
        "sqs", region_name=config.aws_region, endpoint_url=config.sqs_endpoint_url
    ) as sqs:
        rows = list(await asyncio.gather(*(_queue_row(sqs, q) for q in queues)))
    _print_json(rows)
    return 0


def job_message(
    name: str, payload: Any, *, lane: str, actor: str, queue: str, deadline_at: float
) -> dict[str, Any]:
    """Taskiq message for one job, the same body `LLMQueue.send_job` kiqs (task `TASK_JOB`)."""
    job_id = f"job-{uuid.uuid4().hex}"
    body = {
        "v": JOB_VERSION,
        "request_id": job_id,
        "job": name,
        "payload": payload,
        "deadline_at": deadline_at,
    }
    return {
        "task_id": job_id,
        "task_name": TASK_JOB,
        "labels": {"lane": lane, "job": name, "sender": actor, "queue_name": queue},
        "labels_types": None,
        "args": [body],
        "kwargs": {},
    }


async def _job_send(args: argparse.Namespace, config: QueueConfig) -> int:
    from aiobotocore.session import get_session  # SQS client only for this command

    payload = orjson.loads(args.payload_file.read_bytes())
    queue: str = args.queue
    queue_name = queue.rstrip("/").rsplit("/", 1)[-1]
    if not isinstance(payload, dict):
        raise ConfigError("the job payload file must hold a JSON object")
    message = job_message(
        args.name,
        payload,
        lane=args.lane,
        actor=_actor(args),
        queue=queue_name,
        deadline_at=time.time() + config.state_ttl_secs,
    )
    session = get_session()
    async with session.create_client(
        "sqs", region_name=config.aws_region, endpoint_url=config.sqs_endpoint_url
    ) as sqs:
        if queue.startswith(("http://", "https://")):
            url = queue
        else:
            url = (await sqs.get_queue_url(QueueName=queue))["QueueUrl"]
        sent = await sqs.send_message(QueueUrl=url, MessageBody=orjson.dumps(message).decode())
    _print_json({"task_id": message["task_id"], "message_id": sent["MessageId"], "queue": url})
    return 0


if __name__ == "__main__":
    console_main()
