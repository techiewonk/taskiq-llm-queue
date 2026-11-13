"""The `llm-queue` command line for 0.3 (plan §3.12, §3.18): registry, status, operations.

Every command reads `LLMQ_*` from the environment (`InfraSettings.from_env`) and talks to the
stores directly; writes go through the registry store (validated, protected fields, audited
with `--actor`, default the OS user). Output is JSON, one document per command.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any

import orjson
import redis.asyncio

from taskiq_llm_queue.engine.http import redacted
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.queue import StreamQueues
from taskiq_llm_queue.engine.settings import InfraSettings
from taskiq_llm_queue.engine.status import StatusReader
from taskiq_llm_queue.engine.store_client import store_client
from taskiq_llm_queue.errors import ConfigError, LLMQueueError
from taskiq_llm_queue.registry.store import RegistryStore


def _value(text: str) -> Any:
    """A command-line value: JSON when it parses (`400`, `true`, `["a"]`), else the text."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="llm-queue", description=__doc__.splitlines()[0])
    parser.add_argument("--actor", default=None, help="who makes a change (audit)")
    sub = parser.add_subparsers(dest="command", required=True)

    reg = sub.add_parser("registry", help="read and change the registry").add_subparsers(
        dest="action", required=True
    )
    get = reg.add_parser("get")
    get.add_argument("--model")
    setp = reg.add_parser("set", help="live edit: path=value ... (JSON values)")
    setp.add_argument("--model")
    setp.add_argument("pairs", nargs="+")
    for name in ("enable", "disable"):
        p = reg.add_parser(name)
        p.add_argument("model")
        p.add_argument("deployment")
    reg.add_parser("diff", help="what is live but not in this config")
    log = reg.add_parser("log")
    log.add_argument("--count", type=int, default=20)
    log.add_argument("--path", default="")
    pub = reg.add_parser("publish", help="publish LLMQ_REGISTRY, or roll back")
    pub.add_argument("--from-version", type=int, dest="from_version")
    reg.add_parser("gc", help="delete keys of models that no longer exist")

    settings = sub.add_parser("settings").add_subparsers(dest="action", required=True)
    sget = settings.add_parser("get")
    sget.add_argument("--model", required=True)
    sget.add_argument("--deployment")

    status = sub.add_parser("status")
    status.add_argument("--model")
    request = sub.add_parser("request")
    request.add_argument("request_id")

    for name in ("block", "unblock"):
        p = sub.add_parser(name)
        p.add_argument("--region")
        p.add_argument("--account")

    drain = sub.add_parser("drain", help="disable a model for new calls; serve waiting ones")
    drain.add_argument("model")
    drain.add_argument("--wait", action="store_true")
    drain.add_argument("--timeout", type=float, default=600.0)

    dlq = sub.add_parser("dlq").add_subparsers(dest="action", required=True)
    for name in ("list", "show", "redrive", "purge"):
        p = dlq.add_parser(name)
        p.add_argument("--model", required=True)
        p.add_argument("--priority", action="store_true")
        p.add_argument("--count", type=int, default=10)
        if name == "show":
            p.add_argument("--unsafe", action="store_true", help="print message bodies")

    queues = sub.add_parser("queues").add_subparsers(dest="action", required=True)
    queues.add_parser("list")
    return parser


class Cli:
    """The command implementations over one environment's stores."""

    def __init__(
        self,
        settings: InfraSettings,
        client: redis.asyncio.Redis,
        actor: str,
        queue_client: redis.asyncio.Redis | None = None,
    ) -> None:
        """Wire the stores: queues live on `queue_client` when `LLMQ_QUEUE_STORE_URL` is set."""
        self.settings = settings
        self.client = client
        self.actor = actor
        self.keys = Keys(settings.key_prefix)
        config = None
        if settings.registry or settings.registry_file:
            config = orjson.loads(settings.registry_document())
        self.registry = RegistryStore(
            client,
            settings.key_prefix,
            config=config,
            live_edit=settings.registry_live_edit,
            allowed_hosts=settings.base_url_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        self.queues = StreamQueues(queue_client or client, self.keys)

    async def loaded(self) -> Any:
        """The stored registry; `ConfigError` when none is published."""
        view = await self.registry.load()
        if view is None:
            raise ConfigError("no registry is published yet (llm-queue registry publish)")
        return view

    def _queue(self, view: Any, model: str, priority: bool) -> str:
        alias = view.registry.alias_of(model)
        name = self.settings.queue_prefix + view.queue_name(alias)
        return f"{name}-priority" if priority else name

    async def run(self, args: argparse.Namespace) -> Any:
        """Run one parsed command; returns what to print."""
        handler = getattr(self, f"_{args.command}")
        return await handler(args)

    async def _registry(self, args: argparse.Namespace) -> Any:
        action = args.action
        if action == "publish":
            if args.from_version is not None:
                result = await self.registry.rollback(args.from_version, actor=self.actor)
            else:
                if self.registry.config is None:
                    raise ConfigError("LLMQ_REGISTRY (or LLMQ_REGISTRY_FILE) is not set")
                result = await self.registry.publish(self.registry.config, actor=self.actor)
            return {"action": result.action, "version": result.version}
        if action == "log":
            return await self.registry.log(args.count, args.path)
        if action == "diff":
            return redacted(await self.registry.diff_with_config())
        view = await self.loaded()
        if action == "get":
            doc = dict(view.registry.raw)
            if args.model:
                doc = doc["models"][view.registry.alias_of(args.model)]
            return redacted(doc)
        if action == "set":
            values = {}
            for pair in args.pairs:
                path, sep, text = pair.partition("=")
                if not sep:
                    raise ConfigError(f"expected path=value, got {pair!r}")
                values[path] = _value(text)
            changed = await self.registry.set(values, actor=self.actor, model=args.model)
            return {"changes": self.registry.changes, "version": changed.registry.version}
        if action in ("enable", "disable"):
            await self.registry.set_enabled(
                args.model, args.deployment, action == "enable", actor=self.actor
            )
            return {"model": args.model, "deployment": args.deployment, action: True}
        if action == "gc":
            return await self._gc(view)
        raise ConfigError(f"unknown registry action {action}")

    async def _gc(self, view: Any) -> dict[str, Any]:
        """Delete keys of models that are not in the registry any more."""
        known = set(view.registry.models)
        removed = 0
        async for key in self.client.scan_iter(match=f"{self.keys.prefix}:{{m:*", count=500):
            text = key.decode() if isinstance(key, bytes) else str(key)
            alias = text.split("{m:", 1)[1].split("}", 1)[0]
            if alias not in known:
                await self.client.delete(key)
                removed += 1
        return {"deleted_keys": removed}

    async def _settings(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        table = view.table(args.model, args.deployment)
        return {name: {"value": e.value, "level": e.level} for name, e in table.items()}

    async def _status(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        reader = StatusReader(self.client, self.keys, self.queues, self.settings.queue_prefix)
        if args.model:
            return await reader.model(view, view.registry.alias_of(args.model))
        return await reader.report(view)

    async def _request(self, args: argparse.Namespace) -> Any:
        reader = StatusReader(self.client, self.keys, self.queues, self.settings.queue_prefix)
        return await reader.request(args.request_id) or {"error": "unknown or expired request"}

    async def _block(self, args: argparse.Namespace, on: bool = True) -> Any:
        if not args.region and not args.account:
            raise ConfigError("give --region or --account")
        await self.loaded()
        view = await self.registry.block(
            region=args.region, account=args.account, on=on, actor=self.actor
        )
        routing = view.registry.routing
        return {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }

    async def _unblock(self, args: argparse.Namespace) -> Any:
        return await self._block(args, on=False)

    async def _drain(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        alias = view.registry.alias_of(args.model)
        await self.registry.set({"enabled": False}, model=alias, actor=self.actor)
        if not args.wait:
            return {"model": alias, "enabled": False}
        loop = asyncio.get_running_loop()
        until = loop.time() + args.timeout
        while loop.time() < until:
            depth = [await self.queues.depth(self._queue(view, alias, p)) for p in (True, False)]
            if all(total == 0 for total, _, _ in depth):
                return {"model": alias, "enabled": False, "drained": True}
            await asyncio.sleep(1)
        return {"model": alias, "enabled": False, "drained": False}

    async def _dlq(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        queue = self._queue(view, args.model, args.priority)
        if args.action == "list":
            letters = await self.queues.dead_letters(queue, args.count)
            return [{k: v for k, v in letter.items() if k != "body"} for letter in letters]
        if args.action == "show":
            letters = await self.queues.dead_letters(queue, args.count)
            if not args.unsafe:
                return [{k: v for k, v in letter.items() if k != "body"} for letter in letters]
            return [
                {**letter, "body": letter["body"].decode(errors="replace")} for letter in letters
            ]
        if args.action == "redrive":
            return {"redriven": await self.queues.redrive(queue, args.count)}
        await self.queues.purge_dead(queue)
        return {"purged": queue}

    async def _queues(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        out = {}
        for alias in view.registry.models:
            for priority in (False, True):
                name = self._queue(view, alias, priority)
                total, held, dead = await self.queues.depth(name)
                out[name] = {"messages": total, "held": held, "dead": dead}
        return out


async def amain(argv: Sequence[str], environ: Mapping[str, str] | None = None) -> Any:
    """Parse and run one command; returns its JSON-able result."""
    args = _parser().parse_args(list(argv))
    settings = InfraSettings.from_env(environ)
    client = store_client(settings.store_url)
    queue_client = store_client(settings.queue_store_url) if settings.queue_store_url else None
    try:
        cli = Cli(settings, client, args.actor or getpass.getuser(), queue_client)
        return await cli.run(args)
    finally:
        await client.aclose()
        if queue_client is not None:
            await queue_client.aclose()


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    """Console entry point: print the result as JSON; exit 1 with the message on errors."""
    try:
        result = asyncio.run(amain(sys.argv[1:] if argv is None else argv, environ))
    except (LLMQueueError, KeyError) as exc:  # a bad name or edit: one line, not a traceback
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0
