"""Status of models, deployments and requests, read from the stores (plan §3.12).

`StatusReader.model()` reports, per model: waiting calls per class, calls in progress, and per
deployment the RPM / TPM used and left this minute, concurrency limit and use, breaker, health,
latency and throttles. `request()` reports one call. `AlertEvaluator` turns a report into state
alerts, sent as gauges every sample, with a log line once per change (plan §3.18 R32).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from taskiq_llm_queue._logging import get_logger
from taskiq_llm_queue.engine.keys import Keys
from taskiq_llm_queue.engine.queue import StreamQueues

if TYPE_CHECKING:
    import redis.asyncio

    from taskiq_llm_queue.registry import ResolvedRegistry

logger = get_logger(__name__)

# One model's numbers in one command: TIME, calls in progress, health, and per deployment its
# rate hash, limiter, slots in use, breaker state and throttles this minute. As ~180 separate
# commands per model, client-side packing and parsing made a 600-deployment report 140 ms+.
# KEYS: none. ARGV: model key prefix (`<p>:{m:<alias>}:`), deployment names...
_MODEL_STATUS = """
local base = ARGV[1]
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000
local minute = math.floor(tonumber(t[1]) / 60)
local out = {t[1], redis.call('ZCOUNT', base .. 'inprog', now_ms, '+inf'),
             redis.call('HGETALL', base .. 'health')}
for i = 2, #ARGV do
  local dep = ARGV[i]
  table.insert(out, {
    redis.call('HGETALL', base .. 'rate:' .. dep),
    redis.call('HGET', base .. 'ac:' .. dep, 'limit') or '',
    redis.call('ZCOUNT', base .. 'lease:' .. dep .. ':p', now_ms, '+inf')
      + redis.call('ZCOUNT', base .. 'lease:' .. dep .. ':n', now_ms, '+inf'),
    redis.call('HGET', base .. 'cb:' .. dep, 'state') or 'closed',
    redis.call('GET', base .. 'thr:' .. dep .. ':' .. minute) or '0',
  })
end
return out
"""


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(_text(value))
    except (TypeError, ValueError):
        return default


class StatusReader:
    """Read-only view of one environment's stores."""

    def __init__(
        self, client: redis.asyncio.Redis, keys: Keys, queues: StreamQueues, queue_prefix: str = ""
    ) -> None:
        """Keep the clients; every report reads the store fresh."""
        self._client = client
        self.keys = keys
        self.queues = queues
        self.queue_prefix = queue_prefix
        self._model_status = client.register_script(_MODEL_STATUS)

    async def model(self, view: ResolvedRegistry, alias: str) -> dict[str, Any]:
        """One model: queues, calls in progress, and every deployment's numbers.

        All deployment reads are one script (one command per model): read one by one, 20
        models x 30 deployments took 6.5 s (plan B91 asks for 200 ms; now 47 ms).
        """
        name = self.queue_prefix + view.queue_name(alias)
        deps = view.deployments(alias)
        base = self.keys.model(alias, "")
        depths, raw = await asyncio.gather(
            asyncio.gather(self.queues.depth(f"{name}-priority"), self.queues.depth(name)),
            self._model_status([], [base, *(d.key for d in deps)]),
        )
        waiting = {
            cls: max(0, total - held)
            for cls, (total, held, _) in zip(("priority", "normal"), depths, strict=True)
        }
        dead = sum(letters for _, _, letters in depths)
        minute = int(raw[0]) // 60
        inprog, flat = raw[1], raw[2]
        health = {_text(flat[i]): _text(flat[i + 1]) for i in range(0, len(flat), 2)}
        deployments = [
            self._deployment(view, alias, dep, raw[3 + i], health, minute)
            for i, dep in enumerate(deps)
        ]
        return {
            "model": alias,
            "enabled": bool(view.effective(alias, "enabled").value),
            "waiting": waiting,
            "in_progress": int(inprog),
            "dead_letters": dead,
            "deployments": deployments,
        }

    def _deployment(
        self,
        view: ResolvedRegistry,
        alias: str,
        deployment: Any,
        replies: list[Any],
        health: Mapping[str, str],
        minute: int,
    ) -> dict[str, Any]:
        flat_rate, raw_limit, in_use, breaker, throttles = replies
        dep = deployment.key
        rate = {_text(flat_rate[i]): _text(flat_rate[i + 1]) for i in range(0, len(flat_rate), 2)}
        fresh = rate.get("seen_win") == str(minute)
        used_calls = int(_num(rate.get("seen_calls"))) if fresh else 0
        used_tokens = int(_num(rate.get("seen_tokens"))) if fresh else 0
        rpm = view.value(alias, "rpm", dep)
        tpm = view.value(alias, "tpm", dep)
        cap = view.value(alias, "max_concurrency", dep) or 200
        limit = _num(raw_limit, cap) if _text(raw_limit) else cap
        disabled_until = health.get(f"{dep}|until")
        return {
            "name": dep,
            "group": deployment.group,
            "enabled": bool(view.value(alias, "enabled", dep)),
            "healthy": disabled_until is None,
            "next_probe": _num(disabled_until) / 1000 if disabled_until else None,
            "outside_residency": view.outside_residency(deployment),
            "rpm": {
                "limit": rpm,
                "used": used_calls,
                "left": None if rpm is None else max(0, rpm - used_calls),
            },
            "tpm": {
                "limit": tpm,
                "used": used_tokens,
                "left": None if tpm is None else max(0, tpm - used_tokens),
            },
            "concurrency": {"limit": round(limit, 2), "in_use": int(in_use)},
            "breaker": _text(breaker),
            "latency_ms": round(_num(health.get(f"{dep}|lat")), 1)
            if f"{dep}|lat" in health
            else None,
            "samples": int(_num(health.get(f"{dep}|n"))),
            "throttles_1m": int(_num(throttles)),
        }

    async def report(self, view: ResolvedRegistry) -> dict[str, Any]:
        """Every model, the registry version and the routing switches."""
        models = list(await asyncio.gather(*(self.model(view, a) for a in view.registry.models)))
        routing = view.registry.routing
        return {
            "registry_version": view.registry.version,
            "kill_switch": routing.kill_switch,
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
            "models": models,
        }

    async def request(self, request_id: str) -> dict[str, Any]:
        """One call: its status, and its in-progress entry while it runs."""
        status = {
            _text(k): _text(v)
            for k, v in (await self._client.hgetall(self.keys.status(request_id))).items()
        }
        if not status:
            return {}
        model = status.get("served_model") or status.get("model")
        if model and status.get("state") == "waiting":
            entry = await self._client.hgetall(self.keys.model(model, f"inprog:{request_id}"))
            if entry:
                fields = {_text(k): _text(v) for k, v in entry.items()}
                status["state"] = "in_progress"
                status["deployment"] = fields.get("dep", "")
        return {"request_id": request_id, **status}


@dataclass(slots=True)
class Alert:
    """One alert state: on or off, with its details."""

    name: str
    on: bool
    labels: dict[str, str] = field(default_factory=dict)
    detail: str = ""


class AlertEvaluator:
    """State alerts from a report (thresholds from the model's `alerts` setting).

    Gauges are returned every time; `changed()` logs a line only when a state flips, with the
    store marker deciding which process logs it.
    """

    def __init__(self, client: redis.asyncio.Redis, keys: Keys) -> None:
        """Keep the client for the change markers."""
        self._client = client
        self.keys = keys

    def evaluate(self, view: ResolvedRegistry, report: Mapping[str, Any]) -> list[Alert]:
        """Every alert state of a report."""
        alerts: list[Alert] = []
        for model in report["models"]:
            alias = model["model"]
            thresholds = view.value(alias, "alerts")
            deps = model["deployments"]
            usable = [
                d for d in deps if d["enabled"] and d["healthy"] and not d["outside_residency"]
            ]
            alerts.append(Alert("model_unavailable", not usable, {"model": alias}))
            for dep in deps:
                labels = {"model": alias, "deployment": dep["name"]}
                alerts.append(Alert("deployment_disabled", not dep["healthy"], labels))
                limit = dep["rpm"]["limit"]
                used_pct = 100 * dep["rpm"]["used"] / limit if limit else 0
                alerts.append(
                    Alert(
                        "quota_near_limit",
                        used_pct >= thresholds["quota_used_pct"],
                        labels,
                        f"{used_pct:.0f}%",
                    )
                )
                quota_left = limit is None or dep["rpm"]["left"] > 0
                alerts.append(
                    Alert(
                        "quota_above_observed",
                        dep["throttles_1m"] > 0 and quota_left,
                        labels,
                        f"{dep['throttles_1m']} throttles with budget left",
                    )
                )
            alerts.append(
                Alert(
                    "dead_letters",
                    model["dead_letters"] >= thresholds["dlq_messages"],
                    {"model": alias},
                )
            )
        return alerts

    async def changed(self, alerts: list[Alert]) -> list[Alert]:
        """Alerts whose state flipped since the last evaluation (logged once, by one process)."""
        flipped = []
        for alert in alerts:
            marker = f"{self.keys.prefix}:alert:{alert.name}:" + ",".join(
                f"{k}={v}" for k, v in sorted(alert.labels.items())
            )
            state = "1" if alert.on else "0"
            previous = await self._client.getset(marker, state)
            await self._client.expire(marker, 86_400)
            if (_text(previous) if previous is not None else "0") != state:
                flipped.append(alert)
                log = logger.error if alert.on else logger.info
                log(
                    "llmq_alert", alert=alert.name, on=alert.on, detail=alert.detail, **alert.labels
                )
        return flipped
