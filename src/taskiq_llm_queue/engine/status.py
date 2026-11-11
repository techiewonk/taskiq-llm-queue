"""Status of models, deployments and requests, read from the stores (plan §3.12).

`StatusReader.model()` reports, per model: waiting calls per class, calls in progress, and per
deployment the RPM / TPM used and left this minute, concurrency limit and use, breaker, health,
latency and throttles. `request()` reports one call. `AlertEvaluator` turns a report into state
alerts, sent as gauges every sample, with a log line once per change (plan §3.18 R32).
"""

from __future__ import annotations

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

    async def model(self, view: ResolvedRegistry, alias: str) -> dict[str, Any]:
        """One model: queues, calls in progress, and every deployment's numbers."""
        secs, _ = await self._client.time()
        now_ms = int(secs) * 1000
        minute = int(secs) // 60
        name = self.queue_prefix + view.queue_name(alias)
        waiting = {}
        dead = 0
        for cls, queue in (("priority", f"{name}-priority"), ("normal", name)):
            total, held, letters = await self.queues.depth(queue)
            waiting[cls] = max(0, total - held)
            dead += letters
        inprog = await self._client.zcount(self.keys.model(alias, "inprog"), now_ms, "+inf")
        health = {
            _text(k): _text(v)
            for k, v in (await self._client.hgetall(self.keys.model(alias, "health"))).items()
        }
        deployments = []
        for dep in view.deployments(alias):
            deployments.append(
                await self._deployment(view, alias, dep.key, dep.group, health, minute, now_ms)
            )
        return {
            "model": alias,
            "enabled": bool(view.effective(alias, "enabled").value),
            "waiting": waiting,
            "in_progress": int(inprog),
            "dead_letters": dead,
            "deployments": deployments,
        }

    async def _deployment(
        self,
        view: ResolvedRegistry,
        alias: str,
        dep: str,
        group: str,
        health: Mapping[str, str],
        minute: int,
        now_ms: int,
    ) -> dict[str, Any]:
        rate = {
            _text(k): _text(v)
            for k, v in (await self._client.hgetall(self.keys.rate(alias, dep))).items()
        }
        fresh = rate.get("seen_win") == str(minute)
        used_calls = int(_num(rate.get("seen_calls"))) if fresh else 0
        used_tokens = int(_num(rate.get("seen_tokens"))) if fresh else 0
        rpm = view.value(alias, "rpm", dep)
        tpm = view.value(alias, "tpm", dep)
        cap = view.value(alias, "max_concurrency", dep) or 200
        limit = _num(await self._client.hget(self.keys.limiter(alias, dep), "limit"), cap)
        in_use = 0
        for cls in ("p", "n"):
            in_use += int(
                await self._client.zcount(
                    self.keys.model(alias, f"lease:{dep}:{cls}"), now_ms, "+inf"
                )
            )
        breaker = {
            _text(k): _text(v)
            for k, v in (await self._client.hgetall(self.keys.breaker(alias, dep))).items()
        }
        throttles = await self._client.get(self.keys.model(alias, f"thr:{dep}:{minute}"))
        disabled_until = health.get(f"{dep}|until")
        deployment = next(d for d in view.deployments(alias) if d.key == dep)
        return {
            "name": dep,
            "group": group,
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
            "concurrency": {"limit": round(limit, 2), "in_use": in_use},
            "breaker": breaker.get("state", "closed"),
            "latency_ms": round(_num(health.get(f"{dep}|lat")), 1)
            if f"{dep}|lat" in health
            else None,
            "samples": int(_num(health.get(f"{dep}|n"))),
            "throttles_1m": int(_num(throttles)),
        }

    async def report(self, view: ResolvedRegistry) -> dict[str, Any]:
        """Every model, the registry version and the routing switches."""
        models = [await self.model(view, alias) for alias in view.registry.models]
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
