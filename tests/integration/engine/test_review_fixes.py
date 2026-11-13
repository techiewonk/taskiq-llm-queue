"""Bugs found by the 0.3.0 review (eight parallel reviews), each pinned by a test.

Failure paths that left a call held and hidden for good, refunds that did not match the charge,
lifecycle and sync-runner races, keyed id hashes and settings that were too easy to get wrong.
No real LLM is called.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest
from engine_testkit import CHAT, Make
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ResponseError

from taskiq_llm_queue import LLMQueue, UnknownModel
from taskiq_llm_queue.clients.pool import _SESSION_UNSAFE
from taskiq_llm_queue.engine.errors import CallFailed
from taskiq_llm_queue.engine.runtime import Engine, EngineSettings
from taskiq_llm_queue.engine.settings import InfraSettings
from taskiq_llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration


async def _calm(engine: Engine, request_id: str, secs: float = 3.0) -> None:
    """Until the call is no longer held by this engine (or fail after `secs`)."""
    for _ in range(int(secs / 0.05)):
        if request_id not in engine._held:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{request_id} is still held")


async def test_admission_error_does_not_hide_the_call(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: one store blip in PICK_TAKE left the call held and its message hidden for good."""
    engine, _ = await make({"small": {"queue": {"visibility_secs": 2}}})
    real = engine.admission.pick_take
    fail = {"left": 1}

    async def blip(*args: Any, **kwargs: Any) -> Any:
        if fail["left"]:
            fail["left"] -= 1
            raise StoreConnectionError("blip")
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.admission, "pick_take", blip)
    reply = await engine.call("small", CHAT, request_id="r-blip")
    assert reply.ok
    await _calm(engine, "r-blip")


async def test_store_refusal_mid_call_releases_and_reruns(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: an OOM / MISCONF reply after the provider answered killed the run and leaked it."""
    engine, _ = await make({"small": {"queue": {"visibility_secs": 2}}})
    real = engine.requests.put_result
    fail = {"left": 1}

    async def oom(*args: Any, **kwargs: Any) -> Any:
        if fail["left"]:
            fail["left"] -= 1
            raise ResponseError("OOM command not allowed when used memory > 'maxmemory'.")
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "put_result", oom)
    reply = await engine.call("small", CHAT, request_id="r-oom")
    assert reply.ok
    await _calm(engine, "r-oom")
    assert not engine._held


async def test_refund_matches_the_charge(make: Make) -> None:
    """Bug: an account-level max_tokens made refunds larger than the charge; TPM stopped holding."""
    engine, _ = await make(
        {"small": {"tpm": 100_000, "rate_window": "fixed"}},
    )
    doc = dict(engine.registry.current.registry.raw)
    doc["accounts"]["aws-a"]["settings"] = {"inference": {"max_tokens": 4096}}
    doc["registry_version"] = int(doc["registry_version"]) + 1
    await engine.registry.publish(doc, actor="test")
    minute = engine.clock.now() // 60
    reply = await engine.call("small", CHAT)
    status = await engine.status("small")
    used = status["deployments"][0]["tpm"]["used"]
    if engine.clock.now() // 60 == minute:  # counters are per calendar minute
        assert reply.meta.tokens_in is not None
        assert used >= 0, "a refund larger than the charge drove the counter negative"
        assert used <= (reply.meta.tokens_in or 0) + (reply.meta.tokens_out or 0) + 10


async def test_missing_admission_numbers_do_not_hang(make: Make) -> None:
    """Bug: with the model's numbers gone (`no_config`) a call stayed held for good."""
    engine, _ = await make()
    await engine.state.delete(engine.registry.numbers_key("small"))
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, request_id="r-nocfg", timeout_secs=4)
    await _calm(engine, "r-nocfg", secs=8)


async def test_unknown_model_is_a_library_error(make: Make) -> None:
    engine, _ = await make()
    with pytest.raises(UnknownModel) as info:
        await engine.call("nope", CHAT)
    assert isinstance(info.value, KeyError)  # HTTP 404 and the CLI still treat it as a lookup
    assert isinstance(info.value, ConfigError)


def test_id_hash_is_keyed() -> None:
    """Bug: LLMQ_ID_HASH_SALT was required in prod but never used; hashes were plain sha256."""
    one = Engine.hash_id(_with_salt("a"), "client-123")
    two = Engine.hash_id(_with_salt("b"), "client-123")
    assert one != two
    assert len(one) == 16


def _with_salt(salt: str) -> Any:
    class Holder:
        settings = EngineSettings(id_hash_salt=salt)

    return Holder()


def test_role_session_name_is_valid_for_aws() -> None:
    """Bug: a worker id like `pod:1a2b` made every AssumeRole fail (`:` is not allowed)."""
    assert _SESSION_UNSAFE.sub("-", "llmq-prod-pod-7:1a2b c") == "llmq-prod-pod-7-1a2b-c"


@pytest.mark.parametrize(
    ("env", "problem"),
    [
        ({"LLMQ_ENVIRONMENT": "Production"}, "must use TLS"),
        ({"LLMQ_ENVIRONMENT": "uat"}, "LLMQ_ID_HASH_SALT is required"),
        (
            {
                "LLMQ_ENVIRONMENT": "prod",
                "LLMQ_STORE_URL": "rediss://c.aws:6379/0?ssl_cert_reqs=none",
            },
            "certificate",
        ),
        (
            {"LLMQ_ENVIRONMENT": "prod", "LLMQ_STORE_URL": "redis://c.aws:6379/0#@localhost"},
            "must use TLS",
        ),
        ({"LLMQ_METRICS_SINK": "prometeus"}, "LLMQ_METRICS_SINK"),
        ({"LLMQ_LEASE_SECS": "nan"}, "finite"),
        ({"LLMQ_QUEUE_STORE_URL": "http://x"}, "LLMQ_QUEUE_STORE_URL must be a redis"),
    ],
)
def test_settings_refuse_unsafe_values(env: dict[str, str], problem: str) -> None:
    base = {"LLMQ_STORE_URL": "redis://cache.example.com:6379/0", "LLMQ_ID_HASH_SALT": "s"}
    if env.get("LLMQ_ENVIRONMENT") == "uat":
        base.pop("LLMQ_ID_HASH_SALT")
        base["LLMQ_STORE_URL"] = "rediss://cache.example.com:6379/0"
    with pytest.raises(ConfigError, match=problem):
        InfraSettings.from_env({**base, **env})


def test_settings_accept_valkey_urls_and_hide_the_password() -> None:
    settings = InfraSettings.from_env({"LLMQ_STORE_URL": "valkey://:secret@localhost:6379/0"})
    assert "secret" not in repr(settings)


async def test_start_twice_and_restart(make: Make, store: Any, prefix: str) -> None:
    """Bug: a second start() doubled every loop; a start() after stop() ran no worker."""
    import json

    engine, _ = await make()
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(dict(engine.registry.current.registry.raw)),
        "LLMQ_METRICS_SINK": "none",
    }
    queue = LLMQueue.from_env(env)
    await queue.start()
    tasks = len(queue.engine._tasks)
    await queue.start()
    assert len(queue.engine._tasks) == tasks
    await queue.stop(drain_secs=0)
    await queue.stop(drain_secs=0)  # twice: nothing happens
    with pytest.raises(Exception, match="stopped"):
        await queue.start()


async def test_first_sync_calls_from_many_threads_build_one_runner(
    make: Make, store: Any, prefix: str
) -> None:
    """Bug: threads making their first sync call together each built a private engine."""
    import json

    engine, _ = await make()
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(dict(engine.registry.current.registry.raw)),
        "LLMQ_METRICS_SINK": "none",
    }
    queue = LLMQueue.from_env(env)
    runners = []

    def first() -> None:
        runners.append(queue._sync_runner())

    threads = [threading.Thread(target=first) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    try:
        assert len({id(r) for r in runners}) == 1
    finally:
        await asyncio.to_thread(queue.close_sync)
    alive = [t for t in threading.enumerate() if t.name == "llmq-sync"]
    assert not alive


async def test_store_error_after_send_waits_instead_of_running_twice(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: a store error while waiting for a sent call ran it again on the store_down path."""
    engine, fake = await make()
    real = engine.requests.take_result
    fail = {"left": 2}

    async def blip(*args: Any, **kwargs: Any) -> Any:
        if fail["left"]:
            fail["left"] -= 1
            raise StoreConnectionError("blip")
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "take_result", blip)
    reply = await engine.call("small", CHAT, request_id="r-after-send")
    assert reply.ok
    assert fail["left"] == 0
    calls = sum(stats.calls for stats in fake.stats.values())
    assert calls == 1  # one provider call, not a second one on the store_down path


async def test_cancelling_one_local_waiter_keeps_the_shared_call(make: Make) -> None:
    """Bug: two waiters in one process share a caller entry; cancelling one cancelled both.

    Also: the cancelled one removed the shared wake-up, so the other slept until the last read
    at its deadline (15 s here) instead of waking with the reply.
    """
    import time

    engine, _ = await make(scenario={"defaults": {"latency_ms": {"p50": 800, "p95": 800}}})
    started = time.monotonic()
    first = asyncio.create_task(engine.call("small", CHAT, request_id="r-shared"))
    await asyncio.sleep(0.2)
    second = asyncio.create_task(engine.call("small", CHAT, request_id="r-shared"))
    await asyncio.sleep(0.2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    reply = await second
    assert reply.ok
    assert time.monotonic() - started < 5  # woken by the reply, not by the deadline's read
    assert not engine._local_callers


async def test_worker_killed_mid_call_resumes_elsewhere(make: Make) -> None:
    """Plan B13 / B30 / B71: a worker dies mid-call (no cleanup); another finishes the call.

    The dead worker's grant lease expires, its message becomes visible again, and a second
    worker takes the call over; the caller gets one reply and nothing stays held.
    """
    models = {"small": {"queue": {"visibility_secs": 2}, "timeout_secs": 30}}
    slow = {"defaults": {"latency_ms": {"p50": 4000, "p95": 4000}}}
    dying, _ = await make(models, slow)
    caller, _ = await make(models, worker=False)
    task = asyncio.create_task(caller.call("small", CHAT, request_id="r-killed"))
    for _ in range(100):  # until the dying worker runs it
        if "r-killed" in dying._held and dying._held["r-killed"].run is not None:
            break
        await asyncio.sleep(0.05)
    assert "r-killed" in dying._held
    # Crash: every task of the worker stops at once, nothing is settled or returned.
    for job in [*dying._tasks, *dying._running]:
        job.cancel()
    await asyncio.gather(*dying._tasks, *dying._running, return_exceptions=True)
    rescuer, _ = await make(models)
    reply = await asyncio.wait_for(task, 30)
    assert reply.ok
    assert reply.meta.worker == rescuer.worker_id
    await _calm(rescuer, "r-killed")
    status = await caller.request_status("r-killed")
    assert status["state"] == "done"


async def test_deadline_cuts_do_not_open_the_breaker(make: Make) -> None:
    """Bug (found by scripts/load.py): attempts cut by the caller's own deadline counted as
    provider failures; under a rate limit five late grants opened the breaker and every
    other call got model_unavailable."""
    engine, _ = await make(
        {"small": {"breaker_threshold": 2}},
        {"defaults": {"latency_ms": {"p50": 2500, "p95": 2500}}},
    )
    for i in range(4):
        with pytest.raises(CallFailed):
            await engine.call("small", CHAT, request_id=f"r-cut-{i}", timeout_secs=1)
    dep = (await engine.status("small"))["deployments"][0]
    assert dep["breaker"] == "closed"


async def test_unreplicated_send_runs_the_call_direct(make: Make) -> None:
    """`queue_wait_replicas` (plan B23): a send no replica acknowledged is a failed send.

    The call runs direct instead of failing (837 calls failed in B23 before), once.
    """
    import dataclasses

    engine, fake = await make()
    engine.settings = dataclasses.replace(engine.settings, queue_wait_replicas=1, queue_wait_ms=50)
    reply = await engine.call("small", CHAT, request_id="r-unreplicated")
    assert reply.ok
    await asyncio.sleep(1.0)  # the unreplicated copy, if received, finds no caller
    assert sum(stats.calls for stats in fake.stats.values()) == 1
    await _calm(engine, "r-unreplicated")


async def test_call_past_max_wait_is_refused_on_arrival(make: Make) -> None:
    """Bug (load drill, 1 in 6 runs): a call that waited past `max_wait_secs` unreceived was
    granted on arrival with no time left, ran past its deadline and the caller gave up."""
    caller, _ = await make(worker=False)
    task = asyncio.create_task(
        caller.call("small", CHAT, request_id="r-late", timeout_secs=10, max_wait_secs=1)
    )
    await asyncio.sleep(1.5)  # no worker yet: it waits past max_wait in the queue
    _, fake = await make()
    started = asyncio.get_running_loop().time()
    with pytest.raises(CallFailed) as info:
        await task
    assert info.value.kind == "rate_limited"
    assert asyncio.get_running_loop().time() - started < 5
    assert sum(stats.calls for stats in fake.stats.values()) == 0


async def test_retry_backoff_never_outlives_the_deadline(make: Make) -> None:
    """Bug: a retry backoff (up to `retry_max_secs`) slept past the call's deadline; the
    caller timed out first and was told nothing."""
    engine, _ = await make(
        {"small": {"retry_base_secs": 30, "retry_max_secs": 30, "max_attempts": 5}},
        {"defaults": {"error_rate_5xx": 1.0}},
    )
    started = asyncio.get_running_loop().time()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=3)
    assert info.value.kind == "deadline_exceeded"
    assert asyncio.get_running_loop().time() - started < 4.5  # the worker's answer, in time


async def test_a_call_never_answers_after_its_deadline(make: Make) -> None:
    """Soak bug: a store block sent calls down `store_down` with a fresh full timeout.

    135 of 252,000 calls answered after their deadline. Now one deadline (timeout plus caller
    margin) caps the whole call, whatever path it takes.
    """
    import dataclasses
    import time

    engine, _ = await make()
    engine.settings = dataclasses.replace(engine.settings, caller_margin_secs=0.2)

    async def stuck(*_: Any, **__: Any) -> Any:
        await asyncio.sleep(60)

    engine._call_once = stuck
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, timeout_secs=1)
    assert info.value.kind == "deadline_exceeded"
    assert time.monotonic() - started < 1.2 + 0.5


async def test_store_down_pause_keeps_the_call_deadline(make: Make) -> None:
    """Soak bug: `pause` waited a whole new timeout from the moment the store failed."""
    import time

    from taskiq_llm_queue.engine.envelope import Envelope

    engine, _ = await make({"big": {"store_down": "pause"}})

    async def down() -> None:
        raise StoreConnectionError("down")

    engine.state.ping = down
    env = Envelope(
        request_id="p", op="converse", model="big", params=CHAT, caller="c", timeout_secs=30
    )
    loop = asyncio.get_running_loop()
    started = time.monotonic()
    deadline = loop.time() + 1.5 + engine.settings.caller_margin_secs  # 1.5 s left of 30
    with pytest.raises(CallFailed) as info:
        await engine._store_down(
            engine.registry.current, "big", env, StoreConnectionError("down"), deadline
        )
    assert info.value.kind == "queue_unavailable"
    assert time.monotonic() - started < 3.0


async def test_a_receive_error_marks_store_trouble(make: Make) -> None:
    """Soak bug: only the heartbeat marked store trouble, so a block between two beats went
    unseen and callers whose liveness keys lapsed in it were dropped as gone (`cancelled`).
    """
    engine, _ = await make()
    before = engine._store_trouble_at

    async def broken(_: str) -> bool:
        raise StoreConnectionError("down")

    engine._receive_once = broken
    for _ in range(50):
        if engine._store_trouble_at > before:
            break
        await asyncio.sleep(0.1)
    assert engine._store_trouble_at > before


async def test_a_call_held_past_its_deadline_is_let_go(make: Make) -> None:
    """One-hour soak bug: two calls stayed in progress for 50 minutes after a store block.

    The heartbeat renewed the grants of any held call for good. A call held a lease past its
    deadline is now logged, its task cancelled, and it is no longer held or renewed.
    """
    from taskiq_llm_queue.engine.envelope import Envelope
    from taskiq_llm_queue.engine.runtime import _Held

    engine, _ = await make()
    env = Envelope(
        request_id="stuck-1", op="converse", model="small", params=CHAT, caller="c", timeout_secs=1
    )
    long_ago_ms = int((engine.clock.now() - 100) * 1000)
    held = _Held(None, env, first_seen=engine.clock.now(), position_ms=long_ago_ms, tag="dstuck")
    held.grants.add(("small", "stuck-owner"))
    held.run = asyncio.create_task(asyncio.sleep(3600))  # a task that never ends on its own
    engine._held["stuck-1"] = held
    for _ in range(50):  # the heartbeat beats every lease / 3 (1 s here)
        if "stuck-1" not in engine._held:
            break
        await asyncio.sleep(0.1)
    assert "stuck-1" not in engine._held
    for _ in range(50):  # the grants go back in a task of their own
        if not held.grants:
            break
        await asyncio.sleep(0.1)
    assert not held.grants
    assert held.run.cancelled()


async def test_credentials_refused_in_one_region_leave_the_others(make: Make) -> None:
    """Scenario P5 bug: an auth error in one region marked the whole account bad, so every
    region of it stopped (an opt-in region such as ap-south-2 can refuse a token the others
    accept). Credentials are now judged per account and region.
    """
    from engine_testkit import GPT

    scenario = {"models": {f"{GPT}@r1": {"force_error": "UnrecognizedClientException"}}}
    engine, _ = await make(
        {"big": {"deployments": {"aws-a": {"confirmed": ["r1", "r2"]}}}}, scenario
    )
    for _ in range(6):
        reply = await engine.call("big", CHAT)
        assert reply.ok
        assert reply.meta.deployment == "aws-a/r2"
    assert ("aws-a", "r1") in engine._bad_accounts
    assert ("aws-a", "r2") not in engine._bad_accounts


async def test_failover_to_an_untried_region_spends_no_retry_budget(make: Make) -> None:
    """Scenario P3 bug: moving to a healthy region counted as a retry, and with one region
    failing calls ran out of retry budget (`retry_budget_exhausted`) while the other region
    was fine. A move to a deployment the call has not tried is now free.
    """
    from engine_testkit import GPT

    scenario = {"models": {f"{GPT}@r1": {"force_error": "InternalServerException"}}}
    budget = {"retry_budget_min": 1, "retry_budget_pct": 0, "breaker_threshold": 100}
    deployments = {"deployments": {"aws-a": {"confirmed": ["r1", "r2"]}}}
    engine, _ = await make({"big": {**budget, **deployments}}, scenario)
    for _ in range(8):  # every call fails on r1 first, then moves to r2
        reply = await engine.call("big", CHAT, allowed_regions=["r1", "r2"])
        assert reply.ok, reply


async def test_retries_after_every_region_failed_still_spend_the_budget(make: Make) -> None:
    """The budget still stops retry storms: once every deployment failed, a retry spends it."""
    from engine_testkit import GPT

    failing = {"force_error": "InternalServerException"}
    scenario = {"models": {f"{GPT}@r1": failing, f"{GPT}@r2": failing}}
    budget = {"retry_budget_min": 1, "retry_budget_pct": 0, "breaker_threshold": 100}
    deployments = {"deployments": {"aws-a": {"confirmed": ["r1", "r2"]}}}
    engine, _ = await make({"big": {**budget, **deployments, "retry_base_secs": 0.01}}, scenario)
    kinds = []
    for _ in range(4):
        with pytest.raises(CallFailed) as info:
            await engine.call("big", CHAT)
        kinds.append(info.value.kind)
    assert "retry_budget_exhausted" in kinds


async def test_a_store_out_of_memory_is_a_call_failure(make: Make) -> None:
    """Scenario S9 bug: a store over maxmemory refused the call's registration, and redis-py's
    `OutOfMemoryError` (its "OOM" prefix stripped, so the old string check missed it) reached
    the app instead of `CallFailed`. It is now `queue_unavailable`, out of memory.
    """
    from redis.exceptions import OutOfMemoryError

    engine, _ = await make()

    async def refuse(*_: Any, **__: Any) -> Any:
        raise OutOfMemoryError("command not allowed when used memory > 'maxmemory'.")

    engine.requests.register = refuse
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "queue_unavailable"
    assert "out of memory" in str(info.value)


async def test_admin_operations_raise_store_unavailable() -> None:
    """Scenario X2 bug: a live registry edit (and cancel / status) while the store was down
    raised redis-py's own ConnectionError to the app. Admin operations now raise
    `StoreUnavailable`, an `LLMQueueError` (and a `ConnectionError`).
    """
    from taskiq_llm_queue import LLMQueueError, StoreUnavailable
    from taskiq_llm_queue.engine.store_client import store_client
    from taskiq_llm_queue.registry.store import RegistryStore

    dead = store_client("redis://127.0.0.1:1/0")  # nothing listens on port 1
    registry = RegistryStore(dead, "llmq-dead")
    for operation in (registry.versions(), registry.log(), registry.set({"rpm": 1}, actor="t")):
        with pytest.raises(StoreUnavailable) as info:
            await operation
        assert isinstance(info.value, LLMQueueError)
        assert isinstance(info.value, ConnectionError)
    await dead.aclose()
