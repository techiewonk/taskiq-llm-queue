"""The research-assistant app's LLM and embedding flows, replayed through the queue.

Each flow copies one call site of the app (`mofsl-research-assistant-repo/src`): the same
model, message shape, LangChain wrapper and lane, with tiny prompts. Every flow prints the
request (no secrets), the reply, how the queue served it, the queue wait and the latency.

Flows (`--flow NAME`; app call site under `src/`, model, lane):

- `agent_graph`: `core/agents/runnable.py:45` + `base_agent.py:165` in the LangGraph of
  `graph_builder.py`; gpt20b, prompt | trim_messages | `bind_tools`, `astream(["custom"])`; rt
- `human_condense`: `core/orchestrator/human_node.py:54`; gpt20b, `ainvoke`; bg
- `guardrail`: `core/guardrail/light_guardrails.py:447`; gpt120b, JSON verdict; rt
- `response_fix`: `core/orchestrator/response_generator_node.py:326`; gpt120b; rt
- `followup_writer`: `followup/writer.py:125`; gpt120b, system + user; rt
- `offline_converse`: `followup/serving/query_generator.py:534`, `followup/extraction/`;
  gpt120b, raw boto3 `converse(**kwargs)`; bulk
- `followup_embed`: `followup/embedder.py:61`; titan `aembed_query`; rt
- `query_embed`: `utils/memory/retrieval.py:613`, `utils/helpers/light_utils.py:579`,
  `core/query_planning/retrieve_query_plan.py:61`; titan + cohere; rt
- `safe_embedding`: `client/bedrock_client.py:103` (`safe_get_embedding`); titan
  `invoke_model`; rt
- `memory_write`: `utils/memory/storage.py:77` (daemon thread); titan, sync; bg
- `nudge_job`: `api/nudge/nudge_service.py:75`; titan, as a job; bg
- `ingest_job`: `core/query_planning/ingest_query_plans.py:114`, `weaviate_ingest/*`;
  cohere + titan, as a job; bulk

Targets:

- `--target local` (default): the Docker stack, like samples 01-11 (fakellm as Bedrock).
- `--target aws`: real Bedrock with the AWS credentials of the environment / `.env` /
  profile; the queue still runs on the local Valkey + ElasticMQ unless `LLMQ_*` says
  otherwise (`LLMQ_SAMPLES_APP_VALKEY=true` uses the app's `VALKEY_*` instead).

Settings (`--env-source`, default `auto`): the real environment always wins. `dotenv` reads
`LLMQ_ENV_FILE` or `<package root>/.env`; `secrets` reads the app's Secrets Manager secret
`SECRET_NAME_TEMPLATE.format(env=ENV)` (default template `{env}/researchassistant/secret`,
region `AWS_REGION`), which, like the app, wins over `.env`. `auto` = `.env`, plus the secret
on `--target aws` when `ENV` or `SECRET_NAME_TEMPLATE` is set. Values are never printed.

Run:

    .venv/Scripts/python samples/13_research_assistant_flow.py                 # all, local
    .venv/Scripts/python samples/13_research_assistant_flow.py --flow guardrail
    .venv/Scripts/python samples/13_research_assistant_flow.py --target aws --env-source dotenv
"""

from __future__ import annotations

import argparse
import asyncio
import contextvars
import json
import os
import sys
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Annotated, Any, TypedDict

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    BaseMessage,
    HumanMessage,
    trim_messages,
)
from langchain_core.messages.utils import count_tokens_approximately
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

from taskiq_llm_queue import CallMeta, LLMQueue, QueueConfig, request_context
from taskiq_llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

REAL_ENV = frozenset(os.environ)  # captured before `.env` / secrets touch os.environ

# App model ids (src/configs/llm_config.py, src/constants/llm.py, retrieve_query_plan.py).
GPT120B, GPT20B = "openai.gpt-oss-120b-1:0", "openai.gpt-oss-20b-1:0"
TITAN, COHERE = "amazon.titan-embed-text-v2:0", "cohere.embed-english-v3"
# Small replies keep AWS runs cheap; gpt-oss spends part of it on reasoning.
MAX_TOKENS = 512
JOB_QUEUES = {"ingest": "jobs-ingest", "nudge": "jobs-nudge"}
# Errors that mean "no usable AWS credentials / access": stop instead of trying more flows.
AUTH_ERRORS = (
    "NoCredentialsError",
    "Unable to locate credentials",
    "ExpiredToken",
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "AccessDenied",
    "security token",
)


# ---------------------------------------------------------------- reporting


@dataclass
class Ctx:
    """What every flow needs: the queue, the target and the app's service tier."""

    llmq: LLMQueue
    target: str
    service_tier: str | None
    jobs_done: dict[str, asyncio.Future[str]] = field(default_factory=dict)


def text_of(content: Any) -> str:
    """Flatten LangChain content like the app's `response_text` (followup/writer.py)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(b.get("text", "") for b in content if isinstance(b, dict))
    return str(content or "")


def short(text: str, n: int = 70) -> str:
    """One line, at most `n` characters."""
    flat = " ".join(text.split())
    return flat if len(flat) <= n else flat[: n - 3] + "..."


def line(label: str, value: str) -> None:
    """Print one indented `label: value` line."""
    print(f"  {label:9s}: {value}")


def served(meta: CallMeta | dict[str, Any]) -> None:
    """Print how the queue served a call (model, target, fallback, waits)."""
    m = meta if isinstance(meta, CallMeta) else CallMeta.model_validate(meta)
    line(
        "served",
        f"{m.served_model}@{m.served_target} fallback_used={m.fallback_used} "
        f"attempts={m.attempts} queue_wait_ms={m.queue_wait_ms} provider_ms={m.provider_ms} "
        f"worker={m.worker}",
    )


def usage(message: BaseMessage | None = None, raw: dict[str, Any] | None = None) -> None:
    """Print token usage from a LangChain message or a raw Converse `usage` dict."""
    if raw is not None:
        line("usage", f"in={raw.get('inputTokens')} out={raw.get('outputTokens')}")
    elif isinstance(message, AIMessage) and message.usage_metadata:
        u = message.usage_metadata
        line("usage", f"in={u['input_tokens']} out={u['output_tokens']}")


def chat_model(ctx: Ctx, model_id: str, alias: str, max_tokens: int = MAX_TOKENS) -> Any:
    """`get_llm_model(...)` with the queue: same kwargs, plus the app's service tier."""
    return QueuedChatBedrockConverse(
        llmq=ctx.llmq,
        model_id=model_id,
        model_alias=alias,
        max_tokens=max_tokens,
        temperature=0.0,
        service_tier=ctx.service_tier,
        fallback_policy="chain",  # INTEGRATION_RESEARCH_ASSISTANT.md §3
    )


def report_ai(reply: BaseMessage) -> None:
    """Print the reply text/tool calls, serving metadata and usage of one chat call."""
    calls = reply.tool_calls if isinstance(reply, AIMessage) else []
    line("response", f"tool_calls={calls}" if calls else repr(short(text_of(reply.content))))
    served(reply.response_metadata)
    usage(reply)


# ---------------------------------------------------------------- chat flows


@tool
def get_stock_snapshot(symbol: str) -> str:
    """Return price and valuation for an NSE stock symbol."""
    return json.dumps({"symbol": symbol, "price": 1520.5, "pe": 24.1})


class AgentState(TypedDict):
    """Cut-down `LightStateGraphSchema`: the conversation only."""

    messages: Annotated[list[AnyMessage], add_messages]


async def flow_agent_graph(ctx: Ctx) -> None:
    """`lightAgent_runnable` inside a LangGraph run with `astream(stream_mode=["custom"])`."""
    trimmer = trim_messages(
        strategy="last",
        token_counter=count_tokens_approximately,
        max_tokens=100_000,  # MAX_TOKENS_OSS_20B
        start_on="human",
        include_system=True,
    )
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a stock research assistant. Use tools for data."),
            ("placeholder", "{messages}"),
        ]
    )
    runnable = prompt | trimmer | chat_model(ctx, GPT20B, "gpt20b").bind_tools([get_stock_snapshot])

    async def agent(state: AgentState) -> dict[str, list[BaseMessage]]:
        get_stream_writer()({"node": "agent", "status": "thinking"})  # app nodes emit these
        return {"messages": [await runnable.ainvoke({"messages": state["messages"]})]}

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode([get_stock_snapshot]))
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")
    graph.add_edge("agent", END)
    app = graph.compile()

    line("request", "system + human 'P/E of TCS?', tools=[get_stock_snapshot], gpt20b")
    events, messages = 0, list[BaseMessage]()
    # The app runs the graph inside its WS producer task; the lane is set there (D85).
    # It streams ["custom"]; "values" is added here only to read the final state.
    with request_context(lane="rt", session_id="ra-s1", user_id="ra-u1"):
        async for mode, chunk in app.astream(
            {"messages": [HumanMessage("What is the P/E of TCS?")]},
            stream_mode=["custom", "values"],
        ):
            if mode == "custom":
                events += 1
            else:
                messages = chunk["messages"]  # type: ignore[index,assignment]
    line("graph", f"{events} custom events; final state has {len(messages)} messages")
    for message in messages:
        if isinstance(message, AIMessage):
            report_ai(message)


async def flow_human_condense(ctx: Ctx) -> None:
    """`_store_interrupt_memory`: condense an askback answer into one query (background)."""
    prompt = (
        "An AI assistant asked the user: 'Which company?'\nThe user replied: 'Infosys'\n\n"
        "Write a single concise sentence that captures the user's complete intent. "
        "Output only the sentence."
    )
    line("request", f"1 HumanMessage ({len(prompt)} chars), gpt20b")
    with request_context(lane="bg", session_id="ra-s1", user_id="ra-u1"):
        reply = await chat_model(ctx, GPT20B, "gpt20b").ainvoke([HumanMessage(content=prompt)])
    report_ai(reply)


async def flow_guardrail(ctx: Ctx) -> None:
    """`check_light_grounding`: one user prompt, the model answers with a JSON verdict."""
    prompt = (
        "Query: P/E of TCS?\nChunks: TCS P/E is 24.1.\nResponse: TCS trades at 24.1x.\n"
        'Reply only JSON: {"is_grounded": true|false, "issues": []}'
    )
    line("request", f"1 user message ({len(prompt)} chars), gpt120b, fallback=chain")
    with request_context(lane="rt", session_id="ra-s1", user_id="ra-u1"):
        reply = await chat_model(ctx, GPT120B, "gpt120b").ainvoke(
            [{"role": "user", "content": prompt}]
        )
    report_ai(reply)
    try:
        verdict = json.loads(text_of(reply.content).strip().strip("`").removeprefix("json"))
        line("parsed", f"is_grounded={verdict.get('is_grounded')}")
    except (json.JSONDecodeError, AttributeError):
        line("parsed", "not JSON (fakellm replies with plain text)")


async def flow_response_fix(ctx: Ctx) -> None:
    """`fix_response_with_llm`: rewrite a failed answer; the app retries at most twice."""
    prompt = "Fix this answer so it only states grounded facts: 'TCS P/E is 40.' Source: P/E 24.1."
    line("request", f"1 user message ({len(prompt)} chars), gpt120b, <=2 tries")
    model = chat_model(ctx, GPT120B, "gpt120b")
    with request_context(lane="rt", session_id="ra-s1", user_id="ra-u1"):
        attempt = 0
        while attempt < 2:
            attempt += 1
            reply = await model.ainvoke([{"role": "user", "content": prompt}])
            if text_of(reply.content).strip():
                break
    line("tries", str(attempt))
    report_ai(reply)


async def flow_followup_writer(ctx: Ctx) -> None:
    """`FollowUpWriter.write`: system + user prompt, one call per chat turn."""
    system = "Write one follow-up question per candidate, one per line."
    user = "Original: P/E of TCS?\nCandidates: 1) peers 2) 5y trend"
    line("request", f"system ({len(system)} chars) + user ({len(user)} chars), gpt120b")
    with request_context(lane="rt", session_id="ra-s1", user_id="ra-u1"):
        reply = await chat_model(ctx, GPT120B, "gpt120b").ainvoke(
            [{"role": "system", "content": system}, {"role": "user", "content": user}]
        )
    report_ai(reply)


async def flow_offline_converse(ctx: Ctx) -> None:
    """`BedrockQueryGenerator` / extraction: raw boto3 `converse(**kwargs)` -> `llmq.converse`."""
    kwargs: dict[str, Any] = {  # the app's build_converse_kwargs shape, minus modelId
        "system": [{"text": "You phrase follow-up questions."}],
        "messages": [{"role": "user", "content": [{"text": "Phrase: peers of TCS"}]}],
        "inferenceConfig": {"maxTokens": MAX_TOKENS, "temperature": 0.0},
    }
    if ctx.service_tier:
        kwargs["serviceTier"] = {"type": ctx.service_tier}
    line("request", "raw Converse kwargs: 1 system block + 1 user block, gpt120b")
    result = await ctx.llmq.converse("gpt120b", kwargs, lane="bulk")
    blocks = result.response["output"]["message"]["content"]
    text = "".join(b.get("text", "") for b in blocks)
    line("response", f"{short(text)!r} stopReason={result.response.get('stopReason')}")
    served(result.meta)
    usage(raw=result.response.get("usage", {}))


# ---------------------------------------------------------------- embedding flows


async def flow_followup_embed(ctx: Ctx) -> None:
    """`RAEmbedder.aembed`: today `to_thread(embed_query)`, here `aembed_query` on `rt`."""
    emb = QueuedBedrockEmbeddings(llmq=ctx.llmq, model_id=TITAN, model_alias="titan")
    line("request", "titan aembed_query('peers of TCS')")
    with request_context(lane="rt"):
        vector = await emb.aembed_query("peers of TCS")
    line("response", f"1 vector x {len(vector)} dims")
    line("served", "n/a: embeddings return vectors only (no CallMeta)")


async def flow_query_embed(ctx: Ctx) -> None:
    """Retrieval query embeddings: Titan (memory, light_utils) and Cohere (query plans)."""
    titan = QueuedBedrockEmbeddings(llmq=ctx.llmq, model_id=TITAN, model_alias="titan")
    cohere = QueuedBedrockEmbeddings(llmq=ctx.llmq, model_id=COHERE, model_alias="cohere")
    line("request", "titan + cohere aembed_query('TCS valuation')")
    with request_context(lane="rt"):
        a, b = await asyncio.gather(
            titan.aembed_query("TCS valuation"), cohere.aembed_query("TCS valuation")
        )
    line("response", f"titan {len(a)} dims, cohere {len(b)} dims")


async def flow_safe_embedding(ctx: Ctx) -> None:
    """`safe_get_embedding`: raw `invoke_model` body; any error -> zero vector, like the app."""
    body = {"inputText": "TCS valuation", "dimensions": 1024}
    line("request", f"invoke_model titan {body}")
    try:
        raw = await ctx.llmq.invoke_model("titan", body, lane="rt")
        vector = raw.json().get("embedding", [0.0] * 1024)
        line("response", f"{raw.content_type} -> {len(vector)} dims")
    except Exception as exc:  # the app returns a zero vector on any error
        line("response", f"error -> zero vector ({type(exc).__name__})")
        raise


async def flow_memory_write(ctx: Ctx) -> None:
    """`store_query_to_vectorstore` in a daemon thread: sync `embed_query`, context copied."""
    emb = QueuedBedrockEmbeddings(llmq=ctx.llmq, model_id=TITAN, model_alias="titan")
    out: dict[str, Any] = {}

    def store() -> None:
        try:
            out["vector"] = emb.embed_query("user asked about TCS P/E")
        except Exception as exc:  # reported by the flow, not lost in the thread
            out["error"] = exc

    line("request", "titan embed_query (sync) from threading.Thread(copy_context().run)")
    with request_context(lane="bg", session_id="ra-s1", user_id="ra-u1"):
        thread = threading.Thread(target=contextvars.copy_context().run, args=(store,))
        thread.start()
    await asyncio.to_thread(thread.join)
    if "error" in out:
        raise out["error"]
    line("response", f"1 vector x {len(out['vector'])} dims (sync path: direct + limiter)")


# ---------------------------------------------------------------- background jobs


def register_jobs(ctx: Ctx) -> None:
    """Allow-list the app's background jobs on this pod (before `start`)."""

    async def nudge(payload: dict[str, Any]) -> None:
        # nudge_service.store_nudge_in_history -> store_query_to_vectorstore -> embed_query
        vectors = await ctx.llmq.embed("titan", [payload["message"]], lane="bg")
        finish(payload["run"], f"stored nudge vector ({len(vectors[0])} dims)")

    async def ingest(payload: dict[str, Any]) -> None:
        # ingest_query_plans.py (cohere) + weaviate_ingest/* (titan), bulk lane
        docs = await ctx.llmq.embed(
            "cohere", payload["texts"], lane="bulk", input_type="search_document"
        )
        titan = await ctx.llmq.embed("titan", payload["texts"], lane="bulk")
        finish(payload["run"], f"cohere {len(docs)} + titan {len(titan)} vectors")

    def finish(run: str, summary: str) -> None:
        future = ctx.jobs_done.get(run)
        if future is not None and not future.done():
            future.set_result(summary)

    ctx.llmq.register_job("nudge", nudge)
    ctx.llmq.register_job("ingest", ingest)


async def run_job(ctx: Ctx, name: str, payload: dict[str, Any]) -> None:
    """`send_job` and wait (sample only: real callers do not wait) for this pod to run it."""
    run = f"{name}-{uuid.uuid4().hex[:6]}"
    ctx.jobs_done[run] = asyncio.get_running_loop().create_future()
    job_id = await ctx.llmq.send_job(name, {**payload, "run": run}, timeout_secs=600)
    line("request", f"send_job({name!r}) id={job_id} payload keys={sorted(payload)}")
    summary = await asyncio.wait_for(ctx.jobs_done[run], timeout=120)
    line("response", summary)


async def flow_nudge_job(ctx: Ctx) -> None:
    """Nudge history write as a background job (bg)."""
    await run_job(ctx, "nudge", {"message": "Check TCS results"})


async def flow_ingest_job(ctx: Ctx) -> None:
    """Offline ingestion as a job (bulk lane)."""
    await run_job(ctx, "ingest", {"texts": ["TCS P/E", "INFY margins", "HDFC NIM"]})


FLOWS: dict[str, Callable[[Ctx], Awaitable[None]]] = {
    "agent_graph": flow_agent_graph,
    "human_condense": flow_human_condense,
    "guardrail": flow_guardrail,
    "response_fix": flow_response_fix,
    "followup_writer": flow_followup_writer,
    "offline_converse": flow_offline_converse,
    "followup_embed": flow_followup_embed,
    "query_embed": flow_query_embed,
    "safe_embedding": flow_safe_embedding,
    "memory_write": flow_memory_write,
    "nudge_job": flow_nudge_job,
    "ingest_job": flow_ingest_job,
}


# ---------------------------------------------------------------- settings + main


async def load_settings(source: str, target: str) -> None:
    """Fill os.environ from `.env` and/or Secrets Manager; print only names and sources."""
    import _local  # imported after LLMQ_SAMPLES_TARGET is set (see main)

    if source in ("auto", "dotenv"):
        keys = _local.load_env_file()
        path = _local.env_file_path()
        print(f"settings: .env ({path}): {len(keys)} keys set: {', '.join(sorted(keys)) or '-'}")
    want_secret = source == "secrets" or (
        source == "auto" and target == "aws" and _local.app_secret_name() is not None
    )
    if want_secret:
        try:
            name, keys = await _local.load_app_secret(set(REAL_ENV))
            print(
                f"settings: secretsmanager:{name}: {len(keys)} keys set: {', '.join(sorted(keys))}"
            )
        except Exception as exc:
            msg = f"settings: secretsmanager failed: {type(exc).__name__}: {short(str(exc), 160)}"
            if source == "secrets":
                sys.exit(msg)
            print(msg + " (continuing with environment / .env)")


def build_config(target: str) -> QueueConfig:
    """Local stack config, or local queue + real Bedrock for `aws`."""
    import _local

    config = _local.aws_config() if target == "aws" else _local.local_config()
    return config.model_copy(update={"job_queues": {**config.job_queues, **JOB_QUEUES}})


def describe(config: QueueConfig, target: str, tier: str | None) -> None:
    """Print where things run: names and endpoints only, never credentials."""
    print(f"target  : {target}")
    print(
        f"queue   : valkey {config.redis_host}:{config.redis_port} ssl={config.redis_ssl}, "
        f"sqs {config.sqs_endpoint_url or 'AWS ' + (config.aws_region or 'default chain')}"
    )
    print(f"bedrock : {config.bedrock_endpoint_url or 'AWS (real)'} service_tier={tier}")
    for alias, model in config.models.items():
        targets = ", ".join(
            f"{n}:{t.region or t.profile_id or t.base_url}" for n, t in model.targets.items()
        )
        print(f"  model {alias:8s} {model.model_id} [{targets}]")


async def main() -> int:
    """Parse arguments, load settings, start the queue, run the flows, report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--flow", choices=[*FLOWS, "all"], default="all")
    parser.add_argument("--target", choices=["local", "aws"], default="local")
    parser.add_argument(
        "--env-source", choices=["auto", "dotenv", "secrets", "none"], default="auto"
    )
    args = parser.parse_args()

    os.environ["LLMQ_SAMPLES_TARGET"] = args.target  # before the first `import _local`
    await load_settings(args.env_source, args.target)
    import _local

    if args.target == "local":
        _local.use_dummy_credentials()  # after .env, so real keys are never mixed with dummies
    tier = _local.app_service_tier()
    config = build_config(args.target)
    describe(config, args.target, tier)
    await _local.ensure_queues(config)

    llmq = LLMQueue(config)
    ctx = Ctx(llmq=llmq, target=args.target, service_tier=tier)
    register_jobs(ctx)
    await llmq.start(mode="both")
    names = list(FLOWS) if args.flow == "all" else [args.flow]
    results: dict[str, str] = {}
    try:
        for name in names:
            print(f"\n[{name}] {short(FLOWS[name].__doc__ or '', 90)}")
            t0 = time.perf_counter()
            try:
                await FLOWS[name](ctx)
            except Exception as exc:
                ms = (time.perf_counter() - t0) * 1000
                detail = f"{type(exc).__name__}: {short(str(exc), 200)}"
                line("latency", f"{ms:.0f} ms  FAILED {detail}")
                results[name] = "FAIL"
                if args.target == "aws" and any(k in detail for k in AUTH_ERRORS):
                    print("\nAWS credentials missing/expired or access denied: stopping.")
                    break
                continue
            line("latency", f"{(time.perf_counter() - t0) * 1000:.0f} ms  OK")
            results[name] = "ok"
    finally:
        await llmq.stop()
    failed = [n for n in names if results.get(n) != "ok"]
    print(
        f"\n{len(names) - len(failed)}/{len(names)} flows ok"
        + (f"; failed: {failed}" if failed else "")
    )
    print(f"sync fallbacks: {llmq.sync_fallback_count}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
