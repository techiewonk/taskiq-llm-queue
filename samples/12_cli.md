# 12 · The `llm-queue` operator CLI

The CLI reads the same `LLMQ_*` variables as `QueueConfig.from_env()`. For the local stack,
`samples/_local.py env` prints them, including `LLMQ_MODELS` (the sample models as JSON) and
`LLMQ_FALLBACK_CHAINS`. Run everything from the package root (`libs/taskiq_llm_queue`).

```bash
# bash / zsh (Git Bash on Windows too)
eval "$(.venv/Scripts/python samples/_local.py env)"     # .venv/bin/python on Linux/macOS
alias llmq=.venv/Scripts/llm-queue                        # or: uv run llm-queue
```

```powershell
# PowerShell
.venv\Scripts\python samples\_local.py env --powershell | Invoke-Expression
Set-Alias llmq .venv\Scripts\llm-queue.exe
```

Every change is validated, then appended to the `<key_prefix>:cfg:audit` stream with
`--actor` (default: your OS user). Pods pick changes up within `cfg_reload_secs` (30 s by default), with no restart.

## Limiter and breaker state

```bash
llmq limiter show                     # every configured model x target
llmq limiter show --model gpt120b     # one alias (all its targets), or gpt120b@apac
```

Output is one row per target: `limit` (learned concurrency; `null` until the first call),
`inflight`, `paused_secs` (> 0 right after a throttle) and `breaker`
(`closed` / `open` / `half_open`). Run `05_fallback_on_throttle.py`, then this, to see a
throttled target's limit drop and its breaker open.

## Fallback chains

```bash
llmq fallback get                               # stored config as JSON
llmq fallback get --model gpt120b               # resolved chain: gpt120b@primary -> gpt120b@apac [-> gpt20b@primary]
llmq fallback test --model gpt120b              # dry run: which step would serve right now, with limiter/breaker per step
llmq --actor me fallback set --model gpt120b --chain "gpt20b"   # other models tried under policy "chain"
llmq fallback set --file fallback.json          # whole config: {"chains": {...}, "skip_targets": {...}}
llmq fallback set --model gpt120b --chain "gpt120b"   # rejected (self-reference); last good config stays, exit 1
llmq fallback reset                             # delete the stored config (back to each pod's startup default)
```

> **Note:** the CLI does not use `LLMQ_FALLBACK_CHAINS`. Until something is stored,
> `fallback get` / `test` show only the same-model targets, even though the sample pods start
> with `gpt120b -> gpt20b` as their default. `fallback set --chain` writes on top of an
> empty config too, so other models' default chains are not carried over.

## Canary routing (sqs vs direct)

Only pods with `QueueConfig(routing_enabled=True)` read this (the samples leave it off).

```bash
llmq routing get
llmq routing set --model gpt20b --lane rt --pct 10   # 10 % of rt sessions go through SQS (sticky per session)
llmq routing allow add u-123                         # this user always goes through SQS
llmq routing kill on                                 # everything direct within cfg_reload_secs
llmq routing kill off
```

There is no `routing reset`; set the percentages back, or run `samples/_local.py reset`.

## Jobs

```bash
echo '{"source": "cli", "texts": ["a", "b"]}' > payload.json
llmq job send ingest --payload-file payload.json     # queue jobs-ingest by default; --queue, --lane
```

Prints `task_id`, `message_id` and the queue URL. Only SQS send permission is needed. A pod
that registered `ingest` runs it: `09_jobs.py` does, so a job sent here runs on its next run.

## Queues and dead-letter queues

The CLI has no queue or DLQ commands. For the local stack:

```bash
.venv/Scripts/python samples/_local.py queues        # visible / in-flight per sample queue and its -dlq
```

With the AWS CLI (any credentials work against ElasticMQ):

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=ap-south-1
SQS="aws --endpoint-url http://localhost:9324 sqs"
$SQS list-queues --queue-name-prefix llm-
$SQS get-queue-attributes --queue-url http://localhost:9324/000000000000/llm-gpt120b-dlq \
     --attribute-names ApproximateNumberOfMessages
$SQS receive-message --queue-url http://localhost:9324/000000000000/llm-gpt120b-dlq \
     --max-number-of-messages 1 --visibility-timeout 0  # peek at one message without consuming it
```

On AWS, drop `--endpoint-url`. To move DLQ messages back, use `aws sqs start-message-move-task`.

## Audit trail and reset

```bash
docker exec llmq-test-valkey-1 valkey-cli -a localdev --no-auth-warning \
    XREVRANGE llmq:local:cfg:audit + - COUNT 5      # last 5 config changes with actor
.venv/Scripts/python samples/_local.py reset         # wipe llmq:local:* keys + default fakellm scenario
```
