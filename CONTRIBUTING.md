# Contributing to taskiq-llm-queue

This is a library: other apps install it and run it inside their own processes. Most of the
rules below follow from that. Where this file and the house standard disagree, this file wins.

## Setup

You need Python 3.11+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync --all-extras          # .venv with every extra and the dev group (uv.lock)
python scripts/stack.py up    # or: make up
python scripts/stack.py status
```

| Service | Port | Used for |
|---|---|---|
| Valkey 8.1 | 6379 | the main store for stack tests, samples, drills |
| Redis 7.4 | 6380 | the second store engine; every engine test runs on both |
| fakellm | 8080 | a fake Bedrock / OpenAI-compatible server (`tools/fakellm`) |

`python scripts/stack.py rebuild` after changing `tools/fakellm`; `down` to stop. The store
password for tests is `localdev` (`LLMQ_TEST_REDIS_PASSWORD` overrides it).

`uv.lock` is the lock file. Change dependencies in `pyproject.toml` and run `uv lock`; never
edit the lock by hand, and commit both together. Dependencies are ranges (`>=x,<y`), never
pins, and each one has a comment saying why it is there and why the bound is where it is.
Optional features go behind an extra (`openai`, `langchain-aws`, `langchain-openai`,
`metrics`, `http`), not into the base install.

## Running checks

**Every check goes through `scripts/check.py`.** A step is done only when its check passes.

```bash
python scripts/check.py --list      # the steps and what each one runs
python scripts/check.py a6          # one step
python scripts/check.py a6 a8       # several
python scripts/check.py all         # everything (or: make check-all)
python scripts/check.py all --quick # drills shortened: gates without B17 / B91 / B92
python scripts/check.py soak3       # 3-minute soak: smoke test while fixing
python scripts/check.py soak10      # 10 minutes: after a change to the engine
python scripts/check.py soak20      # 20 minutes: before a release
```

**Logs.** Every check writes its whole output, live, to
`reports/check-<time>/<step>.<check>.log` (git-ignored); the console prints one PASS / FAIL
line per check with its log path, and `summary.log` in the same folder keeps the console.
pytest runs with `-rA`, so a log lists every test and its outcome. Follow a long drill with
`tail -f reports/check-<time>/gates.*.log`.

**Scenarios first.** `scripts/scenarios.py` runs one fault at a time (or a chosen pair) and
judges it by calls, not by time: a phase of healthy calls, the fault with calls still coming
until it ends, then a phase after it settles. Each scenario has its own pass rule and runs
under three loads (50 %, 100 % and 300 % of the budget). The `fast` profile (default)
shortens leases, visibility, deadlines and lifetimes, so a scenario takes 10-40 s; `--profile
prod` re-runs it with production timers.

```bash
python scripts/scenarios.py --list           # every scenario and its rule
python scripts/scenarios.py S4 --load under  # one, while fixing it
python scripts/scenarios.py --group store    # a group
python scripts/check.py scenarios --quick    # every scenario, one load each
```

| Group | Scenarios |
|---|---|
| worker | S1 killed, S2 graceful restart, S3 all down then back |
| store | S4 blocked, S5 half-open, S8 keys reset, S9 out of memory, S10 slow; S6 / S7 / S11 failover drills on Docker nodes |
| provider (fakellm) | P1 throttled, P2 5xx, P3 hangs, P4 model not in region, P5 bad credentials, P7 every region down |
| registry | R1 rpm cut live, R2 region disabled, R3 kill switch, R4 bad edit refused |
| caller | C1 cancel, C2 caller process dies, C3 duplicate request id |
| pairs | X1-X8: two faults at once (store block + worker kill, rpm cut + throttle ...) |

A new bug gets a scenario (or a check in one) before its fix, like a unit test.

**Short first, longer before a release.** `all --quick` leaves out the three slow
calibration gates. The soak has three lengths, and each scales its chaos spacing, store block,
status expiry and sampling to fit, so even 3 minutes sees every chaos event:

| Step | Calls | Chaos | Use |
|---|---|---|---|
| `soak3` | ~12,600 | every 30 s, block 10 s | smoke test while fixing |
| `soak10` | ~42,000 | every 60 s, block 20 s | after a change to the engine |
| `soak20` | ~84,000 | every 120 s, block 20 s | before a release |

Shorter runs catch broken recovery, leftovers and deadline bugs as well as long ones; only
time shows slow leaks and drift (more calls, more growth above the noise). Run `all` without
`--quick`, and `soak20`, before a release.

Each step runs `ruff check`, `ruff format --check` and `mypy` on its source paths, then
`pytest` on its tests, then any extra script. Steps that need the stack fail at once if a
service does not answer; they never skip silently.

| Steps | What they cover |
|---|---|
| `a0` | fakellm itself (accounts, TPM, speed, hangs, streaming) |
| `a1`-`a2` | registry: schema, settings, validation; store, live edits, audit, rollback |
| `a3` | clients per account and region, providers, attempt timers |
| `a4`-`a6` | keys, message format, stream queue, admission (Lua) |
| `a7`-`a13` | tokens, engine end to end, hooks, operations, health, status, embeddings |
| `a14`-`a17` | front door, LangChain adapters, assume-role, latency exclusion, streaming |
| `store` | store failover tests plus `scripts/failover.py` (Docker failover drill) |
| `load` | `scripts/load.py`: 2,000 calls over three workers; a burst over the RPM budget |
| `gates` | `scripts/gates.py`: the plan's load and chaos gates on worker processes (~20 min) |
| `soak3` / `soak10` / `soak20` (not in `all`) | `scripts/soak.py --minutes N`: 8 worker processes, chaos (kill, store block, live edit, restart), leak and drift checks; `soak20` before a release |
| `samples` | `scripts/samples.py`: every file in `samples/` run as app code on fakellm |

**No test calls a real LLM.** Unit tests use fakes, stack tests and drills use fakellm. The
only path to a real model is `python scripts/samples.py --live` (real Bedrock, AWS keys read
from `--env-file`, default `.env`), and it runs only when someone asks for it. `--store env`
points the samples at the env file's store; the run touches only its own key prefix and
deletes it afterwards.

CI (`.github/workflows/ci.yml`) runs ruff, format, mypy and the unit tests on Python
3.11-3.14, the unit tests on the lowest allowed versions (`--resolution lowest-direct`), the
LangChain 1.x adapters, and unit plus integration tests against the same Docker stack.

### Adding a check step

Add an entry to `STEPS` in `scripts/check.py`: a title, the `sources` it lints and
type-checks, the `tests` it runs (files or `file::test` ids), `needs_stack=True` if it uses
Docker, and `extra` for a script to run after the tests. Keep the source paths narrow so a
step stays fast. If the new tests live in a new folder, check that `mypy` and pytest still
find their helpers (see the overrides in `pyproject.toml`).

## Code standards

Ruff (rules `ASYNC B C4 D E F I PIE PT RET RUF SIM UP W`, Google docstrings, line length 100)
and **strict mypy** over `src`, `tests`, `tools`, `scripts` and `samples` must pass.

- **Docstrings** on every public module, class and function: what it does, why it exists,
  where it is used, in 2-4 lines. A multi-line docstring has a one-line summary, a blank
  line, then the body (D205). `Args:` / `Returns:` only when the types do not say it.
  Tests follow the same 2-4 lines even though lint does not check them.
- `from __future__ import annotations` at the top of every module.
- **Names carry units**: `_secs` (float seconds), `_ms` (int milliseconds), `_bytes`;
  counts are bare; booleans are positive (`enabled`, not `disabled`).
- **No side effects at import**: no clients, network, file reads, threads or logging
  configuration. The app configures structlog; the library only logs.
- **Optional dependencies are imported lazily**, at the point of use, and a missing one
  raises `MissingExtra(feature, extra)`, which names the extra to install.
- **One error family.** Everything raised to callers is an `LLMQueueError`. Bad config is
  `ConfigError`, raised at startup or on a refused registry edit. A failed call is
  `CallFailed` with a `kind`. Do not add new exception types where a `kind` will do.
- **Never raise into a live call because of bad config.** Validate at boot or when the
  registry is published, list every problem at once, and fall back to the default at
  run time.
- **Performance claims come with numbers**, measured, with the date, next to the claim
  (see the comments on `hiredis`, `orjson`, `uuid-utils` in `pyproject.toml`). If you
  have not measured it, write "unverified".
- **Keep the "why" comments** that explain a trap, a measurement or a failure mode. Shorten
  them if they are long; do not delete them.
- `# noqa` only for a selected rule, with the code and a reason on the same line.

## Library rules

- **No app or business config inside the library.** Accounts, models, regions, budgets and
  keys come from the registry document (`LLMQ_REGISTRY`) and the `LLMQ_*` settings
  (`LLMQueue.from_env`, optionally a `secrets=` lookup). No defaults that only make sense
  for one consumer.
- **No backward-compatibility shims in 0.3.** 0.3.0 is a clean break from 0.2
  (STABILITY.md). A removed 0.2 setting is refused at startup with its replacement
  (`"<old> is removed in 0.3; use <new>"`), not quietly mapped to the new one.
- The public API is `taskiq_llm_queue.__all__`, the registry setting names and the
  `llm-queue` commands. Changing any of them is a CHANGELOG entry.
- Message format (`v`) and store key schema changes follow STABILITY.md: a worker returns a
  newer message to the queue instead of dropping it, and a schema change ships with a
  migration note.

## Tests

| Folder | Runs | Needs |
|---|---|---|
| `tests/unit/` | registry, clients, tokens and inference, fakellm | nothing (default `testpaths`) |
| `tests/integration/registry/` | the registry store | the stack |
| `tests/integration/engine/` | queue, admission, runtime, ops, streaming, front door | the stack |

Engine tests share `engine_testkit.py` (fakellm on `sys.path`, the test registry, `stats`)
and these fixtures from `tests/integration/engine/conftest.py`:

- `store`: an async client, parametrised over Valkey (6379) and Redis (6380), so every
  engine test runs on **both** engines. Do not narrow a test to one engine.
- `prefix`: a unique key prefix per test, every key under it deleted at teardown.
- `make`: builds an `Engine` with its own in-process fakellm (`make(models, scenario)`),
  and stops it at teardown.

Rules:

- **Pin a bug with a test** that fails without the fix. Its docstring names the bug and
  what used to happen.
- **No fixed sleeps to wait for state.** Poll for the condition with a deadline and fail
  with a clear message when the deadline passes. A sleep is fine only when time itself is
  the thing under test (a lease, a window).
- **Fixed-window (per-minute) budgets reset on the calendar minute.** A test that counts
  against one must start away from the minute's end (see `_away_from_minute_end` in
  `test_runtime.py`) or check that the minute did not change, or it fails at random.
- Test failure paths and cleanup (nothing left held, no keys left behind), not only the
  happy path.

## Lua scripts

Admission (`engine/_admission_lua.py`) and other atomic store operations are Lua.

- **One script per atomic operation** (PICK_TAKE, FINISH, UNDO, RENEW, SWEEP). Do not split
  an operation across round trips, and do not merge unrelated operations into one script.
- **Key names are built inside the script from the prefix in `ARGV`.** This depends on the
  single-primary store 0.3 supports (cluster mode is not supported); say so if you change it.
- **Time is the store's clock** (`redis.call('TIME')`), never the caller's.
- Measure a script change by its server time on the stack, before and after, and put the
  numbers in the change.

## Performance changes

A change made for speed needs before and after numbers from the same machine: the
`gates.py` gate it affects, `load.py`, or a micro-benchmark of the hot path (state the
Python version and date). Record the result in the CHANGELOG `Performance` section. No
numbers, no merge.

## Docs site

The site (Zensical, GitHub Pages) is built from `zensical.toml`: Home is the README, then
Architecture, Samples, the API reference (generated from the docstrings), Changelog, Stability
and Contributing. Internal docs (`docs/ACCOUNT_ROUTING_*`, `RELEASE_PLAN_*`, `RELEASING.md`,
`TEST_AND_FIX_PLAN.md`) are excluded. Links to other files are absolute GitHub URLs, so they
work on the site, on GitHub and on PyPI.

```bash
uvx --with "mkdocstrings-python==2.0.11" "zensical==0.1.1" build --clean --strict  # as CI
uvx --with "mkdocstrings-python==2.0.11" "zensical==0.1.1" serve                   # local preview
```

`.github/workflows/docs.yml` builds every pull request with `--strict` and deploys on a
published release or a manual run (Actions > Docs > Run workflow).

## Docs to update with a change

| Doc | When |
|---|---|
| `README.md` | a public name, setting or behaviour changes |
| `CHANGELOG.md` | always: under the unreleased version, in `Added`, `Fixed`, `Performance`, `Removed` or `Changed` |
| `docs/ACCOUNT_ROUTING_PLAN.md` §13 | implementation status of a plan item changes |
| `samples/README.md` (and the sample) | a usage pattern changes or a sample is added |
| `docs/ARCHITECTURE.md` | the structure, the data flow, the failure model or a measured number changes |

CHANGELOG entries are plain sentences: what changed and, for a fix, what used to go wrong.

## Commits

- Imperative subject under about 72 characters, a blank line, then a body that explains
  **why**.
- No `Co-Authored-By` lines and no "Generated with" footers.
- Stage only the files you changed. Never sweep in unrelated work.
- Never commit `.env`, `.env.*` or `.pypirc` (they are git-ignored and kept out of the sdist
  by its allow-list; keep it that way).
- Never skip hooks (`--no-verify`) or bypass signing.

## Releases

Releases go out **only** through a GitHub release and `.github/workflows/publish.yml`, which
re-runs CI, builds once and uploads with PyPI trusted publishing (OIDC, with provenance).
Never upload with `twine` or by hand, and never store a PyPI token. A manual run of the
workflow goes to TestPyPI for a trial install. The steps (version bump, CHANGELOG, local
`make check-all`, TestPyPI, tag) are in [docs/RELEASING.md](https://github.com/techiewonk/taskiq-llm-queue/blob/main/docs/RELEASING.md).
