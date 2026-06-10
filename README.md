# Agentic Airbyte

**The agent plans. Crabbox runs. Airbyte moves. Evidence decides.**

An execution model for agents that own bidirectional data movement — governed, auditable, with no rows and no secrets ever entering the prompt. Not a diagram: we ran it, and the bytes match.

## The Receipt

A real verified run — 50,000 rows, ClickHouse → DuckDB, inside a borrowed islo sandbox:

| | |
|---|---|
| rows moved | **50,000** (source = destination) |
| revenue parity | **$611,815.02 = $611,815.02** |
| parity checks | **4 / 4 PASS** |
| content SHA-256 | `a82239cc73c96e68528e61f885d0b073ea60fb68e4332aec8d586c2a5c73ebcb` (source = destination, byte-exact) |
| wall time | **0.549 s** · **150,700 rows/s** |
| rows through the model | **0** |
| exit code | **0** — the worker exits non-zero unless all four checks pass |

The run emitted nine `::CRABBOX_PHASE::` markers — `bootstrap → boot_clickhouse → seed → discover → write_setup → sync → verify → analytics → emit` — so the orchestrator can time each step, attach evidence to it, and reason over it. The seed is deterministic: same bytes, every run.

## The Pages

- **The model (doc AA-01):** https://zozo123.github.io/agentic-airbyte/ — stamped run receipt, the execution loop, the evidence exhibits, the three contracts, the failure triage map, run-it-yourself.
- **The proof appendix (doc AA-02):** https://zozo123.github.io/agentic-airbyte/poc.html — every phase of the real run, walked end to end.
- **Evidence artifacts:** [`poc/evidence/`](poc/evidence/) — sandbox logs, metrics JSON, live capture.
- **Worker source:** [`poc/worker/`](poc/worker/) — `seed.py`, `etl.py`, `config.json`, plus [`poc/run_e2e.sh`](poc/run_e2e.sh) and [`poc/islo.yaml`](poc/islo.yaml).

## Core Model

- **You / Caller**: Describe outcomes ("Keep Salesforce profiles enriched and fresh from the warehouse. Respect policy X.").
- **Agent Harness**: Your orchestrator (Claude Code, LangGraph, custom service, scheduler, etc.). It turns goals into job specs and bounded runs. It never touches data or long-lived secrets directly.
- **Crabbox**: The run dispatcher. Turns a job spec into an auditable run boundary — lease, scoped env (profiles + strict allow-env), artifacts, durable run id.
- **Sandbox Workers**: The hands. Fresh, repo-defined microVMs (this proof used islo; any provider can sit behind Crabbox). Hydrated with exactly the stack you define. Execute the movement. Return artifacts, phase timings, JUnit, full logs.
- **Airbyte (inside workers)**: The mover. Reads the source, writes the target — inside the box, outside the model context. Rows never pass through a prompt.
- **The Evidence**: The judge. Logs, JUnit, counts, checksums — the only thing the agent is allowed to reason from.

Every data-moving action is a governed `crabbox run --pool ...` with full provenance. Failures are boundary breaks, not mysteries: six classes (F1 plan → F6 validation), each with an owner, a signal to read, and the one bounded input the next run is allowed to change.

## Harness in Action (Real Commands)

The harness is ordinary code that calls Crabbox. Typical sequence it runs:

```bash
# Ensure capacity for the right worker type
crabbox pool ensure example-org/data-movement/main/hetzner/linux/cpx51 \
  --min-ready 3 --create -- --provider hetzner --cache-volume airbyte-etl

# Dispatch a standard ETL task (Airbyte inside the sandbox)
crabbox run --pool example-org/data-movement/main/... \
  --shell 'python -m workers.airbyte_ingest --config /tmp/generated.json' \
  --allow-env 'AIRBYTE_*,SOURCE_*,WAREHOUSE_*' \
  --env-from-profile etl-warehouse \
  --artifact-glob 'reports/**,metrics.json' --junit reports/

# Dispatch complex rETL / AI activation
crabbox run --pool example-org/data-movement/main/... \
  --script workers/activate_with_scoring.py \
  --allow-env 'DEST_*,MODEL_*' \
  --artifact-glob 'evidence/**'

# Harness reflects (feeds this back to its memory / LLM)
crabbox history --limit 50 --json
crabbox results <id>
crabbox artifacts download <id>
```

The agent's output is three contracts, not prose: a **spec** that names references and rules (never secret values or row payloads), a **handoff** a runner can execute (pool id, command, profile name, artifact contract), and a **repair rule** — the next run changes one bounded input tied to the failing owner, keeping every attempt comparable.

## Run It Yourself

The proof reproduces with one command — lease a sandbox, hydrate from the repo, run, tear down:

```bash
islo use airbyte-etl --config poc/islo.yaml --source github://zozo123/agentic-airbyte -- bash poc/run_e2e.sh
```

Or dispatch it as a governed Crabbox run with `--shell 'bash poc/run_e2e.sh' --artifact-glob 'poc/reports/**' --junit poc/reports/`.

## Honest Scope

The proof uses the **Airbyte source→destination contract on a custom-connector (Airbyte CDK) path**, not a packaged connector deployment — that's what lets it run self-contained in a sandbox in under a second. What it does prove is the part that matters for agentic data movement: a goal-driven worker can be dispatched into an isolated box, move real typed data end-to-end, and return evidence strong enough — a byte-exact checksum — for a harness to trust the result and decide what to do next.

## Explore

- Live model page: https://zozo123.github.io/agentic-airbyte/
- Proof appendix: https://zozo123.github.io/agentic-airbyte/poc.html
- Crabbox: https://github.com/openclaw/crabbox (and crabbox.sh)
- Airbyte: https://airbyte.com

---

Agent plans. Crabbox runs. Airbyte moves. Evidence returns. Repeat only when the evidence says what changed.
