# Agent Harness for Data Movement

**Tell the harness a high-level goal. It moves the data by orchestrating Crabbox remote sandboxes as ETL & rETL workers.**

The harness is the brain: it receives intent, observes state, plans, ensures worker capacity, dispatches tasks, collects rich evidence, reflects, and iterates.

The actual movement happens inside **Crabbox remote sandboxes** — warm, isolated, repo-defined execution environments that act as specialized, auditable workers. Airbyte runs inside them for reliable standard pipes; custom and AI-driven logic handles the complex, reverse, on-prem, or dynamic cases.

This is how agents will *actually* own bidirectional data movement: high-level goals in, safe + observable execution out.

## The Live Vision

**https://zozo123.github.io/agentic-airbyte/**

The one-pager above is the definitive visual explanation — architecture, flow diagrams, why sandboxes make great workers, and exactly how a harness dispatches real work.

## Core Model

- **You / Caller**: Describe outcomes ("Keep Salesforce profiles enriched and fresh from the warehouse. Respect policy X.").
- **Agent Harness**: Your orchestrator (LangGraph, CrewAI, custom service, scheduler, etc.). It turns goals into plans and worker tasks. It never touches data or long-lived secrets directly.
- **Crabbox**: The fleet manager. Maintains ready pools of sandboxes via `pool ensure` + prewarm + Actions hydration from *your* repo. Handles leasing, secret forwarding (profiles + strict allow-env), and audit.
- **Sandbox Workers**: The hands. Borrowed in seconds from warm pools. Hydrated with exactly the stack you define (Airbyte, CDK, Python libs, etc.). Execute the movement. Return artifacts, metrics (via phases + JUnit), full history, and logs for the harness to reason over.
- **Airbyte (inside workers)**: The declarative, observable engine for standard src ↔ dst. CDK + custom code for everything else.

Every data-moving action is a governed `crabbox run --pool ...` with full provenance.

## Why Sandboxes as Workers Win

- **Speed**: Warm pools + `--pool` borrow = seconds, not minutes. Cache volumes for deps.
- **Safety & Isolation**: Fresh or borrowed env per task. Strict env allowlisting + profiles. Harness stays out of the blast radius.
- **Reproducibility**: Defined in your git repo via `.github/workflows` (Actions hydration). Same on any provider — cloud, Hetzner, your Proxmox/SSH on-prem boxes.
- **Observability for Agents**: Every run produces structured results the harness can ingest: history, events with `CRABBOX_PHASE`, artifacts, JUnit summaries, timings. Perfect for reflection loops.
- **Flexibility**: One worker model for bulk ETL, low-latency rETL/activation, AI enrichment, regulated flows, internal networks.

Different pools for different worker "personalities." The harness picks the right one.

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

Use `jobs:` in `crabbox.yaml` for reusable named worker flows. Use ponds when tasks need coordinated boxes.

## The Vision

Traditional ETL/rETL is static pipelines maintained by humans.

Raw agents with tool access are powerful but fragile on secrets, state, audit, and reproducibility.

**The harness + sandbox workers model** gives you both: dynamic, goal-driven intelligence *and* governed, fast, fully-auditable execution that works everywhere.

Airbyte for the reliable pipes.  
Crabbox for the real remote hands.  
The harness for the brain that tells them what to do.

Built on actual Crabbox capabilities (ready pools, prewarm, Actions hydration, env profiles, artifacts + history, jobs, cache volumes, multi-provider including on-prem). Neutral examples. Self-host friendly.

## Explore

- Live one-pager (recommended): https://zozo123.github.io/agentic-airbyte/
- Crabbox: https://github.com/openclaw/crabbox (and crabbox.sh)
- Airbyte: https://airbyte.com

This repo exists to clearly present the vision. The page is the artifact. PRs that improve clarity, diagrams, or accuracy are welcome.

---

Neutral. Execution-focused. Ready for agents.