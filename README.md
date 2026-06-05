# Agent Harness for Data Movement • Crabbox Sandbox Workers + Airbyte

You tell the **agent harness** a high-level goal for data movement.

The harness plans and dispatches the real work to **Crabbox remote sandboxes** running as specialized ETL &amp; rETL workers (with Airbyte providing the reliable standard pipes inside them).

- **Agent Harness**: Your goal-oriented orchestrator (LangGraph, Crew, custom loop, service). It observes, plans, ensures capacity, dispatches, collects evidence, reflects.
- **Crabbox Sandbox Workers**: Warm pooled, repo-hydrated, isolated remote environments. The actual "hands" that move data safely. Full artifacts + history returned.
- **Airbyte**: The fast, observable src ↔ dst pipes that run inside the workers for standard flows (CDK + custom logic for the rest).

## View

Open `index.html` in a browser.

## Live site

**https://zozo123.github.io/agentic-airbyte/**

This repo is set up for automatic GitHub Pages deploys via `.github/workflows/deploy.yml` (triggered on push to main or `gh workflow run`).

## Local preview

Just open `index.html` directly in a browser (all assets via CDN).

## Core idea

Agents no longer just plan pipelines. They actively manipulate src ↔ dst in real time:

- Discover sources → auto-configure Airbyte connections
- Generate and test custom CDK connectors/transforms
- Drive intelligent reverse / activation logic
- React to data and business signals

Airbyte handles the standard declarative movement.  
Crabbox gives agents safe, reproducible, auditable hands to run the custom and complex parts.

## Practical pattern

```bash
# Agent configures Airbyte (executed through Crabbox for audit)
crabbox run --pool ... --shell 'airbyte connections create ...'

# Agent runs custom manipulation / reverse
crabbox run --pool ... \
  --script /tmp/ai_manip.py \
  --allow-env 'AIRBYTE_*,WAREHOUSE_*,DEST_*' \
  --env-from-profile ~/.agent/creds.env \
  --artifact-glob 'reports/*' \
  --label "agent:airbyte-src-dst:$(date -I)"
```

Pre-warm pools once with Airbyte CDK + your stack. Agents get instant execution environments.

## Why this matters

- Full audit trail of AI decisions (every action = a Crabbox run)
- Secret-safe by design (strict allowlisting)
- Works everywhere (SSH-static, cloud, on-prem, delegated)
- Fast iteration (warm boxes + cache volumes)
- True bidirectional src ↔ dst intelligence

## Related

- Full Crabbox: https://github.com/openclaw/crabbox
- Airbyte: https://airbyte.com
- Deeper design notes: see `../docs/plan/`

Neutral, execution-focused, agent-ready.