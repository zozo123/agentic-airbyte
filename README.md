# Agentic src ↔ dst with Airbyte + Crabbox

Concise one-pager explaining the pattern for **AI agents that intelligently manipulate data flows** between sources and destinations.

- **Airbyte**: Reliable, open-source src → dst pipes (300+ connectors, CDK, self-hostable).
- **AI Agents**: Dynamic configuration, custom logic generation, bidirectional decisions, and in-flight data manipulation.
- **Crabbox**: The trusted execution layer — warm pooled boxes, safe secrets, full audit of every agent action, any environment (cloud / on-prem / sandboxes).

## View

Open `index.html` in a browser.

## Publish on zozo123.github.io (recommended)

1. In your `zozo123/zozo123.github.io` repo, copy the entire `agentic-airbyte/` folder into it (or the contents of index.html + README into a subdir).
2. Commit and push to the `main` (or `gh-pages`) branch.
3. GitHub Pages will automatically serve it at:
   **https://zozo123.github.io/agentic-airbyte/**

For a completely separate project page, create a new repo named `agentic-airbyte` under your zozo123 account, put these files at the root, and enable GitHub Pages (source: root, branch main). The URL will be the same: https://zozo123.github.io/agentic-airbyte/ 

The page is designed to work perfectly as a sub-directory on your personal site.

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