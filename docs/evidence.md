# Evidence map

This repository uses a deliberately conservative evidence model. It distinguishes what was actually executed from what was later configured/validated.

| Claim | Evidence | Status |
|---|---|---|
| Gold Top-50 output exists and can be queried | `evidence/gold-top50-refreshed.png` | Executed result |
| Silver tables rerun without duplicate/null accumulation | `evidence/silver-quality-check.png` | Executed result |
| Final ADF topology contains success path plus per-stage notification/Fail branches | `evidence/adf-final-topology.png` and sanitized pipeline JSON | Published/validated configuration |
| The weekly schedule trigger started a run automatically | `evidence/schedule-trigger-succeeded.png` | Executed on 2 Oct 2026 |
| Bronze, Silver and Gold all succeeded in dependency order | `evidence/pipeline-activities-succeeded.png` | Executed on 2 Oct 2026 |
| The centralized notification endpoint can deliver the structured failure email | `evidence/logic-app-notification-redacted.png` | Independently verified on 5 Oct 2026 |
| Business-facing analytical views exist | `../powerbi/screenshots/` | Exported final Power BI views |

## Verification chronology

### 2 October 2026 — executed success path

A temporary schedule window was used to prove automatic execution. The trigger fired and the three Databricks processing stages completed successfully in Bronze → Silver → Gold order.

### Final ADF failure design — added afterward

The final pipeline definition added per-stage failure notifications and explicit Fail activities. ADF accepted/published the topology. It was not deliberately forced through a full Databricks processing failure only for evidence.

### 5 October 2026 — independent notification test

The Logic App HTTP endpoint was tested with a controlled failure payload. The endpoint run succeeded and the structured Outlook message was delivered. The public screenshot redacts personal account details.

## Snapshot chronology

Two ranking snapshots are intentionally retained:

- the earlier verified baseline snapshot,
- the later refreshed/automated snapshot used by the Power BI portfolio views.

Rank/value differences between them are expected because the source APIs include rolling and refreshed data. For a given input snapshot the model is deterministic; live external data is not immutable across dates.
