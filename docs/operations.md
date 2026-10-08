# Operations and safe reruns

## Normal flow

```text
Weekly trigger
  → ADF orchestration pipeline
  → Bronze API ingestion
  → Silver transformation
  → Gold decision support
  → Top-50 result
```

The intended business cadence is weekly on **Monday at 06:00 Stockholm time**. The cadence matches an infrastructure-planning use case and avoids unnecessary API traffic and compute.

## Monitoring

1. In ADF Monitor, open **Pipeline runs** and locate the relevant run.
2. Inspect **Activity runs** to find the first failed stage and its detailed error.
3. In Databricks, open **Jobs & Pipelines → Runs** and inspect the corresponding notebook output.
4. If an alert was received, use its Run ID and failed-stage name before changing or rerunning anything.

## Failure response

- **Bronze/API failure:** allow the bounded retries to finish, then inspect the first HTTP/network error and returned source response.
- **Source schema change:** re-check SCB metadata/dimensions/codes before modifying downstream transformations.
- **401/403/policy issue:** inspect ADF linked service, managed identity/access connector, Unity Catalog and storage permissions; restore access before rerun.
- **Silver failure:** confirm Bronze succeeded, then inspect Bronze schema/types, duplicate/null results, and the first Silver transformation error.
- **Gold failure:** confirm Silver succeeded, then inspect Gold inputs, price normalization, score fields and quality output.
- **Notification missing:** inspect Logic App run history and the ADF `notify_*` Web activity. The downstream stage-specific Fail activity is designed so a notification-delivery problem does not turn the original processing failure into a successful pipeline run.

## Safe rerun

After the root cause is fixed, rerun the **complete ADF pipeline**, not an isolated production stage.

- Bronze overwrites the current snapshots.
- Silver deduplicates on business keys and overwrites its tables.
- Gold is rebuilt deterministically from clean inputs.
- After the run, verify Silver duplicate/null checks and the Gold quality contract.

The full-refresh design is appropriate to the relatively small current-snapshot datasets used in this project. A production audit/history requirement would justify dated raw snapshots or versioned ingestion.
