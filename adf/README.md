# Azure Data Factory orchestration

The files here are sanitized extracts from the exported ADF ARM template. They preserve the real orchestration topology while removing environment-specific identifiers that should not be public. The public linked-service name is also normalized so the portfolio package is not tied to environment-specific deployment naming.

## Included

- `pipeline/pl_charging_infrastructure_orchestration.json`
- `trigger/trg_charging_infrastructure_weekly.executed-test.json`
- `linked-service/ls_databricks.json`

## Sanitization

The public versions replace:

- Logic App signed HTTP callback URLs,
- Databricks job IDs,
- Databricks workspace hostname,
- Azure subscription/resource-group/workspace resource identifiers.

The linked service still shows the verified authentication design: **managed identity (MSI)** rather than a notebook-embedded secret.

## Trigger chronology

The trigger JSON is historical execution evidence, not a rewritten idealized schedule. It reflects the temporary Friday 16:45 verification window used for the successful 2 October 2026 scheduled run.

The intended business cadence documented in the project is **Monday 06:00 Stockholm time, weekly**. This distinction is kept explicit so the repository does not claim that the historical verification export was configured differently than it really was.

## Failure topology

Bronze, Silver and Gold each have a failure notification branch plus a stage-specific `Fail` activity. Bronze is the external-API stage and therefore has 2 retries at 60-second intervals with a 1-hour timeout.
