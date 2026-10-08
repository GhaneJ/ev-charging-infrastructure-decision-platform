# Architecture notes

The diagram in this folder is intentionally small enough to understand at first glance while preserving the project's separation of concerns.

- Public APIs provide vehicle registrations, passenger-car stock, population and electricity prices.
- Azure Databricks performs the Bronze → Silver → Gold transformations and writes Delta tables governed through Unity Catalog.
- Azure Data Factory orchestrates the Databricks stages and owns schedule/retry/monitor/failure-routing behavior.
- Power BI consumes exported Gold-result snapshots for portfolio analysis and snapshot comparison.

The project does **not** claim that ADF performs the Gold scoring logic. The scoring implementation lives in the Gold Databricks notebook.
