# Power BI portfolio layer

Power BI is the business-facing exploration layer over the Gold result; it is not another transformation pipeline.

## Included source

`source/` contains the PBIP report and TMDL semantic-model source. Local Power BI cache/state (`.pbi/`) and the binary `.pbix` file are intentionally excluded.

The original semantic model referenced a local Windows download path. In the public source that path is replaced with:

```text
C:\PATH\TO\EV_Charging_Gold_Snapshots_PowerBI.xlsx
```

To open the project locally, update the three workbook-backed queries to the copy in `data/EV_Charging_Gold_Snapshots_PowerBI.xlsx`.

## Snapshot model

The workbook contains two historical Top-50 output snapshots plus comparison/metadata tables. It is **not** the full 290-row municipality Gold table.

That scope is intentional: the full Gold model is produced in Databricks, while Power BI uses the two Top-50 snapshots to demonstrate current decision support, reproducibility, ranking movement and snapshot comparison.

## Report pages

The six exported views are available under `screenshots/`:

1. Command View
2. Candidate Geography
3. Demand vs Cost
4. Decision Anatomy
5. Ranking Movers
6. Snapshot Comparison
