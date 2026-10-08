# Databricks implementation

This folder contains the authentic project notebooks in two review-friendly forms:

- `notebooks/` — sanitized `.ipynb` exports with execution outputs and transient metadata removed.
- `src/` — plain-Python exports generated directly from the code cells for easy GitHub inspection.

Only presentation metadata/output was removed; the transformation logic was not rewritten.

## Stages

### 01 — Bronze ingestion

- Reads SCB metadata before building source requests.
- Loads the latest 12 months of vehicle registrations.
- Loads 2025 passenger-car stock and population context.
- Loads a rolling 30-day electricity-price window for SE1–SE4.
- Writes controlled snapshots as Delta tables using overwrite semantics.

### 02 — Silver transformation

- Applies explicit types and analytical column names.
- Deduplicates on the natural business keys for each source.
- Preserves the source grains rather than joining everything prematurely.
- Runs duplicate/null checks for the four Silver tables.

### 03 — Gold decision support

- Creates the governed municipality dimension and price-area mapping.
- Aggregates the 30-day electricity-price series by price area.
- Calculates 12-month EV volume and six-month absolute change.
- Applies percentile scores for EV volume/growth and inverse min–max normalization for price.
- Produces the ranked Top-50 candidate table and validates the Gold quality contract.
