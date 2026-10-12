# Research Telemetry Lab 2 — curated data README

## Purpose and origin
This package contains **synthetic teaching observations** for the VNU-UET Big Data Lab 2 curation exercise (`research-telemetry-v1`). No real personal or research telemetry are included. The raw collection mixes a CSV source (A) and a JSON Lines source (B); both are frozen against an instructor-controlled `trusted_manifest.json`.

## Files and keys
- Raw inputs (restricted): `research-raw/lab2/inputs/batch-01/{observations_a.csv,observations_b.jsonl,sensors.csv,qa_reference.csv}`.
- Staging (restricted): curated Parquet, `quarantine.csv`, and `duplicates.csv` under `research-raw/lab2/staging/<run_id>/`.
- Machine-readable metadata: `catalog.json`, `contract.json`, `quality_before.json` / `quality_after.json`, `lineage.jsonl`, `run-record.json`, `retention-plan.json`.
- Physical provenance key: `(source_object, source_row)`; business key: normalized `record_id`.

## Timestamps and units
All contract timestamps use UTC `YYYY-MM-DDTHH:MM:SSZ`. Supported units are C and F; released `temperature_c` is `DECIMAL(8,2)` Celsius after validation. The fixed evaluation cutoff is **AS_OF = 2026-02-09T00:00:00Z** (data clock, not wall-clock execution time). Late but otherwise valid rows (`lag_seconds > 900`) are retained with `is_late = true`.

## Curation and deduplication
Malformed rows are quarantined as `PARSE`. Missing/invalid IDs (`R[0-9]{6}`) and unparseable ingest times are quarantined before deduplication (`KEY`, `INGEST_PARSE`). Remaining rows are ranked by `ingest_time DESC`, then `source_object ASC`, `source_row ASC`. Non-winners enter the duplicate ledger as `SUPERSEDED`. Invalid newer winners are quarantined; older valid versions are **not** substituted back into the candidate. `qa_reference.csv` evaluates agreement/coverage only and never repairs values. Restricted `operator_email` / `raw_payload` are excluded from the curated allowlist.

## Quality cohorts and limitations
Before metrics use winner population W; after metrics recompute on the curated candidate, including reference coverage (100 IDs) and agreement (±0.05 °C). Completeness improving after rejection does **not** mean missing values were recovered—see retained/rejected fractions in the quality reports. Sample agreement does not prove every real-world sensor is accurate.

Measured curated row count for run `885c06ee-6d42-49fd-a5d1-7db1fe77b2ce`: **9610**. Conservation and metric details: `quality_after`.

## Permitted use and access boundary
Allowed use is synthetic teaching only. Curator identities may read raw/staging and must not publish releases; publishers re-check and write release prefixes; analysts read approved release objects only. Metadata labels are not access controls. Do not redistribute restricted quarantine payloads or raw emails.

## Software and reproduction
DuckDB 1.4.5, Python 3.13.5. Reproduce locally after staging:

```text
python scripts/stage_inputs.py local fixture/trusted_manifest.json --output-dir work
python code/run_curation.py
python code/build_task4.py
```

Hashes binding this run: input_manifest `fe048451e073…`, contract `8d3ab69399fd…`, code `d5aaafb7fa5f…`. See `catalog.json` and `run-record.json` for full digests and locations.
