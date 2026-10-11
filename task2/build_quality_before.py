
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "work" / "task2.duckdb"
CONTRACT_FILE = ROOT / "contract.json"
OUTPUT = ROOT / "work" / "quality_before.json"

contract = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
contract_hash = hashlib.sha256(CONTRACT_FILE.read_bytes()).hexdigest()

con = duckdb.connect(str(DB), read_only=True)


def scalar(sql):
    return con.execute(sql).fetchone()[0]


def metric(rule_id, cohort, numerator, denominator, threshold,
           severity="high", description=""):
    if denominator == 0:
        value = None
        status = "NOT_EVALUATED"
    else:
        value = numerator / denominator
        status = "PASS" if value >= threshold else "FAIL"

    return {
        "rule_id": rule_id,
        "dataset_version": contract["schema_version"],
        "cohort": cohort,
        "description": description,
        "numerator": numerator,
        "denominator": denominator,
        "value": value,
        "threshold": threshold,
        "severity": severity,
        "status": status
    }


raw_count = scalar("SELECT COUNT(*) FROM raw_envelopes")
parse_failures = scalar("""
    SELECT COUNT(*) FROM typed
    WHERE parse_ok IS NOT TRUE
""")

key_rejected = scalar("""
    SELECT COUNT(*) FROM typed
    WHERE parse_ok IS TRUE
      AND (
        record_id IS NULL
        OR trim(record_id) = ''
        OR NOT regexp_matches(record_id, '^R[0-9]{6}$')
      )
""")

ingest_parse_failures = scalar("""
    SELECT COUNT(*) FROM typed
    WHERE parse_ok IS TRUE
      AND record_id IS NOT NULL
      AND trim(record_id) <> ''
      AND regexp_matches(record_id, '^R[0-9]{6}$')
      AND ingest_ts IS NULL
""")

eligible_count = scalar("SELECT COUNT(*) FROM key_eligible")
winner_count = scalar("SELECT COUNT(*) FROM winners")
distinct_eligible_ids = scalar("""
    SELECT COUNT(DISTINCT record_id) FROM key_eligible
""")
duplicate_excess = eligible_count - winner_count

# Six required fields must be present and nonblank.
required_ok = scalar("""
    SELECT COUNT(*)
    FROM winners
    WHERE record_id IS NOT NULL AND trim(record_id) <> ''
      AND sensor_id IS NOT NULL AND trim(sensor_id) <> ''
      AND event_time IS NOT NULL AND trim(event_time) <> ''
      AND ingest_time IS NOT NULL AND trim(ingest_time) <> ''
      AND reading IS NOT NULL AND trim(reading) <> ''
      AND unit IS NOT NULL AND trim(unit) <> ''
""")

# Validate finite numeric readings, supported units and Celsius range.
value_ok = scalar("""
    WITH converted AS (
      SELECT *,
        CASE
          WHEN unit = 'C' THEN value_num
          WHEN unit = 'F' THEN (value_num - 32.0) * 5.0 / 9.0
          ELSE NULL
        END AS temperature_c
      FROM winners
    )
    SELECT COUNT(*)
    FROM converted
    WHERE value_num IS NOT NULL
      AND isfinite(value_num)
      AND unit IN ('C', 'F')
      AND temperature_c IS NOT NULL
      AND isfinite(temperature_c)
      AND temperature_c >= -30
      AND temperature_c <= 60
""")

# A sensor must exist in the supplied registry.
sensor_ok = scalar("""
    SELECT COUNT(*)
    FROM winners w
    WHERE EXISTS (
      SELECT 1
      FROM registry r
      WHERE upper(trim(r.sensor_id)) = w.sensor_id
    )
""")

# Chronology: event_time <= ingest_time <= fixed AS_OF.
as_of = contract["as_of"].replace("T", " ").replace("Z", "")
chronology_ok = scalar(f"""
    SELECT COUNT(*)
    FROM winners
    WHERE event_ts IS NOT NULL
      AND ingest_ts IS NOT NULL
      AND event_ts <= ingest_ts
      AND ingest_ts <= TIMESTAMP '{as_of}'
""")

chronology_population = scalar(f"""
    SELECT COUNT(*)
    FROM winners
    WHERE event_ts IS NOT NULL
      AND ingest_ts IS NOT NULL
      AND event_ts <= ingest_ts
      AND ingest_ts <= TIMESTAMP '{as_of}'
""")

# Timely rows are the chronologically valid rows with lag <= 900 seconds.
timely = scalar(f"""
    SELECT COUNT(*)
    FROM winners
    WHERE event_ts IS NOT NULL
      AND ingest_ts IS NOT NULL
      AND event_ts <= ingest_ts
      AND ingest_ts <= TIMESTAMP '{as_of}'
      AND date_diff('second', event_ts, ingest_ts) <= 900
""")
thresholds = contract["release_thresholds"]
metrics = [
    metric(
        "Q_REQUIRED", "W: deterministic winners before validity filtering",
        required_ok, winner_count,
        thresholds["required_completeness"],
        description="All six required source fields are nonblank."
    ),
    metric(
        "Q_KEY_UNIQUENESS",
        "Key-eligible rows before deduplication",
        distinct_eligible_ids, eligible_count,
        thresholds["business_key_uniqueness"],
        description="Distinct normalized record IDs divided by eligible rows."
    ),
    metric(
        "Q_VALUE", "W: deterministic winners before validity filtering",
        value_ok, winner_count,
        thresholds["value_validity"],
        description="Finite numeric reading, supported unit and Celsius range."
    ),
    metric(
        "Q_SENSOR", "W: deterministic winners before validity filtering",
        sensor_ok, winner_count,
        thresholds["sensor_consistency"],
        description="Normalized sensor ID exists in the sensor registry."
    ),
    metric(
        "Q_CHRONOLOGY", "W: deterministic winners before validity filtering",
        chronology_ok, winner_count,
        thresholds["chronology"],
        description="Parseable timestamps satisfying event <= ingest <= AS_OF."
    ),
    metric(
        "Q_TIMELINESS",
        "Chronologically valid rows in W",
        timely, chronology_population,
        thresholds["timeliness"],
        description="Ingestion lag is at most the contract threshold of 900 seconds."
    )
]

# Collect a few safe-to-share examples for defect investigation.
# Restricted raw_payload and operator_email are deliberately excluded.
def examples(where_clause):
    sql = f"""
        SELECT source_object, source_row, record_id, sensor_id,
               event_time, ingest_time, reading, unit
        FROM typed
        WHERE {where_clause}
        ORDER BY source_object, source_row
        LIMIT 3
    """
    return con.execute(sql).df().to_dict(orient="records")


report = {
    "report_id": "quality_before",
    "dataset_version": contract["schema_version"],
    "as_of": contract["as_of"],
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "contract_sha256": contract_hash,
    "scope": (
        "Before curation. W is the deterministic winner set before "
        "row-validity filtering. Key uniqueness uses key-eligible rows "
        "before deduplication."
    ),
    "inventory": {
        "raw_physical_rows": raw_count,
        "parse_failures": parse_failures,
        "key_rejected_rows": key_rejected,
        "ingest_parse_failures_after_key_check": ingest_parse_failures,
        "key_eligible_rows": eligible_count,
        "winner_rows_W": winner_count,
        "distinct_eligible_ids": distinct_eligible_ids,
        "duplicate_excess": duplicate_excess
    },
    "metrics": metrics,
    "defect_investigation": {
        "parse_failures": examples("parse_ok IS NOT TRUE"),
        "missing_or_invalid_id": examples("""
            parse_ok IS TRUE AND (
              record_id IS NULL OR trim(record_id) = ''
              OR NOT regexp_matches(record_id, '^R[0-9]{6}$')
            )
        """),
        "invalid_ingest_time": examples("""
            parse_ok IS TRUE
            AND record_id IS NOT NULL
            AND regexp_matches(record_id, '^R[0-9]{6}$')
            AND ingest_ts IS NULL
        """),
                "invalid_reading": examples("""
            parse_ok IS TRUE
            AND record_id IS NOT NULL
            AND regexp_matches(record_id, '^R[0-9]{6}$')
            AND ingest_ts IS NOT NULL
            AND (
              value_num IS NULL
              OR NOT isfinite(value_num)
              OR unit IS NULL
              OR unit NOT IN ('C', 'F')
              OR (
                CASE
                  WHEN unit = 'C' THEN value_num
                  WHEN unit = 'F' THEN (value_num - 32.0) * 5.0 / 9.0
                  ELSE NULL
                END
              ) IS NULL
              OR NOT isfinite(
                CASE
                  WHEN unit = 'C' THEN value_num
                  WHEN unit = 'F' THEN (value_num - 32.0) * 5.0 / 9.0
                  ELSE NULL
                END
              )
              OR (
                CASE
                  WHEN unit = 'C' THEN value_num
                  WHEN unit = 'F' THEN (value_num - 32.0) * 5.0 / 9.0
                  ELSE NULL
                END
              ) < -30
              OR (
                CASE
                  WHEN unit = 'C' THEN value_num
                  WHEN unit = 'F' THEN (value_num - 32.0) * 5.0 / 9.0
                  ELSE NULL
                END
              ) > 60
            )
        """)
    },
    "limitations": [
        "This is the before-quality checkpoint; it is not release approval.",
        "Reference agreement and coverage must be evaluated on the curated candidate in Task 3.",
        "After-quality metrics must be recomputed after curation.",
        "A zero denominator is reported as NOT_EVALUATED, not 100%."
    ]
}

OUTPUT.write_text(
    json.dumps(report, ensure_ascii=False, indent=2, default=str),
    encoding="utf-8"
)

con.close()

print("REPORT:", OUTPUT)
print("RAW:", raw_count)
print("PARSE_FAILURES:", parse_failures)
print("KEY_REJECTED:", key_rejected)
print("INGEST_PARSE_FAILURES:", ingest_parse_failures)
print("KEY_ELIGIBLE:", eligible_count)
print("WINNERS_W:", winner_count)
print("DUPLICATE_EXCESS:", duplicate_excess)
print("\n=== METRICS ===")
for item in metrics:
    print(
        item["rule_id"],
        f'{item["numerator"]}/{item["denominator"]}',
        "value =", item["value"],
        "threshold =", item["threshold"],
        "status =", item["status"]
    )
