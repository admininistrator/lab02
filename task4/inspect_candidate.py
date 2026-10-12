"""Measure the Task 4 curated Parquet schema, row count, nulls and SHA-256.

Run in an environment with DuckDB installed:
  python task4/inspect_candidate.py work/candidate/curated.parquet
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb


EXPECTED_SCHEMA = [
    ('record_id', 'VARCHAR'),
    ('sensor_id', 'VARCHAR'),
    ('site', 'VARCHAR'),
    ('event_time_utc', 'TIMESTAMP'),
    ('ingest_time_utc', 'TIMESTAMP'),
    ('temperature_c', 'DECIMAL(8,2)'),
    ('is_late', 'BOOLEAN'),
    ('source_object', 'VARCHAR'),
    ('source_row', 'BIGINT'),
    ('source_sha256', 'VARCHAR'),
]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python task4/inspect_candidate.py PATH_TO_PARQUET')
    path = Path(sys.argv[1]).resolve()
    if not path.is_file():
        raise SystemExit(f'Parquet file not found: {path}')
    sql_path = path.as_posix().replace("'", "''")
    con = duckdb.connect(':memory:')
    actual = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{sql_path}')"
    ).fetchall()
    schema = [(row[0], row[1].upper()) for row in actual]
    row_count, distinct_ids = con.execute(
        f"SELECT count(*), count(DISTINCT record_id) "
        f"FROM read_parquet('{sql_path}')"
    ).fetchone()
    required = [column for column, _ in EXPECTED_SCHEMA]
    null_counts = con.execute(
        f"SELECT {', '.join(f'count(*) FILTER (WHERE {name} IS NULL) AS {name}' for name in required)} "
        f"FROM read_parquet('{sql_path}')"
    ).fetchone()
    con.close()
    nulls = dict(zip(required, null_counts))
    report = {
        'artifact': str(path),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'schema': [{'name': name, 'type': typ} for name, typ in schema],
        'schema_matches_allowlist_and_types': schema == EXPECTED_SCHEMA,
        'row_count': row_count,
        'expected_row_count': 9610,
        'row_count_matches_run_record': row_count == 9610,
        'distinct_record_ids': distinct_ids,
        'business_key_unique': distinct_ids == row_count,
        'required_null_counts': nulls,
        'required_values_present': all(count == 0 for count in nulls.values()),
        'restricted_columns_present': any(
            name.lower() in {'operator_email', 'raw_payload'} for name, _ in schema
        ),
    }
    report['status'] = 'PASS' if (
        report['schema_matches_allowlist_and_types']
        and report['row_count_matches_run_record']
        and report['business_key_unique']
        and report['required_values_present']
        and not report['restricted_columns_present']
    ) else 'FAIL'
    print(json.dumps(report, indent=2))
    if report['status'] != 'PASS':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
