"""Run typed + curate + quality SQL and export candidate artifacts."""

from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'code'))
from helpers import (  # noqa: E402
    append_event,
    code_bundle_sha256,
    lineage_event,
    new_run,
    repo_rel,
    sha256,
)


def _status(value, threshold):
    if value is None:
        return 'NOT_EVALUATED'
    return 'PASS' if value >= threshold else 'FAIL'


def main():
    work = Path(os.environ.get('LAB2_WORK', ROOT / 'work')).resolve()
    run_id = os.environ.get('CURATE_RUN_ID') or new_run()
    store_id = os.environ.get('STORE_ID', 'bd-g06-objects')
    staging_key = f'lab2/staging/{run_id}'

    candidate = work / 'candidate'
    staging = work / 'staging'
    evidence = work / 'evidence'
    for d in (candidate, staging, evidence):
        d.mkdir(parents=True, exist_ok=True)

    # SQL expects envelopes.csv and snapshot/ relative to CWD.
    envelopes = work / 'envelopes.csv'
    if not envelopes.exists():
        raise SystemExit(f'Missing {envelopes}; run stage_inputs.py first')

    db_path = work / 'work.duckdb'
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    cwd = Path.cwd()
    try:
        os.chdir(work)
        for rel in ('sql/typed.sql', 'sql/task2_views.sql', 'sql/curate.sql', 'sql/quality.sql'):
            sql_path = ROOT / rel
            con.execute((sql_path).read_text(encoding='utf-8'))
    finally:
        os.chdir(cwd)

    curated_path = candidate / 'curated.parquet'
    quarantine_path = staging / 'quarantine.csv'
    duplicates_path = staging / 'duplicates.csv'

    con.execute(f"COPY curated TO '{curated_path.as_posix()}' (FORMAT PARQUET)")
    con.execute(
        f"COPY quarantine TO '{quarantine_path.as_posix()}' (HEADER, FORMAT CSV)"
    )
    con.execute(
        f"COPY duplicates TO '{duplicates_path.as_posix()}' (HEADER, FORMAT CSV)"
    )

    def rows_as_dicts(rel):
        cur = con.execute(f'SELECT * FROM {rel}')
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    inventory = rows_as_dicts('quality_inventory')[0]
    conservation = rows_as_dicts('conservation_check')[0]
    metrics_rows = rows_as_dicts('metric_after')

    contract_path = ROOT / 'contract.json'
    code_paths = [
        ROOT / 'sql' / 'typed.sql',
        ROOT / 'sql' / 'task2_views.sql',
        ROOT / 'sql' / 'curate.sql',
        ROOT / 'sql' / 'quality.sql',
        ROOT / 'code' / 'run_curation.py',
        ROOT / 'code' / 'helpers.py',
    ]
    contract_sha = sha256(contract_path)
    code_sha = code_bundle_sha256(code_paths)
    input_manifest_sha = sha256(ROOT / 'trusted_manifest.json')

    metrics = []
    for row in metrics_rows:
        n, d = int(row['numerator']), int(row['denominator'])
        value = None if d == 0 else n / d
        metrics.append({
            'rule_id': row['rule_id'],
            'dataset_id': 'curated-candidate',
            'run_id': run_id,
            'contract_version': 'research-telemetry-v1',
            'cohort': row['cohort'],
            'numerator': n,
            'denominator': d,
            'value': value,
            'status': _status(value, float(row['threshold'])),
            'threshold': float(row['threshold']),
            'severity': 'BLOCK',
        })

    quality_after = {
        'report_id': 'quality_after',
        'dataset_version': 'research-telemetry-v1',
        'as_of': '2026-02-09T00:00:00Z',
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'run_id': run_id,
        'store_id': store_id,
        'contract_sha256': contract_sha,
        'code_sha256': code_sha,
        'input_manifest_sha256': input_manifest_sha,
        'inventory': {k: int(v) for k, v in inventory.items()},
        'conservation': {
            'n_raw': int(conservation['n_raw']),
            'n_curated': int(conservation['n_curated']),
            'n_quarantine': int(conservation['n_quarantine']),
            'n_duplicates': int(conservation['n_duplicates']),
            'n_outputs': int(conservation['n_outputs']),
            'counts_match': bool(conservation['counts_match']),
            'no_lost_ids': bool(conservation['no_lost_ids']),
            'no_invented_ids': bool(conservation['no_invented_ids']),
            'curated_quarantine_disjoint': bool(conservation['curated_quarantine_disjoint']),
            'curated_duplicates_disjoint': bool(conservation['curated_duplicates_disjoint']),
            'quarantine_duplicates_disjoint': bool(conservation['quarantine_duplicates_disjoint']),
            'curated_business_unique': bool(conservation['curated_business_unique']),
        },
        'metrics': metrics,
        'artifact_hashes': {
            'curated.parquet': sha256(curated_path),
            'quarantine.csv': sha256(quarantine_path),
            'duplicates.csv': sha256(duplicates_path),
        },
        'locations': {
            'curated': f's3://research-raw/{staging_key}/curated.parquet',
            'quarantine': f's3://research-raw/{staging_key}/quarantine.csv',
            'duplicates': f's3://research-raw/{staging_key}/duplicates.csv',
            'local_curated': repo_rel(curated_path, ROOT),
            'local_quarantine': repo_rel(quarantine_path, ROOT),
            'local_duplicates': repo_rel(duplicates_path, ROOT),
            'envelopes': repo_rel(envelopes, ROOT),
        },
    }

    (evidence / 'quality_after.json').write_text(
        json.dumps(quality_after, indent=2), encoding='utf-8'
    )
    shutil.copy2(evidence / 'quality_after.json', ROOT / 'task2' / 'quality_after.json')

    lineage_path = evidence / 'lineage.jsonl'
    if lineage_path.exists():
        lineage_path.unlink()
    curate_inputs = [
        'research-raw/lab2/inputs/batch-01/observations_a.csv',
        'research-raw/lab2/inputs/batch-01/observations_b.jsonl',
        'research-raw/lab2/inputs/batch-01/sensors.csv',
        'contract.json',
    ]
    curate_outputs = [
        f'research-raw/{staging_key}/curated.parquet',
        f'research-raw/{staging_key}/quarantine.csv',
        f'research-raw/{staging_key}/duplicates.csv',
    ]
    start = lineage_event('START', run_id, 'curate', curate_inputs, [], store_id)
    complete = lineage_event(
        'COMPLETE', run_id, 'curate', curate_inputs, curate_outputs, store_id
    )
    append_event(lineage_path, start)
    append_event(lineage_path, complete)
    shutil.copy2(lineage_path, ROOT / 'lineage.jsonl')

    # Sample traces for L02/L03
    acc_cur = con.execute("""
        SELECT record_id, sensor_id, site, temperature_c, is_late,
               source_object, source_row, source_sha256
        FROM curated
        ORDER BY record_id
        LIMIT 1
    """)
    acc_cols = [d[0] for d in acc_cur.description]
    accepted = dict(zip(acc_cols, acc_cur.fetchone()))
    q_cur = con.execute("""
        SELECT source_object, source_row, source_sha256, record_id,
               primary_reason, all_reasons, stage
        FROM quarantine
        ORDER BY source_object, source_row
        LIMIT 1
    """)
    q_cols = [d[0] for d in q_cur.description]
    quarantined = dict(zip(q_cols, q_cur.fetchone()))
    traces = {
        'run_id': run_id,
        'L02_accepted_row': accepted,
        'L02_query': (
            "SELECT record_id, sensor_id, site, temperature_c, is_late, "
            "source_object, source_row, source_sha256 FROM curated "
            "ORDER BY record_id LIMIT 1"
        ),
        'L03_quarantined_row': quarantined,
        'L03_query': (
            "SELECT source_object, source_row, source_sha256, record_id, "
            "primary_reason, all_reasons, stage FROM quarantine "
            "ORDER BY source_object, source_row LIMIT 1"
        ),
    }
    (evidence / 'lineage_traces.json').write_text(
        json.dumps(traces, indent=2, default=str), encoding='utf-8'
    )

    run_record = {
        'run_id': run_id,
        'job': 'curate',
        'status': 'COMPLETE',
        'store_id': store_id,
        'as_of': '2026-02-09T00:00:00Z',
        'started_at_utc': start['eventTime'],
        'ended_at_utc': complete['eventTime'],
        'input_manifest_sha256': input_manifest_sha,
        'contract_sha256': contract_sha,
        'code_sha256': code_sha,
        'software': {
            'python': sys.version.split()[0],
            'duckdb': duckdb.__version__,
        },
        'outputs': {
            'curated.parquet': {
                'sha256': sha256(curated_path),
                'row_count': int(inventory['curated_rows']),
                'path': repo_rel(curated_path, ROOT),
            },
            'quarantine.csv': {
                'sha256': sha256(quarantine_path),
                'row_count': int(inventory['quarantine_rows']),
                'path': repo_rel(quarantine_path, ROOT),
            },
            'duplicates.csv': {
                'sha256': sha256(duplicates_path),
                'row_count': int(inventory['duplicate_rows']),
                'path': repo_rel(duplicates_path, ROOT),
            },
        },
        'operator': os.environ.get('LAB2_OPERATOR', 'Role-C-Curation'),
        'reviewer': os.environ.get('LAB2_REVIEWER', 'PENDING_INDEPENDENT_REVIEW'),
        'conservation_pass': bool(conservation['counts_match'])
        and bool(conservation['no_lost_ids'])
        and bool(conservation['no_invented_ids']),
    }
    (evidence / 'run-record.json').write_text(
        json.dumps(run_record, indent=2), encoding='utf-8'
    )
    shutil.copy2(evidence / 'run-record.json', ROOT / 'run-record.json')

    summary = {
        'run_id': run_id,
        'curated_rows': int(inventory['curated_rows']),
        'quarantine_rows': int(inventory['quarantine_rows']),
        'duplicate_rows': int(inventory['duplicate_rows']),
        'conservation_pass': run_record['conservation_pass'],
        'quality_after': str(evidence / 'quality_after.json'),
    }
    print(json.dumps(summary, indent=2))
    con.close()


if __name__ == '__main__':
    main()
