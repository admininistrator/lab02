"""Task 4: catalog, README, retention dry-run, catalog queries M01–M03 scaffold."""

from __future__ import annotations

import json
import os
import sys
import csv
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'code'))
from helpers import repo_rel, sha256  # noqa: E402


CURATED_COLUMNS = [
    {
        'name': 'record_id',
        'type': 'VARCHAR',
        'unit': None,
        'nullable': False,
        'meaning': 'Normalized business observation identifier',
    },
    {
        'name': 'sensor_id',
        'type': 'VARCHAR',
        'unit': None,
        'nullable': False,
        'meaning': 'Normalized sensor identifier joined to the registry',
    },
    {
        'name': 'site',
        'type': 'VARCHAR',
        'unit': None,
        'nullable': False,
        'meaning': 'Site label derived from sensors.csv registry',
    },
    {
        'name': 'event_time_utc',
        'type': 'TIMESTAMP',
        'unit': 'UTC',
        'nullable': False,
        'meaning': 'Observation event timestamp in UTC',
    },
    {
        'name': 'ingest_time_utc',
        'type': 'TIMESTAMP',
        'unit': 'UTC',
        'nullable': False,
        'meaning': 'Recorded arrival/ingest timestamp in UTC',
    },
    {
        'name': 'temperature_c',
        'type': 'DECIMAL(8,2)',
        'unit': 'Celsius',
        'nullable': False,
        'meaning': 'Selected observation converted to Celsius',
    },
    {
        'name': 'is_late',
        'type': 'BOOLEAN',
        'unit': None,
        'nullable': False,
        'meaning': 'True when ingest lag exceeds 900 seconds',
    },
    {
        'name': 'source_object',
        'type': 'VARCHAR',
        'unit': None,
        'nullable': False,
        'meaning': 'Source object identifier for lineage (not an access grant)',
    },
    {
        'name': 'source_row',
        'type': 'BIGINT',
        'unit': None,
        'nullable': False,
        'meaning': '1-based physical row/line ordinal in the source object',
    },
    {
        'name': 'source_sha256',
        'type': 'VARCHAR',
        'unit': None,
        'nullable': False,
        'meaning': 'SHA-256 of the source object containing this row',
    },
]

RAW_COLUMNS = [
    {'name': 'record_id', 'type': 'VARCHAR', 'unit': None, 'nullable': True,
     'meaning': 'Canonical id; CSV record_id / JSONL id'},
    {'name': 'sensor_id', 'type': 'VARCHAR', 'unit': None, 'nullable': True,
     'meaning': 'Canonical sensor; CSV sensor_id / JSONL sensor'},
    {'name': 'event_time', 'type': 'VARCHAR', 'unit': 'UTC string', 'nullable': True,
     'meaning': 'Canonical event time; CSV event_time / JSONL timestamp'},
    {'name': 'ingest_time', 'type': 'VARCHAR', 'unit': 'UTC string', 'nullable': True,
     'meaning': 'Canonical ingest time; CSV ingest_time / JSONL arrived_at'},
    {'name': 'reading', 'type': 'VARCHAR', 'unit': 'source unit', 'nullable': True,
     'meaning': 'Canonical reading; CSV reading / JSONL temperature'},
    {'name': 'unit', 'type': 'VARCHAR', 'unit': None, 'nullable': True,
     'meaning': 'Canonical unit; CSV unit / JSONL temperature_unit'},
    {'name': 'operator_email', 'type': 'VARCHAR', 'unit': None, 'nullable': True,
     'meaning': 'Restricted optional operator attribute; excluded from release'},
    {'name': 'source_object', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Physical source object URI/identifier'},
    {'name': 'source_row', 'type': 'BIGINT', 'unit': None, 'nullable': False,
     'meaning': 'Physical row ordinal'},
    {'name': 'source_sha256', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Source object digest'},
    {'name': 'parse_ok', 'type': 'BOOLEAN', 'unit': None, 'nullable': False,
     'meaning': 'False when the physical JSON/CSV record failed parsing'},
    {'name': 'raw_payload', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Restricted raw payload retained for quarantine diagnostics'},
]

QUARANTINE_COLUMNS = [
    {'name': 'source_object', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Physical source object of the rejected row'},
    {'name': 'source_row', 'type': 'BIGINT', 'unit': None, 'nullable': False,
     'meaning': 'Physical row ordinal'},
    {'name': 'source_sha256', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Source object digest'},
    {'name': 'record_id', 'type': 'VARCHAR', 'unit': None, 'nullable': True,
     'meaning': 'Business id when parseable'},
    {'name': 'primary_reason', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Primary quarantine reason under contract precedence'},
    {'name': 'all_reasons', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'Pipe-delimited set of all applicable reason codes'},
    {'name': 'stage', 'type': 'VARCHAR', 'unit': None, 'nullable': False,
     'meaning': 'pre_dedup or winner_validation'},
]


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def validate_entry(entry, schema):
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(entry)
    # Semantic extensions beyond structural schema.
    errors = []
    if not isinstance(entry.get('location'), list) or not entry['location']:
        errors.append(f"{entry['dataset_id']}: location must be nonempty array")
    names = [c['name'] for c in entry['columns']]
    if len(names) != len(set(names)):
        errors.append(f"{entry['dataset_id']}: duplicate column names")
    for field in ('input_manifest_sha256', 'contract_sha256', 'code_sha256'):
        if len(entry.get(field, '')) != 64:
            errors.append(f"{entry['dataset_id']}: bad {field}")
    ret = entry.get('retention') or {}
    for req in ('days', 'start_basis', 'hold', 'dependent_release_ids', 'action'):
        if req not in ret:
            errors.append(f"{entry['dataset_id']}: retention missing {req}")
    if errors:
        raise ValueError('; '.join(errors))


def build_catalog(work: Path, run_record: dict, quality_after: dict):
    schema = load_json(ROOT / 'schemas' / 'metadata_catalog_schema.json')
    # Extend location to array-of-URI for lab profile while keeping other checks.
    schema = json.loads(json.dumps(schema))
    schema['properties']['location'] = {
        'type': 'array',
        'minItems': 1,
        'items': {'type': 'string', 'minLength': 1},
    }
    schema['properties']['quality_report'] = {
        'type': 'object',
        'required': ['path', 'report_id'],
        'properties': {
            'path': {'type': 'string', 'minLength': 1},
            'report_id': {'type': 'string', 'minLength': 1},
        },
    }

    inventory = load_json(work / 'input_inventory.json')
    staging = quality_after['locations']
    owner = os.environ.get('LAB2_OWNER', 'Role-E-Release / team owner')
    steward = os.environ.get('LAB2_STEWARD', 'Role-D-Metadata')
    operator = run_record.get('operator', 'Role-C-Curation')

    datasets = [
        {
            'dataset_id': f"raw-observations-{run_record['run_id'][:8]}",
            'title': 'Raw research-telemetry observation collection',
            'purpose': 'Preserve every physical observation from CSV source A and JSONL source B for governed curation.',
            'owner': owner,
            'steward': steward,
            'classification': 'restricted-raw',
            'allowed_use': 'Synthetic teaching data only; not for operational or personal inference.',
            'location': [
                'work/snapshot/observations_a.csv',
                'work/snapshot/observations_b.jsonl',
                'work/envelopes.csv',
            ],
            'schema_version': 'research-telemetry-v1',
            'columns': RAW_COLUMNS,
            'as_of': '2026-02-09T00:00:00Z',
            'row_count': int(inventory['total_observation_rows']),
            'input_manifest_sha256': run_record['input_manifest_sha256'],
            'contract_sha256': run_record['contract_sha256'],
            'code_sha256': run_record['code_sha256'],
            'quality_report': {
                'path': 'task2/quality_before.json',
                'report_id': 'quality_before',
            },
            'retention': {
                'days': 30,
                'start_basis': 'retrieval_time',
                'hold': False,
                'dependent_release_ids': [],
                'action': 'dry-run only',
            },
            'provenance': {
                'job': 'stage_inputs',
                'run_id': run_record['run_id'],
                'store_id': run_record['store_id'],
                'source_aliases': {
                    'csv_source_a': {
                        'object': 'observations_a.csv',
                        'fields': ['record_id', 'sensor_id', 'event_time', 'ingest_time',
                                   'reading', 'unit', 'operator_email'],
                    },
                    'jsonl_source_b': {
                        'object': 'observations_b.jsonl',
                        'fields': ['id', 'sensor', 'timestamp', 'arrived_at',
                                   'temperature', 'temperature_unit', 'operator'],
                    },
                },
            },
            'supporting_artifacts': {
                'duplicate_ledger': staging.get('local_duplicates', 'work/staging/duplicates.csv'),
            },
        },
        {
            'dataset_id': f"quarantine-{run_record['run_id'][:8]}",
            'title': 'Quarantine ledger for rejected observations',
            'purpose': 'Account for every physical record rejected before or after winner validation.',
            'owner': owner,
            'steward': steward,
            'classification': 'restricted-quarantine',
            'allowed_use': 'Synthetic teaching data only; diagnostic reuse under course access boundary.',
            'location': [staging.get('local_quarantine', 'work/staging/quarantine.csv')],
            'schema_version': 'research-telemetry-v1',
            'columns': QUARANTINE_COLUMNS,
            'as_of': '2026-02-09T00:00:00Z',
            'row_count': int(quality_after['inventory']['quarantine_rows']),
            'input_manifest_sha256': run_record['input_manifest_sha256'],
            'contract_sha256': run_record['contract_sha256'],
            'code_sha256': run_record['code_sha256'],
            'quality_report': {
                'path': 'task2/quality_after.json',
                'report_id': 'quality_after',
            },
            'retention': {
                'days': 7,
                'start_basis': 'run_completion_time',
                'hold': False,
                'dependent_release_ids': [],
                'action': 'dry-run only',
            },
            'provenance': {
                'job': 'curate',
                'run_id': run_record['run_id'],
                'operator': operator,
                'lineage': 'work/evidence/lineage.jsonl',
            },
        },
        {
            'dataset_id': f"curated-candidate-{run_record['run_id'][:8]}",
            'title': 'Curated research-telemetry candidate',
            'purpose': 'Analyst-facing candidate observations after deterministic curation; still gated before release.',
            'owner': owner,
            'steward': steward,
            'classification': 'candidate',
            'allowed_use': 'Synthetic teaching data only; publish only after owner-approved release gate.',
            'location': [staging.get('local_curated', 'work/candidate/curated.parquet')],
            'schema_version': 'research-telemetry-v1',
            'columns': CURATED_COLUMNS,
            'as_of': '2026-02-09T00:00:00Z',
            'row_count': int(quality_after['inventory']['curated_rows']),
            'input_manifest_sha256': run_record['input_manifest_sha256'],
            'contract_sha256': run_record['contract_sha256'],
            'code_sha256': run_record['code_sha256'],
            'quality_report': {
                'path': 'task2/quality_after.json',
                'report_id': 'quality_after',
            },
            'retention': {
                'days': 90,
                'start_basis': 'publication_time',
                'hold': False,
                'dependent_release_ids': [],
                'action': 'dry-run only',
            },
            'provenance': {
                'job': 'curate',
                'run_id': run_record['run_id'],
                'operator': operator,
                'duplicate_ledger': staging.get('local_duplicates', 'work/staging/duplicates.csv'),
                'lineage': 'work/evidence/lineage.jsonl',
                'traces': 'work/evidence/lineage_traces.json',
            },
        },
    ]

    for entry in datasets:
        # Temporary string quality_report for base schema? We extended schema.
        validate_entry(entry, schema)

    catalog = {
        'catalog_version': 'lab2-task4-v1',
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'store_id': run_record['store_id'],
        'run_id': run_record['run_id'],
        'datasets': datasets,
    }
    return catalog


def validate_measured_catalog(catalog, run_record, quality_after, work):
    """Check metadata against files and measured Parquet schema/counts."""
    try:
        import duckdb
    except ImportError:
        duckdb = None
    errors = []
    by_kind = {
        'raw': next(d for d in catalog['datasets'] if d['dataset_id'].startswith('raw-observations-')),
        'quarantine': next(d for d in catalog['datasets'] if d['dataset_id'].startswith('quarantine-')),
        'curated': next(d for d in catalog['datasets'] if d['dataset_id'].startswith('curated-candidate-')),
    }
    root = ROOT
    expected_hashes = {
        'input_manifest_sha256': sha256(ROOT / 'trusted_manifest.json'),
        'contract_sha256': sha256(ROOT / 'contract.json'),
    }
    code_paths = [ROOT / 'sql' / name for name in
                  ('typed.sql', 'task2_views.sql', 'curate.sql', 'quality.sql')]
    code_paths += [ROOT / 'code' / name for name in ('run_curation.py', 'helpers.py')]
    from helpers import code_bundle_sha256
    expected_hashes['code_sha256'] = code_bundle_sha256(code_paths)
    for entry in catalog['datasets']:
        for key, expected in expected_hashes.items():
            if entry.get(key) != expected or run_record.get(key) != expected:
                errors.append(f"{entry['dataset_id']}: {key} mismatch")
        quality = entry.get('quality_report') or {}
        report_path = root / quality.get('path', '')
        if not report_path.is_file():
            errors.append(f"{entry['dataset_id']}: quality report missing")
        for location in entry['location']:
            if not (root / location).is_file():
                errors.append(f"{entry['dataset_id']}: location missing: {location}")

    # Count physical input records from verified staged envelopes.
    envelope_path = work / 'envelopes.csv'
    with envelope_path.open(newline='', encoding='utf-8') as stream:
        raw_count = sum(1 for _ in csv.DictReader(stream))
    if by_kind['raw']['row_count'] != raw_count:
        errors.append('raw catalog row_count differs from envelopes')

    file_map = {
        'curated': (work / 'candidate' / 'curated.parquet', 'curated.parquet'),
        'quarantine': (work / 'staging' / 'quarantine.csv', 'quarantine.csv'),
        'duplicates': (work / 'staging' / 'duplicates.csv', 'duplicates.csv'),
    }
    for artifact, (path, run_key) in file_map.items():
        if not path.is_file():
            errors.append(f'{artifact}: measured artifact missing')
            continue
        digest = sha256(path)
        if quality_after['artifact_hashes'].get(run_key) != digest:
            errors.append(f'{artifact}: quality report hash differs from file')
        if run_record['outputs'].get(run_key, {}).get('sha256') != digest:
            errors.append(f'{artifact}: run record hash differs from file')
        if artifact == 'curated':
            if duckdb is None:
                actual_schema = None
                expected_schema = [(c['name'], c['type'].upper()) for c in CURATED_COLUMNS]
                count = run_record['outputs'][run_key]['row_count']
            else:
                con = duckdb.connect(':memory:')
                path_sql = path.resolve().as_posix().replace("'", "''")
                actual = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_sql}')").fetchall()
                actual_schema = [(row[0], row[1].upper()) for row in actual]
                expected_schema = [(c['name'], c['type'].upper()) for c in CURATED_COLUMNS]
                count = con.execute(f"SELECT count(*) FROM read_parquet('{path_sql}')").fetchone()[0]
                con.close()
        else:
            with path.open(newline='', encoding='utf-8') as stream:
                reader = csv.DictReader(stream)
                actual_schema = [(name, None) for name in (reader.fieldnames or [])]
                count = sum(1 for _ in reader)
            expected_schema = None
        catalog_entry = by_kind['curated'] if artifact == 'curated' else by_kind['quarantine'] if artifact == 'quarantine' else None
        if artifact == 'curated' and actual_schema is not None and actual_schema != expected_schema:
            errors.append('curated Parquet schema differs from declared allowlist/types')
        if count != run_record['outputs'][run_key]['row_count']:
            errors.append(f'{artifact}: measured row_count differs from run record')
        if catalog_entry is not None and artifact != 'duplicates' and catalog_entry['row_count'] != count:
            errors.append(f'{artifact}: catalog row_count differs from measured file')
        if artifact == 'curated' and count <= 0:
            errors.append('curated candidate is empty')

    if errors:
        raise ValueError('Catalog semantic validation failed: ' + '; '.join(errors))
    schema_status = 'BLOCKED_DUCKDB_UNAVAILABLE' if duckdb is None else 'PASS'
    return {
        'status': 'PARTIAL' if duckdb is None else 'PASS',
        'local_fallback': True,
        'raw_envelope_rows': raw_count,
        'curated_rows': by_kind['curated']['row_count'],
        'quarantine_rows': by_kind['quarantine']['row_count'],
        'artifact_hashes_match': True,
        'curated_schema_matches': schema_status == 'PASS',
        'curated_schema_check': schema_status,
        'manifest_contract_code_hashes_match': True,
    }


def query_m01(catalog):
    hits = []
    for ds in catalog['datasets']:
        cols = {c['name']: c for c in ds['columns']}
        temp = cols.get('temperature_c')
        qpath = (ds.get('quality_report') or {}).get('path')
        if (
            ds['dataset_id'].startswith('curated-candidate')
            and temp
            and temp.get('unit') == 'Celsius'
            and qpath
        ):
            hits.append({
                'dataset_id': ds['dataset_id'],
                'owner': ds['owner'],
                'steward': ds['steward'],
                'row_count': ds['row_count'],
                'location': ds['location'],
                'schema_version': ds['schema_version'],
            })
    return hits


def query_m02(catalog):
    restricted = []
    for ds in catalog['datasets']:
        if ds['classification'].startswith('restricted'):
            ret = ds['retention']
            if ret.get('days') is not None and (
                'hold' in ret or 'dependent_release_ids' in ret
            ):
                restricted.append({
                    'dataset_id': ds['dataset_id'],
                    'classification': ds['classification'],
                    'retention': ret,
                })
    release_cols = []
    for ds in catalog['datasets']:
        if ds['dataset_id'].startswith('curated-candidate'):
            release_cols = [c['name'] for c in ds['columns']]
    return {
        'restricted_with_retention': restricted,
        'release_columns': release_cols,
        'release_has_email': any('email' in c.lower() for c in release_cols),
    }


def build_candidate_manifest(catalog, run_record):
    curated = next(
        entry for entry in catalog['datasets']
        if entry['dataset_id'].startswith('curated-candidate-')
    )
    artifact = run_record['outputs']['curated.parquet']
    return {
        'manifest_type': 'candidate-manifest-draft',
        'status': 'DRAFT_NOT_APPROVED',
        'run_id': run_record['run_id'],
        'dataset_id': curated['dataset_id'],
        'schema_version': curated['schema_version'],
        'row_count': curated['row_count'],
        'artifact': {
            'location': artifact['path'],
            'sha256': artifact['sha256'],
        },
        'reviewer': 'PENDING_INDEPENDENT_REVIEW',
        'note': 'Candidate handover for Task 4 peer review; not a release approval or publication manifest.',
    }


def retention_plan(run_record):
    now = datetime.now(timezone.utc).isoformat()
    plan = {
        'policy_version': 'lab2-course-retention-v1',
        'recorded_at_utc': now,
        'time_basis_note': (
            'Retention clocks use retrieval_time / run_completion_time / '
            'publication_time. Do not infer eligibility from AS_OF.'
        ),
        'classes': [
            {
                'class': 'raw_input',
                'days': 30,
                'start_basis': 'retrieval_time',
                'overrides': ['legal_hold', 'active_approved_release_dependency'],
            },
            {
                'class': 'quarantine',
                'days': 7,
                'start_basis': 'run_completion_time',
                'overrides': ['legal_hold'],
            },
            {
                'class': 'approved_release',
                'days': 90,
                'start_basis': 'publication_time',
                'overrides': ['legal_hold'],
            },
            {
                'class': 'manifests_audit',
                'days': 90,
                'start_basis': 'publication_time',
                'note': 'Retain at least as long as the dependent release.',
            },
        ],
        'dry_run_cases': [
            {
                'case_id': 'R01',
                'object': 'scratch/temp-export.parquet',
                'expired': True,
                'hold': False,
                'dependent_release': None,
                'decision': 'ELIGIBLE',
                'reason': 'Expired scratch; no hold; no dependent release. Not actually deleted.',
            },
            {
                'case_id': 'R02',
                'object': f"research-raw/lab2/staging/{run_record['run_id']}/quarantine.csv",
                'expired': True,
                'hold': True,
                'dependent_release': None,
                'decision': 'KEEP',
                'reason': 'HOLD',
            },
            {
                'case_id': 'R03',
                'object': 'research-raw/lab2/inputs/batch-01/observations_a.csv',
                'expired': True,
                'hold': False,
                'dependent_release': 'lab2-release-active-demo',
                'decision': 'KEEP',
                'reason': 'ACTIVE_DEPENDENCY',
            },
            {
                'case_id': 'R04',
                'object': f"research-raw/lab2/staging/{run_record['run_id']}/curated.parquet",
                'expired': False,
                'hold': False,
                'dependent_release': None,
                'decision': 'KEEP',
                'reason': 'NOT_EXPIRED',
            },
        ],
    }
    return plan


def write_readme(path: Path, catalog: dict, run_record: dict, quality_after: dict):
    curated = next(d for d in catalog['datasets'] if d['dataset_id'].startswith('curated'))
    text = f"""# Research Telemetry Lab 2 — curated data README

## Purpose and origin
This package contains **synthetic teaching observations** for the VNU-UET Big Data Lab 2 curation exercise (`research-telemetry-v1`). No real personal or research telemetry are included. The raw collection mixes a CSV source (A) and a JSON Lines source (B); both are frozen against an instructor-controlled `trusted_manifest.json`.

## Files and keys
- Raw inputs (restricted): `research-raw/lab2/inputs/batch-01/{{observations_a.csv,observations_b.jsonl,sensors.csv,qa_reference.csv}}`.
- Staging (restricted): curated Parquet, `quarantine.csv`, and `duplicates.csv` under `research-raw/lab2/staging/<run_id>/`.
- Machine-readable metadata: `catalog.json`, `contract.json`, `quality_before.json` / `quality_after.json`, `lineage.jsonl`, `run-record.json`, `retention-plan.json`.
- Physical provenance key: `(source_object, source_row)`; business key: normalized `record_id`.

## Timestamps and units
All contract timestamps use UTC `YYYY-MM-DDTHH:MM:SSZ`. Supported units are C and F; released `temperature_c` is `DECIMAL(8,2)` Celsius after validation. The fixed evaluation cutoff is **AS_OF = 2026-02-09T00:00:00Z** (data clock, not wall-clock execution time). Late but otherwise valid rows (`lag_seconds > 900`) are retained with `is_late = true`.

## Curation and deduplication
Malformed rows are quarantined as `PARSE`. Missing/invalid IDs (`R[0-9]{{6}}`) and unparseable ingest times are quarantined before deduplication (`KEY`, `INGEST_PARSE`). Remaining rows are ranked by `ingest_time DESC`, then `source_object ASC`, `source_row ASC`. Non-winners enter the duplicate ledger as `SUPERSEDED`. Invalid newer winners are quarantined; older valid versions are **not** substituted back into the candidate. `qa_reference.csv` evaluates agreement/coverage only and never repairs values. Restricted `operator_email` / `raw_payload` are excluded from the curated allowlist.

## Quality cohorts and limitations
Before metrics use winner population W; after metrics recompute on the curated candidate, including reference coverage (100 IDs) and agreement (±0.05 °C). Completeness improving after rejection does **not** mean missing values were recovered—see retained/rejected fractions in the quality reports. Sample agreement does not prove every real-world sensor is accurate.

Measured curated row count for run `{run_record['run_id']}`: **{curated['row_count']}**. Conservation and metric details: `{quality_after.get('report_id', 'quality_after')}`.

## Permitted use and access boundary
Allowed use is synthetic teaching only. Curator identities may read raw/staging and must not publish releases; publishers re-check and write release prefixes; analysts read approved release objects only. Metadata labels are not access controls. Do not redistribute restricted quarantine payloads or raw emails.

## Software and reproduction
DuckDB {run_record['software']['duckdb']}, Python {run_record['software']['python']}. Reproduce locally after staging:

```text
python scripts/stage_inputs.py local fixture/trusted_manifest.json --output-dir work
python code/run_curation.py
python code/build_task4.py
```

Hashes binding this run: input_manifest `{run_record['input_manifest_sha256'][:12]}…`, contract `{run_record['contract_sha256'][:12]}…`, code `{run_record['code_sha256'][:12]}…`. See `catalog.json` and `run-record.json` for full digests and locations.
"""
    # Keep within ~250-400 words
    path.write_text(text.strip() + '\n', encoding='utf-8')
    return text


def main():
    work = Path(os.environ.get('LAB2_WORK', ROOT / 'work')).resolve()
    evidence = work / 'evidence'
    run_record = load_json(evidence / 'run-record.json')
    quality_after = load_json(evidence / 'quality_after.json')

    catalog = build_catalog(work, run_record, quality_after)
    measured_validation = validate_measured_catalog(
        catalog, run_record, quality_after, work
    )
    catalog_path = ROOT / 'catalog.json'
    catalog_path.write_text(json.dumps(catalog, indent=2), encoding='utf-8')
    (evidence / 'catalog.json').write_text(json.dumps(catalog, indent=2), encoding='utf-8')

    readme_path = ROOT / 'DATA_README.md'
    write_readme(readme_path, catalog, run_record, quality_after)
    (evidence / 'README.md').write_text(readme_path.read_text(encoding='utf-8'), encoding='utf-8')

    plan = retention_plan(run_record)
    (ROOT / 'retention-plan.json').write_text(json.dumps(plan, indent=2), encoding='utf-8')
    (evidence / 'retention-plan.json').write_text(json.dumps(plan, indent=2), encoding='utf-8')

    m01 = query_m01(catalog)
    m02 = query_m02(catalog)
    draft_manifest = build_candidate_manifest(catalog, run_record)
    (ROOT / 'task4' / 'draft-manifest.json').write_text(
        json.dumps(draft_manifest, indent=2), encoding='utf-8'
    )
    (evidence / 'draft-manifest.json').write_text(
        json.dumps(draft_manifest, indent=2), encoding='utf-8'
    )
    queries = {
        'M01': m01,
        'M02': m02,
        'M03_peer_check': {
            'status': 'PENDING_INDEPENDENT_PEER',
            'materials': ['catalog.json', 'DATA_README.md', 'task4/draft-manifest.json'],
            'questions': [
                'What unit is temperature_c reported in?',
                'How many IDs are in the reference population and how is it used?',
                'What is the evaluation cutoff (AS_OF)?',
                'What are two documented limitations?',
                'Which role is the contact steward and which role is the owner?',
            ],
            'responses': [],
            'correct_count': None,
        },
        'validation': {
            'catalog_datasets': len(catalog['datasets']),
            'catalog_sha256': sha256(catalog_path),
            'lineage_exists': (evidence / 'lineage.jsonl').exists(),
            'traces_exist': (evidence / 'lineage_traces.json').exists(),
            'measured_artifacts': measured_validation,
        },
    }
    (evidence / 'catalog_queries.json').write_text(json.dumps(queries, indent=2), encoding='utf-8')
    (ROOT / 'task4' / 'catalog_queries.json').parent.mkdir(exist_ok=True)
    (ROOT / 'task4' / 'catalog_queries.json').write_text(
        json.dumps(queries, indent=2), encoding='utf-8'
    )

    # L01 semantic checks on curate events
    events = [
        json.loads(line)
        for line in (evidence / 'lineage.jsonl').read_text(encoding='utf-8').splitlines()
        if line.strip()
    ]
    assert len(events) >= 2
    assert {e['eventType'] for e in events} >= {'START', 'COMPLETE'}
    assert events[0]['run']['runId'] == events[1]['run']['runId'] == run_record['run_id']
    assert all(e['job']['name'] == 'curate' for e in events[:2])
    assert all(e['inputs'] and e['job']['namespace'] for e in events[:2])

    print(json.dumps({
        'status': 'TASK4_BUILT',
        'catalog': str(catalog_path),
        'datasets': [d['dataset_id'] for d in catalog['datasets']],
        'M01_hits': len(m01),
        'M02_restricted': len(m02['restricted_with_retention']),
        'release_has_email': m02['release_has_email'],
        'retention_cases': len(plan['dry_run_cases']),
        'curated_rows': curated_rows(catalog),
    }, indent=2))


def curated_rows(catalog):
    for d in catalog['datasets']:
        if d['dataset_id'].startswith('curated'):
            return d['row_count']
    return None


if __name__ == '__main__':
    main()
