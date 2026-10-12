"""Gate checks G01 (staged artifacts), G08 (metadata), G09 (lineage). Recompute from real files."""
import hashlib
import json
import uuid
from pathlib import Path

import duckdb
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def posix(x):
    return Path(x).as_posix()


def res(cid, ok, **observed):
    out = {'check': cid, 'status': 'PASS' if ok else 'FAIL'}
    out.update(observed)
    return out


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def g01_staged(run_record, cand, work):
    rr = load(run_record)
    bad, n = [], 0
    outputs = rr.get('outputs') or {}
    for name, meta in outputs.items():
        f = Path(cand) if name == 'curated.parquet' else Path(work) / 'staging' / name
        if not isinstance(meta, dict) or 'sha256' not in meta:
            bad.append({'artifact': name, 'problem': 'NO_HASH_IN_RUN_RECORD'})
        elif not f.exists():
            bad.append({'artifact': name, 'problem': 'MISSING'})
        elif sha(f) != meta['sha256']:
            bad.append({'artifact': name, 'problem': 'HASH_MISMATCH'})
        else:
            n += 1
    ok = not bad and 'curated.parquet' in outputs
    return res('G01_STAGED_ARTIFACTS', ok, artifacts_ok=n, bad_artifacts=bad,
               run_id=rr.get('run_id'))


def g08(cand, work, quar, catalog, run_record, trusted, released, expected_types):
    cat, rr = load(catalog), load(run_record)
    schema = load(ROOT / 'schemas' / 'metadata_catalog_schema.json')
    problems = []
    datasets = cat.get('datasets') or []
    by = {}
    for kind, prefix in (('raw', 'raw-observations'), ('quarantine', 'quarantine'),
                         ('curated', 'curated-candidate')):
        m = [d for d in datasets if str(d.get('dataset_id', '')).startswith(prefix)]
        if len(m) != 1:
            problems.append(kind + ': expected exactly 1 dataset record, found ' + str(len(m)))
        else:
            by[kind] = m[0]
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for d in datasets:
        for e in validator.iter_errors(d):
            problems.append('schema ' + str(d.get('dataset_id')) + ': ' + e.message[:120])
    if cat.get('run_id') != rr.get('run_id'):
        problems.append('catalog.run_id ' + str(cat.get('run_id'))[:8] + ' != run-record.run_id '
                        + str(rr.get('run_id'))[:8])
    contract_actual = sha(ROOT / 'contract.json')
    if rr.get('contract_sha256') != contract_actual:
        problems.append('run-record.contract_sha256 != sha256(contract.json)')
    for kind, d in by.items():
        for field in ('owner', 'steward'):
            if not str(d.get(field, '')).strip():
                problems.append(kind + ': ' + field + ' missing or empty')
        if d.get('contract_sha256') != contract_actual:
            problems.append(kind + ': contract_sha256 != sha256(contract.json)')
        if d.get('code_sha256') != rr.get('code_sha256'):
            problems.append(kind + ': code_sha256 != run-record.code_sha256')
        if d.get('input_manifest_sha256') != rr.get('input_manifest_sha256'):
            problems.append(kind + ': input_manifest_sha256 != run-record')
        for loc in d.get('location') or []:
            if not (ROOT / loc).exists():
                problems.append(kind + ': location not found ' + str(loc))
    con = duckdb.connect()
    n_cur = con.execute("SELECT count(*) FROM read_parquet('" + posix(cand) + "')").fetchone()[0]
    n_q = con.execute("SELECT count(*) FROM read_csv('" + posix(quar)
                      + "', header=true, all_varchar=true)").fetchone()[0]
    n_raw = con.execute("SELECT count(*) FROM read_csv('" + posix(Path(work) / 'envelopes.csv')
                        + "', header=true, all_varchar=true)").fetchone()[0]
    for kind, n in (('raw', n_raw), ('quarantine', n_q), ('curated', n_cur)):
        if kind in by and by[kind].get('row_count') != n:
            problems.append(kind + ': row_count ' + str(by[kind].get('row_count'))
                            + ' != measured ' + str(n))
    if 'curated' in by:
        cols = {c.get('name'): c.get('type') for c in by['curated'].get('columns') or []}
        if list(cols) != released or cols != expected_types:
            problems.append('curated: catalog columns/types differ from the released allowlist')
        if (by['curated'].get('provenance') or {}).get('run_id') != rr.get('run_id'):
            problems.append('curated: provenance.run_id != run-record.run_id')
    observed = {'catalog_run_id': cat.get('run_id'), 'run_record_run_id': rr.get('run_id'),
                'input_manifest_sha256_in_run_record': rr.get('input_manifest_sha256'),
                'sha256_of_external_trusted_manifest': sha(trusted)}
    return res('G08_METADATA', not problems, problem_count=len(problems),
               problems=problems[:40], observed=observed)


def g09(lineage, run_record):
    rr = load(run_record)
    run_id, store = rr.get('run_id'), rr.get('store_id')
    problems = []
    events = [json.loads(x) for x in Path(lineage).read_text(encoding='utf-8-sig').splitlines()
              if x.strip()]
    schema = load(ROOT / 'schemas' / 'openlineage-2-0-2.json')
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for i, e in enumerate(events):
        for err in validator.iter_errors(e):
            problems.append('event ' + str(i) + ' schema: ' + err.message[:120])
    try:
        uuid.UUID(str(run_id))
    except ValueError:
        problems.append('run_id is not a UUID')
    cur = [e for e in events if (e.get('job') or {}).get('name') == 'curate']
    if sorted(e.get('eventType') for e in cur) != ['COMPLETE', 'START']:
        problems.append('curate needs exactly one START and one COMPLETE')
    if {(e.get('run') or {}).get('runId') for e in cur} != {run_id}:
        problems.append('curate event runId differs from run-record.run_id')
    complete = next((e for e in cur if e.get('eventType') == 'COMPLETE'), None)
    if complete:
        outs = {o.get('name') for o in complete.get('outputs') or []}
        for art in ('curated.parquet', 'quarantine.csv', 'duplicates.csv'):
            want = 'research-raw/lab2/staging/' + str(run_id) + '/' + art
            if want not in outs:
                problems.append('COMPLETE missing output ' + want)
        ins = {o.get('name') for o in complete.get('inputs') or []}
        for need in ('observations_a.csv', 'observations_b.jsonl', 'sensors.csv'):
            if 'research-raw/lab2/inputs/batch-01/' + need not in ins:
                problems.append('COMPLETE missing input ' + need)
        if 'contract.json' not in ins:
            problems.append('COMPLETE missing input contract.json')
        if any('qa_reference' in str(n) for n in ins):
            problems.append('qa_reference must not be a transformation input')
    for e in events:
        for ds in (e.get('inputs') or []) + (e.get('outputs') or []):
            if ds.get('namespace') != 'urn:bigdata:' + str(store):
                problems.append('dataset namespace is not store-qualified: ' + str(ds.get('name')))
    return res('G09_LINEAGE', not problems, scope='lineage only; replay equivalence not included',
               events=len(events), problem_count=len(problems), problems=problems[:40])
