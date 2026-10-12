"""T5 mutation harness: P01-P08 against scripts/release_gate.py (G01-G07 only)."""
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
import release_gate as gate  # noqa: E402

WORK = ROOT / 'work'
CAND = WORK / 'candidate' / 'curated.parquet'
MUT = WORK / 'mutations'
EVID = WORK / 'evidence'
OPERATOR = os.environ.get('OPERATOR', os.environ.get('USERNAME', 'unknown'))
TOL = json.loads((ROOT / 'contract.json').read_text(encoding='utf-8'))[
    'reference_agreement']['max_abs_delta_c']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def posix(x):
    return Path(x).as_posix()


def mutate(case, select_sql):
    """Write a mutated copy of the candidate. The original is only read."""
    out_dir = MUT / case
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    out = out_dir / 'curated.parquet'
    con = duckdb.connect()
    con.execute("CREATE VIEW cur AS SELECT * FROM read_parquet('" + posix(CAND) + "')")
    con.execute("CREATE VIEW qa AS SELECT upper(trim(record_id)) AS record_id, "
                "CAST(reference_c AS DECIMAL(8,2)) AS reference_c FROM read_csv('"
                + posix(WORK / 'snapshot' / 'qa_reference.csv')
                + "', header=true, all_varchar=true)")
    con.execute('COPY (' + select_sql + ") TO '" + posix(out) + "' (FORMAT PARQUET)")
    return out


def cand_case(case, sql, changed):
    return lambda: (dict(candidate=mutate(case, sql)), changed)


def p06():
    mdir = MUT / 'P06' / 'work'
    shutil.rmtree(mdir.parent, ignore_errors=True)
    mdir.mkdir(parents=True)
    shutil.copytree(WORK / 'snapshot', mdir / 'snapshot')
    shutil.copy(WORK / 'envelopes.csv', mdir / 'envelopes.csv')
    shutil.copytree(WORK / 'staging', mdir / 'staging')
    f = mdir / 'snapshot' / 'observations_a.csv'
    b = bytearray(f.read_bytes())
    b[len(b) // 2] ^= 1
    f.write_bytes(bytes(b))
    return (dict(candidate=CAND, work=mdir),
            'disposable copy of snapshot/observations_a.csv (1 byte flipped)')


MISMATCH_ID = ("(SELECT min(c.record_id) FROM cur c JOIN qa q ON q.record_id = c.record_id "
               "WHERE abs(c.temperature_c - q.reference_c) > CAST(" + str(TOL)
               + " AS DECIMAL(8,2)))")
ONE_ID = '(SELECT min(record_id) FROM cur)'

CASES = [
    dict(id='P01', expect='PASS', build=cand_case(
        'P01', 'SELECT * FROM cur', 'none (unmodified copy of candidate)')),
    dict(id='P02', expect=['G05_VALUES_KEYS'], label='REJECT / VALUE', build=cand_case(
        'P02', "SELECT * REPLACE (CASE WHEN record_id = " + ONE_ID
        + " THEN CAST(100.00 AS DECIMAL(8,2)) ELSE temperature_c END AS temperature_c) FROM cur",
        'candidate: one temperature_c set to 100.00')),
    dict(id='P03', expect=['G04_REQUIRED', 'G05_VALUES_KEYS'],
         label='REJECT / REQUIRED or VALUE', build=cand_case(
        'P03', "SELECT * REPLACE (CASE WHEN record_id = " + ONE_ID
        + " THEN CAST(NULL AS DECIMAL(8,2)) ELSE temperature_c END AS temperature_c) FROM cur",
        'candidate: one temperature_c set to NULL')),
    dict(id='P04', expect=['G02_CONSERVATION', 'G05_VALUES_KEYS'],
         label='REJECT / UNIQUENESS or RECONCILIATION', build=cand_case(
        'P04', 'SELECT * FROM cur UNION ALL SELECT * FROM '
        '(SELECT * FROM cur ORDER BY source_object, source_row LIMIT 1)',
        'candidate: one business record appended twice')),
    dict(id='P06', expect=['G01_INTEGRITY'], label='REJECT / INPUT_INTEGRITY', build=p06),
    dict(id='P08', expect=['G07_REFERENCE'], label='REJECT / COVERAGE', build=cand_case(
        'P08', 'SELECT * FROM cur WHERE record_id <> ' + MISMATCH_ID,
        'candidate: one QA-sample ID with mismatching temperature removed')),
]


def run_case(c):
    t0 = time.time()
    started = datetime.now(timezone.utc).isoformat()
    kwargs, changed = c['build']()
    r = gate.evaluate(**kwargs)
    failures = [x for x in r.get('checks', []) if x['status'] != 'PASS']
    failed_ids = [x['check'] for x in failures]
    if c['expect'] == 'PASS':
        ok = r['data_gate'] == 'PASS'
        expected = 'PASS (data checks G01-G07)'
    else:
        ok = r['data_gate'] == 'REJECT' and any(f in c['expect'] for f in failed_ids)
        expected = c['label'] + ' -> one of ' + ', '.join(c['expect'])
    return dict(case=c['id'], changed_artifact=changed, expected=expected,
                observed_gate=r['data_gate'], failed_checks=failed_ids,
                measured_failures=failures or r.get('error'),
                case_result='AS_EXPECTED' if ok else 'UNEXPECTED',
                operator=OPERATOR, executed_at=started,
                duration_s=round(time.time() - t0, 2))


def main():
    cand_before, snap_before = sha(CAND), sha(WORK / 'snapshot' / 'observations_a.csv')
    records = [run_case(c) for c in CASES]
    for cid, why in (('P05', 'needs G08 metadata check (not implemented yet)'),
                     ('P07', 'needs G09 lineage check (not implemented yet)')):
        records.append(dict(case=cid, case_result='NOT_RUN', reason=why, operator=OPERATOR))
    records.sort(key=lambda x: x['case'])
    unchanged = (sha(CAND) == cand_before
                 and sha(WORK / 'snapshot' / 'observations_a.csv') == snap_before)
    EVID.mkdir(parents=True, exist_ok=True)
    out = dict(scope='P01-P04, P06, P08 run; P05, P07 not run', original_inputs_unchanged=unchanged,
               candidate_sha256=cand_before, records=records)
    (EVID / 'gate-tests-data.json').write_text(json.dumps(out, indent=2, default=str),
                                               encoding='utf-8')
    for r in records:
        print(r['case'], r['case_result'], r.get('observed_gate', ''), r.get('failed_checks', ''))
    print('original_inputs_unchanged:', unchanged)
    sys.exit(0 if unchanged and all(r['case_result'] != 'UNEXPECTED' for r in records) else 1)


if __name__ == '__main__':
    main()
