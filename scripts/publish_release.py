"""T5 publish: recheck gate, record a named review, manifest-last upload via the owner Pod."""
import argparse
import unicodedata
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
import release_gate  # noqa: E402

NS = 'bd-g06'
WORK = ROOT / 'work'
AUDIT = WORK / 'evidence' / 'publish-lineage.jsonl'


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def load(p):
    return json.loads(Path(p).read_text(encoding='utf-8-sig'))


def emit(kind, run_id, store, inputs, outputs):
    e = {'eventType': kind, 'eventTime': now(),
         'producer': 'urn:vnu-uet:bigdata:lab2:student-pipeline',
         'schemaURL': 'https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent',
         'run': {'runId': run_id},
         'job': {'namespace': 'vnu-uet.bigdata.lab2', 'name': 'validate-and-publish'},
         'inputs': [{'namespace': 'urn:bigdata:' + store, 'name': n} for n in inputs],
         'outputs': [{'namespace': 'urn:bigdata:' + store, 'name': n} for n in outputs]}
    Draft202012Validator(load(ROOT / 'schemas' / 'openlineage-2-0-2.json'),
                         format_checker=FormatChecker()).validate(e)
    AUDIT.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT.open('a', encoding='utf-8') as f:
        f.write(json.dumps(e) + '\n')


def kube(args):
    return subprocess.run(['kubectl', '-n', NS] + args, capture_output=True, text=True, cwd=ROOT)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--reviewer')
    ap.add_argument('--decision')
    ap.add_argument('--reason')
    ap.add_argument('--suffix', default='')
    ap.add_argument('--supersedes', default='')
    a = ap.parse_args()
    reviewer = (a.reviewer or input('Ten reviewer (khac nguoi curation C): ')).strip()
    decision = (a.decision or input('Quyet dinh (APPROVE hoac REJECT): ')).strip().upper()
    reason = (a.reason or input('Ly do (ngan): ')).strip()
    rr = load(ROOT / 'run-record.json')
    if not reviewer or not reason or reviewer.upper().startswith('PENDING'):
        raise SystemExit('Thieu ten reviewer hoac ly do. Khong publish.')
    if reviewer.lower() in str(rr.get('operator', '')).lower() or 'role-c' in reviewer.lower():
        raise SystemExit('Reviewer trung nguoi curation. Khong publish.')
    folded = ''.join(ch for ch in unicodedata.normalize('NFKD', reviewer.lower()) if not unicodedata.combining(ch))
    if 'tung' in folded:
        raise SystemExit('Reviewer la Tung (Role C, nguoi curation). Khong publish.')
    store, run_id, publish_run = rr['store_id'], rr['run_id'], str(uuid.uuid4())
    rid = 'rel-' + run_id[:8] + ('-' + a.suffix if a.suffix else '')
    stage = 'research-raw/lab2/staging/' + run_id + '/'
    ins = [stage + 'curated.parquet', 'catalog.json', 'run-record.json', 'contract.json',
           'research-raw/lab2/inputs/batch-01/sensors.csv',
           'research-raw/lab2/inputs/batch-01/qa_reference.csv']
    emit('START', publish_run, store, ins, [])

    gate = release_gate.evaluate()
    if gate['data_gate'] != 'PASS':
        emit('ABORT', publish_run, store, ins, [])
        raise SystemExit('GATE ' + gate['data_gate'] + ' ' + str(gate.get('not_passed')) + ' -> ABORT')
    if decision != 'APPROVE':
        emit('ABORT', publish_run, store, ins, [])
        raise SystemExit('Reviewer quyet dinh ' + decision + ' -> ABORT, khong publish.')

    cand = WORK / 'candidate' / 'curated.parquet'
    d = WORK / 'release' / rid
    shutil.rmtree(d, ignore_errors=True)
    files = {'data/curated.parquet': cand, 'metadata/catalog.json': ROOT / 'catalog.json',
             'metadata/README.md': ROOT / 'DATA_README.md',
             'evidence/quality_after.json': ROOT / 'task2' / 'quality_after.json',
             'evidence/run-record.json': ROOT / 'run-record.json'}
    for key, src in files.items():
        (d / key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, d / key)
        if not key.endswith('.parquet') and b'@' in (d / key).read_bytes():
            raise SystemExit('Tim thay ky tu @ trong ' + key + ' -> khong publish.')
    contract = load(ROOT / 'contract.json')
    review = {'release_id': rid, 'decision': decision, 'reviewer': reviewer, 'reason': reason,
              'run_id': run_id, 'candidate_sha256': sha(cand), 'recorded_at': now(),
              'curation_operator': rr.get('operator'),
              'publisher_operator': os.environ.get('USERNAME', 'unknown')}
    (d / 'evidence' / 'review.json').write_text(json.dumps(review, indent=2), encoding='utf-8')
    arts = {}
    for p in sorted(x for x in d.rglob('*') if x.is_file()):
        arts[p.relative_to(d).as_posix()] = {'sha256': sha(p), 'bytes': p.stat().st_size}
    n = duckdb.connect().execute("SELECT count(*) FROM read_parquet('" + cand.as_posix() + "')").fetchone()[0]
    manifest = {'release_id': rid, 'supersedes': a.supersedes or None, 'approved_run_id': run_id, 'publish_run_id': publish_run,
                'store_id': store, 'schema_version': contract['schema_version'],
                'as_of': contract['as_of'], 'curated_row_count': n,
                'candidate_sha256': review['candidate_sha256'], 'reviewer': reviewer,
                'approval_time': review['recorded_at'],
                'gate_result': {c['check']: c['status'] for c in gate['checks']},
                'artifacts': arts}
    (d / 'release_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    base = '/tmp/rel/' + rid
    steps = [['exec', 'owner', '--', 'rm', '-rf', '/tmp/rel'],
             ['exec', 'owner', '--', 'mkdir', '-p', base + '/data', base + '/metadata', base + '/evidence']]
    for step in steps:
        kube(step)
    to_copy = list(arts) + ['release_manifest.json']
    for key in to_copy:
        r = kube(['cp', (d / key).relative_to(ROOT).as_posix(), 'owner:' + base + '/' + key])
        if r.returncode:
            emit('FAIL', publish_run, store, ins, [])
            raise SystemExit('kubectl cp loi: ' + key + ' ' + r.stderr[-300:])
    r = kube(['cp', 'scripts/owner_upload.py', 'owner:/tmp/rel/owner_upload.py'])
    r = kube(['exec', 'owner', '--', 'python3', '/tmp/rel/owner_upload.py', base, rid])
    if r.returncode:
        emit('FAIL', publish_run, store, ins, [])
        raise SystemExit('UPLOAD FAIL: ' + (r.stderr or r.stdout)[-400:])
    up = json.loads(r.stdout.strip().splitlines()[-1])
    outs = ['research-release/lab2/releases/' + rid + '/' + k for k in to_copy]
    emit('COMPLETE', publish_run, store, ins, outs)
    local_manifest_sha = sha(d / 'release_manifest.json')
    result = {'status': 'PUBLISHED', 'release_id': rid, 'publish_run_id': publish_run,
              'approved_run_id': run_id, 'reviewer': reviewer, 'rows': n,
              'manifest_sha256': up['manifest_sha256'],
              'manifest_matches_local': up['manifest_sha256'] == local_manifest_sha,
              'objects_uploaded': up['uploaded']}
    (WORK / 'evidence' / 'publish-result.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
