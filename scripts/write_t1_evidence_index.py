"""Create honest evidence index for the user's actual local T1 execution.
No synthetic outcomes: reads the existing I01-I03 records and contract schema.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='.')
    a = ap.parse_args()
    root = Path(a.root).resolve()
    work = root / 'work'
    ev = work / 'evidence'
    ev.mkdir(parents=True, exist_ok=True)
    checks = {}
    for id_ in ('I01', 'I02', 'I03'):
        p = ev / (id_ + '.json')
        if not p.is_file():
            raise SystemExit(f'Missing actual evidence: {p}; run scripts/verify_t1.py')
        record = json.loads(p.read_text(encoding='utf-8'))
        checks[id_] = {'status': record['status'], 'artifact': f'work/evidence/{id_}.json'}
    i04_file = work / 's3' / 'I04.json'
    if i04_file.is_file():
        j = json.loads(i04_file.read_text(encoding='utf-8-sig'))
        checks['I04'] = {'status': j['status'], 'artifact': 'work/s3/I04.json'}
    else:
        checks['I04'] = {'status': 'BLOCKED', 'reason': 'S3 role access not yet verified'}
    contract = root / 'contract.json'
    contract_obj = json.loads(contract.read_text(encoding='utf-8'))
    manifest = root / 'fixture' / 'trusted_manifest.json'
    inventory = json.loads((work / 'input_inventory.json').read_text(encoding='utf-8'))
    result = {
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_mode': inventory['mode'],
        'store_id': inventory.get('store_id'),
        'contract_version': contract_obj['schema_version'],
        'contract_sha256': sha(contract),
        'contract_review_status': contract_obj.get('review_status', 'UNRECORDED'),
        'trusted_manifest_sha256': sha(manifest),
        'physical_record_count': inventory['total_observation_rows'],
        'parse_failures_preserved': inventory['total_parse_failures'],
        'checks': checks,
        'disclaimer': 'Independent peer review and S3 role tests must be completed before declaring T1 fully done.'
    }
    path = ev / 'T1_STATUS.json'
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print('Evidence index:', path)


if __name__ == '__main__':
    main()
