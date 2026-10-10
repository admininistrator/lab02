"""Produce local evidence for I01-I03. I04 must be tested from the curator S3 identity."""
import csv
import hashlib
import json
import platform
import sys
from pathlib import Path
from importlib import metadata


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def main():
    root = Path(__file__).resolve().parent.parent
    fixture = root / 'fixture'
    work = root / 'work'
    evidence = work / 'evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    manifest_bytes = (fixture / 'trusted_manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    inventory = json.loads((work / 'input_inventory.json').read_text())
    i01 = {'id': 'I01', 'mode': inventory['mode'], 'trusted_manifest_sha256': sha256(manifest_bytes),
           'objects': [], 'status': 'PASS'}
    for obj in manifest['objects']:
        data = (work / 'snapshot' / obj['key']).read_bytes()
        passed = len(data) == obj['bytes'] and sha256(data) == obj['sha256']
        i01['objects'].append({'key': obj['key'], 'byte_size_match': len(data) == obj['bytes'],
                               'sha256_match': sha256(data) == obj['sha256']})
        if not passed:
            i01['status'] = 'FAIL'
    i02 = {'id': 'I02', 'status': 'PASS'}
    physical_ids = set()
    row_total = 0
    parse_failures = 0
    with (work / 'envelopes.csv').open(newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            row_total += 1
            key = (row['source_object'], row['source_row'])
            if key in physical_ids:
                i02['status'] = 'FAIL'
            physical_ids.add(key)
            if row['parse_ok'].lower() == 'false':
                parse_failures += 1
                if not row['raw_payload']:
                    i02['status'] = 'FAIL'
    i02.update({'total_envelopes': row_total, 'unique_physical_keys': len(physical_ids),
                'parse_failures_retained': parse_failures,
                'expected_envelopes': sum(o['records'] for o in manifest['objects'] if o['key'].startswith('observations_'))})
    if row_total != i02['expected_envelopes'] or len(physical_ids) != row_total:
        i02['status'] = 'FAIL'
    # Tamper only in memory: no original or staged fixture is modified.
    obj = manifest['objects'][0]
    original = (work / 'snapshot' / obj['key']).read_bytes()
    tampered = bytearray(original)
    tampered[0] ^= 1
    from stage_inputs import check_data
    try:
        check_data(bytes(tampered), obj)
        rejected = False
    except RuntimeError as exc:
        rejected = 'INPUT_INTEGRITY' in str(exc)
    i03 = {'id': 'I03', 'status': 'PASS' if rejected else 'FAIL',
           'expected_error': 'INPUT_INTEGRITY', 'tampered_copy_rejected': rejected,
           'original_unchanged': sha256(original) == obj['sha256']}
    for record in (i01, i02, i03):
        (evidence / f"{record['id']}.json").write_text(json.dumps(record, indent=2), encoding='utf-8')
    (work / 'integrity-test.json').write_text(json.dumps(i03, indent=2), encoding='utf-8')
    versions = {'python': platform.python_version(), 'platform': platform.platform(),
                'executable': sys.executable}
    for pkg in ['duckdb', 'boto3', 'jsonschema']:
        try: versions[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError: versions[pkg] = 'not installed'
    (evidence / 'runtime_versions.json').write_text(json.dumps(versions, indent=2), encoding='utf-8')
    for r in (i01, i02, i03):
        print(r['id'], r['status'])
    print('mode:', inventory['mode'], '; I04 is NOT tested by this script')
    if not all(x['status'] == 'PASS' for x in (i01, i02, i03)):
        raise SystemExit(2)


if __name__ == '__main__':
    main()
