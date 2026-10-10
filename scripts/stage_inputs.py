"""Verified stage input envelopes for Lab 2, supporting local fallback or actual S3 GET.
Usage:
  python scripts/stage_inputs.py local fixture/trusted_manifest.json --output-dir work
  python scripts/stage_inputs.py s3   fixture/trusted_manifest.json --output-dir work
Local fallback is NOT evidence that S3 identity controls work.
"""
import argparse
import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

OBSERVATION_KEYS = ('observations_a.csv', 'observations_b.jsonl')
ALL_KEYS = {'observations_a.csv', 'observations_b.jsonl', 'sensors.csv', 'qa_reference.csv'}
CANON = ('record_id', 'sensor_id', 'event_time', 'ingest_time', 'reading', 'unit', 'operator_email')
JSON_KEYS = ('id', 'sensor', 'timestamp', 'arrived_at', 'temperature', 'temperature_unit', 'operator')
EXTRA = ('source_object', 'source_row', 'source_sha256', 'parse_ok', 'raw_payload')
PREFIX = 'lab2/inputs/batch-01/'


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def check_data(data, obj):
    if len(data) != obj['bytes'] or sha256(data) != obj['sha256']:
        raise RuntimeError('INPUT_INTEGRITY: ' + obj['key'])


def make_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
                        region_name='us-east-1',
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('local', 's3'))
    parser.add_argument('trusted_manifest')
    parser.add_argument('--output-dir', default='work')
    args = parser.parse_args()
    manifest_path = Path(args.trusted_manifest)
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if {o['key'] for o in manifest['objects']} != ALL_KEYS:
        raise ValueError('Unexpected objects in manifest')
    work = Path(args.output_dir)
    snapshot = work / 'snapshot'
    snapshot.mkdir(parents=True, exist_ok=True)
    s3 = make_client() if args.mode == 's3' else None
    store_id = os.environ.get('STORE_ID', 'bd-g06-objects')
    retrieved_at = datetime.now(timezone.utc).isoformat()
    source_bytes = {}
    for obj in manifest['objects']:
        key = obj['key']
        if args.mode == 's3':
            resp = s3.get_object(Bucket='research-raw', Key=PREFIX + key)
            try:
                data = resp['Body'].read()
            finally:
                resp['Body'].close()
        else:
            input_dir = manifest_path.parent / 'input'
            if not input_dir.exists():
                input_dir = manifest_path.parent
            data = (input_dir / key).read_bytes()
        check_data(data, obj)
        source_bytes[key] = data
    # Writes only after all four integrity checks pass.
    for name, data in source_bytes.items():
        (snapshot / name).write_bytes(data)
    inventory = []
    envelopes = work / 'envelopes.csv'
    with envelopes.open('w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CANON + EXTRA)
        writer.writeheader()
        for key in OBSERVATION_KEYS:
            data = source_bytes[key]
            expected = next(o for o in manifest['objects'] if o['key'] == key)
            is_csv = key.endswith('.csv')
            if is_csv:
                records = csv.DictReader(io.StringIO(data.decode('utf-8'), newline=''))
            else:
                records = io.StringIO(data.decode('utf-8'), newline='')
            n = 0
            parse_failures = 0
            for n, raw in enumerate(records, 1):
                payload = json.dumps(raw, ensure_ascii=False) if is_csv else raw.rstrip('\r\n')
                try:
                    record = raw if is_csv else json.loads(raw)
                    if not isinstance(record, dict):
                        raise ValueError('Not a JSON object')
                    if not is_csv:
                        record = dict(zip(CANON, (record.get(k) for k in JSON_KEYS)))
                    canon = {k: '' if record.get(k) is None else str(record.get(k)) for k in CANON}
                    ok = True
                except (TypeError, ValueError, json.JSONDecodeError):
                    canon = {k: '' for k in CANON}
                    ok = False
                    parse_failures += 1
                source_ref = ('s3://research-raw/' + PREFIX + key) if args.mode == 's3' else ('local://fixture/input/' + key)
                canon.update(source_object=source_ref, source_row=n, source_sha256=sha256(data),
                             parse_ok=str(ok).lower(), raw_payload=payload)
                writer.writerow(canon)
            if n != expected['records']:
                raise RuntimeError('ROW_COUNT: ' + key)
            inventory.append({'store_id': store_id if args.mode == 's3' else None,
                              'mode': args.mode,
                              'key': PREFIX + key, 'records': n,
                              'parse_failures': parse_failures,
                              'bytes': len(data), 'sha256': sha256(data),
                              'retrieved_at': retrieved_at})
    report = {'mode': args.mode, 'store_id': store_id if args.mode == 's3' else None,
              'trusted_manifest_sha256': sha256(manifest_bytes),
              'retrieved_at': retrieved_at,
              'source_objects': [{'key': PREFIX + x['key'], 'bytes': len(source_bytes[x['key']]),
                                  'sha256': sha256(source_bytes[x['key']]), 'records': x['records']} for x in manifest['objects']],
              'observations': inventory,
              'total_observation_rows': sum(x['records'] for x in inventory),
              'total_parse_failures': sum(x['parse_failures'] for x in inventory)}
    (work / 'input_inventory.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({'status': 'VERIFIED', 'mode': args.mode,
                      'observation_rows': report['total_observation_rows'],
                      'parse_failures': report['total_parse_failures']}))


if __name__ == '__main__':
    main()
