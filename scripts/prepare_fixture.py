"""Extract the provided ZIP without modifying source files; copy independent trusted manifest.
Usage: python scripts/prepare_fixture.py path/to/Lab2-datasets.zip path/to/trusted_manifest.json
"""
import hashlib
import json
import shutil
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXPECTED = {'observations_a.csv', 'observations_b.jsonl', 'sensors.csv', 'qa_reference.csv'}


def main():
    if len(sys.argv) != 3:
        raise SystemExit('Usage: python scripts/prepare_fixture.py DATASET_ZIP TRUSTED_MANIFEST')
    archive = Path(sys.argv[1]); trusted_path = Path(sys.argv[2])
    trusted_bytes = trusted_path.read_bytes()
    trusted = json.loads(trusted_bytes)
    assert {o['key'] for o in trusted['objects']} == EXPECTED, 'Unexpected trusted manifest objects'
    fixture = ROOT / 'fixture'
    (fixture / 'input').mkdir(parents=True, exist_ok=True)
    (fixture / 'schemas').mkdir(parents=True, exist_ok=True)
    (fixture / 'templates').mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        prefix = 'Lab2-datasets/'
        members = set(z.namelist())
        for entry in trusted['objects']:
            key = entry['key']
            path = prefix + 'input/' + key
            if path not in members:
                raise RuntimeError(f'Missing ZIP input: {path}')
            data = z.read(path)
            if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
                raise RuntimeError(f'INPUT_INTEGRITY: {key}')
            (fixture / 'input' / key).write_bytes(data)
        for item in sorted(members):
            for category in ('schemas', 'templates'):
                sub = prefix + category + '/'
                if item.startswith(sub) and not item.endswith('/'):
                    (fixture / category / Path(item).name).write_bytes(z.read(item))
    # Authoritative manifest is copied from the separately provided path, NEVER the ZIP.
    (fixture / 'trusted_manifest.json').write_bytes(trusted_bytes)
    print('Prepared and SHA-256 verified all 4 inputs in fixture/input/.')
    print('Stored separate trusted_manifest.json in fixture/.')


if __name__ == '__main__':
    main()
