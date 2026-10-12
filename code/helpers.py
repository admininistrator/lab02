"""Small supplied helpers. They do not compute or certify data quality."""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sha256_bytes(data: bytes):
    return hashlib.sha256(data).hexdigest()


def ratio(numerator, denominator):
    if denominator == 0:
        return dict(n=numerator, d=0, value=None, status='NOT_EVALUATED')
    if not 0 <= numerator <= denominator:
        raise ValueError('Invalid numerator or denominator')
    return dict(n=numerator, d=denominator,
                value=numerator / denominator, status='EVALUATED')


def new_run():
    return str(uuid.uuid4())


def lineage_event(kind, run_id, job, inputs, outputs, store_id):
    if kind not in {'START', 'COMPLETE', 'ABORT', 'FAIL'}:
        raise ValueError('Unsupported run event')

    def dataset(name):
        return {'namespace': 'urn:bigdata:' + store_id, 'name': name}

    return dict(
        eventType=kind,
        eventTime=datetime.now(timezone.utc).isoformat(),
        producer='https://github.com/vnu-uet/bigdata-lab2/student-pipeline',
        schemaURL=('https://openlineage.io/spec/2-0-2/'
                   'OpenLineage.json#/$defs/RunEvent'),
        run={'runId': run_id},
        job={'namespace': 'vnu-uet.bigdata.lab2', 'name': job},
        inputs=[dataset(x) for x in inputs],
        outputs=[dataset(x) for x in outputs],
    )


def append_event(path, event):
    with Path(path).open('a', encoding='utf-8') as f:
        f.write(json.dumps(event) + '\n')


def validate_event(event, schema_path):
    from jsonschema import Draft202012Validator, FormatChecker
    schema = json.loads(Path(schema_path).read_text(encoding='utf-8'))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)


def code_bundle_sha256(paths):
    """Canonical digest: sorted filename -> per-file sha256."""
    items = []
    for p in paths:
        path = Path(p)
        items.append(f'{path.name}:{sha256(path)}')
    payload = '\n'.join(sorted(items)).encode('utf-8')
    return sha256_bytes(payload)


def repo_rel(path, root=None):
    """Portable repo-relative POSIX path for shared catalogs (no machine drive letters)."""
    root = Path(root) if root is not None else Path(__file__).resolve().parent.parent
    path = Path(path).resolve()
    try:
        return path.relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()
