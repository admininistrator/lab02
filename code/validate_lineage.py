"""Validate lineage.jsonl against cached OpenLineage 2-0-2 schema + lab semantics."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent


def main():
    schema_path = ROOT / 'schemas' / 'openlineage-2-0-2.json'
    lineage_path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / 'lineage.jsonl'
    schema = json.loads(schema_path.read_text(encoding='utf-8'))
    # Root document is oneOf RunEvent | DatasetEvent | JobEvent
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    events = [
        json.loads(line)
        for line in lineage_path.read_text(encoding='utf-8').splitlines()
        if line.strip()
    ]
    errors = []
    for i, event in enumerate(events):
        for err in sorted(validator.iter_errors(event), key=lambda e: list(e.path)):
            errors.append(f'event[{i}] {event.get("eventType")}: {err.message}')
    # Lab semantic checks for curate pair
    if len(events) < 2:
        errors.append('Need at least START/COMPLETE pair')
    else:
        if events[0]['eventType'] != 'START' or events[1]['eventType'] != 'COMPLETE':
            errors.append('First pair must be START then COMPLETE')
        if events[0]['run']['runId'] != events[1]['run']['runId']:
            errors.append('Paired run IDs differ')
        if not events[0].get('inputs'):
            errors.append('START inputs empty')
        if not events[1].get('outputs'):
            errors.append('COMPLETE outputs empty')
        for ev in events[:2]:
            if not ev['job'].get('namespace') or not ev['job'].get('name'):
                errors.append('Missing job identity')
            for ds in ev.get('inputs', []) + ev.get('outputs', []):
                if not str(ds.get('namespace', '')).startswith('urn:bigdata:'):
                    errors.append(f"Dataset namespace not store-qualified: {ds}")
    if errors:
        print(json.dumps({'status': 'FAIL', 'errors': errors}, indent=2))
        raise SystemExit(1)
    print(json.dumps({
        'status': 'PASS',
        'events': len(events),
        'run_id': events[0]['run']['runId'],
        'job': events[0]['job']['name'],
    }, indent=2))


if __name__ == '__main__':
    main()
