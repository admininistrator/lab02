"""T5 release gate (data part G02-G07). Recompute every check from real files."""
import hashlib
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def ratio(n, d):
    if d == 0:
        return {'n': n, 'd': 0, 'value': None, 'status': 'NOT_EVALUATED'}
    return {'n': n, 'd': d, 'value': n / d, 'status': 'EVALUATED'}


def passes(r, threshold):
    return r['value'] is not None and r['value'] >= threshold


def res(cid, ok, **observed):
    out = {'check': cid, 'status': 'PASS' if ok else 'FAIL'}
    out.update(observed)
    return out


def safe(cid, fn):
    try:
        return fn()
    except Exception as e:
        return {'check': cid, 'status': 'BLOCKED',
                'error': type(e).__name__ + ': ' + str(e)[:300]}


def evaluate(candidate=None, quarantine=None, duplicates=None, work=None, trusted=None,
             catalog=None, lineage=None, run_record=None):
    work = Path(work) if work else ROOT / 'work'
    cand = Path(candidate) if candidate else work / 'candidate' / 'curated.parquet'
    quar = Path(quarantine) if quarantine else work / 'staging' / 'quarantine.csv'
    dup = Path(duplicates) if duplicates else work / 'staging' / 'duplicates.csv'
    k = json.loads((ROOT / 'contract.json').read_text(encoding='utf-8'))
    th = k['release_thresholds']
    lo, hi = k['temperature_range_c']['min'], k['temperature_range_c']['max']
    late = k['late_threshold_seconds']
    tol = k['reference_agreement']['max_abs_delta_c']
    n_ref_expected = k['reference_agreement']['sample_count']
    as_of = k['as_of'].replace('T', ' ').replace('Z', '')
    pat = k['record_id_pattern']
    released = k['released_columns']
    expected_types = {
        'record_id': 'VARCHAR', 'sensor_id': 'VARCHAR', 'site': 'VARCHAR',
        'event_time_utc': 'TIMESTAMP', 'ingest_time_utc': 'TIMESTAMP',
        'temperature_c': 'DECIMAL(8,2)', 'is_late': 'BOOLEAN',
        'source_object': 'VARCHAR', 'source_row': 'BIGINT',
        'source_sha256': 'VARCHAR'}

    def p(x):
        return Path(x).as_posix()

    def csv(path):
        return "read_csv('" + p(path) + "', header=true, all_varchar=true)"

    ids = 'source_object, CAST(source_row AS BIGINT) AS source_row'
    con = duckdb.connect()
    try:
        con.execute("CREATE VIEW cur AS SELECT * FROM read_parquet('" + p(cand) + "')")
        con.execute('CREATE VIEW raw_ids AS SELECT ' + ids + ' FROM ' + csv(work / 'envelopes.csv'))
        con.execute('CREATE VIEW q_ids AS SELECT ' + ids + ' FROM ' + csv(quar))
        con.execute('CREATE VIEW d_ids AS SELECT ' + ids + ' FROM ' + csv(dup))
        con.execute('CREATE VIEW cur_ids AS SELECT source_object, source_row FROM cur')
        con.execute('CREATE VIEW reg AS SELECT upper(trim(sensor_id)) AS sensor_id, site FROM '
                    + csv(work / 'snapshot' / 'sensors.csv'))
        con.execute('CREATE VIEW qa AS SELECT upper(trim(record_id)) AS record_id, '
                    'CAST(reference_c AS DECIMAL(8,2)) AS reference_c FROM '
                    + csv(work / 'snapshot' / 'qa_reference.csv'))
    except Exception as e:
        return {'data_gate': 'BLOCKED', 'error': type(e).__name__ + ': ' + str(e)[:300]}

    n_cur = one(con, 'SELECT count(*) FROM cur') if True else 0

    def dist(v):
        return one(con, 'SELECT count(*) FROM (SELECT DISTINCT source_object, source_row FROM ' + v + ')')

    def g02():
        n_raw = one(con, 'SELECT count(*) FROM raw_ids')
        n = {v: one(con, 'SELECT count(*) FROM ' + v) for v in ('cur_ids', 'q_ids', 'd_ids')}
        out_all = ('SELECT source_object, source_row FROM cur_ids UNION ALL '
                   'SELECT source_object, source_row FROM q_ids UNION ALL '
                   'SELECT source_object, source_row FROM d_ids')
        missing = one(con, 'SELECT count(*) FROM (SELECT * FROM raw_ids EXCEPT '
                      'SELECT source_object, source_row FROM (' + out_all + ') AS u)')
        invented = one(con, 'SELECT count(*) FROM (SELECT source_object, source_row FROM ('
                       + out_all + ') AS u EXCEPT SELECT * FROM raw_ids)')
        union_distinct = one(con, 'SELECT count(*) FROM (SELECT DISTINCT source_object, '
                             'source_row FROM (' + out_all + ') AS u)')
        repeated = {v: n[v] - dist(v) for v in n}
        overlap = sum(dist(v) for v in n) - union_distinct
        cur_dup_id = n['cur_ids'] - one(con, 'SELECT count(DISTINCT record_id) FROM cur')
        raw_dup = n_raw - dist('raw_ids')
        ok = (n_raw > 0 and n_raw == sum(n.values()) and missing == 0 and invented == 0
              and overlap == 0 and not any(repeated.values()) and cur_dup_id == 0
              and raw_dup == 0)
        return res('G02_CONSERVATION', ok, n_raw=n_raw, n_curated=n['cur_ids'],
                   n_quarantine=n['q_ids'], n_duplicates=n['d_ids'], missing_from_outputs=missing,
                   invented_ids=invented, overlapping_ids=overlap, repeated_within_output=repeated,
                   duplicate_record_id_in_curated=cur_dup_id, duplicate_raw_ids=raw_dup)

    def g03():
        cols = {r[0]: r[1] for r in con.execute('DESCRIBE SELECT * FROM cur').fetchall()}
        restricted = [c for c in k['restricted_fields'] if c in cols]
        ok = (list(cols) == released and cols == expected_types
              and not restricted and n_cur > 0)
        return res('G03_SCHEMA', ok, columns=cols, restricted_present=restricted, rows=n_cur)

    def g04():
        conds = []
        per_col = {}
        for c in released:
            col = '"' + c + '"'
            cond = col + ' IS NULL'
            if expected_types.get(c) == 'VARCHAR':
                cond += ' OR trim(' + col + ") = ''"
            conds.append('(' + cond + ')')
            per_col[c] = one(con, 'SELECT count(*) FROM cur WHERE ' + cond)
        bad_rows = one(con, 'SELECT count(*) FROM cur WHERE ' + ' OR '.join(conds))
        r = ratio(n_cur - bad_rows, n_cur)
        return res('G04_REQUIRED', passes(r, th['required_completeness']),
                   completeness=r, threshold=th['required_completeness'], bad_by_column=per_col)

    def g05():
        valid = one(con, 'SELECT count(*) FROM cur WHERE COALESCE(temperature_c BETWEEN '
                    + str(lo) + ' AND ' + str(hi) + ', false)')
        key_ok = one(con, "SELECT count(*) FROM cur WHERE COALESCE(regexp_full_match(record_id, '"
                     + pat + "'), false)")
        distinct_ids = one(con, 'SELECT count(DISTINCT record_id) FROM cur')
        sensor_ok = one(con, 'SELECT count(*) FROM cur c WHERE EXISTS '
                        '(SELECT 1 FROM reg r WHERE r.sensor_id = c.sensor_id)')
        site_ok = one(con, 'SELECT count(*) FROM cur c WHERE EXISTS (SELECT 1 FROM reg r '
                      'WHERE r.sensor_id = c.sensor_id AND r.site = c.site)')
        v = ratio(valid, n_cur)
        u = ratio(min(distinct_ids, key_ok), n_cur)
        s = ratio(min(sensor_ok, site_ok), n_cur)
        ok = (passes(v, th['value_validity']) and passes(u, th['business_key_uniqueness'])
              and passes(s, th['sensor_consistency']))
        return res('G05_VALUES_KEYS', ok, value_validity=v, key_uniqueness_and_format=u,
                   sensor_and_site=s)

    def g06():
        chrono_where = ('COALESCE(event_time_utc <= ingest_time_utc AND ingest_time_utc <= '
                        "TIMESTAMP '" + as_of + "', false)")
        lag = "date_diff('second', event_time_utc, ingest_time_utc)"
        n_chrono = one(con, 'SELECT count(*) FROM cur WHERE ' + chrono_where)
        timely = one(con, 'SELECT count(*) FROM cur WHERE ' + chrono_where + ' AND ' + lag
                     + ' <= ' + str(late))
        flag_bad = one(con, 'SELECT count(*) FROM cur WHERE ' + chrono_where
                       + ' AND is_late IS DISTINCT FROM (' + lag + ' > ' + str(late) + ')')
        late_rows = one(con, 'SELECT count(*) FROM cur WHERE is_late')
        c = ratio(n_chrono, n_cur)
        t = ratio(timely, n_chrono)
        ok = passes(c, th['chronology']) and passes(t, th['timeliness']) and flag_bad == 0
        return res('G06_TIME', ok, chronology=c, timeliness=t, is_late_flag_mismatch=flag_bad,
                   late_rows_retained=late_rows)

    def g07():
        n_ref = one(con, 'SELECT count(*) FROM qa')
        matched = one(con, 'SELECT count(*) FROM qa q WHERE EXISTS '
                      '(SELECT 1 FROM cur c WHERE c.record_id = q.record_id)')
        agree = one(con, 'SELECT count(DISTINCT q.record_id) FROM qa q JOIN cur c ON '
                    'c.record_id = q.record_id WHERE COALESCE(abs(c.temperature_c - q.reference_c)'
                    ' <= CAST(' + str(tol) + ' AS DECIMAL(8,2)), false)')
        cov = ratio(matched, n_ref)
        agr = ratio(min(agree, matched), matched)
        ok = (n_ref == n_ref_expected and passes(cov, th['qa_coverage'])
              and passes(agr, th['qa_agreement']))
        return res('G07_REFERENCE', ok, reference_ids=n_ref, coverage=cov, agreement=agr)

    def g01():
        tpath = Path(trusted) if trusted else ROOT.parent / 'trusted_manifest.json'
        tm = json.loads(tpath.read_text(encoding='utf-8'))
        want = {o['key']: o for o in tm['objects']}
        bad, ok_n = [], 0
        for key, o in want.items():
            f = work / 'snapshot' / key
            if not f.exists():
                bad.append({'key': key, 'problem': 'MISSING'})
                continue
            data = f.read_bytes()
            if len(data) != o['bytes'] or hashlib.sha256(data).hexdigest() != o['sha256']:
                bad.append({'key': key, 'problem': 'INPUT_INTEGRITY'})
            else:
                ok_n += 1
        by_name = {k: o['sha256'] for k, o in want.items()}
        rows = con.execute('SELECT DISTINCT source_object, source_sha256 FROM cur').fetchall()
        mismatch = [r[0] for r in rows if by_name.get(r[0].rsplit('/', 1)[-1]) != r[1]]
        ok = not bad and not mismatch and len(want) == 4 and len(rows) > 0
        return res('G01_INTEGRITY', ok, objects_ok=ok_n, objects_expected=len(want),
                   bad_objects=bad, candidate_source_hash_mismatch=mismatch,
                   reason=None if ok else 'INPUT_INTEGRITY')

    import gate_meta
    cat_p = Path(catalog) if catalog else ROOT / 'catalog.json'
    lin_p = Path(lineage) if lineage else ROOT / 'lineage.jsonl'
    rr_p = Path(run_record) if run_record else ROOT / 'run-record.json'
    tru_p = Path(trusted) if trusted else ROOT.parent / 'trusted_manifest.json'

    def g01s():
        return gate_meta.g01_staged(rr_p, cand, work)

    def g08():
        return gate_meta.g08(cand, work, quar, cat_p, rr_p, tru_p, released, expected_types)

    def g09():
        return gate_meta.g09(lin_p, rr_p)

    checks = [safe('G01_INTEGRITY', g01), safe('G01_STAGED_ARTIFACTS', g01s),
              safe('G02_CONSERVATION', g02), safe('G03_SCHEMA', g03),
              safe('G04_REQUIRED', g04), safe('G05_VALUES_KEYS', g05),
              safe('G06_TIME', g06), safe('G07_REFERENCE', g07),
              safe('G08_METADATA', g08), safe('G09_LINEAGE', g09)]
    statuses = [c['status'] for c in checks]
    if all(s == 'PASS' for s in statuses):
        gate = 'PASS'
    elif 'BLOCKED' in statuses:
        gate = 'BLOCKED'
    else:
        gate = 'REJECT'
    return {'scope': 'G01-G09 (G09 = lineage only; replay equivalence C02 not in gate yet).', 'candidate': str(cand),
            'data_gate': gate, 'not_passed': [c['check'] for c in checks if c['status'] != 'PASS'],
            'checks': checks}


if __name__ == '__main__':
    result = evaluate(candidate=sys.argv[1] if len(sys.argv) > 1 else None)
    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result['data_gate'] == 'PASS' else 1)
