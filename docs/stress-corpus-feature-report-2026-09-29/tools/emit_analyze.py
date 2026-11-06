"""The same measures over legend-engine's EMIT models (parsed in one PMCD, attributed by line).

Env PMCD_JSON=emit_all.json; args: <ranges.json> <out.json>
"""
import bisect
import collections
import json
import sys

from lib import E, classes, enums, services, mappings, dbs, functions, Walk, lambda_env, fp, tname, mult, kind_of

ranges = json.load(open(sys.argv[1], encoding='utf-8'))
OUT = sys.argv[2]
starts = [r[0] for r in ranges]


def model_of(e):
    si = e.get('sourceInformation') or {}
    ln = si.get('startLine')
    if ln is None:
        return '?'
    i = bisect.bisect_right(starts, ln) - 1
    return ranges[i][2] if i >= 0 else '?'


R = collections.defaultdict(collections.Counter)
models_with = collections.defaultdict(set)


def note(key, val, e, n=1):
    R[key][val] += n
    models_with[key + '=' + str(val)].add(model_of(e))


# ---- services
for s in services:
    note('element', 'service', s)
    ex = s['execution']
    note('service exec', ex.get('_type'), s)
    lam = ex.get('func') or {}
    for p in lam.get('parameters') or []:
        t = tname(p.get('genericType'))
        k = kind_of(t)
        note('service param type', ('enum' if k == 'enum' else k), s)
        note('service param mult', mult(p.get('multiplicity')), s)
    w = Walk()
    w.walk(lam, lambda_env(lam))
    for k, n in w.lits.items():
        note('service query literal', k, s, n)
    for k, n in w.enum_lits.items():
        note('service query literal', 'enum', s, n)
    for k in w.funcs:
        note('service query function', k, s)
    for ts in s.get('testSuites') or []:
        for cd in (ts.get('testData') or {}).get('connectionsTestData') or []:
            note('service test data', (cd.get('data') or {}).get('_type'), s)
        for t in ts.get('tests') or []:
            note('service test', 'test', s)
            note('service serialization', t.get('serializationFormat') or '(none)', s)
            for a in t.get('assertions') or []:
                note('assertion (service)', a.get('_type'), s)
            for p in t.get('parameters') or []:
                v = p.get('value') or {}
                note('service test param value', v.get('_type'), s)
    if s.get('test'):
        note('service legacy test', 'yes', s)
# ---- mappings
for m in mappings:
    for cm in m.get('classMappings') or []:
        note('class mapping', cm['_type'], m)
    for ts in m.get('testSuites') or []:
        note('mapping test suite', 'suite', m)
        lam = ts.get('func') or ts.get('query') or {}
        w = Walk()
        w.walk(lam, lambda_env(lam))
        for k, n in w.lits.items():
            note('mapping-test query literal', k, m, n)
        for k, n in w.enum_lits.items():
            note('mapping-test query literal', 'enum', m, n)
        for k in w.funcs:
            note('mapping-test query function', k, m)
        for t in ts.get('tests') or []:
            note('mapping test', 'test', m)
            for d in t.get('storeTestData') or []:
                note('mapping test data', (d.get('data') or {}).get('_type'), m)
            for a in t.get('assertions') or []:
                note('assertion (mapping)', a.get('_type'), m)
    if m.get('tests'):
        note('mapping legacy tests', 'yes', m, len(m['tests']))
# ---- functions
for f in functions:
    note('element', 'function', f)
    for p in f.get('parameters') or []:
        k = kind_of(tname(p.get('genericType')))
        note('function param type', 'enum' if k == 'enum' else k, f)
    w = Walk()
    w.walk(f.get('body') or [], {p['name']: tname(p.get('genericType')) for p in f.get('parameters') or []})
    for k, n in w.lits.items():
        note('function body literal', k, f, n)
    for k, n in w.enum_lits.items():
        note('function body literal', 'enum', f, n)
    for ts in f.get('tests') or []:
        for t in ts.get('tests') or []:
            note('function test', 'test', f)
            for a in t.get('assertions') or []:
                note('assertion (function)', a.get('_type'), f)
            for p in t.get('parameters') or []:
                note('function test param value', (p.get('value') or {}).get('_type'), f)
# ---- classes / constraints
for c, e in classes.items():
    note('element', 'class', e)
    for k in e.get('constraints') or []:
        w = Walk()
        w.walk((k.get('functionDefinition') or {}).get('body') or [], {'this': c})
        note('constraint', 'constraint', e)
        R['constraint functions'].update(w.funcs)
        R['constraint literal'].update(w.lits)
        if k.get('enforcementLevel'):
            note('constraint enforcement', k['enforcementLevel'], e)
        if k.get('messageFunction'):
            note('constraint message', 'yes', e)
    for p in e.get('properties') or []:
        note('class property type', kind_of(tname(p.get('genericType'))), e)
for e in E:
    note('element', e['_type'], e)

res = {k: dict(v) for k, v in R.items()}
res['__models_with'] = {k: len(v) for k, v in models_with.items()}
json.dump(res, open(OUT, 'w', encoding='utf-8'), indent=1)
for k in sorted(R):
    items = ', '.join('%s=%d(%dm)' % (v, n, len(models_with.get(k + '=' + str(v), ()))) for v, n in R[k].most_common(40))
    print('%-30s %s' % (k, items[:600]))
