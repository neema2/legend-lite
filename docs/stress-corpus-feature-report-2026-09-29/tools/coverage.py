"""Function coverage vs the PCT inventory, literal types per context, service-test details,
and size/combination measures for mappings, stores and tests.

Env PMCD_JSON; args: <pct_functions.tsv> <out.json>
"""
import collections
import json
import re
import sys

from lib import E, classes, enums, services, mappings, dbs, functions, Walk, lambda_env, fp, tname, mult, kind_of, assocs

PCT, OUT = sys.argv[1:3]
pct = collections.defaultdict(set)   # name -> categories
for line in open(PCT, encoding='utf-8'):
    n, cat, f, repo = line.rstrip('\n').split('\t')
    pct[n].add(cat)

contexts = collections.defaultdict(lambda: {'funcs': collections.Counter(), 'lits': collections.Counter(),
                                            'enum': collections.Counter(), 'n': 0})


def run(ctx, node, env):
    w = Walk()
    w.walk(node, env)
    c = contexts[ctx]
    c['funcs'].update(w.funcs)
    c['lits'].update(w.lits)
    c['enum'].update(w.enum_lits)
    c['n'] += 1
    return w


for s in services:
    lam = s['execution'].get('func') or {}
    run('service query', lam, lambda_env(lam))
    for pv in s.get('postValidations') or []:
        for a in pv.get('assertions') or []:
            run('service post-validation', a.get('assertion'), {})
    for ts in s.get('testSuites') or []:
        for t in ts.get('tests') or []:
            for p in t.get('parameters') or []:
                run('service test parameter', p.get('value'), {})
for f in functions:
    env = {p['name']: tname(p.get('genericType')) for p in f.get('parameters') or []}
    run('function body', f.get('body') or [], env)
    for ts in f.get('tests') or []:
        for t in ts.get('tests') or []:
            for p in t.get('parameters') or []:
                run('function test parameter', p.get('value'), {})
for c, e in classes.items():
    for q in e.get('qualifiedProperties') or []:
        env = {'this': c}
        for p in q.get('parameters') or []:
            env[p['name']] = tname(p.get('genericType'))
        run('derived/qualified property', q.get('body') or [], env)
    for k in e.get('constraints') or []:
        run('constraint', (k.get('functionDefinition') or {}).get('body') or [], {'this': c})
        if k.get('messageFunction'):
            run('constraint message', k['messageFunction'], {'this': c})
    for p in e.get('properties') or []:
        if p.get('defaultValue'):
            run('property default value', p['defaultValue'], {})
for a in assocs:
    for q in a.get('qualifiedProperties') or []:
        run('association qualified property', q.get('body') or [], {})
dyna = collections.Counter()


def dyn(op):
    if isinstance(op, dict):
        if op.get('_type') == 'dynaFunc':
            dyna[op.get('funcName')] += 1
        for v in op.values():
            dyn(v)
    elif isinstance(op, list):
        for v in op:
            dyn(v)


for m in mappings:
    for cm in m.get('classMappings') or []:
        if cm['_type'] == 'pureInstance':
            src = cm.get('srcClass')
            for pm in cm.get('propertyMappings') or []:
                run('M2M transform', pm.get('transform') or {}, {'src': src})
            if cm.get('filter'):
                run('M2M filter', cm['filter'], {'src': src})
        if cm['_type'] == 'mergeOperation' and cm.get('validationFunction'):
            run('merge validation', cm['validationFunction'], {})
        if cm['_type'] == 'aggregationAware':
            for a in cm.get('aggregateSetImplementations') or []:
                sp = a.get('aggregateSpecification') or {}
                for g in sp.get('groupByFunctions') or []:
                    run('aggregation-aware spec', g.get('groupByFn'), {})
                for v in sp.get('aggregateValues') or []:
                    run('aggregation-aware spec', v.get('mapFn'), {})
                    run('aggregation-aware spec', v.get('aggregateFn'), {})
        dyn(cm.get('propertyMappings'))
    for am in m.get('associationMappings') or []:
        if am.get('_type') == 'modelJoin':
            run('modelJoin condition', am.get('joinCondition'), {})
        for pm in am.get('propertyMappings') or []:
            if pm.get('crossExpression'):
                run('XStore cross expression', pm['crossExpression'], {})
store_dyna = collections.Counter()
for d in dbs:
    for j in d.get('joins') or []:
        before = dict(dyna)
        dyn(j.get('operation'))
    for f in d.get('filters') or []:
        dyn(f.get('operation'))
    for s in d.get('schemas') or []:
        for v in s.get('views') or []:
            dyn(v.get('columnMappings'))
            dyn(v.get('groupBy'))
for m in [e for e in E if e['_type'] == 'measure']:
    for u in [m.get('canonicalUnit')] + (m.get('nonCanonicalUnits') or []):
        if u and u.get('conversionFunction'):
            run('measure conversion', u['conversionFunction'], {})

# ---- roll-up: every function name used anywhere in a Pure expression
used_pure = collections.Counter()
for c in contexts.values():
    for k, n in c['funcs'].items():
        used_pure[k.split('::')[-1] if '::' not in k or k.startswith('meta::') else k] += n
used_names = {k for k in used_pure if '::' not in k}
dyna_names = set(dyna)
by_cat = collections.defaultdict(lambda: [set(), set()])   # cat -> [used, all]
for n, cats in pct.items():
    for c in cats:
        by_cat[c][1].add(n)
        if n in used_names or n in dyna_names:
            by_cat[c][0].add(n)
non_pct_used = sorted(n for n in used_names if n not in pct)

# ---- service tests
ser_formats = collections.Counter()
assert_types = collections.Counter()
data_kinds = collections.Counter()
tests_per_suite = collections.Counter()
asserts_per_test = collections.Counter()
suites_per_service = collections.Counter()
expected_sizes = []
test_params = []
for s in services:
    n_s = len(s.get('testSuites') or [])
    if n_s:
        suites_per_service[n_s] += 1
    for ts in s.get('testSuites') or []:
        tests_per_suite[len(ts.get('tests') or [])] += 1
        for cd in (ts.get('testData') or {}).get('connectionsTestData') or []:
            data_kinds[(cd.get('data') or {}).get('_type')] += 1
        for t in ts.get('tests') or []:
            ser_formats[t.get('serializationFormat') or '(none)'] += 1
            asserts_per_test[len(t.get('assertions') or [])] += 1
            for a in t.get('assertions') or []:
                assert_types[a.get('_type')] += 1
                exp = a.get('expected') or {}
                expected_sizes.append(len(exp.get('data') or ''))
            for p in t.get('parameters') or []:
                v = p.get('value') or {}
                test_params.append((fp(s), p.get('name'), v.get('_type'), v.get('value')))
# ---- data elements
data_el = collections.Counter()
csv_rows = collections.Counter()
for e in E:
    if e['_type'] == 'dataElement':
        d = e.get('data') or {}
        data_el[d.get('_type')] += 1
        if d.get('_type') == 'relationalCSVData':
            for t in d.get('tables') or []:
                csv_rows[fp(e)] += max(0, len((t.get('values') or '').strip().splitlines()) - 1)

out = dict(
    contexts={k: dict(n=v['n'], funcs=dict(v['funcs']), lits=dict(v['lits']), enum=dict(v['enum']))
              for k, v in contexts.items()},
    pct_by_category={c: dict(used=sorted(u), total=len(a), unused=sorted(a - u)) for c, (u, a) in by_cat.items()},
    used_pure_funcs=dict(used_pure), non_pct_used=non_pct_used,
    mapping_dynafunctions=dict(dyna),
    ser_formats=dict(ser_formats), assert_types=dict(assert_types), data_kinds=dict(data_kinds),
    tests_per_suite=dict(tests_per_suite), asserts_per_test=dict(asserts_per_test),
    suites_per_service=dict(suites_per_service), test_params=test_params,
    expected_json_size=dict(max=max(expected_sizes), median=sorted(expected_sizes)[len(expected_sizes) // 2],
                            over_10k=sum(1 for x in expected_sizes if x > 10000)),
    data_elements=dict(data_el), csv_rows=dict(csv_rows),
)
json.dump(out, open(OUT, 'w', encoding='utf-8'), indent=1)
tot_used = len({n for c, (u, a) in by_cat.items() for n in u})
tot = len(pct)
print('PCT names used anywhere (Pure expression or mapping dynafunction): %d of %d' % (tot_used, tot))
for c in sorted(by_cat):
    u, a = by_cat[c]
    print('  %-34s %3d / %3d   %s' % (c, len(u), len(a), ' '.join(sorted(u))[:120]))
print('non-PCT functions used in Pure expressions:', non_pct_used)
print()
for k, v in contexts.items():
    print('%-32s n=%5d lits=%s enum=%d funcs=%s' % (k, v['n'], dict(v['lits']), sum(v['enum'].values()),
                                                 ' '.join(sorted(v['funcs']))[:160]))
print()
print('serialization formats', dict(ser_formats))
print('assert types', dict(assert_types), 'data kinds', dict(data_kinds))
print('tests/suite', dict(tests_per_suite), 'asserts/test', dict(asserts_per_test), 'suites/service', dict(suites_per_service))
print('test params', test_params)
print('expected json size', out['expected_json_size'])
print('data elements', dict(data_el), 'csv rows', dict(csv_rows))
