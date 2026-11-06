"""Size and combination complexity across queries, mappings, stores and classes.

Env PMCD_JSON=<stress-pmcd.json>. Usage: complexity.py <analysis.json> <profile.json> <complexity.json>
"""
import collections
import json
import statistics
import sys

from lib import E, classes, enums, services, mappings, dbs, fp, tname, mult, kind_of

A = json.load(open(sys.argv[1], encoding='utf-8'))
P = {r['svc']: r for r in json.load(open(sys.argv[2], encoding='utf-8'))}


def dist(xs, cuts):
    c = collections.Counter()
    for x in xs:
        for lo, hi, lab in cuts:
            if lo <= x <= hi:
                c[lab] += 1
                break
    return dict(c)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


out = {}
# ---------------- queries: size
rows = A['svc_rows']
cols = [r['colspecs'] for r in rows]
nodes = [r['nodes'] for r in rows]
trees = [r['tree'] for r in rows if r['tree']]
out['query_size'] = dict(
    nodes=dict(median=statistics.median(nodes), p90=pct(nodes, .9), max=max(nodes)),
    project_columns=dict(median=statistics.median(cols), p90=pct(cols, .9), max=max(cols),
                         buckets=dist(cols, [(0, 0, '0'), (1, 5, '1-5'), (6, 10, '6-10'), (11, 20, '11-20'),
                                             (21, 50, '21-50'), (51, 100, '51-100'), (101, 10**6, '>100')])),
    graph=dict(count=len(trees), max_depth=max(t['depth'] for t in trees), max_nodes=max(t['nodes'] for t in trees),
               max_leaves=max(t['leaves'] for t in trees), subtype_trees=sum(1 for t in trees if t['subtypes']),
               qualified_in_tree=sum(1 for t in trees if t['qualified']),
               leaves_buckets=dist([t['leaves'] for t in trees], [(0, 5, '0-5'), (6, 10, '6-10'), (11, 20, '11-20'), (21, 10**6, '>20')])),
    widest=sorted(((r['colspecs'], r['svc'], r['status'], r['exp']) for r in rows), reverse=True)[:6],
    biggest_trees=sorted(((t['tree']['leaves'], t['tree']['nodes'], t['tree']['depth'], t['svc'], t['status']) for t in rows if t['tree']), reverse=True)[:6],
)
# ---------------- queries: combination
df = [r['distinct_funcs'] for r in rows]
nfeat = {s: len([f for f in p['feats'] if not f.startswith('scaffolding:')]) for s, p in P.items()}
emitfeat = {s: len([f for f in p['feats'] if not f.startswith(('q:', 'x-', 'scaffolding:', 'execution:'))]) for s, p in P.items()}
lit_kinds = [len(r['lits']) + (1 if r['enum_lits'] else 0) for r in rows]
out['query_combination'] = dict(
    distinct_functions=dict(median=statistics.median(df), max=max(df), buckets=dict(collections.Counter(df))),
    literal_kinds_per_query=dict(collections.Counter(lit_kinds)),
    non_execution_emit_features_per_service=dict(collections.Counter(emitfeat.values())),
    top_by_features=sorted(((emitfeat[s], nfeat[s], s, P[s]['status'], P[s]['exp']) for s in P), reverse=True)[:12],
    emit_complexity=dict(collections.Counter(p['complexity'] for p in P.values())),
)
# ---------------- mappings
per_cm = []
for m in mappings:
    for cm in m.get('classMappings') or []:
        feats = set()
        if cm.get('extendsClassMappingId'):
            feats.add('extends')
        if cm.get('filter'):
            feats.add('filter' + ('-via-join' if (cm['filter'].get('joins')) else ''))
        if cm.get('distinct'):
            feats.add('distinct')
        if cm.get('groupBy'):
            feats.add('groupBy')
        if cm.get('primaryKey'):
            feats.add('primaryKey')
        pms = cm.get('propertyMappings') or []
        for pm in pms:
            t = pm['_type']
            if t != 'relationalPropertyMapping':
                feats.add(t.replace('PropertyMapping', ''))
            op = json.dumps(pm.get('relationalOperation'))
            if '"dynaFunc"' in op:
                feats.add('dynafunction')
            if '"elemtWithJoins"' in op:
                feats.add('join')
                if op.count('"name"') > 1:
                    feats.add('join-chain')
            if '"literal"' in op:
                feats.add('literal')
            if pm.get('enumMappingId'):
                feats.add('enum-transform')
            if pm.get('localMappingProperty'):
                feats.add('local-property')
            if pm.get('bindingTransformer'):
                feats.add('binding')
            if pm.get('target'):
                feats.add('target-set')
        per_cm.append((len(feats), len(pms), cm['_type'], cm.get('class'), fp(m), sorted(feats)))
pm_counts = [x[1] for x in per_cm]
out['mapping'] = dict(
    class_mappings=len(per_cm),
    property_mappings_per_class_mapping=dict(median=statistics.median(pm_counts), max=max(pm_counts)),
    features_per_class_mapping=dict(collections.Counter(x[0] for x in per_cm)),
    top_combined=sorted(per_cm, reverse=True)[:8],
    largest_by_pm=sorted(per_cm, key=lambda x: -x[1])[:5],
)
# ---------------- stores
tables = []
for d in dbs:
    for s in d.get('schemas') or []:
        for t in s.get('tables') or []:
            tables.append((len(t.get('columns') or []), len(t.get('primaryKey') or []), len(t.get('milestoning') or []),
                           fp(d) + '.' + s['name'] + '.' + t['name']))
tc = [t[0] for t in tables]
dbsize = sorted(((sum(len(t.get('columns') or []) for s in d.get('schemas') or [] for t in s.get('tables') or []),
                  sum(len(s.get('tables') or []) for s in d.get('schemas') or []), len(d.get('joins') or []),
                  len(d.get('filters') or []), fp(d)) for d in dbs), reverse=True)
join_complexity = []
for d in dbs:
    for j in d.get('joins') or []:
        s = json.dumps(j.get('operation'))
        n_and = s.count('"funcName": "and"')
        n_or = s.count('"funcName": "or"')
        fns = set(__import__('re').findall(r'"funcName": "(\w+)"', s))
        join_complexity.append((len(fns), n_and + n_or, j['name'], fp(d), sorted(fns)))
out['store'] = dict(
    tables=len(tables), columns_per_table=dict(median=statistics.median(tc), p90=pct(tc, .9), max=max(tc),
                                               buckets=dist(tc, [(1, 5, '1-5'), (6, 10, '6-10'), (11, 20, '11-20'), (21, 50, '21-50'), (51, 10**6, '>50')])),
    databases=dbsize[:6],
    join_functions_per_join=dict(collections.Counter(x[0] for x in join_complexity)),
    most_complex_joins=sorted(join_complexity, reverse=True)[:8],
)
# ---------------- classes
cls_rows = []
for c, e in classes.items():
    kinds = set()
    for p in e.get('properties') or []:
        kinds.add(kind_of(tname(p.get('genericType'))))
    mults = {mult(p.get('multiplicity')) for p in e.get('properties') or []}
    feats = set()
    if e.get('qualifiedProperties'):
        feats.add('derived/qualified')
    if e.get('constraints'):
        feats.add('constraints')
    if e.get('superTypes'):
        feats.add('supertype')
    if e.get('stereotypes'):
        feats.add('stereotype')
    if e.get('taggedValues'):
        feats.add('tagged')
    if any(p.get('defaultValue') for p in e.get('properties') or []):
        feats.add('default')
    cls_rows.append((len(kinds) + len(feats) + len(mults), len(e.get('properties') or []), c, sorted(kinds), sorted(feats), sorted(mults)))
out['classes'] = dict(top_combined=sorted(cls_rows, reverse=True)[:8],
                      property_kinds_per_class=dict(collections.Counter(len(x[3]) for x in cls_rows)))
json.dump(out, open(sys.argv[3], 'w', encoding='utf-8'), indent=1, default=str)
for k, v in out.items():
    print('=====', k)
    print(json.dumps(v, default=str)[:2500])
