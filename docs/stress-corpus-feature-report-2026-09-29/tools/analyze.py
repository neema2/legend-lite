"""Element-level census of the stress corpus: services, functions, classes, constraints, mappings, stores.

Env PMCD_JSON=<stress-pmcd.json>. Usage: analyze.py <expectations.tsv> <stress-suites-pass.txt> <analysis.json>
"""
import collections
import json
import sys

from lib import *  # noqa: F401,F403 -- the model index and the walker

EXP, PASS, OUT = sys.argv[1:4]

# ------------------------------------------------------------------------ expectations/status
exp = collections.defaultdict(list)
for line in open(EXP, encoding='utf-8'):
    svc, test, fam, kind, n, detail = line.rstrip('\n').split('\t')
    exp[svc].append(kind)
passed = {l.split(' / ')[0] for l in open(PASS, encoding='utf-8', errors='replace') if ' / ' in l}

R = {}  # results

# ------------------------------------------------------------------------------ services
svc_rows = []
param_types = collections.Counter()
param_mults = collections.Counter()
param_svcs = []
svc_lits = collections.Counter()
svc_lit_svcs = collections.defaultdict(set)
svc_enum_lits = collections.Counter()
svc_funcs = collections.Counter()
svc_funcs_svcs = collections.defaultdict(set)
for s in services:
    ex = s['execution']
    lam = ex.get('func') or {}
    params = lam.get('parameters') or []
    env = lambda_env(lam)
    for p in params:
        t = tname(p.get('genericType'))
        param_types[kind_of(t) if kind_of(t) != 'enum' else 'enum:' + t] += 1
        param_mults[mult(p.get('multiplicity'))] += 1
    if params:
        param_svcs.append((fp(s), [(p['name'], tname(p.get('genericType')), mult(p.get('multiplicity'))) for p in params]))
    w = Walk()
    w.walk(lam, env)
    for k, n in w.lits.items():
        svc_lits[k] += n
        svc_lit_svcs[k].add(fp(s))
    for k, n in w.enum_lits.items():
        svc_enum_lits[k] += n
        svc_lit_svcs['enum'].add(fp(s))
    for k in w.funcs:
        svc_funcs[k] += w.funcs[k]
        svc_funcs_svcs[k].add(fp(s))
    tests = [t for ts in s.get('testSuites') or [] for t in ts.get('tests') or []]
    name = fp(s)
    status = 'PASS' if name in passed else ('no tests' if not tests else 'not passing')
    svc_rows.append(dict(
        svc=name, exec=ex.get('_type'), nparams=len(params), nodes=w.nodes, funcs=dict(w.funcs),
        distinct_funcs=len(w.funcs), lits=dict(w.lits), enum_lits=sum(w.enum_lits.values()),
        max_hops=w.max_hops, to_many=w.to_many_hops, prop_kinds=dict(w.prop_kinds), leaf_types=dict(w.leaf_types),
        tree=w.tree, colspecs=w.colspecs, agg_cols=w.agg_cols, lambda_depth=w.lambda_depth,
        user_funcs=dict(w.user_funcs), status=status, exp=sorted(set(exp.get(name, []))),
        ntests=len(tests)))

R['services'] = dict(
    count=len(services),
    exec_types=collections.Counter(s['execution']['_type'] for s in services),
    with_params=len(param_svcs), param_types=param_types, param_mults=param_mults, param_svcs=param_svcs,
    literal_types=svc_lits, literal_type_services={k: len(v) for k, v in svc_lit_svcs.items()},
    enum_literals=svc_enum_lits, functions=svc_funcs,
    function_services={k: len(v) for k, v in svc_funcs_svcs.items()},
)

# ------------------------------------------------------------------------------ functions
fn_rows = []
fn_funcs = collections.Counter()
fn_lits = collections.Counter()
fn_param_types = collections.Counter()
for f in functions:
    env = {p['name']: tname(p.get('genericType')) for p in f.get('parameters') or []}
    for p in f.get('parameters') or []:
        t = tname(p.get('genericType'))
        fn_param_types[kind_of(t) if kind_of(t) != 'enum' else 'enum'] += 1
    w = Walk()
    w.walk(f.get('body') or [], env)
    fn_funcs.update(w.funcs)
    fn_lits.update(w.lits)
    tests = [t for ts in f.get('tests') or [] for t in ts.get('tests') or []]
    fn_rows.append(dict(fn=fp(f), params=[(p['name'], tname(p.get('genericType')), mult(p.get('multiplicity'))) for p in f.get('parameters') or []],
                        ret=tname(f.get('returnGenericType')), nodes=w.nodes, funcs=dict(w.funcs),
                        lits=dict(w.lits), enum_lits=dict(w.enum_lits), tests=len(tests),
                        stereotypes=[s.get('value') for s in f.get('stereotypes') or []],
                        pre=len(f.get('preConstraints') or []), post=len(f.get('postConstraints') or [])))
R['functions'] = dict(count=len(functions), rows=fn_rows, funcs=fn_funcs, lits=fn_lits, param_types=fn_param_types)

# ------------------------------------------------------------------------------ classes
ptype = collections.Counter()
pmult = collections.Counter()
prim_used = collections.Counter()
derived = qualified = 0
qual_param_types = collections.Counter()
dp_funcs = collections.Counter()
dp_lits = collections.Counter()
default_values = []
stereos = collections.Counter()
tags = collections.Counter()
class_sizes = []
constraints = []
depth_of = {}


def inh_depth(c, seen=()):
    if c in depth_of:
        return depth_of[c]
    if c in seen:
        return 0
    d = 0
    for s in supers.get(c, []):
        if s in classes:
            d = max(d, 1 + inh_depth(s, seen + (c,)))
    depth_of[c] = d
    return d


for c, e in classes.items():
    np_ = len(e.get('properties') or [])
    nq = len(e.get('qualifiedProperties') or [])
    class_sizes.append((np_ + nq, np_, nq, c))
    for s in e.get('stereotypes') or []:
        stereos[(s.get('profile') or '') + '.' + (s.get('value') or '')] += 1
    for tv in e.get('taggedValues') or []:
        tags[(tv.get('tag') or {}).get('profile', '') + '.' + (tv.get('tag') or {}).get('value', '')] += 1
    for p in e.get('properties') or []:
        t = tname(p.get('genericType'))
        k = kind_of(t)
        ptype[k] += 1
        if k in PRIMS:
            prim_used[k] += 1
        pmult[mult(p.get('multiplicity'))] += 1
        if p.get('defaultValue'):
            default_values.append((c, p['name'], t))
        for s in p.get('stereotypes') or []:
            stereos['prop:' + (s.get('profile') or '') + '.' + (s.get('value') or '')] += 1
        for tv in p.get('taggedValues') or []:
            tags['prop:' + (tv.get('tag') or {}).get('profile', '') + '.' + (tv.get('tag') or {}).get('value', '')] += 1
    for q in e.get('qualifiedProperties') or []:
        if q.get('parameters'):
            qualified += 1
            for p in q['parameters']:
                qual_param_types[kind_of(tname(p.get('genericType')))] += 1
        else:
            derived += 1
        env = {'this': c}
        for p in q.get('parameters') or []:
            env[p['name']] = tname(p.get('genericType'))
        w = Walk()
        w.walk(q.get('body') or [], env)
        dp_funcs.update(w.funcs)
        dp_lits.update(w.lits)
    for k in e.get('constraints') or []:
        lam = k.get('functionDefinition') or {}
        w = Walk()
        w.walk(lam.get('body') or [], {'this': c})
        leaf = collections.Counter()
        propsref = set()
        for rt, hops in w.paths:
            propsref.add('.'.join(h[0] for h in hops))
            if hops and hops[-1][1]:
                leaf[kind_of(hops[-1][1][0])] += 1
        constraints.append(dict(cls=c, name=k.get('name'), funcs=dict(w.funcs), lits=dict(w.lits),
                                props=sorted(propsref), leaf_types=dict(leaf), nodes=w.nodes,
                                enforcement=k.get('enforcementLevel'), externalId=k.get('externalId'),
                                message=bool(k.get('messageFunction'))))
for c in classes:
    inh_depth(c)
R['classes'] = dict(
    count=len(classes), property_kinds=ptype, primitive_property_types=prim_used, multiplicities=pmult,
    derived=derived, qualified=qualified, qualified_param_kinds=qual_param_types,
    derived_funcs=dp_funcs, derived_lits=dp_lits, default_values=default_values,
    stereotypes=stereos, tagged_values=tags, largest=sorted(class_sizes, reverse=True)[:10],
    inheritance_depth=collections.Counter(depth_of.values()),
    with_supertypes=sum(1 for c in classes if supers.get(c)),
    constraints=constraints, enums=len(enums), enum_sizes=sorted((len(e.get('values') or []), n) for n, e in enums.items())[-5:],
    associations=len(assocs), assoc_mults=collections.Counter(
        '%s-%s' % (mult(a['properties'][0].get('multiplicity')), mult(a['properties'][1].get('multiplicity')))
        for a in assocs if len(a.get('properties') or []) == 2),
    assoc_qualified=sum(1 for a in assocs if a.get('qualifiedProperties')),
    measures=len(measures),
)

# ------------------------------------------------------------------------------ mappings
cm_types = collections.Counter()
pm_types = collections.Counter()
m2m = []
op_types = collections.Counter()
relational_features = collections.Counter()
dyna_in_pm = collections.Counter()
enum_src = collections.Counter()
incl_subst = 0
local_props = 0
mapping_tests = 0
legacy_mapping_tests = 0
mapping_sizes = []
agg_aware = 0


def walk_rel_op(op, where):
    if not isinstance(op, dict):
        return
    t = op.get('_type')
    if t == 'dynaFunc':
        dyna_in_pm[op.get('funcName')] += 1
        for p in op.get('parameters') or []:
            walk_rel_op(p, where)
    elif t == 'elemtWithJoins':
        js = op.get('joins') or []
        relational_features['join chain length %d' % len(js)] += 1
        for j in js:
            if j.get('joinType'):
                relational_features['typed join hop ' + j['joinType']] += 1
        if op.get('relationalElement'):
            walk_rel_op(op['relationalElement'], where)
    elif t == 'literal':
        relational_features['literal ' + type(op.get('value')).__name__] += 1
    elif t == 'literalList':
        relational_features['literal list'] += 1


for m in mappings:
    n_cm = len(m.get('classMappings') or [])
    n_pm = 0
    for inc in m.get('includedMappings') or []:
        if inc.get('sourceDatabasePath') or inc.get('targetDatabasePath') or inc.get('storeSubstitutions'):
            incl_subst += 1
    if m.get('testSuites'):
        mapping_tests += len(m['testSuites'])
    if m.get('tests'):
        legacy_mapping_tests += len(m['tests'])
    for em in m.get('enumerationMappings') or []:
        for v in em.get('enumValueMappings') or []:
            for sv in v.get('sourceValues') or []:
                enum_src[sv.get('_type')] += 1
    for cm in m.get('classMappings') or []:
        t = cm.get('_type')
        cm_types[t] += 1
        if t == 'relational':
            if cm.get('distinct'):
                relational_features['~distinct'] += 1
            if cm.get('groupBy'):
                relational_features['~groupBy'] += 1
            if cm.get('filter'):
                f = cm['filter']
                relational_features['~filter' + (' via join' if f.get('joins') else '')] += 1
            if cm.get('primaryKey'):
                relational_features['~primaryKey'] += 1
            if cm.get('extendsClassMappingId'):
                relational_features['extends'] += 1
            if cm.get('root'):
                relational_features['root (*)'] += 1
            if cm.get('mainTable'):
                relational_features['~mainTable'] += 1
        if t == 'operation':
            op_types[cm.get('operation')] += 1
        if t == 'mergeOperation':
            op_types['MERGE'] += 1
        if t == 'aggregationAware':
            agg_aware += 1
        if t == 'pureInstance':
            m2m.append(dict(mapping=fp(m), cls=cm.get('class'), src=cm.get('srcClass'), filter=bool(cm.get('filter')),
                            pms=len(cm.get('propertyMappings') or [])))
        for pm in cm.get('propertyMappings') or []:
            n_pm += 1
            pt = pm.get('_type')
            pm_types[pt] += 1
            if pm.get('localMappingProperty'):
                local_props += 1
            if pm.get('enumMappingId'):
                relational_features['enum transformer'] += 1
            if pm.get('bindingTransformer'):
                relational_features['binding transformer'] += 1
            if pm.get('target'):
                relational_features['property with explicit target set id'] += 1
            walk_rel_op(pm.get('relationalOperation'), 'pm')
            if pt in ('embeddedPropertyMapping', 'otherwiseEmbeddedPropertyMapping'):
                for sub in (pm.get('classMapping') or {}).get('propertyMappings') or pm.get('propertyMappings') or []:
                    walk_rel_op(sub.get('relationalOperation'), 'embedded')
    mapping_sizes.append((n_pm, n_cm, fp(m)))

for am in [a for m in mappings for a in m.get('associationMappings') or []]:
    relational_features['association mapping ' + am.get('_type')] += 1

R['mappings'] = dict(
    count=len(mappings), class_mapping_types=cm_types, property_mapping_types=pm_types,
    relational_features=relational_features, dynafunctions_in_property_mappings=dyna_in_pm,
    operation_types=op_types, aggregation_aware=agg_aware, m2m=m2m, enum_source_value_types=enum_src,
    includes=sum(len(m.get('includedMappings') or []) for m in mappings), includes_with_substitution=incl_subst,
    local_properties=local_props, mapping_test_suites=mapping_tests, legacy_mapping_tests=legacy_mapping_tests,
    largest=sorted(mapping_sizes, reverse=True)[:8],
    enumeration_mappings=sum(len(m.get('enumerationMappings') or []) for m in mappings),
)

# ------------------------------------------------------------------------------ stores
col_types = collections.Counter()
tables = []
pk = collections.Counter()
views = []
join_ops = collections.Counter()
join_cols = collections.Counter()
join_self = 0
filters = collections.Counter()
milestoning = collections.Counter()
schemas = collections.Counter()
nullable = collections.Counter()


def op_funcs(op, acc):
    if isinstance(op, dict):
        if op.get('_type') == 'dynaFunc':
            acc[op.get('funcName')] += 1
        if op.get('_type') == 'column' and op.get('tableAlias') == '{target}':
            acc['{target}'] += 1
        for v in op.values():
            op_funcs(v, acc)
    elif isinstance(op, list):
        for v in op:
            op_funcs(v, acc)


for d in dbs:
    for s in d.get('schemas') or []:
        schemas['default' if s.get('name') == 'default' else 'named'] += 1
        for t in s.get('tables') or []:
            cols = t.get('columns') or []
            tables.append((len(cols), fp(d) + '.' + s.get('name') + '.' + t.get('name')))
            for c in cols:
                ct = (c.get('type') or {}).get('_type')
                col_types[ct] += 1
                nullable['NOT NULL' if c.get('nullable') is False else 'nullable'] += 1
            k = len(t.get('primaryKey') or [])
            pk['none' if k == 0 else ('single' if k == 1 else 'composite(%d)' % k)] += 1
            for ms in t.get('milestoning') or []:
                milestoning[ms.get('_type')] += 1
        for v in s.get('views') or []:
            views.append(dict(name=v.get('name'), filter=bool(v.get('filter')), groupBy=len(v.get('groupBy') or []),
                              distinct=bool(v.get('distinct')), cols=len(v.get('columnMappings') or []),
                              pk=len(v.get('primaryKey') or [])))
    for j in d.get('joins') or []:
        acc = collections.Counter()
        op_funcs(j.get('operation'), acc)
        for k in acc:
            join_ops[k] += 1
        if acc.get('{target}'):
            join_self += 1
        join_cols[sum(acc[k] for k in ('equal',))] += 1
    for f in d.get('filters') or []:
        filters[f.get('_type')] += 1
R['stores'] = dict(
    databases=len(dbs), tables=len(tables), columns=sum(n for n, _ in tables), column_types=col_types,
    nullability=nullable, primary_keys=pk, widest_tables=sorted(tables, reverse=True)[:8], views=views,
    joins=sum(len(d.get('joins') or []) for d in dbs), join_dynafunctions=join_ops,
    joins_by_equality_count=join_cols, self_joins=join_self, filters=filters, milestoning=milestoning,
    schemas=schemas, includes=sum(len(d.get('includedStores') or []) for d in dbs),
    tabular_functions=sum(len(s.get('tabularFunctions') or []) for d in dbs for s in d.get('schemas') or []),
)

R['svc_rows'] = svc_rows
json.dump(R, open(OUT, 'w', encoding='utf-8'), indent=1, default=lambda o: dict(o) if isinstance(o, collections.Counter) else str(o))
print('written', OUT)
