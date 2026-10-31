"""Per-service feature profile in EMIT's taxonomy (plus query-shape features EMIT does not name).

For each service: resolve the query's root class and every navigated property through the
service mapping's include closure, and collect the mapping/store/milestoning/grammar/execution
features the query actually reaches. Then roll up per feature and per EMIT complexity class.

Env PMCD_JSON=<pmcd.json>; args: <expectations.tsv> <pass.txt> <out.json> [<engine-pass.txt>]
<pass.txt> is lite's (gate 10's stress-suites-pass.txt). <engine-pass.txt>, when given, is legend-engine's
(scripts/corpus/run.py writes it in the same form), and each service gains an `engine` status beside lite's: the
plan's measures are judged on the engine (docs/STRESS_CORPUS_PLAN_2026_10_19.md).
"""
import collections
import json
import sys

from lib import (E, classes, enums, measures, assocs, functions, services, mappings, dbs, props, supers, lookup,
                 Walk, tname, mult, kind_of, fp, lambda_env)

EXP, PASS, OUT = sys.argv[1:4]
ENGINE_PASS = sys.argv[4] if len(sys.argv) > 4 else None
exp = collections.defaultdict(set)
for line in open(EXP, encoding='utf-8'):
    s, t, fam, kind, n, detail = line.rstrip('\n').split('\t')
    exp[s].add(kind)


def pass_list(path):
    return {l.split(' / ')[0] for l in open(path, encoding='utf-8', errors='replace') if ' / ' in l}


passed = pass_list(PASS)
engine_passed = pass_list(ENGINE_PASS) if ENGINE_PASS else None

MAP = {fp(m): m for m in mappings}
# Profiles the corpus declares, as against the platform's (meta::pure::profiles::*): EMIT's grammar:profile is a
# model's own profile, and a service reaches it when a class it touches carries one of its stereotypes or tags.
CORPUS_PROFILES = {fp(e) for e in E if e['_type'] == 'profile'}
DB = {fp(d): d for d in dbs}
CONN = {fp(e): e for e in E if e['_type'] == 'connection'}
RT = {fp(e): e for e in E if e['_type'] == 'runtime'}


def chain_mappings(rt):
    """The mappings the ModelChainConnections of a runtime name. An M2M set's source class is mapped there, not in the
    service's mapping, when the M2M mapping is chained to its source through the runtime (added 2026-10-30, the
    improvement plan's Phase 5: m2m::SourceChain, stress::TradeChain, surface::GapChain)."""
    out = []

    def walk(o):
        if isinstance(o, dict):
            if 'chain' in str(o.get('_type') or '').lower() and isinstance(o.get('mappings'), list):
                out.extend(m if isinstance(m, str) else (m or {}).get('path') for m in o['mappings'])
            for v in o.values():
                if isinstance(v, str) and v in CONN:
                    walk(CONN[v].get('connectionValue') or {})
                else:
                    walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(RT.get(rt) or {})
    return [m for m in out if m]


# the chained mappings of the service being profiled (set per service below)
CHAIN = []

# ------------------------------------------------------------------ store facts
table_facts = {}   # (db, schema, table) -> set(features)
view_names = set()
join_facts = {}    # (db, name) -> set(features)
filter_facts = {}  # (db, name) -> set


def dyna_names(op, acc):
    if isinstance(op, dict):
        if op.get('_type') == 'dynaFunc':
            acc.add(op.get('funcName'))
        if op.get('_type') == 'column' and op.get('tableAlias') == '{target}':
            acc.add('{target}')
        if op.get('_type') == 'column':
            t = op.get('table') or {}
            acc.add('schema:' + str(t.get('schema')))
        for v in op.values():
            dyna_names(v, acc)
    elif isinstance(op, list):
        for v in op:
            dyna_names(v, acc)


for name, d in DB.items():
    for s in d.get('schemas') or []:
        for t in s.get('tables') or []:
            f = set()
            for ms in t.get('milestoning') or []:
                f.add({'businessMilestoning': 'milestoning:business-temporal',
                       'processingMilestoning': 'milestoning:processing-temporal',
                       'businessSnapshotMilestoning': 'milestoning:business-temporal',
                       'processingSnapshotMilestoning': 'milestoning:processing-temporal'}[ms['_type']])
                f.add('x-store:' + ms['_type'])
            if s.get('name') != 'default':
                f.add('x-store:named-schema')
            for c in t.get('columns') or []:
                ct = (c.get('type') or {}).get('_type')
                if ct in ('SemiStructured', 'Json'):
                    f.add('store:relational-semistructured')
            table_facts[(name, s.get('name'), t.get('name'))] = f
        for v in s.get('views') or []:
            f = {'store:relational-inline-view'}
            if v.get('groupBy'):
                f.add('x-store:view-groupBy')
            if v.get('distinct'):
                f.add('x-store:view-distinct')
            if v.get('filter'):
                f.add('x-store:view-filter')
            table_facts[(name, s.get('name'), v.get('name'))] = f
            view_names.add((name, s.get('name'), v.get('name')))
    for j in d.get('joins') or []:
        acc = set()
        dyna_names(j.get('operation'), acc)
        f = set()
        fn = {a for a in acc if not a.startswith('schema:') and a != '{target}'}
        if fn - {'equal', 'and'}:
            f.add('store:relational-dyna-function')
        if 'or' in fn:
            f.add('x-join:or')
        if fn & {'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual', 'notEqual', 'notEqualAnsi'}:
            f.add('x-join:non-equi')
        if '{target}' in acc:
            f.add('x-join:self')
        if len({a for a in acc if a.startswith('schema:')}) > 1:
            f.add('store:relational-cross-schema')
        eqs = json.dumps(j.get('operation')).count('"funcName": "equal"')
        if eqs > 1:
            f.add('x-join:multi-column')
        join_facts[(name, j.get('name'))] = f
    for flt in d.get('filters') or []:
        f = {'store:relational-filter'}
        if flt.get('_type') == 'multigrain':
            f.add('x-store:multigrain-filter')
        filter_facts[(name, flt.get('name'))] = f
# included stores: a join/filter referenced through an including db is still the same object


# ------------------------------------------------------------------ mapping closure & set index
closure_cache = {}


def closure(m):
    if m in closure_cache:
        return closure_cache[m]
    out, stack = [], [m]
    seen = set()
    while stack:
        x = stack.pop()
        if x in seen or x not in MAP:
            continue
        seen.add(x)
        out.append(x)
        for inc in MAP[x].get('includedMappings') or []:
            stack.append(inc.get('includedMapping'))
    closure_cache[m] = out
    return out


index_cache = {}


def index(m):
    """class -> [set impl], id -> set impl, association property -> [(assoc mapping, pm)]"""
    if m in index_cache:
        return index_cache[m]
    by_class = collections.defaultdict(list)
    by_id = {}
    assoc_pm = collections.defaultdict(list)
    subst = False
    for mm in closure(m):
        mp = MAP[mm]
        for inc in mp.get('includedMappings') or []:
            if inc.get('storeSubstitutions') or inc.get('sourceDatabasePath'):
                subst = True
        for cm in mp.get('classMappings') or []:
            cm = dict(cm)
            cm['__mapping'] = mm
            by_class[cm.get('class')].append(cm)
            if cm.get('id'):
                by_id[cm['id']] = cm
            else:
                by_id[cm.get('class', '').replace('::', '_')] = cm
        for am in mp.get('associationMappings') or []:
            for pm in am.get('propertyMappings') or []:
                assoc_pm[(pm.get('property') or {}).get('property')].append((am, pm))
            # A ModelJoin holds a join condition and no property mappings: registered under both ends of its
            # association, or no navigation finds it (added 2026-10-30, the improvement plan's Phase 6)
            if am.get('_type') == 'modelJoin':
                path = am.get('association')
                path = path.get('path') if isinstance(path, dict) else path
                for a in assocs:
                    if fp(a) == path:
                        for ap in a.get('properties') or []:
                            assoc_pm[ap.get('name')].append((am, {'property': {'property': ap.get('name')}}))
    index_cache[m] = (by_class, by_id, assoc_pm, subst)
    return index_cache[m]


def root_set(m, cls):
    by_class, by_id, _, _ = index(m)
    cands = by_class.get(cls) or []
    if not cands:
        return None
    roots = [c for c in cands if c.get('root')]
    return (roots or cands)[0]


def pm_in(setimpl, prop, m):
    """Property mapping for prop in a set implementation, following extends to the base set."""
    by_class, by_id, _, _ = index(m)
    seen = set()
    s = setimpl
    while s is not None and id(s) not in seen:
        seen.add(id(s))
        for pm in s.get('propertyMappings') or []:
            if (pm.get('property') or {}).get('property') == prop:
                return pm, s
        s = by_id.get(s.get('extendsClassMappingId')) if s.get('extendsClassMappingId') else None
    return None, None


def op_feats(op, feats, tables_touched, db_hint):
    """Relational operation: joins, dyna functions, literals, columns."""
    if not isinstance(op, dict):
        return
    t = op.get('_type')
    if t == 'column':
        tb = op.get('table') or {}
        tables_touched.add((tb.get('database') or db_hint, tb.get('schema'), tb.get('table')))
    elif t == 'dynaFunc':
        feats.add('store:relational-dyna-function')
        feats.add('x-mapping:dynafunction-property')
        for p in op.get('parameters') or []:
            op_feats(p, feats, tables_touched, db_hint)
    elif t == 'elemtWithJoins':
        js = op.get('joins') or []
        feats.add('store:relational-inner-join')   # EMIT's tag for "a Join traversed"
        if len(js) > 1:
            feats.add('store:relational-nested-join')
        for j in js:
            jt = (j.get('joinType') or '').upper()
            if jt == 'INNER':
                feats.add('x-join:typed-inner')
            elif jt in ('OUTER', 'LEFT_OUTER'):
                feats.add('store:relational-left-outer-join')
            elif jt == 'RIGHT_OUTER':
                feats.add('store:relational-right-outer-join')
            elif jt == 'FULL_OUTER':
                feats.add('store:relational-outer-join')
            feats |= join_facts.get((j.get('db'), j.get('name')), set())
        if op.get('relationalElement'):
            op_feats(op['relationalElement'], feats, tables_touched, db_hint)
    elif t == 'literal':
        feats.add('mapping:relational-literal')
    elif t == 'literalList':
        feats.add('mapping:relational-literal-list')


def set_feats(s, m, feats, tables):
    t = s.get('_type')
    if t == 'relational':
        feats.add('scaffolding:relational-mapping')
        if s.get('distinct'):
            feats.add('mapping:relational-distinct')
        if s.get('groupBy'):
            feats.add('mapping:relational-group-by')
        if s.get('primaryKey'):
            feats.add('mapping:relational-primary-key')
        f = s.get('filter')
        if f:
            fl = f.get('filter') or {}
            feats |= filter_facts.get((fl.get('db'), fl.get('name')), {'store:relational-filter'})
            if f.get('joins'):
                feats.add('store:relational-cross-table-filter')
                for j in f['joins']:
                    feats |= join_facts.get((j.get('db'), j.get('name')), set())
                    if (j.get('joinType') or '').upper() == 'INNER':
                        feats.add('x-join:typed-inner')
        mt = s.get('mainTable')
        if mt:
            key = (mt.get('database'), mt.get('schema'), mt.get('table'))
            tables.add(key)
        if s.get('extendsClassMappingId'):
            by_class, by_id, _, _ = index(m)
            base = by_id.get(s['extendsClassMappingId'])
            bmt = (base or {}).get('mainTable') or {}
            if mt and bmt and (mt.get('table') != bmt.get('table')):
                feats.add('mapping:relational-joined-table-inheritance')
            else:
                feats.add('mapping:relational-single-table-inheritance')
            if base and base.get('mainTable'):
                b = base['mainTable']
                tables.add((b.get('database'), b.get('schema'), b.get('table')))
    elif t == 'operation':
        op = s.get('operation')
        by_class, by_id, _, _ = index(m)
        # A union of relation sets, or of a relation set and a relational one, is EMIT's relation-union, which its
        # models tag instead of store-union (added 2026-10-30, the improvement plan's Phase 6)
        if op == 'STORE_UNION' and any((by_id.get(p) or {}).get('_type') == 'relation' for p in s.get('parameters') or []):
            feats.add('mapping:relation-union')
        else:
            feats.add({'STORE_UNION': 'mapping:store-union', 'ROUTER_UNION': 'mapping:router-union',
                       'INHERITANCE': 'mapping:operation-mapping'}.get(op, 'mapping:operation-mapping'))
        for p in s.get('parameters') or []:
            sub = by_id.get(p)
            if sub is not None and sub is not s:
                set_feats(sub, m, feats, tables)
    elif t == 'mergeOperation':
        feats.add('mapping:operation-mapping-merge')
        if s.get('validationFunction'):
            feats.add('mapping:operation-mapping-merge-validation')
    elif t == 'aggregationAware':
        feats.add('mapping:aggregation-aware-mapping')
        ms = s.get('mainSetImplementation')
        if ms:
            set_feats(ms, m, feats, tables)
    elif t == 'pureInstance':
        feats.add('scaffolding:m2m-mapping')
        if s.get('filter'):
            feats.add('x-mapping:m2m-filter')
        src = s.get('srcClass')
        if src and any(root_set(x, src) is not None and root_set(x, src).get('_type') == 'relational'
                       for x in [m] + CHAIN):
            feats.add('mapping:m2m-chained-relational')
    elif t == 'relation':
        feats.add('scaffolding:relation-mapping')
        feats.add('scaffolding:relation-function')
        # EMIT's relation-mapping features of the set itself (added 2026-10-30, the improvement plan's Phase 6): the
        # `~src` form, an explicit primary key, and what the source relation does -- its function's body, or the
        # `~src` expression and the functions it calls
        if s.get('sourceLambda'):
            feats.add('mapping:relation-src')
        if s.get('primaryKey'):
            feats.add('mapping:relation-primary-key')
        feats.update(relation_source_feats(s))


def relation_function(name):
    """The function a `~func` descriptor or a call names: `pkg::f():Relation<Any>[1]` or `pkg::f`, whose protocol name
    carries its signature after the name (`f__Relation_1_`)."""
    name = name.split('(')[0]
    return [f for f in functions if fp(f) == name or fp(f).startswith(name + '_')]


def relation_source_feats(s):
    """mapping:relation-filter, -group-by and -window-function, from what the source relation applies: `->filter`,
    `->groupBy`, and `->extend(over(...), ...)`."""
    found = set()
    seen = set()

    def walk(o):
        if isinstance(o, dict):
            if o.get('_type') == 'func':
                fn = o.get('function')
                if fn == 'filter':
                    found.add('mapping:relation-filter')
                elif fn == 'groupBy':
                    found.add('mapping:relation-group-by')
                elif fn == 'over':
                    found.add('mapping:relation-window-function')
                elif fn and '::' in fn and fn not in seen:
                    seen.add(fn)
                    for f in relation_function(fn):
                        walk(f.get('body'))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    rf = s.get('relationFunction')
    rf = rf.get('path') if isinstance(rf, dict) else rf
    if rf:
        for f in relation_function(rf):
            walk(f.get('body'))
    walk(s.get('sourceLambda'))
    return found


def reads_derived(transform, src_cls):
    """Whether an M2M transform reads a derived or qualified property of its source class, `$src.ref` where the source
    declares `ref() { ... }` (EMIT's m2m-derived-source-property; added 2026-10-30, the improvement plan's Phase 5)."""
    found = []

    def walk(o):
        if isinstance(o, dict):
            ps = o.get('parameters') or []
            if (o.get('_type') == 'property' and ps and isinstance(ps[0], dict) and ps[0].get('_type') == 'var'
                    and ps[0].get('name') == 'src'):
                info = lookup(src_cls, o.get('property'))
                if info and info[2] in ('derived', 'qualified'):
                    found.append(o.get('property'))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(transform)
    return bool(found)


def prop_feats(pm, feats, tables, m, db_hint, src_cls=None):
    t = pm.get('_type')
    if pm.get('localMappingProperty'):
        feats.add({'relationalPropertyMapping': 'x-mapping:relational-local-property',
                   'purePropertyMapping': 'mapping:m2m-local-property',
                   'relationFunctionPropertyMapping': 'mapping:relation-local-property'}.get(t, 'x-mapping:local-property'))
    if pm.get('enumMappingId'):
        feats.add('mapping:enumeration-mapping')
    if pm.get('bindingTransformer'):
        feats.add('execution:binding')
        # a relation mapping's binding transformer is EMIT's relation-binding-transformer (added 2026-10-30, the
        # improvement plan's Phase 6)
        feats.add('mapping:relation-binding-transformer' if t == 'relationFunctionPropertyMapping'
                  else 'store:relational-semistructured-binding')
    if pm.get('target'):
        feats.add('x-mapping:explicit-target-set')
    if t == 'relationalPropertyMapping':
        op_feats(pm.get('relationalOperation'), feats, tables, db_hint)
        # A `scope([db]T) ( p: COL )` block: the protocol keeps no scope, but its column's table is the one the block
        # names, on the block's line, above the property's own -- written plainly, `p: [db]T.COL`, the table sits on
        # the property's line (relational-main-table-alias, added 2026-10-29)
        op = pm.get('relationalOperation') or {}
        line = (pm.get('property') or {}).get('sourceInformation', {}).get('startLine')
        tline = ((op.get('table') or {}).get('sourceInformation') or {}).get('startLine')
        if op.get('_type') == 'column' and line and tline and tline < line:
            feats.add('mapping:relational-main-table-alias')
    elif t == 'embeddedPropertyMapping':
        feats.add('mapping:relational-embedded')
    elif t == 'otherwiseEmbeddedPropertyMapping':
        feats.add('mapping:relational-otherwise-embedded')
        o = pm.get('otherwisePropertyMapping')
        if o:
            op_feats(o.get('relationalOperation'), feats, tables, db_hint)
    elif t == 'inlineEmbeddedPropertyMapping':
        feats.add('mapping:relational-inline-embedded')
    elif t == 'purePropertyMapping':
        feats.add('mapping:m2m-transform')
        s = json.dumps(pm.get('transform'))
        if '"function": "new"' in s:
            feats.add('mapping:m2m-target-instantiation')
        if pm.get('explodeProperty'):
            feats.add('x-mapping:m2m-explode')
        # added 2026-10-30, the improvement plan's Phase 5
        if src_cls and reads_derived(pm.get('transform'), src_cls):
            feats.add('mapping:m2m-derived-source-property')
        prop = pm.get('property') or {}
        info = lookup(prop.get('class'), prop.get('property')) if prop.get('class') else None
        # a class-typed property mapped with no set of its own and none for its class: the source object passes through
        if info and info[0] in classes and not pm.get('target') and root_set(m, info[0]) is None:
            feats.add('mapping:m2m-complex-property-passthrough')
    elif t == 'relationFunctionPropertyMapping':
        # a `$src` expression on the right-hand side rather than a bare column (added 2026-10-30, the improvement plan's
        # Phase 6)
        if pm.get('valueFn'):
            feats.add('mapping:relation-expression-rhs')
    elif t == 'relationFunctionEmbeddedPropertyMapping':
        # `prop ( ... )`, or `prop () Inline [id]`: the inline form names a set and maps nothing of its own
        feats.add('mapping:relation-inline-embedded' if pm.get('id') and not pm.get('propertyMappings')
                  else 'mapping:relation-embedded')



# (owner class, property) -> association path, so an association end is matched by its association
assoc_owner = {}
for a in assocs:
    ps = a.get('properties') or []
    if len(ps) == 2:
        t0, t1 = tname(ps[0].get('genericType')), tname(ps[1].get('genericType'))
        assoc_owner[(t1, ps[0]['name'])] = fp(a)
        assoc_owner[(t0, ps[1]['name'])] = fp(a)


def owning_assoc(cls, pname, seen=None):
    seen = seen or set()
    if cls is None or cls in seen:
        return None
    seen.add(cls)
    if (cls, pname) in assoc_owner:
        return assoc_owner[(cls, pname)]
    for s in supers.get(cls, []):
        r = owning_assoc(s, pname, seen)
        if r:
            return r
    return None


def assoc_relation_feats(am, apm, m, feats):
    """An XStore or a ModelJoin navigated (added 2026-10-30, the improvement plan's Phase 6). A ModelJoin is EMIT's
    relation-model-join; an XStore between relation sets is its relation-xstore-association, and any other XStore its
    cross-store. Either reaches the relation-local-property its predicate reads: a `+prop` of an end's set."""
    by_class, by_id, _, _ = index(m)
    if am.get('_type') == 'modelJoin':
        feats.add('mapping:relation-model-join')
        ends = [s for c in (am.get('stores') or []) for s in [by_id.get(c)] if s] or [
            s for ss in by_class.values() for s in ss if s.get('_type') == 'relation']
        pred = am.get('joinCondition')
    else:
        ends = [by_id.get(apm.get('source')), by_id.get(apm.get('target'))]
        relation = [e for e in ends if e is not None and e.get('_type') == 'relation']
        feats.add('mapping:relation-xstore-association' if relation and len(relation) == len(ends)
                  else 'mapping:cross-store')
        pred = apm.get('crossExpression')
    read = set()

    def walk(o):
        if isinstance(o, dict):
            if o.get('_type') == 'property':
                read.add(o.get('property'))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(pred)
    for e in ends:
        for pm in (e or {}).get('propertyMappings') or []:
            if pm.get('localMappingProperty') and (pm.get('property') or {}).get('property') in read:
                feats.add({'relationFunctionPropertyMapping': 'mapping:relation-local-property'}.get(
                    pm.get('_type'), 'x-mapping:local-property'))


def assoc_candidates(assoc_pm, cls, pname, cur_set):
    path = owning_assoc(cls, pname)
    out = [(am, apm) for am, apm in assoc_pm.get(pname) or []
           if ((am.get('association') or {}).get('path') if isinstance(am.get('association'), dict) else am.get('association')) == path]
    sid = (cur_set or {}).get('id')
    pref = [x for x in out if sid and x[1].get('source') == sid]
    return pref or out

# ------------------------------------------------------------------ per-service profile
DOMAINS = ('grammar', 'mapping', 'store', 'milestoning', 'execution', 'persistence')
rows = []
for s in services:
    name = fp(s)
    ex = s['execution']
    feats = set(['execution:service'])
    tables = set()
    if ex.get('_type') == 'pureMultiExecution':
        feats.add('execution:multi-execution-service')
        mp = (ex.get('executionParameters') or [{}])[0].get('mapping')
    else:
        mp = ex.get('mapping')
    lam = ex.get('func') or {}
    CHAIN[:] = chain_mappings((ex.get('runtime') or {}).get('runtime')) if ex.get('_type') == 'pureSingleExecution' else []
    if lam.get('parameters'):
        feats.add('execution:service-parameter')
    if s.get('postValidations'):
        feats.add('execution:post-validation')
    for ts in s.get('testSuites') or []:
        feats.add('execution:service-test')
        td = ts.get('testData') or {}
        for cd in td.get('connectionsTestData') or []:
            k = (cd.get('data') or {}).get('_type')
            feats.add('execution:test-data')
            if k == 'reference':
                feats.add('execution:shared-test-data')
                feats.add('execution:data-element')
            elif k == 'externalFormat':
                feats.add('execution:external-format')
    w = Walk()
    w.walk(lam, lambda_env(lam))
    f = w.funcs
    # query-shape features (not in EMIT's taxonomy): prefix q:
    for fn in ('filter', 'project', 'groupBy', 'sort', 'limit', 'graphFetch', 'serialize', 'distinct', 'take',
               'drop', 'slice', 'first', 'olapGroupBy', 'extend', 'join', 'restrict', 'renameColumns', 'exists',
               'isEmpty', 'isNotEmpty', 'map', 'if', 'in', 'or', 'not', 'subType', 'toOne'):
        if f.get(fn):
            feats.add('q:' + fn)
    # EMIT's execution:graph-fetch (added to emit.md after 2026-09-29): a graph fetch query executed in a test
    if f.get('graphFetch') and s.get('testSuites'):
        feats.add('execution:graph-fetch')
    for agg in ('count', 'sum', 'max', 'min', 'average', 'stdDev', 'median', 'distinctCount', 'uniqueValueOnly'):
        if f.get(agg):
            feats.add('q:agg:' + agg)
    if w.user_funcs:
        feats.add('grammar:function')
    if w.enum_lits:
        feats.add('grammar:enumeration')
        feats.add('q:enum-literal')
    for lt in w.lits:
        feats.add('q:literal:' + lt)
    if w.tree:
        feats.add('q:graphFetch-depth-%d' % w.tree['depth'])
        if w.tree.get('subtypes'):
            feats.add('q:graphFetch-subtype')
        if w.tree.get('qualified'):
            feats.add('q:graphFetch-qualified-property')
    if f.get('getAllVersions'):
        feats.add('milestoning:all-versions-query')
    if f.get('getAllVersionsInRange'):
        feats.add('milestoning:all-versions-in-range-query')
    getall = [n for n in [lam] if n]
    # point-in-time: getAll(date) or a milestoned property called with a date argument
    js = json.dumps(lam)
    if w.funcs.get('getAll') and ('"strictDate"' in js or '"dateTime"' in js or '"latestDate"' in js):
        feats.add('milestoning:point-in-time-query')
    # classes and mapping resolution
    root_cls = None
    for p in (lam.get('body') or [])[:1]:
        pass
    touched_classes = set()
    nav_assoc_depth = 0
    for rt, hops in w.paths:
        cur_cls = rt
        if rt:
            touched_classes.add(rt)
        cur_set = root_set(mp, rt) if (mp and rt) else None
        if cur_set is not None:
            set_feats(cur_set, mp, feats, tables)
        assoc_depth = 0
        for i, (pname, info) in enumerate(hops):
            if info is None:
                break
            ptype, pmult, pkind = info
            if pkind == 'derived':
                feats.add('grammar:derived-property')
            elif pkind == 'qualified':
                feats.add('grammar:qualified-property')
            if pkind == 'assoc':
                assoc_depth += 1
                feats.add('grammar:association')
            if ptype in enums:
                feats.add('grammar:enumeration')
            if ptype in measures or (ptype and '~' in ptype):
                feats.add('grammar:measure')
            if pmult and pmult.endswith('*]'):
                feats.add('q:to-many-navigation')
            nxt_set = None
            if cur_set is not None and pkind in ('prop', 'assoc'):
                pm, owner = pm_in(cur_set, pname, mp)
                db_hint = ((cur_set.get('mainTable') or {}).get('database'))
                if pm is not None:
                    prop_feats(pm, feats, tables, mp, db_hint, (owner or cur_set).get('srcClass'))
                    if pm.get('_type') in ('embeddedPropertyMapping', 'otherwiseEmbeddedPropertyMapping'):
                        nxt_set = dict(pm.get('classMapping') or {})
                        nxt_set['_type'] = 'embedded'
                        nxt_set['mainTable'] = cur_set.get('mainTable')
                    elif pm.get('_type') == 'inlineEmbeddedPropertyMapping':
                        by_class, by_id, _, _ = index(mp)
                        nxt_set = by_id.get(pm.get('setImplementationId'))
                    elif pm.get('_type') == 'relationFunctionEmbeddedPropertyMapping':
                        # a relation mapping's embedded property maps its leaves in place; the inline form names a set
                        by_class, by_id, _, _ = index(mp)
                        if pm.get('id') and not pm.get('propertyMappings'):
                            nxt_set = by_id.get(pm['id'])
                        else:
                            nxt_set = {'_type': 'relation-embedded', 'propertyMappings': pm.get('propertyMappings')}
                    elif pm.get('target'):
                        by_class, by_id, _, _ = index(mp)
                        nxt_set = by_id.get(pm['target'])
                else:
                    by_class, by_id, assoc_pm, _ = index(mp)
                    cands = assoc_candidates(assoc_pm, cur_cls, pname, cur_set)
                    for am, apm in cands:
                        if am.get('_type') == 'relational':
                            feats.add('mapping:relational-association-implementation')
                            op_feats(apm.get('relationalOperation'), feats, tables, db_hint)
                        elif am.get('_type') in ('xStore', 'modelJoin'):
                            assoc_relation_feats(am, apm, mp, feats)
                        if apm.get('target'):
                            nxt_set = by_id.get(apm['target'])
                        break
            if nxt_set is None and ptype in classes and mp:
                nxt_set = root_set(mp, ptype)
            if ptype in classes:
                touched_classes.add(ptype)
            cur_cls = ptype
            if nxt_set is not None and nxt_set.get('_type') in ('relational', 'operation', 'pureInstance', 'aggregationAware', 'mergeOperation', 'relation'):
                set_feats(nxt_set, mp, feats, tables)
            cur_set = nxt_set
        nav_assoc_depth = max(nav_assoc_depth, assoc_depth)
    if nav_assoc_depth >= 2:
        feats.add('grammar:nested-association')
    # graph fetch trees navigate without $x paths; resolve the tree's property chains too
    if w.tree and mp:
        def tree_walk(node, cls, cur_set, depth):
            for st in node.get('subTrees') or []:
                pname = st.get('property')
                info = lookup(cls, pname)
                if not info:
                    continue
                ptype, pmult, pkind = info
                if pkind == 'derived':
                    feats.add('grammar:derived-property')
                elif pkind == 'qualified':
                    feats.add('grammar:qualified-property')
                if pkind == 'assoc':
                    feats.add('grammar:association')
                    if depth >= 1:
                        feats.add('grammar:nested-association')
                if ptype in enums:
                    feats.add('grammar:enumeration')
                nxt = None
                if cur_set is not None and pkind in ('prop', 'assoc'):
                    pm, owner = pm_in(cur_set, pname, mp)
                    db_hint = (cur_set.get('mainTable') or {}).get('database')
                    if pm is not None:
                        prop_feats(pm, feats, tables, mp, db_hint, (owner or cur_set).get('srcClass'))
                    else:
                        by_class, by_id, assoc_pm, _ = index(mp)
                        for am, apm in assoc_candidates(assoc_pm, cls, pname, cur_set):
                            if am.get('_type') == 'relational':
                                feats.add('mapping:relational-association-implementation')
                                op_feats(apm.get('relationalOperation'), feats, tables, db_hint)
                            elif am.get('_type') in ('xStore', 'modelJoin'):
                                assoc_relation_feats(am, apm, mp, feats)
                            break
                if ptype in classes:
                    touched_classes.add(ptype)
                    nxt = root_set(mp, ptype)
                    if nxt is not None:
                        set_feats(nxt, mp, feats, tables)
                    tree_walk(st, ptype, nxt, depth + 1)
        root_cls = None
        for rt, hops in w.paths[:1]:
            root_cls = rt
        # the tree's own root class
        tj = json.dumps(lam)
        import re as _re
        mm = _re.search(r'"rootGraphFetchTree".*?"class": "([^"]+)"', tj)
        if mm:
            root_cls = mm.group(1)
        if root_cls:
            touched_classes.add(root_cls)
            rs = root_set(mp, root_cls)
            if rs is not None:
                set_feats(rs, mp, feats, tables)
            tree = None

            def find_tree(o):
                if isinstance(o, dict):
                    if o.get('_type') == 'classInstance' and o.get('type') == 'rootGraphFetchTree':
                        return o.get('value')
                    for v in o.values():
                        r = find_tree(v)
                        if r:
                            return r
                elif isinstance(o, list):
                    for v in o:
                        r = find_tree(v)
                        if r:
                            return r
                return None
            tree = find_tree(lam)
            if tree:
                tree_walk(tree, root_cls, rs, 0)
    # the getAll root (projection over a class that navigates nothing still touches its set)
    def find_getall(o):
        if isinstance(o, dict):
            if o.get('_type') == 'func' and o.get('function') in ('getAll', 'getAllVersions', 'getAllVersionsInRange'):
                ps = o.get('parameters') or []
                if ps and ps[0].get('_type') == 'packageableElementPtr':
                    return ps[0].get('fullPath')
            for v in o.values():
                r = find_getall(v)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = find_getall(v)
                if r:
                    return r
        return None
    ga = find_getall(lam)
    if ga:
        touched_classes.add(ga)
        rs = root_set(mp, ga) if mp else None
        if rs is not None:
            set_feats(rs, mp, feats, tables)
    for c in touched_classes:
        e = classes.get(c)
        if not e:
            continue
        if supers.get(c):
            feats.add('grammar:class-inheritance')
        if e.get('constraints') and w.tree:
            feats.add('grammar:constraint')
        if any(st.get('profile') in CORPUS_PROFILES for st in e.get('stereotypes') or []) or any(
                (tv.get('tag') or {}).get('profile') in CORPUS_PROFILES for tv in e.get('taggedValues') or []):
            feats.add('grammar:profile')
        for st in e.get('stereotypes') or []:
            v = st.get('value')
            if v == 'businesstemporal':
                feats.add('milestoning:business-temporal')
            elif v == 'processingtemporal':
                feats.add('milestoning:processing-temporal')
            elif v == 'bitemporal':
                feats.add('milestoning:bi-temporal')
    for tkey in tables:
        feats |= table_facts.get(tkey, set())
    if len(tables) > 1:
        feats.add('store:relational-multi-table')
    if len({t[1] for t in tables if t[1]}) > 1:
        feats.add('store:relational-cross-schema')
    if any(f.startswith('milestoning:') for f in feats):
        feats.add('milestoning:milestoning')
    # connection time zone / runtime kinds
    rtp = (ex.get('runtime') or {}).get('runtime') if ex.get('_type') == 'pureSingleExecution' else None
    emit_feats = {x for x in feats if not x.startswith(('q:', 'x-', 'scaffolding:'))}
    doms = {x.split(':')[0] for x in emit_feats}
    ndom = len(doms)
    cx = 'basic' if ndom <= 2 else ('intermediate' if ndom <= 4 else 'advanced')
    tests = [t for ts in s.get('testSuites') or [] for t in ts.get('tests') or []]
    status = 'PASS' if name in passed else ('no-tests' if not tests else ('not-run' if not name.startswith('stress::') else 'FAIL/SKIP'))
    row = dict(svc=name, mapping=mp, feats=sorted(feats), domains=sorted(doms), complexity=cx,
               status=status, exp=sorted(exp.get(name, [])), tables=len(tables), table_list=sorted('.'.join(map(str, t)) for t in tables),
               classes=len(touched_classes))
    if engine_passed is not None:
        # run.py takes the same stress:: set lite's runner does (the plan's D4)
        row['engine'] = 'PASS' if name in engine_passed else ('no-tests' if not tests else ('not-run' if not name.startswith('stress::') else 'FAIL'))
    rows.append(row)

json.dump(rows, open(OUT, 'w', encoding='utf-8'), indent=0)

# ------------------------------------------------------------------ roll-up
tot = collections.Counter()
ok = collections.Counter()
ok_engine = collections.Counter()
for r in rows:
    real = r['status'] == 'PASS' and 'ROWS' in r['exp']
    real_engine = r.get('engine') == 'PASS' and 'ROWS' in r['exp']
    for f in r['feats']:
        tot[f] += 1
        if real:
            ok[f] += 1
        if real_engine:
            ok_engine[f] += 1
print('services profiled:', len(rows))
print('EMIT complexity of stress services:', collections.Counter(r['complexity'] for r in rows))
print('  among PASS+ROWS:', collections.Counter(r['complexity'] for r in rows if r['status'] == 'PASS' and 'ROWS' in r['exp']))
print('domain sets:', collections.Counter(tuple(r['domains']) for r in rows).most_common(12))
print()
if engine_passed is not None:
    print('  among engine PASS+ROWS:', collections.Counter(r['complexity'] for r in rows if r.get('engine') == 'PASS' and 'ROWS' in r['exp']))
for f in sorted(tot):
    print('  %-58s services %5d   pass-with-rows %5d' % (f, tot[f], ok[f])
          + ('   engine %5d' % ok_engine[f] if engine_passed is not None else ''))
