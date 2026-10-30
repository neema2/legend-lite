"""Shared model index and value-specification walker over a legend-engine protocol document.

The document is read from the path in the PMCD_JSON environment variable (the stress corpus, or the
EMIT models, as written by `bazel run //tools/engine-runner:testable -- ... --pmcd-json=<file>`).
"""
import collections
import json
import re
import sys

import os
PMCD = os.environ['PMCD_JSON']
doc = json.load(open(PMCD, encoding='utf-8'))
E = doc['elements']

PRIMS = ['String', 'Boolean', 'Integer', 'Float', 'Decimal', 'Number', 'Date', 'StrictDate', 'DateTime',
         'StrictTime', 'LatestDate', 'Binary', 'Byte']
LIT = {'string': 'String', 'cString': 'String', 'integer': 'Integer', 'cInteger': 'Integer', 'float': 'Float',
       'cFloat': 'Float', 'decimal': 'Decimal', 'cDecimal': 'Decimal', 'boolean': 'Boolean', 'cBoolean': 'Boolean',
       'strictDate': 'StrictDate', 'cStrictDate': 'StrictDate', 'dateTime': 'DateTime', 'cDateTime': 'DateTime',
       'strictTime': 'StrictTime', 'cStrictTime': 'StrictTime', 'latestDate': 'LatestDate', 'cLatestDate': 'LatestDate',
       'byteArray': 'Binary', 'cByteArray': 'Binary'}


def fp(e):
    return (e.get('package') + '::' if e.get('package') else '') + e['name']


def tname(gt):
    if not gt:
        return None
    rt = gt.get('rawType') if 'rawType' in gt else gt
    return rt.get('fullPath') if isinstance(rt, dict) else None


def mult(m):
    if not m:
        return '?'
    lo, hi = m.get('lowerBound'), m.get('upperBound')
    if hi is None:
        return '[%s..*]' % lo if lo else '[*]'
    return '[%s]' % lo if lo == hi else '[%s..%s]' % (lo, hi)


# ---------------------------------------------------------------------------------------- model
classes = {fp(e): e for e in E if e['_type'] == 'class'}
enums = {fp(e): e for e in E if e['_type'] == 'Enumeration'}
measures = {fp(e): e for e in E if e['_type'] == 'measure'}
assocs = [e for e in E if e['_type'] == 'association']
functions = [e for e in E if e['_type'] == 'function']
services = [e for e in E if e['_type'] == 'service']
mappings = [e for e in E if e['_type'] == 'mapping']
dbs = [e for e in E if e['_type'] == 'relational']
unit_names = set()
for m in measures.values():
    for u in [m.get('canonicalUnit')] + (m.get('nonCanonicalUnits') or []):
        if u:
            unit_names.add(fp(m) + '~' + u['name'].split('~')[-1])


def kind_of(t):
    if t is None:
        return '?'
    if t in PRIMS:
        return t
    if t in enums:
        return 'enum'
    if t in classes:
        return 'class'
    if t in measures or '~' in t:
        return 'measure'
    return 'other:' + t


# property table: class -> name -> (type, multiplicity, kind: prop|derived|qualified|assoc)
props = collections.defaultdict(dict)
for c, e in classes.items():
    for p in e.get('properties', []):
        props[c][p['name']] = (tname(p.get('genericType')), mult(p.get('multiplicity')), 'prop')
    for q in e.get('qualifiedProperties', []):
        k = 'qualified' if q.get('parameters') else 'derived'
        props[c][q['name']] = (tname(q.get('returnGenericType')), mult(q.get('returnMultiplicity')), k)
for a in assocs:
    ps = a.get('properties', [])
    if len(ps) == 2:
        t0, t1 = tname(ps[0].get('genericType')), tname(ps[1].get('genericType'))
        # property p0 is owned by the class of the OTHER end
        props[t1][ps[0]['name']] = (t0, mult(ps[0].get('multiplicity')), 'assoc')
        props[t0][ps[1]['name']] = (t1, mult(ps[1].get('multiplicity')), 'assoc')
supers = {c: [(s.get("path") or tname(s)) if isinstance(s, dict) else s for s in (e.get("superTypes") or [])] for c, e in classes.items()}


def lookup(c, name, seen=None):
    seen = seen or set()
    if c in seen or c is None:
        return None
    seen.add(c)
    if name in props.get(c, {}):
        return props[c][name]
    for s in supers.get(c, []):
        r = lookup(s, name, seen)
        if r:
            return r
    return None


# ------------------------------------------------------------------------------- value specs
class Walk:
    """Walks one value specification, collecting features."""

    def __init__(self, var_types=None):
        self.funcs = collections.Counter()
        self.lits = collections.Counter()
        self.enum_lits = collections.Counter()
        self.nodes = 0
        self.paths = []           # (root type, [prop...], final (type, mult, kind))
        self.prop_kinds = collections.Counter()
        self.leaf_types = collections.Counter()
        self.max_hops = 0
        self.to_many_hops = 0
        self.lambda_depth = 0
        self.var_types = dict(var_types or {})
        self.tree = None          # graphFetch tree stats
        self.colspecs = 0
        self.project_cols = 0
        self.agg_cols = 0
        self.group_keys = 0
        self.qualified_args = 0
        self.user_funcs = collections.Counter()
        self.classinstance = collections.Counter()
        self.collections = 0
        self.let = 0

    def ty(self, node, env):
        """Best-effort static type of a node: class/enum/prim path resolution for $x.a.b."""
        t = node.get('_type')
        if t == 'var':
            return env.get(node.get('name'))
        if t == 'property':
            ps = node.get('parameters') or []
            if not ps:
                return None
            recv = ps[0]
            if recv.get('_type') == 'packageableElementPtr' and recv.get('fullPath') in enums:
                return recv.get('fullPath')
            rt = self.ty(recv, env)
            r = lookup(rt, node.get('property'))
            return r[0] if r else None
        if t == 'func' and node.get('function') in ('toOne', 'toOneMany', 'first', 'at', 'cast', 'map', 'filter'):
            ps = node.get('parameters') or []
            return self.ty(ps[0], env) if ps else None
        if t == 'func' and node.get('function') == 'getAll':
            ps = node.get('parameters') or []
            return ps[0].get('fullPath') if ps and ps[0].get('_type') == 'packageableElementPtr' else None
        return None

    def chain(self, node, env):
        """Return (root type, [(prop, (type, mult, kind))]) for a $var.a.b.c chain, or None."""
        hops = []
        n = node
        while n.get('_type') == 'property':
            ps = n.get('parameters') or []
            if not ps:
                return None
            hops.append(n)
            n = ps[0]
            if n.get('_type') == 'func' and n.get('function') in ('toOne', 'first', 'at'):
                n = (n.get('parameters') or [{}])[0]
        if n.get('_type') != 'var':
            return None
        rt = env.get(n.get('name'))
        hops.reverse()
        out = []
        cur = rt
        for h in hops:
            r = lookup(cur, h.get('property'))
            out.append((h.get('property'), r))
            cur = r[0] if r else None
        return rt, out

    def walk(self, node, env, depth=0, parent=None):
        if isinstance(node, list):
            for x in node:
                self.walk(x, env, depth, parent)
            return
        if not isinstance(node, dict):
            return
        t = node.get('_type')
        if t is None:
            return
        self.nodes += 1
        if t in LIT:
            self.lits[LIT[t]] += 1
            return
        if t == 'collection':
            self.collections += 1
            self.walk(node.get('values') or [], env, depth, node)
            return
        if t == 'enumValue':
            self.enum_lits[node.get('fullPath', '?')] += 1
            return
        if t == 'property':
            ps = node.get('parameters') or []
            if ps and ps[0].get('_type') == 'packageableElementPtr' and ps[0].get('fullPath') in enums:
                self.enum_lits[ps[0]['fullPath'] + '.' + node.get('property')] += 1
                return
            ch = self.chain(node, env)
            if ch and (parent is None or parent.get('_type') != 'property'):
                rt, hops = ch
                self.paths.append((rt, hops))
                nav = [h for h in hops[:-1]]
                self.max_hops = max(self.max_hops, len(hops) - 1)
                for name, r in hops:
                    if r:
                        self.prop_kinds[r[2]] += 1
                        if r[1] in ('[*]',) or (r[1].endswith('..*]')):
                            self.to_many_hops += 1
                if hops and hops[-1][1]:
                    self.leaf_types[kind_of(hops[-1][1][0])] += 1
            if len(ps) > 1:
                self.qualified_args += 1
            for p in ps:
                self.walk(p, env, depth, node)
            return
        if t == 'func':
            f = node.get('function')
            self.funcs[f] += 1
            if '::' in f:
                self.user_funcs[f] += 1
            if f == 'letFunction':
                self.let += 1
            ps = node.get('parameters') or []
            # bind lambda parameters to the element type of the receiver for common collection functions
            recv_t = self.ty(ps[0], env) if ps and isinstance(ps[0], dict) else None
            if f == 'getAll' and ps and ps[0].get('_type') == 'packageableElementPtr':
                recv_t = ps[0].get('fullPath')
            for i, p in enumerate(ps):
                if isinstance(p, dict) and p.get('_type') == 'lambda' and recv_t:
                    lp = p.get('parameters') or []
                    if lp:
                        env2 = dict(env)
                        env2[lp[0].get('name')] = recv_t
                        self.walk(p, env2, depth, node)
                        continue
                # A legacy TDS projection's or groupBy's lambdas travel in a collection, `project([x|$x.a], ['a'])`,
                # `groupBy([x|$x.k], [agg(x|$x.v, y|$y->sum())], ...)`: each is over the receiver's class too. Walked
                # unbound, no path in a TDS projection resolved (added 2026-10-30, the improvement plan's Phase 5).
                if (isinstance(p, dict) and p.get('_type') == 'collection' and recv_t
                        and any(self.binds(v) for v in p.get('values') or [])):
                    self.collections += 1
                    for v in p.get('values') or []:
                        self.walk_bound(v, env, depth, p, recv_t)
                    continue
                if isinstance(p, dict) and p.get('_type') == 'classInstance' and recv_t:
                    self.class_instance(p, env, depth, recv_t)
                    continue
                self.walk(p, env, depth, node)
            return
        if t == 'lambda':
            self.lambda_depth = max(self.lambda_depth, depth + 1)
            self.walk(node.get('body') or [], env, depth + 1, node)
            return
        if t == 'classInstance':
            self.class_instance(node, env, depth, None)
            return
        if t in ('var', 'packageableElementPtr', 'genericTypeInstance'):
            return
        for k, v in node.items():
            if isinstance(v, (dict, list)):
                self.walk(v, env, depth, node)

    @staticmethod
    def binds(v):
        """A lambda over the receiver, or an `agg` whose first argument is one."""
        if not isinstance(v, dict):
            return False
        if v.get('_type') == 'lambda':
            return True
        ps = v.get('parameters') or []
        return v.get('_type') == 'func' and v.get('function') == 'agg' and bool(ps) and ps[0].get('_type') == 'lambda'

    def walk_bound(self, v, env, depth, parent, recv_t):
        """Walk one value of a TDS collection, its lambda's parameter bound to the receiver's class."""
        if isinstance(v, dict) and v.get('_type') == 'func' and v.get('function') == 'agg':
            self.funcs['agg'] += 1
            ps = v.get('parameters') or []
            self.walk_bound(ps[0], env, depth, v, recv_t)
            self.walk(ps[1:], env, depth, v)
            return
        lp = (v.get('parameters') or []) if isinstance(v, dict) and v.get('_type') == 'lambda' else []
        if lp:
            env2 = dict(env)
            env2[lp[0].get('name')] = recv_t
            self.walk(v, env2, depth, parent)
        else:
            self.walk(v, env, depth, parent)

    def class_instance(self, node, env, depth, recv_t):
        ty = node.get('type')
        self.classinstance[ty] += 1
        v = node.get('value') or {}
        if ty in ('rootGraphFetchTree',):
            self.tree = tree_stats(v)
            return
        if ty in ('colSpecArray', 'funcColSpecArray', 'aggColSpecArray'):
            cols = v.get('colSpecs') or []
            self.colspecs += len(cols)
            for c in cols:
                self.col(c, env, depth, recv_t, ty)
            return
        if ty in ('colSpec', 'funcColSpec', 'aggColSpec'):
            self.colspecs += 1
            self.col(v, env, depth, recv_t, ty)
            return
        for k, x in v.items() if isinstance(v, dict) else []:
            if isinstance(x, (dict, list)):
                self.walk(x, env, depth)

    def col(self, c, env, depth, recv_t, ty):
        for key in ('function1', 'function2'):
            fn = c.get(key)
            if isinstance(fn, dict) and fn.get('_type') == 'lambda':
                lp = fn.get('parameters') or []
                env2 = dict(env)
                if lp and recv_t and key == 'function1':
                    env2[lp[0].get('name')] = recv_t
                if key == 'function2':
                    self.agg_cols += 1
                self.walk(fn, env2, depth)


def tree_stats(t):
    """graphFetch tree: depth, nodes, leaves, subtype trees, qualified props in tree."""
    st = {'depth': 0, 'nodes': 0, 'leaves': 0, 'subtypes': 0, 'qualified': 0}

    def rec(n, d):
        subs = (n.get('subTrees') or [])
        st['depth'] = max(st['depth'], d)
        for s in subs:
            st['nodes'] += 1
            if s.get('parameters'):
                st['qualified'] += 1
            if s.get('subTrees'):
                rec(s, d + 1)
            else:
                st['leaves'] += 1
        for s in n.get('subTypeTrees') or []:
            st['subtypes'] += 1
            rec(s, d)
    rec(t, 0)
    return st


def lambda_env(lam, var_types=None):
    env = dict(var_types or {})
    for p in lam.get('parameters') or []:
        env[p.get('name')] = tname(p.get('genericType'))
    return env
