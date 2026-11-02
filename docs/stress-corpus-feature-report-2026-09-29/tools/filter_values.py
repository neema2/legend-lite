"""What the comparisons inside service-query filters compare against (report §4.1, §4.2; the improvement plan's
"filters comparing against a real value" row).

Every comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`) inside a `filter` lambda of a `stress::` service query, by its
other operand:

  guard   the string ' ', so `$x.p > ' '`: a test that a value is present, not a comparison of two values
  value   any other literal, an enumeration value included
  param   a service parameter

Counted over every service, and over the services whose every test passes with a populated expected result (ROWS in
expectations.py's reading) -- in lite and, given its pass list, on legend-engine: the plan judges on the engine.

Env PMCD_JSON=<stress-pmcd.json>. Usage: filter_values.py <expectations.tsv> <lite pass list> [<engine pass list>]
"""
import collections
import sys

from lib import LIT, fp, services

EXP, LITE = sys.argv[1], sys.argv[2]
ENGINE = sys.argv[3] if len(sys.argv) > 3 else None

CMP = {'equal', 'notEqual', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual'}

rows = collections.defaultdict(set)
for line in open(EXP, encoding='utf-8'):
    svc, _test, _fam, kind = line.rstrip('\n').split('\t')[:4]
    rows[svc].add(kind)
populated = {s for s, kinds in rows.items() if kinds == {'ROWS'}}


def passes(path):
    out = collections.defaultdict(set)
    for line in open(path, encoding='utf-8', errors='replace'):
        if ' / ' in line:
            svc, rest = line.strip().split(' / ', 1)
            out[svc].add(rest)
    return set(out)


def operand(p):
    """guard / value / param for a comparison's non-property side, or None for a property path."""
    t = p.get('_type')
    if t in LIT:
        return 'guard' if p.get('value') == ' ' else 'value'
    if t == 'var':
        return 'param'
    if t == 'property' and p.get('parameters') and p['parameters'][0].get('_type') == 'packageableElementPtr':
        return 'value'                    # an enumeration value: Grade.PRIME
    if t == 'func' and p.get('function') == 'toOne':
        return operand(p['parameters'][0])
    return None


def comparisons(o, in_filter, out):
    if isinstance(o, dict):
        if o.get('_type') == 'func':
            fn = o.get('function')
            params = o.get('parameters') or []
            if in_filter and fn in CMP:
                kinds = [k for k in (operand(p) for p in params) if k]
                if kinds:
                    out.append(kinds[0])
            if fn == 'filter':
                comparisons(params[0], in_filter, out)
                for p in params[1:]:
                    comparisons(p, True, out)
                return
        for v in o.values():
            comparisons(v, in_filter, out)
    elif isinstance(o, list):
        for v in o:
            comparisons(v, in_filter, out)


lite = passes(LITE)
engine = passes(ENGINE) if ENGINE else None
by = {k: collections.Counter() for k in ('all', 'lite', 'engine')}
svcs = {k: collections.Counter() for k in by}
for s in services:
    name = fp(s)
    if not name.startswith('stress::'):
        continue
    found = []
    comparisons(s.get('execution', {}).get('func'), False, found)
    if not found:
        continue
    judged = [('all', True), ('lite', name in lite and name in populated),
              ('engine', engine is not None and name in engine and name in populated)]
    for key, counted in judged:
        if counted:
            by[key].update(found)
            svcs[key].update(set(found))
for key in ('all', 'lite') + (('engine',) if engine is not None else ()):
    c, n = by[key], svcs[key]
    print('%-7s comparisons in filters: %5d against a value (%d services), %5d guards (%d services), %4d against a '
          'parameter (%d services)' % (key, c['value'], n['value'], c['guard'], n['guard'], c['param'], n['param']))
