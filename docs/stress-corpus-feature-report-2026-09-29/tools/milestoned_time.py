"""DateTime literals, and milestoned queries at a time of day (report §3 Milestoning, §4.2; the improvement plan's
"DateTime literals / milestoned queries at time of day" row, Phase 3).

For every `stress::` service query:

  DateTime literal   the query carries a DateTime literal anywhere: a filter, all(), a milestoned property, a graph
                     fetch tree
  milestoned         the query passes a milestoning date: to all(), to getAllVersionsInRange, or to a milestoned
                     property (a property on an association end whose class is temporal, or its generated
                     `<end>AllVersionsInRange`), in a lambda or a graph fetch tree
  at time of day     one of those dates is a DateTime literal, a parameter some test binds to a DateTime, or a
                     DateTime property read from the row (`$x.checkedAt`)

Counted over every service, and over the services whose every test passes with a populated expected result (ROWS in
expectations.py's reading) -- in lite and, given its pass list, on legend-engine: the plan judges on the engine.
A milestoned property is recognised by NAME, from the associations' ends onto temporal classes, which is exact for
this corpus (no other property shares one of those names) and is said here in case that changes.

Env PMCD_JSON=<stress-pmcd.json>. Usage: milestoned_time.py <expectations.tsv> <lite pass list> [<engine pass list>]
"""
import collections
import sys

from lib import assocs, classes, fp, services, tname

EXP, LITE = sys.argv[1], sys.argv[2]
ENGINE = sys.argv[3] if len(sys.argv) > 3 else None

DATETIME = {'dateTime', 'cDateTime'}

rows = collections.defaultdict(set)
for line in open(EXP, encoding='utf-8'):
    svc, _test, _fam, kind = line.rstrip('\n').split('\t')[:4]
    rows[svc].add(kind)
populated = {s for s, kinds in rows.items() if kinds == {'ROWS'}}


def passes(path):
    out = set()
    for line in open(path, encoding='utf-8', errors='replace'):
        if ' / ' in line:
            out.add(line.strip().split(' / ', 1)[0])
    return out


def temporal(cls):
    e = classes.get(cls)
    return e is not None and any((st.get('value') or '').endswith('temporal') for st in e.get('stereotypes') or [])


# the property names that navigate onto a milestoned class, and their generated range form
MILESTONED = set()
for a in assocs:
    for p in a.get('properties') or []:
        if temporal(tname(p.get('genericType'))):
            MILESTONED |= {p['name'], p['name'] + 'AllVersionsInRange'}
# every DateTime property, for a date read from the row
DATETIME_PROPS = {p['name'] for e in classes.values() for p in e.get('properties') or []
                  if tname(p.get('genericType')) in ('DateTime', 'meta::pure::metamodel::type::DateTime')}


def walk(o, dates, lits):
    """Collects the milestoning dates a value specification passes, and whether it holds a DateTime literal."""
    if isinstance(o, dict):
        t = o.get('_type')
        if t in DATETIME:
            lits.append(o)
        params = o.get('parameters') or []
        if t == 'func' and o.get('function') in ('getAll', 'getAllVersionsInRange'):
            dates += params[1:]
        elif t == 'property' and o.get('property') in MILESTONED:
            dates += params[1:]
        elif t == 'propertyGraphFetchTree' and o.get('property') in MILESTONED:
            dates += params
        for v in o.values():
            walk(v, dates, lits)
    elif isinstance(o, list):
        for v in o:
            walk(v, dates, lits)


def bound_datetime(s, name):
    """Whether some test of the service binds parameter `name` to a DateTime."""
    for suite in s.get('testSuites') or []:
        for test in suite.get('tests') or []:
            for p in test.get('parameters') or []:
                if p.get('name') == name and (p.get('value') or {}).get('_type') in DATETIME:
                    return True
    return False


def at_time_of_day(s, d):
    t = d.get('_type')
    if t in DATETIME:
        return True
    if t == 'var':
        return bound_datetime(s, d.get('name'))
    if t == 'property':
        return d.get('property') in DATETIME_PROPS
    return False


lite = passes(LITE)
engine = passes(ENGINE) if ENGINE else None
counts = {k: collections.Counter() for k in ('all', 'lite', 'engine')}
for s in services:
    name = fp(s)
    if not name.startswith('stress::'):
        continue
    dates, lits = [], []
    walk(s.get('execution', {}).get('func'), dates, lits)
    found = set()
    if lits:
        found.add('datetime literal')
    if dates:
        found.add('milestoned')
    if any(at_time_of_day(s, d) for d in dates):
        found.add('at time of day')
    for key, counted in [('all', True), ('lite', name in lite and name in populated),
                         ('engine', engine is not None and name in engine and name in populated)]:
        if counted:
            counts[key].update(found)
for key in ('all', 'lite') + (('engine',) if engine is not None else ()):
    c = counts[key]
    print('%-7s services: %4d with a DateTime literal, %4d milestoned, %4d milestoned at a time of day'
          % (key, c['datetime literal'], c['milestoned'], c['at time of day']))
