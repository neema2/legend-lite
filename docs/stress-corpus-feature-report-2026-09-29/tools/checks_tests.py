"""Constraints and tests, judged by what passes (§5 and §6; docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 8).

Env PMCD_JSON=<stress-pmcd.json>. Usage: checks_tests.py <stress-suites-pass.txt> <checks_tests.json> [<engine-pass.txt>]

analyze.py counts the constraints DECLARED and the mapping test suites WRITTEN. The plan's targets count only what a
passing test reaches, so this reads, per constraint:

  combining    its function calls a function other than the comparison, boolean and arithmetic operators, reads
               properties of two kinds of type, or reads a navigated property
  fetched      a passing service graph-fetches its class (checked or not: the engine checks the constraints of every
               object a fetch materializes, F16)
  violated     a passing service's expected answer holds a defect of it (graphFetchChecked's {defects, value})

and, per mapping, its test suites and those whose every test passes. A pass list's lines are `<testable> / <suite> /
<test> :: PASS`, lite's (gate 10) and, given, legend-engine's (scripts/corpus/run.py), which judges a mapping's suites
too; lite's runner takes services alone, so no mapping suite passes there. It also counts the services' test shapes:
tests per suite, assertions per test, assertion kinds, and the tests passing on each engine by kind.
"""
import collections
import json
import sys

from lib import *  # noqa: F401,F403 -- the model index and the walker

PASS, OUT = sys.argv[1:3]
ENGINE = sys.argv[3] if len(sys.argv) > 3 else None

OPERATORS = {'and', 'or', 'not', 'equal', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual', 'plus',
             'minus', 'times', 'divide'}


def pass_lines(path):
    """(testable, suite, test) per passing test."""
    out = set()
    for line in open(path, encoding='utf-8', errors='replace'):
        if ' :: ' not in line:
            continue
        parts = line.split(' :: ')[0].split(' / ')
        if len(parts) == 3:
            out.add(tuple(p.strip() for p in parts))
    return out


lite = pass_lines(PASS)
engine = pass_lines(ENGINE) if ENGINE else None


def passing_services(lines):
    return {t for t, _s, _x in lines}


def graph_roots(node, out):
    """The classes a query graph-fetches at its root."""
    if isinstance(node, dict):
        if node.get('_type') == 'rootGraphFetchTree' and node.get('class'):
            out.add(node['class'])
        for v in node.values():
            graph_roots(v, out)
    elif isinstance(node, list):
        for v in node:
            graph_roots(v, out)
    return out


def defects_expected(s):
    """(defining class, constraint) per defect a service's expected answers hold."""
    out = set()
    for ts in s.get('testSuites') or []:
        for t in ts.get('tests') or []:
            for a in t.get('assertions') or []:
                data = ((a.get('expected') or {}).get('data')) if a.get('_type') == 'equalToJson' else None
                if not data or '"defects"' not in data:
                    continue
                try:
                    payload = json.loads(data)
                except ValueError:
                    continue
                vals = payload.get('values') if isinstance(payload, dict) and 'values' in payload else payload
                for v in vals if isinstance(vals, list) else [vals]:
                    for d in (v or {}).get('defects') or []:
                        out.add((d.get('ruleDefinerPath'), d.get('id')))
    return out


fetched_by = collections.defaultdict(set)    # class -> services that graph-fetch it
violated_by = collections.defaultdict(set)   # (class, constraint) -> services that expect a defect of it
for s in services:
    name = fp(s)
    for cls in graph_roots(s.get('execution') or {}, set()):
        fetched_by[cls].add(name)
    for key in defects_expected(s):
        violated_by[key].add(name)

rows = []
for cls, e in sorted(classes.items()):
    for k in e.get('constraints') or []:
        w = Walk()
        w.walk((k.get('functionDefinition') or {}).get('body') or [], {'this': cls})
        kinds = set()
        navigated = False
        for _rt, hops in w.paths:
            if len(hops) > 1:
                navigated = True
            if hops and hops[-1][1]:
                kinds.add(kind_of(hops[-1][1][0]))
        funcs = set(w.funcs) - OPERATORS
        combining = bool(funcs) or len(kinds) >= 2 or navigated
        row = dict(cls=cls, name=k.get('name'), combining=combining, funcs=sorted(funcs), kinds=sorted(kinds),
                   navigated=navigated)
        for label, lines in (('lite', lite), ('engine', engine)):
            if lines is None:
                continue
            ok = passing_services(lines)
            row[label + '_fetched'] = sorted(fetched_by[cls] & ok)
            row[label + '_violated'] = sorted(violated_by[(cls, k.get('name'))] & ok)
        rows.append(row)


def tally(label):
    if label + '_fetched' not in rows[0]:
        return None
    tested = [r for r in rows if r[label + '_fetched']]
    return dict(tested=len(tested), tested_combining=sum(1 for r in tested if r['combining']),
                violated=sum(1 for r in rows if r[label + '_violated']),
                violated_combining=sum(1 for r in rows if r[label + '_violated'] and r['combining']))


# ---------------------------------------------------------------- mapping test suites
suites = []
for m in mappings:
    for ts in m.get('testSuites') or []:
        suites.append((fp(m), ts.get('id'), [t.get('id') for t in ts.get('tests') or []]))


def suites_passing(lines):
    if lines is None:
        return None
    return sum(1 for m, sid, tests in suites if tests and all((m, sid, t) in lines for t in tests))


# ---------------------------------------------------------------- service test shapes
tests_per_suite, asserts_per_test, kinds = collections.Counter(), collections.Counter(), collections.Counter()
passing_by_kind = {'lite': collections.Counter(), 'engine': collections.Counter()}
for s in services:
    for ts in s.get('testSuites') or []:
        tests_per_suite[len(ts.get('tests') or [])] += 1
        for t in ts.get('tests') or []:
            assertions = t.get('assertions') or []
            asserts_per_test[len(assertions)] += 1
            shape = '+'.join(sorted({a.get('_type') for a in assertions}))
            kinds[shape] += 1
            for label, lines in (('lite', lite), ('engine', engine)):
                if lines is not None and (fp(s), ts.get('id'), t.get('id')) in lines:
                    passing_by_kind[label][shape] += 1

R = dict(constraints=rows, constraint_totals=dict(declared=len(rows), combining=sum(1 for r in rows if r['combining']),
                                                    lite=tally('lite'), engine=tally('engine')),
         mapping_suites=dict(suites=len(suites), tests=sum(len(t) for _m, _s, t in suites),
                             mappings=len({m for m, _s, _t in suites}), passing_lite=suites_passing(lite),
                             passing_engine=suites_passing(engine)),
         test_shapes=dict(tests_per_suite=dict(tests_per_suite), asserts_per_test=dict(asserts_per_test),
                          assertion_kinds=dict(kinds), passing_lite=dict(passing_by_kind['lite']),
                          passing_engine=dict(passing_by_kind['engine']) if engine is not None else None))
json.dump(R, open(OUT, 'w', encoding='utf-8'), indent=1, default=list)

t = R['constraint_totals']
print(f"CONSTRAINTS: {t['declared']} declared, {t['combining']} combining types or functions")
for label in ('engine', 'lite'):
    x = t[label]
    if x:
        print(f"  {label}: {x['tested']} graph-fetched by a passing service ({x['tested_combining']} combining); "
              f"{x['violated']} with a violation a passing service asserts ({x['violated_combining']} combining)")
for r in rows:
    print(f"  {r['cls']:<26} {r['name']:<34} {'combining' if r['combining'] else '-':<10} "
          f"engine fetched {len(r.get('engine_fetched') or [])} violated {len(r.get('engine_violated') or [])}  "
          f"lite fetched {len(r.get('lite_fetched') or [])} violated {len(r.get('lite_violated') or [])}")
ms = R['mapping_suites']
print(f"MAPPING TEST SUITES: {ms['suites']} suites, {ms['tests']} tests, in {ms['mappings']} mappings; passing on the "
      f"engine {ms['passing_engine']}, in lite {ms['passing_lite']}")
sh = R['test_shapes']
print(f"SERVICE TESTS: tests per suite {dict(sorted(sh['tests_per_suite'].items()))}, assertions per test "
      f"{dict(sorted(sh['asserts_per_test'].items()))}")
print(f"  assertion kinds {sh['assertion_kinds']}")
print(f"  passing on the engine {sh['passing_engine']}; in lite {sh['passing_lite']}")
