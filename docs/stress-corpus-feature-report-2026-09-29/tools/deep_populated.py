"""For every query that navigates: is the DEEPEST navigated value ever non-null in the expectation?

Usage: deep_populated.py <stress dir> <stress-suites-pass.txt> [<engine pass list>]

Given legend-engine's pass list, also counts the populated ones that pass on the engine (the improvement plan judges
on legend-engine: docs/STRESS_CORPUS_PLAN_2026_10_19.md).

TDS: the projected columns whose path has the most hops -- any non-null cell in any row?
graph: the deepest OBJECT level the tree asks for -- any non-null object there?
"""
import collections
import glob
import json
import os
import re
import sys

STRESS, PASS = sys.argv[1:3]
passed = {l.split(' / ')[0] for l in open(PASS, encoding='utf-8', errors='replace') if ' / ' in l}
engine = ({l.split(' / ')[0] for l in open(sys.argv[3], encoding='utf-8', errors='replace') if ' / ' in l}
          if len(sys.argv) > 3 else None)


def unescape(s):
    return s.replace("\\'", "'").replace('\\\\', '\\')


def tree_levels(body):
    """Depth of the deepest object node in a graphFetch tree (root class = level 0)."""
    inner = body[body.index('{') + 1:]
    d, best = 0, 0
    for ch in inner:
        if ch == '{':
            d += 1
            best = max(best, d)
        elif ch == '}':
            d -= 1
    return best


def obj_depth(v, d=0):
    """Deepest level at which a non-null OBJECT sits (a value's own root is level 0)."""
    best = -1
    if isinstance(v, dict):
        best = d
        for x in v.values():
            if isinstance(x, (dict, list)):
                best = max(best, obj_depth(x, d + 1))
    elif isinstance(v, list):
        for x in v:
            best = max(best, obj_depth(x, d - 0))
    return best


res = collections.Counter()
by_depth = collections.defaultdict(collections.Counter)
examples = collections.defaultdict(list)
for f in glob.glob(os.path.join(STRESS, '*.pure')):
    t = open(f, encoding='utf-8').read()
    for m in re.finditer(r'^Service (\S+)(.*?)(?=^Service |\Z)', t, re.S | re.M):
        svc, body = m.group(1), m.group(2)
        qm = re.search(r'query\s*:(.*?)\n\s*mapping', body, re.S)
        datas = re.findall(r"data: '((?:[^'\\]|\\.)*)';", body, re.S)
        if not qm or not datas:
            continue
        q = qm.group(1)
        g = re.search(r'graphFetch\(#\{(.*?)\}#', q, re.S)
        try:
            vals = [json.loads(unescape(d)) for d in datas]
        except Exception:
            continue
        if g:
            asked = tree_levels(g.group(1))
            if asked == 0:
                continue
            reached = -1
            for v in vals:
                if isinstance(v, dict) and 'values' in v:
                    for x in v['values']:
                        reached = max(reached, obj_depth(x, 0))
            if reached < 0:
                verdict = 'no rows'
            elif reached >= asked:
                verdict = 'deepest level populated'
            else:
                verdict = 'deepest level never populated'
            key = 'graph %d levels' % asked
        else:
            q2 = re.sub(r'(\.\w+)\([^()]*\)', r'\1()', q)
            cols = {}
            for a, p in re.findall(r'(\w+)\s*:\s*\w+\s*\|\s*\$\w+((?:\.\w+(?:\(\))?)+)', q2):
                cols[a] = p.count('.') - 1
            if not cols or max(cols.values()) == 0:
                continue
            asked = max(cols.values())
            deep = [a for a, h in cols.items() if h == asked]
            rows = [r for v in vals if isinstance(v, list) for r in v if isinstance(r, dict)]
            if not rows:
                verdict = 'no rows'
            elif any(r.get(a) is not None for r in rows for a in deep):
                verdict = 'deepest level populated'
            else:
                verdict = 'deepest level never populated'
            key = 'tds %d hops' % asked
        res[verdict] += 1
        by_depth[key][verdict] += 1
        if verdict == 'deepest level populated' and engine is not None and svc in engine:
            by_depth[key]['populated, passing on the engine'] += 1
        if verdict == 'deepest level populated':
            examples[key].append((svc, os.path.basename(f), t.count('\n', 0, m.start()) + 1,
                                  'PASS' if svc in passed else 'not passing'))

print('navigating queries with an expectation:', sum(res.values()), dict(res))
for k in sorted(by_depth, key=lambda k: (k.split()[0], -int(k.split()[1]))):
    c = by_depth[k]
    print('  %-16s populated %4d   never %4d   no rows %4d%s   e.g. %s' % (
        k, c['deepest level populated'], c['deepest level never populated'], c['no rows'],
        '' if engine is None else '   populated and passing on the engine %4d' % c['populated, passing on the engine'],
        [e[0].split("::")[1] for e in examples[k]][:16] if k in ("tds 4 hops", "graph 4 levels") else ""))
