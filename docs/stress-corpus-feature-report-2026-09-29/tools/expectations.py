"""Per-test oracle expectations of the stress corpus, classified by how much they can catch.

  EMPTY   the expected result has no rows (TDS `[]`, or a graph envelope with no values)
  HOLLOW  rows come back, but the expectation cannot see past them:
            TDS    every NAVIGATED column (a projected name built from a path, i.e. not a
                   root-local column) is null in every row -- approximated as: at least one
                   column, and every column but the first, null in every row
            graph  the tree asks for levels (keys present) that are null in every value
  ROWS    anything else

Writes a TSV (service, test, family, kind, nrows, detail) and prints the census.
"""
import collections
import glob
import json
import re
import sys

STRESS = sys.argv[1]
OUT = sys.argv[2]

MAIN = ('DSDeep', 'DSPost', 'DSGroup', 'DSTree', 'DSLocal', 'DSSub', 'DSAgg', 'DSQual')


def family(name):
    fam = name.split('_')[0]
    if fam in MAIN:
        return fam
    if fam.endswith('X') and fam != 'TX':
        return 'subdomain *X'
    return 'other'


def unescape(s):
    return s.replace("\\'", "'").replace('\\\\', '\\')


def depths(v, d=0):
    """(deepest level a key is present at, deepest level holding a non-null object)."""
    asked, reached = d, d if v is not None else d - 1
    if isinstance(v, dict):
        for x in v.values():
            a, r = depths(x, d + 1)
            asked, reached = max(asked, a), max(reached, r)
    elif isinstance(v, list):
        for x in v:
            a, r = depths(x, d)
            asked, reached = max(asked, a), max(reached, r)
    return asked, reached


def classify(data):
    try:
        v = json.loads(unescape(data))
    except Exception as e:  # noqa: BLE001
        return 'UNPARSED', -1, str(e)[:40]
    if isinstance(v, dict) and 'values' in v:          # graph envelope
        vals = v['values']
        if not vals:
            return 'EMPTY', 0, 'graph'
        asked, reached = 0, 0
        for x in vals:
            a, r = depths(x, 1)
            asked, reached = max(asked, a), max(reached, r)
        if reached < asked:
            return 'HOLLOW', len(vals), 'graph asks depth %d, populated to %d' % (asked, reached)
        return 'ROWS', len(vals), 'graph depth %d' % asked
    if isinstance(v, list):
        if not v:
            return 'EMPTY', 0, 'tds'
        if not all(isinstance(r, dict) for r in v):
            return 'ROWS', len(v), 'scalar list'
        cols = list(v[0].keys())
        nullcols = [c for c in cols if all(r.get(c) is None for r in v)]
        if len(cols) > 1 and set(nullcols) >= set(cols[1:]):
            return 'HOLLOW', len(v), 'tds: every column but %s null in every row' % cols[0]
        return 'ROWS', len(v), 'tds %d/%d cols always null' % (len(nullcols), len(cols))
    return 'ROWS', 1, 'scalar'


TEST = re.compile(r"^\s*([A-Za-z0-9_]+):\s*\n\s*\{\s*\n\s*(?:serializationFormat|asserts)", re.M)
DATA = re.compile(r"data: '((?:[^'\\]|\\.)*)';", re.S)

rows = []
for f in sorted(glob.glob(STRESS + '/*.pure')):
    txt = open(f, encoding='utf-8').read()
    for m in re.finditer(r'^Service (stress::\S+)(.*?)(?=^Service |\Z)', txt, re.S | re.M):
        svc, body = m.group(1), m.group(2)
        tests = [(t.start(), t.group(1)) for t in TEST.finditer(body)]
        for d in DATA.finditer(body):
            tid = [n for (p, n) in tests if p < d.start()]
            if not tid:
                continue
            kind, n, detail = classify(d.group(1))
            rows.append((svc, tid[-1], family(svc.split('::')[-1]), kind, n, detail))

with open(OUT, 'w', encoding='utf-8') as o:
    for r in rows:
        o.write('\t'.join(map(str, r)) + '\n')

tot = collections.Counter(r[2] for r in rows)
by = collections.Counter((r[2], r[3]) for r in rows)
kinds = ['ROWS', 'HOLLOW', 'EMPTY', 'UNPARSED']
print('%-14s %6s ' % ('family', 'tests') + ' '.join('%8s' % k for k in kinds))
for k, v in tot.most_common():
    print('%-14s %6d ' % (k, v) + ' '.join('%8d' % by[(k, x)] for x in kinds))
allk = collections.Counter(r[3] for r in rows)
print('%-14s %6d ' % ('ALL', len(rows)) + ' '.join('%8d' % allk[x] for x in kinds))
