"""Where each literal type appears in the service queries: the enclosing function (or qualified
property) and the most common values. Backs the constants table (report §4.2).

Env PMCD_JSON=<stress-pmcd.json>. Usage: literal_sites.py
"""
import collections

from lib import services, LIT

where = collections.defaultdict(collections.Counter)
vals = collections.defaultdict(collections.Counter)


def walk(o, parent):
    if isinstance(o, dict):
        t = o.get('_type')
        if t in LIT:
            where[LIT[t]][parent] += 1
            vals[LIT[t]][str(o.get('value'))[:30]] += 1
            return
        p = parent
        if t == 'func':
            p = o.get('function')
        elif t == 'property' and len(o.get('parameters') or []) > 1:
            p = 'qualified property ' + o.get('property')
        for v in o.values():
            walk(v, p)
    elif isinstance(o, list):
        for v in o:
            walk(v, parent)


for s in services:
    walk(s['execution'].get('func'), 'root')
for k in where:
    print(k, dict(where[k].most_common(8)))
    print('   values:', dict(vals[k].most_common(8)))
