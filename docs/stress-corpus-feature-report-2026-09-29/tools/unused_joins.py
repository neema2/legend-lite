"""Joins that no mapping, view or filter references (report §8).

Env PMCD_JSON=<stress-pmcd.json>. Usage: unused_joins.py

A reference is a join pointer, `{"db": ..., "name": ...}` in a `joins` list, anywhere in a mapping, or in a database's
views and filters. A database's own `joins` list holds the DECLARATIONS, which carry an `operation`; until 2026-10-29
a regex over the JSON read those too and stopped at the first `]`, so the first join each database declared counted
as used, and 5 unused joins went uncounted (32 reported where 37 were unused).
"""
from lib import E, fp, dbs

used = set()


def walk(o):
    if isinstance(o, dict):
        js = o.get('joins')
        if isinstance(js, list):
            for j in js:
                if isinstance(j, dict) and 'name' in j and 'operation' not in j:
                    used.add(j['name'])
        for v in o.values():
            walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)


for e in E:
    if e['_type'] == 'mapping':
        walk(e)
    elif e['_type'] == 'relational':
        for s in e.get('schemas') or []:
            walk(s.get('views'))
        walk(e.get('filters'))
alljoins = [(fp(d), j['name']) for d in dbs for j in d.get('joins') or []]
unused = [j for j in alljoins if j[1] not in used]
print('joins', len(alljoins), 'unused by any mapping/view/filter:', len(unused))
for db, name in unused:
    print('  %s  %s' % (db, name))
