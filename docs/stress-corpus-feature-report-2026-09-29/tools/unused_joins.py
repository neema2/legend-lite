"""Joins that no mapping, view or filter references (report §8).

Env PMCD_JSON=<stress-pmcd.json>. Usage: unused_joins.py
"""
import json
import re

from lib import E, fp, dbs

used = set()
s = json.dumps([e for e in E if e['_type'] in ('mapping', 'relational')])
for m in re.finditer(r'"joins": \[(.*?)\]', s):
    for n in re.findall(r'"name": "([^"]+)"', m.group(1)):
        used.add(n)
alljoins = [(fp(d), j['name']) for d in dbs for j in d.get('joins') or []]
unused = [j for j in alljoins if j[1] not in used]
print('joins', len(alljoins), 'unused by any mapping/view/filter:', len(unused))
for db, name in unused:
    print('  %s  %s' % (db, name))
