# Builds inventory.tsv / inventory.json from Bazel's own graph (targets.jsonl, edges.dot, reach_*.txt).
import json, re, glob, os, collections
I = os.path.dirname(os.path.abspath(__file__))
NOISE = re.compile(r'node_modules|\.aspect_rules_js|_types_\w+$')
rows = {}
for line in open(f'{I}/targets.jsonl'):
    r = json.loads(line)
    if r['type'] != 'RULE': continue
    r = r['rule']; name = r['name']
    a = {x['name']: x for x in r['attribute']}
    def val(k, f='stringListValue'):
        x = a.get(k); return (x or {}).get(f) if x and x.get('explicitlySpecified') else None
    def strv(k): x = a.get(k); return (x or {}).get('stringValue') if x and x.get('explicitlySpecified') else None
    def lst(k): x = a.get(k); return (x or {}).get('stringListValue', []) if x and x.get('explicitlySpecified') else []
    tags = lst('tags')
    gen = a.get('generator_function', {}).get('stringValue', '')
    rows[name] = dict(label=name, kind=r['ruleClass'], macro=gen, loc=os.path.relpath(r['location'].rsplit(':',2)[0], os.path.dirname(os.path.dirname(I))) + ':' + r['location'].rsplit(':',2)[1],
        tags=tags, manual='manual' in tags, testonly=bool((a.get('testonly') or {}).get('booleanValue')) ,
        size=strv('size'), srcs=lst('srcs'), outs=lst('outs'), data=lst('data'), deps=[], users=[], noise=bool(NOISE.search(name)))
for l in open(f'{I}/edges.dot'):
    m = re.match(r'\s*"(.+?)"\s*->\s*"(.+?)"', l)
    if m and m[1] in rows and m[2] in rows and m[1] != m[2]:
        rows[m[1]]['deps'].append(m[2]); rows[m[2]]['users'].append(m[1])
reach = {}
for f in glob.glob(f'{I}/reach_*.txt'):
    k = os.path.basename(f)[6:-4]
    for t in open(f).read().split():
        if t in rows: rows[t].setdefault('reaches', []).append(k)
json.dump(rows, open(f'{I}/inventory.json', 'w'), indent=1)
with open(f'{I}/inventory.tsv', 'w') as o:
    o.write('label\tkind\tmacro\tmanual\ttestonly\tsize\tnsrcs\tndeps\tnusers\treaches\tloc\n')
    for k, r in sorted(rows.items()):
        if r['noise']: continue
        o.write('\t'.join(map(str, [k, r['kind'], r['macro'], int(r['manual']), int(r['testonly']), r['size'] or '', len(r['srcs']), len(r['deps']), len(r['users']), ','.join(sorted(r.get('reaches', []))), r['loc']])) + '\n')
c = collections.Counter(r['kind'] for r in rows.values() if not r['noise'])
print(sum(c.values()), 'targets (noise excluded)'); [print(f'{n:5} {k}') for k, n in c.most_common()]
