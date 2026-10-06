# The obvious check: of what the product carries from upstream (prelude declarations + Pure.java's names, minus m3
# and lite's own), what lies OUTSIDE legend-pure platform* + engine core_functions_*, and in which engine packages,
# files and folders? How dense is what we need in each (needed elements / all non-test elements there)?
import re, sys, os, json, collections
H, B = sys.argv[1:3]
sys.path.insert(0, H); from docstart import element_starts, is_element_block, strip_doc, decl_head
repos = json.load(open(f'{H}/repos.json'))
files = collections.defaultdict(list)
for line in open(f'{H}/files.tsv'):
    r, p = line.rstrip('\n').split('\t'); files[r].append(p)
START = re.compile(r'^(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\b', re.M)
SECTION = re.compile(r'^###(\w*)', re.M)
FQN = re.compile(r'(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)')
TESTSTEREO = re.compile(r'<<[^>]*\b(test\.\w+|PCT\.test\w*|PCT\.\w*[Tt]est\w*)\b[^>]*>>')
DECL = re.compile(r'^\s*(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
def is_test(head, fqn): return bool(TESTSTEREO.search(head)) or bool(re.search(r'::tests::|::tests$', fqn))
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
need = set(DECL.findall(pr)) | set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
need = {f for f in need if not f.startswith('meta::pure::metamodel') and not f.startswith('meta::legend::lite')}
M0 = {r for r in repos if r.startswith('platform') or r.startswith('core_functions')}
EL = []   # repo file rel dir fqn pkg kind bytes test
for r, ps in files.items():
    for p in ps:
        if r == 'platform' and p.endswith('/m3.pure'): continue
        t = open(p, errors='replace').read()
        cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
        rel = p.split('/src/main/resources/')[-1]
        for a, b in zip(cuts, cuts[1:]):
            blk = t[a:b]
            if not is_element_block(blk, START): continue
            d = strip_doc(blk); fqn, head = decl_head(blk)
            if not fqn: continue
            kind = d.split(None, 1)[0] if not d.startswith('native') else 'native function'
            EL.append(dict(repo=r, file=p, rel=rel, dir=os.path.dirname(rel), fqn=fqn, pkg='::'.join(fqn.split('::')[:-1]),
                           kind=kind, bytes=b - a, test=is_test(head, fqn)))
in_m0 = {e['fqn'] for e in EL if e['repo'] in M0 and not e['test']}
anywhere = {e['fqn'] for e in EL if not e['test']}
eng_need = {f for f in need if f not in in_m0 and f in anywhere}
print(f"carried: {len(need)}; in platform+core_functions: {len(need & in_m0)}; ENGINE-SIDE: {len(eng_need)}; "
      f"only as upstream TEST elements: {len({f for f in need - in_m0 - anywhere if f in {e['fqn'] for e in EL}})}; nowhere: {len(need - {e['fqn'] for e in EL})}")
E = [e for e in EL if e['repo'] not in M0 and not e['test']]          # every non-test engine element outside M0
NE = [e for e in E if e['fqn'] in eng_need]
print(f"engine-side needed elements (overloads counted): {len(NE)}, {sum(e['bytes'] for e in NE)/1e3:.0f} KB; kinds: {dict(collections.Counter(e['kind'] for e in NE))}")
print("by module:", dict(collections.Counter(e['repo'] for e in NE).most_common()))
def table(key, title, limit=40):
    tot = collections.defaultdict(lambda: [0, 0]); nd = collections.defaultdict(lambda: [0, 0])
    for e in E:
        k = key(e); tot[k][0] += 1; tot[k][1] += e['bytes']
        if e['fqn'] in eng_need: nd[k][0] += 1; nd[k][1] += e['bytes']
    rows = sorted(nd, key=lambda k: -nd[k][0])
    print(f"\n{title}: {len(rows)} hold needed names. needed/total elements, needed KB / total KB")
    for k in rows[:limit]:
        print(f"   {nd[k][0]:4}/{tot[k][0]:<5} {nd[k][1]/1e3:6.1f}/{tot[k][1]/1e3:7.1f} KB  {nd[k][0]/tot[k][0]:4.0%}  {k}")
    whole = sum(tot[k][1] for k in rows); els = sum(tot[k][0] for k in rows)
    print(f"   -> taking all {len(rows)} whole: {els} elements, {whole/1e6:.2f} MB (needed alone: {sum(nd[k][1] for k in rows)/1e6:.2f} MB)")
    return rows
pk = table(lambda e: e['pkg'], 'PACKAGES (exact)')
table(lambda e: e['rel'], 'FILES', 30)
table(lambda e: e['dir'], 'FOLDERS (exact)', 30)
table(lambda e: '/'.join(e['dir'].split('/')[:3]), 'FOLDERS (3 levels)', 30)
mods = {e['repo'] for e in NE}
print(f"\nwhole modules {sorted(mods)}: {sum(1 for e in E if e['repo'] in mods)} elements, {sum(e['bytes'] for e in E if e['repo'] in mods)/1e6:.2f} MB")
json.dump(sorted(eng_need), open(f'{H}/eng_need.json', 'w'), indent=0)
