# Does upstream core + the 15 files cover everything? (1) what we implement in Java, (2) what the prelude carries,
# (3) what those 15 files themselves mention that lives outside that world.
import csv, re, sys, collections, json, subprocess, glob
H, B = sys.argv[1:3]
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0, DECL = g['EL'], g['M0'], g['DECL']
rows = list(csv.DictReader(open(f'{H}/englist.tsv'), delimiter='\t'))
GA = r'^meta::(external::store::relational::runtime|pure::alloy::connections|external::store::model|pure::runtime|core::runtime|relational::runtime)::|^meta::relational::metamodel::(DatabaseMapper|RelationalMapper|SchemaMapper|TableMapper)$'
GB = r'^meta::pure::functions::(date|string|boolean)::|^meta::pure::functions::collection::removeAll$'
GC = r'^meta::pure::tds::(?!toRelation)|^meta::pure::functions::collection::AggregateValue$|^meta::relational::mapping::TableTDS$'
group = {}
exec(open(f'{H}/enggroups.py').read().split("grp = ")[0].replace("rows = list", "_rows = list"), group)
def grp_of(f):
    for name, rx in group['G']:
        if re.search(rx, f): return name.split(' ')[0]
files15 = sorted({r['file'] for r in rows if any(re.search(x, r['fqn']) for x in (GA, GB, GC))})
W = [e for e in EL if not e['test'] and (e['repo'] in M0 or e['rel'] in files15)]
inW = {e['fqn'] for e in W}
anyup = {e['fqn'] for e in EL}
nontest_up = {e['fqn'] for e in EL if not e['test']}
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
up_lowered = {f for f in lowered if not f.startswith('meta::legend::lite')}
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
pre = {f for f in DECL.findall(pr) if not f.startswith('meta::pure::metamodel')}
def where(f):
    if f in inW: return 'in the world'
    if f in nontest_up: return 'engine harness (outside the world): group ' + str(grp_of(f))
    if f in anyup: return 'upstream TEST element only'
    return 'no upstream element found'
for label, S in [("functions Pure.java implements (upstream names)", up_lowered), ("prelude declarations (minus m3)", pre)]:
    c = collections.Counter(where(f).split(':')[0] for f in S)
    print(f"\n{label}: {len(S)}")
    for k, v in c.most_common(): print(f"   {v:4}  {k}")
    hg = collections.Counter(where(f).split('group ')[1] for f in S if 'group' in where(f))
    if hg: print("      harness by group:", dict(sorted(hg.items())))
nf = sorted(f for f in up_lowered | pre if where(f) == 'no upstream element found')
m3 = open([p for p in subprocess.run(['find', '-L', open(f'{H}/pt').read().strip(), '-name', 'm3.pure'], capture_output=True, text=True).stdout.split() if '/platform/' in p][0]).read()
print(f"\nno upstream element found ({len(nf)}):")
for f in nf:
    s = f.split('::')[-1]
    tag = 'm3.pure' if re.search(r'\b' + re.escape(f) + r'\b', m3) else ''
    print(f"   {f}  {'(Pure.java)' if f in lowered else ''}{'(prelude)' if f in pre else ''} {tag}")
# what the 15 files mention that is defined only outside the world (short names, comments stripped)
defs_out = collections.defaultdict(set)
for e in EL:
    if not e['test'] and e['fqn'] not in inW: defs_out[e['fqn'].split('::')[-1]].add(e['fqn'])
in_short = {f.split('::')[-1] for f in inW}
IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
miss = collections.defaultdict(set)
for e in W:
    if e['rel'] not in files15: continue
    t = open(e['file'], errors='replace').read()
for f in files15:
    p = next(e['file'] for e in W if e['rel'] == f)
    t = re.sub(r'/\*.*?\*/|//[^\n]*', '', open(p, errors='replace').read(), flags=re.S)
    t = re.sub(r"'(?:[^'\\]|\\.)*'", "''", t)
    for tok in set(IDENT.findall(t)):
        if tok in defs_out and tok not in in_short:
            miss[f].add(tok)
print(f"\nnames the 15 files mention that exist only outside the world (short-name match, so an over-count):")
for f in files15:
    if miss[f]: print(f"   {f}: {len(miss[f])}  e.g. {sorted(miss[f])[:10]}")
