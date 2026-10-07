# Index every upstream declaration (by FQN) to its repository, then place what legend-lite carries today.
import re, sys, collections, json
H, B = sys.argv[1:3]
files = [l.rstrip('\n').split('\t') for l in open(f'{H}/files.tsv')]
DECL = re.compile(r'^\s*(?:native\s+)?(function|Class|Enum|Association|Profile|Primitive|Measure)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
NATIVE = re.compile(r'^\s*native\s+function\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
where = collections.defaultdict(set); kind = collections.defaultdict(set); upnative = set()
for repo, path in files:
    t = open(path, errors='replace').read()
    for k, fqn in DECL.findall(t): where[fqn].add(repo); kind[fqn].add(k)
    for fqn in NATIVE.findall(t): upnative.add(fqn)
json.dump({k: sorted(v) for k, v in where.items()}, open(f'{H}/decl_repo.json', 'w'))
print(f"{len(where):,} distinct upstream FQNs declared; {len(upnative)} upstream-native FQNs")
def place(label, fqns):
    c = collections.Counter(); out = []
    for f in fqns:
        rs = where.get(f)
        if not rs: c['(not declared upstream)'] += 1; out.append(f); continue
        for r in sorted(rs): c[r] += 1
    print(f"\n{label}: {len(fqns)} FQNs")
    for r, n in c.most_common(14): print(f"   {n:5}  {r}")
    return out
# 1. Pure.java's platform-lowered signatures (FQN of each `signature("native function <fqn>(...")`)
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
pfq = sorted(set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj)))
lite = [f for f in pfq if f.startswith('meta::legend::lite::')]
nolite = place("Pure.java platform-lowered FQNs (excluding legend-lite's own)", [f for f in pfq if f not in lite])
print(f"   (+ {len(lite)} legend-lite's own meta::legend::lite natives: ours, not upstream)")
print("   not declared upstream:", nolite[:8])
# 2. prelude.pure declarations
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
prf = sorted(set(f for k, f in DECL.findall(pr)))
miss = place("prelude.pure declarations", prf)
print("   not declared upstream:", miss[:8])
