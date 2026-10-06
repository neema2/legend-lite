# Which upstream names does today's BOOT itself demand (system metamodel text; Pure.java catalog signatures), which
# are missing from each candidate world, and what do they drag in (closure through declarations and runnable bodies)?
import sys, re, json, collections
H, B = sys.argv[1:3]
exec(open(f'{H}/closure.py').read().split("report = {}")[0].replace("print(f\"universe", "_ = (f\"universe"), globals())
rep = json.load(open(f'{H}/closure.json'))
C = f'{B}/core/src/main/java/com/legend'
pj = open(f'{C}/builtin/Pure.java').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
for f in [f'{C}/platform/CoreFn.java', f'{C}/platform/WalledBodies.java']:
    lowered |= set(re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)"', open(f).read()))
dem = collections.defaultdict(set)
for line in open(f'{H}/boot_demands.tsv'):
    src_, self_, ref, ctx = line.rstrip('\n').split('\t')
    if ref in elems: dem[src_].add(ref)
worlds = {'core': set(base), 'S2': set(base) | set(rep['S2 our 101 names|runnable bodies']), 'S1': set(base) | set(rep['S1 upstream lists|runnable bodies'])}
def close_from(seed, world):
    seen = set(world); todo = [f for f in seed if f not in seen]; seen |= set(todo); added = list(todo)
    while todo:
        f = todo.pop()
        for t in decl[f] | (set() if f in lowered else body[f]):
            if t in elems and t not in seen: seen.add(t); todo.append(t); added.append(t)
    return added
for src_ in ('system', 'catalog'):
    d = dem[src_]
    print(f"\n{src_}: references {len(d)} upstream names; by area: {dict(sorted(collections.Counter(area(f) for f in d).items()))}")
    print("   harness ones:", sorted(f.split('::')[-1] for f in d if area(f) in 'DEFGH')[:40])
print()
for w, ws in worlds.items():
    for label, seed in [('system only', dem['system']), ('system + catalog', dem['system'] | dem['catalog'])]:
        added = close_from(seed, ws)
        print(f"{w:5} + boot demands ({label}): +{len(added)} names, +{sum(size[f] for f in added)/1e3:.0f} KB; by area: {dict(sorted(collections.Counter(area(f) for f in added).items()))}")
        rep[f'{w}|boot {label}'] = sorted(set(added))
json.dump(rep, open(f'{H}/closure.json', 'w'), indent=0)
