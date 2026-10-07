# Experiment 4: the default world from three starting sets, closed over OUR resolver's references (uni_edges.tsv),
# once through declarations only and once through bodies too. Upstream core is always in, whole.
import sys, re, json, csv, collections
H, B = sys.argv[1:3]
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0 = g['EL'], g['M0']
size = collections.Counter(); rel = {}; repo = {}
for e in EL:
    if e['test']: continue
    size[e['fqn']] += e['bytes']; rel.setdefault(e['fqn'], e['rel']); repo.setdefault(e['fqn'], e['repo'])
decl, body, elems = collections.defaultdict(set), collections.defaultdict(set), set()
for line in open(f'{H}/uni_edges.tsv'):
    s, kind, t, ctx = line.rstrip('\n').split('\t')
    elems.add(s)
    if ctx == 'decl': decl[s].add(t)
    elif ctx == 'body': body[s].add(t)
grp = {}
exec(open(f'{H}/enggroups.py').read().split("grp = ")[0].replace("rows = list", "_r = list"), grp)
def area(f):
    if repo.get(f) in M0: return 'core'
    for name, rx in grp['G']:
        if name.startswith('I'): break
        if re.search(rx, f): return name.split(' ')[0]
    return 'other'
seeds = json.load(open(f'{H}/e1_seeds.json'))
files15 = sorted({rel[f] for f in seeds['user101'] if f in rel})
S = {'S1 upstream lists': set(seeds['functions']) | set(seeds['classes']),
     'S2 our 101 names': set(seeds['user101']),
     'S3 the 15 files whole': {f for f in elems if rel.get(f) in files15}}
base = {f for f in elems if repo.get(f) in M0}
print(f"universe {len(elems)} names; upstream core {len(base)} names {sum(size[f] for f in base)/1e6:.2f} MB; 15 files: {len(files15)}")
def close(seed, with_body):
    seen = {f: None for f in base}
    todo = []
    for f in seed:
        if f in elems and f not in seen: seen[f] = '(seed)'; todo.append(f)
    while todo:
        f = todo.pop()
        for t in decl[f] | (body[f] if with_body else set()):
            if t in elems and t not in seen: seen[t] = f; todo.append(t)
    return seen
def chain(seen, f):
    out = [f]
    while seen.get(out[-1]) not in (None, '(seed)') and len(out) < 8: out.append(seen[out[-1]])
    return ' <- '.join(x.split('::')[-1] for x in out)
report = {}
for name, seed in S.items():
    miss = {f for f in seed if f not in elems}
    for mode in ('declarations', 'with bodies'):
        seen = close(seed, mode == 'with bodies')
        extra = [f for f in seen if f not in base]
        by = collections.Counter(area(f) for f in extra)
        kb = sum(size[f] for f in extra) / 1e3
        harness = sorted(f for f in extra if area(f) in 'DEFGH')
        print(f"\n{name} ({len(seed)} seeds, {len(miss)} not in universe) — {mode}: +{len(extra)} names, +{kb:.0f} KB on top of upstream core; by area: {dict(sorted(by.items()))}")
        for f in harness[:6]: print(f"     harness: {chain(seen, f)}")
        report[f'{name}|{mode}'] = sorted(extra)
json.dump(report, open(f'{H}/closure.json', 'w'), indent=0)

# Mode 3: bodies followed only through functions that would RUN upstream's body — not through the ones the platform
# lowers (Pure.java's catalog), owns as a language form (CoreFn) or walls (WalledBodies).
import glob
C = f'{B}/core/src/main/java/com/legend'
pj = open(f'{C}/builtin/Pure.java').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
for f in [f'{C}/platform/CoreFn.java', f'{C}/platform/WalledBodies.java']:
    lowered |= set(re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)"', open(f).read()))
print(f"\nfunctions whose upstream body never runs (lowered, forms, walled): {len(lowered)}")
def close_runnable(seed):
    seen = {f: None for f in base}; todo = []
    for f in seed:
        if f in elems and f not in seen: seen[f] = '(seed)'; todo.append(f)
    while todo:
        f = todo.pop()
        for t in decl[f] | (set() if f in lowered else body[f]):
            if t in elems and t not in seen: seen[t] = f; todo.append(t)
    return seen
for name, seed in S.items():
    seen = close_runnable(seed)
    extra = [f for f in seen if f not in base]
    by = collections.Counter(area(f) for f in extra)
    print(f"{name} — bodies of runnable functions only: +{len(extra)} names, +{sum(size[f] for f in extra)/1e3:.0f} KB; by area: {dict(sorted(by.items()))}")
    for f in sorted(f for f in extra if area(f) in 'DEFGH')[:4]: print(f"     harness: {chain(seen, f)}")
    report[f'{name}|runnable bodies'] = sorted(extra)
json.dump(report, open(f'{H}/closure.json', 'w'), indent=0)
