# The upstream module graph: every Pure code repository (legend-pure's platform.json and *.definition.json, legend-engine's
# *.definition.json), its package pattern, dependencies, the .pure files under its resource root, and sizes.
import json, os, re, sys, collections
PT, ET, OUT = sys.argv[1:4]
repos = {}
def scan(tree, side):
    for root, dirs, files in os.walk(tree, followlinks=True):
        if '/src/main/resources' not in root + '/': continue
        for f in files:
            if f == 'platform.json' or f.endswith('.definition.json'):
                p = os.path.join(root, f)
                try: d = json.load(open(p))
                except Exception: continue
                if 'name' not in d: continue
                res = root[:root.index('/src/main/resources') + len('/src/main/resources')]
                repos[d['name']] = dict(name=d['name'], side=side, pattern=d.get('pattern', ''), deps=d.get('dependencies', []),
                                        definition=os.path.relpath(p, tree), resources=res)
scan(PT, 'pure'); scan(ET, 'engine')
# files: a repository's .pure files live in <resources>/<name>/ (legend's layout); count them
for r in repos.values():
    d = os.path.join(r['resources'], r['name'])
    files = []
    if os.path.isdir(d):
        for root, dirs, fs in os.walk(d, followlinks=True):
            files += [os.path.join(root, f) for f in fs if f.endswith('.pure')]
    r['files'] = sorted(files)
    r['lines'] = sum(sum(1 for _ in open(f, errors='replace')) for f in files)
def closure(names):
    seen, stack = set(), list(names)
    while stack:
        n = stack.pop()
        if n in seen: continue
        seen.add(n)
        if n in repos: stack += repos[n]['deps']
    return seen
missing = sorted({d for r in repos.values() for d in r['deps'] if d not in repos})
json.dump({n: {k: v for k, v in r.items() if k != 'files'} | {'nfiles': len(r['files'])} for n, r in repos.items()},
          open(os.path.join(OUT, 'repos.json'), 'w'), indent=1)
with open(os.path.join(OUT, 'files.tsv'), 'w') as o:
    for r in repos.values():
        for f in r['files']: o.write(f"{r['name']}\t{f}\n")
print(f"{len(repos)} repositories ({sum(1 for r in repos.values() if r['side']=='pure')} legend-pure, "
      f"{sum(1 for r in repos.values() if r['side']=='engine')} legend-engine); deps naming no known repository: {missing}")
print(f"{sum(len(r['files']) for r in repos.values())} .pure files, {sum(r['lines'] for r in repos.values()):,} lines")
def size(ns): return (len(ns), sum(len(repos[n]['files']) for n in ns if n in repos), sum(repos[n]['lines'] for n in ns if n in repos))
platform = {n for n in repos if repos[n]['side'] == 'pure'}
for label, ns in [('legend-pure platform (all 9)', platform),
                  ('closure(core_relational)', closure(['core_relational'])),
                  ('closure(core_functions_*)', closure([n for n in repos if n.startswith('core_functions')])),
                  ('everything', set(repos))]:
    k, f, l = size(ns); print(f"  {label:32} {k:4} repos {f:6} files {l:9,} lines")
