# usage.py, refined: (1) who uses what, each seed on its own; (2) the universe against today's prelude;
# (3) synthesized worlds (U, U minus never-mentioned, U as user programs need it) written as .pure files for timing.
import re, sys, os
sys.path.insert(0, sys.argv[1]); from docstart import element_starts, is_element_block, strip_doc, decl_head
import re as _re, glob, collections, json, shutil
H, B, ET = sys.argv[1:4]
START = re.compile(r'^(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\b', re.M)
SECTION = re.compile(r'^###(\w*)', re.M)
FQN = re.compile(r'(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)')
TESTSTEREO = re.compile(r'<<[^>]*\b(test\.\w+|PCT\.test\w*|PCT\.\w*[Tt]est\w*)\b[^>]*>>')
DECL = re.compile(r'^\s*(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
def spans(t):
    cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
    for a, b in zip(cuts, cuts[1:]):
        blk = t[a:b]
        if is_element_block(blk, START):
            fqn, head = decl_head(blk)
            yield a, b, fqn, head
def is_test(head, fqn): return bool(TESTSTEREO.search(head)) or bool(re.search(r'::tests::|::tests$', fqn))
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
prelude_decls = set(DECL.findall(pr))
used_today = lowered | prelude_decls
w1 = [p for p in open(f'{H}/W1.files').read().split() if not p.endswith('/m3.pure')]
w2x = [p for p in open(f'{H}/W2.files').read().split() if p not in set(open(f'{H}/W1.files').read().split())]
whole, mixed = [], []
for p in w2x:
    ds = set(DECL.findall(open(p, errors='replace').read()))
    (whole if ds and len(ds & used_today) / len(ds) >= 0.5 else mixed).append(p)
U = []  # dicts: fqn simple pkg file rel a b bytes text
dropped = collections.defaultdict(list)  # file -> spans always dropped (tests, unpicked)
texts = {}
for paths, pick in [(w1, None), (whole, None), (mixed, used_today)]:
    for p in paths:
        t = texts[p] = open(p, errors='replace').read()
        for a, b, fqn, head in spans(t):
            if not fqn or is_test(head, fqn) or (pick is not None and fqn not in pick):
                dropped[p].append((a, b)); continue
            U.append(dict(fqn=fqn, simple=fqn.split('::')[-1], pkg='::'.join(fqn.split('::')[:-1]), file=p,
                          rel=p.split('/src/main/resources/')[-1], a=a, b=b, bytes=b - a, text=t[a:b]))
by_simple = collections.defaultdict(list)
for i, u in enumerate(U): by_simple[u['simple']].append(i)
fq_index = {u['fqn']: i for i, u in enumerate(U)}
KEYWORDS = set('function native Class Enum Association Profile Primitive Measure import let if true false extends self this'.split())
def names(text): return set(IDENT.findall(text)) - KEYWORDS
toks = {}
rel_root = os.path.join(ET, 'legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation')
corpus = [p for p in glob.glob(rel_root + '/**/*.pure', recursive=True) if re.search(r'/tests?/', p)]
projects = glob.glob(f'{B}/projects/**/*.pure', recursive=True)
demos = sum([glob.glob(f'{B}/{d}/**/*.pure', recursive=True) for d in ['datacube/demo', 'query/demo', 'studio/demo']], [])
java_fqns = set()
for p in glob.glob(f'{B}/core/src/main/java/**/*.java', recursive=True):
    java_fqns |= set(re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)', open(p, errors='replace').read()))
def seed_from(paths):
    s = set()
    for p in paths:
        for t in names(open(p, errors='replace').read()):
            s.update(by_simple.get(t, []))
    return s
def seed_java():
    s = set()
    for f in lowered | java_fqns:
        if f in fq_index: s.add(fq_index[f])
        s.update(by_simple.get(f.split('::')[-1], []))
    return s
def close(seed):
    used = set(seed); frontier = list(seed)
    while frontier:
        nxt = []
        for i in frontier:
            for t in names(U[i]['text']):
                for j in by_simple.get(t, []):
                    if j not in used: used.add(j); nxt.append(j)
        frontier = nxt
    return used
S = {'projects': seed_from(projects), 'demos': seed_from(demos), 'our Java': seed_java(), 'corpus tests': seed_from(corpus)}
C = {k: close(v) for k, v in S.items()}
C['user programs + our Java'] = close(S['projects'] | S['demos'] | S['our Java'])
C['all'] = close(set().union(*S.values()))
tot = sum(u['bytes'] for u in U)
def mb(ix): return sum(U[i]['bytes'] for i in ix) / 1e6
print(f"universe: {len(U)} elements, {tot/1e6:.2f} MB (files: {len(w1)} W1 + {len(whole)} whole + {len(mixed)} mixed)")
print(f"{'seed':28} {'direct':>7} {'closed':>7} {'MB':>6} {'share':>6}")
for k in ['projects', 'demos', 'our Java', 'corpus tests', 'user programs + our Java', 'all']:
    d = len(S[k]) if k in S else ''
    print(f"{k:28} {d:>7} {len(C[k]):>7} {mb(C[k]):6.2f} {mb(C[k])/(tot/1e6):6.0%}")
# the universe against today's prelude
inp = {i for i, u in enumerate(U) if u['fqn'] in prelude_decls}
new = set(range(len(U))) - inp
print(f"\nin today's prelude: {len(inp)} elements {mb(inp):.2f} MB; NEW (not in today's prelude): {len(new)} elements {mb(new):.2f} MB")
for k in ['user programs + our Java', 'all']:
    print(f"   used by {k}: of prelude part {len(inp & C[k])}/{len(inp)}, of new part {len(new & C[k])}/{len(new)} ({mb(new & C[k]):.2f} of {mb(new):.2f} MB)")
ufq = {u['fqn'] for u in U}
pre_out = sorted(prelude_decls - ufq)
roots = collections.Counter(f.split('::')[0] + '::' + f.split('::')[1] if f.count('::') > 1 else f for f in pre_out)
print(f"prelude declarations NOT in the universe: {len(pre_out)}; by top package: {roots.most_common(8)}")
mm = [f for f in pre_out if f.startswith('meta::pure::metamodel')]
print(f"   of which m3 metamodel: {len(mm)}; others e.g. {[f for f in pre_out if not f.startswith('meta::pure::metamodel')][:12]}")
# per package: user-programs view
pk = collections.defaultdict(lambda: [0, 0, 0, 0, 0])
for i, u in enumerate(U):
    v = pk[u['pkg']]; v[0] += 1; v[3] += u['bytes']
    if i in C['user programs + our Java']: v[1] += 1
    if i in C['all']: v[2] += 1; v[4] += u['bytes']
print(f"\npackages by bytes (elements: user-used / any-used / total):")
for k, v in sorted(pk.items(), key=lambda kv: -kv[1][3])[:25]:
    print(f"   {v[3]/1e3:6.1f} KB  {v[1]:4}/{v[2]:4}/{v[0]:4}  {k}")
# synthesized worlds
def write_world(name, keep):
    root = f'{H}/synth/{name}'
    shutil.rmtree(root, ignore_errors=True)
    out = []
    for p, t in texts.items():
        cut = list(dropped[p]) + [(u['a'], u['b']) for i, u in enumerate(U) if u['file'] == p and i not in keep]
        if not any(i in keep for i in range(len(U)) if U[i]['file'] == p): continue
        cut.sort(); s = []; pos = 0
        for a, b in cut: s.append(t[pos:a]); pos = b
        s.append(t[pos:])
        dst = f"{root}/{p.split('/src/main/resources/')[-1]}"
        os.makedirs(os.path.dirname(dst), exist_ok=True); open(dst, 'w').write(''.join(s)); out.append(dst)
    open(f'{H}/synth/{name}.files', 'w').write('\n'.join(out) + '\n')
    print(f"world {name:6}: {len(keep)} elements, {len(out)} files, {sum(os.path.getsize(f) for f in out)/1e6:.2f} MB on disk")
by_file = collections.defaultdict(list)
for i, u in enumerate(U): by_file[u['file']].append(i)
os.makedirs(f'{H}/synth', exist_ok=True)
print()
write_world('U', set(range(len(U))))
write_world('Uall', C['all'])
write_world('Uuser', C['user programs + our Java'])
json.dump({'user_unused': sorted(U[i]['fqn'] for i in range(len(U)) if i not in C['user programs + our Java']),
           'all_unused': sorted(U[i]['fqn'] for i in range(len(U)) if i not in C['all'])}, open(f'{H}/unused2.json', 'w'), indent=0)
