# Which parts of the candidate default world does anything we know actually use?
# Universe = the non-test elements of: platform* + core_functions_* (whole), the 21 mostly-used engine files (whole),
# and the 160 used elements of the 55 mixed engine files. "Used" = named (by simple name or FQN) by a real program
# (corpus tests, the 56 model projects, the demo models), by our Java (Pure.java, any FQN string in core main), or,
# transitively, by a used element's own text. Simple-name matching over-approximates use on purpose: "unused" means
# nothing anywhere mentions the name.
import re, sys, os, glob, collections, json
H, B, ET = sys.argv[1:4]
START = re.compile(r'^(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\b', re.M)
SECTION = re.compile(r'^###', re.M)
FQN = re.compile(r'(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)')
TESTSTEREO = re.compile(r'<<[^>]*\b(test\.\w+|PCT\.test\w*|PCT\.\w*[Tt]est\w*)\b[^>]*>>')
DECL = re.compile(r'^\s*(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
def blocks(t):
    cuts = sorted([m.start() for m in START.finditer(t)] + [m.start() for m in SECTION.finditer(t)] + [len(t)])
    for a, b in zip(cuts, cuts[1:]):
        blk = t[a:b]
        if START.match(blk):
            head = blk[:400]; m = FQN.search(head.split(None, 1)[1] if ' ' in head else head)
            yield blk, (m.group(1) if m else ''), head
def is_test(head, fqn): return bool(TESTSTEREO.search(head)) or bool(re.search(r'::tests?::|::pct::|::test$|Test$|::tests$', fqn))
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
used_today = lowered | set(DECL.findall(pr))
# the universe
w1 = open(f'{H}/W1.files').read().split(); w2x = [p for p in open(f'{H}/W2.files').read().split() if p not in set(w1)]
whole, mixed = [], []
for p in w2x:
    ds = set(DECL.findall(open(p, errors='replace').read()))
    (whole if ds and len(ds & used_today) / len(ds) >= 0.5 else mixed).append(p)
U = []  # (fqn, simple, pkg, file, bytes, text)
for paths, pick in [(w1, None), (whole, None), (mixed, used_today)]:
    for p in paths:
        rel = p.split('/src/main/resources/')[-1]
        for blk, fqn, head in blocks(open(p, errors='replace').read()):
            if not fqn or is_test(head, fqn): continue
            if pick is not None and fqn not in pick: continue
            U.append((fqn, fqn.split('::')[-1], '::'.join(fqn.split('::')[:-1]), rel, len(blk), blk))
by_simple = collections.defaultdict(list)
for i, u in enumerate(U): by_simple[u[1]].append(i)
fq_index = {u[0]: i for i, u in enumerate(U)}
KEYWORDS = set('function native Class Enum Association Profile Primitive Measure import let if true false extends self this'.split())
def names(text): return set(IDENT.findall(text)) - KEYWORDS
# seeds
programs = {}
rel_root = os.path.join(ET, 'legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation')
for p in glob.glob(rel_root + '/**/*.pure', recursive=True):
    if re.search(r'/tests?/', p): programs.setdefault('corpus tests', []).append(p)
for d, label in [('projects', 'model projects'), ('datacube/demo', 'demo models'), ('query/demo', 'demo models'), ('studio/demo', 'demo models')]:
    programs.setdefault(label, []).extend(glob.glob(f'{B}/{d}/**/*.pure', recursive=True))
java_fqns = set()
for p in glob.glob(f'{B}/core/src/main/java/**/*.java', recursive=True):
    java_fqns |= set(re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)', open(p, errors='replace').read()))
used = set(); why = {}
def mark(i, reason):
    if i not in used: used.add(i); why[i] = reason; return True
    return False
for label, ps in programs.items():
    toks = set()
    for p in ps: toks |= names(open(p, errors='replace').read())
    n0 = len(used)
    for t in toks:
        for i in by_simple.get(t, []): mark(i, label)
    print(f"seed {label:16} {len(ps):5} files -> {len(used) - n0:5} elements")
n0 = len(used)
for f in lowered | java_fqns:
    if f in fq_index: mark(fq_index[f], 'our Java')
    for i in by_simple.get(f.split('::')[-1], []): mark(i, 'our Java')
print(f"seed our Java (Pure.java + FQN strings)  -> {len(used) - n0:5} elements")
# transitive closure through used elements' text
frontier = list(used); rounds = 0
while frontier:
    rounds += 1; nxt = []
    for i in frontier:
        for t in names(U[i][5]):
            for j in by_simple.get(t, []):
                if mark(j, 'closure'): nxt.append(j)
    frontier = nxt
tot_b = sum(u[4] for u in U); used_b = sum(U[i][4] for i in used)
print(f"\nuniverse {len(U)} elements {tot_b/1e6:.2f} MB; used {len(used)} ({used_b/1e6:.2f} MB); UNUSED {len(U)-len(used)} ({(tot_b-used_b)/1e6:.2f} MB); closure rounds {rounds}")
print("used by first reason:", dict(collections.Counter(why.values())))
pk = collections.defaultdict(lambda: [0, 0, 0, 0])  # elements, used, bytes, used bytes
fl = collections.defaultdict(lambda: [0, 0, 0, 0])
for i, u in enumerate(U):
    for d, k in [(pk, u[2]), (fl, u[3])]:
        d[k][0] += 1; d[k][2] += u[4]
        if i in used: d[k][1] += 1; d[k][3] += u[4]
dead_p = [(k, v) for k, v in pk.items() if v[1] == 0]
dead_f = [(k, v) for k, v in fl.items() if v[1] == 0]
print(f"\npackages: {len(pk)}; ENTIRELY unused: {len(dead_p)} ({sum(v[2] for k,v in dead_p)/1e3:.0f} KB)")
for k, v in sorted(dead_p, key=lambda kv: -kv[1][2])[:15]: print(f"   {v[2]/1e3:6.1f} KB {v[0]:4} el  {k}")
print(f"files: {len(fl)}; ENTIRELY unused: {len(dead_f)} ({sum(v[2] for k,v in dead_f)/1e3:.0f} KB)")
for k, v in sorted(dead_f, key=lambda kv: -kv[1][2])[:15]: print(f"   {v[2]/1e3:6.1f} KB {v[0]:4} el  {k}")
part = sorted([(k, v) for k, v in pk.items() if 0 < v[1] < v[0]], key=lambda kv: -(kv[1][2] - kv[1][3]))
print(f"\npackages partly used (most unused bytes first):")
for k, v in part[:10]: print(f"   {k:60} used {v[1]}/{v[0]} elements, unused {(v[2]-v[3])/1e3:.1f} KB")
json.dump({'unused': [U[i][0] for i in range(len(U)) if i not in used]}, open(f'{H}/unused.json', 'w'), indent=0)
