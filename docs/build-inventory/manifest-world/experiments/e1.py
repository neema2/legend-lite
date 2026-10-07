# Experiment 1: upstream's own two lists. Functions: every signature id / FQN registered as a handler in the core
# compiler (Handlers.java, CoreCompilerExtension) and the relational compiler extension. Types: every class those two
# compiler modules INSTANTIATE (new Root_meta_..._Impl). Other engine extensions are listed for information only.
import re, sys, os, glob, json, csv, collections
H, B = sys.argv[1:3]
ET = open(f'{H}/et').read().strip()
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0 = g['EL'], g['M0']
fn_fqns = {e['fqn'] for e in EL if e['kind'] in ('function', 'native function')}
ty = {e['fqn'] for e in EL if e['kind'] in ('Class', 'Enum', 'Association', 'Profile', 'Measure', 'Primitive')}
home = {}
for e in EL:
    if not e['test'] or e['fqn'] not in home: home.setdefault(e['fqn'], 'upstream core' if e['repo'] in M0 else e['repo'])
CORE = 'legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java'
REL = 'legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar/src/main/java'
ID = re.compile(r'"(meta::[A-Za-z0-9_:$]+)"')
def fqn_of(s):
    if s in fn_fqns: return s
    parts = s.split('_')
    for k in range(len(parts) - 1, 0, -1):
        c = '_'.join(parts[:k])
        if c in fn_fqns: return c
    return None
def handlers(files):
    out = set(); unmatched = set()
    for f in files:
        for s in ID.findall(open(f, errors='replace').read()):
            if s in ty: continue
            q = fqn_of(s)
            (out.add(q) if q else unmatched.add(s))
    return out, unmatched
core_files = [os.path.join(ET, CORE, p) for p in ['org/finos/legend/engine/language/pure/compiler/toPureGraph/handlers/Handlers.java',
                                                   'org/finos/legend/engine/language/pure/compiler/toPureGraph/CoreCompilerExtension.java']]
core_files = [f for f in core_files if os.path.exists(f)] or [f for f in glob.glob(os.path.join(ET, CORE, '**/*.java'), recursive=True) if os.path.basename(f) in ('Handlers.java', 'CoreCompilerExtension.java')]
rel_files = glob.glob(os.path.join(ET, REL, '**/RelationalCompilerExtension.java'), recursive=True)
hf_core, um_core = handlers(core_files); hf_rel, um_rel = handlers(rel_files)
print(f"handler functions: core compiler {len(hf_core)} (from {[os.path.basename(f) for f in core_files]}), relational extension {len(hf_rel)}; unmatched strings {len(um_core) + len(um_rel)}")
others = [l.strip() for l in open(f'{H}/e1_handler_files.txt') if CORE.split('/src/')[0] not in l and REL.split('/src/')[0] not in l]
for o in others:
    hf_o, _ = handlers([os.path.join(ET, o)])
    print(f"   (not counted) {o.split('/')[1]:60} {len(hf_o)} functions")
# classes the two compiler modules instantiate
java_name = {'Root_' + f.replace('::', '_'): f for f in ty}
NEW = re.compile(r'new\s+(Root_meta_[A-Za-z0-9_]+?)_Impl\b')
REF = re.compile(r'\b(Root_meta_[A-Za-z0-9_]+?)(?:_Impl)?\b')
def classes(root):
    made, refd = set(), set()
    for f in glob.glob(os.path.join(ET, root, '**/*.java'), recursive=True):
        t = open(f, errors='replace').read()
        made |= {java_name[n] for n in NEW.findall(t) if n in java_name}
        refd |= {java_name[n] for n in REF.findall(t) if n in java_name}
    return made, refd
mk_core, rf_core = classes(CORE); mk_rel, rf_rel = classes(REL)
print(f"classes instantiated: core compiler {len(mk_core)}, relational extension {len(mk_rel)} (classes merely named: {len(rf_core)}, {len(rf_rel)})")
S1f, S1c = hf_core | hf_rel, mk_core | mk_rel
S1 = S1f | S1c
print(f"S1 = {len(S1f)} functions + {len(S1c)} classes; where they live: {dict(collections.Counter(home.get(f, '?') for f in S1).most_common(8))}")
rows = {r['fqn']: r for r in csv.DictReader(open(f'{H}/englist.tsv'), delimiter='\t')}
grp = {}
exec(open(f'{H}/enggroups.py').read().split("grp = ")[0].replace("rows = list", "_r = list"), grp)
def gof(f):
    for name, rx in grp['G']:
        if re.search(rx, f): return name.split(' ')[0]
eng = set(rows)
print(f"\nour 269 engine-side names in S1: {len(eng & S1)}  by group: {dict(sorted(collections.Counter(gof(f) for f in eng & S1).items()))}")
user = {f for f in eng if gof(f) in ('A', 'B', 'C')}
print(f"our 101 user-facing names NOT in S1 ({len(user - S1)}): {sorted(f.split('::')[-1] for f in user - S1)}")
print(f"harness names IN S1: {sorted(f.split('::')[-1] + '(' + gof(f) + ')' for f in eng & S1 if gof(f) not in ('A', 'B', 'C'))}")
eng_S1 = {f for f in S1 if home.get(f) not in ('upstream core', None)}
print(f"\nS1 names outside upstream core: {len(eng_S1)}; of them not among our 269: {len(eng_S1 - eng)}")
print("   e.g.", sorted(f for f in eng_S1 - eng)[:40])
json.dump({'functions': sorted(S1f), 'classes': sorted(S1c), 'user101': sorted(user)}, open(f'{H}/e1_seeds.json', 'w'), indent=0)
