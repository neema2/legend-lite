# Experiment 8 setup: the corpus's REAL manifest. The runner's own composition stays as it is (relational tree, M2M
# test models, graph-fetch domain, LIBRARY_FILES, SHAPE_FILES classes); this adds the REST of the manifest: every file
# of the repositories the relational tree belongs to and their whole dependency closure, tests included, written as
# extra library sources. Each element is loaded once: nothing the composition or the default world (the boot) already
# declares, no platform-namespace function (the runner's guard), no function the platform owns (the ownership filter).
import sys, re, os, json, collections, shutil
H, B, world = sys.argv[1:4]
sys.path.insert(0, H); from docstart import element_starts, is_element_block, decl_head
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, repos, files, START, SECTION, M0 = g['EL'], g['repos'], g['files'], g['START'], g['SECTION'], g['M0']
ET = open(f'{H}/et').read().strip()
RELATIONAL = 'legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/'
M2M = 'core/store/m2m/tests'; GF = 'core/pure/graphFetch/domain'
LIB = ['pureToSQLQuery/pureToSQLQuery.pure', 'core_external_store_relational_sql_dialect_translation/utils.pure']
EXCLUDED_IMPL = 'lineage/scanRelations/scanRelations.pure'
def in_composition(p):
    r = p.split(ET + '/')[-1]
    if r.startswith(RELATIONAL): return not r.endswith(EXCLUDED_IMPL)
    return (M2M in p) or (GF in p) or any(p.endswith(x) for x in LIB)
corpus_repos = {r for r, ps in files.items() if any(RELATIONAL in p for p in ps)}
def closure(roots):
    out, todo = set(), list(roots)
    while todo:
        r = todo.pop()
        if r in out or r not in repos: continue
        out.add(r); todo.extend(repos[r]['deps'])
    return out
man = closure(corpus_repos)
NONPURE = re.compile(r'^\s*(?:Database|Mapping|Runtime|SingleConnectionRuntime|RelationalDatabaseConnection|Service|Connection\w*|\w+Connection)\s+((?:\w+::)+\w+)', re.M)
def declared(text):
    cuts = sorted(set(element_starts(text, START) + [m.start() for m in SECTION.finditer(text)] + [len(text)]))
    names = set()
    for a, b in zip(cuts, cuts[1:]):
        blk = text[a:b]
        if is_element_block(blk, START):
            f, h = decl_head(blk)
            if f: names.add(f)
    names |= set(NONPURE.findall(text))
    return names
comp_files = sorted({p for r in files for p in files[r] if in_composition(p)})
comp = set()
for p in comp_files: comp |= declared(open(p, errors='replace').read())
boot = declared(open(world).read())
# the ownership filter, as in synth_prelude.py
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure').read()
claimed = {l.split('\t')[0] for l in open(f'{B}/core/src/main/resources/com/legend/builtin/native-claims.tsv') if l.strip() and not l.startswith(('#', 'fqn\t'))}
corefn = open(f'{B}/core/src/main/java/com/legend/platform/CoreFn.java').read()
forms = set(re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)"', corefn)); form_names = set(re.findall(r'^\s+[A-Z_0-9]+\("([A-Za-z_]+)"', corefn, re.M))
def footer(title):
    out, on = set(), False
    for line in pr.splitlines():
        if line.startswith('// ') and line[3:4].isupper(): on = line.startswith('// ' + title)
        elif on and line.startswith('//   meta::'): out.add(line[5:].split()[0])
    return out
system = set(open(f'{H}/system_fqns.txt').read().split())
owned_fns = claimed | forms | footer('PLATFORM-OWNED NAMES') | footer('SYSTEM-OWNED FUNCTIONS') | set(open(f'{H}/e6_owned_final.txt').read().split())
why = collections.Counter()
def keep(f, h):
    fn = h.lstrip().split(None, 1)[0] in ('function', 'native')
    if f in comp: why['already in the composition'] += 1; return False
    if f in boot: why['already in the default world'] += 1; return False
    if f in system: why['the system metamodel owns it'] += 1; return False
    if fn and f.startswith('meta::pure::functions::'): why['platform namespace'] += 1; return False
    if fn and (f in owned_fns or f.split('::')[-1] in form_names): why['the platform owns it'] += 1; return False
    return True
def cut(t):
    cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
    parts = [t[:cuts[0]]]; pure = True; n = 0; skip_section = False
    for a, b in zip(cuts, cuts[1:]):
        blk = t[a:b]
        sm = SECTION.match(blk)
        if sm:
            pure = sm.group(1) in ('Pure', '')
            if not pure:
                names = set(NONPURE.findall(blk))
                if names & (comp | boot): why['non-Pure section already loaded'] += 1; continue
                n += 1
            parts.append(blk); continue
        if not pure: parts.append(blk); continue
        if is_element_block(blk, START):
            f, h = decl_head(blk)
            if f and keep(f, h): parts.append(blk); n += 1
        else: parts.append(blk)
    return ''.join(parts), n
out_dir = f'{H}/e8_extra'; shutil.rmtree(out_dir, ignore_errors=True); os.makedirs(out_dir)
listed = []; kept = 0
extra_files = sorted({p for r in man for p in files[r] if not in_composition(p) and not (r == 'platform' and p.endswith('/m3.pure'))})
for i, p in enumerate(extra_files):
    txt, n = cut(open(p, errors='replace').read())
    if n:
        dst = f'{out_dir}/{i:04d}_{os.path.basename(p)}'; open(dst, 'w').write(txt); listed.append(dst); kept += n
open(f'{H}/e8_extra.files', 'w').write('\n'.join(listed) + '\n')
print(f"corpus repositories: {sorted(corpus_repos)}")
print(f"real manifest: {len(man)} repositories; composition files {len(comp_files)}; extra files {len(extra_files)} -> {len(listed)} written, {kept} elements/sections kept, {sum(os.path.getsize(f) for f in listed)/1e6:.2f} MB")
print("left out:", dict(why))
