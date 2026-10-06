# Upstream-only worlds: whole repositories (upstream's own unit), closed over upstream's declared dependencies,
# with test elements stripped by upstream's own markers. The only choice is the ROOT repositories.
import re, sys, os
sys.path.insert(0, sys.argv[1]); from docstart import element_starts, is_element_block, strip_doc, decl_head
import re as _re, json, collections, shutil
H, B = sys.argv[1:3]
repos = json.load(open(f'{H}/repos.json'))
files = collections.defaultdict(list)
for line in open(f'{H}/files.tsv'):
    r, p = line.rstrip('\n').split('\t'); files[r].append(p)
decl_repo = json.load(open(f'{H}/decl_repo.json'))
START = re.compile(r'^(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\b', re.M)
SECTION = re.compile(r'^###(\w*)', re.M)
FQN = re.compile(r'(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)')
TESTSTEREO = re.compile(r'<<[^>]*\b(test\.\w+|PCT\.test\w*|PCT\.\w*[Tt]est\w*)\b[^>]*>>')
DECL = re.compile(r'^\s*(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?((?:\w+::)+\w+)', re.M)
NONPURE_EL = re.compile(r'^\s*(?:Database|Mapping|Runtime|SingleConnectionRuntime|RelationalDatabaseConnection|Service|Binding|SchemaSet|ExternalFormat\w*|Connection\w*)\s+((?:\w+::)+\w+)', re.M)
def is_test(head, fqn): return bool(TESTSTEREO.search(head)) or bool(re.search(r'::tests::|::tests$', fqn))
def closure(roots):
    out, todo = set(), list(roots)
    while todo:
        r = todo.pop()
        if r in out or r not in repos: continue
        out.add(r); todo.extend(repos[r]['deps'])
    return out
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
need = (set(DECL.findall(pr)) | set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj)))
need = {f for f in need if not f.startswith('meta::pure::metamodel') and not f.startswith('meta::legend::lite')}
where = collections.Counter()
for f in need:
    for r in decl_repo.get(f, ['(nowhere)']): where[r] += 1
print(f"what the product carries today (prelude + Pure.java, minus m3 and lite's own): {len(need)} FQNs; repositories holding them:")
for r, n in where.most_common(): print(f"   {n:4} {r}")
def strip(p):
    t = open(p, errors='replace').read()
    cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
    keep, els, el_bytes, test_bytes, nonpure, tests_np = [], 0, 0, 0, 0, 0
    pos = 0
    out = [t[:cuts[0]]]
    for a, b in zip(cuts, cuts[1:]):
        blk = t[a:b]
        if is_element_block(blk, START):
            fqn, head = decl_head(blk)
            if not fqn or is_test(head, fqn): test_bytes += len(blk); continue
            els += 1; el_bytes += len(blk); out.append(blk)
        else:
            sm = SECTION.match(blk)
            if sm and sm.group(1) not in ('Pure', ''):
                names = NONPURE_EL.findall(blk)
                if any(re.search(r'::tests?::|::pct::|Test', n) for n in names) or '/test' in p: tests_np += len(blk); continue
                nonpure += len(blk)
            out.append(blk)
    return ''.join(out), els, el_bytes, test_bytes, nonpure, tests_np
def world(name, roots, write=False):
    rs = closure(roots); fs = [p for r in sorted(rs) for p in files[r] if not p.endswith('/m3.pure')]
    E = EB = TB = NP = TNP = D = 0; got = set(); outfs = []
    root = f'{H}/synth/{name}'
    if write: shutil.rmtree(root, ignore_errors=True)
    for p in fs:
        text, e, eb, tb, np_, tnp = strip(p); E += e; EB += eb; TB += tb; NP += np_; TNP += tnp; D += len(text)
        got |= set(DECL.findall(text))
        if write and e:
            dst = f"{root}/{len(outfs):04d}_{os.path.basename(p)}"; os.makedirs(root, exist_ok=True); open(dst, 'w').write(text); outfs.append(dst)
    if write: open(f'{H}/synth/{name}.files', 'w').write('\n'.join(outfs) + '\n')
    cov = len(need & got)
    print(f"{name:4} {len(rs):3} repos {len(fs):5} files | kept {E:6} elements {EB/1e6:5.2f} MB (on disk after strip {D/1e6:5.2f} MB, non-Pure sections {NP/1e6:4.2f} MB) | stripped tests {TB/1e6:5.2f} MB + {TNP/1e6:4.2f} MB | covers {cov}/{len(need)}")
    return rs
core_fns = [r for r in repos if r.startswith('core_functions')]
plat = [r for r in repos if r.startswith('platform')]
print()
world('M0', plat + core_fns, write=True)
world('M1', plat + core_fns + ['core'], write=True)
world('M2', plat + core_fns + ['core', 'core_relational'], write=True)
top = [r for r, n in where.most_common() if r in repos]
world('M3', plat + core_fns + top, write=True)
print("M3 roots:", top)
