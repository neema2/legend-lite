# Phase 2b: every upstream function/native declaration in upstream core (platform* + core_functions_*, m3.pure
# included) at a name the catalog implements, with its file's imports. Output: TSV fqn, file, kind, imports, text
# (base64 for the last two).
import sys, os, re, base64
H, B, OUT = sys.argv[1:4]
sys.path.insert(0, H); from docstart import element_starts, is_element_block, decl_head
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0].replace("if r == 'platform' and p.endswith('/m3.pure'): continue", "pass")
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0, START, SECTION, is_test = g['EL'], g['M0'], g['START'], g['SECTION'], g['is_test']
catalog = set()
for l in open(f'{B}/core/src/main/resources/com/legend/builtin/native-claims.tsv'):
    if l.strip() and not l.startswith('#') and not l.startswith('fqn\t'):
        catalog.add(l.split('\t')[0])

DOC = re.compile(r"^\s*'{3}.*?'{3}", re.S)
LEAD = re.compile(r"^(\s|//[^\n]*\n|/\*.*?\*/)*", re.S)
def block_kind(blk):
    """The block's own kind: an fqn may have native and bodied overloads in one file."""
    head = LEAD.sub("", DOC.sub("", blk))
    return 'native function' if head.startswith('native') else 'function'
IMPORT = re.compile(r'^\s*import\s+[^;]+;', re.M)
rows, seen_files = [], {}
for e in EL:
    if e['repo'] not in M0 or e['test'] or e['fqn'] not in catalog or 'function' not in e['kind']:
        continue
    p = e['file']
    if p not in seen_files:
        t = open(p, errors='replace').read()
        cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
        blocks = []
        for a, b in zip(cuts, cuts[1:]):
            blk = t[a:b]
            if is_element_block(blk, START):
                fqn, head = decl_head(blk)
                if fqn: blocks.append((fqn, blk))
        seen_files[p] = ("\n".join(IMPORT.findall(t)), blocks)
    imports, blocks = seen_files[p]
    for fqn, blk in blocks:
        if fqn == e['fqn']:
            rows.append((fqn, p, block_kind(blk), imports, blk))
# one row per distinct block (an fqn's overloads are separate blocks)
uniq = list(dict.fromkeys(rows))
with open(OUT, 'w') as f:
    for fqn, p, kind, imports, blk in uniq:
        f.write('\t'.join([fqn, p, kind, base64.b64encode(imports.encode()).decode(), base64.b64encode(blk.encode()).decode()]) + '\n')
print(f"catalog fqns {len(catalog)}; upstream core declaration blocks at them: {len(uniq)} "
      f"({sum(1 for r in uniq if r[2] == 'native function')} native, {sum(1 for r in uniq if r[2] != 'native function')} with a body)")
