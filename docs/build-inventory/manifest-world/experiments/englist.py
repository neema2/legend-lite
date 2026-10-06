# Every engine-side name we carry (outside legend-pure platform* + engine core_functions_*), with why it is there.
import json, re, sys, glob, os, collections
H, B, ET = sys.argv[1:4]
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0 = g['EL'], g['M0']
need = sorted(json.load(open(f'{H}/eng_need.json')))
E = {}
for e in EL:
    if e['repo'] not in M0 and not e['test'] and e['fqn'] in need: E.setdefault(e['fqn'], e)
pj = open(f'{B}/core/src/main/java/com/legend/builtin/Pure.java').read()
lowered = set(re.findall(r'signature\("native function (?:<<[^>]*>>\s*)?((?:\w+::)+\w+)\(', pj))
javas = {p: open(p, errors='replace').read() for p in glob.glob(f'{B}/core/src/main/java/**/*.java', recursive=True) if not p.endswith('/Pure.java')}
rel_root = os.path.join(ET, 'legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation')
corpus = {p: open(p, errors='replace').read() for p in glob.glob(rel_root + '/**/*.pure', recursive=True) if re.search(r'/tests?/', p)}
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure', errors='replace').read()
sys.path.insert(0, H); from docstart import element_starts, is_element_block, strip_doc, decl_head
START = g['START']; FQN = g['FQN']
cuts = sorted(set(element_starts(pr, START) + [m.start() for m in re.finditer(r'^###', pr, re.M)] + [len(pr)]))
pels = collections.defaultdict(str)
for a, b in zip(cuts, cuts[1:]):
    blk = pr[a:b]
    if is_element_block(blk, START):
        f0, _ = decl_head(blk)
        if f0: pels[f0] += blk
rows = []
for f in need:
    e = E.get(f); s = f.split('::')[-1]
    jf = sorted({os.path.basename(p)[:-5] for p, t in javas.items() if f in t})
    srx = re.compile(r'(?<![\w])' + re.escape(s) + r'(?!\w)')
    parents = [p for p, t in pels.items() if p != f and srx.search(t)]
    seedp = [p for p in parents if p in lowered or any(p in t for t in javas.values())]
    c_full = sum(f in t for t in corpus.values()); c_short = sum(bool(srx.search(t)) for t in corpus.values())
    why = 'lowered' if f in lowered else ('java' if jf else ('closure' if parents else 'untraced'))
    rows.append(dict(fqn=f, kind=e['kind'] if e else '?', repo=e['repo'] if e else '?', file=e['rel'] if e else '?',
                     why=why, java=','.join(jf), parent=(seedp or parents or [''])[0], corpus_full=c_full, corpus_short=c_short))
with open(f'{H}/englist.tsv', 'w') as o:
    o.write('\t'.join(rows[0].keys()) + '\n')
    for r in rows: o.write('\t'.join(str(v) for v in r.values()) + '\n')
print(collections.Counter(r['why'] for r in rows))
jc = collections.Counter(j for r in rows for j in r['java'].split(',') if j)
print("our Java files naming them (full name):", jc.most_common(30))
