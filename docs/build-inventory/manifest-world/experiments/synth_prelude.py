# Experiment 5 setup: a prelude.pure = today's built-in m3 metamodel section (+ lite's GrammarInfoStub)
# + upstream core (platform* + core_functions_*, whole, tests stripped) + one closure from closure.json.
# Each upstream file is its own ###Pure section (its imports stay its own); legend-pure's files first, as today.
import sys, re, os, json, collections
H, B, key, out = sys.argv[1:5]
sys.path.insert(0, H); from docstart import element_starts, is_element_block, decl_head
src = open(f'{H}/enginepat.py').read().split("in_m0 = ")[0]
g = {'__name__': 'x', 'sys': sys}; sys.argv = [sys.argv[0], H, B]; exec(compile(src, 'enginepat', 'exec'), g)
EL, M0, START, SECTION, is_test, repos = g['EL'], g['M0'], g['START'], g['SECTION'], g['is_test'], g['repos']
_rep = json.load(open(f'{H}/closure.json'))
def _repo_closure(roots):
    out, todo = set(), list(roots)
    while todo:
        r = todo.pop()
        if r in out or r not in repos: continue
        out.add(r); todo.extend(repos[r]['deps'])
    return out
sel = set()
for k in key.split(' + '):
    if k == 'core-only': continue
    if k.startswith('repos:'):
        rs = _repo_closure(k[6:].split(','))
        sel |= {e['fqn'] for e in EL if not e['test'] and e['repo'] in rs}
    else: sel |= set(_rep[k])
repo_of = {}
for e in EL: repo_of.setdefault(e['file'], e['repo'])
need = {e['file'] for e in EL if not e['test'] and (e['repo'] in M0 or e['fqn'] in sel)}
order = sorted(need, key=lambda p: (repos[repo_of[p]]['side'] != 'pure', repo_of[p], p))
EXCLUDE_RX = re.compile(os.environ['EXCLUDE_RX']) if os.environ.get('EXCLUDE_RX') else None
EXCLUDED = []
EXCLUDE_FQNS = set(open(os.environ['EXCLUDE_FQNS']).read().split()) if os.environ.get('EXCLUDE_FQNS') else set()
def cut(t, keep):
    cuts = sorted(set(element_starts(t, START) + [m.start() for m in SECTION.finditer(t)] + [len(t)]))
    parts = [t[:cuts[0]]]; pure = True; n = 0
    for a, b in zip(cuts, cuts[1:]):
        blk = t[a:b]
        sm = SECTION.match(blk)
        if sm:
            pure = sm.group(1) in ('Pure', '')
            if pure: parts.append(blk)
            continue
        if not pure: continue
        if is_element_block(blk, START):
            fqn, head = decl_head(blk)
            if fqn and keep(fqn, head) and fqn not in EXCLUDE_FQNS and not (EXCLUDE_RX and EXCLUDE_RX.search(blk)): parts.append(blk); n += 1
            elif fqn and EXCLUDE_RX and EXCLUDE_RX.search(blk): EXCLUDED.append(fqn)
        else:
            parts.append(blk)
    return ''.join(parts), n
pr = open(f'{B}/core/src/main/resources/com/legend/builtin/prelude.pure').read()
core_fqns = {e['fqn'] for e in EL if not e['test'] and e['repo'] in M0}
m3, n3 = cut(pr, lambda f, h: (f.startswith('meta::pure::metamodel') and f not in core_fqns and f not in sel) or f == 'meta::pure::tools::GrammarInfoStub')
system_fqns = set(open(f'{H}/system_fqns.txt').read().split())
claimed = {l.split('\t')[0] for l in open(f'{B}/core/src/main/resources/com/legend/builtin/native-claims.tsv') if l.strip() and not l.startswith('#') and not l.startswith('fqn\t')}
import re as _re
forms = set(_re.findall(r'"((?:meta|core)::(?:\w+::)*\w+)"', open(f'{B}/core/src/main/java/com/legend/platform/CoreFn.java').read()))
def _footer(title):
    out, on = set(), False
    for line in pr.splitlines():
        if line.startswith('// ') and line[3:4].isupper(): on = line.startswith('// ' + title)
        elif on and line.startswith('//   meta::'): out.add(line[5:].split()[0])
    return out
footer_owned = _footer('PLATFORM-OWNED NAMES') | _footer('SYSTEM-OWNED FUNCTIONS')
form_names = set(_re.findall(r'^\s+[A-Z_0-9]+\("([A-Za-z_]+)"', open(f'{B}/core/src/main/java/com/legend/platform/CoreFn.java').read(), _re.M))
def owned(f, h):
    # what the platform owns: the system metamodel's own versions (any kind), and every function we implement (by name, as today's generator drops them)
    return f in system_fqns or ((f in claimed or f in forms or f in footer_owned or f.split('::')[-1] in form_names) and h.lstrip().split(None, 1)[0] in ('function', 'native'))
chunks = [m3]; total = n3; nfiles = 0; dropped = 0
for p in order:
    core = repo_of[p] in M0
    txt, n = cut(open(p, errors='replace').read(), lambda f, h: not is_test(h, f) and (core or f in sel) and not owned(f, h))
    if n: chunks.append('\n###Pure\n' + txt.lstrip('﻿')); total += n; nfiles += 1
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, 'w').write(''.join(chunks))
print('excluded:', sorted(set(EXCLUDED))[:30], len(set(EXCLUDED)))
print(f"{key}: {total} elements ({n3} built-in m3 + lite stub), {nfiles} upstream files, {os.path.getsize(out)/1e6:.2f} MB -> {out}")
