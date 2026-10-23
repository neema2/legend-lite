"""The platform function inventory: every function carrying <<PCT.function>> in legend-pure and
legend-engine, with a category from its source path. Writes pct_functions.tsv (name, category, file, repo).

Usage: pct_inventory.py <legend-pure root> <legend-engine root> <out.tsv> (run.sh passes the pinned trees)."""
import collections
import glob
import os
import re
import sys

PURE, ENGINE, OUT = sys.argv[1:4]
FN = re.compile(r'function\s+(<<[^>]*>>\s*)?(\{[^}]*\}\s*)?([\w:]+?)(?:<[^>(]*>)?\s*\(', re.S)

rows = set()
for repo, root in (('legend-pure', PURE), ('legend-engine', ENGINE)):
    for f in glob.glob(os.path.join(root, '**', '*.pure'), recursive=True):
        # the path within its tree, so the categories read the same wherever the tree is
        rel = os.path.relpath(f, root).replace('\\', '/')
        p = '/' + rel
        if '/target/' in p or '/src/test/' in p:
            continue
        txt = open(f, encoding='utf-8', errors='replace').read()
        if 'PCT.function' not in txt:
            continue
        m = re.search(r'/(essential|grammar|standard|unclassified|relation|tds)/(.*)$', p)
        mm = re.search(r'core_functions_(\w+)/(.*)$', p)
        if mm:
            cat = mm.group(1) + '/' + mm.group(2).split('/')[0 if mm.group(1) == 'relation' else 0]
            parts = mm.group(2).split('/')
            cat = mm.group(1) + ('/' + parts[0] if len(parts) > 1 else '')
            if mm.group(1) == 'relation':
                cat = 'relation'
        elif m:
            parts = m.group(2).split('/')
            cat = m.group(1) + ('/' + parts[0] if len(parts) > 1 else '')
        else:
            cat = 'other:' + '/'.join(p.split('/')[-3:-1])
        # only functions whose own stereotype block names PCT.function
        for fm in FN.finditer(txt):
            st = fm.group(1) or ''
            if 'PCT.function' not in st:
                continue
            name = fm.group(3).split('::')[-1]
            rows.add((name, cat, rel, repo))

with open(OUT, 'w', encoding='utf-8') as o:
    for r in sorted(rows):
        o.write('\t'.join(r) + '\n')
names = collections.defaultdict(set)
for n, c, f, repo in rows:
    names[c].add(n)
print('distinct PCT function names:', len({r[0] for r in rows}), ' overloads/defs:', len(rows))
for c in sorted(names):
    print('  %-34s %3d  %s' % (c, len(names[c]), ' '.join(sorted(names[c]))[:150]))
