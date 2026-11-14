"""Census of every *.emit.yaml in legend-engine: features, stores, complexity, module, and the
models' own .pure files (for later feature mining). Writes emit_models.json."""
import collections
import glob
import json
import os
import re
import sys

E = sys.argv[1]
OUT = sys.argv[2]


def parse_yaml(text):
    """Tiny YAML reader for the flat descriptor shape (no dependency on PyYAML)."""
    d = {'features': [], 'stores': [], 'tags': [], 'files': [], 'roots': []}
    key = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        m = re.match(r'^(\w+):\s*(.*)$', line)
        if m:
            key = m.group(1)
            v = m.group(2).strip().strip('"').strip("'")
            if v and not v.startswith('|'):
                d[key] = v
            continue
        m = re.match(r'^\s+-\s+(.*)$', line)
        if m and key in ('features', 'stores', 'tags'):
            d[key].append(m.group(1).strip().strip('"'))
            continue
        m = re.match(r'^\s+root:\s*(.*)$', line)
        if m:
            d['roots'].append(m.group(1).strip())
            continue
        m = re.match(r'^\s+-\s+(.*\.pure)\s*$', line)
        if m:
            d['files'].append(m.group(1).strip())
    return d


models = []
for y in glob.glob(E + '/**/*.emit.yaml', recursive=True):
    if '/target/' in y.replace('\\', '/'):
        continue
    d = parse_yaml(open(y, encoding='utf-8').read())
    rel = os.path.relpath(y, E).replace('\\', '/')
    mod = re.search(r'([^/]+)/src/test/resources/([^/]+)/', rel)
    d['path'] = rel
    d['module'] = mod.group(1) if mod else '?'
    d['suite'] = mod.group(2) if mod else '?'
    # the model's own .pure files: the sibling directory named like the yaml
    base = y[:-len('.emit.yaml')]
    pures = glob.glob(base + '/**/*.pure', recursive=True)
    d['pure_files'] = [os.path.relpath(p, E).replace('\\', '/') for p in pures]
    models.append(d)

json.dump(models, open(OUT, 'w', encoding='utf-8'), indent=1)
print('models:', len(models))
print('by suite:', dict(collections.Counter(m['module'] + '/' + m['suite'] for m in models).most_common()))
print('complexity:', dict(collections.Counter(m.get('complexity', '?') for m in models)))
print('stores:', dict(collections.Counter(s for m in models for s in m['stores'])))
feat = collections.Counter(f for m in models for f in m['features'])
print('distinct features used:', len(feat))
for f, n in sorted(feat.items()):
    print('  %4d  %s' % (n, f))
