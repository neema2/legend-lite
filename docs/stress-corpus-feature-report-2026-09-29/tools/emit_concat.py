"""Concatenate every EMIT model's .pure files into one text (one parse), recording which line
range belongs to which model.

Usage: emit_concat.py <emit_models.json> <legend-engine root> <out.pure> [skip-substring...]
Files whose path contains a skip substring are left out (persistence needs a grammar the runner lacks).
"""
import json
import sys

MODELS, ENGINE, out = sys.argv[1:4]
skip = sys.argv[4:]
E = ENGINE.rstrip('/\\') + '/'
ms = json.load(open(MODELS, encoding='utf-8'))
text = []
ranges = []
line = 1
seen = set()
for m in ms:
    name = m['suite'] + '__' + m['path'].split('/')[-1][:-len('.emit.yaml')]
    for p in sorted(m['pure_files']):
        if p in seen or any(s in p for s in skip):
            continue
        seen.add(p)
        src = open(E + p, encoding='utf-8').read().replace('\r\n', '\n')
        if not src.lstrip().startswith('###'):
            src = '###Pure\n' + src
        n = src.count('\n') + 1
        ranges.append((line, line + n, name, p))
        text.append(src)
        line += n + 1
open(out, 'w', encoding='utf-8', newline='\n').write('\n\n'.join(text))
json.dump(ranges, open(out + '.ranges.json', 'w', encoding='utf-8'))
print('files', len(ranges), 'lines', line)
