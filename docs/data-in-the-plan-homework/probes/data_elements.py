"""Measure ###Data elements in .pure/.legend files (and Java text blocks) under given roots.
Usage: python3 -I data_elements.py <label> <root> [<root>...]
Prints per-element: file, name, kind, bytes, csv rows; then a summary."""
import os, re, sys, json, statistics

def files(roots, exts):
    for r in roots:
        for dp, dn, fn in os.walk(r):
            dn[:] = [d for d in dn if not d.startswith('bazel-') and d not in ('node_modules', '.git')]
            for f in fn:
                if f.endswith(exts):
                    yield os.path.join(dp, f)

DATA_HDR = re.compile(r'^\s*Data\s+([A-Za-z0-9_:$]+)\s*$|^\s*Data\s+([A-Za-z0-9_:$]+)\s*\{', re.M)

def elements(text):
    """Yield (name, body) for each Data element in ###Data sections."""
    # split into sections
    parts = re.split(r'^(###\w+)\s*$', text, flags=re.M)
    # parts: [pre, hdr, body, hdr, body...]
    cur = None
    for i in range(1, len(parts), 2):
        hdr, body = parts[i], parts[i+1]
        if hdr != '###Data':
            continue
        pos = 0
        while True:
            m = re.search(r'(?m)^\s*Data\s+([A-Za-z0-9_:$]+)', body[pos:])
            if not m:
                break
            start = pos + m.start()
            j = body.find('{', pos + m.end())
            if j < 0:
                break
            depth = 0; k = j; inq = False
            while k < len(body):
                c = body[k]
                if inq:
                    if c == '\\': k += 2; continue
                    if c == "'": inq = False
                else:
                    if c == "'": inq = True
                    elif c == '{': depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0: break
                k += 1
            yield m.group(1), body[start:k+1]
            pos = k + 1

def kind(body):
    inner = body[body.find('{')+1:].lstrip()
    for kw in ('Relational', 'ExternalFormat', 'ModelStore', 'Reference', 'DataspaceDataElementReference', 'ServiceStore'):
        if inner.startswith(kw):
            return kw
    return inner.split(None, 1)[0][:30] if inner else '?'

def csv_rows(body):
    # count CSV lines inside quoted strings: approximate by '\n' escapes or real newlines within quotes
    return body.count('\\n') + body.count('\n')

label = sys.argv[1]; roots = sys.argv[2:]
rows = []
nfiles = 0
for f in files(roots, ('.pure', '.legend')):
    try:
        t = open(f, encoding='utf-8', errors='replace').read()
    except Exception:
        continue
    if '###Data' not in t:
        continue
    nfiles += 1
    for name, body in elements(t):
        rows.append((f, name, kind(body), len(body.encode('utf-8'))))
out = {'label': label, 'files_with_data': nfiles, 'elements': len(rows)}
if rows:
    sizes = sorted(r[3] for r in rows)
    out.update(total_bytes=sum(sizes), median=statistics.median(sizes), p90=sizes[int(len(sizes)*0.9)] if len(sizes)>1 else sizes[0], max=sizes[-1])
    kinds = {}
    for r in rows:
        k = kinds.setdefault(r[2], [0, 0]); k[0] += 1; k[1] += r[3]
    out['by_kind'] = kinds
    out['largest'] = [(os.path.relpath(r[0], roots[0]), r[1], r[2], r[3]) for r in sorted(rows, key=lambda r: -r[3])[:8]]
    # buckets
    b = {'<1KB':0,'1-10KB':0,'10-100KB':0,'100KB-1MB':0,'>=1MB':0}
    for s in sizes:
        if s < 1024: b['<1KB'] += 1
        elif s < 10240: b['1-10KB'] += 1
        elif s < 102400: b['10-100KB'] += 1
        elif s < 1048576: b['100KB-1MB'] += 1
        else: b['>=1MB'] += 1
    out['size_buckets'] = b
print(json.dumps(out, indent=1))
