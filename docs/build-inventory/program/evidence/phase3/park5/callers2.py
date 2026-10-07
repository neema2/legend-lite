import json, sys, collections, re
def frames_of(path):
    d = json.load(open(path))
    for e in d['recording']['events']:
        st = e['values'].get('stackTrace')
        if not st or not st['frames']:
            continue
        names = []
        for f in st['frames']:
            m = f['method']
            cls = re.sub(r'\$\$(Lambda|TypeSwitch)/0x[0-9a-f]+', r'$$\1', m['type']['name'].replace('/', '.'))
            names.append((cls, m['name'], f.get('lineNumber', '')))
        yield names
SKIP = ('com.legend.compiler.BareNames', 'com.legend.compiler.ResolvedNames', 'java.', 'jdk.')
for label, path in (('tip', sys.argv[1]), ('main', sys.argv[2])):
    total = 0; hits = 0
    site = collections.Counter(); entry = collections.Counter()
    for names in frames_of(path):
        total += 1
        idx = next((i for i, n in enumerate(names) if n[0] == 'com.legend.compiler.BareNames' and n[1] == 'catalog'), None)
        if idx is None:
            continue
        hits += 1
        # the ResolvedNames entry used, and the first call site outside the lookup classes
        rn = next((n for n in names[idx + 1:] if n[0] == 'com.legend.compiler.ResolvedNames'), None)
        j = idx + 1
        while j < len(names) and names[j][0].startswith(SKIP):
            j += 1
        s = names[j] if j < len(names) else ('?', '?', '')
        key = '%s.%s:%s' % (s[0].split('.')[-1], s[1], s[2])
        last_rn = None
        for n in names[idx + 1:j]:
            if n[0] == 'com.legend.compiler.ResolvedNames':
                last_rn = n[1]
        entry[last_rn or ('BareNames-direct')] += 1
        site['%-14s %s' % (last_rn or '(direct)', key)] += 1
    print('== %s: %d samples total, %d under BareNames.catalog' % (label, total, hits))
    print('  by ResolvedNames entry:', dict(entry))
    for k, n in site.most_common(30):
        print('  %5d  %s' % (n, k))
