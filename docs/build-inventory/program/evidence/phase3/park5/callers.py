import json, sys, collections, re
def load(path):
    d = json.load(open(path))
    out = []
    for e in d['recording']['events']:
        st = e['values'].get('stackTrace')
        if not st or not st['frames']:
            continue
        names = []
        for f in st['frames']:
            m = f['method']
            cls = re.sub(r'\$\$(Lambda|TypeSwitch)/0x[0-9a-f]+', r'$$\1', m['type']['name'].replace('/', '.'))
            names.append(cls.split('.')[-1] + '.' + m['name'] + ':' + str(f.get('lineNumber', '')))
        out.append(names)
    return out
for label, path in (('tip', sys.argv[1]), ('main', sys.argv[2])):
    stacks = load(path)
    callers = collections.Counter()
    hits = 0
    for names in stacks:
        idx = next((i for i, n in enumerate(names) if n.startswith('BareNames.catalog:')), None)
        if idx is None:
            continue
        hits += 1
        # the first frame above the BareNames/ResolvedNames chain
        j = idx + 1
        while j < len(names) and (names[j].startswith('BareNames.') or names[j].startswith('ResolvedNames.')):
            j += 1
        chain = ' <- '.join(names[idx + 1:j + 1])
        callers[chain] += 1
    print('== %s: %d samples under BareNames.catalog' % (label, hits))
    for c, n in callers.most_common(25):
        print('  %5d  %s' % (n, c))
    # inside catalog: where is the time spent
    inner = collections.Counter()
    for names in stacks:
        idx = next((i for i, n in enumerate(names) if n.startswith('BareNames.catalog:')), None)
        if idx is None:
            continue
        inner[' > '.join(reversed(names[max(0, idx - 3):idx]))] += 1
    print('  -- inside catalog (top frames):')
    for c, n in inner.most_common(12):
        print('  %5d  %s' % (n, c))
