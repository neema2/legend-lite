import os, re, sys, collections
roots = sys.argv[1:]
decl = collections.defaultdict(list)
fn_re = re.compile(r'\bfunction\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?([A-Za-z_][\w:]*)\s*(<[^(]*?>)?\s*\(')
def match_paren(s, i):
    depth = 0
    for j in range(i, len(s)):
        c = s[j]
        if c in '({[<':
            if c == '<' and s[j-1:j+1] == '-<': pass
            depth += 1
        elif c in ')}]>':
            if c == '>' and s[j-1] == '-':
                continue
            depth -= 1
            if depth == 0:
                return j
    return -1
def split_top(s):
    out, depth, cur = [], 0, []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == '-' and i + 1 < len(s) and s[i+1] == '>':
            cur.append('->'); i += 2; continue
        if ch in '<({[': depth += 1
        elif ch in '>)}]': depth -= 1
        if ch == ',' and depth == 0:
            out.append(''.join(cur)); cur = []
        else:
            cur.append(ch)
        i += 1
    if cur: out.append(''.join(cur))
    return [x.strip() for x in out if x.strip()]
for root in roots:
    for dp, dn, fns in os.walk(root):
        if '/test' in dp.lower() and '/tests/' in dp.lower():
            pass
        for fn in fns:
            if not fn.endswith('.pure'): continue
            path = os.path.join(dp, fn)
            try:
                txt = open(path, errors='replace').read()
            except Exception:
                continue
            # package context: crude, skip; use the declared name as written
            for m in fn_re.finditer(txt):
                name = m.group(1); tps = m.group(2) or ''
                start = m.end() - 1
                end = match_paren(txt, start)
                if end < 0: continue
                params = split_top(txt[start+1:end])
                tpset = set(x.strip() for x in tps.strip('<>').split('|')[0].split(',') if x.strip())
                kinds = []
                for p in params:
                    if ':' not in p: kinds.append('?'); continue
                    t = p.split(':', 1)[1].strip()
                    t = re.sub(r'\[[^\]]*\]\s*$', '', t).strip()
                    base = t.split('<', 1)[0].strip()
                    if t in tpset: k = 'T'
                    elif base.split('::')[-1] == 'Any': k = 'Any'
                    elif base.split('::')[-1] in ('Function', 'FunctionDefinition', 'LambdaFunction', 'ConcreteFunctionDefinition') and '{' in t: k = 'Fn'
                    elif t.startswith('{'): k = 'Fn'
                    else: k = 'other'
                    kinds.append(k)
                short = name.split('::')[-1]
                decl[(short, len(params))].append((kinds, path.split('/legend-')[-1][:90], txt[m.start():end+1][:160].replace('\n', ' ')))
hits = 0
for (short, n), ds in sorted(decl.items()):
    if len(ds) < 2: continue
    for i in range(n):
        ks = set(d[0][i] for d in ds)
        if 'Fn' in ks and ('T' in ks or 'Any' in ks):
            hits += 1
            print('%s/%d pos %d %s' % (short, n, i, sorted(ks)))
            for d in ds[:8]:
                print('     ', d[0][i], '|', d[2][:150])
print('hits', hits)
