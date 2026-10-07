import sys
def split_top(s, sep=','):
    out, depth, cur = [], 0, []
    for ch in s:
        if ch in '<({[':
            depth += 1
        elif ch in '>)}]':
            depth -= 1
        if ch == sep and depth == 0:
            out.append(''.join(cur)); cur = []
        else:
            cur.append(ch)
    if cur:
        out.append(''.join(cur))
    return [x.strip() for x in out if x.strip()]

def mult_id(m):
    m = m.strip()
    if m == '*': return 'MANY'
    if '..' in m:
        lo, hi = m.split('..')
        if hi == '*':
            return 'MANY' if lo == '0' else '$%s_MANY$' % lo
        return hi if lo == hi else '$%s_%s$' % (lo, hi)
    return m  # exact number or parameter name

def type_id(t):
    t = t.strip()
    if t.startswith('{'):
        return 'Function'
    if t.startswith('('):
        return 'Relation'
    base = t.split('<', 1)[0].strip()
    cut = base.rfind('::')
    return base if cut < 0 else base[cut + 2:]

def split_type_mult(s):
    # s = Type[mult]; find the last top-level [ ... ]
    s = s.strip()
    assert s.endswith(']'), s
    depth = 0
    for i in range(len(s) - 1, -1, -1):
        ch = s[i]
        if ch in '>)}]':
            depth += 1
        elif ch in '<({[':
            depth -= 1
            if depth == 0 and ch == '[':
                return s[:i], s[i + 1:-1]
    raise ValueError(s)

def mangle(fqn, sig):
    # sig = <T|m>(p:T[m], ...):R[m]
    if sig.startswith('<'):
        depth = 0
        for i, ch in enumerate(sig):
            if ch == '<': depth += 1
            elif ch == '>':
                depth -= 1
                if depth == 0:
                    sig = sig[i + 1:]
                    break
    assert sig.startswith('('), sig
    depth = 0
    for i, ch in enumerate(sig):
        if ch in '<({[':
            depth += 1
        elif ch in '>)}]':
            depth -= 1
            if depth == 0:
                close = i
                break
    params = sig[1:close]
    ret = sig[close + 1:].lstrip(':')
    out = fqn
    ps = split_top(params)
    if not ps:
        out += '_'
    for p in ps:
        name, tm = p.split(':', 1)
        t, m = split_type_mult(tm)
        out += '_' + type_id(t) + '_' + mult_id(m) + '_'
    t, m = split_type_mult(ret)
    out += '_' + type_id(t) + '_' + mult_id(m) + '_'
    return out

ids = set()
for line in open(sys.argv[1]):
    if line.startswith('#') or line.startswith('fqn\t'):
        continue
    cols = line.rstrip('\n').split('\t')
    try:
        ids.add(mangle(cols[0], cols[1].replace('->', '\u2192')))
    except Exception as e:
        print('PARSE-FAIL', cols[0], cols[1][:80], e, file=sys.stderr)
for i in sorted(ids):
    print(i)
