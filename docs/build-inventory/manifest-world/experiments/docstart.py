# Element segmentation helpers for upstream Pure files (homework scratch).
# - An element starts at its keyword at a line start, or at the doc string (three single quotes) written just above
#   it: upstream's documentation syntax. Cutting between the two leaves an orphan doc string, a parse error.
# - A keyword at a line start INSIDE a doc string or a block comment is prose, never a declaration.
# - The declared name is read after the keyword, every <<stereotype>> and every {tagged value}, however long.
import re, bisect
Q3 = "'" * 3
_DOC = re.compile("^" + Q3 + ".*?" + Q3, re.M | re.S)
_BLOCKC = re.compile(r'/\*.*?\*/', re.S)
def masked_spans(t):
    spans = sorted([(m.start(), m.end()) for m in _DOC.finditer(t)] + [(m.start(), m.end()) for m in _BLOCKC.finditer(t)])
    merged = []
    for a, b in spans:
        if merged and a < merged[-1][1]: merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
        else: merged.append((a, b))
    return merged
def element_starts(t, start_re):
    spans = masked_spans(t); heads = [a for a, b in spans]
    def inside(p):
        i = bisect.bisect_right(heads, p) - 1
        return i >= 0 and spans[i][0] < p < spans[i][1]
    out = []
    for m in start_re.finditer(t):
        if inside(m.start()): continue
        s = k = m.start()
        while k > 0 and t[k - 1] in ' \t\r\n': k -= 1
        if t.endswith(Q3, 0, k):
            o = t.rfind(Q3, 0, k - 3)
            if o >= 0: s = t.rfind('\n', 0, o) + 1
        out.append(s)
    return out
def strip_doc(blk):
    b = blk.lstrip()
    if b.startswith(Q3):
        e = b.find(Q3, 3)
        if e >= 0: return b[e + 3:].lstrip()
    return blk
def is_element_block(blk, start_re):
    return bool(start_re.match(strip_doc(blk).lstrip()))
_KW = re.compile(r'(?:native\s+)?(?:function|Class|Enum|Association|Profile|Primitive|Measure)\b')
_PATH = re.compile(r'((?:\w+::)+\w+)')
def decl_head(blk):
    """(fqn, header): header = the text up to the declared name."""
    s = strip_doc(blk).lstrip()
    m = _KW.match(s)
    if not m: return '', ''
    i, n = m.end(), len(s)
    while True:
        while i < n and s[i] in ' \t\r\n': i += 1
        if s.startswith('<<', i):
            j = s.find('>>', i + 2)
            if j < 0: return '', ''
            i = j + 2; continue
        if i < n and s[i] == '{':
            depth, q, j = 0, None, i
            while j < n:
                c = s[j]
                if q:
                    if c == '\\': j += 2; continue
                    if c == q: q = None
                elif c in "'\"": q = c
                elif c == '{': depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0: break
                j += 1
            i = j + 1; continue
        break
    m = _PATH.match(s, i)
    return (m.group(1), s[:m.end()]) if m else ('', s[:i])
