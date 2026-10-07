import re
p = "runs/homework/phase2b/extract.py"
s = open(p).read()
a = """            rows.append((fqn, p, e['kind'], imports, blk))"""
b = """            rows.append((fqn, p, block_kind(blk), imports, blk))"""
assert s.count(a) == 1
s = s.replace(a, b, 1)
helper = '''
DOC = re.compile(r"^\\s*'{3}.*?'{3}", re.S)
LEAD = re.compile(r"^(\\s|//[^\\n]*\\n|/\\*.*?\\*/)*", re.S)
def block_kind(blk):
    """The block's own kind: an fqn may have native and bodied overloads in one file."""
    head = LEAD.sub("", DOC.sub("", blk))
    return 'native function' if head.startswith('native') else 'function'
'''
s = s.replace("IMPORT = re.compile(", helper + "IMPORT = re.compile(", 1)
open(p, "w").write(s)
