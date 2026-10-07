#!/usr/bin/env python3
"""The eager probe's failures by message shape and package (the compiler design rev 2, §1.3).
Usage: residue_shapes.py <eager-residue.txt> [<eager-corpus.txt>] ; names and numbers in messages are replaced by '_' / N
so that one shape stands for every body that fails the same way."""
import collections, re, sys
def shape(msg):
    m = re.sub(r"'[^']*'", "'_'", msg); m = re.sub(r"\b\d+\b", "N", m); return m[:110]
def agg(lines, label):
    c = collections.Counter(); pk = collections.Counter(); ex = {}
    for l in lines:
        if " :: " not in l or l.startswith("#"): continue
        body, msg = l.split(" :: ", 1); s = shape(msg); c[s] += 1; ex.setdefault(s, l[:260]); pk[body.split("::")[1] if "::" in body else "?"] += 1
    print(f"\n== {label}: {sum(c.values())} bodies, {len(c)} message shapes; top 30:")
    for s, n in c.most_common(30): print(f"  {n:4d}  {s}")
    print("  by package:", pk.most_common(12))
for path in sys.argv[1:]:
    agg(open(path).read().split("\n"), path)
