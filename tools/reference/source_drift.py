#!/usr/bin/env python3
"""Which Pure sources differ between the reference jar (its own .pure resources, 4.138.5) and the
pinned trees our compiler loads (engine 4.145.0, pure 5.99.0)? A positional join is only valid
in a file whose text is identical on both sides; in a file that changed, line numbers shift and
two unrelated calls meet at one position. This writes, per reference source id, SAME / DIFFERS /
MISSING, for join.py --source-drift.

usage: source_drift.py <ref-resolutions.tsv> <shaded.jar> <engine-tree> <pure-tree> <out.tsv>
"""
import hashlib, os, sys, zipfile

ref, jar, engine, pure, out = sys.argv[1:6]
ids = set()
with open(ref, encoding="utf-8") as f:
    f.readline()
    for line in f:
        ids.add(line.split("\t", 1)[0])

# index every .pure under both pinned trees by its path suffix (module/relative), longest match wins
index = {}
for root in (engine, pure):
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in ("target", ".git", "node_modules")]
        for fn in fns:
            if fn.endswith(".pure"):
                p = os.path.join(dp, fn)
                rel = os.path.relpath(p, root).replace(os.sep, "/")
                # a source id is /<module>/<path-inside-the-module-resources>; the module dir is
                # .../src/main/resources/<module>/...
                if "/src/main/resources/" in rel:
                    key = "/" + rel.split("/src/main/resources/", 1)[1]
                    index.setdefault(key, p)

def sha(b):
    return hashlib.sha256(b).hexdigest()

counts = {"SAME": 0, "DIFFERS": 0, "MISSING": 0}
with zipfile.ZipFile(jar) as z, open(out, "w", encoding="utf-8") as w:
    names = set(z.namelist())
    w.write("sourceId\tstatus\n")
    for sid in sorted(ids):
        entry = sid.lstrip("/")
        ours = index.get(sid)
        if entry not in names or ours is None:
            status = "MISSING"
        else:
            status = "SAME" if sha(z.read(entry)) == sha(open(ours, "rb").read()) else "DIFFERS"
        counts[status] += 1
        w.write(sid + "\t" + status + "\n")
print(counts)
