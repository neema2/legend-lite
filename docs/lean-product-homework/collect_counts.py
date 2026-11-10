#!/usr/bin/env python3
"""Sums the throwaway counter's lines (P3COUNT <key> <n> [where...]) over every test.outputs/p3count.txt under the
given bazel-testlogs directories, per target and in total. Usage: python3 -I collect_counts.py bazel-testlogs/core ..."""
import sys, pathlib, re, collections
tot = collections.Counter(); per = collections.defaultdict(collections.Counter); where = collections.defaultdict(set)
for root in sys.argv[1:]:
    for f in pathlib.Path(root).rglob("p3count.txt"):
        target = str(f.parent.parent.relative_to(pathlib.Path(root).parent))
        for line in f.read_text().splitlines():
            m = re.match(r"P3COUNT (\S+) (\d+) \[(.*)\]$", line)
            if not m: continue
            k, n = m.group(1), int(m.group(2))
            tot[k] += n; per[k][target] += n
            for w in m.group(3).split(", "):
                if w: where[k].add(w)
for k in sorted(tot):
    print(f"{k}\t{tot[k]}")
    for t, n in per[k].most_common(6): print(f"      {t}: {n}")
    if where[k]: print("      e.g. " + ", ".join(sorted(where[k])[:6]))
