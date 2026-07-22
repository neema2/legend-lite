#!/usr/bin/env python3
"""Wall-depth history: how many DISTINCT error messages each corpus test
has shown across the committed history of docs/RELATIONAL_CORPUS.md.
Median depth is the honest estimate multiplier (message churn inflates
the tail — Binder errors embed volatile SQL text)."""
import subprocess, re, collections, sys
REPO = "/Users/neema/legend/legend-lite"
shas = subprocess.run(["git","log","--reverse","--format=%H","--","docs/RELATIONAL_CORPUS.md"],
    capture_output=True, text=True, cwd=REPO).stdout.split()
picks = shas[::3] + [shas[-1]]
line_re = re.compile(r"^- (FAIL|ERROR|SHAPE) (\w+) \[([^\]]+)\]: (.*)$")
hist = collections.defaultdict(list)
for sha in picks:
    txt = subprocess.run(["git","show",f"{sha}:docs/RELATIONAL_CORPUS.md"],
        capture_output=True, text=True, cwd=REPO).stdout
    for line in txt.splitlines():
        m = line_re.match(line)
        if m:
            key = (m.group(2), m.group(3)); msg = m.group(4)[:80]
            if not hist[key] or hist[key][-1] != msg:
                hist[key].append(msg)
with open(REPO + "/docs/WALL_DEPTH.txt","w") as f:
    for (t,fam),msgs in sorted(hist.items(), key=lambda kv:-len(kv[1])):
        f.write(f"{len(msgs)}\t{t}\t{fam}\n")
depth = collections.Counter(len(m) for m in hist.values())
n = sum(depth.values())
print("tests ever non-passing:", n, " median depth:",
      sorted(d for d,c in depth.items() for _ in range(c))[n//2])
