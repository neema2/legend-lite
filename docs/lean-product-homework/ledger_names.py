#!/usr/bin/env python3
"""Every class-like name JavaEvalLedgerTest's text cites, checked against the sources (2026-11-10, P3).
Usage: python3 -I ledger_names.py core/src/test/java/com/legend/JavaEvalLedgerTest.java
A name counts as LIVE when some .java/.ts file in the tree declares a class, record, interface or enum of that name,
or a file of that name exists. For each dead name it prints the ledger lines citing it, and flags a line that does not
itself say the thing is gone (deleted / left / died / renamed / gone / evicted / moved / retired / dead / was)."""
import re, sys, pathlib, subprocess, collections
ledger = pathlib.Path(sys.argv[1]).read_text().splitlines()
root = pathlib.Path(".")
decl = re.compile(r"\b(?:class|record|interface|enum)\s+([A-Z][A-Za-z0-9]+)")
declared = set()
for f in subprocess.run(["git", "ls-files", "*.java", "*.ts", "*.kt"], capture_output=True, text=True).stdout.split():
    try:
        t = pathlib.Path(f).read_text(errors="ignore")
    except Exception:
        continue
    declared.update(decl.findall(t))
    declared.add(pathlib.Path(f).stem)
name = re.compile(r"\b([A-Z][a-z0-9]+(?:[A-Z][A-Za-z0-9]*)+)(?:\.java|\.|#|\b)")
history = re.compile(r"delet|\bleft\b|died|DIED|renam|gone|GONE|evict|moved|retir|dead|DEAD|\bwas\b|until|absorbed|became|replac|split|merged", re.I)
cites = collections.defaultdict(list)
for i, line in enumerate(ledger, 1):
    for m in name.finditer(line):
        cites[m.group(1)].append(i)
dead = {n: ls for n, ls in cites.items() if n not in declared}
print(f"{len(cites)} distinct class-like names cited; {len(dead)} not declared anywhere in the tree")
for n, ls in sorted(dead.items()):
    unflagged = [l for l in ls if not history.search(ledger[l - 1])]
    mark = "  <-- cited as if live on lines " + str(unflagged) if unflagged else ""
    print(f"  {n}: lines {ls[:12]}{' ...' if len(ls) > 12 else ''}{mark}")
