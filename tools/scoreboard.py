#!/usr/bin/env python3
"""The corpus scoreboard (docs/PHASE_K_EXECUTION.md, docs/SCOREBOARD.md).

Parses engine/target/surefire-reports after a full engine-suite run:

    mvn -pl engine test -Dmaven.test.failure.ignore=true
    python3 tools/scoreboard.py            # print the report
    python3 tools/scoreboard.py --record   # append a run row to docs/SCOREBOARD.md

Buckets are keyed on error-message patterns; keep them in sync with the
bucket table in docs/SCOREBOARD.md when new categories emerge.
"""

import collections
import datetime
import glob
import os
import re
import subprocess
import sys

REPORTS = os.path.join(os.path.dirname(__file__), "..", "engine", "target", "surefire-reports")
SCOREBOARD = os.path.join(os.path.dirname(__file__), "..", "docs", "SCOREBOARD.md")

BUCKETS = [
    ("H: class sources / property nav / mappings",
     ["TypedGetAll", "unbound type variable", "has no property",
      "no compiled mapping function", "graphFetch tree"]),
    ("CORE: scalar/agg function registrations",
     ["no scalar lowering registered", "no aggregate lowering", "unknown function"]),
    ("CORE: unlowered constructs",
     ["scalar lowering not yet implemented", "lowering not yet implemented",
      "Not yet ported"]),
    ("CORE(G): overload/typing gaps",
     ["no overload of", "lambda has", "enumeration "]),
    ("FIXTURE: unknown refs (to diagnose)",
     ["is not a known class", "unknown table", "Unknown type"]),
    ("CORE(parse): query syntax gaps",
     ["trailing tokens", "expected "]),
]


def bucket_of(msg):
    for name, needles in BUCKETS:
        if any(n in msg for n in needles):
            return name
    return "OTHER"


def main():
    topline = collections.Counter()
    green_classes = 0
    buckets = collections.Counter()
    detail = collections.Counter()
    failures = collections.Counter()

    for f in sorted(glob.glob(os.path.join(REPORTS, "*.txt"))):
        text = open(f, errors="replace").read()
        m = re.search(r"Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)", text)
        if not m:
            continue
        t, fl, er, sk = map(int, m.groups())
        topline["tests"] += t
        topline["failures"] += fl
        topline["errors"] += er
        topline["skipped"] += sk
        if fl == 0 and er == 0:
            green_classes += 1
        for line in text.splitlines():
            if "Exception:" in line and not line.strip().startswith("at "):
                msg = line.split("Exception:", 1)[1].strip()
                if not msg:
                    continue
                buckets[bucket_of(msg)] += 1
                detail[msg[:70]] += 1
            if "AssertionFailedError" in line or "ComparisonFailure" in line:
                failures[line.strip()[:80]] += 1

    passes = topline["tests"] - topline["failures"] - topline["errors"] - topline["skipped"]
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True).stdout.strip()

    lines = []
    lines.append(f"### Run {datetime.date.today()} @ {commit}")
    lines.append("")
    lines.append(f"| tests | pass | failures | errors | skipped | green classes |")
    lines.append(f"|---|---|---|---|---|---|")
    lines.append(f"| {topline['tests']} | **{passes}** | {topline['failures']} "
                 f"| {topline['errors']} | {topline['skipped']} | {green_classes} |")
    lines.append("")
    lines.append("| bucket | exception lines |")
    lines.append("|---|---|")
    for name, count in buckets.most_common():
        lines.append(f"| {name} | {count} |")
    lines.append("")
    lines.append("Top detail:")
    lines.append("```")
    for msg, count in detail.most_common(25):
        lines.append(f"{count:5}  {msg}")
    lines.append("```")
    report = "\n".join(lines)
    print(report)

    if "--record" in sys.argv:
        with open(SCOREBOARD, "a") as f:
            f.write("\n" + report + "\n")
        print(f"\n[recorded to {SCOREBOARD}]")


if __name__ == "__main__":
    main()
