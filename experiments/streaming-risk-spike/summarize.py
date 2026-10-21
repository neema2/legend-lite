"""Prints the measurements of one or more runs side by side: python summarize.py results/*.json"""

import json
import sys


def g(d, *path):
    for p in path:
        if d is None:
            return None
        d = d.get(p) if isinstance(d, dict) else None
    return d


rows = [
    ("engine", lambda r: r["engine"] + (f" ({r['protonShape']}, {r['protonEmit']})" if r.get("protonShape") else "")),
    ("views subscribed", lambda r: len(r["views"])),
    ("price ticks/s sent", lambda r: g(r, "load", "achievedTicksPerSecond")),
    ("setup: compile or create (s)", lambda r: g(r, "setup", "compileSeconds") or g(r, "setup", "createSeconds")),
    ("setup: first complete total (s)", lambda r: g(r, "setup", "firstCompleteTotalSeconds")),
    ("tick -> total p50 / p95 / p99 (ms)", lambda r: "{} / {} / {}".format(
        *(g(r, "tickToCheckViewMs", "latency", k) for k in ("p50", "p95", "p99")))),
    ("FX tick -> all 48 books p50 / p95 (ms)", lambda r: "{} / {}".format(
        g(r, "fxFanOutMs", "allBooks", "p50"), g(r, "fxFanOutMs", "allBooks", "p95"))),
    ("by-book updates freshness p50 / p95 (ms)", lambda r: "{} / {}".format(
        g(r, "freshnessMs", "risk_by_book", "p50"), g(r, "freshnessMs", "risk_by_book", "p95"))),
    ("by-book change sets/s", lambda r: g(r, "changeSetsPerSecond", "risk_by_book")),
    ("drain after last batch (ms)", lambda r: r.get("drainMillis")),
    ("engine CPU (cores)", lambda r: g(r, "load", "engineCpuCores")),
    ("engine memory (MB)", lambda r: g(r, "load", "engineMemoryMB")),
    ("correct (subscriber, snapshot)", lambda r: "{} / {}".format(
        "yes" if all(c["mismatches"] == 0 and c["extraRows"] == 0 for c in r["correctness"]["subscriberState"].values()) else "NO",
        "yes" if all(c["mismatches"] == 0 and c["extraRows"] == 0 for c in r["correctness"]["snapshot"].values()) else "NO")),
    ("new live grouping: first rows (ms)", lambda r: f"{g(r, 'adHoc', 'firstRowsMillis')} ({g(r, 'adHoc', 'kind')})"),
]

runs = [json.load(open(p)) for p in sys.argv[1:]]
width = max(len(n) for n, _ in rows)
for name, f in rows:
    print(name.ljust(width), " | ".join(str(f(r)) for r in runs))
