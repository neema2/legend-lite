#!/usr/bin/env python3
"""The ledger's counts per route, arm and backend: a secondary table, generated from the ledger and diff-tested.

    summary.py LEDGER OUT
"""
import sys
from collections import Counter
from pathlib import Path

import judge_logic as logic


def main() -> int:
    rows = logic.read_ledger(Path(sys.argv[1]))
    counts = Counter((row["route"], row["arm"], row["backend"], row["road"], row["verdict"]) for row in rows)
    groups = sorted({key[:4] for key in counts})
    lines = ["\t".join(("route", "arm", "backend", "road", "rows") + logic.VERDICTS)]
    for group in groups:
        cells = [counts[(*group, verdict)] for verdict in logic.VERDICTS]
        lines.append("\t".join([*group, str(sum(cells)), *map(str, cells)]))
    Path(sys.argv[2]).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
