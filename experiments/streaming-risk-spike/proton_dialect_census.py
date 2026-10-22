"""How much of legend-lite's ClickHouse dialect Proton accepts as written: every identifier-like
string literal in core's ClickHouse.java checked against Proton's system.functions (names and
aliases, with its case-insensitivity flag), and against the snake_case spelling Proton uses.

    python proton_dialect_census.py --dialect ../../core/src/main/java/com/legend/sql/dialect/ClickHouse.java --out x.json
"""

import argparse
import json
import re

from proton_lab import Lab

parser = argparse.ArgumentParser()
parser.add_argument("--dialect", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

src = open(args.dialect).read()
# a literal that is a call ("toString(") or that the dialect passes as a function name ("toString")
calls = set(re.findall(r'"([A-Za-z_][A-Za-z0-9_]*)\(', src))
names = set(re.findall(r'"([a-z][A-Za-z0-9_]{2,})"', src))
candidates = sorted(calls | {n for n in names if re.search(r"[A-Z]", n) or n in calls})

lab = Lab()
rows = lab.query("SELECT name, is_aggregate, case_insensitive, alias_to FROM system.functions")
exact = {r["name"]: r for r in rows}
lower = {r["name"].lower(): r for r in rows if r["case_insensitive"]}


def snake(n):
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", n).lower()


result = {"asWritten": [], "snakeCaseOnly": [], "missing": []}
for n in candidates:
    if n in exact or n.lower() in lower:
        result["asWritten"].append(n)
    elif snake(n) in exact or snake(n) in lower:
        result["snakeCaseOnly"].append(f"{n} -> {snake(n)}")
    else:
        result["missing"].append(n)
result["counts"] = {k: len(v) for k, v in result.items()}
result["protonFunctions"] = len(rows)
with open(args.out, "w") as f:
    json.dump(result, f, indent=2)
print(json.dumps(result["counts"]))
print("snake_case only:", ", ".join(result["snakeCaseOnly"][:60]))
print("missing:", ", ".join(result["missing"][:80]))
import os  # noqa: E402
os._exit(0)
