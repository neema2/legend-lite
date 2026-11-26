#!/usr/bin/env python3
"""The product's SQL changes between two dumps: sql_diff.py <before out dir> <after out dir> <report.md> [<label A> <label B>].

Each out dir holds sql.tsv (//core:sql_dump). Lists every query whose outcome differs on DuckDB (channel_a) or on its
own captured runtime (core): (i) SQL text differs, (ii) refused before and planned now, (iii) planned before and
refused now, each grouped by cause where the text tells it; then notes where the same query's change on H2 or SQLite
differs in kind; then totals."""
import collections
import csv
import difflib
import re
import sys

csv.field_size_limit(1 << 30)
PLANNED = {"SQL", "CONSTANT"}


def read(d):
    rows = {}
    with open(d + "/sql.tsv", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            r["text"] = unesc(r["text"])
            r["query"] = unesc(r["query"])
            rows[(r["set"], r["index"], r["database"] if r["set"] == "channel_a" else "own")] = r
    return rows


def unesc(s):
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), s)


ACOS = re.compile(r"\b(acos|asin)\b", re.I)


def text_cause(q, a, b):
    if ACOS.search(q):
        return "acos/asin (not equality)"
    removed = " ".join(l[2:] for l in difflib.ndiff(a.split(), b.split()) if l.startswith("- "))
    added = " ".join(l[2:] for l in difflib.ndiff(a.split(), b.split()) if l.startswith("+ "))
    if re.search(r"\bCAST\b|::\s*(DOUBLE|DECIMAL|BIGINT)|\d\.\d", added) and re.search(r"\b\d+\b", removed):
        return "a number literal read as another number type"
    if re.search(r"\d+\.0\b|\d+D\b|DECIMAL", added):
        return "a number literal read as another number type"
    return "other"


def refusal_cause(q, err):
    if ACOS.search(q) or ACOS.search(err):
        return "acos/asin (not equality)"
    m = re.search(r"(\S+) compares (.+?) with (.+?):(?=\s)", err)
    if m:
        return f"strict typing: {m.group(1)} {m.group(2)} with {m.group(3)}"
    return re.sub(r"\d+", "N", err[:80])


def main():
    a_dir, b_dir, out = sys.argv[1:4]
    la, lb = (sys.argv[4], sys.argv[5]) if len(sys.argv) > 5 else ("before", "after")
    A, B = read(a_dir), read(b_dir)
    lists = {"i": collections.defaultdict(list), "ii": collections.defaultdict(list), "iii": collections.defaultdict(list)}
    other = []
    totals = collections.Counter()
    chars = collections.Counter()
    kinds = collections.defaultdict(dict)  # (set, index) -> db -> change kind
    for k in sorted(set(A) | set(B), key=lambda k: (k[0], int(k[1]), k[2])):
        a, b = A.get(k), B.get(k)
        totals[(k[0], k[2], "queries")] += 1
        if not a or not b:
            continue
        if a["outcome"] == b["outcome"] and a["text"] == b["text"]:
            continue
        totals[(k[0], k[2], "changed")] += 1
        if a["outcome"] in PLANNED and b["outcome"] in PLANNED:
            kind = "i"
        elif b["outcome"] in PLANNED:
            kind = "ii"
        elif a["outcome"] in PLANNED:
            kind = "iii"
        else:
            kind = "refusal text"
        kinds[(k[0], k[1])][k[2]] = kind
        if k[2] not in ("DuckDB", "own"):
            continue
        q = b["query"]
        if kind == "i":
            lists["i"][text_cause(q, a["text"], b["text"])].append((k, q, a["text"], b["text"]))
            chars[(k[0], "before")] += len(a["text"])
            chars[(k[0], "after")] += len(b["text"])
        elif kind == "ii":
            lists["ii"]["acos/asin (not equality)" if ACOS.search(q) else refusal_cause(q, a["text"])].append(
                (k, q, a["text"], b["text"]))
            chars[(k[0], "after")] += len(b["text"])
        elif kind == "iii":
            lists["iii"][refusal_cause(q, b["text"])].append((k, q, a["text"], b["text"]))
            chars[(k[0], "before")] += len(a["text"])
        else:
            other.append((k, q, a["text"], b["text"]))
    L = []
    p = L.append
    p(f"# The product's SQL: {la} -> {lb}\n")
    p("## Totals\n\n| set | database | queries | changed |\n|---|---|---|---|")
    for (s, db, what), n in sorted(totals.items()):
        if what == "queries":
            p(f"| {s} | {db} | {n} | {totals[(s, db, 'changed')]} |")
    for s in ("channel_a", "core"):
        p(f"- {s} (DuckDB / own runtime): characters of the changed queries' SQL, before {chars[(s, 'before')]}, "
          f"after {chars[(s, 'after')]}")
    titles = {"i": "(i) SQL text differs", "ii": "(ii) refused before, planned now",
              "iii": "(iii) planned before, refused now"}
    for kind in ("i", "ii", "iii"):
        n = sum(len(v) for v in lists[kind].values())
        p(f"\n## {titles[kind]}: {n}\n")
        for cause in sorted(lists[kind], key=lambda c: (c.startswith("acos"), -len(lists[kind][c]))):
            items = lists[kind][cause]
            p(f"\n### {cause}: {len(items)}\n")
            for k, q, a, b in items:
                p(f"- `{k[0]}#{k[1]}` ({k[2]}) `{q}`")
                if kind == "i":
                    p(f"  - before ({len(a)} chars): `{a}`\n  - after ({len(b)} chars): `{b}`")
                elif kind == "ii":
                    p(f"  - before: {a}\n  - after ({len(b)} chars): `{b}`")
                else:
                    p(f"  - before ({len(a)} chars): `{a}`\n  - after: {b}")
    if other:
        p(f"\n## refused both times, the refusal's text changed: {len(other)}\n")
        for k, q, a, b in other:
            p(f"- `{k[0]}#{k[1]}` `{q}`\n  - before: {a}\n  - after: {b}")
    differ = [(sk, d) for sk, d in kinds.items() if len(set(d.values())) > 1]
    p(f"\n## channel_a queries whose change differs in kind across DuckDB, H2 and SQLite: {len(differ)}\n")
    for (s, i), d in differ:
        q = (B.get((s, i, "DuckDB")) or B.get((s, i, "H2")) or {}).get("query", "")
        p(f"- `{s}#{i}` `{q}`: " + ", ".join(f"{db} {kind}" for db, kind in sorted(d.items())))
    text = "\n".join(L) + "\n"
    open(out, "w", encoding="utf-8").write(text)
    print("\n".join(L[:12]))


if __name__ == "__main__":
    main()
