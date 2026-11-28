#!/usr/bin/env python3
"""Summarize a measurement: report.py <out dir> (reads bodies.tsv, results.tsv, census.tsv, probes.tsv,
helpers.tsv there; writes summary.md there and prints it)."""
import collections
import csv
import os
import re
import sys

csv.field_size_limit(1 << 30)
DBS = ["DuckDB", "H2", "SQLite"]
JUDGES = ["HOST", "DATABASE"]
VERDICT = {"PASS", "FAIL"}


def read(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def unesc(s):
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), s)


PAIR_RE = re.compile(r"compares (.+?) with (.+?):(?=\s)")
RULES = [
    ("strict typing: a compile refusal", lambda d, db: PAIR_RE.search(d)),
    ("the database judge's UNJUDGED forms", lambda d, db: d.startswith("UNJUDGED")),
    ("the mixed-days refusal", lambda d, db: "both days and date-times" in d),
    ("the program road does not plan the body (an element's message in forAll, ...)",
     lambda d, db: "does not plan this body" in d),
    ("a mixed-list syntax error (H2)", lambda d, db: db == "H2" and "Syntax error in SQL" in d),
    ("H2's missing list encodings", lambda d, db: db == "H2" and re.search(
        r"LIST_|list encoding|unnest placement|MAP_FROM_ENTRIES|STRING_SPLIT|not comparable", d)),
    ("DuckDB list shapes (a scalar against a list, list lambdas)", lambda d, db: db == "DuckDB" and re.search(
        r"Unimplemented type for cast|can't be cast to the dest|subqueries in lambda|More than one row returned", d)),
    ("SQLite list and map forms", lambda d, db: db == "SQLite" and re.search(
        r"lists or maps of values|malformed JSON|unnest in a select list", d)),
    ("empties: == over an empty side answers nothing", lambda d, db: "must be one value, got 0" in d),
    ("hour-precision date-time literals unreadable", lambda d, db: re.search(
        r"invalid timestamp field format|Cannot parse \"TIMESTAMP\"|date field value out of range", d)),
    ("`is` not a product function", lambda d, db: re.search(r"\bis\b.*(not .*function|no .*native|can't find)", d)),
    ("a rewrite artifact (parse error)", lambda d, db: "ParseException" in d),
]
FEATURES = [
    ("Variants", r"toVariant|fromJson|Variant|->to\(@"),
    ("grids", r"#TDS|->project\(|->select\(|->extend\(|Relation|->groupBy\("),
    ("instances and keys", r"\^[\w:]+\(|\.all\(\)|getAll"),
    ("dates and written precision", r"%\d"),
    ("number types", r"\d\.\d+[dDfF]?\b|\d[dD]\b|Decimal|Float"),
    ("empties", r"\[\]"),
    ("an enumeration against a String", r"\w+::\w+\.\w+"),
]


def cause(w, r, db, body):
    """The group of a body's difference between its two forms."""
    for o in (r, w):
        if o["outcome"] not in VERDICT and o["outcome"] != "SAME":
            d = o["outcome"] + " " + o["detail"]
            for name, test in RULES:
                if test(d, db):
                    return name, d
            exc = re.match(r"(\w+): (.{0,70})", o["detail"])
            key = (exc.group(1) + ": " + re.sub(r"\d+", "N", exc.group(2))) if exc else o["detail"][:70]
            return "other: " + o["outcome"] + " " + key, d
    for name, rx in FEATURES:
        if re.search(rx, body):
            return "verdict moved: " + name, w["outcome"] + "->" + r["outcome"]
    return "verdict moved: other", w["outcome"] + "->" + r["outcome"]


def main():
    out = sys.argv[1]
    bodies = {b["id"]: b for b in read(os.path.join(out, "..", "em", "bodies.tsv"))} \
        if not os.path.exists(os.path.join(out, "bodies.tsv")) else {b["id"]: b for b in read(os.path.join(out, "bodies.tsv"))}
    res = collections.defaultdict(dict)
    for row in read(os.path.join(out, "results.tsv")):
        res[row["id"]][(row["database"], row["form"], row["judge"])] = row
    lines = []
    p = lines.append
    ran = [i for i in res]
    by_src = collections.Counter(("PCT " + i.split(":")[0]) if i.split(":")[0] in ("pure", "engine") else "core tests"
                                 for i in ran)
    status = collections.Counter(b["status"] for b in bodies.values())
    p(f"# Equality measurement\n\nbodies collected: {len(bodies)}; run: {len(ran)} ({dict(by_src)})")
    p("left out: " + ", ".join(f"{k} {v}" for k, v in status.most_common() if k.startswith("excluded")))
    p(f"unchanged by the rewrite (only assert/assertFalse... already plain): {status['unchanged']}")

    def outcome(i, db, form, j):
        o = res[i].get((db, form, j))
        if o and o["outcome"] == "SAME":
            return res[i][(db, "written", j)]
        return o

    # 4a
    p("\n## 4a. bodies whose two forms differ (any judge), per database")
    groups = collections.defaultdict(lambda: collections.defaultdict(list))
    differ_any = set()
    for db in DBS:
        n = 0
        for i in ran:
            for j in JUDGES:
                w, r = outcome(i, db, "written", j), outcome(i, db, "rewritten", j)
                if w and r and w["outcome"] != r["outcome"]:
                    g, d = cause(w, r, db, unesc(bodies[i]["as_written"]))
                    groups[g][db].append((i, d))
                    n += 1
                    differ_any.add(i)
                    break
        p(f"- {db}: {n}")
    p(f"- on at least one database: {len(differ_any)} of {len(ran)}")
    p("\n| group | DuckDB | H2 | SQLite | examples |\n|---|---|---|---|---|")
    for g in sorted(groups, key=lambda g: -sum(len(v) for v in groups[g].values())):
        ex = []
        for db in DBS:
            for i, d in groups[g][db]:
                if i not in [e[0] for e in ex] and len(ex) < 2:
                    ex.append((i, d))
        p(f"| {g} | " + " | ".join(str(len(groups[g][db])) for db in DBS) + " | "
          + "; ".join(f"`{i}` {unesc(bodies[i]['as_written'])[:90]!r} ({d[:90]})" for i, d in ex).replace("|", "\\|")
          + " |")

    # 4b
    p("\n## 4b. judge disagreements (HOST vs DATABASE), per database and form")
    p("| database | form | any outcome differs | of which verdict-vs-verdict (PASS/FAIL) | UNJUDGED on DATABASE |")
    p("|---|---|---|---|---|")
    disagree_ex = collections.defaultdict(list)
    for db in DBS:
        for form in ("written", "rewritten"):
            anyd = vv = unj = 0
            for i in ran:
                h, d = outcome(i, db, form, "HOST"), outcome(i, db, form, "DATABASE")
                if not h or not d or h["outcome"] == d["outcome"]:
                    continue
                anyd += 1
                if h["outcome"] in VERDICT and d["outcome"] in VERDICT:
                    vv += 1
                    disagree_ex[(db, form)].append(i)
                if d["outcome"] == "UNJUDGED":
                    unj += 1
            p(f"| {db} | {form} | {anyd} | {vv} | {unj} |")
    for (db, form), ids in disagree_ex.items():
        p(f"- verdict disagreements {db} {form}: " + ", ".join(ids[:8]) + (" ..." if len(ids) > 8 else ""))

    # 4c
    p("\n## 4c. compile refusals 'compares X with Y' (distinct bodies, HOST judge, any database)")
    pairs = {"written": collections.Counter(), "rewritten": collections.Counter()}
    newly = collections.Counter()
    for i in ran:
        for form in pairs:
            seen = set()
            for db in DBS:
                o = outcome(i, db, form, "HOST")
                m = o and PAIR_RE.search(o["detail"])
                if m:
                    seen.add((m.group(1), m.group(2)))
            for s in seen:
                pairs[form][s] += 1
        w = {outcome(i, db, "written", "HOST")["outcome"] for db in DBS if outcome(i, db, "written", "HOST")}
        rr = [outcome(i, db, "rewritten", "HOST") for db in DBS]
        if w & VERDICT and any(o and PAIR_RE.search(o["detail"]) for o in rr):
            m = next(PAIR_RE.search(o["detail"]) for o in rr if o and PAIR_RE.search(o["detail"]))
            newly[(m.group(1), m.group(2))] += 1
    for form in pairs:
        p(f"- {form}: {sum(pairs[form].values())} body-pairs; " + ", ".join(
            f"{a} with {b} {n}" for (a, b), n in pairs[form].most_common()))
    p(f"- answered as written, a compile error once rewritten: {sum(newly.values())}; " + ", ".join(
        f"{a} with {b} {n}" for (a, b), n in newly.most_common()))

    # 4d
    p("\n## 4d. censuses")
    hp = os.path.join(out, "helpers.txt")
    if os.path.exists(hp):
        p(open(hp, encoding="utf-8").read().rstrip())
    cen = read(os.path.join(out, "census.tsv"))
    rt = re.compile(r"^(Number|Date|Any|[A-Z])$|::Any$|::Number$|::Date$")
    hits = collections.defaultdict(set)
    calls = collections.defaultdict(set)
    for c in cen:
        if c["call"] == "(untyped)":
            continue
        src = "PCT" if c["id"].split(":")[0] in ("pure", "engine") else "core"
        if bodies[c["id"]]["status"] == "unchanged" and c["form"] == "rewritten":
            continue
        calls[(src, c["form"])].add(c["id"])
        sides = [s.strip() for s in c["types"].split("|")]
        for s in sides:
            m = rt.search(s)
            if m:
                kind = s.split("::")[-1] if len(s.split("::")[-1]) > 1 else "a type variable"
                hits[(src, c["form"], kind)].add(c["id"])
    p("\nbodies with a comparison whose side is typed only at run time (statically Number, Date, Any or a type variable):")
    for k in sorted(hits):
        p(f"- {k[0]} {k[1]}: {k[2]} {len(hits[k])} (of {len(calls[(k[0], k[1])])} bodies with a typed comparison)"
          + "; e.g. " + ", ".join(sorted(hits[k])[:3]))

    # 4e
    pp = os.path.join(out, "probes.tsv")
    if os.path.exists(pp):
        p("\n## 4e. the product against itself\n\n| expression | DuckDB | H2 | SQLite |\n|---|---|---|---|")
        probes = collections.defaultdict(dict)
        for r in read(pp):
            a = r["answer"]
            m = re.search(r'"rows":\[(.*?)\]\}?$', a)
            v = (m.group(1) if m else a)[:80] if r["outcome"] == "ANSWER" else r["outcome"] + " " + a[:80]
            probes[r["expression"]][r["database"]] = v
        for e, d in probes.items():
            p(f"| `{e}` | " + " | ".join(d.get(db, "").replace("|", "\\|") for db in DBS) + " |")
    text = "\n".join(lines) + "\n"
    open(os.path.join(out, "summary.md"), "w", encoding="utf-8").write(text)
    print(text)


if __name__ == "__main__":
    main()
