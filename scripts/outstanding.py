#!/usr/bin/env python3
"""Generate docs/OUTSTANDING.md — the feature-keyed ledger of every
non-passing corpus test.

Primary key = WHERE THE TEST LIVES + WHAT IT'S CALLED (path segments,
defining .pure file, camelCase tokens of the test name) — the feature
being tested, stable across sequential walls. Secondary = the CURRENT
wall (pipeline stage classified from the error message) + stereotypes
(<<test.AlloyOnly>> etc.) + intent (row-asserting vs sql-only vs
tooling), parsed from the test body.
"""
import os, re, sys, subprocess, collections

REPO = "/Users/neema/legend/legend-lite"
SCOREBOARD = os.path.join(REPO, "docs/RELATIONAL_CORPUS.md")
CORPUS = ("/Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/"
          "legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/"
          "legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational")

# ---- 1. index corpus: test function name -> (file, stereotypes, body) ----
fn_re = re.compile(r"^function\s*(<<([^>]*)>>)?\s*(\{[^}]*\})?\s*([\w:$]+)::(\w+)\s*\(\s*\)\s*:", re.M)
tests = {}  # simple name -> list of dicts (name collisions across families kept)
for root, dirs, files in os.walk(CORPUS):
    for f in files:
        if not f.endswith(".pure"):
            continue
        path = os.path.join(root, f)
        try:
            src = open(path, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        rel = os.path.relpath(path, CORPUS)
        for m in fn_re.finditer(src):
            stereo = m.group(2) or ""
            name = m.group(5)
            if "test" not in stereo and not name.startswith("test"):
                continue
            # body slice for intent tags (up to 4000 chars after the match)
            body = src[m.end():m.end() + 4000]
            tests.setdefault(name, []).append({
                "file": rel, "stereo": stereo.replace("test.", ""), "body": body})

# ---- 2. parse scoreboard non-passing lines ----
line_re = re.compile(r"^- (FAIL|ERROR|SHAPE) (\w+) \[([^\]]+)\]: (.*)$")
rows = []
for line in open(SCOREBOARD, encoding="utf-8"):
    m = line_re.match(line.rstrip())
    if m:
        rows.append({"status": m.group(1), "test": m.group(2),
                     "family": m.group(3), "msg": m.group(4)[:160]})

# ---- 3. classify wall stage from message ----
def stage(msg):
    m = msg.lower()
    if "binder error" in m or "conversion error" in m or "referenced table" in m:
        return "execute(DuckDB)"
    if "tosqlstring for databasetype" in m or "db2" in m:
        return "render(DB2)"
    if m.startswith("assert"):
        return "rows-differ"
    if "sql-only" in m or "no execute(" in m or "no verifying assert" in m or "plan-assertion" in m:
        return "harness-shape"
    if "unknown function" in m or "unknown class" in m or "is not a known" in m \
       or "has no property" in m and "class 'meta::external" in m \
       or "compilelegendgrammar" in m or "alloyconfig" in m:
        return "platform-surface"
    if "no overload" in m or "expected at most one value" in m or "not a known primitive" in m \
       or "structurally matches" in m or "common supertype" in m:
        return "typer"
    if "is not mapped in mapping" in m or "mapping ~filter" in m or "union" in m and "member" in m \
       or "not found in db" in m or "already exists in the relation" in m:
        return "normalize(mapping)"
    if "resolver" in m or "navigation" in m or "graph child" in m or "serialize" in m \
       or "store resolution" in m or "join slot" in m or "demanded" in m or "graphfetch tree" in m \
       or "subtype" in m or "mileston" in m:
        return "resolve"
    if "lowering" in m or "sql type for" in m or "column specifications" in m:
        return "lower"
    return "other"

# ---- 4. intent from body ----
def intent(body):
    tags = []
    if "assertSameSQL" in body or "assertEqualsH2Compatible" in body:
        tags.append("golden-sql")
    if re.search(r"assert(Equals|SameElements|Size|JsonStringsEqual)", body):
        tags.append("row-assert")
    if "scanRelations" in body or "scanColumns" in body:
        tags.append("lineage")
    if "executionPlan" in body and "assert" in body:
        tags.append("plan-assert")
    if "->serialize(" in body or "graphFetch" in body:
        tags.append("graph")
    if "generateDatasetData" in body or "getDataGenerationInput" in body:
        tags.append("test-data-gen")
    if "validate" in body.lower() and "constraint" in body.lower():
        tags.append("constraints")
    if "toPure" in body or "pureToGrammar" in body or "toGrammar" in body:
        tags.append("printer")
    return "+".join(tags) if tags else "?"

# ---- 5. feature key: family dir + file + name tokens ----
def camel_tokens(name):
    n = re.sub(r"^test", "", name)
    return [t.lower() for t in re.findall(r"[A-Z][a-z0-9]*|[a-z0-9]+", n)][:6]

out_rows = []
for r in rows:
    cands = tests.get(r["test"], [])
    # prefer candidate whose file dir matches the family
    best = None
    for c in cands:
        d = os.path.dirname(c["file"])
        if r["family"].replace("tests/", "") in d or d in r["family"] \
           or r["family"] in ("tests/" + d, d):
            best = c
            break
    if best is None and cands:
        best = cands[0]
    fdir = r["family"]
    fname = os.path.basename(best["file"]) if best else "?"
    stereo = best["stereo"] if best else "?"
    it = intent(best["body"]) if best else "?"
    out_rows.append({**r, "file": fname, "stereo": stereo, "intent": it,
                     "stage": stage(r["msg"]),
                     "tokens": " ".join(camel_tokens(r["test"]))})

# ---- 6. emit ----
os.makedirs(os.path.join(REPO, "docs"), exist_ok=True)
out = open(os.path.join(REPO, "docs/OUTSTANDING.md"), "w", encoding="utf-8")
out.write("# OUTSTANDING — feature-keyed ledger of non-passing corpus tests\n\n")
out.write("GENERATED by tmp/outstanding.py over docs/RELATIONAL_CORPUS.md + the corpus sources.\n")
out.write("Primary key = family dir + defining file + test-name tokens (the FEATURE).\n")
out.write("`stage` = the CURRENT wall only — sequential walls hide behind it.\n\n")

def pivot(title, keyfn):
    out.write(f"## {title}\n\n")
    cnt = collections.Counter(keyfn(r) for r in out_rows)
    for k, v in cnt.most_common():
        out.write(f"- {v:4d}  {k}\n")
    out.write("\n")

pivot("Pivot: family dir x current stage",
      lambda r: f"{r['family']}  ::  {r['stage']}")
pivot("Pivot: intent x status", lambda r: f"{r['intent']}  ::  {r['status']}")
pivot("Pivot: stereotype", lambda r: r["stereo"])

out.write("## Ledger (one row per test)\n\n")
out.write("| status | family | file | test | stage | intent | stereo | wall |\n")
out.write("|---|---|---|---|---|---|---|---|\n")
for r in sorted(out_rows, key=lambda r: (r["family"], r["file"], r["test"])):
    msg = r["msg"].replace("|", "\\|")[:100]
    out.write(f"| {r['status']} | {r['family']} | {r['file']} | {r['test']} | "
              f"{r['stage']} | {r['intent']} | {r['stereo']} | {msg} |\n")
out.close()

# console summary
cnt = collections.Counter((r["stage"]) for r in out_rows)
print("BY STAGE:", dict(cnt.most_common()))
cnt2 = collections.Counter((r["intent"]) for r in out_rows if r["status"] == "ERROR")
print("ERROR BY INTENT:", dict(cnt2.most_common(12)))
print("rows:", len(out_rows), " matched-to-source:",
      sum(1 for r in out_rows if r["file"] != "?"))
