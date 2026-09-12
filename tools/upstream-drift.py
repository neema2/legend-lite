#!/usr/bin/env python3
"""UPSTREAM DRIFT — what a legend-engine / legend-pure bump would cost, before doing it.

Phase 0 of the bump procedure (docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md §5) is
"decide the target". This answers the two questions that decision needs:

  1. Do the ~122 upstream PATHS this repo hardcodes still resolve at the target?
     (They all did at engine 4.145.0 / pure 5.99.0 — see §4a. The check is cheap
     and the failure mode is silent, so it is worth re-running every time.)
  2. How much does each GATE's universe move? Per-universe file-set drift, plus
     the new <<test.Test>> / <<PCT.test>> functions a bump would import.

Reads the target's file tree from the GitHub trees API (one request per repo, no
clone) and the pinned side from the local checkouts named by LEGEND_ENGINE_ROOT /
LEGEND_PURE_ROOT (tools/oracle-roots.sh precedence).

Usage:
    tools/upstream-drift.py                         # target = latest release tag
    tools/upstream-drift.py 4.145.0 5.99.0          # explicit target versions
    tools/upstream-drift.py --paths-only            # just the path check
    tools/upstream-drift.py --tests                 # also count new test functions
                                                    #   (one request per new file)

Exit 1 if any hardcoded path fails to resolve at the target.
"""
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

PIN_ENGINE = os.environ.get("LEGEND_ENGINE_ROOT", os.path.expanduser("~/legend/legend-engine"))
PIN_PURE = os.environ.get("LEGEND_PURE_ROOT", os.path.expanduser("~/legend/legend-pure"))

# ---------------------------------------------------------------------------
# the upstream roots the repo derives everything else from. Mirrors of
# rcorpus/Corpus.java and SpecBodyCensusTest — kept here only so this tool can
# run without a JVM; the Java constants are the authority.
RELATIONAL = ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
              "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
              "src/main/resources/core_relational/relational")
CORE_PURE = ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/"
             "src/main/resources/core")


def get(url):
    """Fetch over curl, not urllib: a python.org macOS install ships no CA bundle
    and every https call dies on CERTIFICATE_VERIFY_FAILED. curl is already this
    repo's house HTTP client (tools/version-report.sh) and is present on all
    three CI platforms."""
    r = subprocess.run(["curl", "-sSf", "--max-time", "90",
                        "-H", "User-Agent: legend-lite-upstream-drift", url],
                       capture_output=True)
    if r.returncode != 0:
        sys.exit(f"fetch failed: {url}\n{r.stderr.decode('utf-8', 'replace').strip()}")
    return r.stdout


def latest_release(group_path, artifact):
    meta = get(f"https://repo1.maven.org/maven2/{group_path}/{artifact}/maven-metadata.xml").decode()
    return re.search(r"<release>([^<]+)</release>", meta).group(1)


def tree(repo, ref):
    d = json.loads(get(f"https://api.github.com/repos/{repo}/git/trees/{ref}?recursive=1"))
    if d.get("truncated"):
        sys.exit(f"{repo}@{ref}: tree truncated — this tool needs the whole listing")
    files = {e["path"] for e in d["tree"] if e["type"] == "blob"}
    dirs = {e["path"] for e in d["tree"] if e["type"] == "tree"}
    return files, dirs


def pin_tree(root):
    if not os.path.isdir(root):
        sys.exit(f"missing checkout: {root} (set LEGEND_ENGINE_ROOT / LEGEND_PURE_ROOT)")
    out = subprocess.run(["git", "-C", root, "ls-tree", "-r", "--name-only", "HEAD"],
                         capture_output=True, text=True, check=True)
    return set(out.stdout.splitlines())


def src(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def list_block(text, marker):
    i = text.index(marker)
    return text[i:text.index(");", i)]


def declared_paths():
    """Every upstream path the repo hardcodes: (site, repo, relpath, kind)."""
    sites = [("rcorpus Corpus.RELATIONAL", "engine", RELATIONAL, "dir"),
             ("rcorpus Corpus.CORE_PURE", "engine", CORE_PURE, "dir"),
             ("rcorpus Corpus.M2M_TESTS", "engine", CORE_PURE + "/store/m2m/tests", "dir")]

    c = src("spec/src/test/java/com/legend/rcorpus/Corpus.java")
    bases = {"ENGINE_ROOT": "", "RELATIONAL": RELATIONAL + "/", "CORE_PURE": CORE_PURE + "/"}
    for name in ("LIBRARY_FILES", "SHAPE_FILES"):
        b = list_block(c, f"public static final java.util.List<Path> {name}")
        for m in re.finditer(r'(ENGINE_ROOT|RELATIONAL|CORE_PURE)\.resolve\(\s*((?:"[^"]*"\s*\+?\s*)+)\)', b):
            lit = "".join(re.findall(r'"([^"]*)"', m.group(2)))
            sites.append((f"rcorpus Corpus.{name}", "engine", bases[m.group(1)] + lit, "file"))

    mc = src("spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java")
    for lit in re.findall(r'"([^"]*\.pure)"', list_block(mc, "ENGINE_IMPLEMENTATION_FILES = Map.of(")):
        sites.append(("MinimalCorpus.ENGINE_IMPLEMENTATION_FILES", "engine",
                      RELATIONAL + "/" + lit, "file"))

    sb = src("spec/src/test/java/com/legend/generators/SpecBodyCensusTest.java")
    for lit in re.findall(r'"([^"]+)"', list_block(sb, "PLATFORM_ROOTS = List.of(")):
        sites.append(("SpecBodyCensusTest.PLATFORM_ROOTS", "pure", lit, "dir"))

    pg = src("spec/src/test/java/com/legend/generators/PreludeGeneratorTest.java")
    gen = pg[pg.index("static String generate()"):][:4000]
    for m in re.finditer(r'engine\.resolve\(\s*((?:"[^"]*"\s*\+?\s*)+)\)', gen):
        sites.append(("PreludeGeneratorTest roots", "engine",
                      "".join(re.findall(r'"([^"]*)"', m.group(1))), "dir"))
    # the prelude's corpus root resolves to the same path as Corpus.RELATIONAL,
    # but it is a SEPARATE declaration and can go stale on its own
    m = re.search(r'Path corpus = engine\.resolve\(\s*((?:"[^"]*"\s*\+?\s*)+)\)', gen)
    if m:
        sites.append(("PreludeGeneratorTest corpus", "engine",
                      "".join(re.findall(r'"([^"]*)"', m.group(1))), "dir"))
    m = re.search(r'Path m3 = pure\.resolve\("([^"]+)"\)', pg)
    if m:
        sites.append(("PreludeGeneratorTest m3.pure", "pure", m.group(1), "file"))

    # the five ChannelB suites each resolve a platform root and a SCOPE root.
    # These were missed by the first census pass: they live in pct/, not core/,
    # and they are the sites the engine->pure relocation of left/right.pure
    # actually moves (the unclassified scope lost two files, essential gained
    # them), so they matter more than most.
    for name in ("Essential", "Standard", "Relation", "Unclassified", "Grammar"):
        p = os.path.join(ROOT, "pct/src/test/java/org/finos/legend/lite/pct/channelb",
                         f"ChannelB{name}Test.java")
        if not os.path.exists(p):
            continue
        body = open(p, encoding="utf-8").read()
        roots = {}
        for m in re.finditer(r'Path (\w+) = (pureRoot|engineRoot)\(\)\.resolve\(\s*((?:"[^"]*"\s*\+?\s*)+)\)',
                             body):
            lit = "".join(re.findall(r'"([^"]*)"', m.group(3)))
            roots[m.group(1)] = ("pure" if m.group(2) == "pureRoot" else "engine", lit)
            sites.append((f"ChannelB{name}Test",
                          "pure" if m.group(2) == "pureRoot" else "engine", lit, "dir"))
        # a scope resolved off one of those roots (modelRoot.resolve("essential"))
        for m in re.finditer(r'Path \w+ = (\w+)\.resolve\("([^"]+)"\)', body):
            if m.group(1) in roots:
                repo, base = roots[m.group(1)]
                sites.append((f"ChannelB{name}Test", repo, base + "/" + m.group(2), "dir"))

    for ledger in ("docs/version-skew-claims.tsv", "docs/refusal-allowlist.tsv"):
        p = os.path.join(ROOT, ledger)
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            if line.startswith("#") or not line.strip():
                continue
            key = line.split("\t")[0].split("#")[0].strip()
            if "/" not in key:
                continue
            sites.append((ledger, "pure" if key.startswith("legend-pure") else "engine",
                          key, "file"))
    return sites


PLATFORM_ROOTS = [p for s, r, p, k in declared_paths() if s == "SpecBodyCensusTest.PLATFORM_ROOTS"]


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    paths_only = "--paths-only" in flags
    want_tests = "--tests" in flags

    eng_v = args[0] if args else latest_release("org/finos/legend/engine",
                                                "legend-engine-language-pure-grammar")
    pur_v = args[1] if len(args) > 1 else latest_release("org/finos/legend/pure",
                                                         "legend-pure-m3-core")
    print(f"target: legend-engine {eng_v}  /  legend-pure {pur_v}")
    print(f"pinned: {PIN_ENGINE}\n        {PIN_PURE}\n")

    eng_f, eng_d = tree("finos/legend-engine", f"legend-engine-{eng_v}")
    pur_f, pur_d = tree("finos/legend-pure", f"legend-pure-{pur_v}")
    eng_p, pur_p = pin_tree(PIN_ENGINE), pin_tree(PIN_PURE)

    # ---- 1. the paths -----------------------------------------------------
    print("==== hardcoded upstream paths ====")
    rows, bad = {}, []
    for site, repo, rel, kind in declared_paths():
        f, d = (eng_f, eng_d) if repo == "engine" else (pur_f, pur_d)
        root = PIN_ENGINE if repo == "engine" else PIN_PURE
        at_pin = os.path.isfile(os.path.join(root, rel)) if kind == "file" \
            else os.path.isdir(os.path.join(root, rel))
        at_tgt = rel in f if kind == "file" else rel in d
        rows.setdefault(site, []).append(at_tgt)
        if not at_tgt:
            bad.append((site, repo, rel, kind, at_pin))
    for site, oks in rows.items():
        print(f"  {'OK  ' if all(oks) else 'BREAK'} {len(oks):>3} path(s)  {site}")
    if bad:
        print(f"\n  {len(bad)} PATH(S) DO NOT RESOLVE AT THE TARGET:")
        for site, repo, rel, kind, at_pin in bad:
            print(f"    [{site}] {repo}:{rel} ({kind}){'  — also absent at the pin' if not at_pin else ''}")
    else:
        print(f"\n  all {sum(len(v) for v in rows.values())} paths resolve at the target.")
    if paths_only:
        return 1 if bad else 0

    # ---- 2. per-universe drift --------------------------------------------
    def sel(files, pred):
        return {f for f in files if pred(f)}

    universes = [
        ("engine: all .pure (parser corpus C3/C10)", eng_p, eng_f,
         lambda f: f.endswith(".pure")),
        ("pure: all .pure (C10)", pur_p, pur_f,
         lambda f: f.endswith(".pure")),
        ("engine: relational corpus (gates 4/5)", eng_p, eng_f,
         lambda f: f.startswith(RELATIONAL + "/") and f.endswith(".pure")),
        ("pure: PLATFORM_ROOTS (spec census + prelude)", pur_p, pur_f,
         lambda f: f.endswith(".pure") and any(f.startswith(r + "/") for r in PLATFORM_ROOTS)),
        ("engine: src/test .java (inline C4/C12)", eng_p, eng_f,
         lambda f: f.endswith(".java") and "/src/test/" in f),
        ("pure: src/test .java (inline C5)", pur_p, pur_f,
         lambda f: f.endswith(".java") and "/src/test/" in f),
        ("engine: .txt resources (C11)", eng_p, eng_f,
         lambda f: f.endswith(".txt")),
    ]
    print("\n==== file-set drift, per gate universe ====")
    print(f"  {'universe':<46} {'pin':>6} {'target':>7} {'+':>6} {'-':>6}")
    detail = {}
    for label, pin, tgt, pred in universes:
        a, b = sel(pin, pred), sel(tgt, pred)
        detail[label] = (sorted(b - a), sorted(a - b))
        print(f"  {label:<46} {len(a):>6} {len(b):>7} {len(b - a):>+6} {-len(a - b):>+6}")

    for label in ("engine: relational corpus (gates 4/5)",
                  "pure: PLATFORM_ROOTS (spec census + prelude)"):
        added, removed = detail[label]
        # strip the longest shared directory prefix: these paths run 200+ chars
        # and the interesting part is the tail
        pre = os.path.commonpath(added + removed) + "/" if (added or removed) else ""
        print(f"\n---- {label} ----" + (f"\n     under {pre}" if pre else ""))
        for f in added:
            print("  + " + f[len(pre):])
        for f in removed:
            print("  - " + f[len(pre):])

    # ---- 3. the new test functions ---------------------------------------
    if want_tests:
        print("\n==== new test functions in the added files ====")
        for label, repo, ref in (("engine: relational corpus (gates 4/5)", "legend-engine",
                                  f"legend-engine-{eng_v}"),
                                 ("pure: PLATFORM_ROOTS (spec census + prelude)", "legend-pure",
                                  f"legend-pure-{pur_v}")):
            totals = {}
            for f in detail[label][0]:
                body = get(f"https://raw.githubusercontent.com/finos/{repo}/{ref}/{f}").decode(
                    "utf-8", "replace")
                for st in ("test.Test", "paramTest.Test", "PCT.test", "PCT.function"):
                    totals[st] = totals.get(st, 0) + body.count(f"<<{st}>>") \
                        + (body.count(f"<<{st},") if st.startswith("PCT") else 0)
            print(f"  {label}: " + ", ".join(f"{k}={v}" for k, v in totals.items() if v))

    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
