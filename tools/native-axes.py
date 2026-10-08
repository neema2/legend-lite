#!/usr/bin/env python3
"""NATIVE AXES — the two meanings of "native", measured (program §0, §3 D; homework §3n).

  AXIS U  how UPSTREAM declares a function: `native function` (body is Java) or a Pure body.
          An implementation detail of theirs.
  AXIS O  whether OUR platform lowers it. A semantic claim of ours — what Pure.java means.

This tool puts every Pure.java FQN on both axes and classifies the ones no lowering
registry claims. It is READ-ONLY and needs no build — except that axis O's registry
half comes from a one-shot JUnit probe whose output it reads if present:

    core/target/lowering-coverage-probe.txt      (the probe source is in homework §3n)

Without that file the tool still reports axis U and the name-based proxies, and says so.

Usage:
    tools/native-axes.py                 # full report
    tools/native-axes.py --tsv OUT.tsv   # also write the 175-style classification

Roots come from LEGEND_ENGINE_ROOT / LEGEND_PURE_ROOT (tools/oracle-roots.sh precedence).
All counts are REGEX CENSUSES: a declaration whose header spans lines in an unusual way
can be missed (homework §3n found 3 such of 42). Treat them as exact to within that.
"""
import collections
import glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
P = os.environ.get("LEGEND_PURE_ROOT", os.path.expanduser("~/legend/legend-pure"))
E = os.environ.get("LEGEND_ENGINE_ROOT", os.path.expanduser("~/legend/legend-engine"))

# the roots the prelude generator reads (PreludeGeneratorTest.generate + PLATFORM_ROOTS)
PLAT_ROOTS = [f"{P}/legend-pure-core/legend-pure-m3-core/src/main/resources/platform",
              f"{P}/legend-pure-core/legend-pure-m3-precisePrimitives/src/main/resources",
              f"{P}/legend-pure-dsl", f"{P}/legend-pure-store"]
ENG_ROOTS = [f"{E}/legend-engine-xts-relationalStore",
             f"{E}/legend-engine-core/legend-engine-core-pure",
             f"{E}/legend-engine-xts-service/legend-engine-language-pure-dsl-service-pure/src/main/resources/core_service"]

HEAD = re.compile(r'^\s*(native\s+)?function\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?'
                  r'([A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+)\s*[<(]', re.M)


def scan(roots):
    nat, bod = set(), set()
    for r in roots:
        if not os.path.isdir(r):
            sys.exit(f"missing upstream root: {r}")
        for dp, _, fs in os.walk(r):
            for f in fs:
                if not f.endswith(".pure"):
                    continue
                try:
                    t = open(os.path.join(dp, f), encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                for m in HEAD.finditer(t):
                    (nat if m.group(1) else bod).add(m.group(2))
    return nat, bod


def main():
    tsv = None
    if "--tsv" in sys.argv:
        tsv = sys.argv[sys.argv.index("--tsv") + 1]

    src = open(f"{ROOT}/core/src/main/java/com/legend/builtin/Pure.java", encoding="utf-8").read()
    ours = sorted(set(re.findall(r'native function\s+(?:<<[^>]*>>\s*)?(meta::[A-Za-z0-9_:]+)', src)))
    consts = collections.defaultdict(set)
    for c, f in re.findall(r'public static final NativeFunctionDefinition (\w+)\s*=\s*signature\(\s*'
                           r'"native function\s+(?:<<[^>]*>>\s*)?(meta::[A-Za-z0-9_:]+)', src):
        consts[f].add(c)
    lite_names = {c: n for c, n in re.findall(r'public static final String ([A-Z0-9_]+)\s*=\s*PKG \+ "([A-Za-z0-9_]+)"', src)}
    lite = [f for f in ours if f.startswith("meta::legend::lite")]
    ours_up = [f for f in ours if not f.startswith("meta::legend::lite")]

    # ---- axis U ------------------------------------------------------------
    pn, pb = scan(PLAT_ROOTS)
    en, eb = scan(ENG_ROOTS)
    up_nat, up_bod = pn | en, pb | eb
    print("==== AXIS U — how upstream declares functions, in the roots the prelude reads ====")
    print(f"  legend-pure platform roots : native {len(pn):>5}   bodied {len(pb):>6}")
    print(f"  engine roots               : native {len(en):>5}   bodied {len(eb):>6}")
    print(f"  union                      : native {len(up_nat):>5}   bodied {len(up_bod):>6}   both {len(up_nat & up_bod)}")
    print(f"\n  Pure.java: {len(ours)} FQNs = {len(ours_up)} upstream-named + {len(lite)} meta::legend::lite")
    q = collections.Counter(
        "native-only" if f in up_nat and f not in up_bod else
        "bodied-only" if f in up_bod and f not in up_nat else
        "both" if f in up_nat else "not in roots" for f in ours_up)
    for k in ("native-only", "bodied-only", "both", "not in roots"):
        print(f"    upstream {k:<13}: {q[k]:>4}")
    undeclared = sorted(up_nat - set(ours))
    print(f"\n  upstream natives NOT declared in Pure.java ('unknown function' today): {len(undeclared)}")

    # ---- axis O ------------------------------------------------------------
    probe = f"{ROOT}/core/target/lowering-coverage-probe.txt"
    unreg = None
    if os.path.exists(probe):
        unreg = sorted(l.split()[1] for l in open(probe) if l.startswith("@@U "))
        print(f"\n==== AXIS O — from the probe ({probe}) ====")
        for l in open(probe):
            if l.startswith("@@PROBE"):
                print("  " + l.strip()[8:])
    else:
        print(f"\n==== AXIS O — probe output ABSENT ({probe}); run the §3n probe for registry truth ====")

    # name-based handler census over core/main (the 80 ad-hoc sites), for whichever set we have
    target = unreg if unreg is not None else ours
    files = [f for f in glob.glob(f"{ROOT}/core/src/main/java/**/*.java", recursive=True)
             if not f.endswith("builtin/Pure.java")]
    CONST_ONLY = {"compiler/element/type/PlatformTypes.java", "builtin/SystemMetamodel.java", "builtin/Prelude.java"}
    LOW = ("lowering/", "sql/", "exec/", "setup/")
    ROOT_VERDICT = {"StatementExecutor", "AssertVerdicts", "SqlTextVerdicts", "LineageTreeVerdicts", "CsvLoad",
                    "SeedSqlForms", "PlanAllocations", "ConnectionLets", "AggAwareActivities"}
    handlers = collections.defaultdict(set)
    for f in files:
        t = open(f, encoding="utf-8").read()
        code = "\n".join(l for l in t.split("\n") if not l.strip().startswith(("//", "*", "/*")))
        lits = set(re.findall(r'"([A-Za-z0-9_]+)"', code))
        fq = set(re.findall(r'"(meta::[A-Za-z0-9_:]+)"', code))
        crefs = set(re.findall(r'Pure\.([A-Z][A-Z0-9_]+)\b', code))
        lrefs = set(re.findall(r'Lite\.([A-Z0-9_]+)\b', code))
        short = f.split("core/src/main/java/com/legend/")[1]
        for fqn in target:
            s = fqn.rsplit("::", 1)[1]
            if (s in lits or fqn in fq or (consts[fqn] & crefs)
                    or (fqn.startswith("meta::legend::lite") and any(lite_names.get(c) == s for c in lrefs))):
                handlers[fqn].add(short)

    def cls(fqn):
        hs = handlers[fqn] - CONST_ONLY
        if not hs:
            return "NONE" if not handlers[fqn] else "CONSTANT-ONLY"
        if any(h.startswith(LOW) or h.split("/")[-1].replace(".java", "") in ROOT_VERDICT for h in hs):
            return "OFF-REGISTRY IMPL"
        return "FRONT-END-ONLY"

    rows = [(cls(f), f, sorted(handlers[f] - CONST_ONLY)) for f in target]
    c = collections.Counter(r[0] for r in rows)
    label = "the registry-unclaimed set" if unreg is not None else "ALL Pure.java FQNs (no probe: proxy only)"
    print(f"\n==== handler census over {label} ({len(target)}) ====")
    for k in ("OFF-REGISTRY IMPL", "FRONT-END-ONLY", "CONSTANT-ONLY", "NONE"):
        print(f"  {c[k]:>4}  {k}")
    if unreg is not None:
        print(f"\n  implemented but invisible to any registry (must CLAIM): {c['OFF-REGISTRY IMPL']}")
        print(f"  certainly unimplemented (CONSTANT-ONLY + NONE)       : {c['CONSTANT-ONLY'] + c['NONE']}")
        print(f"  grey                                                   : {c['FRONT-END-ONLY']}")
    if tsv:
        with open(tsv, "w") as out:
            out.write("class\tfqn\thandlers\n")
            for cl, f, hs in sorted(rows):
                out.write(f"{cl}\t{f}\t{'; '.join(h.split('/')[-1].replace('.java', '') for h in hs)}\n")
        print(f"\n  wrote {tsv}")


if __name__ == "__main__":
    main()
