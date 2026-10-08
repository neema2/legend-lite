# THE BASELINE (the compiler plan's W1.0b, amended by D25: the stress corpus and the eager probe are the measure, nothing
# new is built to measure): one receipt, in markdown, of the numbers every later compiler push must move.
#   bazel run //tools/metrics:baseline -- [--out FILE] [--skip-latency] [--probe FILE] [--wasm FILE]
# Run it alone on a quiet machine (it times things; it is never a test); the receipt records the load averages at its
# start, and whether the machine was otherwise quiet is the operator's statement in the GATES entry. Paths are from
# where it is run. The sections:
#   1. product lines: Java under `*/src/main` per top-level directory, with a total (rule 0b.17's number), and per
#      core package; the other source files there (.pure resources, .ts, .py, .bzl) in their own table;
#   2. the whole-world compile: //spec:eager_corpus_compile's output, read from bazel-bin (build it first);
#   3. compile-only latency per query, no database (//core:compile_latency, run from the runfiles): the stress
#      corpus's service tests and the DataCube-shaped set wasm/corpus/queries.tsv, p50/p95/p99 per stage, the last
#      pass (the model's demand caches filled), the first pass's wall clock and its first case for the cold number;
#   4. the reference lane's buckets, read from the committed golden (never re-run for metrics);
#   5. the corpus rosters' sizes; 6. the planner module's bytes, read from bazel-bin (build //wasm:planner first).
import argparse, collections, os, pathlib, re, subprocess, sys, tempfile, time
from python.runfiles import runfiles

JAVA = ".java"
OTHER = (".pure", ".ts", ".tsx", ".py", ".bzl")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency"); ap.add_argument("--golden"); ap.add_argument("--rosters", nargs="*", default=[])
    ap.add_argument("--model"); ap.add_argument("--queries"); ap.add_argument("--runtime", default="trades::RT")
    ap.add_argument("--probe"); ap.add_argument("--wasm"); ap.add_argument("--out"); ap.add_argument("--skip-latency", action="store_true")
    a = ap.parse_args()
    r = runfiles.Create()
    loc = lambda p: r.Rlocation(p) if p else None
    workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    cwd = os.environ.get("BUILD_WORKING_DIRECTORY", ".")
    out = ["# Baseline (W1.0b), " + time.strftime("%Y-%m-%d %H:%M %Z") + "\n",
           "Produced by `bazel run //tools/metrics:baseline` (tools/metrics/baseline.py). " + conditions() + " The latencies are the\n"
           "last pass over each set (the model's demand caches filled); the phase shares are the Flight Recorder profiles in\n"
           "`docs/build-inventory/program/evidence/compiler/`.\n"]
    if workspace:
        out.append(product_lines(workspace))
    out.append(probe(os.path.join(cwd, a.probe) if a.probe else (workspace and os.path.join(workspace, "bazel-bin", "spec", "eager_corpus_compile", "eager-corpus.txt"))))
    if not a.skip_latency and a.latency:
        tmp = tempfile.mkdtemp(prefix="baseline-")
        env = {**os.environ, **r.EnvVars()}
        out.append(latency("3a", loc(a.latency), ["--corpus", "stress"], tmp, env, "the stress corpus's service tests (compile only, no database)"))
        if a.model and a.queries:
            n = sum(1 for l in open(loc(a.queries), encoding="utf-8") if l.strip() and not l.startswith("#") and "\t" in l)
            out.append(latency("3b", loc(a.latency), ["--model", loc(a.model), "--queries", loc(a.queries), "--runtime", a.runtime], tmp, env,
                               f"the DataCube-shaped set (wasm/corpus/queries.tsv, {n} queries)"))
        print("per-query timings (not part of the receipt): " + tmp, file=sys.stderr)
    if a.golden:
        out.append(golden(loc(a.golden)))
    if a.rosters:
        out.append(rosters([loc(p) for p in a.rosters]))
    out.append(wasm(os.path.join(cwd, a.wasm) if a.wasm else (workspace and os.path.join(workspace, "bazel-bin", "wasm", "planner", "classes.wasm"))))
    text = "\n".join(out)
    sys.stdout.write(text)
    if a.out:
        pathlib.Path(cwd, a.out).write_text(text, encoding="utf-8"); print("\nwritten " + a.out)

def conditions():
    try:
        l1, l5, l15 = os.getloadavg()
        return f"Load averages at its start: {l1:.1f}, {l5:.1f}, {l15:.1f} (1, 5, 15 minutes)."
    except (OSError, AttributeError):
        return "Load averages at its start: not available on this platform."

def count_lines(p):
    with open(p, "rb") as f:
        return f.read().count(b"\n")

def product_lines(ws):
    java_top = collections.Counter(); other_top = collections.Counter(); per_pkg = collections.Counter(); files = 0
    for top in sorted(os.listdir(ws)):
        src = pathlib.Path(ws, top, "src", "main")
        if not src.is_dir() or top.startswith("."):
            continue
        for p in src.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix == JAVA:
                n = count_lines(p); java_top[top] += n; files += 1
                if top == "core":
                    parts = p.relative_to(src).parts   # <root>/com/legend/<pkg>/... (roots: java, duckdb, ...)
                    if len(parts) > 3 and parts[1:3] == ("com", "legend"):
                        pkg = parts[3] if len(parts) > 4 else "(com.legend itself)"
                        per_pkg[pkg if parts[0] == "java" else pkg + " (" + parts[0] + " root)"] += n
                    else:
                        per_pkg["(" + parts[0] + ")"] += n
            elif p.suffix in OTHER:
                other_top[top] += count_lines(p)
    s = ["## 1. Product lines (Java under `*/src/main`, " + str(files) + " files; rule 0b.17's number)\n", "| directory | Java lines |", "|---|---|"]
    s += [f"| {k} | {v:,} |" for k, v in java_top.most_common()]
    s += [f"| **total** | **{sum(java_top.values()):,}** |", "", "| core package (`com/legend/<package>`) | Java lines |", "|---|---|"]
    s += [f"| {k} | {v:,} |" for k, v in per_pkg.most_common(24)]
    s += ["", "Other source files under `*/src/main` (" + ", ".join(OTHER) + "; resources such as the prelude):", "", "| directory | lines |", "|---|---|"]
    s += [f"| {k} | {v:,} |" for k, v in other_top.most_common()]
    return "\n".join(s) + "\n"

def probe(path):
    s = ["## 2. The whole-world compile (`//spec:eager_corpus_compile`: every body of core_relational's world typed)\n"]
    if not path or not os.path.exists(path):
        return "\n".join(s + ["(not built: `bazel build //spec:eager_corpus_compile`, then run this again)\n"])
    s.append("The counts are exact. The milliseconds are those recorded when this output was produced: its action ran then, on this\n"
             "desk or elsewhere (the disk and remote caches are shared across worktrees and with CI), beside whatever else was\n"
             "building; an alone measurement is a Flight Recorder run of the probe (the evidence folder's profiles).\n")
    for l in open(path, encoding="utf-8").read().split("\n")[:8]:
        m = re.search(r"bodies=(\d+) failed=(\d+) build=(\d+)ms typeAll=(\d+)ms", l)
        if m:
            s.append(f"| bodies | failed | build ms | typing ms |\n|---|---|---|---|\n| {int(m[1]):,} | {int(m[2]):,} | {m[3]} | {m[4]} |\n")
        elif l.startswith("# by reason") or l.startswith("# families"):
            s.append(l[2:] + "\n")
    return "\n".join(s)

def latency(num, binary, args, tmp, env, title):
    t0 = time.time()
    p = subprocess.run([binary] + args + ["--out", tmp], capture_output=True, text=True, encoding="utf-8", env=env)
    lines = [l for l in p.stdout.split("\n") if l.startswith("[latency]")]
    s = [f"## {num}. Compile-only latency: " + title + "\n"]
    if p.returncode != 0:
        s.append("FAILED (exit " + str(p.returncode) + "):\n```\n" + (p.stderr or p.stdout)[-2000:] + "\n```\n"); return "\n".join(s)
    stages = []
    for l in lines:
        body = l[len("[latency] "):]
        if body.startswith("stage="):
            stages.append(dict(kv.split("=", 1) for kv in body.split()))
        elif body.startswith(("model:", "pass=", "cold first case:", "set=")):
            s.append(body + "  ")
    if stages:
        s.append("\n| stage | p50 ms | p95 ms | p99 ms | max ms | mean ms | sum ms |\n|---|---|---|---|---|---|---|")
        for d in stages:
            s.append(f"| {d['stage']} | {d['p50']} | {d['p95']} | {d['p99']} | {d['max']} | {d['mean']} | {d['sum']} |")
    notes = [l[len("[latency] "):] for l in lines if l.startswith(("[latency] fail ", "[latency] skip "))]
    if notes:
        s.append("\nNot planned (compile-only), by reason:\n"); s += ["- " + f for f in notes[:15]]
    s.append(f"\n(wall clock of the tool, the model build and both passes: {time.time() - t0:.0f} s)\n")
    return "\n".join(s)

def golden(path):
    txt = open(path, encoding="utf-8").read().split("\n"); s = ["## 4. The reference lane (the committed golden `core_relational.txt`; never re-run for metrics)\n"]
    sec = None
    for l in txt:
        if l.startswith("== "):
            sec = l[3:]
            if sec in ("coverage", "buckets"):
                s.append(f"\n**{sec}**\n\n| key | count |\n|---|---|")
            elif sec.startswith("dropped"):
                break
        elif sec in ("coverage", "buckets") and "\t" in l:
            k, v = l.split("\t", 1); s.append(f"| {k} | {int(v):,} |")
    return "\n".join(s) + "\n"

def rosters(paths):
    s = ["## 5. The corpus rosters (lines)\n", "| roster | lines |", "|---|---|"]
    for p in paths:
        n = sum(1 for l in open(p, encoding="utf-8") if l.strip() and not l.startswith("#")); s.append(f"| {os.path.basename(p)} | {n:,} |")
    return "\n".join(s) + "\n"

def wasm(path):
    s = ["## 6. The planner module (`//wasm:planner`)\n"]
    if not path or not os.path.exists(path):
        return "\n".join(s + ["(not built: `bazel build //wasm:planner`, then run this again)\n"])
    return "\n".join(s + ["| file | bytes |", "|---|---|", f"| {os.path.basename(path)} | {os.path.getsize(path):,} |"]) + "\n"

if __name__ == "__main__":
    main()
