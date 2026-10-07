#!/usr/bin/env python3
"""Flight Recorder samples of a legend-lite compile, by phase and by frame (the compiler design of 2026-10-07, §0).
Usage: jfr_phases.py <path to the JDK's jfr tool> <recording.jfr>... ; prints markdown. A sample is attributed to the
first phase found on its stack, top-down; a frame's inclusive count is the number of samples whose stack holds it."""
import collections, re, subprocess, sys
PH = [("type", r"compiler\.spec\.(Typer|SpecCompiler|Overloads|InferenceKernel|[A-Za-z]+Checker|CallShapes|TdsDesugars|StaticFold|LiteralUnroll|DeferredArgs|Env|Bindings|SignatureApart|FunctionMatch|SourceSubst|CallNodes|NumberKinds)"),
      ("inline", r"UserCallInliner|StatementInline"), ("lower", r"com\.legend\.lowering\."), ("store-resolve", r"com\.legend\.resolver\."),
      ("names", r"compiler\.(NameResolver|BareNames|ResolvedNames|CoreImports)"), ("parse", r"com\.legend\.(parser|lexer)\."), ("normalize", r"com\.legend\.normalizer\."),
      ("model", r"compiler\.element\.|compiler\.(KnowledgeLayer|ModelBuilder)|builtin\.(Pure|Prelude|SystemMetamodel)"), ("probe/io", r"rcorpus\.|java\.io\.|java\.nio\.|java\.util\.zip")]
jfr, files = sys.argv[1], sys.argv[2:]
phase = collections.Counter(); top = collections.Counter(); incl = collections.Counter(); n = 0
for f in files:
    txt = subprocess.run([jfr, "print", "--events", "jdk.ExecutionSample", f], capture_output=True, text=True).stdout
    for ev in txt.split("jdk.ExecutionSample")[1:]:
        frames = re.findall(r"^\s+([a-zA-Z_$][\w.$]*)\.([\w$<>]+)\(", ev, re.M)
        if not frames: continue
        n += 1; names = [f"{c}.{m}" for c, m in frames]; stack = "\n".join(names)
        top[names[0]] += 1
        phase[next((p for p, rx in PH if re.search(rx, stack)), "other")] += 1
        for x in dict.fromkeys(x for x in names if x.startswith("com.legend")): incl[x] += 1
print(f"samples: {n} over {len(files)} recording(s)\n\n| phase | samples | share |\n|---|---|---|")
for p, c in phase.most_common(): print(f"| {p} | {c} | {100*c/n:.1f}% |")
print("\n| hottest frame at the top of the stack | samples |\n|---|---|")
for f, c in top.most_common(12): print(f"| `{f}` | {c} |")
print("\n| hottest legend frame, inclusive | samples | share |\n|---|---|---|")
for f, c in incl.most_common(40): print(f"| `{f}` | {c} | {100*c/n:.1f}% |")
