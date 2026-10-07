#!/usr/bin/env python3
"""The stress profile's unattributed third, string building, hashing and line-start callers (the compiler design rev 2, §1.2).
Usage: jfr_other.py <path to the JDK's jfr tool> <recording.jfr>."""
import collections, re, subprocess, sys
jfr, f = sys.argv[1], sys.argv[2]
PH = [("type", r"compiler\.spec\.(Typer|SpecCompiler|Overloads|InferenceKernel|[A-Za-z]+Checker|CallShapes|TdsDesugars|StaticFold|LiteralUnroll|DeferredArgs|Env|Bindings|SignatureApart|FunctionMatch|SourceSubst|CallNodes|NumberKinds)"),
      ("inline", r"UserCallInliner|StatementInline"), ("lower", r"com\.legend\.lowering\."), ("store-resolve", r"com\.legend\.resolver\."),
      ("names", r"compiler\.(NameResolver|BareNames|ResolvedNames|CoreImports)"), ("parse", r"com\.legend\.(parser|lexer)\."), ("normalize", r"com\.legend\.normalizer\."),
      ("model", r"compiler\.element\.|compiler\.(KnowledgeLayer|ModelBuilder)|builtin\.(Pure|Prelude|SystemMetamodel)"), ("probe/io", r"rcorpus\.|java\.io\.|java\.nio\.|java\.util\.zip")]
txt = subprocess.run([jfr, "print", "--events", "jdk.ExecutionSample", f], capture_output=True, text=True).stdout
other_first = collections.Counter(); other_pkg = collections.Counter(); sb_callers = collections.Counter(); hm_callers = collections.Counter(); ls_callers = collections.Counter(); n=0
for ev in txt.split("jdk.ExecutionSample")[1:]:
    frames = re.findall(r"^\s+([a-zA-Z_$][\w.$]*)\.([\w$<>]+)\(", ev, re.M)
    if not frames: continue
    n+=1; names=[f"{c}.{m}" for c,m in frames]; stack="\n".join(names)
    ph = next((p for p, rx in PH if re.search(rx, stack)), "other")
    legend = [x for x in names if x.startswith("com.legend")]
    if ph == "other":
        other_first[legend[0] if legend else names[-1]] += 1
        other_pkg[(legend[0].rsplit(".",2)[0] if legend else "no-legend:"+names[0].rsplit(".",1)[0])] += 1
    if names[0].startswith("java.lang.AbstractStringBuilder") or names[0].startswith("java.lang.String"):
        sb_callers[next((x for x in names if x.startswith("com.legend")), names[-1])] += 1
    if names[0].startswith("java.util.HashMap"):
        hm_callers[next((x for x in names if x.startswith("com.legend")), names[-1])] += 1
    if "com.legend.lexer.TokenStream.lineStarts" in names:
        ls_callers[next((x for x in names[names.index("com.legend.lexer.TokenStream.lineStarts")+1:] if x.startswith("com.legend") and "TokenStream" not in x), "?")] += 1
print("samples", n)
print("\n'other' by first legend frame:"); [print(f"  {c:5d}  {k}") for k,c in other_first.most_common(25)]
print("\n'other' by legend package:"); [print(f"  {c:5d}  {k}") for k,c in other_pkg.most_common(15)]
print("\nString/StringBuilder work, by nearest legend caller:"); [print(f"  {c:5d}  {k}") for k,c in sb_callers.most_common(15)]
print("\nHashMap work, by nearest legend caller:"); [print(f"  {c:5d}  {k}") for k,c in hm_callers.most_common(15)]
print("\nTokenStream.lineStarts, by caller:"); [print(f"  {c:5d}  {k}") for k,c in ls_callers.most_common(10)]
