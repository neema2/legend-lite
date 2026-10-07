#!/usr/bin/env python3
"""Callers and leaf frames of named frames in a Flight Recorder recording (the compiler design rev 2, §1.2).
Usage: jfr_callers.py <path to the JDK's jfr tool> <recording.jfr> <frame suffix>..."""
import collections, re, subprocess, sys
jfr, f, frames_wanted = sys.argv[1], sys.argv[2], sys.argv[3:]
txt = subprocess.run([jfr, "print", "--events", "jdk.ExecutionSample", f], capture_output=True, text=True).stdout
stacks = []
for ev in txt.split("jdk.ExecutionSample")[1:]:
    frames = re.findall(r"^\s+([a-zA-Z_$][\w.$]*)\.([\w$<>]+)\(", ev, re.M)
    if frames: stacks.append([f"{c}.{m}" for c, m in frames])
print("samples", len(stacks))
for w in frames_wanted:
    callers = collections.Counter(); tops = collections.Counter(); incl = 0
    for names in stacks:
        idx = [i for i, x in enumerate(names) if x.endswith(w)]
        if not idx: continue
        incl += 1; i = idx[-1]
        tops[names[0]] += 1
        callers[next((x for x in names[i+1:] if x.startswith("com.legend") and not x.endswith(w)), "?")] += 1
    print(f"\n== {w}: inclusive {incl} ({100*incl/len(stacks):.1f}%)")
    print("   callers:"); [print(f"     {c:5d}  {k}") for k, c in callers.most_common(8)]
    print("   top frames:"); [print(f"     {c:5d}  {k}") for k, c in tops.most_common(6)]
