#!/usr/bin/env python3
"""The writer guard's scan (WriterCarriageGuardTest.VOCABULARY and .stripped, copied verbatim) run over a directory.
Usage: python3 -I writer_guard_on_exec.py core/src/main/java/com/legend/exec [more dirs or files]
Prints, per file, each spelling and its count, and per hit its line (the line of the stripped source)."""
import re, sys, pathlib, collections
VOCAB = re.compile(
    r"\.(subtract|multiply|divide|remainder|pow|negate|signum|abs|compareTo|setScale|stripTrailingZeros"
    r"|toBigInteger|toBigIntegerExact|bitLength|doubleValue|longValue|intValue|floatValue|scale"
    r"|getHour|getMinute|getSecond|getNano|getYear|getMonthValue|getDayOfMonth|isBefore|isAfter|plusDays"
    r"|minusDays|atLeast|toInstantFloor|presentedAt|trimmed)\("
    r"|\b(Math)\.\w+\("
    r"|\bnew (BigDecimal|BigInteger)\("
    r"|\b(BigDecimal|Double|Float|Long|Integer)\.(valueOf|parseDouble|parseInt|parseLong|isFinite|isNaN"
    r"|isInfinite)\(")
def stripped(src):
    # the guard strips comments then strings; line structure kept here so hits can be located (block comments
    # replaced by their newlines)
    src = re.sub(r"(?s)/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"), src)
    src = re.sub(r"//.*", "", src)
    return re.sub(r'"(?:\\.|[^"\\])*"', '""', src)
def spelling(m):
    if m.group(1): return m.group(1)
    if m.group(2): return "Math."
    if m.group(3): return "new " + m.group(3)
    return m.group(4) + "." + m.group(5)
files = []
for a in sys.argv[1:]:
    p = pathlib.Path(a)
    files += sorted(p.rglob("*.java")) if p.is_dir() else [p]
total = 0
for f in files:
    s = stripped(f.read_text())
    hits = collections.defaultdict(list)
    for m in VOCAB.finditer(s):
        hits[spelling(m)].append(s.count("\n", 0, m.start()) + 1)
    if hits:
        n = sum(len(v) for v in hits.values()); total += n
        print(f"{f.name}: {n}")
        for k, v in sorted(hits.items()):
            print(f"    {k} x{len(v)}  lines {v}")
print("TOTAL", total, "in", len(files), "files")
