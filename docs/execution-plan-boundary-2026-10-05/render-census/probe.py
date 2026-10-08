"""The render census probe, applied by matching method SIGNATURES (not a patch's context lines), so it applies to every
stage of E unchanged: `python3 -I probe.py apply` adds RenderCensus.java and wraps each outermost render entry;
`python3 -I probe.py remove` undoes it. Run from the repository root.

Each wrapped entry keeps its body as a private method and records its result through RenderCensus.record; the census
counts only the outermost render on a thread, so a dialect's call up to its super is counted once.
"""
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent
D = pathlib.Path("core/src/main/java/com/legend/sql/dialect")
CENSUS = D / "RenderCensus.java"

# (file, the entry's signature as written, kind, the private method its body becomes, the parameter)
ENTRIES = [
    ("AnsiSqlRenderer.java", "    public String render(SqlQuery query) {", "query", "censusQuery0", "query"),
    ("AnsiSqlRenderer.java", "    public String render(com.legend.sql.SqlDdl ddl) {", "ddl", "censusDdl0", "ddl"),
    ("AnsiSqlRenderer.java", "    public String render(com.legend.sql.SqlDml dml) {", "dml", "censusDml0", "dml"),
    ("AnsiSqlRenderer.java", "    public RenderedStatement renderStatement(SqlQuery query) {", "statement",
     "censusStatement0", "query"),
    ("Postgres.java", "    public String render(com.legend.sql.SqlDdl ddl) {", "ddl", "censusPgDdl0", "ddl"),
    ("EngineStyleH2.java", "    public String render(SqlQuery query) {", "query", "censusEngineQuery0", "query"),
]
MARK = "    // census probe: wrapped\n"


def wrapper(sig, kind, private, param):
    returns = sig.split("(", 1)[0].split()[-2]   # String, or RenderedStatement
    record = "record" if returns == "String" else "recordStatement"
    return (MARK + sig + "\n        return RenderCensus." + record + "(\"" + kind + "\", this, () -> " + private + "("
            + param + "));\n    }\n\n    private " + returns + " " + private + "(" + sig.split("(", 1)[1])


def apply():
    # every check first, then every write: a tree the probe does not fit is left untouched
    texts = {}
    for f, sig, kind, private, param in ENTRIES:
        t = texts.setdefault(f, (D / f).read_text())
        if MARK in t or CENSUS.exists():
            sys.exit("probe: already applied (%s)" % f)
        if t.count(sig) != 1:
            sys.exit("probe: %s has %d of '%s'" % (f, t.count(sig), sig.strip()))
    for f, sig, kind, private, param in ENTRIES:
        texts[f] = texts[f].replace(sig, wrapper(sig, kind, private, param), 1)
    CENSUS.write_text((HERE / "RenderCensus.java.txt").read_text())
    for f, t in texts.items():
        (D / f).write_text(t)


def remove():
    if CENSUS.exists():
        CENSUS.unlink()
    for f, sig, kind, private, param in ENTRIES:
        p = D / f
        t = p.read_text()
        w = wrapper(sig, kind, private, param)
        if w in t:
            p.write_text(t.replace(w, sig, 1))


if __name__ == "__main__":
    {"apply": apply, "remove": remove}[sys.argv[1]]()
