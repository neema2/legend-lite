"""W1d/FFM: read every Arrow stream CDataIpc framed with pyarrow (a standard reader) and compare its values
with DuckDB's own answer for the same SQL. Prints OK / DIFF per stream."""
import sys, math, datetime, pathlib, decimal
import pyarrow as pa, pyarrow.ipc as ipc, duckdb

out, tsv = pathlib.Path(sys.argv[1]), sys.argv[2]
con = duckdb.connect()
con.execute("SET TimeZone='UTC'")

def norm(x):
    """One comparable form: instants in UTC, maps as sorted pairs, UUIDs and ints as text where the stream sends text."""
    if isinstance(x, dict): return sorted([norm(k), norm(v)] for k, v in x.items())
    if isinstance(x, list): return [norm(v) for v in x]
    if isinstance(x, tuple): return [norm(v) for v in x]   # DuckDB gives a fixed array as a tuple, pyarrow as a list
    if isinstance(x, datetime.datetime) and x.tzinfo: return x.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    if isinstance(x, float) and math.isnan(x): return "NaN"
    if hasattr(x, "hex") and type(x).__name__ == "UUID": return str(x)
    return x

bad = 0
for line in open(tsv):
    name, sql = line.rstrip("\n").split("\t", 1)
    t = ipc.open_stream((out / f"{name}.arrows").read_bytes()).read_all()
    arrow_types = ", ".join(str(f.type) for f in t.schema)
    if name == "date_bc":   # Python's date cannot hold BC years: compare days since the epoch
        got = t.column(0).cast(pa.int32()).to_pylist()
        want = [r[0] for r in con.execute(f"SELECT (v - DATE '1970-01-01') FROM ({sql})").fetchall()]
    elif name == "timestamp_ns":
        got = t.column(0).cast(pa.int64()).to_pylist()
        want = [r[0] for r in con.execute(f"SELECT epoch_ns(v) FROM ({sql})").fetchall()]
    elif name == "interval":
        got = [None if v is None else (v.months, v.days, v.nanoseconds) for v in t.column(0).to_pylist()]
        want = [None if r[0] is None else (r[0], r[1], r[2]) for r in con.execute(
            f"SELECT datepart('month', v) + 12 * datepart('year', v), datepart('day', v), epoch_ns(v) - epoch_ns(to_months(datepart('month', v) + 12 * datepart('year', v)) + to_days(datepart('day', v))) FROM ({sql})").fetchall()]
    elif name == "many":
        got = [tuple(norm(v) for v in row.values()) for row in t.to_pylist()]
        want = [tuple(norm(v) for v in r) for r in con.execute(sql).fetchall()]
    else:
        got = [norm(v) for v in t.column(0).to_pylist()]
        want = [norm(r[0]) for r in con.execute(sql).fetchall()]
        if name in ("uhugeint", "uuid", "enum"):   # text in the stream by our decision
            want = [None if w is None else str(w) for w in want]
        if name == "blob": want = [None if w is None else bytes(w) for w in want]
    ok = got == want
    bad += not ok
    shown = got if len(str(got)) < 90 else f"{len(got)} rows"
    print(f"{'OK  ' if ok else 'DIFF'} {name:16} {arrow_types[:60]:60} {shown}")
    if not ok: print(f"      want {want if len(str(want)) < 200 else str(want)[:200]}")
print(f"\n{bad} differences")
