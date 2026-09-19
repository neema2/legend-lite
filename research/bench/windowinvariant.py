#!/usr/bin/env python3
"""The invariant horizontal scrolling depends on.

Scrolling the column axis re-runs the cell query with a different pinned
IN list. That is only safe if a cell's value does not depend on which
window it was fetched in.

Claim: cell values are window-invariant, because sum(x) FILTER (WHERE
pk = k) reads only rows with pk = k, so pre-filtering the source to a
window containing k cannot change it. Row MEMBERSHIP and row TOTALS are
not invariant (pre-filtering drops groups), which is why those come from
a separate unpivoted query.

This checks the claim against a full-width pivot, including the case
where a group's keys fall partly outside the window.
"""
import duckdb

con = duckdb.connect()
con.execute("SET threads=1")

# Deliberately ragged: some groups have keys only in low windows, some
# only in high, some spanning. This is the shape that breaks naive
# assumptions.
con.execute("""
    CREATE TABLE t AS
    SELECT * FROM (VALUES
        ('sparse-low',   1, 10.0), ('sparse-low',   2,  5.0),
        ('sparse-high', 18, 70.0), ('sparse-high', 19,  1.0),
        ('spanning',     1,  3.0), ('spanning',    10, 40.0),
        ('spanning',    19,  9.0),
        ('dense',        1,  1.0), ('dense',        5,  2.0),
        ('dense',       10,  4.0), ('dense',       15,  8.0),
        ('dense',       19, 16.0)
    ) v(grp, pk, amt)
""")

ALL_KEYS = list(range(1, 21))
WINDOWS = [
    ("W1 [1-5]",    list(range(1, 6))),
    ("W2 [6-10]",   list(range(6, 11))),
    ("W3 [11-15]",  list(range(11, 16))),
    ("W4 [16-20]",  list(range(16, 21))),
    ("overlap [4-12]", list(range(4, 13))),
]


def pivot(keys, prefilter=False):
    """Return {(grp, pk): value} for a windowed pivot.

    prefilter=True reproduces what legend-lite's Pivots.lower emits when
    values are pinned: the source is pre-filtered to the pinned keys, to
    match legend-engine semantics. prefilter=False is DuckDB's raw form,
    which keeps groups with no matching rows.
    """
    kl = ",".join(str(k) for k in keys)
    src = f"(SELECT * FROM t WHERE pk IN ({kl}))" if prefilter else "t"
    rows = con.execute(
        f"PIVOT {src} ON pk IN ({kl}) USING sum(amt) GROUP BY grp"
    ).fetchall()
    cols = [d[0] for d in con.description]
    out = {}
    for r in rows:
        grp = r[cols.index("grp")]
        for i, c in enumerate(cols):
            if c == "grp":
                continue
            out[(grp, int(c))] = r[i]
    return out


truth = pivot(ALL_KEYS)
print("full-width pivot: %d (grp, pk) cells\n" % len(truth))

ok = True
for name, keys in WINDOWS:
    got = pivot(keys, prefilter=True)
    mismatches = []
    for (grp, pk), v in got.items():
        t = truth.get((grp, pk))
        if t != v:
            mismatches.append(f"{grp}/{pk}: window={v} full={t}")
    present = sorted({g for (g, _) in got})
    status = "OK " if not mismatches else "FAIL"
    if mismatches:
        ok = False
    print(f"{status} {name:>16}  cells={len(got):>3}"
          f"  groups={len(present)} {present}")
    for m in mismatches:
        print(f"       mismatch: {m}")

print()
print("CELL VALUES window-invariant:" , "YES" if ok else "NO")

# The counter-claim: row membership is NOT invariant under the
# pre-filtered form legend-lite actually emits.
print()
full_groups = sorted({g for (g, _) in truth})
for name, keys in WINDOWS:
    raw = sorted({g for (g, _) in pivot(keys, prefilter=False)})
    pre = sorted({g for (g, _) in pivot(keys, prefilter=True)})
    print(f"{name:>16}  raw groups={len(raw)}  "
          f"legend-lite (pre-filtered) groups={len(pre)} {pre}")
print(f"\n{'full width':>16}  groups={len(full_groups)} {full_groups}")
print("\nRow membership under legend-lite's emission is NOT invariant:"
      "\ngroups whose keys all fall outside the window vanish, which is"
      "\nwhy the row axis must come from a separate unpivoted query.")
