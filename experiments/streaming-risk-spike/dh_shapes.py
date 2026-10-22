"""The query shapes DataCube asks for (proton_shapes.py's list) on Deephaven, two ways, checked against
DuckDB after the feed stops:

  native  table operations through the client (what a legend-lite plan would send)
  sql     the same shape as the ANSI SQL DuckDB runs, through Deephaven's SQL
          (deephaven.experimental.sql.evaluate, server side, over the risk table bound as `r`)

    python dh_shapes.py --out results/x.json

Recorded per shape and route: accepted or the error, whether the result ticks (is refreshing,
and updated during the feed), time to first rows, and the final rows against DuckDB.
"""

import argparse
import json
import time

from dh_lab import DhLab, Feed, Subscription, start_server, stop_server
from probe import add_probe
from proton_lab import ORACLE_RISK, compare, oracle
from scenario import CCYS, reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--out", required=True)
args = parser.parse_args()

PIVOT_D = ", ".join(f"SUM(CASE WHEN ccy = '{c}' THEN pnl_usd ELSE 0 END) AS p_{c.lower()}" for c in CCYS)
# name: (duckdb select over r, keys, compared values)
SQL = {
    "avg_by_desk": ("SELECT desk, AVG(mv_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "ratio_measure_by_desk": ("SELECT desk, SUM(beta_delta_usd) / SUM(mv_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "count_distinct_by_desk": ("SELECT desk, COUNT(DISTINCT instrument) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "max_qty_by_book": ("SELECT book, MAX(qty) AS v FROM r GROUP BY book", ["book"], ["v"]),
    "min_pnl_by_sector": ("SELECT sector, MIN(pnl_usd) AS v FROM r GROUP BY sector", ["sector"], ["v"]),
    "in_filter": ("SELECT desk, trader, SUM(pnl_usd) AS v FROM r WHERE desk IN ('Macro', 'Rates') GROUP BY desk, trader",
                  ["desk", "trader"], ["v"]),
    "calculated_columns": ("SELECT desk, SUM(CASE WHEN pnl_usd > 0 THEN pnl_usd ELSE 0 END) AS v, "
                           "SUM(CASE WHEN pnl_usd < 0 THEN pnl_usd ELSE 0 END) AS w FROM r GROUP BY desk", ["desk"], ["v", "w"]),
    "pivot_by_ccy": (f"SELECT desk, {PIVOT_D} FROM r GROUP BY desk", ["desk"], [f"p_{c.lower()}" for c in CCYS]),
    "having": ("SELECT book, SUM(pnl_usd) AS v FROM r GROUP BY book HAVING SUM(pnl_usd) < 0", ["book"], ["v"]),
    "stddev_by_desk": ("SELECT desk, STDDEV_SAMP(pnl_usd) AS v FROM r WHERE desk <> 'Probe' GROUP BY desk", ["desk"], ["v"]),
    "median_by_desk": ("SELECT desk, MEDIAN(pnl_usd) AS v FROM r WHERE desk <> 'Probe' GROUP BY desk", ["desk"], ["v"]),
    "top10_by_abs_pnl": ("SELECT instrument, SUM(pnl_usd) AS v FROM r WHERE instrument <> 'PROBE' GROUP BY instrument "
                         "ORDER BY ABS(SUM(pnl_usd)) DESC LIMIT 10", ["instrument"], ["v"]),
    "rank_window": ("SELECT book, v, RANK() OVER (ORDER BY v DESC) AS rk FROM "
                    "(SELECT book, SUM(pnl_usd) AS v FROM r GROUP BY book) AS b", ["book"], ["v", "rk"]),
    "detail_rows": ("SELECT book, instrument, mv_usd AS v FROM r WHERE desk = 'Macro'", ["book", "instrument"], ["v"]),
}

ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
proc, up_s = start_server(log_path=args.out + ".server.log")
lab = DhLab(proc.pid)
r = lab.create()
lab.load(ref)

from pydeephaven import agg  # noqa: E402
from pydeephaven.updateby import cum_sum  # noqa: E402

pivot = r.update_view([f"c_{c} = ccy == `{c}` ? pnl_usd : 0.0" for c in CCYS])
NATIVE = {
    "avg_by_desk": lambda: r.agg_by([agg.avg("v = mv_usd")], by=["desk"]),
    "ratio_measure_by_desk": lambda: r.agg_by([agg.sum_(["b = beta_delta_usd", "m = mv_usd"])], by=["desk"])
    .update_view(["v = b / m"]),
    "count_distinct_by_desk": lambda: r.agg_by([agg.count_distinct("v = instrument")], by=["desk"]),
    "max_qty_by_book": lambda: r.agg_by([agg.max_("v = qty")], by=["book"]),
    "min_pnl_by_sector": lambda: r.agg_by([agg.min_("v = pnl_usd")], by=["sector"]),
    "in_filter": lambda: r.where("desk in `Macro`, `Rates`").agg_by([agg.sum_("v = pnl_usd")], by=["desk", "trader"]),
    "calculated_columns": lambda: r.update_view(["pos = pnl_usd > 0 ? pnl_usd : 0.0", "neg = pnl_usd < 0 ? pnl_usd : 0.0"])
    .agg_by([agg.sum_(["v = pos", "w = neg"])], by=["desk"]),
    "pivot_by_ccy": lambda: pivot.agg_by([agg.sum_([f"p_{c.lower()} = c_{c}" for c in CCYS])], by=["desk"]),
    "having": lambda: r.agg_by([agg.sum_("v = pnl_usd")], by=["book"]).where("v < 0"),
    "stddev_by_desk": lambda: r.where("desk != `Probe`").agg_by([agg.std("v = pnl_usd")], by=["desk"]),
    "median_by_desk": lambda: r.where("desk != `Probe`").agg_by([agg.median("v = pnl_usd")], by=["desk"]),
    "top10_by_abs_pnl": lambda: r.where("instrument != `PROBE`").agg_by([agg.sum_("v = pnl_usd")], by=["instrument"])
    .update_view(["a = abs(v)"]).sort_descending("a").head(10),
    "rank_window": lambda: r.agg_by([agg.sum_("v = pnl_usd")], by=["book"]).sort_descending("v")
    .update_view(["one = 1L"]).update_by(cum_sum("rk = one")),
    "detail_rows": lambda: r.where("desk == `Macro`").view(["book", "instrument", "v = mv_usd"]),
}

lab.s.bind_table("r", r)
tables, subs, errors = {}, {}, {}
for name, (dsql, keys, _) in SQL.items():
    for route in ("native", "sql"):
        key = f"{route}:{name}"
        t0 = time.time()
        try:
            if route == "native":
                t = NATIVE[name]()
            else:
                lab.s.run_script(f"from deephaven.experimental import sql\nq_{name} = sql.evaluate({dsql!r})")
                t = lab.s.open_table(f"q_{name}")
            tables[key] = t
            subs[key] = Subscription(lab, t, keys, name=key)
        except Exception as e:
            # the server's Python traceback: keep the line that names the cause
            cause = [ln.strip() for ln in str(e).split("\n") if ln.strip().startswith("RuntimeError:")]
            errors[key] = (cause[-1] if cause else repr(e))[:400]

feed = Feed(lab, ref, batches)
time.sleep(2)
updates_before = {k: s.updates for k, s in subs.items()}
feed.start()
feed.thread.join()
time.sleep(3)

db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
result = {"seconds": args.seconds, "shapes": {}}
for name, (dsql, keys, values) in SQL.items():
    exp = db.execute(dsql).fetchall()
    names = [d[0] for d in db.description]
    exp_rows = [dict(zip(names, row)) for row in exp]
    for route in ("native", "sql"):
        key = f"{route}:{name}"
        out = {"error": errors.get(key)}
        if key in subs:
            s = subs[key]
            snap_rows = lab.snapshot(tables[key])
            lower = [{k.lower(): v for k, v in row.items()} for row in snap_rows]
            snap = {tuple(row.get(k) for k in keys): row for row in lower}
            out.update({"refreshing": tables[key].is_refreshing, "updatesDuringFeed": s.updates - updates_before[key],
                        "firstRowsMillis": s.first_rows_ms(), "clientErrors": s.errors[:1],
                        "columns": list(snap_rows[0]) if snap_rows else None,
                        **compare(exp_rows, snap, keys, values, tol=1e-9)})
        result["shapes"][key] = out
for s in subs.values():
    s.stop()
stop_server(proc)
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
for key, o in result["shapes"].items():
    verdict = ("ERROR " + o["error"][:160]) if o["error"] else (
        ("correct" if o["ok"] else f"WRONG {o['mismatches']}/{o['rows']} extra {o['extraRows']} {o['examples'][:1]}")
        + f" | live {o['refreshing']} updates {o['updatesDuringFeed']} first {o['firstRowsMillis']} ms")
    print(key.ljust(32), verdict)
import os  # noqa: E402
os._exit(0)
