"""The query shapes DataCube asks for, each as a live Proton query over the shared join, checked
against DuckDB after the feed stops.

    python proton_shapes.py --out results/x.json

The shared join writes a changelog_kv stream (deletes and moves retract correctly); positions are
a versioned_kv stream here (bookings only). Each shape runs with EMIT CHANGELOG PERIODIC 100ms;
what is recorded: accepted or the error, time to first rows, and the subscriber's final state
against DuckDB.
"""

import argparse
import json
import time

from proton_lab import ORACLE_RISK, Feed, Lab, Subscription, compare, oracle
from probe import add_probe
from scenario import CCYS, reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--out", required=True)
args = parser.parse_args()

PIVOT_P = ", ".join(f"sum(if(ccy = '{c}', pnl_usd, 0)) AS p_{c.lower()}" for c in CCYS)
PIVOT_D = ", ".join(f"SUM(CASE WHEN ccy = '{c}' THEN pnl_usd ELSE 0 END) AS p_{c.lower()}" for c in CCYS)

# name: (proton select ... without the EMIT, duckdb select, keys, compared values)
SHAPES = {
    "avg_by_desk": ("SELECT desk, avg(mv_usd) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                    "SELECT desk, AVG(mv_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "ratio_measure_by_desk": ("SELECT desk, sum(beta_delta_usd) / sum(mv_usd) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                              "SELECT desk, SUM(beta_delta_usd) / SUM(mv_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "count_distinct_by_desk": ("SELECT desk, uniq_exact(instrument) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                               "SELECT desk, COUNT(DISTINCT instrument) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "count_distinct_sql_by_desk": ("SELECT desk, count(DISTINCT instrument) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                                   "SELECT desk, COUNT(DISTINCT instrument) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "max_qty_by_book": ("SELECT book, max(qty) AS v, _tp_delta FROM position_risk_s GROUP BY book",
                        "SELECT book, MAX(qty) AS v FROM r GROUP BY book", ["book"], ["v"]),
    "min_pnl_by_sector": ("SELECT sector, min(pnl_usd) AS v, _tp_delta FROM position_risk_s GROUP BY sector",
                          "SELECT sector, MIN(pnl_usd) AS v FROM r GROUP BY sector", ["sector"], ["v"]),
    "in_filter": ("SELECT desk, trader, sum(pnl_usd) AS v, _tp_delta FROM position_risk_s WHERE desk IN ('Macro', 'Rates') GROUP BY desk, trader",
                  "SELECT desk, trader, SUM(pnl_usd) AS v FROM r WHERE desk IN ('Macro', 'Rates') GROUP BY desk, trader",
                  ["desk", "trader"], ["v"]),
    "calculated_columns": ("SELECT desk, sum(if(pnl_usd > 0, pnl_usd, 0)) AS v, sum(if(pnl_usd < 0, pnl_usd, 0)) AS w, _tp_delta "
                           "FROM position_risk_s GROUP BY desk",
                           "SELECT desk, SUM(CASE WHEN pnl_usd > 0 THEN pnl_usd ELSE 0 END) AS v, "
                           "SUM(CASE WHEN pnl_usd < 0 THEN pnl_usd ELSE 0 END) AS w FROM r GROUP BY desk", ["desk"], ["v", "w"]),
    "pivot_by_ccy": (f"SELECT desk, {PIVOT_P}, _tp_delta FROM position_risk_s GROUP BY desk",
                     f"SELECT desk, {PIVOT_D} FROM r GROUP BY desk", ["desk"], [f"p_{c.lower()}" for c in CCYS]),
    "having": ("SELECT book, sum(pnl_usd) AS v, _tp_delta FROM position_risk_s GROUP BY book HAVING sum(pnl_usd) < 0",
               "SELECT book, SUM(pnl_usd) AS v FROM r GROUP BY book HAVING SUM(pnl_usd) < 0", ["book"], ["v"]),
    "stddev_by_desk": ("SELECT desk, stddev_pop(pnl_usd) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                       "SELECT desk, STDDEV_POP(pnl_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    "median_by_desk": ("SELECT desk, median(pnl_usd) AS v, _tp_delta FROM position_risk_s GROUP BY desk",
                       "SELECT desk, MEDIAN(pnl_usd) AS v FROM r GROUP BY desk", ["desk"], ["v"]),
    # the probe's P&L is a timestamp (about 1.8e12): kept out of every top-N
    "top10_by_abs_pnl": ("SELECT instrument, sum(pnl_usd) AS v, _tp_delta FROM position_risk_s WHERE instrument != 'PROBE' "
                         "GROUP BY instrument ORDER BY abs(v) DESC LIMIT 10",
                         "SELECT instrument, SUM(pnl_usd) AS v FROM r WHERE instrument != 'PROBE' GROUP BY instrument "
                         "ORDER BY ABS(SUM(pnl_usd)) DESC LIMIT 10", ["instrument"], ["v"]),
    "stddev_as_sums_by_desk": ("SELECT desk, sqrt(sum(pnl_usd * pnl_usd) / count() - pow(sum(pnl_usd) / count(), 2)) AS v, _tp_delta "
                               "FROM position_risk_s WHERE desk != 'Probe' GROUP BY desk",
                               "SELECT desk, STDDEV_POP(pnl_usd) AS v FROM r WHERE desk != 'Probe' GROUP BY desk", ["desk"], ["v"]),
    "rank_window": ("SELECT book, v, rank() OVER (ORDER BY v DESC) AS rk, _tp_delta FROM "
                    "(SELECT book, sum(pnl_usd) AS v FROM position_risk_s GROUP BY book)",
                    "SELECT book, v, RANK() OVER (ORDER BY v DESC) AS rk FROM (SELECT book, SUM(pnl_usd) AS v FROM r GROUP BY book)",
                    ["book"], ["v", "rk"]),
    "detail_rows": ("SELECT book, instrument, mv_usd AS v, _tp_delta FROM position_risk_s WHERE desk = 'Macro'",
                    "SELECT book, instrument, mv_usd AS v FROM r WHERE desk = 'Macro'", ["book", "instrument"], ["v"]),
}

ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
lab = Lab()
lab.reset()
lab.create(target="changelog_kv")
lab.load(ref)
time.sleep(2)
feed = Feed(lab, ref, batches)
feed.start()
time.sleep(3)
subs = {}
SHAPES_EXTRA = {}
for name, (psql, _, keys, _) in SHAPES.items():
    subs[name] = Subscription(lab, f"{psql} EMIT CHANGELOG PERIODIC 100ms", keys, name=name)
    if name == "top10_by_abs_pnl":  # the same top-N as a full result every period (no changelog)
        subs["top10_periodic"] = Subscription(lab, psql.replace(", _tp_delta", "") + " EMIT PERIODIC 100ms", keys,
                                              name="top10_periodic")
        SHAPES_EXTRA["top10_periodic"] = SHAPES[name]
    time.sleep(0.3)
feed.thread.join()
time.sleep(8)

db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
result = {"seconds": args.seconds, "shapes": {}}
for name, (_, dsql, keys, values) in {**SHAPES, **SHAPES_EXTRA}.items():
    s = subs[name]
    out = {"error": s.error, "firstRowsMillis": s.first_rows_ms(), "rowsReceived": s.rows}
    if s.error is None and s.first is not None:
        exp = db.execute(dsql).fetchall()
        names = [d[0] for d in db.description]
        with lab.lock:
            got = dict(s.state)
        out.update(compare([dict(zip(names, r)) for r in exp], got, keys, values, tol=1e-9))
    result["shapes"][name] = out
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
for name, o in result["shapes"].items():
    verdict = ("ERROR " + o["error"][:150]) if o["error"] else ("no rows" if o.get("ok") is None else
               ("correct" if o["ok"] else f"WRONG {o['mismatches']}/{o['rows']} extra {o['extraRows']} {o['examples'][:1]}"))
    print(name.ljust(28), "first rows", o["firstRowsMillis"], "|", verdict)
import os  # noqa: E402
os._exit(0)
