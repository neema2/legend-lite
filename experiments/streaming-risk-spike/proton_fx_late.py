"""FX applied after aggregation instead of per position: a rewrite a planner can make because USD
values are linear in the rate (the sum over positions of value x rate is the sum over currencies of
rate x the currency's summed value). An FX tick then changes one row per (group, currency), not a
fifth of all positions.

    python proton_fx_late.py --scale 5 [--shape nested|leaf] [--seconds 30] --out x.json

The shared view joins positions, instruments, books, prices and closes into a changelog_kv stream
of local-currency values. Then, by --shape:
  nested  each grouping is one streaming query: aggregated by (keys, ccy) over that stream, joined
          to fx, summed by keys
  leaf    two more views: the local values summed by (desk, trader, book, sector, ccy) into a
          changelog_kv leaf cube, and that cube joined to fx into a USD leaf cube; each grouping
          sums the USD leaf cube
Measured as in proton_scale_check.py (when the shared stream stopped changing after the feed, the
probe's latency) and checked against DuckDB.
"""

import argparse
import json
import time

from proton_lab import ORACLE_RISK, Feed, Lab, Subscription, compare, oracle, pct
from probe import add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--scale", type=int, default=5)
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--shape", default="nested", choices=["nested", "leaf"])
parser.add_argument("--all16", action="store_true", help="the 16 groupings of dynamic_groupings_spec.py (leaf only)")
parser.add_argument("--out", required=True)
args = parser.parse_args()

LOCAL_JOIN = """SELECT p.book AS book, p.instrument AS instrument, b.desk AS desk, b.trader AS trader,
  i.sector AS sector, i.ccy AS ccy, 'ROOT' AS root, p.qty AS qty, q.px AS px,
  p.qty * q.px * i.multiplier AS mv_local, p.qty * (q.px - c.close_px) * i.multiplier AS pnl_local, q.ts AS px_ts, _tp_delta
FROM positions AS p
JOIN instruments AS i ON p.instrument = i.instrument
JOIN books AS b ON p.book = b.book
JOIN prices AS q ON p.instrument = q.instrument
JOIN closes AS c ON p.instrument = c.instrument"""
LOCAL_COLUMNS = ("book string, instrument string, desk string, trader string, sector string, ccy string, root string, "
                 "qty float64, px float64, mv_local float64, pnl_local float64, px_ts int64")
GROUPINGS = {"desk": ["desk"], "desk_trader": ["desk", "trader"], "book": ["book"], "sector": ["sector"],
             "root": ["root"]}
WHERE = {}
if args.all16:
    from dynamic_groupings_spec import FILTERS, SUBS
    GROUPINGS = {("desk" if i == 0 else f"g{i}"): (list(SUBS[i]) or ["root"]) for i in range(16)}
    WHERE = {("desk" if i == 0 else f"g{i}"): " AND ".join(f"{k[2:]} = '{v}'" for k, v in FILTERS[i].items())
             for i in FILTERS}


LEAF_KEYS = "desk, trader, book, sector, ccy, root"


def grouping_sql(cols, where=""):
    k = ", ".join(cols)
    if args.shape == "leaf":
        w = f" WHERE {where}" if where else ""
        return (f"SELECT {k}, sum(mv_usd) AS mv_usd, sum(pnl_usd) AS pnl_usd, sum(n) AS n, _tp_delta FROM leaf_usd_s{w} "
                f"GROUP BY {k} EMIT CHANGELOG PERIODIC 100ms")
    gk = ", ".join(f"g.{c} AS {c}" for c in cols)
    return (f"SELECT {k}, sum(mv_l * usd_rate) AS mv_usd, sum(pnl_l * usd_rate) AS pnl_usd, sum(n) AS n, _tp_delta FROM "
            f"(SELECT {gk}, g.mv_l AS mv_l, g.pnl_l AS pnl_l, g.n AS n, f.usd_rate AS usd_rate FROM "
            f"(SELECT {k}, ccy, sum(mv_local) AS mv_l, sum(pnl_local) AS pnl_l, count() AS n FROM position_local_s "
            f"GROUP BY {k}, ccy) AS g JOIN fx AS f ON g.ccy = f.ccy) "
            f"GROUP BY {k} EMIT CHANGELOG PERIODIC 100ms")


ref = reference(instruments=2000 * args.scale, positions_per_book=400 * args.scale)
batches = schedule(ref, seconds=args.seconds, ticks_per_s=args.rate)
add_probe(ref)
lab = Lab()
lab.reset(extra_views=("g_desk_mv", "leaf_usd_mv", "leaf_local_mv", "position_local_mv"),
          extra_streams=("g_desk_s", "leaf_usd_s", "leaf_local_s", "position_local_s"))
lab.create(target="changelog_kv")
lab.c.execute("DROP VIEW position_risk_mv")    # the per-position FX join is not used here
lab.c.execute(f"CREATE STREAM position_local_s ({LOCAL_COLUMNS}) PRIMARY KEY (book, instrument) SETTINGS mode='changelog_kv'")
lab.c.execute(f"CREATE MATERIALIZED VIEW position_local_mv INTO position_local_s AS {LOCAL_JOIN} EMIT CHANGELOG")
if args.shape == "leaf":
    leaf_cols = "desk string, trader string, book string, sector string, ccy string, root string"
    lab.c.execute(f"CREATE STREAM leaf_local_s ({leaf_cols}, mv_l float64, pnl_l float64, n uint64) "
                  "PRIMARY KEY (desk, trader, book, sector, ccy, root) SETTINGS mode='changelog_kv'")
    lab.c.execute(f"CREATE MATERIALIZED VIEW leaf_local_mv INTO leaf_local_s AS SELECT {LEAF_KEYS}, sum(mv_local) AS mv_l, "
                  f"sum(pnl_local) AS pnl_l, count() AS n, _tp_delta FROM position_local_s GROUP BY {LEAF_KEYS} "
                  "EMIT CHANGELOG PERIODIC 100ms")
    lab.c.execute(f"CREATE STREAM leaf_usd_s ({leaf_cols}, mv_usd float64, pnl_usd float64, n uint64) "
                  "PRIMARY KEY (desk, trader, book, sector, ccy, root) SETTINGS mode='changelog_kv'")
    lab.c.execute("CREATE MATERIALIZED VIEW leaf_usd_mv INTO leaf_usd_s AS SELECT "
                  + ", ".join(f"l.{c} AS {c}" for c in LEAF_KEYS.split(", "))
                  + ", l.mv_l * f.usd_rate AS mv_usd, l.pnl_l * f.usd_rate AS pnl_usd, l.n AS n, _tp_delta "
                  "FROM leaf_local_s AS l JOIN fx AS f ON l.ccy = f.ccy EMIT CHANGELOG")
lab.load(ref)
time.sleep(3)
subs = {name: Subscription(lab, grouping_sql(cols, WHERE.get(name, "")), cols, probe=(name == "desk"), name=name)
        for name, cols in GROUPINGS.items()}
deadline = time.time() + 60
while time.time() < deadline and any(s.first is None and s.error is None for s in subs.values()):
    time.sleep(0.1)
c0, _ = lab.usage()
feed = Feed(lab, ref, batches)
t0 = time.time()
with lab.lock:
    subs["desk"].measuring = True
feed.start()
feed.thread.join()
feed_s = time.time() - t0
c1, _ = lab.usage()
with lab.lock:
    subs["desk"].measuring = False
    lat = list(subs["desk"].lat)

t_stop = time.time()
last, quiet_since = None, time.time()
while time.time() - t_stop < 900:
    cur = lab.query("SELECT count() AS n, max(_tp_time) AS t FROM table(position_local_s)")[0]
    cur = (cur["n"], cur["t"])
    if cur != last:
        last, quiet_since = cur, time.time()
    elif time.time() - quiet_since >= 10:
        break
    time.sleep(1)
time.sleep(3)

db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
checks = {}
for name, cols in GROUPINGS.items():
    w = f" WHERE {WHERE[name]}" if name in WHERE else ""
    exp = db.execute(f"SELECT {', '.join(cols)}, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n FROM r{w} "
                     f"GROUP BY {', '.join(cols)}").fetchall()
    names = [d[0] for d in db.description]
    with lab.lock:
        got = dict(subs[name].state)
    checks[name] = {**compare([dict(zip(names, r)) for r in exp], got, cols, ["mv_usd", "pnl_usd", "n"]),
                    "error": subs[name].error, "firstRowsMillis": subs[name].first_rows_ms()}
result = {"scale": args.scale, "shape": args.shape, "groupings": len(GROUPINGS), "rate": args.rate, "positions": len(ref.positions), "feedSeconds": round(feed_s, 1),
          "engineCpuCores": round((c1 - c0) / feed_s, 2), "lastWriteAfterFeedSeconds": round(quiet_since - t_stop, 1),
          "tickToSubscriberMillis": {"p50": pct(lat, 0.5), "p95": pct(lat, 0.95), "n": len(lat)},
          "checks": checks, "correct": all(c["ok"] and not c["error"] for c in checks.values())}
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k != "checks"}, default=str))
print(json.dumps({k: (v["ok"], v["mismatches"], v["rows"], v["extraRows"], (v["error"] or "")[:200]) for k, v in checks.items()}))
import os  # noqa: E402
os._exit(0)
