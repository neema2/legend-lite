"""Deephaven's limits with the 16 groupings live (proton_perf.py's variants): tick rate, book size,
update cycle, viewers.

    python dh_perf.py [--rate 1000] [--scale 1] [--cycle-ms 100] [--viewers 0 --viewer-mode tables|subscribers]
                      [--seconds 30] --out results/x.json

The 16 groupings of dynamic_groupings_spec.py are aggregations over the per-position risk table
(natural joins; FX applied per position), each subscribed. --scale multiplies the book
(instruments x scale, positions per book x scale). --viewers adds that many viewers of the desk
grouping: each its own aggregation (tables), or each a subscription to the one desk grouping
(subscribers). Measured over the window: the server's CPU and resident memory, the probe's
tick-to-subscriber latency, the time the last batch took to reach the desk grouping (drain);
then every grouping, the subscriber's state and a server snapshot, against DuckDB.
"""

import argparse
import json
import time

from dh_lab import DhLab, Feed, Subscription, start_server, stop_server
from dynamic_groupings_spec import FILTERS, SUBS
from probe import PROBE, add_probe
from proton_lab import ORACLE_RISK, compare, oracle, pct
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--scale", type=int, default=1)
parser.add_argument("--cycle-ms", type=int, default=100)
parser.add_argument("--heap", default="4g")
parser.add_argument("--viewers", type=int, default=0)
parser.add_argument("--viewer-mode", default="tables", choices=["tables", "subscribers"])
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--drain", type=float, default=30)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference(instruments=2000 * args.scale, positions_per_book=400 * args.scale)
batches = schedule(ref, seconds=args.seconds + 60, ticks_per_s=args.rate)
add_probe(ref)
proc, up_s = start_server(cycle_ms=args.cycle_ms, heap=args.heap, log_path=args.out + ".server.log")
lab = DhLab(proc.pid)
risk = lab.create()
t0 = time.time()
lab.load(ref)
load_s = time.time() - t0

from pydeephaven import agg  # noqa: E402

MEAS = [agg.sum_(["mv_usd", "pnl_usd"]), agg.count_("n")]


def grouping(i):
    t = risk
    f = FILTERS.get(i, {})
    if f:
        t = t.where([f"{k[2:]} == `{v}`" for k, v in f.items()])
    return t.agg_by(MEAS, by=list(SUBS[i]) or ["root"])


feed = Feed(lab, ref, batches)
time.sleep(2)
feed.start()
time.sleep(3)
tables, groupings = {}, {}
for i in range(16):
    tables[i] = grouping(i)
    groupings[i] = Subscription(lab, tables[i], list(SUBS[i]) or ["root"], probe=(i == 0), name=f"g{i}")
viewers = []
for v in range(args.viewers):
    t = risk.agg_by(MEAS, by=["desk"]) if args.viewer_mode == "tables" else tables[0]
    viewers.append(Subscription(lab, t, ["desk"], probe=(v == 0), name=f"viewer{v}"))
probe_sub = viewers[0] if viewers else groupings[0]

deadline = time.time() + 60
while time.time() < deadline and any(s.first is None and not s.errors for s in [*groupings.values(), *viewers]):
    time.sleep(0.05)
time.sleep(3)
c0, _ = lab.usage()
sent0 = feed.sent
with lab.lock:
    probe_sub.lat.clear()
    probe_sub.measuring = True
time.sleep(args.seconds)
with lab.lock:
    probe_sub.measuring = False
    lat = list(probe_sub.lat)
c1, rss = lab.usage()
batches_in_window = feed.sent - sent0
feed.on.clear()
feed.thread.join()

t_stop = time.time()
target = feed.final["prices"][PROBE["instrument"]]
while time.time() - t_stop < args.drain:
    with lab.lock:
        row = groupings[0].state.get((PROBE["desk"],))
    if row is not None and row["mv_usd"] == target:
        break
    time.sleep(0.01)
drain_ms = round((time.time() - t_stop) * 1000)
time.sleep(2)

db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
bad = {}
for i in range(16):
    cols = list(SUBS[i])
    f = FILTERS.get(i, {})
    where = " AND ".join(f"{k[2:]} = '{v}'" for k, v in f.items()) or "true"
    sel = ", ".join(cols + ["SUM(mv_usd) AS mv_usd", "SUM(pnl_usd) AS pnl_usd", "COUNT(*) AS n"])
    q = f"SELECT {sel} FROM r WHERE {where}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    exp_raw = db.execute(q)
    names = [d[0] for d in exp_raw.description]
    keys = cols or ["root"]
    exp = [dict(zip(names, r), **({} if cols else {"root": "ROOT"})) for r in exp_raw.fetchall()]
    with lab.lock:
        got = dict(groupings[i].state)
    snap = {tuple(r[k] for k in keys): r for r in lab.snapshot(tables[i])}
    cs, cn = compare(exp, got, keys, ["mv_usd", "pnl_usd", "n"]), compare(exp, snap, keys, ["mv_usd", "pnl_usd", "n"])
    # correctness is the values; a client error (the ticking client's, then resubscribed) is counted apart
    if not cs["ok"] or not cn["ok"]:
        bad[i] = {"subscriber": (cs["mismatches"], cs["rows"], cs["extraRows"]),
                  "snapshot": (cn["mismatches"], cn["rows"], cn["extraRows"]), "clientErrors": groupings[i].errors[:1]}
# every row of the risk table against the final inputs: a stale price or FX rate, a missing or extra position
rows = lab.snapshot(risk.view(["book", "instrument", "px", "usd_rate"]))
fx_final = feed.final["fx"]
ccy = {i["instrument"]: i["ccy"] for i in ref.instruments}
have = {(r["book"], r["instrument"]) for r in rows}
risk_check = {"rows": len(rows), "expected": len(feed.final["positions"]),
              "stalePrice": sum(1 for r in rows if r["px"] != feed.final["prices"][r["instrument"]]),
              "staleFx": sum(1 for r in rows if r["usd_rate"] != fx_final[ccy[r["instrument"]]]),
              "missing": sum(1 for k in feed.final["positions"] if k not in have),
              "extra": sum(1 for k in have if k not in feed.final["positions"])}
client_errors = sum(len(s.errors) for s in [*groupings.values(), *viewers])
for s in [*groupings.values(), *viewers]:
    s.stop()
stop_server(proc)

result = {"rate": args.rate, "scale": args.scale, "cycleMs": args.cycle_ms, "viewers": args.viewers,
          "viewerMode": args.viewer_mode if args.viewers else None, "positions": len(ref.positions),
          "instruments": len(ref.instruments), "serverStartSeconds": up_s, "loadSeconds": round(load_s, 1),
          "batchesPerSecond": round(batches_in_window / args.seconds, 1),
          "engineCpuCores": round((c1 - c0) / args.seconds, 2), "serverRssMB": round(rss / 2**20),
          "tickToSubscriberMillis": {"p50": pct(lat, 0.5), "p95": pct(lat, 0.95), "p99": pct(lat, 0.99), "n": len(lat)},
          "drainMillis": drain_ms, "groupingFirstRowsMillis": [groupings[i].first_rows_ms() for i in range(16)],
          "viewerFirstRowsMillis": [v.first_rows_ms() for v in viewers][:5], "clientErrors": client_errors,
          "riskRows": risk_check,
          "correct": not bad and not any(risk_check[k] for k in ("stalePrice", "staleFx", "missing", "extra")),
          "badGroupings": bad}
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: result[k] for k in ("rate", "scale", "cycleMs", "viewers", "viewerMode", "batchesPerSecond",
                                          "engineCpuCores", "serverRssMB", "tickToSubscriberMillis", "drainMillis",
                                          "clientErrors", "correct")}, default=str))
import os  # noqa: E402
os._exit(0)
