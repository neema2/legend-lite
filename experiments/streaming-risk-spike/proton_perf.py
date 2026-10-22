"""Proton's limits with the join shared: join order, tick rate, book size, emit policy, viewers.

    python proton_perf.py [--join fx_first|flat] [--target changelog|versioned] [--rate 1000]
                          [--scale 1] [--emit "PERIODIC 100ms"] [--viewers 0 --viewer-mode queries|mv]
                          [--seconds 30] --out results/x.json

The 16 groupings of dynamic_groupings_spec.py run as streaming aggregations over the shared join's
stream. --scale multiplies the book (instruments x scale, positions per book x scale). --viewers
adds that many viewers of the desk grouping: each its own streaming query (queries), or each a
tail of one materialized desk grouping (mv: the grouping runs once into a changelog_kv stream).
Measured over the window: the container's CPU and memory, the probe's tick-to-subscriber latency
(desk grouping, or a viewer of it), the time the last batch took to reach the desk grouping
(drain); then every grouping against DuckDB.
"""

import argparse
import json
import time

from dynamic_groupings_spec import FILTERS, SUBS
from proton_lab import ORACLE_RISK, Feed, Lab, Subscription, compare, oracle, pct
from probe import PROBE, add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--join", default="fx_first", choices=["fx_first", "flat"])
parser.add_argument("--target", default="changelog", choices=["changelog", "versioned"])
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--scale", type=int, default=1)
parser.add_argument("--emit", default="PERIODIC 100ms")
parser.add_argument("--viewers", type=int, default=0)
parser.add_argument("--viewer-mode", default="queries", choices=["queries", "mv"])
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--drain", type=float, default=15)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference(instruments=2000 * args.scale, positions_per_book=400 * args.scale)
batches = schedule(ref, seconds=args.seconds + 60, ticks_per_s=args.rate)
add_probe(ref)
lab = Lab()
lab.reset(extra_views=("g_desk_mv",), extra_streams=("g_desk_s",))
lab.create(join=args.join, target="changelog_kv" if args.target == "changelog" else "versioned_kv")
t0 = time.time()
lab.load(ref)
load_s = time.time() - t0

MEAS = "sum(mv_usd) AS mv_usd, sum(pnl_usd) AS pnl_usd, count() AS n"


def where(i):
    f = FILTERS.get(i, {})
    return " AND ".join(f"{k[2:]} = '{v}'" for k, v in f.items()) or "true"


feed = Feed(lab, ref, batches)
time.sleep(2)
feed.start()
time.sleep(3)
groupings = {}
for i in range(16):
    cols = list(SUBS[i]) or ["root"]
    groupings[i] = Subscription(lab, f"SELECT {', '.join(cols)}, {MEAS}, _tp_delta FROM position_risk_s "
                                     f"WHERE {where(i)} GROUP BY {', '.join(cols)} EMIT CHANGELOG {args.emit}",
                                cols, probe=(i == 0), name=f"g{i}")
viewers = []
if args.viewers:
    if args.viewer_mode == "mv":
        lab.c.execute("CREATE STREAM g_desk_s (desk string, mv_usd float64, pnl_usd float64, n uint64) "
                      "PRIMARY KEY desk SETTINGS mode='changelog_kv'")
        lab.c.execute(f"CREATE MATERIALIZED VIEW g_desk_mv INTO g_desk_s AS SELECT desk, {MEAS}, _tp_delta "
                      f"FROM position_risk_s GROUP BY desk EMIT CHANGELOG {args.emit}")
        sql = "SELECT desk, mv_usd, pnl_usd, n, _tp_delta FROM g_desk_s"
    else:
        sql = f"SELECT desk, {MEAS}, _tp_delta FROM position_risk_s GROUP BY desk EMIT CHANGELOG {args.emit}"
    for v in range(args.viewers):
        viewers.append(Subscription(lab, sql, ["desk"], probe=(v == 0), name=f"viewer{v}"))
probe_sub = viewers[0] if viewers else groupings[0]

deadline = time.time() + 60
while time.time() < deadline and any(s.first is None and s.error is None for s in [*groupings.values(), *viewers]):
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
c1, mem = lab.usage()
batches_in_window = feed.sent - sent0
feed.on.clear()
feed.thread.join()
last_ts = None
with lab.lock:
    pass
# drain: until the desk grouping shows the probe's last price
t_stop = time.time()
target_px = feed.final["prices"][PROBE["instrument"]]
while time.time() - t_stop < args.drain:
    with lab.lock:
        row = groupings[0].state.get((PROBE["desk"],))
    if row is not None and row["mv_usd"] == target_px:
        break
    time.sleep(0.01)
drain_ms = round((time.time() - t_stop) * 1000)
time.sleep(max(0.0, args.drain - (time.time() - t_stop)))

db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
bad_groupings = {}
for i in range(16):
    cols = list(SUBS[i])
    sel = ", ".join(cols + ["SUM(mv_usd) AS mv_usd", "SUM(pnl_usd) AS pnl_usd", "COUNT(*) AS n"])
    q = f"SELECT {sel} FROM r WHERE {where(i)}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    exp = db.execute(q).fetchall()
    names = [d[0] for d in db.description]
    keys = cols or ["root"]
    rows = [dict(zip(names, r), root="ROOT") for r in exp] if not cols else [dict(zip(names, r)) for r in exp]
    with lab.lock:
        got = dict(groupings[i].state)
    c = compare(rows, got, keys, ["mv_usd", "pnl_usd", "n"])
    if not c["ok"] or groupings[i].error:
        bad_groupings[i] = {"mismatches": c["mismatches"], "rows": c["rows"], "extra": c["extraRows"], "error": groupings[i].error}
viewer_errors = [v.error for v in viewers if v.error]

result = {"join": args.join, "target": args.target, "rate": args.rate, "scale": args.scale, "emit": args.emit,
          "viewers": args.viewers, "viewerMode": args.viewer_mode if args.viewers else None,
          "positions": len(ref.positions), "instruments": len(ref.instruments), "loadSeconds": round(load_s, 1),
          "achievedTicksPerSecond": round(batches_in_window / args.seconds * args.rate / 10 * 10) if batches_in_window else 0,
          "batchesPerSecond": round(batches_in_window / args.seconds, 1),
          "engineCpuCores": round((c1 - c0) / args.seconds, 2), "engineMemoryMB": round(mem / 2**20),
          "tickToSubscriberMillis": {"p50": pct(lat, 0.5), "p95": pct(lat, 0.95), "p99": pct(lat, 0.99), "n": len(lat)},
          "drainMillis": drain_ms, "groupingFirstRowsMillis": [groupings[i].first_rows_ms() for i in range(16)],
          "viewerFirstRowsMillis": [v.first_rows_ms() for v in viewers][:5],
          "correct": not bad_groupings and not viewer_errors, "badGroupings": bad_groupings, "viewerErrors": viewer_errors[:3]}
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: result[k] for k in ("join", "target", "rate", "scale", "emit", "viewers", "viewerMode", "batchesPerSecond",
                                          "engineCpuCores", "engineMemoryMB", "tickToSubscriberMillis", "drainMillis",
                                          "correct")}, default=str))
import os  # noqa: E402
os._exit(0)
