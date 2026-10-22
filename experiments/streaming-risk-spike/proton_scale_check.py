"""Where the shared join stops being right as the book grows: after the feed, once the view has
stopped writing, every row of the shared stream against the same join computed in Proton as a
batch query over the inputs' current state (table()).

    python proton_scale_check.py --scale 5 [--target changelog|versioned] [--seconds 30] --out x.json

Counted: rows whose price, FX rate or value differ from the batch join's (stale), positions with
no row, and rows with no position. --shape perquery skips the materialized view: a desk grouping
straight over the join, compared with the batch join's desk totals.
"""

import argparse
import json
import time

from proton_lab import JOIN_FLAT, JOIN_FX_FIRST, RISK_COLUMNS, Feed, Lab, Subscription
from probe import add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--scale", type=int, default=5)
parser.add_argument("--target", default="changelog", choices=["changelog", "versioned", "plain"],
                    help="the shared stream: changelog_kv, versioned_kv, or a plain changelog stream (no key, no collapsing)")
parser.add_argument("--shape", default="shared", choices=["shared", "perquery"])
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference(instruments=2000 * args.scale, positions_per_book=400 * args.scale)
batches = schedule(ref, seconds=args.seconds, ticks_per_s=args.rate)
add_probe(ref)
lab = Lab()
lab.reset(extra_views=("g_desk_mv",), extra_streams=("g_desk_s",))
BATCH = JOIN_FLAT
for s in ("positions", "instruments", "books", "prices", "fx", "closes"):
    BATCH = BATCH.replace(f"{s} AS ", f"table({s}) AS ")
if args.shape == "shared" and args.target == "plain":
    lab.create(target="versioned_kv")
    lab.c.execute("DROP VIEW position_risk_mv")
    lab.c.execute("DROP STREAM position_risk_s")
    lab.c.execute(f"CREATE STREAM position_risk_s ({RISK_COLUMNS}) SETTINGS mode='changelog'")
    sel = JOIN_FX_FIRST.replace("AS fx_ts\nFROM positions", "AS fx_ts, _tp_delta\nFROM positions", 1)
    lab.c.execute(f"CREATE MATERIALIZED VIEW position_risk_mv INTO position_risk_s AS {sel} EMIT CHANGELOG")
elif args.shape == "shared":
    lab.create(target="changelog_kv" if args.target == "changelog" else "versioned_kv")
else:
    lab.create(target="versioned_kv")
    lab.c.execute("DROP VIEW position_risk_mv")
lab.load(ref)
desk = None
if args.shape == "perquery":
    desk = Subscription(lab, f"SELECT desk, sum(mv_usd) AS mv_usd, count() AS n, _tp_delta FROM ({JOIN_FX_FIRST}) "
                             "GROUP BY desk EMIT CHANGELOG PERIODIC 100ms", ["desk"])
time.sleep(3)
feed = Feed(lab, ref, batches)
t0 = time.time()
feed.start()
feed.thread.join()
feed_s = time.time() - t0

# settle: until the shared stream has had no write for 10 s (or the desk grouping no change)
t_stop = time.time()
last, quiet_since = None, time.time()
while time.time() - t_stop < 900:
    if args.shape == "shared":
        cur = lab.query("SELECT count() AS n, max(_tp_time) AS t FROM table(position_risk_s)")[0]
        cur = (cur["n"], cur["t"])
    else:
        with lab.lock:
            cur = tuple(sorted((k, round(v["mv_usd"], 3), v["n"]) for k, v in desk.state.items()))
    if cur != last:
        last, quiet_since = cur, time.time()
    elif time.time() - quiet_since >= 10:
        break
    time.sleep(1)
settle_s = round(quiet_since - t_stop, 1)

result = {"scale": args.scale, "target": args.target, "shape": args.shape, "rate": args.rate,
          "positions": len(ref.positions), "feedSeconds": round(feed_s, 1), "lastWriteAfterFeedSeconds": settle_s}
if args.shape == "shared" and args.target == "plain":
    # the stream is every -1/+1 the view wrote: the net of each key, and the desks' totals as
    # sums of value x delta, against the batch join
    q = lambda sql: lab.query(sql)[0]["n"]  # noqa: E731
    net = ("SELECT book, instrument, sum(_tp_delta) AS d FROM table(position_risk_s) GROUP BY book, instrument")
    exp = {r["desk"]: r for r in lab.query(f"SELECT desk, sum(mv_usd) AS mv_usd, count() AS n FROM ({BATCH}) GROUP BY desk")}
    got = {r["desk"]: r for r in lab.query("SELECT desk, sum(mv_usd * _tp_delta) AS mv_usd, sum(_tp_delta) AS n "
                                           "FROM table(position_risk_s) GROUP BY desk")}
    bad = [d for d, e in exp.items() if d not in got or got[d]["n"] != e["n"]
           or abs(got[d]["mv_usd"] - e["mv_usd"]) > 1e-9 * (1 + abs(e["mv_usd"]))]
    result.update({"changelogRows": q("SELECT count() AS n FROM table(position_risk_s)"),
                   "keysNetNotOne": q(f"SELECT count() AS n FROM ({net}) WHERE d != 1"),
                   "expectedKeys": q(f"SELECT count() AS n FROM ({BATCH})"),
                   "keys": q(f"SELECT count() AS n FROM ({net})"),
                   "wrongDesks": len(bad), "examples": [{"desk": d, "expected": exp[d], "got": got.get(d)} for d in bad[:2]]})
    result["correct"] = not bad and result["keysNetNotOne"] == 0
elif args.shape == "shared":
    q = lambda sql: lab.query(sql)[0]["n"]  # noqa: E731
    on = (f"FROM table(position_risk_s) AS r JOIN ({BATCH}) AS e ON r.book = e.book AND r.instrument = e.instrument")
    result.update({
        "rows": q("SELECT count() AS n FROM table(position_risk_s)"),
        "expectedRows": q(f"SELECT count() AS n FROM ({BATCH})"),
        "stalePrice": q(f"SELECT count() AS n {on} WHERE r.px != e.px"),
        "staleFx": q(f"SELECT count() AS n {on} WHERE r.fx_ts != e.fx_ts"),
        "wrongValue": q(f"SELECT count() AS n {on} WHERE abs(r.mv_usd - e.mv_usd) > 1e-9 * (1 + abs(e.mv_usd))"),
        "missing": q(f"SELECT count() AS n FROM ({BATCH}) AS e LEFT ANTI JOIN table(position_risk_s) AS r "
                     "ON r.book = e.book AND r.instrument = e.instrument"),
        "extra": q(f"SELECT count() AS n FROM table(position_risk_s) AS r LEFT ANTI JOIN ({BATCH}) AS e "
                   "ON r.book = e.book AND r.instrument = e.instrument"),
    })
    result["correct"] = all(result[k] == 0 for k in ("stalePrice", "staleFx", "wrongValue", "missing", "extra")) \
        and result["rows"] == result["expectedRows"]
else:
    exp = {r["desk"]: r for r in lab.query(f"SELECT desk, sum(mv_usd) AS mv_usd, count() AS n FROM ({BATCH}) GROUP BY desk")}
    with lab.lock:
        got = {k[0]: v for k, v in desk.state.items()}
    bad = [d for d, e in exp.items() if d not in got or got[d]["n"] != e["n"]
           or abs(got[d]["mv_usd"] - e["mv_usd"]) > 1e-9 * (1 + abs(e["mv_usd"]))]
    result.update({"desks": len(exp), "wrongDesks": len(bad), "error": desk.error,
                   "examples": [{"desk": d, "expected": exp[d], "got": got.get(d)} for d in bad[:2]]})
    result["correct"] = not bad and not desk.error
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps(result, default=str))
import os  # noqa: E402
os._exit(0)
