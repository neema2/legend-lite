"""Deephaven under the changes a real risk feed carries, checked against DuckDB (proton_correctness.py's
scenario).

    python dh_correctness.py --out results/x.json

Positions are a keyed input table: an update is an upsert, a close is a delete of the key. Five
groupings stay live (subscribed) while the feed runs at 1,000 ticks a second and, mid-run:
  batch 100  book EQU-T1-B1 moves to the Macro desk (trader MAC-T1)
  batch 150  I0001's multiplier is corrected (x10); I0003 is re-currencied to JPY
  batch 200  50 positions are closed out (deleted)
After the feed, every grouping (the subscriber's state and a server snapshot) is compared with
DuckDB over the final inputs, and so is the number of rows in the risk table.
"""

import argparse
import json
import random
import time

from dh_lab import DhLab, Feed, Subscription, start_server, stop_server
from probe import add_probe
from proton_lab import ORACLE_RISK, compare, oracle
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=float, default=40)
parser.add_argument("--drain", type=float, default=5)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
proc, up_s = start_server(log_path=args.out + ".server.log")
lab = DhLab(proc.pid)
risk = lab.create()
lab.load(ref)

from pydeephaven import agg  # noqa: E402

GROUPINGS = {"desk": ["desk"], "desk_trader": ["desk", "trader"], "book": ["book"], "ccy": ["ccy"],
             "sector": ["sector"]}
MEAS = [agg.sum_(["mv_usd", "pnl_usd"]), agg.count_("n")]
tables = {name: risk.agg_by(MEAS, by=cols) for name, cols in GROUPINGS.items()}
subs = {name: Subscription(lab, tables[name], cols, name=name) for name, cols in GROUPINGS.items()}
extra_books, extra_instruments, deleted = {}, {}, []


def move_book(lab):
    lab.add("books", [{"book": "EQU-T1-B1", "trader": "MAC-T1", "desk": "Macro"}])
    extra_books["EQU-T1-B1"] = {"book": "EQU-T1-B1", "trader": "MAC-T1", "desk": "Macro"}


def correct_instruments(lab):
    by = {i["instrument"]: dict(i) for i in ref.instruments}
    a = dict(by["I0001"], multiplier=by["I0001"]["multiplier"] * 10)
    b = dict(by["I0003"], ccy="JPY")
    lab.add("instruments", [a, b])
    extra_instruments.update({"I0001": a, "I0003": b})


def close_out(lab):
    keys = random.Random(3).sample(sorted(k for k in feed.final["positions"] if k[0] != "PROBE-B1"), 50)
    lab.delete("positions", [{"book": b, "instrument": i} for b, i in keys])
    for k in keys:
        feed.final["positions"].pop(k, None)
    deleted.extend(keys)


feed = Feed(lab, ref, batches, hooks={100: move_book, 150: correct_instruments, 200: close_out})
time.sleep(3)
feed.start()
feed.thread.join()
time.sleep(args.drain)

db = oracle(ref, feed.final, extra_books, extra_instruments)
result = {"seconds": args.seconds, "serverStartSeconds": up_s, "batchesSent": feed.sent,
          "deletedPositions": len(deleted), "groupings": {}}
for name, cols in GROUPINGS.items():
    expected = db.execute(f"SELECT {', '.join(cols)}, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n "
                          f"FROM ({ORACLE_RISK}) GROUP BY {', '.join(cols)}").fetchall()
    names = [d[0] for d in db.description]
    exp = [dict(zip(names, r)) for r in expected]
    with lab.lock:
        got = dict(subs[name].state)
    snap = {tuple(r[c] for c in cols): r for r in lab.snapshot(tables[name])}
    result["groupings"][name] = {"subscriber": compare(exp, got, cols, ["mv_usd", "pnl_usd", "n"]),
                                 "snapshot": compare(exp, snap, cols, ["mv_usd", "pnl_usd", "n"]),
                                 "clientErrors": subs[name].errors[:2]}
expected_rows = db.execute(f"SELECT COUNT(*) FROM ({ORACLE_RISK})").fetchone()[0]
result["riskRows"] = {"expected": expected_rows, "got": len(lab.snapshot(risk.view(["book"])))}
result["allCorrect"] = all(g["subscriber"]["ok"] and g["snapshot"]["ok"] for g in result["groupings"].values()) \
    and result["riskRows"]["expected"] == result["riskRows"]["got"]
for s in subs.values():
    s.stop()
stop_server(proc)
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({"allCorrect": result["allCorrect"], "riskRows": result["riskRows"],
                  "groupings": {k: (v["subscriber"]["mismatches"], v["snapshot"]["mismatches"], v["subscriber"]["rows"],
                                    len(v["clientErrors"])) for k, v in result["groupings"].items()}}, default=str))
import os  # noqa: E402
os._exit(0)
