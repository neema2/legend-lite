"""Proton under the changes a real risk feed carries, checked against DuckDB.

    python proton_correctness.py --target changelog|versioned --out results/x.json

Positions are a changelog_kv stream (so a position can be deleted): an update is sent as the old
row retracted (_tp_delta -1) and the new one added, a delete as the old row retracted. (A plain
insert into a changelog_kv stream for a key already there adds a second row; a versioned_kv
stream upserts but cannot delete.) The shared join writes into a changelog_kv or a versioned_kv
stream. Five groupings stay live while the feed runs at 1,000 ticks a second and, mid-run:
  batch 100  book EQU-T1-B1 moves to the Macro desk (trader MAC-T1)
  batch 150  I0001's multiplier is corrected (x10); I0003 is re-currencied to JPY
  batch 200  50 positions are closed out (deleted)
After the feed stops and the engine drains, every grouping is compared with DuckDB over the final
inputs, and so is the count of rows in the shared stream.
"""

import argparse
import json
import random
import time

from proton_lab import ORACLE_RISK, Feed, Lab, Subscription, compare, oracle
from probe import add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--target", default="changelog", choices=["changelog", "versioned"])
parser.add_argument("--seconds", type=float, default=40)
parser.add_argument("--drain", type=float, default=10)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
lab = Lab()
lab.reset()
lab.create(target="changelog_kv" if args.target == "changelog" else "versioned_kv", positions_mode="changelog_kv")
lab.load(ref)

GROUPINGS = {"desk": ["desk"], "desk_trader": ["desk", "trader"], "book": ["book"], "ccy": ["ccy"],
             "sector": ["sector"]}
MEAS = "sum(mv_usd) AS mv_usd, sum(pnl_usd) AS pnl_usd, count() AS n"
subs = {name: Subscription(lab, f"SELECT {', '.join(cols)}, {MEAS}, _tp_delta FROM position_risk_s "
                                f"GROUP BY {', '.join(cols)} EMIT CHANGELOG PERIODIC 100ms", cols, name=name)
        for name, cols in GROUPINGS.items()}

extra_books, extra_instruments, deleted = {}, {}, []
current = dict(ref.positions)   # what the positions stream holds, for retractions


class PositionsFeed(Feed):
    """Positions as a changelog: -old/+new for an update, +new for a new key."""

    def _send(self, batch, ts):
        prices = [(i, p, ts) for i, p in batch["prices"].items()]
        if prices:
            self.client.execute("INSERT INTO prices (instrument, px, ts) VALUES", prices)
        fxs = [(c, r, ts) for c, r in batch["fx"].items()]
        if fxs:
            self.client.execute("INSERT INTO fx (ccy, usd_rate, ts) VALUES", fxs)
        rows = []
        for (b, i), q in batch["positions"].items():
            if (b, i) in current:
                rows.append((b, i, current[(b, i)], -1))
            rows.append((b, i, q, 1))
            current[(b, i)] = q
        if rows:
            self.client.execute("INSERT INTO positions (book, instrument, qty, _tp_delta) VALUES", rows)


def move_book(client):
    client.execute("INSERT INTO books (book, trader, desk) VALUES", [("EQU-T1-B1", "MAC-T1", "Macro")])
    extra_books["EQU-T1-B1"] = {"book": "EQU-T1-B1", "trader": "MAC-T1", "desk": "Macro"}


def correct_instruments(client):
    by = {i["instrument"]: dict(i) for i in ref.instruments}
    a = dict(by["I0001"], multiplier=by["I0001"]["multiplier"] * 10)
    b = dict(by["I0003"], ccy="JPY")
    client.execute("INSERT INTO instruments (instrument, sector, ccy, beta, multiplier) VALUES",
                   [(x["instrument"], x["sector"], x["ccy"], x["beta"], x["multiplier"]) for x in (a, b)])
    extra_instruments.update({"I0001": a, "I0003": b})


def close_out(client):
    keys = random.Random(3).sample(sorted(k for k in current if k[0] != "PROBE-B1"), 50)
    client.execute("INSERT INTO positions (book, instrument, qty, _tp_delta) VALUES",
                   [(b, i, current[(b, i)], -1) for b, i in keys])
    for k in keys:
        del current[k]
        feed.final["positions"].pop(k, None)
    deleted.extend(keys)


feed = PositionsFeed(lab, ref, batches, hooks={100: move_book, 150: correct_instruments, 200: close_out})
time.sleep(3)
feed.start()
feed.thread.join()
time.sleep(args.drain)

db = oracle(ref, feed.final, extra_books, extra_instruments)
result = {"target": args.target, "seconds": args.seconds, "batchesSent": feed.sent,
          "deletedPositions": len(deleted), "groupings": {}}
for name, cols in GROUPINGS.items():
    expected = db.execute(f"SELECT {', '.join(cols)}, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n "
                          f"FROM ({ORACLE_RISK}) GROUP BY {', '.join(cols)}").fetchall()
    names = [d[0] for d in db.description]
    with lab.lock:
        got = dict(subs[name].state)
    result["groupings"][name] = {**compare([dict(zip(names, r)) for r in expected], got, cols,
                                           ["mv_usd", "pnl_usd", "n"]), "error": subs[name].error}
expected_rows = db.execute(f"SELECT COUNT(*) FROM ({ORACLE_RISK})").fetchone()[0]
result["sharedStreamRows"] = {"expected": expected_rows,
                              "got": lab.query("SELECT count() AS n FROM table(position_risk_s)")[0]["n"]}
result["allCorrect"] = all(g["ok"] for g in result["groupings"].values())
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({"target": args.target, "allCorrect": result["allCorrect"],
                  "groupings": {k: (v["mismatches"], v["rows"], v["extraRows"]) for k, v in result["groupings"].items()},
                  "sharedStreamRows": result["sharedStreamRows"]}, default=str))
import os  # noqa: E402
os._exit(0)
