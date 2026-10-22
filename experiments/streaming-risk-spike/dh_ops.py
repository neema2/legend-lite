"""Deephaven as a service: a restart in the middle of the feed, and memory over time
(proton_ops.py's questions).

    python dh_ops.py recovery --kill graceful|hard --out x.json
    python dh_ops.py memory --seconds 240 --out x.json

recovery: Deephaven Community keeps its tables in memory only, so a restarted server starts empty.
The feed runs at 1,000 ticks a second and, 15 s in, the server process is stopped (graceful:
SIGTERM; hard: SIGKILL) and started again; the target then does what it would have to do: create
the inputs and the view again, load the current state from the sources of record (here the
feed's last state), subscribe again. The feed holds each batch and retries it until the new server
takes it. Measured: how long the process took to stop and to start, the reload, inserts refused,
the time until the desk grouping showed a tick again, then everything against DuckDB.

memory: the same feed for --seconds, the server's resident memory every 10 s.
"""

import argparse
import json
import threading
import time

from dh_lab import DhLab, Feed, Subscription, start_server, stop_server
from probe import PROBE, add_probe
from proton_lab import ORACLE_RISK, compare, oracle, pct
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("what", choices=["recovery", "memory"])
parser.add_argument("--kill", default="graceful", choices=["graceful", "hard"])
parser.add_argument("--seconds", type=float, default=40)
parser.add_argument("--kill-at", type=int, default=150)
parser.add_argument("--out", required=True)
args = parser.parse_args()

GROUPINGS = {"desk": ["desk"], "desk_trader": ["desk", "trader"], "book": ["book"], "ccy": ["ccy"],
             "sector": ["sector"]}
ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
log = args.out + ".server.log"
proc, up_s = start_server(log_path=log)

from pydeephaven import agg  # noqa: E402

MEAS = [agg.sum_(["mv_usd", "pnl_usd"]), agg.count_("n")]


def build(lab, state):
    """Inputs, view and the desk grouping on a fresh server, loaded from `state`."""
    risk = lab.create()
    lab.load(state)
    desk_t = risk.agg_by(MEAS, by=["desk"])
    return risk, desk_t


class State:
    """The sources of record as the target would read them: reference data plus the feed's last
    positions, prices and rates."""

    def __init__(self, ref, final):
        self.books, self.instruments, self.closes = ref.books, ref.instruments, ref.closes
        self.positions, self.prices, self.fx = dict(final["positions"]), dict(final["prices"]), dict(final["fx"])


lab = DhLab(proc.pid)
risk, desk_t = build(lab, ref)
desk = Subscription(lab, desk_t, ["desk"], probe=True, name="desk")
desk.measuring = True
time.sleep(2)

if args.what == "recovery":
    restart = {}
    current = {"lab": lab, "desk": desk, "risk": risk}

    def do_restart(_lab):
        def run():
            restart["at"] = time.time()
            current["desk"].on = False   # no resubscribing to the stopped server
            restart["stopSeconds"] = stop_server(current["proc"], hard=(args.kill == "hard"))
            p2, up2 = start_server(log_path=log)
            restart["startSeconds"] = up2
            t_b = time.time()
            lab2 = DhLab(p2.pid)
            snapshot = State(ref, feed.final)   # the feed is holding its batch until the new server takes it
            risk2, desk2_t = build(lab2, snapshot)
            restart["reloadSeconds"] = round(time.time() - t_b, 1)
            d2 = Subscription(lab2, desk2_t, ["desk"], probe=True, name="desk2")
            d2.measuring = True
            current.update({"lab": lab2, "desk": d2, "risk": risk2, "desk_t": desk2_t, "proc": p2})
            restart["ready"] = time.time()
        threading.Thread(target=run, daemon=True).start()

    current["proc"] = proc
    current["desk_t"] = desk_t

    def reconnect():
        feed.lab = current["lab"]

    feed = Feed(lab, ref, batches, hooks={args.kill_at: do_restart}, retry=True, reconnect=reconnect)
    feed.start()
    feed.thread.join()
    time.sleep(5)
    lab, risk, d2 = current["lab"], current["risk"], current["desk"]
    lat_after = list(d2.lat)

    db = oracle(ref, feed.final)
    checks = {}
    for name, cols in GROUPINGS.items():
        exp = db.execute(f"SELECT {', '.join(cols)}, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n "
                         f"FROM ({ORACLE_RISK}) GROUP BY {', '.join(cols)}").fetchall()
        names = [d[0] for d in db.description]
        snap = {tuple(r[c] for c in cols): r for r in lab.snapshot(risk.agg_by(MEAS, by=cols))}
        checks[name] = compare([dict(zip(names, r)) for r in exp], snap, cols, ["mv_usd", "pnl_usd", "n"])
    with lab.lock:
        got = dict(d2.state)
    exp = db.execute(f"SELECT desk, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n FROM ({ORACLE_RISK}) "
                     "GROUP BY desk").fetchall()
    checks["resubscribed_desk"] = compare([{"desk": r[0], "mv_usd": r[1], "pnl_usd": r[2], "n": r[3]} for r in exp], got,
                                          ["desk"], ["mv_usd", "pnl_usd", "n"])
    checks["riskRows"] = {"expected": db.execute(f"SELECT COUNT(*) FROM ({ORACLE_RISK})").fetchone()[0],
                          "got": len(lab.snapshot(risk.view(["book"])))}
    first_after = None if d2.first is None else round(d2.first - restart["ready"] * 1000)
    result = {"kill": args.kill, "batchesSent": feed.sent, "restart": {
                  "stopSeconds": restart["stopSeconds"], "startSeconds": restart["startSeconds"],
                  "reloadSeconds": restart["reloadSeconds"], "insertsRefusedSeconds": [round(b - a, 1) for a, b in feed.refused],
                  "firstGroupingRowsAfterReloadMillis": first_after,
                  "tickToSubscriberAfter": {"p50": pct(lat_after, 0.5), "p95": pct(lat_after, 0.95), "n": len(lat_after)}},
              "tickToSubscriberBefore": {"p50": pct(desk.lat, 0.5), "p95": pct(desk.lat, 0.95)},
              "checks": checks,
              "allCorrect": all(v.get("ok", True) for v in checks.values()) and checks["riskRows"]["expected"] == checks["riskRows"]["got"]}
    stop_server(current["proc"])
else:
    feed = Feed(lab, ref, schedule(ref, seconds=args.seconds, ticks_per_s=1000))
    samples = [{"t": 0, "rssMB": round(lab.usage()[1] / 2**20)}]
    feed.start()
    t0 = time.time()
    while feed.thread.is_alive():
        time.sleep(10)
        samples.append({"t": round(time.time() - t0), "rssMB": round(lab.usage()[1] / 2**20)})
    time.sleep(3)
    db = oracle(ref, feed.final)
    exp = db.execute(f"SELECT desk, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n FROM ({ORACLE_RISK}) "
                     "GROUP BY desk").fetchall()
    snap = {(r["desk"],): r for r in lab.snapshot(desk_t)}
    check = compare([{"desk": r[0], "mv_usd": r[1], "pnl_usd": r[2], "n": r[3]} for r in exp], snap, ["desk"],
                    ["mv_usd", "pnl_usd", "n"])
    result = {"seconds": args.seconds, "samples": samples, "check": check, "allCorrect": check["ok"]}
    stop_server(proc)

with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k not in ("checks", "samples")}, default=str))
if "checks" in result:
    print(json.dumps({k: (v.get("ok"), v.get("mismatches"), v.get("rows")) if "ok" in v else v
                      for k, v in result["checks"].items()}, default=str))
import os  # noqa: E402
os._exit(0)
