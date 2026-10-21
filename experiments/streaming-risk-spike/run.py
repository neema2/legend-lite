"""Runs the risk scenario against one engine and writes what it measured.

    python run.py --engine feldera|proton|deephaven [--seconds 60] [--rate 1000] --out results/x.json

1. setup: the engine creates its inputs and live views, subscribes to them, loads the reference
   data and the opening positions, prices and rates;
2. the schedule is sent in real time, one batch every 100 ms, prices and rates stamped with the
   send time; every change set the subscriptions deliver is applied to a per-view state and timed;
3. after the last batch, the run waits until the grand total reflects it (the drain), then
   compares both the subscriber's state and a fresh snapshot of every view with an oracle:
   DuckDB running the same SQL over the final inputs.
"""

import argparse
import json
import statistics
import threading
import time

from scenario import reference, schedule
from views import CHECKED, TABLES_ANSI, VIEWS, aggregate, position_risk

parser = argparse.ArgumentParser()
parser.add_argument("--engine", required=True, choices=["feldera", "proton", "deephaven"])
parser.add_argument("--seconds", type=float, default=60)
parser.add_argument("--rate", type=int, default=1000, help="price ticks a second")
parser.add_argument("--fx-rate", type=int, default=5)
parser.add_argument("--bookings", type=int, default=20)
parser.add_argument("--batch-ms", type=int, default=100)
parser.add_argument("--deephaven-pid", type=int)
parser.add_argument("--proton-shape", default="flat", choices=["flat", "fx_first"])
parser.add_argument("--proton-emit", default="EMIT CHANGELOG PERIODIC 100ms")
parser.add_argument("--views", default=",".join(VIEWS), help="the views to subscribe to")
parser.add_argument("--out", required=True)
args = parser.parse_args()
views = [v for v in args.views.split(",") if v]
CHECK = "risk_total" if "risk_total" in views else "risk_by_book"
assert CHECK in views, "subscribe to risk_total or risk_by_book: the run's completion checks read one of them"

ref = reference()
batches = schedule(ref, seconds=args.seconds, batch_ms=args.batch_ms, ticks_per_s=args.rate,
                   fx_ticks_per_s=args.fx_rate, bookings_per_s=args.bookings)

import engines  # noqa: E402

engine = {"feldera": lambda: engines.Feldera(), "proton": lambda: engines.Proton(shape=args.proton_shape, emit=args.proton_emit),
          "deephaven": lambda: engines.Deephaven(pid=args.deephaven_pid)}[args.engine]()

lock = threading.Lock()
state = {v: {} for v in VIEWS}
measuring = threading.Event()
fresh = {v: [] for v in VIEWS}
changesets = {v: 0 for v in VIEWS}
rows_received = {v: 0 for v in VIEWS}
book_fx = {}            # book -> last_fx_ts its row shows
fx_ticks = []           # [send ts, first book seen at, all books seen at]
px_batches = []         # [send ts, reflected in the check view at]; reflected in send order
px_next = [0]


def key_of(view, row):
    return tuple(row[c] for c in VIEWS[view][0])


def on_change(view, inserts, deletes, recv):
    with lock:
        st = state[view]
        for r in deletes:
            k = key_of(view, r)
            if k in st and all(st[k].get(c) == r.get(c) for c in CHECKED[view]):
                del st[k]
        newest = None
        for r in inserts:
            st[key_of(view, r)] = r
            ts = r.get("last_px_ts")
            if ts:
                newest = ts if newest is None else max(newest, ts)
        if not measuring.is_set():
            return
        if view == CHECK and newest:
            while px_next[0] < len(px_batches) and px_batches[px_next[0]][0] <= newest:
                px_batches[px_next[0]][1] = recv
                px_next[0] += 1
        changesets[view] += 1
        rows_received[view] += len(inserts) + len(deletes)
        if newest:
            fresh[view].append(recv - newest)
        if view == "risk_by_book" and inserts:
            for r in inserts:
                if r.get("last_fx_ts") is not None:
                    book_fx[r["book"]] = r["last_fx_ts"]
            if book_fx:
                lo, hi = min(book_fx.values()), max(book_fx.values())
                for tick in fx_ticks:
                    if tick[1] is None and hi >= tick[0]:
                        tick[1] = recv
                    if tick[2] is None and lo >= tick[0] and len(book_fx) == len(ref.books):
                        tick[2] = recv


def stats(xs):
    if not xs:
        return None
    s = sorted(xs)
    pick = lambda p: round(s[min(len(s) - 1, int(p * len(s)))], 1)
    return {"p50": pick(0.5), "p95": pick(0.95), "p99": pick(0.99), "max": round(s[-1], 1), "n": len(s)}


result = {"engine": engine.name, "seconds": args.seconds, "rate": args.rate, "fxRate": args.fx_rate,
          "bookings": args.bookings, "positions": len(ref.positions), "instruments": len(ref.instruments),
          "books": len(ref.books), "views": views,
          "protonShape": args.proton_shape if args.engine == "proton" else None,
          "protonEmit": args.proton_emit if args.engine == "proton" else None}

# 1. setup
t0 = time.time()
result["setup"] = engine.setup(ref, on_change, views)
expected_n = len(ref.positions)
deadline = time.time() + 300
while time.time() < deadline:
    with lock:
        rows = list(state[CHECK].values())
    if rows and sum(int(r["n"]) for r in rows) == expected_n:
        break
    time.sleep(0.05)
result["setup"]["firstCompleteTotalSeconds"] = round(time.time() - t0, 1)
time.sleep(2)

# 2. the schedule, in real time
final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
send_ms = []
cpu0, _ = engine.usage()
measuring.set()
start = time.time()
last_ts = None
for n, batch in enumerate(batches):
    target = start + n * args.batch_ms / 1000
    delay = target - time.time()
    if delay > 0:
        time.sleep(delay)
    ts = int(time.time() * 1000)
    s = time.time()
    with lock:
        if batch["fx"]:
            fx_ticks.append([ts, None, None])
        if batch["prices"]:
            px_batches.append([ts, None])
    engine.send(batch, ts)
    send_ms.append((time.time() - s) * 1000)
    final["positions"].update(batch["positions"])
    final["prices"].update(batch["prices"])
    final["fx"].update(batch["fx"])
    if batch["prices"]:
        last_ts = ts
elapsed = time.time() - start
cpu1, mem = engine.usage()
result["load"] = {
    "batches": len(batches), "elapsedSeconds": round(elapsed, 1),
    "priceTicksSent": sum(len(b["prices"]) for b in batches),
    "achievedTicksPerSecond": round(sum(len(b["prices"]) for b in batches) / elapsed),
    "sendMillis": stats(send_ms),
    "engineCpuCores": round((cpu1 - cpu0) / elapsed, 2),
    "engineMemoryMB": round(mem / 2**20),
}

# 3. drain: until the grand total reflects the last batch's prices
drain_start = time.time()
while time.time() - drain_start < 60:
    with lock:
        rows = list(state[CHECK].values())
    if rows and max(r.get("last_px_ts") or 0 for r in rows) == last_ts:
        break
    time.sleep(0.01)
result["drainMillis"] = round((time.time() - drain_start) * 1000)
time.sleep(1.5)
measuring.clear()

result["freshnessMs"] = {v: stats(fresh[v]) for v in views}
result["tickToCheckViewMs"] = {"view": CHECK,
                               "latency": stats([b[1] - b[0] for b in px_batches if b[1] is not None]),
                               "neverReflected": sum(1 for b in px_batches if b[1] is None)}
result["changeSetsPerSecond"] = {v: round(changesets[v] / elapsed, 1) for v in views}
result["rowsPerSecond"] = {v: round(rows_received[v] / elapsed, 1) for v in views}
done = [t for t in fx_ticks if t[2] is not None]
result["fxFanOutMs"] = {
    "ticks": len(fx_ticks),
    "firstBook": stats([t[1] - t[0] for t in fx_ticks if t[1] is not None]),
    "allBooks": stats([t[2] - t[0] for t in done]),
    "neverCompleted": len(fx_ticks) - len(done),
}

# 4. the oracle
import duckdb  # noqa: E402

db = duckdb.connect()
db.execute(TABLES_ANSI)
db.executemany("INSERT INTO books VALUES (?, ?, ?)", [(b["book"], b["trader"], b["desk"]) for b in ref.books])
db.executemany("INSERT INTO instruments VALUES (?, ?, ?, ?, ?)",
               [(i["instrument"], i["sector"], i["ccy"], i["beta"], i["multiplier"]) for i in ref.instruments])
db.executemany("INSERT INTO closes VALUES (?, ?)", [(c["instrument"], c["close_px"]) for c in ref.closes])
db.executemany("INSERT INTO positions VALUES (?, ?, ?)", [(b, i, q) for (b, i), q in final["positions"].items()])
db.executemany("INSERT INTO prices VALUES (?, ?, 0)", list(final["prices"].items()))
db.executemany("INSERT INTO fx VALUES (?, ?, 0)", list(final["fx"].items()))


def compare(view, rows):
    expected = db.execute(aggregate(view, f"({position_risk()})")).fetchall()
    names = [d[0] for d in db.description]
    exp = {key_of(view, dict(zip(names, r))): dict(zip(names, r)) for r in expected}
    got = {key_of(view, r): r for r in rows}
    worst, bad = 0.0, []
    for k, e in exp.items():
        g = got.get(k)
        if g is None:
            bad.append({"key": list(k), "missing": True})
            continue
        for c in CHECKED[view]:
            ev, gv = e[c], g.get(c)
            if ev is None and gv is None:
                continue
            if ev is None or gv is None:
                bad.append({"key": list(k), "column": c, "expected": ev, "got": gv})
                continue
            err = abs(float(gv) - float(ev)) / max(1.0, abs(float(ev)))
            worst = max(worst, err)
            if err > 1e-9:
                bad.append({"key": list(k), "column": c, "expected": ev, "got": gv})
    extra = [list(k) for k in got if k not in exp]
    return {"rows": len(exp), "mismatches": len(bad), "extraRows": len(extra),
            "maxRelativeError": worst, "examples": bad[:3]}


with lock:
    subscribed = {v: list(state[v].values()) for v in VIEWS}
result["correctness"] = {
    "subscriberState": {v: compare(v, subscribed[v]) for v in views},
    "snapshot": {v: compare(v, engine.snapshot(v)) for v in VIEWS},
}

# 5. a new live grouping while running (DataCube: the user regroups by sector)
result["adHoc"] = engine.ad_hoc(on_change)

engine.close()
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: result[k] for k in ("engine", "setup", "load", "drainMillis", "freshnessMs",
                                          "fxFanOutMs", "adHoc")}, indent=1, default=str))
print(json.dumps({k: {v: (c["mismatches"], c["rows"]) for v, c in result["correctness"][k].items()}
                  for k in result["correctness"]}))
