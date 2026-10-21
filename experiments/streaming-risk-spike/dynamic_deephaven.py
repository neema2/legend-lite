"""The same 16 groupings as dynamic_groupings.py, on Deephaven: each is a new aggregation attached
at runtime to the live risk table, subscribed with the ticking client. Measured as there: time to
first rows per grouping; CPU and tick-to-subscriber latency with 0, 1, 4 and 16 live; every
grouping against DuckDB at the end.

    python dynamic_deephaven.py --pid <deephaven server pid> --out results/deephaven-dynamic.json
"""

import argparse
import json
import os
import threading
import time

import engines
from dynamic_groupings_spec import DIMS, FILTERS, SUBS
from scenario import reference, schedule
from views import TABLES_ANSI, position_risk

parser = argparse.ArgumentParser()
parser.add_argument("--pid", type=int, required=True)
parser.add_argument("--out", required=True)
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--window", type=float, default=10)
args = parser.parse_args()

ref = reference()
batches = schedule(ref, seconds=200, ticks_per_s=args.rate)
dh = engines.Deephaven(pid=args.pid)
lock = threading.Lock()
state = {}             # (sub, *group values) -> row
first_rows = {}
watch = {"pending": [], "next": 0, "lat": []}


def on_change_for(sub, cols):
    def on_change(view, inserts, deletes, recv):
        with lock:
            for r in deletes:
                state.pop((sub, *(r[c] for c in cols)), None)
            newest = None
            for r in inserts:
                state[(sub, *(r[c] for c in cols))] = r
                if sub == 0 and r.get("last_px_ts"):
                    newest = max(newest or 0, r["last_px_ts"])
            if inserts:
                first_rows.setdefault(sub, recv)
            p = watch["pending"]
            while newest and watch["next"] < len(p) and p[watch["next"]] <= newest:
                watch["lat"].append(recv - p[watch["next"]])
                watch["next"] += 1
    return on_change


result = {"rate": args.rate}
result["setup"] = dh.setup(ref, lambda *a: None, views=())
final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
sending = threading.Event()
sending.set()


def sender():
    t0 = time.time()
    for n, batch in enumerate(batches):
        if not sending.is_set():
            break
        delay = t0 + n * 0.1 - time.time()
        if delay > 0:
            time.sleep(delay)
        ts = int(time.time() * 1000)
        with lock:
            if batch["prices"]:
                watch["pending"].append(ts)
        dh.send(batch, ts)
        for k in final:
            final[k].update(batch[k])


th = threading.Thread(target=sender, daemon=True)
th.start()
time.sleep(5)

from pydeephaven import agg  # noqa: E402

measures = [agg.sum_(["mv_usd", "pnl_usd", "beta_delta_usd"]), agg.count_("n"),
            agg.max_(["last_px_ts = px_ts", "last_fx_ts = fx_ts"])]


def grouping(i):
    t = dh.risk
    f = FILTERS.get(i, {})
    if f:
        t = t.where([f"{k[2:]} == `{v}`" for k, v in f.items()])
    cols = list(SUBS[i])
    # no dimensions: the grand total, grouped by the constant root column (pydeephaven's agg_by needs a by)
    return t.agg_by(measures, by=cols or ["root"]), cols


tables = {}


def activate(ids):
    lat = {}
    for i in ids:
        start = time.time() * 1000
        table, cols = grouping(i)
        tables[i] = table
        dh.subscribe(f"sub{i}", table, on_change_for(i, cols))
        while i not in first_rows and time.time() * 1000 - start < 30000:
            time.sleep(0.002)
        lat[i] = round(first_rows[i] - start, 1) if i in first_rows else None
    return lat


def window():
    c0, _ = dh.usage()
    time.sleep(args.window)
    c1, rss = dh.usage()
    return {"engineCpuCores": round((c1 - c0) / args.window, 2), "engineRssMB": round(rss / 2**20)}


phases = [{"active": 0, **window()}]
active = 0
for target in (1, 4, 16):
    lat = activate(range(active, target))
    active = target
    with lock:
        watch["lat"].clear()
    w = window()
    with lock:
        xs = sorted(watch["lat"])
        groups = len(state)
    pick = lambda p: round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None
    phases.append({"active": active, "activationMillis": lat, **w, "groupRowsLive": groups,
                   "tickToSubscriberMillis": {"p50": pick(0.5), "p95": pick(0.95), "n": len(xs)}})
result["phases"] = phases
sending.clear()
th.join()
time.sleep(3)

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
def compare(expected, got):
    """mv and P&L within 1e-9 relative to the larger of the value and the group's gross market value
    (P&L nets long against short, so its own size can be far below the values it sums); counts exact."""
    bad, worst, examples = 0, 0.0, []
    for k, e in expected.items():
        g = got.get(k)
        if g is None:
            bad += 1
            examples.append({"key": list(k), "missing": True})
            continue
        scale = max(1.0, abs(e[0]))
        err = max(abs(g[0] - e[0]) / scale, abs(g[1] - e[1]) / scale)
        worst = max(worst, err)
        if err > 1e-9 or int(g[2]) != int(e[2]):
            bad += 1
            if len(examples) < 3:
                examples.append({"key": list(k), "expected": list(e), "got": list(g)})
    return {"rows": len(expected), "mismatches": bad, "extraRows": len(set(got) - set(expected)),
            "maxRelativeError": worst, "examples": examples}


checks = {}
with lock:
    live = dict(state)
for i in range(16):
    cols = list(SUBS[i])
    f = FILTERS.get(i, {})
    where = " AND ".join(f"{k[2:]} = '{v}'" for k, v in f.items()) or "TRUE"
    sel = ", ".join(cols + ["SUM(mv_usd)", "SUM(pnl_usd)", "COUNT(*)"])
    q = f"SELECT {sel} FROM ({position_risk()}) WHERE {where}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    expected = {tuple(r[:len(cols)]): r[len(cols):] for r in db.execute(q).fetchall()}
    got = {k[1:]: (r["mv_usd"], r["pnl_usd"], r["n"]) for k, r in live.items() if k[0] == i}
    snap = {tuple(r[c] for c in cols): (r["mv_usd"], r["pnl_usd"], r["n"]) for r in tables[i].to_arrow().to_pylist()}
    checks[i] = {"groupBy": cols, "filters": f, "subscriber": compare(expected, got), "snapshot": compare(expected, snap)}
result["correctness"] = checks
result["subscriptionErrors"] = dh.errors
with open(args.out, "w") as out:
    json.dump(result, out, indent=2, default=str)
print(json.dumps(result, indent=1, default=str))
dh.close()
os._exit(0)  # the ticking client's reader threads are not daemons
