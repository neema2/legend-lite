"""Groupings as data, on Feldera: one program compiled up front with every dimension in it; a
grouping runs only while a row of the `subscriptions` table asks for it.

    python dynamic_groupings.py --out results/feldera-dynamic.json

A subscription row says which dimensions to group by (by_desk, by_trader, by_book, by_sector,
by_ccy) and optional filters (f_desk, f_sector). The view live_groups joins every position's risk
with every subscription, keeps the rows the filters pass, and groups by the subscription id plus,
per dimension, the value when the subscription groups by it and NULL when it does not. Inserting a
row is an incremental change like any other: its first answer is computed from the state the
pipeline already holds, and it ticks from then on. Deleting the row retracts its rows.

Measured under the same feed as run.py (1,000 price ticks a second): the time from inserting a
subscription to its first rows; CPU with 0, 1, 4 and 16 subscriptions active; tick to update for
one subscription; retraction on delete; every active subscription against DuckDB at the end.
"""

import argparse
import json
import threading
import time
import urllib.parse

import engines
from scenario import reference, schedule
from dynamic_groupings_spec import DIMS, FILTERS, SUBS
from views import TABLES_ANSI, position_risk

MEASURES = ("SUM(r.mv_usd) AS mv_usd, SUM(r.pnl_usd) AS pnl_usd, SUM(r.beta_delta_usd) AS beta_delta_usd, "
            "COUNT(*) AS n, MAX(r.px_ts) AS last_px_ts, MAX(r.fx_ts) AS last_fx_ts")
KEYS = [f"CASE WHEN s.by_{d} THEN r.{d} END" for d in DIMS]

SUBSCRIPTIONS = """
CREATE TABLE subscriptions (sub_id INT NOT NULL PRIMARY KEY,
  by_desk BOOLEAN NOT NULL, by_trader BOOLEAN NOT NULL, by_book BOOLEAN NOT NULL,
  by_sector BOOLEAN NOT NULL, by_ccy BOOLEAN NOT NULL,
  f_desk VARCHAR, f_sector VARCHAR);
"""
FILTER = "WHERE (s.f_desk IS NULL OR r.desk = s.f_desk) AND (s.f_sector IS NULL OR r.sector = s.f_sector)"
GROUPS = f"GROUP BY s.sub_id, {', '.join(KEYS)}"
SELECT_KEYS = ", ".join(f"{k} AS {d}" for k, d in zip(KEYS, DIMS))

# direct: every position's risk joined with every subscription
DIRECT = TABLES_ANSI + SUBSCRIPTIONS + f"""
CREATE VIEW position_risk AS {position_risk()};
CREATE MATERIALIZED VIEW live_groups AS
SELECT s.sub_id, {SELECT_KEYS}, {MEASURES}
FROM position_risk r CROSS JOIN subscriptions s {FILTER} {GROUPS};
"""

# leaf: positions aggregated first to the finest grain of every dimension (about 2,400 rows),
# then the leaf joined with every subscription and rolled up; every measure re-aggregates
# (sums of sums, a sum of counts, a max of maxes)
LEAF = TABLES_ANSI + SUBSCRIPTIONS + f"""
CREATE VIEW position_risk AS {position_risk()};
CREATE VIEW leaf AS SELECT {", ".join(DIMS)}, {MEASURES.replace("r.", "")}
FROM position_risk GROUP BY {", ".join(DIMS)};
CREATE MATERIALIZED VIEW live_groups AS
SELECT s.sub_id, {SELECT_KEYS},
  SUM(r.mv_usd) AS mv_usd, SUM(r.pnl_usd) AS pnl_usd, SUM(r.beta_delta_usd) AS beta_delta_usd,
  SUM(r.n) AS n, MAX(r.last_px_ts) AS last_px_ts, MAX(r.last_fx_ts) AS last_fx_ts
FROM leaf r CROSS JOIN subscriptions s {FILTER} {GROUPS};
"""

def sub_row(i):
    row = {"sub_id": i, **{f"by_{d}": d in SUBS[i] for d in DIMS}, "f_desk": None, "f_sector": None}
    row.update(FILTERS.get(i, {}))
    return row


parser = argparse.ArgumentParser()
parser.add_argument("--out", required=True)
parser.add_argument("--shape", default="leaf", choices=["direct", "leaf"])
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--window", type=float, default=10, help="seconds per CPU window")
args = parser.parse_args()

PROGRAM = {"direct": DIRECT, "leaf": LEAF}[args.shape]
ref = reference()
batches = schedule(ref, seconds=200, ticks_per_s=args.rate)
f = engines.Feldera(pipeline=f"risk_dyn_{args.shape}")

lock = threading.Lock()
state = {}              # (sub_id, *dims) -> row
first_rows = {}         # sub_id -> recv ms of its first rows
gone = {}               # sub_id -> recv ms when its last row was retracted
watch = {"sub": 0, "pending": [], "next": 0, "lat": []}   # tick -> update for one subscription


def key(r):
    return (r["sub_id"], *(r[d] for d in DIMS))


def on_change(view, inserts, deletes, recv):
    with lock:
        for r in deletes:
            state.pop(key(r), None)
        newest = None
        for r in inserts:
            state[key(r)] = r
            first_rows.setdefault(r["sub_id"], recv)
            if r["sub_id"] == watch["sub"] and r.get("last_px_ts"):
                newest = max(newest or 0, r["last_px_ts"])
        for r in deletes:
            sid = r["sub_id"]
            if sid in gone and gone[sid] is None and not any(k[0] == sid for k in state):
                gone[sid] = recv
        p = watch["pending"]
        while newest and watch["next"] < len(p) and p[watch["next"]][0] <= newest:
            watch["lat"].append(recv - p[watch["next"]][0])
            watch["next"] += 1


result = {"shape": args.shape, "program": PROGRAM, "rate": args.rate}
start = time.time()
f._teardown()
f._req("PUT", f"/v0/pipelines/{f.p}", json.dumps({"name": f.p, "program_code": PROGRAM,
                                                   "runtime_config": {"workers": f.workers}}))
f._wait("program_status", ("Success",))
result["compileSeconds"] = round(time.time() - start, 1)
f._req("POST", f"/v0/pipelines/{f.p}/start")
f._wait("deployment_status", ("Running",), 300)
threading.Thread(target=f._egress, args=("live_groups", on_change), daemon=True).start()
time.sleep(1)
for table, rows in engines.reference_rows(ref).items():
    f._ingest(table, rows)

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
                watch["pending"].append((ts, None))
        f.send(batch, ts)
        for k in final:
            final[k].update(batch[k])


th = threading.Thread(target=sender, daemon=True)
th.start()
time.sleep(5)


def cpu_window():
    """CPU over the window; meanwhile, Feldera's own lag: the age of subscription 0's newest price,
    read straight from the view (no egress, no subscriber), and the input it has not yet processed."""
    q = urllib.parse.quote("SELECT MAX(last_px_ts) AS t FROM live_groups WHERE sub_id = 0")
    ages, backlog = [], []
    s0 = json.loads(f._req("GET", f"/v0/pipelines/{f.p}/stats"))["global_metrics"]
    c0, _ = f.usage()
    end = time.time() + args.window
    while time.time() < end:
        rows = f._req("GET", f"/v0/pipelines/{f.p}/query?format=json&sql={q}").decode().splitlines()
        t = json.loads(rows[0])["t"] if rows else None
        if t:
            ages.append(time.time() * 1000 - t)
        backlog.append(json.loads(f._req("GET", f"/v0/pipelines/{f.p}/stats"))["global_metrics"]["buffered_input_records"])
        time.sleep(0.2)
    c1, mem = f.usage()
    s1 = json.loads(f._req("GET", f"/v0/pipelines/{f.p}/stats"))["global_metrics"]
    ages.sort()
    pick = lambda p: round(ages[min(len(ages) - 1, int(p * len(ages)))]) if ages else None
    return {"engineCpuCores": round((c1 - c0) / args.window, 2),
            "pipelineRssMB": round(s1["rss_bytes"] / 2**20),
            "pipelineCpuCores": round((s1["cpu_msecs"] - s0["cpu_msecs"]) / 1000 / args.window, 2),
            "internalAgeMillis": {"p50": pick(0.5), "p95": pick(0.95)},
            "maxBufferedInput": max(backlog) if backlog else None}


def activate(ids):
    lat = {}
    for i in ids:
        t = time.time() * 1000
        f._ingest("subscriptions", [sub_row(i)])
        while i not in first_rows and time.time() * 1000 - t < 30000:
            time.sleep(0.002)
        lat[i] = round(first_rows[i] - t, 1) if i in first_rows else None
    return lat


phases = []
phases.append({"active": 0, **cpu_window()})
active = 0
for target in (1, 4, 16):
    lat = activate(range(active, target))
    active = target
    with lock:
        watch["lat"].clear()
    window = cpu_window()
    with lock:
        lat_watch = sorted(watch["lat"])
    pick = lambda p: round(lat_watch[min(len(lat_watch) - 1, int(p * len(lat_watch)))], 1) if lat_watch else None
    with lock:
        groups = len(state)
    phases.append({"active": active, "activationMillis": lat, **window, "groupRowsLive": groups,
                   "tickToSubscriberMillis": {"p50": pick(0.5), "p95": pick(0.95), "n": len(lat_watch)}})
result["phases"] = phases

# a delete retracts the subscription's rows
victim = 15
with lock:
    gone[victim] = None
t = time.time() * 1000
f._req("POST", f"/v0/pipelines/{f.p}/ingress/subscriptions?format=json&update_format=insert_delete",
       json.dumps({"delete": sub_row(victim)}))
while gone.get(victim) is None and time.time() * 1000 - t < 30000:
    time.sleep(0.002)
result["deleteRetractMillis"] = round(gone[victim] - t, 1) if gone.get(victim) else None

sending.clear()
th.join()
time.sleep(3)

# every remaining subscription against DuckDB over the final inputs
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
    if i == victim:
        checks[i] = {"rowsLeft": sum(1 for k in live if k[0] == i)}
        continue
    s = sub_row(i)
    cols = [d for d in DIMS if s[f"by_{d}"]]
    where = " AND ".join(f"{c[2:]} = '{v}'" for c, v in (("f_desk", s["f_desk"]), ("f_sector", s["f_sector"])) if v) or "TRUE"
    sel = ", ".join(cols + ["SUM(mv_usd)", "SUM(pnl_usd)", "COUNT(*)"])
    q = f"SELECT {sel} FROM ({position_risk()}) WHERE {where}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    expected = {tuple(r[:len(cols)]): r[len(cols):] for r in db.execute(q).fetchall()}
    got = {tuple(r[d] for d in cols): (r["mv_usd"], r["pnl_usd"], r["n"]) for k, r in live.items() if k[0] == i}
    checks[i] = {"groupBy": cols, "filters": FILTERS.get(i, {}), **compare(expected, got)}
result["correctness"] = checks
f.close()
f._teardown()
with open(args.out, "w") as out:
    json.dump(result, out, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k != "program"}, indent=1, default=str))
