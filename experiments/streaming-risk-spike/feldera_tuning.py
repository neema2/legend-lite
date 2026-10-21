"""Feldera variants on the risk scenario, one per run: what each program shape and runtime setting
costs at the same feed.

    python feldera_tuning.py --variant static16 [--workers 4] [--storage on|off] [--profiler on|off]
                             [--min-batch N] [--seconds 30] --out results/x.json

Variants (the 16 groupings of dynamic_groupings_spec.py throughout):
  static16       each grouping its own materialized view over position_risk (compiled in)
  static16_leaf  the same views over a leaf (positions summed to all five dimensions first)
  dyn_direct     groupings as data: position_risk joined with a subscriptions table (no key)
  dyn_leaf       the same over the leaf
  dyn_bucket     the leaf copied into one copy per worker (UNION ALL, a constant bucket per copy),
                 subscriptions keyed by sub_id mod workers: the join has a key, so the workers
                 share it

Measured over the window, after a warm-up: Feldera's own lag (the age of the desk grouping's newest
price, read from the view, polled every 200 ms), the subscriber's tick-to-update latency on that
grouping, the pipeline process's CPU and memory (its own statistics), the largest input backlog,
and every grouping against DuckDB once the feed stops.
"""

import argparse
import http.client
import json
import threading
import time
import urllib.parse

import engines
from dynamic_groupings_spec import DIMS, FILTERS, SUBS
from scenario import reference, schedule
from views import TABLES_ANSI, position_risk

parser = argparse.ArgumentParser()
parser.add_argument("--variant", required=True,
                    choices=["static16", "static16_leaf", "dyn_direct", "dyn_leaf", "dyn_bucket"])
parser.add_argument("--workers", type=int, default=4)
parser.add_argument("--storage", default="on", choices=["on", "off"])
parser.add_argument("--profiler", default="on", choices=["on", "off"])
parser.add_argument("--min-batch", type=int, default=0)
parser.add_argument("--egress", default="all", choices=["all", "probe"],
                    help="subscribe to every grouping's view, or to the probe (desk) view only")
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--fx-rate", type=int, default=5)
parser.add_argument("--groupings", type=int, default=16, help="the first N of the 16 groupings")
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--out", required=True)
args = parser.parse_args()

MEAS = ("SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, SUM(beta_delta_usd) AS beta_delta_usd, "
        "COUNT(*) AS n, MAX(px_ts) AS last_px_ts, MAX(fx_ts) AS last_fx_ts")
REAGG = ("SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, SUM(beta_delta_usd) AS beta_delta_usd, "
         "SUM(n) AS n, MAX(last_px_ts) AS last_px_ts, MAX(last_fx_ts) AS last_fx_ts")
LEAF = f"CREATE VIEW leaf AS SELECT {', '.join(DIMS)}, {MEAS} FROM position_risk GROUP BY {', '.join(DIMS)};\n"


def where(i):
    f = FILTERS.get(i, {})
    return " AND ".join(f"{k[2:]} = '{v}'" for k, v in f.items()) or "TRUE"


def static_view(i, source, measures):
    cols = list(SUBS[i])
    if cols:
        return (f"CREATE MATERIALIZED VIEW g{i} AS SELECT {', '.join(cols)}, {measures} FROM {source} "
                f"WHERE {where(i)} GROUP BY {', '.join(cols)};\n")
    return f"CREATE MATERIALIZED VIEW g{i} AS SELECT 'ROOT' AS root, {measures} FROM {source} WHERE {where(i)};\n"


KEYS = [f"CASE WHEN s.by_{d} THEN r.{d} END" for d in DIMS]
SUBS_TABLE = """CREATE TABLE subscriptions (sub_id INT NOT NULL PRIMARY KEY,
  by_desk BOOLEAN NOT NULL, by_trader BOOLEAN NOT NULL, by_book BOOLEAN NOT NULL,
  by_sector BOOLEAN NOT NULL, by_ccy BOOLEAN NOT NULL, f_desk VARCHAR, f_sector VARCHAR);
"""
FILT = "(s.f_desk IS NULL OR r.desk = s.f_desk) AND (s.f_sector IS NULL OR r.sector = s.f_sector)"


def dyn_view(source, measures, join):
    m = measures.replace("SUM(", "SUM(r.").replace("MAX(", "MAX(r.").replace("COUNT(*)", "COUNT(*)")
    return (f"CREATE MATERIALIZED VIEW live_groups AS SELECT s.sub_id, "
            f"{', '.join(f'{k} AS {d}' for k, d in zip(KEYS, DIMS))}, {m} "
            f"FROM {source} r {join} WHERE {FILT} GROUP BY s.sub_id, {', '.join(KEYS)};\n")


base = TABLES_ANSI + f"CREATE VIEW position_risk AS {position_risk()};\n"
if args.variant == "static16":
    program = base + "".join(static_view(i, "position_risk", MEAS) for i in range(args.groupings))
elif args.variant == "static16_leaf":
    program = base + LEAF + "".join(static_view(i, "leaf", REAGG) for i in range(args.groupings))
elif args.variant == "dyn_direct":
    program = base + SUBS_TABLE + dyn_view("position_risk", MEAS, "CROSS JOIN subscriptions s")
elif args.variant == "dyn_leaf":
    program = base + LEAF + SUBS_TABLE + dyn_view("leaf", REAGG, "CROSS JOIN subscriptions s")
else:  # dyn_bucket
    copies = " UNION ALL ".join(f"SELECT leaf.*, {b} AS b FROM leaf" for b in range(args.workers))
    program = (base + LEAF + SUBS_TABLE + f"CREATE VIEW leaf_b AS {copies};\n"
               f"CREATE VIEW subs_b AS SELECT subscriptions.*, MOD(sub_id, {args.workers}) AS b FROM subscriptions;\n"
               + dyn_view("leaf_b", REAGG, "JOIN subs_b s ON r.b = s.b"))
dynamic = args.variant.startswith("dyn")

ref = reference()
batches = schedule(ref, seconds=args.seconds + 60, ticks_per_s=args.rate, fx_ticks_per_s=args.fx_rate)
f = engines.Feldera(pipeline=f"tune_{args.variant}_{args.groupings}", workers=args.workers)
runtime = {"workers": args.workers, "cpu_profiler": args.profiler == "on", "min_batch_size_records": args.min_batch}
if args.storage == "off":
    runtime["storage"] = None

result = {"variant": args.variant, "workers": args.workers, "storage": args.storage, "profiler": args.profiler,
          "minBatch": args.min_batch, "rate": args.rate, "egress": args.egress, "fxRate": args.fx_rate,
          "groupings": args.groupings}
start = time.time()
f._teardown()
f._req("PUT", f"/v0/pipelines/{f.p}", json.dumps({"name": f.p, "program_code": program, "runtime_config": runtime}))
f._wait("program_status", ("Success",))
result["compileSeconds"] = round(time.time() - start, 1)
result["runtimeConfig"] = json.loads(f._req("GET", f"/v0/pipelines/{f.p}"))["runtime_config"]
f._req("POST", f"/v0/pipelines/{f.p}/start")
f._wait("deployment_status", ("Running",), 300)

lock = threading.Lock()
probe = {"pending": [], "next": 0, "lat": [], "measuring": False}
egress_bytes = [0]


def egress(view, is_probe):
    conn = http.client.HTTPConnection("localhost", 8080, timeout=None)
    conn.request("POST", f"/v0/pipelines/{f.p}/egress/{view}?format=json&backpressure=true")
    resp = conn.getresponse()
    while not f.stop:
        line = resp.readline()
        if not line:
            return
        egress_bytes[0] += len(line)
        if not is_probe:
            continue
        recv = time.time() * 1000
        data = json.loads(line).get("json_data") or []
        newest = max((d["insert"]["last_px_ts"] or 0 for d in data if "insert" in d
                      and (not dynamic or d["insert"]["sub_id"] == 0)), default=None)
        with lock:
            p = probe["pending"]
            while probe["measuring"] and newest and probe["next"] < len(p) and p[probe["next"]] <= newest:
                probe["lat"].append(recv - p[probe["next"]])
                probe["next"] += 1


views = ["live_groups"] if dynamic else ([f"g{i}" for i in range(args.groupings)] if args.egress == "all" else ["g0"])
for v in views:
    threading.Thread(target=egress, args=(v, v in ("live_groups", "g0")), daemon=True).start()
time.sleep(1)
for table, rows in engines.reference_rows(ref).items():
    f._ingest(table, rows)
if dynamic:
    for i in range(args.groupings):
        row = {"sub_id": i, **{f"by_{d}": d in SUBS[i] for d in DIMS}, "f_desk": None, "f_sector": None}
        row.update(FILTERS.get(i, {}))
        f._ingest("subscriptions", [row])

final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
sending = threading.Event()
sending.set()


def sender():
    t0 = time.time()
    for n, batch in enumerate(batches):
        if not sending.is_set():
            return
        delay = t0 + n * 0.1 - time.time()
        if delay > 0:
            time.sleep(delay)
        ts = int(time.time() * 1000)
        with lock:
            if batch["prices"] and probe["measuring"]:
                probe["pending"].append(ts)
        f.send(batch, ts)
        for k in final:
            final[k].update(batch[k])


th = threading.Thread(target=sender, daemon=True)
th.start()
time.sleep(5)  # warm-up

age_sql = urllib.parse.quote("SELECT MAX(last_px_ts) AS t FROM " +
                             ("live_groups WHERE sub_id = 0" if dynamic else "g0"))
stats = lambda: json.loads(f._req("GET", f"/v0/pipelines/{f.p}/stats"))["global_metrics"]
s0 = stats()
b0 = egress_bytes[0]
with lock:
    probe["measuring"] = True
ages, backlog = [], []
end = time.time() + args.seconds
while time.time() < end:
    rows = f._req("GET", f"/v0/pipelines/{f.p}/query?format=json&sql={age_sql}").decode().splitlines()
    t = json.loads(rows[0])["t"] if rows else None
    if t:
        ages.append(time.time() * 1000 - t)
    backlog.append(stats()["buffered_input_records"])
    time.sleep(0.2)
s1 = stats()
with lock:
    probe["measuring"] = False
    lat = sorted(probe["lat"])
sending.clear()
th.join()


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None


secs = args.seconds
result["window"] = {
    "internalAgeMillis": {"p50": pct(ages, 0.5), "p95": pct(ages, 0.95), "max": pct(ages, 1.0)},
    "tickToSubscriberMillis": {"p50": pct(lat, 0.5), "p95": pct(lat, 0.95), "n": len(lat)},
    "pipelineCpuCores": round((s1["cpu_msecs"] - s0["cpu_msecs"]) / 1000 / secs, 2),
    "pipelineRssMB": round(s1["rss_bytes"] / 2**20),
    "maxBufferedInput": max(backlog) if backlog else None,
    "egressMBPerSecond": round((egress_bytes[0] - b0) / secs / 2**20, 2),
}

# drain, then every grouping against DuckDB
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
bad, worst = 0, 0.0
for i in range(args.groupings):
    cols = list(SUBS[i])
    sel = ", ".join(cols + ["SUM(mv_usd)", "SUM(pnl_usd)", "COUNT(*)"])
    q = f"SELECT {sel} FROM ({position_risk()}) WHERE {where(i)}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    expected = {tuple(r[:len(cols)]): r[len(cols):] for r in db.execute(q).fetchall()}
    src = f"live_groups WHERE sub_id = {i}" if dynamic else f"g{i}"
    text = f._req("GET", f"/v0/pipelines/{f.p}/query?format=json&sql={urllib.parse.quote('SELECT * FROM ' + src)}")
    got = {}
    for line in text.decode().splitlines():
        r = json.loads(line)
        got[tuple(r[c] for c in cols)] = (r["mv_usd"], r["pnl_usd"], r["n"])
    for k, e in expected.items():
        g = got.get(k)
        err = 1.0 if g is None else max(abs(g[0] - e[0]), abs(g[1] - e[1])) / max(1.0, abs(e[0]))
        worst = max(worst, err)
        if err > 1e-9 or (g is not None and int(g[2]) != int(e[2])):
            bad += 1
    bad += len(set(got) - set(expected))
result["correct"] = bad == 0
result["mismatches"] = bad
result["maxRelativeError"] = worst
f.close()
f._teardown()
with open(args.out, "w") as out:
    json.dump(result, out, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k != "runtimeConfig"}, default=str))
