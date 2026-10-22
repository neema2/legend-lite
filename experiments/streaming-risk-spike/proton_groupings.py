"""The same 16 groupings as dynamic_groupings.py and dynamic_deephaven.py, on Proton.

    python proton_groupings.py --mode perquery|shared [--measures full|linear] --out results/x.json

perquery: each grouping is its own streaming query over the whole join (instruments to FX first,
          then positions by instrument: the order Proton keeps up with), so each computes the join.
shared:   one materialized view runs the join once and writes each position's risk into a keyed
          stream (versioned_kv by book and instrument, or changelog_kv with --target changelog);
          each grouping is a streaming aggregation over that stream.

Every grouping emits a changelog every 100 ms (EMIT CHANGELOG PERIODIC 100ms). Latency from the
probe (probe.py): the Probe desk's mv_usd in the desk grouping is the send time of the batch it
reflects. Phases with 1, 4 and 16 groupings live under the feed; per phase: CPU and memory of the
container, the probe's tick-to-subscriber latency, the time each new grouping took to its first
rows; at the end every grouping's subscriber state against DuckDB.
"""

import argparse
import json
import os
import threading
import time

import engines
from dynamic_groupings_spec import DIMS, FILTERS, SUBS
from probe import PROBE, add_probe, stamp
from scenario import reference, schedule
from views import TABLES_ANSI, TABLES_PROTON, position_risk

parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, choices=["perquery", "shared"])
parser.add_argument("--target", default="versioned", choices=["versioned", "changelog"])
parser.add_argument("--measures", default="full", choices=["full", "linear"])
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--window", type=float, default=10)
parser.add_argument("--drain", type=float, default=5, help="seconds to wait after the feed stops, before the check")
parser.add_argument("--out", required=True)
args = parser.parse_args()

JOIN = """SELECT p.book AS book, p.instrument AS instrument, b.desk AS desk, b.trader AS trader,
  x.sector AS sector, x.ccy AS ccy, 'ROOT' AS root,
  p.qty * q.px * x.multiplier * x.usd_rate AS mv_usd,
  p.qty * (q.px - c.close_px) * x.multiplier * x.usd_rate AS pnl_usd,
  p.qty * q.px * x.multiplier * x.usd_rate * x.beta AS beta_delta_usd,
  q.ts AS px_ts, x.fx_ts AS fx_ts
FROM positions AS p
JOIN (SELECT i.instrument AS instrument, i.sector AS sector, i.ccy AS ccy, i.beta AS beta,
             i.multiplier AS multiplier, f.usd_rate AS usd_rate, f.ts AS fx_ts
      FROM instruments AS i JOIN fx AS f ON i.ccy = f.ccy) AS x ON p.instrument = x.instrument
JOIN books AS b ON p.book = b.book
JOIN prices AS q ON p.instrument = q.instrument
JOIN closes AS c ON p.instrument = c.instrument"""

MEAS = "sum(mv_usd) AS mv_usd, sum(pnl_usd) AS pnl_usd, sum(beta_delta_usd) AS beta_delta_usd, count() AS n"
if args.measures == "full":
    MEAS += ", max(px_ts) AS last_px_ts, max(fx_ts) AS last_fx_ts"


def where(i):
    f = FILTERS.get(i, {})
    return " AND ".join(f"{k[2:]} = '{v}'" for k, v in f.items()) or "true"


def grouping_sql(i):
    cols = list(SUBS[i]) or ["root"]
    source = f"({JOIN})" if args.mode == "perquery" else "position_risk_s"
    return (f"SELECT {', '.join(cols)}, {MEAS}, _tp_delta FROM {source} WHERE {where(i)} "
            f"GROUP BY {', '.join(cols)} EMIT CHANGELOG PERIODIC 100ms"), cols


ref = reference()
batches = schedule(ref, seconds=300, ticks_per_s=args.rate)
add_probe(ref)

px = engines.Proton()
c = px.sender
result = {"mode": args.mode, "target": args.target if args.mode == "shared" else None,
          "measures": args.measures, "rate": args.rate, "drainSeconds": args.drain}
for name in ("position_risk_mv",):
    c.execute(f"DROP VIEW IF EXISTS {name}")
for name in ("position_risk_s", "books", "instruments", "closes", "positions", "prices", "fx"):
    c.execute(f"DROP STREAM IF EXISTS {name}")
for ddl in TABLES_PROTON:
    c.execute(ddl)
if args.mode == "shared":
    mode = "versioned_kv" if args.target == "versioned" else "changelog_kv"
    c.execute(f"""CREATE STREAM position_risk_s (book string, instrument string, desk string, trader string,
      sector string, ccy string, root string, mv_usd float64, pnl_usd float64, beta_delta_usd float64,
      px_ts int64, fx_ts int64) PRIMARY KEY (book, instrument) SETTINGS mode='{mode}'""")
    if args.target == "versioned":
        c.execute(f"CREATE MATERIALIZED VIEW position_risk_mv INTO position_risk_s AS {JOIN}")
    else:
        sel = JOIN.replace("q.ts AS px_ts, x.fx_ts AS fx_ts", "q.ts AS px_ts, x.fx_ts AS fx_ts, _tp_delta", 1)
        c.execute(f"CREATE MATERIALIZED VIEW position_risk_mv INTO position_risk_s AS {sel} EMIT CHANGELOG")
t0 = time.time()
for table, rows in engines.reference_rows(ref).items():
    px._insert(table, rows)
result["loadSeconds"] = round(time.time() - t0, 1)

lock = threading.Lock()
state = {}          # (grouping, *group values) -> row
first_rows = {}
watch = {"lat": [], "measuring": False}
errors = []


def subscribe(i):
    sql, cols = grouping_sql(i)

    def run():
        try:
            cl = px.client_class(host=px.host, port=px.port)
            names = None
            for item in cl.execute_iter(sql, with_column_types=True):
                if names is None:
                    names = [n for n, _ in item]
                    continue
                recv = time.time() * 1000
                row = dict(zip(names, item))
                delta = row.pop("_tp_delta")
                key = (i, *(row[col] for col in cols))
                with lock:
                    first_rows.setdefault(i, recv)
                    if delta < 0:
                        old = state.get(key)
                        if old is not None and old["mv_usd"] == row["mv_usd"] and old["n"] == row["n"]:
                            del state[key]
                    else:
                        state[key] = row
                        if i == 0 and row.get("desk") == PROBE["desk"] and watch["measuring"]:
                            watch["lat"].append(recv - row["mv_usd"])
        except Exception as e:  # a failed grouping is reported, never silent
            errors.append({"grouping": i, "error": repr(e)[:400]})

    threading.Thread(target=run, daemon=True).start()


final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
sending = threading.Event()
sending.set()


def sender():
    t = time.time()
    for n, batch in enumerate(batches):
        if not sending.is_set():
            return
        delay = t + n * 0.1 - time.time()
        if delay > 0:
            time.sleep(delay)
        ts = int(time.time() * 1000)
        stamp(batch, ts)
        px.send(batch, ts)
        for k in final:
            final[k].update(batch[k])


th = threading.Thread(target=sender, daemon=True)
th.start()
time.sleep(5)


def activate(ids):
    lat = {}
    for i in ids:
        start = time.time() * 1000
        subscribe(i)
        while i not in first_rows and time.time() * 1000 - start < 60000 and not any(e["grouping"] == i for e in errors):
            time.sleep(0.002)
        lat[i] = round(first_rows[i] - start, 1) if i in first_rows else None
    return lat


def window():
    c0, _ = px.usage()
    with lock:
        watch["lat"].clear()
        watch["measuring"] = True
    time.sleep(args.window)
    with lock:
        watch["measuring"] = False
        xs = sorted(watch["lat"])
        groups = len(state)
    c1, mem = px.usage()
    pick = lambda p: round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None
    return {"engineCpuCores": round((c1 - c0) / args.window, 2), "engineMemoryMB": round(mem / 2**20),
            "groupRowsLive": groups, "tickToSubscriberMillis": {"p50": pick(0.5), "p95": pick(0.95), "n": len(xs)}}


phases = []
c0, _ = px.usage()
time.sleep(args.window)
c1, mem = px.usage()
phases.append({"active": 0, "engineCpuCores": round((c1 - c0) / args.window, 2), "engineMemoryMB": round(mem / 2**20)})
active = 0
for target in (1, 4, 16):
    lat = activate(range(active, target))
    active = target
    phases.append({"active": active, "activationMillis": lat, **window()})
result["phases"] = phases
sending.clear()
th.join()
time.sleep(args.drain)

import duckdb  # noqa: E402

db = duckdb.connect()
db.execute(TABLES_ANSI)
db.executemany("INSERT INTO books VALUES (?, ?, ?)", [(b["book"], b["trader"], b["desk"]) for b in ref.books])
db.executemany("INSERT INTO instruments VALUES (?, ?, ?, ?, ?)",
               [(i["instrument"], i["sector"], i["ccy"], i["beta"], i["multiplier"]) for i in ref.instruments])
db.executemany("INSERT INTO closes VALUES (?, ?)", [(cl["instrument"], cl["close_px"]) for cl in ref.closes])
db.executemany("INSERT INTO positions VALUES (?, ?, ?)", [(b, i, q) for (b, i), q in final["positions"].items()])
db.executemany("INSERT INTO prices VALUES (?, ?, 0)", list(final["prices"].items()))
db.executemany("INSERT INTO fx VALUES (?, ?, 0)", list(final["fx"].items()))
checks = {}
with lock:
    live = dict(state)
for i in range(16):
    cols = list(SUBS[i])
    sel = ", ".join(cols + ["SUM(mv_usd)", "SUM(pnl_usd)", "COUNT(*)"])
    q = f"SELECT {sel} FROM ({position_risk()}) WHERE {where(i)}" + (f" GROUP BY {', '.join(cols)}" if cols else "")
    expected = {tuple(r[:len(cols)]): r[len(cols):] for r in db.execute(q).fetchall()}
    got = {(k[1:] if cols else ()): (r["mv_usd"], r["pnl_usd"], r["n"]) for k, r in live.items() if k[0] == i}
    bad, worst = 0, 0.0
    for k, e in expected.items():
        g = got.get(k)
        err = 1.0 if g is None else max(abs(g[0] - e[0]), abs(g[1] - e[1])) / max(1.0, abs(e[0]))
        worst = max(worst, err)
        if err > 1e-9 or (g is not None and int(g[2]) != int(e[2])):
            bad += 1
    checks[i] = {"groupBy": cols, "rows": len(expected), "mismatches": bad,
                 "extraRows": len(set(got) - set(expected)), "maxRelativeError": worst}
result["correctness"] = checks
result["allCorrect"] = all(v["mismatches"] == 0 and v["extraRows"] == 0 for v in checks.values())
result["errors"] = errors
with open(args.out, "w") as out:
    json.dump(result, out, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k != "correctness"}, default=str))
os._exit(0)
