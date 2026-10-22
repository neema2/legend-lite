"""Proton's ASOF JOIN for trading analytics: each trade with the quote in force when it printed,
then slippage aggregated live, against DuckDB's ASOF JOIN over the same rows.

    python proton_asof.py [--late-share 0.0] --out results/x.json

trades: an append stream (trade_id, instrument, side, qty, px, tts); quotes: a versioned_kv stream
keyed by instrument (bid, ask, qts), the join keeping the last versions per key
(SETTINGS keep_versions). Event times are explicit columns. With --late-share > 0, that share of
quotes is held back and sent 500 ms late (event time unchanged): a streaming as-of join matches
each trade against the quotes present when the trade arrives, so a late quote is never matched;
the batch join over the full history does match it. The difference is counted, not hidden.
"""

import argparse
import json
import random
import threading
import time

from proton_lab import Lab, Subscription, compare

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--instruments", type=int, default=200)
parser.add_argument("--quotes-per-s", type=int, default=1000)
parser.add_argument("--trades-per-s", type=int, default=200)
parser.add_argument("--late-share", type=float, default=0.0)
parser.add_argument("--keep-versions", type=int, default=100)
parser.add_argument("--out", required=True)
args = parser.parse_args()

lab = Lab()
c = lab.c
for s in ("trades", "quotes"):
    c.execute(f"DROP STREAM IF EXISTS {s}")
c.execute("CREATE STREAM trades (trade_id int64, instrument string, side string, qty float64, px float64, tts int64)")
c.execute("CREATE STREAM quotes (instrument string, bid float64, ask float64, qts int64) "
          "PRIMARY KEY instrument SETTINGS mode='versioned_kv'")

rng = random.Random(21)
insts = [f"A{i:03d}" for i in range(args.instruments)]
mid = {i: rng.uniform(20, 400) for i in insts}
all_quotes, all_trades = [], []
# opening quotes, before any trade
t0 = int(time.time() * 1000) - 1000
opening = [(i, round(mid[i] - 0.01, 4), round(mid[i] + 0.01, 4), t0) for i in insts]
c.execute("INSERT INTO quotes (instrument, bid, ask, qts) VALUES", opening)
all_quotes.extend(opening)

JOINED = (f"SELECT t.instrument AS instrument, t.side AS side, t.px AS px, q.bid AS bid, q.ask AS ask, "
          f"if(t.side = 'BUY', t.px - q.ask, q.bid - t.px) AS slip "
          f"FROM trades AS t ASOF JOIN quotes AS q ON t.instrument = q.instrument AND t.tts >= q.qts")
sub = Subscription(lab, f"SELECT instrument, avg(slip) AS avg_slip, count() AS n, _tp_delta FROM ({JOINED}) "
                        f"GROUP BY instrument EMIT CHANGELOG PERIODIC 100ms "
                        f"SETTINGS keep_versions = {args.keep_versions}", ["instrument"], name="slippage")
time.sleep(2)

late, sender = [], lab.client()
start = time.time()
tid = 0
n_batches = int(args.seconds * 10)
for n in range(n_batches):
    delay = start + n * 0.1 - time.time()
    if delay > 0:
        time.sleep(delay)
    now = int(time.time() * 1000)
    quotes = []
    for _ in range(args.quotes_per_s // 10):
        i = rng.choice(insts)
        mid[i] *= 1 + rng.gauss(0, 0.0005)
        spread = 0.01 + 0.02 * rng.random()
        q = (i, round(mid[i] - spread / 2, 4), round(mid[i] + spread / 2, 4), now)
        all_quotes.append(q)
        (late if rng.random() < args.late_share else quotes).append(q)
    if quotes:
        sender.execute("INSERT INTO quotes (instrument, bid, ask, qts) VALUES", quotes)
    ready = [q for q in late if q[3] <= now - 500]
    if ready:
        sender.execute("INSERT INTO quotes (instrument, bid, ask, qts) VALUES", ready)
        late = [q for q in late if q[3] > now - 500]
    trades = []
    for _ in range(args.trades_per_s // 10):
        i = rng.choice(insts)
        side = rng.choice(["BUY", "SELL"])
        tid += 1
        tr = (tid, i, side, float(rng.randint(1, 10) * 100), round(mid[i] * (1 + rng.gauss(0, 0.0003)), 4), now + 1)
        trades.append(tr)
        all_trades.append(tr)
    sender.execute("INSERT INTO trades (trade_id, instrument, side, qty, px, tts) VALUES", trades)
if late:
    sender.execute("INSERT INTO quotes (instrument, bid, ask, qts) VALUES", late)
time.sleep(5)

import duckdb  # noqa: E402

db = duckdb.connect()
db.execute("CREATE TABLE trades (trade_id BIGINT, instrument VARCHAR, side VARCHAR, qty DOUBLE, px DOUBLE, tts BIGINT)")
db.execute("CREATE TABLE quotes (instrument VARCHAR, bid DOUBLE, ask DOUBLE, qts BIGINT)")
db.executemany("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", all_trades)
db.executemany("INSERT INTO quotes VALUES (?, ?, ?, ?)", all_quotes)
# several quotes can share one millisecond for an instrument: DuckDB then picks one of them, Proton
# the last inserted; keep the last per (instrument, qts) so both mean the same quote
db.execute("CREATE TABLE q1 AS SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY instrument, qts ORDER BY rowid DESC) AS rn "
           "FROM quotes) WHERE rn = 1")
expected = db.execute("""SELECT t.instrument AS instrument, AVG(CASE WHEN t.side = 'BUY' THEN t.px - q.ask ELSE q.bid - t.px END)
  AS avg_slip, COUNT(*) AS n FROM trades t ASOF JOIN q1 q ON t.instrument = q.instrument AND t.tts >= q.qts
  GROUP BY t.instrument""").fetchall()
with lab.lock:
    got = dict(sub.state)
check = compare([{"instrument": r[0], "avg_slip": r[1], "n": r[2]} for r in expected], got, ["instrument"],
                ["avg_slip", "n"], tol=1e-9)
result = {"seconds": args.seconds, "trades": len(all_trades), "quotes": len(all_quotes), "lateShare": args.late_share,
          "keepVersions": args.keep_versions, "firstRowsMillis": sub.first_rows_ms(), "error": sub.error,
          "check": check}
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k != "check"} | {"ok": check["ok"], "mismatches": check["mismatches"],
                                                                      "rows": check["rows"], "examples": check["examples"][:2]}, default=str))
import os  # noqa: E402
os._exit(0)
