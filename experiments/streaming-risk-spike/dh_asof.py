"""Deephaven's as-of join for trading analytics (proton_asof.py's scenario): each trade with the quote
in force when it printed, slippage averaged live per instrument, against DuckDB's ASOF JOIN.

    python dh_asof.py [--late-share 0.0] --out results/x.json

trades and quotes are append-only input tables (quotes keep their history; aj needs it). With
--late-share > 0, that share of quotes is held back and sent 500 ms late with its event time
unchanged: the question is whether trades already joined are matched again. Event times are
microseconds and unique per quote (a batch's quotes are now*1000 + their sequence; its trades
now*1000 + 999), so no two quotes of an instrument tie and the oracle needs no tie-break.
"""

import argparse
import json
import random
import time

import pyarrow as pa

from dh_lab import DhLab, Subscription, start_server, stop_server
from proton_lab import compare

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=float, default=30)
parser.add_argument("--instruments", type=int, default=200)
parser.add_argument("--quotes-per-s", type=int, default=1000)
parser.add_argument("--trades-per-s", type=int, default=200)
parser.add_argument("--late-share", type=float, default=0.0)
parser.add_argument("--out", required=True)
args = parser.parse_args()

proc, up_s = start_server(log_path=args.out + ".server.log")
lab = DhLab(proc.pid)
s = lab.s
TS = pa.schema([("trade_id", pa.int64()), ("instrument", pa.string()), ("side", pa.string()), ("qty", pa.float64()),
                ("px", pa.float64()), ("tts", pa.int64())])
QS = pa.schema([("instrument", pa.string()), ("bid", pa.float64()), ("ask", pa.float64()), ("qts", pa.int64())])
trades = s.input_table(schema=TS)
quotes = s.input_table(schema=QS)


def add(it, schema, rows):
    if rows:
        it.add(s.import_table(pa.Table.from_pylist(rows, schema=schema)))


rng = random.Random(21)
insts = [f"A{i:03d}" for i in range(args.instruments)]
mid = {i: rng.uniform(20, 400) for i in insts}
all_quotes, all_trades = [], []
t0 = (int(time.time() * 1000) - 1000) * 1000
opening = [{"instrument": i, "bid": round(mid[i] - 0.01, 4), "ask": round(mid[i] + 0.01, 4), "qts": t0} for i in insts]
add(quotes, QS, opening)
all_quotes.extend(opening)

from pydeephaven import agg  # noqa: E402

joined = (trades.aj(quotes, on=["instrument", "tts >= qts"], joins=["bid", "ask"])
          .update_view(["slip = side == `BUY` ? px - ask : bid - px"]))
slippage = joined.agg_by([agg.avg("avg_slip = slip"), agg.count_("n")], by=["instrument"])
sub = Subscription(lab, slippage, ["instrument"], name="slippage")
time.sleep(2)

late, tid = [], 0
start = time.time()
for n in range(int(args.seconds * 10)):
    delay = start + n * 0.1 - time.time()
    if delay > 0:
        time.sleep(delay)
    now = int(time.time() * 1000)
    qs = []
    for seq in range(args.quotes_per_s // 10):
        i = rng.choice(insts)
        mid[i] *= 1 + rng.gauss(0, 0.0005)
        spread = 0.01 + 0.02 * rng.random()
        q = {"instrument": i, "bid": round(mid[i] - spread / 2, 4), "ask": round(mid[i] + spread / 2, 4),
             "qts": now * 1000 + seq}
        all_quotes.append(q)
        (late if rng.random() < args.late_share else qs).append(q)
    add(quotes, QS, qs)
    ready = [q for q in late if q["qts"] <= (now - 500) * 1000]
    if ready:
        add(quotes, QS, ready)
        late = [q for q in late if q["qts"] > (now - 500) * 1000]
    ts = []
    for _ in range(args.trades_per_s // 10):
        i = rng.choice(insts)
        tid += 1
        tr = {"trade_id": tid, "instrument": i, "side": rng.choice(["BUY", "SELL"]), "qty": float(rng.randint(1, 10) * 100),
              "px": round(mid[i] * (1 + rng.gauss(0, 0.0003)), 4), "tts": now * 1000 + 999}
        ts.append(tr)
        all_trades.append(tr)
    add(trades, TS, ts)
add(quotes, QS, late)
time.sleep(5)

import duckdb  # noqa: E402

db = duckdb.connect()
db.execute("CREATE TABLE trades (trade_id BIGINT, instrument VARCHAR, side VARCHAR, qty DOUBLE, px DOUBLE, tts BIGINT)")
db.execute("CREATE TABLE quotes (instrument VARCHAR, bid DOUBLE, ask DOUBLE, qts BIGINT)")
db.executemany("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", [tuple(t.values()) for t in all_trades])
db.executemany("INSERT INTO quotes VALUES (?, ?, ?, ?)", [tuple(q.values()) for q in all_quotes])
# no two quotes of an instrument share a time (above); the dedupe is a no-op kept for the record
db.execute("CREATE TABLE q1 AS SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY instrument, qts ORDER BY rowid DESC) "
           "AS rn FROM quotes) WHERE rn = 1")
expected = db.execute("""SELECT t.instrument, AVG(CASE WHEN t.side = 'BUY' THEN t.px - q.ask ELSE q.bid - t.px END),
  COUNT(*) FROM trades t ASOF JOIN q1 q ON t.instrument = q.instrument AND t.tts >= q.qts GROUP BY t.instrument""").fetchall()
exp = [{"instrument": r[0], "avg_slip": r[1], "n": r[2]} for r in expected]
with lab.lock:
    got = dict(sub.state)
snap = {(r["instrument"],): r for r in lab.snapshot(slippage)}
result = {"seconds": args.seconds, "trades": len(all_trades), "quotes": len(all_quotes), "lateShare": args.late_share,
          "firstRowsMillis": sub.first_rows_ms(), "clientErrors": sub.errors[:2],
          "subscriber": compare(exp, got, ["instrument"], ["avg_slip", "n"], tol=1e-9),
          "snapshot": compare(exp, snap, ["instrument"], ["avg_slip", "n"], tol=1e-9)}
sub.stop()
stop_server(proc)
with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k not in ("subscriber", "snapshot")}
                 | {"subscriberOk": result["subscriber"]["ok"], "subscriberMismatches": result["subscriber"]["mismatches"],
                    "snapshotOk": result["snapshot"]["ok"], "snapshotMismatches": result["snapshot"]["mismatches"],
                    "examples": result["snapshot"]["examples"][:2]}, default=str))
import os  # noqa: E402
os._exit(0)
