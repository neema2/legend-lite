"""How results leave Proton, and how much of a ClickHouse dialect it takes: HTTP streaming, a live
top-N, and the ClickHouse-compatible parser.

    python proton_delivery.py --dialect ../../core/src/main/java/com/legend/sql/dialect/ClickHouse.java --out x.json

With the shared join (changelog_kv) live and the feed at 1,000 ticks a second:
  http       a changelog grouping over HTTP on 3218 (JSONEachRow): time to first bytes, lines
             and chunks per second, the CORS header; the same query on 8123
  top_n      the ten instruments by |P&L| kept live as max_k over a nested grouping (EMIT
             PERIODIC 1s), against DuckDB after the feed stops
  distinct   unique_exact / count_distinct / uniq / count(DISTINCT) over changelog input
  dialect    every function name core's ClickHouse dialect writes, called under
             is_clickhouse_compatible=1 (native protocol) with no arguments: UNKNOWN_FUNCTION
             means the parser does not know it, an argument error means it does; the same for
             ClickHouse type names in CAST
"""

import argparse
import http.client
import json
import os
import re
import threading
import time

from proton_lab import ORACLE_RISK, Feed, Lab, Subscription, compare, oracle
from probe import add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("--dialect", required=True)
parser.add_argument("--seconds", type=float, default=40)
parser.add_argument("--out", required=True)
args = parser.parse_args()

ref = reference()
batches = schedule(ref, seconds=args.seconds, ticks_per_s=1000)
add_probe(ref)
lab = Lab()
lab.reset()
lab.create(target="changelog_kv")
lab.load(ref)
time.sleep(2)
feed = Feed(lab, ref, batches)
feed.start()
time.sleep(3)
result = {}
# a second server on other ports (proton_ops.py's on-disk container): PROTON_PORT, PROTON_HTTP_PORT, PROTON_CH_PORT
HTTP_PORT = int(os.environ.get("PROTON_HTTP_PORT", 3218))
CH_PORT = int(os.environ.get("PROTON_CH_PORT", 8123))

# --- top-N and distinct counts, live while the HTTP test runs
TOPN = ("SELECT max_k(abs(total), 10, instrument, total) AS top10 FROM "
        "(SELECT instrument, sum(pnl_usd) AS total FROM position_risk_s WHERE instrument != 'PROBE' GROUP BY instrument) "
        "EMIT PERIODIC 1s")
topn = {"last": None, "emits": 0, "error": None}


def run_topn():
    try:
        for item in lab.client().execute_iter(TOPN):
            topn["last"], topn["emits"] = item[0], topn["emits"] + 1
    except Exception as e:
        topn["error"] = repr(e)[:300]


threading.Thread(target=run_topn, daemon=True).start()
DISTINCT = {
    "unique_exact": "unique_exact(instrument)",
    "count_distinct": "count_distinct(instrument)",
    "uniq": "uniq(instrument)",
    "uniq_exact": "uniq_exact(instrument)",
    "count_DISTINCT": "count(DISTINCT instrument)",
}
dsubs = {k: Subscription(lab, f"SELECT desk, {f} AS v, _tp_delta FROM position_risk_s GROUP BY desk "
                              f"EMIT CHANGELOG PERIODIC 100ms", ["desk"], name=k) for k, f in DISTINCT.items()}

# --- HTTP streaming
Q = ("SELECT desk, sum(mv_usd) AS mv_usd, count() AS n, _tp_delta FROM position_risk_s GROUP BY desk "
     "EMIT CHANGELOG PERIODIC 100ms")


def http_stream(port, path, seconds=10, headers=None):
    conn = http.client.HTTPConnection("localhost", port, timeout=seconds + 10)
    t0 = time.time()
    conn.request("POST", path, body=Q.encode(), headers=headers or {})
    resp = conn.getresponse()
    out = {"status": resp.status, "headers": {k.lower(): v for k, v in resp.getheaders()
                                              if k.lower() in ("transfer-encoding", "access-control-allow-origin",
                                                               "content-type", "x-clickhouse-format",
                                                               "x-timeplus-query-id", "x-proton-query-id")}}
    first, lines, reads, sample = None, 0, 0, []
    buf = b""
    resp.fp.raw._sock.settimeout(1.0) if hasattr(resp.fp, "raw") else None
    while time.time() - t0 < seconds:
        try:
            chunk = resp.read1(65536)
        except Exception:
            continue
        if not chunk:
            break
        reads += 1
        if first is None:
            first = time.time()
        buf += chunk
        *done, buf = buf.split(b"\n")
        lines += len(done)
        if len(sample) < 3:
            sample += [d.decode()[:200] for d in done[:3 - len(sample)]]
    conn.close()
    out.update({"firstBytesMillis": None if first is None else round((first - t0) * 1000),
                "linesPerSecond": round(lines / seconds, 1), "readsPerSecond": round(reads / seconds, 1),
                "sample": sample})
    return out


result["http"] = {
    "port3218": http_stream(HTTP_PORT, "/?default_format=JSONEachRow&add_http_cors_header=1"),
    "port8123": http_stream(CH_PORT, "/?default_format=JSONEachRow", seconds=5),
}
print("http", json.dumps(result["http"], default=str)[:800])

feed.thread.join()
time.sleep(8)
db = oracle(ref, feed.final)
db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
exp = db.execute("SELECT instrument, SUM(pnl_usd) AS v FROM r WHERE instrument != 'PROBE' GROUP BY instrument "
                 "ORDER BY ABS(SUM(pnl_usd)) DESC LIMIT 10").fetchall()
got = [(t[1], t[2]) for t in (topn["last"] or [])]
result["top_n"] = {"sql": TOPN, "emits": topn["emits"], "error": topn["error"],
                   "sameInstrumentsInOrder": [g[0] for g in got] == [e[0] for e in exp],
                   "maxRelativeError": max((abs(g[1] - e[1]) / max(1.0, abs(e[1])) for g, e in zip(got, exp)), default=None),
                   "expected": exp[:3], "got": got[:3]}
expd = db.execute("SELECT desk, COUNT(DISTINCT instrument) AS v FROM r GROUP BY desk").fetchall()
result["distinct"] = {}
for k, s in dsubs.items():
    o = {"error": s.error}
    if not s.error:
        with lab.lock:
            st = dict(s.state)
        o.update(compare([{"desk": d, "v": v} for d, v in expd], st, ["desk"], ["v"]))
    result["distinct"][k] = o

# --- the ClickHouse-compatible parser
src = open(args.dialect).read()
calls = set(re.findall(r'"([A-Za-z_][A-Za-z0-9_]*)\(', src))
names = set(re.findall(r'"([a-z][A-Za-z0-9_]{2,})"', src))
candidates = sorted(calls | {n for n in names if re.search(r"[A-Z]", n) or n in calls})
cl = lab.client()


def probe(sql, compat):
    try:
        cl.execute(sql, settings={"is_clickhouse_compatible": 1} if compat else {})
        return "ok"
    except Exception as e:
        m = re.search(r"Code: (\d+)", str(e))
        return int(m.group(1)) if m else repr(e)[:80]


UNKNOWN = {46, 63}   # UNKNOWN_FUNCTION, UNKNOWN_AGGREGATE_FUNCTION
census = {"known": [], "unknown": [], "other": {}}
for n in candidates:
    code = probe(f"SELECT {n}()", True)
    if code in UNKNOWN:
        census["unknown"].append(n)
    elif code == "ok" or isinstance(code, int):
        census["known"].append(n)
    else:
        census["other"][n] = code
plain = {n: probe(f"SELECT {n}()", False) for n in candidates}
census["knownOnlyWithCompat"] = [n for n in census["known"] if plain[n] in UNKNOWN]
TYPES = ["Nullable(String)", "Array(Int64)", "Map(String, Float64)", "DateTime64(3)", "Decimal(18, 4)", "String",
         "Float64", "Int64", "UInt8", "Date", "Date32", "LowCardinality(String)", "Tuple(Int64, String)", "Bool", "UUID"]
census["types"] = {t: {"compat": probe(f"SELECT CAST(NULL AS Nullable(String)) AS x, CAST(1 AS {t}) AS y"
                                       if "Nullable" not in t else f"SELECT CAST(NULL AS {t})", True),
                       "plain": probe(f"SELECT CAST(1 AS {t})" if "Nullable" not in t else f"SELECT CAST(NULL AS {t})", False)}
                   for t in TYPES}
census["counts"] = {"candidates": len(candidates), "known": len(census["known"]), "unknown": len(census["unknown"]),
                    "knownOnlyWithCompat": len(census["knownOnlyWithCompat"])}
# a streaming query written in ClickHouse spelling, on the streaming port
census["streamingWithCompat"] = http_stream(HTTP_PORT, "/?default_format=JSONEachRow&is_clickhouse_compatible=1", seconds=3)
result["dialect"] = census

with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print("top_n", json.dumps({k: v for k, v in result["top_n"].items() if k != "sql"}, default=str))
print("distinct", json.dumps({k: (v.get("error") or "")[:100] or (v["ok"], v["mismatches"]) for k, v in result["distinct"].items()}))
print("dialect", json.dumps(census["counts"]), "unknown:", ", ".join(census["unknown"]))
print("types", json.dumps(census["types"]))
os._exit(0)
