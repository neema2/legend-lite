"""Proton as a service: its data on disk, a restart in the middle of the feed, and the native log's
growth and retention.

    python proton_ops.py recovery --kill graceful|hard --container proton-disk --out x.json
    python proton_ops.py log [--retention-ms 30000] --seconds 240 --container proton-disk --out x.json

recovery: the shared join (fx_first) writes a changelog_kv stream; the feed runs at 1,000 ticks a
second and, --kill-at batches in (150: 15 s), the container is restarted (graceful: SIGTERM, up to 60 s to stop; hard:
SIGKILL). The feed holds each insert and retries it until the server answers (an upstream that
buffers); the desk grouping reconnects when its query dies. Measured: how long inserts were
refused, how long the reconnected grouping took to show a tick sent after the restart, and then,
after the feed stops, everything against DuckDB: the inputs, the shared stream (rows, and keys with
more than one row), the reconnected grouping, five fresh groupings, and a table() aggregate. If any
of it is wrong, the shared stream and its view are dropped and created again (rebuild), timed, and
checked again.

log: the same join and feed for --seconds; every 10 s, each stream's native log and historical
store on disk. With --retention-ms, every stream is created with that logstore_retention_ms (the
server's retention check, segment size and minimum to keep are lowered in the container's config
so a run of minutes can show deletion). After the feed, five fresh groupings against DuckDB: a
query that starts after retention has deleted log segments reads what it needs from the
historical store.
"""

import argparse
import json
import subprocess
import threading
import time

from proton_lab import ORACLE_RISK, Feed, Lab, compare, oracle, pct
from probe import PROBE, add_probe
from scenario import reference, schedule

parser = argparse.ArgumentParser()
parser.add_argument("what", choices=["recovery", "log"])
parser.add_argument("--kill", default="graceful", choices=["graceful", "hard"])
parser.add_argument("--container", default="proton-disk")
parser.add_argument("--seconds", type=float, default=40)
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--retention-ms", type=int, default=0)
parser.add_argument("--kill-at", type=int, default=150, help="the batch after which the container restarts")
parser.add_argument("--checkpoint-interval", type=int, default=0, help="the MV's checkpoint_interval (s); 0: Proton's")
parser.add_argument("--out", required=True)
args = parser.parse_args()

GROUPINGS = {"desk": ["desk"], "desk_trader": ["desk", "trader"], "book": ["book"], "ccy": ["ccy"],
             "sector": ["sector"]}
MEAS = "sum(mv_usd) AS mv_usd, sum(pnl_usd) AS pnl_usd, count() AS n"
STREAMS = ("books", "instruments", "closes", "positions", "prices", "fx", "position_risk_s")


def grouping_sql(cols):
    return (f"SELECT {', '.join(cols)}, {MEAS}, _tp_delta FROM position_risk_s "
            f"GROUP BY {', '.join(cols)} EMIT CHANGELOG PERIODIC 100ms")


class RetryingFeed(Feed):
    """Each insert is retried until the server takes it; refused time is recorded."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.refused = []          # (first refusal, first success after it)

    def _send(self, batch, ts):
        down = None
        while True:
            try:
                super()._send(batch, ts)   # versioned_kv inputs: a resent row is an upsert
                break
            except Exception:
                down = down or time.time()
                time.sleep(0.2)
                try:
                    self.client = self.lab.client()
                except Exception:
                    pass
        if down:
            self.refused.append((down, time.time()))


class Reconnecting:
    """A changelog grouping that reconnects when its query dies (state starts again from what the
    new query emits). Records each reconnect, the probe's latency, and errors."""

    def __init__(self, lab, sql, keys, probe=False):
        self.lab, self.sql, self.keys, self.probe = lab, sql, keys, probe
        self.state, self.errors, self.reconnects, self.lat = {}, [], [], []
        self.first = None
        self.started = time.time() * 1000
        self.on = True
        self.last_probe = 0
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while self.on:
            try:
                cl = self.lab.client()
                names = None
                for item in cl.execute_iter(self.sql, with_column_types=True):
                    if names is None:
                        names = [n for n, _ in item]
                        continue
                    recv = time.time() * 1000
                    row = dict(zip(names, item))
                    delta = row.pop("_tp_delta", 1)
                    key = tuple(row[k] for k in self.keys)
                    with self.lab.lock:
                        if self.first is None:
                            self.first = recv
                        if delta < 0:
                            if self.state.get(key) == row:
                                del self.state[key]
                        else:
                            self.state[key] = row
                            # the probe desk's mv_usd is the send time of the batch it reflects
                            # (an initial snapshot row before the feed is not: skipped)
                            if self.probe and row.get("desk") == PROBE["desk"] and 0 <= recv - row["mv_usd"] < 60000:
                                self.last_probe = row["mv_usd"]
                                self.lat.append((recv, recv - row["mv_usd"]))
            except Exception as e:
                self.errors.append((time.time(), repr(e)[:200]))
                with self.lab.lock:
                    self.state = {}
                time.sleep(0.5)
                self.reconnects.append(time.time())

    def first_rows_ms(self):
        return None if self.first is None else round(self.first - self.started, 1)


def wait_ready(lab, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            lab.client().execute("SELECT 1")
            return round(time.time() - t0, 1)
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server did not come back")


def check_against_oracle(lab, ref, final):
    db = oracle(ref, final)
    db.execute(f"CREATE VIEW r AS {ORACLE_RISK}")
    out = {}
    fresh = {name: Reconnecting(lab, grouping_sql(cols), cols) for name, cols in GROUPINGS.items()}
    time.sleep(6)
    for name, cols in GROUPINGS.items():
        exp = db.execute(f"SELECT {', '.join(cols)}, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n "
                         f"FROM r GROUP BY {', '.join(cols)}").fetchall()
        names = [d[0] for d in db.description]
        with lab.lock:
            got = dict(fresh[name].state)
        out[f"fresh_{name}"] = {**compare([dict(zip(names, r)) for r in exp], got, cols, ["mv_usd", "pnl_usd", "n"]),
                                "firstRowsMillis": fresh[name].first_rows_ms(), "errors": fresh[name].errors[:2]}
        fresh[name].on = False
    exp = db.execute("SELECT desk, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n FROM r GROUP BY desk").fetchall()
    got = {(r["desk"],): r for r in lab.query(f"SELECT desk, {MEAS} FROM table(position_risk_s) GROUP BY desk")}
    out["table_desk"] = compare([{"desk": r[0], "mv_usd": r[1], "pnl_usd": r[2], "n": r[3]} for r in exp], got,
                                ["desk"], ["mv_usd", "pnl_usd", "n"])
    out["sharedStreamRows"] = {"expected": db.execute("SELECT COUNT(*) FROM r").fetchone()[0],
                               "got": lab.query("SELECT count() AS n FROM table(position_risk_s)")[0]["n"],
                               "keysWithMoreThanOneRow": lab.query(
                                   "SELECT count() AS n FROM (SELECT book, instrument FROM table(position_risk_s) "
                                   "GROUP BY book, instrument HAVING count() > 1)")[0]["n"]}
    prices = {r["instrument"]: r["px"] for r in lab.query("SELECT instrument, px FROM table(prices)")}
    out["pricesInput"] = {"rows": len(prices), "expected": len(final["prices"]),
                          "wrong": sum(1 for i, p in final["prices"].items() if prices.get(i) != p)}
    return out, db


def disk(lab):
    uuids = {r["name"]: str(r["uuid"]) for r in lab.query("SELECT name, uuid FROM system.tables WHERE database = 'default'")}
    paths = " ".join(f"/var/lib/proton/nativelog/default/{uuids[s]}.0 /var/lib/proton/store/{uuids[s]}" for s in STREAMS)
    out = subprocess.run(["docker", "exec", args.container, "sh", "-c", f"du -sb {paths} 2>/dev/null; du -sb /var/lib/proton"],
                         capture_output=True, text=True).stdout.split("\n")
    sizes = {}
    for line in out:
        if not line.strip():
            continue
        b, p = line.split("\t")
        sizes[p] = int(b)
    by_uuid = {u: n for n, u in uuids.items()}
    res = {"total": sizes.get("/var/lib/proton")}
    for p, b in sizes.items():
        name = p.rstrip("/").split("/")[-1].replace(".0", "")
        if name in by_uuid:
            kind = "log" if "/nativelog/" in p else "store"
            res.setdefault(by_uuid[name], {})[kind] = b
    return res


ref = reference()
n_batches = int(args.seconds * 10)
batches = schedule(ref, seconds=args.seconds, ticks_per_s=args.rate)[:n_batches]
add_probe(ref)   # after the schedule, so no booking touches the probe's book
lab = Lab(container=args.container)
lab.reset(extra_views=("djoin_mv", "djoin2_mv", "gagg_mv", "g_desk_mv"),
          extra_streams=("djoin", "djoin2", "gagg", "gsrc", "dpos", "dpx", "g_desk_s", "trades", "quotes"))
retention = f", logstore_retention_ms={args.retention_ms}" if args.retention_ms else ""
lab.create(join="fx_first", target="changelog_kv", retention=retention,
           mv_settings=f"checkpoint_interval={args.checkpoint_interval}" if args.checkpoint_interval else "")
lab.load(ref)
time.sleep(2)

if args.what == "recovery":
    restart = {}

    def do_restart(_client):
        def run():
            restart["at"] = time.time()
            if args.kill == "graceful":
                subprocess.run(["docker", "restart", "-t", "60", args.container], capture_output=True)
            else:
                subprocess.run(["docker", "kill", args.container], capture_output=True)
                subprocess.run(["docker", "start", args.container], capture_output=True)
            restart["containerBack"] = time.time()
            restart["serverReadySeconds"] = wait_ready(lab)
            restart["ready"] = time.time()
        threading.Thread(target=run, daemon=True).start()

    desk = Reconnecting(lab, grouping_sql(["desk"]), ["desk"], probe=True)
    time.sleep(3)
    feed = RetryingFeed(lab, ref, batches, hooks={args.kill_at: do_restart})
    feed.start()
    feed.thread.join()
    time.sleep(10)
    t_r = restart["at"]
    after = [(t, l) for t, l in desk.lat if t / 1000 > restart["ready"]]
    before = [l for t, l in desk.lat if t / 1000 < t_r]
    lab.c = lab.client()   # the first connection died with the server
    checks, _ = check_against_oracle(lab, ref, feed.final)
    with lab.lock:
        got = dict(desk.state)
    db = oracle(ref, feed.final)
    exp = db.execute(f"SELECT desk, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, COUNT(*) AS n FROM ({ORACLE_RISK}) "
                     "GROUP BY desk").fetchall()
    checks["reconnected_desk"] = compare([{"desk": r[0], "mv_usd": r[1], "pnl_usd": r[2], "n": r[3]} for r in exp], got,
                                         ["desk"], ["mv_usd", "pnl_usd", "n"])
    rebuild = None
    if not all(v.get("ok", True) for v in checks.values() if isinstance(v, dict)):
        # the inputs are intact: drop the derived stream and its view, create them again (the new
        # view reads the keyed inputs' current state from their historical store), check again
        t_b = time.time()
        lab.c.execute("DROP VIEW position_risk_mv")
        lab.c.execute("DROP STREAM position_risk_s")
        lab.create_join(target="changelog_kv")
        expected = checks["sharedStreamRows"]["expected"]
        while time.time() - t_b < 120:
            if lab.query("SELECT count() AS n FROM table(position_risk_s)")[0]["n"] == expected:
                break
            time.sleep(0.2)
        rebuild_s = round(time.time() - t_b, 1)
        again, _ = check_against_oracle(lab, ref, feed.final)
        rebuild = {"seconds": rebuild_s, "checks": again,
                   "allCorrect": all(v.get("ok", True) for v in again.values() if isinstance(v, dict))}
    logs = subprocess.run(["docker", "logs", "--since", str(int(t_r)), args.container], capture_output=True, text=True)
    errs = [ln for ln in (logs.stdout + logs.stderr).split("\n") if "<Error>" in ln or "Exception" in ln]
    result = {"kill": args.kill, "killAtSecond": args.kill_at / 10, "checkpointInterval": args.checkpoint_interval,
              "batchesSent": feed.sent,
              "restart": {"containerSeconds": round(restart["containerBack"] - t_r, 1),
                          "serverReadySecondsAfterContainer": restart["serverReadySeconds"],
                          "insertsRefusedSeconds": [round(b - a, 1) for a, b in feed.refused]},
              "desk": {"reconnects": len(desk.reconnects), "errors": desk.errors[:3],
                       "tickToSubscriberBefore": {"p50": pct(before, 0.5), "p95": pct(before, 0.95)},
                       "firstProbeAfterReadyMillis": round(after[0][0] - restart["ready"] * 1000) if after else None,
                       "tickToSubscriberAfter": {"p50": pct([l for _, l in after], 0.5), "p95": pct([l for _, l in after], 0.95)}},
              "checks": checks, "allCorrect": all(v.get("ok", True) for v in checks.values() if isinstance(v, dict)),
              "rebuild": rebuild, "serverErrorLines": len(errs), "serverErrorSample": errs[:5]}
else:
    feed = Feed(lab, ref, batches)
    samples = [{"t": 0, **disk(lab)}]
    feed.start()
    t0 = time.time()
    while feed.thread.is_alive():
        time.sleep(10)
        samples.append({"t": round(time.time() - t0), **disk(lab)})
    time.sleep(10)
    samples.append({"t": round(time.time() - t0), **disk(lab)})
    checks, _ = check_against_oracle(lab, ref, feed.final)
    result = {"retentionMs": args.retention_ms, "seconds": args.seconds, "rate": args.rate, "batchesSent": feed.sent,
              "samples": samples, "checks": checks,
              "allCorrect": all(v.get("ok", True) for v in checks.values() if isinstance(v, dict))}

with open(args.out, "w") as f:
    json.dump(result, f, indent=2, default=str)
print(json.dumps({k: v for k, v in result.items() if k not in ("samples", "checks")}, default=str))
print(json.dumps({k: ({kk: vv for kk, vv in v.items() if kk in ("ok", "mismatches", "rows", "extraRows", "expected", "got",
                                                                 "keysWithMoreThanOneRow", "wrong")} if isinstance(v, dict) else v)
                  for k, v in result["checks"].items()}, default=str))
import os  # noqa: E402
os._exit(0)
