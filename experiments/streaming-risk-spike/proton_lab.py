"""Shared pieces of the Proton experiments (proton_correctness.py, proton_shapes.py, proton_perf.py,
proton_ops.py): the risk streams and the shared join, loading and feeding, changelog subscribers,
the DuckDB oracle, and the container's CPU and memory.
"""

import threading
import time

import engines
from probe import PROBE, stamp
from views import TABLES_ANSI

JOIN_FX_FIRST = """SELECT p.book AS book, p.instrument AS instrument, b.desk AS desk, b.trader AS trader,
  x.sector AS sector, x.ccy AS ccy, 'ROOT' AS root, p.qty AS qty, q.px AS px, x.beta AS beta,
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

JOIN_FLAT = """SELECT p.book AS book, p.instrument AS instrument, b.desk AS desk, b.trader AS trader,
  i.sector AS sector, i.ccy AS ccy, 'ROOT' AS root, p.qty AS qty, q.px AS px, i.beta AS beta,
  p.qty * q.px * i.multiplier * f.usd_rate AS mv_usd,
  p.qty * (q.px - c.close_px) * i.multiplier * f.usd_rate AS pnl_usd,
  p.qty * q.px * i.multiplier * f.usd_rate * i.beta AS beta_delta_usd,
  q.ts AS px_ts, f.ts AS fx_ts
FROM positions AS p
JOIN instruments AS i ON p.instrument = i.instrument
JOIN books AS b ON p.book = b.book
JOIN prices AS q ON p.instrument = q.instrument
JOIN fx AS f ON i.ccy = f.ccy
JOIN closes AS c ON p.instrument = c.instrument"""

RISK_COLUMNS = ("book string, instrument string, desk string, trader string, sector string, ccy string, "
                "root string, qty float64, px float64, beta float64, mv_usd float64, pnl_usd float64, "
                "beta_delta_usd float64, px_ts int64, fx_ts int64")

# the position_risk select in DuckDB, for the oracle (the same columns as the joins above)
ORACLE_RISK = """SELECT p.book, p.instrument, b.desk, b.trader, i.sector, i.ccy, 'ROOT' AS root, p.qty, q.px, i.beta,
  p.qty * q.px * i.multiplier * f.usd_rate AS mv_usd,
  p.qty * (q.px - c.close_px) * i.multiplier * f.usd_rate AS pnl_usd,
  p.qty * q.px * i.multiplier * f.usd_rate * i.beta AS beta_delta_usd
FROM positions p JOIN instruments i ON p.instrument = i.instrument JOIN books b ON p.book = b.book
JOIN prices q ON p.instrument = q.instrument JOIN fx f ON i.ccy = f.ccy JOIN closes c ON p.instrument = c.instrument"""


def streams_ddl(positions_mode="versioned_kv", retention=""):
    """The six inputs. positions may be changelog_kv (so a position can be deleted)."""
    s = f" {retention}" if retention else ""
    return [
        f"CREATE STREAM books (book string, trader string, desk string) PRIMARY KEY book SETTINGS mode='versioned_kv'{s}",
        "CREATE STREAM instruments (instrument string, sector string, ccy string, beta float64, multiplier float64) "
        f"PRIMARY KEY instrument SETTINGS mode='versioned_kv'{s}",
        f"CREATE STREAM closes (instrument string, close_px float64) PRIMARY KEY instrument SETTINGS mode='versioned_kv'{s}",
        "CREATE STREAM positions (book string, instrument string, qty float64) PRIMARY KEY (book, instrument) "
        f"SETTINGS mode='{positions_mode}'{s}",
        f"CREATE STREAM prices (instrument string, px float64, ts int64) PRIMARY KEY instrument SETTINGS mode='versioned_kv'{s}",
        f"CREATE STREAM fx (ccy string, usd_rate float64, ts int64) PRIMARY KEY ccy SETTINGS mode='versioned_kv'{s}",
    ]


class Lab:
    def __init__(self, container="proton"):
        self.px = engines.Proton(container=container)
        self.c = self.px.sender
        self.lock = threading.Lock()
        self.errors = []

    def reset(self, extra_views=(), extra_streams=()):
        for v in ("position_risk_mv", *extra_views):
            self.c.execute(f"DROP VIEW IF EXISTS {v}")
        for s in ("position_risk_s", *extra_streams, "books", "instruments", "closes", "positions", "prices", "fx"):
            self.c.execute(f"DROP STREAM IF EXISTS {s}")

    def create(self, join="fx_first", target="versioned_kv", positions_mode="versioned_kv", retention=""):
        for ddl in streams_ddl(positions_mode, retention):
            self.c.execute(ddl)
        sql = JOIN_FX_FIRST if join == "fx_first" else JOIN_FLAT
        s = f" {retention}" if retention else ""
        self.c.execute(f"CREATE STREAM position_risk_s ({RISK_COLUMNS}) PRIMARY KEY (book, instrument) "
                       f"SETTINGS mode='{target}'{s}")
        if target == "versioned_kv":
            self.c.execute(f"CREATE MATERIALIZED VIEW position_risk_mv INTO position_risk_s AS {sql}")
        else:
            sel = sql.replace("AS fx_ts\nFROM positions", "AS fx_ts, _tp_delta\nFROM positions", 1)
            self.c.execute(f"CREATE MATERIALIZED VIEW position_risk_mv INTO position_risk_s AS {sel} EMIT CHANGELOG")

    def load(self, ref):
        for table, rows in engines.reference_rows(ref).items():
            self.px._insert(table, rows)

    def usage(self):
        return self.px.usage()

    def query(self, sql):
        rows, types = self.c.execute(sql, with_column_types=True)
        names = [n for n, _ in types]
        return [dict(zip(names, r)) for r in rows]

    def client(self):
        return self.px.client_class(host=self.px.host, port=self.px.port)


class Feed:
    """Sends the schedule in real time (a batch every 100 ms), the probe stamped into every batch;
    keeps the final state the oracle needs; hooks may change reference data mid-feed."""

    def __init__(self, lab, ref, batches, hooks=None):
        self.lab, self.batches, self.hooks = lab, batches, hooks or {}
        self.final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
        self.on = threading.Event()
        self.sent = 0
        self.client = lab.client()

    def _send(self, batch, ts):
        for table, rows in engines.batch_rows(batch, ts).items():
            if rows:
                cols = list(rows[0])
                self.client.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES",
                                    [tuple(r[c] for c in cols) for r in rows])

    def start(self):
        self.on.set()

        def run():
            t0 = time.time()
            for n, batch in enumerate(self.batches):
                if not self.on.is_set():
                    return
                delay = t0 + n * 0.1 - time.time()
                if delay > 0:
                    time.sleep(delay)
                ts = int(time.time() * 1000)
                stamp(batch, ts)
                self._send(batch, ts)
                for k in self.final:
                    self.final[k].update(batch[k])
                self.sent = n + 1
                if n in self.hooks:
                    self.hooks[n](self.client)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self):
        self.on.clear()
        self.thread.join()


class Subscription:
    """A streaming query with changelog output (it must select _tp_delta), kept as keyed state.
    Records the time to first rows, the probe's latency (rows whose `desk` is the probe desk and
    whose mv_usd is the batch's send time), and any error."""

    def __init__(self, lab, sql, keys, probe=False, name=None):
        self.lab, self.sql, self.keys, self.probe = lab, sql, keys, probe
        self.name = name or sql[:60]
        self.state, self.first, self.error, self.lat, self.rows = {}, None, None, [], 0
        self.measuring = False
        self.started = time.time() * 1000
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
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
                    self.rows += 1
                    if self.first is None:
                        self.first = recv
                    if delta < 0:
                        if self.state.get(key) == row:
                            del self.state[key]
                    else:
                        self.state[key] = row
                        if self.probe and self.measuring and row.get("desk") == PROBE["desk"]:
                            self.lat.append(recv - row["mv_usd"])
        except Exception as e:
            self.error = repr(e)[:500]

    def first_rows_ms(self):
        return None if self.first is None else round(self.first - self.started, 1)


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None


def oracle(ref, final, extra_books=None, extra_instruments=None):
    """DuckDB with the final inputs; returns the connection."""
    import duckdb
    db = duckdb.connect()
    db.execute(TABLES_ANSI)
    books = {b["book"]: b for b in ref.books}
    books.update(extra_books or {})
    insts = {i["instrument"]: i for i in ref.instruments}
    insts.update(extra_instruments or {})
    db.executemany("INSERT INTO books VALUES (?, ?, ?)", [(b["book"], b["trader"], b["desk"]) for b in books.values()])
    db.executemany("INSERT INTO instruments VALUES (?, ?, ?, ?, ?)",
                   [(i["instrument"], i["sector"], i["ccy"], i["beta"], i["multiplier"]) for i in insts.values()])
    db.executemany("INSERT INTO closes VALUES (?, ?)", [(c["instrument"], c["close_px"]) for c in ref.closes])
    db.executemany("INSERT INTO positions VALUES (?, ?, ?)", [(b, i, q) for (b, i), q in final["positions"].items()])
    db.executemany("INSERT INTO prices VALUES (?, ?, 0)", list(final["prices"].items()))
    db.executemany("INSERT INTO fx VALUES (?, ?, 0)", list(final["fx"].items()))
    return db


def compare(expected_rows, got_state, keys, values, tol=1e-9):
    """expected_rows: dicts from DuckDB; got_state: {key tuple: row}. Values compared relative to
    max(1, |expected of the first value|) (P&L nets long against short); counts exactly."""
    exp = {tuple(r[k] for k in keys): r for r in expected_rows}
    bad, worst, examples = 0, 0.0, []
    for k, e in exp.items():
        g = got_state.get(k)
        if g is None:
            bad += 1
            examples.append({"key": list(k), "missing": True})
            continue
        scale = max(1.0, abs(float(e[values[0]] or 0)))
        for v in values:
            ev, gv = e[v], g.get(v)
            if ev is None and gv is None:
                continue
            if ev is None or gv is None:
                bad += 1
                examples.append({"key": list(k), "column": v, "expected": ev, "got": gv})
                break
            err = abs(float(gv) - float(ev)) / scale
            worst = max(worst, err)
            if err > tol:
                bad += 1
                if len(examples) < 3:
                    examples.append({"key": list(k), "column": v, "expected": ev, "got": gv})
                break
    extra = [list(k) for k in got_state if k not in exp]
    return {"rows": len(exp), "mismatches": bad, "extraRows": len(extra), "maxRelativeError": worst,
            "examples": examples[:3], "ok": bad == 0 and not extra}
