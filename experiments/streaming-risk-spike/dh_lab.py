"""Shared pieces of the Deephaven homework (dh_correctness.py, dh_asof.py, dh_shapes.py, dh_perf.py,
dh_scale_check.py, dh_ops.py): the server process, the risk inputs as keyed input tables and the
join as natural joins, loading and feeding, subscribers, CPU and memory. The DuckDB oracle and
the comparison are proton_lab's.

The server is dh_server.py in a process of its own (the JVM runs inside it), started here so a
script can restart it.
"""

import os
import socket
import subprocess
import sys
import threading
import time

import pyarrow as pa

from engines import batch_rows, process_usage, reference_rows
from probe import PROBE, stamp

HERE = os.path.dirname(os.path.abspath(__file__))
S, D, L = pa.string(), pa.float64(), pa.int64()
SCHEMAS = {
    "books": (pa.schema([("book", S), ("trader", S), ("desk", S)]), ["book"]),
    "instruments": (pa.schema([("instrument", S), ("sector", S), ("ccy", S), ("beta", D), ("multiplier", D)]),
                    ["instrument"]),
    "closes": (pa.schema([("instrument", S), ("close_px", D)]), ["instrument"]),
    "positions": (pa.schema([("book", S), ("instrument", S), ("qty", D)]), ["book", "instrument"]),
    "prices": (pa.schema([("instrument", S), ("px", D), ("ts", L)]), ["instrument"]),
    "fx": (pa.schema([("ccy", S), ("usd_rate", D), ("ts", L)]), ["ccy"]),
}
RISK_FORMULAS = ["root = `ROOT`",
                 "mv_usd = qty * px * multiplier * usd_rate",
                 "pnl_usd = qty * (px - close_px) * multiplier * usd_rate",
                 "beta_delta_usd = mv_usd * beta"]


def start_server(cycle_ms=100, heap="4g", log_path=None, timeout=120):
    """dh_server.py in a new process; returns (Popen, seconds until a session connects)."""
    env = dict(os.environ, DH_CYCLE_MS=str(cycle_ms), DH_HEAP=heap)
    log = open(log_path or os.devnull, "ab")
    t0 = time.time()
    p = subprocess.Popen([sys.executable, os.path.join(HERE, "dh_server.py")], cwd=HERE, env=env,
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    while time.time() - t0 < timeout:
        if p.poll() is not None:
            raise RuntimeError(f"dh_server.py exited ({p.returncode})")
        try:
            with socket.create_connection(("localhost", 10000), timeout=0.5):
                pass
            s = session()
            s.close()
            return p, round(time.time() - t0, 1)
        except Exception:
            time.sleep(0.3)
    raise RuntimeError("server did not come up")


def stop_server(p, hard=False, timeout=60):
    """SIGTERM (graceful) or SIGKILL; returns the seconds until the process was gone."""
    t0 = time.time()
    p.kill() if hard else p.terminate()
    try:
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    return round(time.time() - t0, 1)


def session():
    from pydeephaven import Session
    return Session(host="localhost", port=10000,
                   auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler", auth_token="spike-psk")


class DhLab:
    def __init__(self, pid):
        self.pid = pid
        self.s = session()
        self.lock = threading.Lock()
        self.inputs = {}
        self.send_lock = threading.Lock()

    def create(self):
        """The six keyed inputs and the per-position risk table (each position joined to its
        instrument, book, price, FX rate and close)."""
        for name, (schema, keys) in SCHEMAS.items():
            self.inputs[name] = self.s.input_table(schema=schema, key_cols=keys)
        i = self.inputs
        self.risk = (i["positions"]
                     .natural_join(i["instruments"], on=["instrument"], joins=["sector", "ccy", "beta", "multiplier"])
                     .natural_join(i["books"], on=["book"], joins=["trader", "desk"])
                     .natural_join(i["prices"], on=["instrument"], joins=["px", "px_ts = ts"])
                     .natural_join(i["fx"], on=["ccy"], joins=["usd_rate", "fx_ts = ts"])
                     .natural_join(i["closes"], on=["instrument"], joins=["close_px"])
                     .update_view(RISK_FORMULAS))
        return self.risk

    def add(self, table, rows):
        if rows:
            t = self.s.import_table(pa.Table.from_pylist(rows, schema=SCHEMAS[table][0]))
            with self.send_lock:
                self.inputs[table].add(t)

    def delete(self, table, key_rows):
        schema, keys = SCHEMAS[table]
        t = self.s.import_table(pa.Table.from_pylist(key_rows, schema=pa.schema([schema.field(k) for k in keys])))
        with self.send_lock:
            self.inputs[table].delete(t)

    def load(self, ref):
        for table, rows in reference_rows(ref).items():
            self.add(table, rows)

    def snapshot(self, table):
        return table.to_arrow().to_pylist()

    def usage(self):
        return process_usage(self.pid)


class Feed:
    """The schedule in real time (a batch every 100 ms), the probe stamped into every batch; keeps
    the final state the oracle needs; hooks may change reference data mid-feed. With retry, a
    batch that fails is retried (through `reconnect`) until it goes in."""

    def __init__(self, lab, ref, batches, hooks=None, retry=False, reconnect=None):
        self.lab, self.batches, self.hooks = lab, batches, hooks or {}
        self.final = {"positions": dict(ref.positions), "prices": dict(ref.prices), "fx": dict(ref.fx)}
        self.on = threading.Event()
        self.sent = 0
        self.retry, self.reconnect = retry, reconnect
        self.refused = []

    def _send(self, batch, ts):
        for table, rows in batch_rows(batch, ts).items():
            self.lab.add(table, rows)

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
                down = None
                while True:
                    try:
                        self._send(batch, ts)
                        break
                    except Exception:
                        if not self.retry:
                            raise
                        down = down or time.time()
                        time.sleep(0.2)
                        if self.reconnect:
                            try:
                                self.reconnect()
                            except Exception:
                                pass
                if down:
                    self.refused.append((down, time.time()))
                for k in self.final:
                    self.final[k].update(batch[k])
                self.sent = n + 1
                if n in self.hooks:
                    self.hooks[n](self.lab)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()


class Subscription:
    """A ticking subscription to a table, kept as keyed state: the client keeps the whole table
    (update.update.current, read whole on every update; see engines.Deephaven.subscribe).
    Records the time to first rows, the probe's latency (rows of the probe desk whose mv_usd is a
    batch's send time), each update's arrival, and errors. The Python ticking client (42.7)
    sometimes fails a subscription with "Chunks have inconsistent sizes" while the server's table
    is right: the subscription is then opened again (the new one starts from the whole table) and
    the error counted, so a client failure is told apart from a wrong result."""

    def __init__(self, lab, table, keys, probe=False, name=None):
        import pydeephaven_ticking.table_listener as tl
        self.lab, self.table, self.keys, self.probe, self.name = lab, table, keys, probe, name
        self.state, self.first, self.errors, self.lat, self.updates = {}, None, [], [], 0
        self.last_update = None
        self.measuring = False
        self.started = time.time() * 1000
        self._tl = tl
        self.resubscribes = 0
        self.on = True
        self.handle = tl.listen(table, self._on_update, self._on_error)
        self.handle.start()

    def _on_update(self, update):
        tl = self._tl
        recv = time.time() * 1000
        cur = update.update.current
        whole = tl._first_or_default(tl._make_generator(cur, cur.get_row_sequence(), None, max(1, cur.num_rows)))
        rows = []
        if whole:
            names = list(whole)
            values = [whole[n].to_pylist() for n in names]
            rows = [dict(zip(names, v)) for v in zip(*values)]
        with self.lab.lock:
            old = self.state
            self.state = {tuple(r[k] for k in self.keys): r for r in rows}
            self.updates += 1
            self.last_update = recv
            if self.first is None and rows:
                self.first = recv
            if self.probe and self.measuring:
                k = next((key for key, r in self.state.items() if r.get("desk") == PROBE["desk"]), None)
                if k is not None:
                    r = self.state[k]
                    if (old.get(k) or {}).get("mv_usd") != r["mv_usd"] and 0 <= recv - r["mv_usd"] < 600000:
                        self.lat.append(recv - r["mv_usd"])

    def _on_error(self, e):
        self.errors.append({"at": time.time(), "error": repr(e)[:300]})

        def again():
            time.sleep(0.2)
            if self.on:
                self.resubscribes += 1
                self.handle = self._tl.listen(self.table, self._on_update, self._on_error)
                self.handle.start()
        threading.Thread(target=again, daemon=True).start()

    def first_rows_ms(self):
        return None if self.first is None else round(self.first - self.started, 1)

    def stop(self):
        self.on = False
        try:
            self.handle.stop()
        except Exception:
            pass
