"""One adapter per engine, the same five operations each:

  setup(ref, on_change)  create the inputs and the live views, subscribe, load the reference data
  send(batch, ts)        upsert one batch of prices, FX rates and positions
  snapshot(view)         the view's rows now, read with a bounded query
  ad_hoc(on_change)      a new live grouping (by sector) asked for while running: time to first rows
  usage()                (cpu seconds, memory bytes) of the engine's process or container

on_change(view, inserts, deletes, recv_ms) is called for every change set a subscription delivers.
"""

import http.client
import json
import threading
import time
import urllib.parse
import urllib.request

from views import TABLES_ANSI, TABLES_PROTON, VIEWS, aggregate, position_risk, position_risk_fx_first


def now_ms():
    return time.time() * 1000


def container_usage(name):
    """CPU seconds and memory bytes of a Docker container, from its cgroup (v1, cgroupfs driver)."""
    import subprocess
    cid = subprocess.check_output(["docker", "inspect", "-f", "{{.Id}}", name], text=True).strip()
    with open(f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage") as f:
        cpu = int(f.read()) / 1e9
    with open(f"/sys/fs/cgroup/memory/docker/{cid}/memory.usage_in_bytes") as f:
        mem = int(f.read())
    return cpu, mem


def process_usage(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().split(") ")[1].split()
    cpu = (int(fields[11]) + int(fields[12])) / 100
    with open(f"/proc/{pid}/status") as f:
        rss = next(int(l.split()[1]) * 1024 for l in f if l.startswith("VmRSS"))
    return cpu, rss


def reference_rows(ref):
    return {
        "books": ref.books,
        "instruments": ref.instruments,
        "closes": ref.closes,
        "positions": [{"book": b, "instrument": i, "qty": q} for (b, i), q in ref.positions.items()],
        "prices": [{"instrument": i, "px": p, "ts": 0} for i, p in ref.prices.items()],
        "fx": [{"ccy": c, "usd_rate": r, "ts": 0} for c, r in ref.fx.items()],
    }


def batch_rows(batch, ts):
    return {
        "prices": [{"instrument": i, "px": p, "ts": ts} for i, p in batch["prices"].items()],
        "fx": [{"ccy": c, "usd_rate": r, "ts": ts} for c, r in batch["fx"].items()],
        "positions": [{"book": b, "instrument": i, "qty": q} for (b, i), q in batch["positions"].items()],
    }


# ---------------------------------------------------------------------------------------------
class Feldera:
    """Feldera (MIT, DBSP): the program is SQL, compiled to a native pipeline; views are pushed
    as insert/delete change sets on an HTTP egress stream."""

    name = "feldera"

    def __init__(self, base="http://localhost:8080", pipeline="risk", workers=4, container="feldera"):
        self.base = base
        self.host, self.port = urllib.parse.urlparse(base).hostname, urllib.parse.urlparse(base).port
        self.p = pipeline
        self.workers = workers
        self.container = container
        self.stop = False

    def _req(self, method, path, body=None, timeout=900):
        data = body.encode() if isinstance(body, str) else body
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()

    def _status(self):
        return json.loads(self._req("GET", f"/v0/pipelines/{self.p}"))

    def _wait(self, key, want, seconds=900):
        end = time.time() + seconds
        while time.time() < end:
            s = self._status().get(key)
            if s in want:
                return s
            if key == "program_status" and isinstance(s, dict) or s in ("SqlError", "RustError", "SystemError"):
                raise RuntimeError(f"feldera {key}: {json.dumps(s)[:2000]}")
            time.sleep(0.5)
        raise TimeoutError(f"feldera {key} never reached {want}")

    def program(self):
        sql = TABLES_ANSI + "\nCREATE MATERIALIZED VIEW position_risk AS " + position_risk() + ";\n"
        for v in VIEWS:
            sql += f"CREATE MATERIALIZED VIEW {v} AS {aggregate(v, 'position_risk')};\n"
        return sql

    def _teardown(self):
        try:
            self._req("POST", f"/v0/pipelines/{self.p}/stop?force=true")
            self._wait("deployment_status", ("Stopped",), 120)
            self._req("POST", f"/v0/pipelines/{self.p}/clear")
            self._wait("storage_status", ("Cleared",), 120)
            self._req("DELETE", f"/v0/pipelines/{self.p}")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

    def setup(self, ref, on_change, views=tuple(VIEWS)):
        self._teardown()
        t = {}
        start = time.time()
        self._req("PUT", f"/v0/pipelines/{self.p}", json.dumps({
            "name": self.p, "program_code": self.program(),
            "runtime_config": {"workers": self.workers}}))
        self._wait("program_status", ("Success",))
        t["compileSeconds"] = round(time.time() - start, 1)
        start = time.time()
        self._req("POST", f"/v0/pipelines/{self.p}/start")
        self._wait("deployment_status", ("Running",), 300)
        t["startSeconds"] = round(time.time() - start, 1)
        for v in views:
            threading.Thread(target=self._egress, args=(v, on_change), daemon=True).start()
        time.sleep(1)  # the egress streams are open before any row arrives
        start = time.time()
        for table, rows in reference_rows(ref).items():
            self._ingest(table, rows)
        t["loadSeconds"] = round(time.time() - start, 1)
        return t

    def _egress(self, view, on_change):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=None)
        conn.request("POST", f"/v0/pipelines/{self.p}/egress/{view}?format=json&backpressure=true")
        resp = conn.getresponse()
        while not self.stop:
            line = resp.readline()
            if not line:
                break
            recv = now_ms()
            data = json.loads(line).get("json_data")
            if data:
                on_change(view, [d["insert"] for d in data if "insert" in d],
                          [d["delete"] for d in data if "delete" in d], recv)

    def _ingest(self, table, rows, chunk=5000):
        for i in range(0, len(rows), chunk):
            body = "\n".join(json.dumps({"insert": r}) for r in rows[i:i + chunk])
            self._req("POST", f"/v0/pipelines/{self.p}/ingress/{table}?format=json&update_format=insert_delete", body)

    def send(self, batch, ts):
        for table, rows in batch_rows(batch, ts).items():
            if rows:
                self._ingest(table, rows)

    def snapshot(self, view):
        q = urllib.parse.quote(f"SELECT * FROM {view}")
        text = self._req("GET", f"/v0/pipelines/{self.p}/query?format=json&sql={q}").decode()
        return [json.loads(l) for l in text.splitlines() if l.strip()]

    def ad_hoc(self, on_change):
        """A new grouping is a new program: Feldera's ad-hoc SQL answers a snapshot at once, but a live
        view needs the program recompiled and the pipeline restarted (measured by setup's compile)."""
        start = time.time()
        sql = urllib.parse.quote("SELECT sector, SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd FROM position_risk GROUP BY sector")
        rows = self._req("GET", f"/v0/pipelines/{self.p}/query?format=json&sql={sql}").decode().splitlines()
        return {"kind": "snapshot (ad-hoc SQL over a materialized view; not live)",
                "firstRowsMillis": round((time.time() - start) * 1000), "rows": len(rows)}

    def usage(self):
        return container_usage(self.container)

    def close(self):
        self.stop = True


# ---------------------------------------------------------------------------------------------
class Proton:
    """Timeplus Proton (Apache-2.0, a ClickHouse fork): versioned key-value streams; a live view is
    a streaming query with EMIT CHANGELOG, which pushes -1/+1 rows per changed group."""

    name = "proton"

    def __init__(self, host="localhost", port=8463, container="proton", shape="flat",
                 emit="EMIT CHANGELOG PERIODIC 100ms"):
        from proton_driver import client
        self.join = {"flat": position_risk, "fx_first": position_risk_fx_first}[shape]
        self.emit = emit  # without PERIODIC, a changelog is emitted every 2 s
        self.client_class = client.Client
        self.host, self.port = host, port
        self.container = container
        self.stop = False
        self.sender = self.client_class(host=host, port=port)

    def _streaming_sql(self, group, select):
        cols = ", ".join(group)
        return (f"SELECT {cols}, {select}, _tp_delta FROM ({self.join()}) "
                f"GROUP BY {cols} {self.emit}")

    def setup(self, ref, on_change, views=tuple(VIEWS)):
        t = {}
        start = time.time()
        for name in ("books", "instruments", "closes", "positions", "prices", "fx"):
            self.sender.execute(f"DROP STREAM IF EXISTS {name}")
        for ddl in TABLES_PROTON:
            self.sender.execute(ddl)
        t["createSeconds"] = round(time.time() - start, 1)
        start = time.time()
        for table, rows in reference_rows(ref).items():
            self._insert(table, rows)
        t["loadSeconds"] = round(time.time() - start, 1)
        for v in views:
            self.subscribe(v, self._streaming_sql(*VIEWS[v]), on_change)
        return t

    def subscribe(self, view, sql, on_change, first=None):
        def run():
            c = self.client_class(host=self.host, port=self.port)
            cols = None
            for item in c.execute_iter(sql, with_column_types=True):
                if self.stop:
                    break
                if cols is None:
                    cols = [name for name, _ in item]
                    continue
                recv = now_ms()
                row = dict(zip(cols, item))
                delta = row.pop("_tp_delta")
                if first is not None and not first.is_set():
                    first.set()
                on_change(view, [row] if delta > 0 else [], [row] if delta < 0 else [], recv)
        threading.Thread(target=run, daemon=True).start()

    def _insert(self, table, rows):
        if not rows:
            return
        cols = list(rows[0])
        self.sender.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES", [tuple(r[c] for c in cols) for r in rows])

    def send(self, batch, ts):
        for table, rows in batch_rows(batch, ts).items():
            self._insert(table, rows)

    def snapshot(self, view):
        group, select = VIEWS[view]
        cols = ", ".join(group)
        sql = f"SELECT {cols}, {select} FROM ({self.join(lambda n: f'table({n})')}) GROUP BY {cols}"
        rows, types = self.sender.execute(sql, with_column_types=True)
        names = [n for n, _ in types]
        return [dict(zip(names, r)) for r in rows]

    def ad_hoc(self, on_change):
        first = threading.Event()
        start = time.time()
        self.subscribe("ad_hoc_by_sector",
                       self._streaming_sql(["sector"], "SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd"),
                       lambda *a: None, first)
        first.wait(60)
        return {"kind": "live (a new streaming query)", "firstRowsMillis": round((time.time() - start) * 1000)}

    def usage(self):
        return container_usage(self.container)

    def close(self):
        self.stop = True


# ---------------------------------------------------------------------------------------------
class Deephaven:
    """Deephaven Community (the baseline): keyed input tables, natural joins, aggregations; live
    views subscribed with the ticking Python client (whole table)."""

    name = "deephaven"

    def __init__(self, host="localhost", port=10000, psk="spike-psk", pid=None):
        from pydeephaven import Session
        self.s = Session(host=host, port=port,
                         auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler", auth_token=psk)
        self.pid = pid
        self.inputs = {}
        self.handles = []

    def _schemas(self):
        import pyarrow as pa
        s, d, l = pa.string(), pa.float64(), pa.int64()
        return {
            "books": (pa.schema([("book", s), ("trader", s), ("desk", s)]), ["book"]),
            "instruments": (pa.schema([("instrument", s), ("sector", s), ("ccy", s), ("beta", d),
                                       ("multiplier", d)]), ["instrument"]),
            "closes": (pa.schema([("instrument", s), ("close_px", d)]), ["instrument"]),
            "positions": (pa.schema([("book", s), ("instrument", s), ("qty", d)]), ["book", "instrument"]),
            "prices": (pa.schema([("instrument", s), ("px", d), ("ts", l)]), ["instrument"]),
            "fx": (pa.schema([("ccy", s), ("usd_rate", d), ("ts", l)]), ["ccy"]),
        }

    def _add(self, table, rows):
        import pyarrow as pa
        if rows:
            schema = self._schemas()[table][0]
            self.inputs[table].add(self.s.import_table(pa.Table.from_pylist(rows, schema=schema)))

    def _views(self):
        from pydeephaven import agg
        i = self.inputs
        risk = (i["positions"]
                .natural_join(i["instruments"], on=["instrument"], joins=["sector", "ccy", "beta", "multiplier"])
                .natural_join(i["books"], on=["book"], joins=["trader", "desk"])
                .natural_join(i["prices"], on=["instrument"], joins=["px", "px_ts = ts"])
                .natural_join(i["fx"], on=["ccy"], joins=["usd_rate", "fx_ts = ts"])
                .natural_join(i["closes"], on=["instrument"], joins=["close_px"])
                .update_view(["root = `ROOT`",
                              "mv_usd = qty * px * multiplier * usd_rate",
                              "pnl_usd = qty * (px - close_px) * multiplier * usd_rate",
                              "beta_delta_usd = mv_usd * beta"]))
        self.risk = risk
        measures = [agg.sum_(["mv_usd", "pnl_usd", "beta_delta_usd"]), agg.count_("n"),
                    agg.max_(["last_px_ts = px_ts", "last_fx_ts = fx_ts"])]
        from scenario import CCYS
        pivot = risk.update_view([f"c_{c} = ccy == `{c}` ? pnl_usd : NULL_DOUBLE" for c in CCYS])
        return {
            "risk_total": risk.agg_by(measures, by=["root"]),
            "risk_by_desk": risk.agg_by(measures, by=["desk"]),
            "risk_by_book": risk.agg_by(measures, by=["desk", "trader", "book"]),
            "pnl_by_desk_ccy": pivot.agg_by([agg.sum_([f"pnl_in_{c.lower()} = c_{c}" for c in CCYS]),
                                             agg.max_(["last_px_ts = px_ts", "last_fx_ts = fx_ts"])], by=["desk"]),
        }

    def subscribe(self, view, table, on_change, first=None):
        import pydeephaven_ticking.table_listener as tl

        def rows(cols):
            names = list(cols)
            values = [cols[n].to_pylist() for n in names]
            return [dict(zip(names, v)) for v in zip(*values)]

        def on_update(update):
            recv = now_ms()
            if first is not None and not first.is_set():
                first.set()
            on_change(view, rows(update.added()) + rows(update.modified()), rows(update.removed()), recv)

        h = tl.listen(table, on_update)
        h.start()
        self.handles.append(h)

    def setup(self, ref, on_change, views=tuple(VIEWS)):
        t = {}
        start = time.time()
        for name, (schema, keys) in self._schemas().items():
            self.inputs[name] = self.s.input_table(schema=schema, key_cols=keys)
        self.tables = self._views()
        t["createSeconds"] = round(time.time() - start, 1)
        for v in views:
            self.subscribe(v, self.tables[v], on_change)
        start = time.time()
        for table, rows in reference_rows(ref).items():
            self._add(table, rows)
        t["loadSeconds"] = round(time.time() - start, 1)
        return t

    def send(self, batch, ts):
        for table, rows in batch_rows(batch, ts).items():
            self._add(table, rows)

    def snapshot(self, view):
        return self.tables[view].to_arrow().to_pylist()

    def ad_hoc(self, on_change):
        from pydeephaven import agg
        first = threading.Event()
        start = time.time()
        t = self.risk.agg_by([agg.sum_(["mv_usd", "pnl_usd"])], by=["sector"])
        self.subscribe("ad_hoc_by_sector", t, lambda *a: None, first)
        first.wait(60)
        return {"kind": "live (a new table operation)", "firstRowsMillis": round((time.time() - start) * 1000)}

    def usage(self):
        return process_usage(self.pid)

    def close(self):
        for h in self.handles:
            try:
                h.stop()
            except Exception:
                pass
        self.s.close()
