"""The engine (legend_lite.engine): lite's own API and the page's routes over frames -- lite's operations and
query/run, plan-and-run and its dry run, the saved pages and queries at lite's mount -- over the frame as it is
then; the refusals in lite's error shape; and only the engine's own page answered (its token, a local Host, no
cross-origin answer). legend-engine's pure/v1 is the edge's, tested by test_edge (the edge program's H5)."""

import datetime
import http.client
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.error
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import ClassVar
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite import _json
from legend_lite._home import locked
from legend_lite._planned import Database, plan_from_answer
from legend_lite.engine import Engine, NativePlanner, WebServer
from legend_lite.keeper import page_hash

GROUP = "->filter(x|$x.qty > 5)->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])->sort(~desk->ascending())"


def trades():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "desk": ["FX", "EQ", "FX", "RATES", "EQ"],
            "qty": [10.5, 20.0, 30.25, 1.0, 7.5],
        }
    )


# A snapshot's pull as its request carries it (the user's ruling of 2026-11-12): the model, its runtime and the query,
# which the planner plans in the call -- never the statement the plane runs
_SNAP_MODEL = """###Relational
Database s::DB ( Table t ( id INTEGER, desk VARCHAR(8), qty DOUBLE ) )

###Connection
RelationalDatabaseConnection s::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

###Runtime
Runtime s::RT { mappings: []; connections: [ s::DB: [ c: s::Conn ] ]; }
"""


def _snap_pull() -> dict:
    return {
        "model": _SNAP_MODEL,
        "runtime": "s::RT",
        "lambda": json.dumps(ll.compiler.parse("|#>{s::DB.t}#->select(~[id, desk, qty])")),
    }


def refusal(plan):
    """A plan's tagged refusal (``ERR`` and lite's error, §3's ``{kind, message, ...}``) as its kind's name in the
    product (``PARSE``, ``COMPILE``, ``REQUEST``, ...: compiler.refusal_kinds) and its message -- None when it is no
    refusal."""
    if not plan.startswith("ERR\n"):
        return None
    said = json.loads(plan[4:])
    names = {text: name for name, text in ll.compiler.refusal_kinds().items()}
    return names.get(said["kind"], said["kind"]), said["message"]


def sent(path, asked):
    """A plan-and-run's request as ``path`` takes it: plan-and-run its query's tree as text (``lambda``), the dry run
    (plan.json) the tree itself (``query``: lite's query/plan, the host train, 2026-11-17)."""
    if path == "/plan.json" and "lambda" in asked:
        asked = {**{k: v for k, v in asked.items() if k != "lambda"}, "query": json.loads(asked["lambda"])}
    return json.dumps(asked)


class Served(unittest.TestCase):
    """An engine over one Live frame, and requests as DataCube's client sends them."""

    def setUp(self):
        self.df = trades()
        self.frames = ll.Frames()
        self.table = self.frames.register("trades", self.df)
        self.server = WebServer(Engine(self.frames))

    def tearDown(self):
        self.server.close()

    def post(self, path, body, content_type="application/json", headers=None):
        """The engine's answer to a POST: status, headers, body bytes."""
        sent = {"Content-Type": content_type, "Authorization": self.server.authorization} | (headers or {})
        request = urllib.request.Request(self.server.url + path, body.encode("utf-8"), sent, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    def query(self, steps):
        """A cube query as the client sends it: over the table, then ->from(runtime) (fromRuntime)."""
        return ll.parse("|" + self.table.accessor + steps + "->from(" + self.table.runtime + ")")


class UnexpectedFailures(Served):
    """The C5 follow-up's audit item 3 (2026-11-15): a failure no route answers is still an answer -- a 500 naming it,
    its words with every value the engine holds taken out -- never a socket closed with the error on stderr alone."""

    def broken(self):
        """A live frame that fails as it is read again, its words holding a value the engine holds (a bearer URL's)."""
        calls = []

        def frame():
            calls.append(1)
            if len(calls) > 1:
                raise RuntimeError("the live frame could not be read: X-Amz-Signature=held-0123456789")
            return trades()

        table = self.frames.register("broken", frame)
        original = self.frames.held_values
        self.frames.held_values = lambda: (*original(), "X-Amz-Signature=held-0123456789")
        return table

    def test_a_planner_calls_and_a_cubes_unexpected_failure_say_no_held_value(self):
        # the C5 follow-up's check (2026-11-15): a page's planner call (plan-and-run reads the frames, a live one read
        # again) and a cube (GET cube.json) answered a failure's raw words; the engine's one unexpected answer now
        table = self.broken()
        asked = {
            "lambda": json.dumps(ll.parse("|" + table.accessor + "->select(~[desk])")),
            "model": table.model,
            "runtime": table.runtime,
        }
        for said in (
            self.server.engine.answer("POST", "/plan-and-run.json", "", json.dumps(asked)),
            self.server.engine.answer("GET", "/cube.json", "table=broken", None),
        ):
            text = said.body.decode("utf-8")
            self.assertEqual(said.status, 500, text)
            self.assertIn("RuntimeError: the live frame could not be read", text)
            self.assertNotIn("held-0123456789", text)


PARTY = """Class demo::party::Party
{
  id: Integer[1];
  name: String[1];
}

###Relational
Database demo::party::PartyDB
(
  Schema PARTY
  (
    Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
  )
)

###Mapping
Mapping demo::party::PartyMapping
(
  *demo::party::Party: Relational
  {
    ~primaryKey ([demo::party::PartyDB] PARTY.PARTY.ID)
    ~mainTable [demo::party::PartyDB] PARTY.PARTY
    id: [demo::party::PartyDB] PARTY.PARTY.ID,
    name: [demo::party::PartyDB] PARTY.PARTY.NAME
  }
)

###Connection
RelationalDatabaseConnection demo::party::Connection
{
  store: demo::party::PartyDB;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}

###Runtime
Runtime demo::party::Runtime
{
  mappings: [demo::party::PartyMapping];
  connections: [demo::party::PartyDB: [db: demo::party::Connection]];
}

###Data
Data demo::party::PartyData
{
  Relational
  #{
    PARTY.PARTY:
      'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n';
  }#
}
"""
PARTY_QUERY = (
    "|demo::party::Party.all()->project(~[name: p|$p.name])->sort(~name->ascending())"
    "->from(demo::party::PartyMapping, demo::party::Runtime)"
)


class ProjectQueries(Served):
    """A project's query run on Python (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): Query or Studio served
    here sends a model none of the frames wrote, with its own Data rows; Python makes that project's database inside its
    DuckDB (the native library's statements), runs the query there on a cursor of its own, and the frames stay as they
    were."""

    def project_input(self, model=PARTY):
        return json.dumps(
            {
                "clientVersion": "vX_X_X",
                "function": ll.parse(PARTY_QUERY),
                "model": {"_type": "text", "code": model},
                "context": {"_type": "BaseExecutionContext", "queryTimeOutInSeconds": 60},
            }
        )

    def test_a_projects_load_is_the_plans_and_its_data_is_checked_by_digest(self):
        # B3 (2026-11-11): a project's rows are the plan's ONCE load, run by Python as the tab runs it; a blob that is not
        # the data planned is refused by name before a statement of it runs
        import duckdb

        from legend_lite import compiler, projects

        p = compiler.project_data(PARTY, "DuckDB")
        load = p["load"]
        con = duckdb.connect()
        for sql in [*p["open"], *p["use"]]:
            con.execute(sql)
        projects.run_load(load, con.execute)
        self.assertEqual(con.execute("SELECT count(*) FROM PARTY.PARTY").fetchone()[0], 2)
        tampered = json.loads(load)
        digest = next(iter(tampered["root"]["content"]))
        tampered["root"]["content"][digest] = tampered["root"]["content"][digest].replace("Meridian", "Kestrel")
        with self.assertRaisesRegex(projects.LoadRefused, "is not the data it was planned against"):
            # tampered by the test, typed as the planner's answer is (the re-audit's M1), so the runner's digest check runs
            projects.run_load(plan_from_answer(json.dumps(tampered)), duckdb.connect().execute)

    def test_the_load_runners_shared_conformance_fixture(self):
        # B3 (2026-11-11): Python's run_load and the tab's runLoad (engine-client plan_runner_test) run one fixture --
        # the same plan's load, the same rows, the same tampered blob refused in the same words
        import duckdb

        from legend_lite import compiler, projects

        with open(os.environ["LEGEND_LITE_RUN_LOAD_FIXTURE"], encoding="utf-8") as f:
            fixture = json.load(f)
        p = compiler.project_data(fixture["model"], fixture["databaseType"])
        con = duckdb.connect()
        for sql in [*p["open"], *p["use"]]:
            con.execute(sql)
        projects.run_load(p["load"], con.execute)
        self.assertEqual([list(r) for r in con.execute(fixture["check"]).fetchall()], fixture["rows"])
        tampered = json.loads(p["load"])
        digest = next(iter(tampered["root"]["content"]))
        tampered["root"]["content"][digest] = tampered["root"]["content"][digest].replace(
            fixture["tamper"]["from"], fixture["tamper"]["to"]
        )
        sent = []
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_load(plan_from_answer(json.dumps(tampered)), sent.append)
        self.assertEqual(str(refused.exception), fixture["refused"])
        self.assertFalse(any("INSERT" in s.upper() for s in sent), sent)
        # a plan of another format version, refused in the same words before anything runs (the tenets audit's LOW 1)
        stale = json.loads(compiler.project_data(fixture["model"], fixture["databaseType"])["load"])
        words = fixture["version"]["refused"].replace("{VERSION}", str(stale["version"]))
        stale["version"] = fixture["version"]["given"]
        sent = []
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_load(plan_from_answer(json.dumps(stale)), sent.append)
        self.assertEqual(str(refused.exception), words)
        self.assertEqual(sent, [])

    def test_a_snaps_stream_lands_exactly_in_one_transaction_and_a_cut_or_tampered_one_rolls_back(self):
        # the user's ruling of 2026-11-11: a snapshot from a remote plane is a planned load whose source is a stream --
        # the plan names the plane, its statement and the table; the chunks are read as one Arrow stream by the
        # database's own import, kept only when the stream completed and the table holds the rows the plane counted
        import duckdb

        from legend_lite import compiler, projects

        def chunk(start, n):
            rows = pa.table(
                {
                    "id": pa.array(range(start, start + n), pa.int32()),
                    "desk": [f"D{i}" for i in range(start, start + n)],
                    "qty": pa.array([i / 4 for i in range(start, start + n)], pa.float64()),
                }
            )
            sink = pa.BufferOutputStream()
            with pyarrow.ipc.new_stream(sink, rows.schema) as w:
                w.write_table(rows)
            return sink.getvalue().to_pybytes()

        class Plane:
            def __init__(self, chunks, counted=7, cut_after=None, silent=False):
                self.chunks, self.counted, self.cut_after, self.silent, self.asked = (
                    chunks,
                    counted,
                    cut_after,
                    silent,
                    [],
                )

            def arrow_chunks(self, sql, on_receipt=None, params=()):
                self.asked.append(sql)
                if on_receipt is not None and not self.silent:
                    on_receipt({"statementId": "s1", "serverRows": self.counted, "as": "rita", "where": "a stand-in"})
                for i, c in enumerate(self.chunks):
                    if self.cut_after is not None and i >= self.cut_after:
                        raise ConnectionResetError("the connection was reset")
                    yield c

        def plan(table, schema="main"):
            # the pull, which the planner plans in the call (the user's ruling of 2026-11-12): never the statement
            return compiler.snap_stream(
                {"databaseType": "DuckDB", "plane": "warehouse", **_snap_pull(), "schema": schema, "table": table}
            )

        con = duckdb.connect()
        exists = lambda t: con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [t]).fetchone()[0]
        step = json.loads(plan("snap"))["root"]["steps"][0]
        self.assertEqual(
            sorted(step),
            ["_type", "count", "plane", "prepare", "schema", "sql", "table", "transaction"],
            "the plan names the plane, its statement and the table: never a row",
        )
        source = Plane([chunk(1, 4), chunk(5, 3)])
        (made,) = projects.run_host_load(plan("snap", "copies"), Database(con), planes={"warehouse": source}).landed
        self.assertEqual(
            (made.rows, made.receipt["statementId"], source.asked),
            (7, "s1", [step["sql"]]),
            "the plane ran the planner's own statement of the pull",
        )
        self.assertEqual(
            con.execute(
                "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = 'snap' ORDER BY column_index"
            ).fetchall(),
            [("id", "INTEGER"), ("desk", "VARCHAR"), ("qty", "DOUBLE")],
        )
        self.assertEqual(
            con.execute("SELECT * FROM copies.snap ORDER BY id").fetchall(), [(i, f"D{i}", i / 4) for i in range(1, 8)]
        )
        # cut short: nothing left, and a table of that name there before as it was (its drop rolled back with it)
        with self.assertRaisesRegex(Exception, "the connection was reset"):
            projects.run_host_load(
                plan("cut"), Database(con), planes={"warehouse": Plane([chunk(1, 4), chunk(5, 3)], cut_after=1)}
            )
        self.assertEqual(exists("cut"), 0)
        con.execute("CREATE TABLE kept AS SELECT 1 AS id, 'D1' AS desk, 0.25 AS qty")
        with self.assertRaisesRegex(Exception, "the connection was reset"):
            projects.run_host_load(
                plan("kept"), Database(con), planes={"warehouse": Plane([chunk(1, 4), chunk(5, 3)], cut_after=1)}
            )
        self.assertEqual(con.execute("SELECT id, desk FROM kept").fetchall(), [(1, "D1")])
        # tampered: fewer rows than the plane counted; a plane that never said it was done
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_host_load(
                plan("tampered"), Database(con), planes={"warehouse": Plane([chunk(1, 4), chunk(5, 2)])}
            )
        self.assertEqual(
            str(refused.exception),
            "the stream from 'warehouse' landed 6 rows in main.tampered, where its "
            "plane counted 7: nothing of it is kept",
        )
        self.assertEqual(exists("tampered"), 0)
        with self.assertRaisesRegex(projects.LoadRefused, "ended before its plane said it was done"):
            projects.run_host_load(
                plan("unsaid"), Database(con), planes={"warehouse": Plane([chunk(1, 7)], silent=True)}
            )
        self.assertEqual(exists("unsaid"), 0)
        # a plane this host does not hold, refused by name, nothing run
        with self.assertRaisesRegex(projects.LoadRefused, "'warehouse' is not a remote plane this host holds"):
            projects.run_host_load(plan("t"), Database(con), planes={})
        self.assertEqual(exists("t"), 0)

    def test_a_page_plans_and_runs_a_projects_query_on_its_own_rows(self):
        # a saved query opened on Python's page (C5): planned over the project's model held beside its database, its
        # mapping in the query, and run there -- and its dry run (the page's compile check) planned over that model too.
        # Its rows are its project's because the request says so (``project``, F28)
        asked = {"lambda": json.dumps(ll.parse(PARTY_QUERY)), "model": PARTY, "runtime": "demo::party::Runtime"}
        status, headers, body = self.post("/plan-and-run.json", json.dumps({**asked, "project": True}))
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.apache.arrow.stream"), body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        self.assertEqual(rows.column("name").to_pylist(), ["Halberd Securities", "Meridian Capital"])
        status, _, body = self.post("/plan.json", sent("/plan.json", {**asked, "project": True}))
        self.assertEqual(status, 200, body)
        self.assertTrue(json.loads(body)["plan"].startswith("OK\n"), body)
        self.assertEqual(len(self.server.engine.projects.names), 1)
        # not said: the same model reads only the tables this engine serves -- refused naming the one it does not, never
        # read from the project's own rows in their place (F28), by the run and the dry run alike
        for path in ("/plan-and-run.json", "/plan.json"):
            status, _, body = self.post(path, sent(path, asked))
            self.assertEqual(status, 200, body)
            self.assertRegex(
                json.loads(body)["plan"],
                r"reads only the tables it serves.*it reads PARTY\.PARTY, which it "
                r"does not serve now",
                path,
            )

    def test_a_saved_querys_model_that_is_no_model_is_refused_by_the_compiler(self):
        # the C5 follow-up's held item (the user's ruling of 2026-11-10: a product error carries its kind and where): a
        # project's model that does not parse was answered "this model has none" (no Data rows); its plan is the
        # compiler's own refusal now, by the run and the dry run alike
        broken = PARTY + "\n###Pure\nClass demo::party::Broken {\n"
        asked = {
            "lambda": json.dumps(ll.parse(PARTY_QUERY)),
            "model": broken,
            "runtime": "demo::party::Runtime",
            "project": True,
        }
        for path in ("/plan-and-run.json", "/plan.json"):
            status, _, body = self.post(path, sent(path, asked))
            self.assertEqual(status, 200, body)
            plan = json.loads(body)["plan"]
            self.assertEqual((refusal(plan) or ("",))[0], "PARSE", plan)
            self.assertNotIn("this model has none", plan)

    def test_a_page_model_that_does_not_compile_is_refused_by_the_compiler(self):
        # the C5 follow-up's held item: a page's model of the tables this engine serves, which does not compile, was
        # answered "reads only the tables it serves", naming none; its plan is the compiler's own refusal now
        broken = self.table.model + "\n###Pure\nClass trades::A\n{\n  x: trades::Nope[1];\n}\n"
        asked = {"lambda": json.dumps(self.query("->select(~[desk])")), "model": broken, "runtime": self.table.runtime}
        for path in ("/plan-and-run.json", "/plan.json"):
            status, _, body = self.post(path, sent(path, asked))
            self.assertEqual(status, 200, body)
            plan = json.loads(body)["plan"]
            self.assertEqual((refusal(plan) or ("",))[0], "COMPILE", plan)
            self.assertIn("trades::Nope", plan)
            self.assertNotIn("reads only the tables it serves", plan)

    def test_a_page_model_that_does_not_parse_is_refused_by_the_compiler(self):
        # the C5 follow-up's check (2026-11-15): a page's model that does not PARSE was still "reads only the tables it
        # serves", naming none (Frames._tables_of read the parser's refusal as no model); the compiler's words now, as a
        # project's model's are
        broken = self.table.model + "\n###Pure\nClass trades::A {"
        asked = {"lambda": json.dumps(self.query("->select(~[desk])")), "model": broken, "runtime": self.table.runtime}
        for path in ("/plan-and-run.json", "/plan.json"):
            status, _, body = self.post(path, sent(path, asked))
            self.assertEqual(status, 200, body)
            plan = json.loads(body)["plan"]
            self.assertEqual((refusal(plan) or ("",))[0], "PARSE", plan)
            self.assertNotIn("reads only the tables it serves", plan)

    def test_a_compilers_own_failure_reading_a_model_is_no_refusal_of_it(self):
        # the C5 follow-up's staging check (2026-11-15): a failure of the compiler's own (no PARSER or COMPILATION: a
        # '500', its parser failing) is no refusal of the model -- the engine's unexpected 500, never `ERR 500 ...` as
        # if the model were refused
        from legend_lite.compiler import LegendError

        def failing(text):
            raise LegendError("IllegalArgumentException: the parser failed", "500")

        model = self.table.model.replace("trades::DB", "proj0::store::DB")
        asked = {
            "lambda": json.dumps(ll.parse("|#>{proj0::store::DB.trades}#->select(~[desk])")),
            "model": model,
            "runtime": self.table.runtime,
        }
        with mock.patch.object(ll.compiler, "model_elements", failing):
            for path in ("/plan-and-run.json", "/plan.json"):
                status, _, body = self.post(path, sent(path, asked))
                self.assertEqual(status, 500, body)
                self.assertIn("IllegalArgumentException: the parser failed", body.decode())

    def test_a_planning_failure_over_a_served_model_is_no_refusal_of_the_model(self):
        # the C5 follow-up's check (2026-11-15): the catch around plan_over is the model's refusal alone (ModelRefused),
        # never a failure of what is planned over it (a model compiling alone but not over DataCube's library): that one
        # stays the unexpected failure it is, a 500, never answered as the model's own refusal
        from legend_lite.compiler import LegendError

        def failing(asked, held):
            raise LegendError("the library could not be held over the model", "COMPILATION")

        asked = {
            "lambda": json.dumps(self.query("->select(~[desk])")),
            "model": self.table.model,
            "runtime": self.table.runtime,
        }
        with mock.patch.object(self.server.engine, "_plan_with", failing):
            for path in ("/plan-and-run.json", "/plan.json"):
                status, _, body = self.post(path, sent(path, asked))
                self.assertEqual(status, 500, body)
                self.assertIn("LegendError: the library could not be held over the model", body.decode())


class Planes(Served):
    """Where the engine plans (R3): one planner, Python's own, which every call the compiler answers goes through, and
    which services.json names for the page."""

    def test_it_plans_on_pythons_own_planner_and_says_so(self):
        said = json.loads(self.server.engine.answer("GET", "/services.json", "", None).body)
        local = {"word": "local", "label": "Plan local (in Python: the native library)"}
        self.assertEqual((said["plane"], said["planes"]), (local, [local]))

    def test_every_plan_goes_through_the_planner_it_was_made_with(self):
        asked = []

        class Counting(NativePlanner):
            def plan(self, query, held, runtime):
                asked.append(runtime)
                return super().plan(query, held, runtime)

        self.server.close()
        # the one place an engine makes its planner, patched for this engine alone
        with mock.patch("legend_lite.engine.NativePlanner", Counting):
            self.server = WebServer(Engine(self.frames))
        status, _, body = self.post(
            "/plan-and-run.json",
            json.dumps(
                {
                    "lambda": json.dumps(ll.parse("|" + self.table.accessor + GROUP)),
                    "model": self.table.model,
                    "runtime": self.table.runtime,
                }
            ),
        )
        self.assertEqual(status, 200, body[:300])
        self.assertEqual(asked, [self.table.runtime])


class QueryRunRoute(Served):
    """LITE'S query/run ON PYTHON'S SERVER (the spec's §2, §4; the host train, 2026-11-17): a query's tree planned by the
    product's query/plan, its run's values bound at run, run where the model's rows are -- the frames' tables, else a
    project's own Data rows -- and answered as Arrow (results stay Arrow on Python), each column's type its field's
    legend.type and the answer's shape the stream's legend.shape, as the server's Arrow answer carries them."""

    def ask(self, **asked):
        status, headers, body = self.post("/api/lite/v1/query/run", json.dumps(asked))
        return status, headers, body

    def frame_query(self, steps=GROUP):
        return {
            "model": self.table.model,
            "query": ll.parse("|" + self.table.accessor + steps),
            "runtime": self.table.runtime,
            "format": "arrow",
        }

    def test_a_frames_query_runs_here_its_rows_typed_and_shaped(self):
        status, headers, body = self.ask(**self.frame_query())
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.apache.arrow.stream"), body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        self.assertEqual(rows.schema.metadata[b"legend.shape"], b"RELATION")
        self.assertEqual({f.name: f.metadata[b"legend.type"] for f in rows.schema}, {"desk": b"String", "q": b"Float"})
        kept = self.df[self.df.qty > 5]
        self.assertEqual(
            rows.to_pylist(), [{"desk": d, "q": q} for d, q in kept.groupby("desk")["qty"].sum().sort_index().items()]
        )

    def test_its_values_are_bound_at_run_and_checked_first(self):
        asked = {
            "model": self.table.model,
            "runtime": self.table.runtime,
            "format": "arrow",
            "query": ll.parse("{d: String[1]|" + self.table.accessor + "->filter(x|$x.desk == $d)->select(~[desk])}"),
        }
        status, _, body = self.ask(**asked, params={"d": "FX"})
        self.assertEqual(status, 200, body[:300])
        self.assertEqual(set(pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all().column("desk").to_pylist()), {"FX"})
        for params in ({}, {"d": 1}):
            status, _, body = self.ask(**asked, params=params)
            self.assertEqual((status, json.loads(body)["kind"]), (400, "parameters"), params)

    def test_a_projects_query_runs_on_its_own_rows(self):
        status, _, body = self.ask(
            model=PARTY, query=ll.parse(PARTY_QUERY), runtime="demo::party::Runtime", format="arrow"
        )
        self.assertEqual(status, 200, body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        self.assertEqual(rows.column("name").to_pylist(), ["Halberd Securities", "Meridian Capital"])

    def test_json_is_the_servers_typed_rows_its_cells_as_the_server_writes_them(self):
        # ONE contract (the orchestrator's ruling of 2026-11-17): json in the server's shape and cells
        frame = pa.table(
            {
                "i": pa.array([9007199254740993, None], pa.int64()),
                "f": pa.array([1e16, 0.1]),
                "d": pa.array([Decimal("12.30"), Decimal("-0.05")], pa.decimal128(10, 2)),
                "s": pa.array(["é", None]),
                "b": pa.array([True, False]),
                "day": pa.array([datetime.date(2024, 1, 2), None]),
                "at": pa.array([datetime.datetime(2024, 1, 2, 3, 4, 5, 120000), None], pa.timestamp("us")),  # noqa: DTZ001 -- zone-less on purpose: the value under test is a frame's own, which names no zone
            }
        )
        table = self.frames.register("typed", frame)
        for asked in ({"format": "json"},):
            status, headers, body = self.ask(
                model=table.model, query=ll.parse("|" + table.accessor + "->select()"), runtime=table.runtime, **asked
            )
            self.assertEqual((status, headers["Content-Type"]), (200, "application/json"), body[:300])
            self.assertEqual(
                body.decode("utf-8"),
                '{"shape":"RELATION","columns":['
                '{"name":"i","type":"Integer"},{"name":"f","type":"Float"},{"name":"d","type":"Decimal"},'
                '{"name":"s","type":"String"},{"name":"b","type":"Boolean"},'
                '{"name":"day","type":"StrictDate"},{"name":"at","type":"DateTime"}],"rows":['
                '[9007199254740993,10000000000000000.0,"12.30","é",true,"2024-01-02",'
                '"2024-01-02T03:04:05.120000000Z"],'
                '[null,0.1,"-0.05",null,false,null,null]]}',
            )

    def test_each_refusal_is_lites_by_its_kind(self):
        good = self.frame_query()
        for asked, status, kind in (
            ({**good, "format": "csv"}, 400, "request"),
            # no default format (the orchestrator's ruling of 2026-11-17, 23:08)
            ({k: v for k, v in good.items() if k != "format"}, 400, "request"),
            ({**good, "extra": 1}, 400, "request"),
            ({k: v for k, v in good.items() if k != "runtime"}, 400, "request"),
            ({**good, "query": ll.parse("|" + self.table.accessor + "->select(~[nope])")}, 400, "compile"),
            ({**good, "model": good["model"] + "\n###Pure\nClass a::B {"}, 400, "parse"),
            # a project's model with no Data rows, over tables this engine does not serve (F28)
            (
                {
                    "model": PARTY.split("###Data")[0],
                    "query": ll.parse(PARTY_QUERY),
                    "runtime": "demo::party::Runtime",
                    "format": "arrow",
                },
                400,
                "request",
            ),
        ):
            with self.subTest(asked={k: v for k, v in asked.items() if k not in ("query", "model")}):
                got, headers, body = self.ask(**asked)
                self.assertEqual(
                    (got, headers["Content-Type"], json.loads(body)["kind"]),
                    (status, "application/json", kind),
                    body[:300],
                )


class HostRoutesConformance(unittest.TestCase):
    """LITE API V1'S CONFORMANCE SUITE, PYTHON'S SERVER'S OWN ROUTES (the spec's §5; the orchestrator's word of
    2026-11-17: so a route cannot go missing again): each case of engine-client/test/fixtures/lite-api-cases.json whose
    operation is a host's own -- query/run (POST /api/lite/v1/query/run), the saved queries' and pages' get (GET
    /api/lite/v1/queries[/dataCube]/<id>) -- and whose bindings name python, asked of Python's engine over a home of
    its own: its answer exact (``answer``), its refusal's kind (``refusal``), as the server's reader asks its routes.
    The other operations are the bindings lane's (test_compiler.LiteApiConformance)."""

    ROUTES: ClassVar[dict] = {
        "query/run": ("POST", "/api/lite/v1/query/run"),
        "queries/get": ("GET", "/api/lite/v1/queries/"),
        "queries/dataCube/get": ("GET", "/api/lite/v1/queries/dataCube/"),
    }

    def test_pythons_server_answers_each_of_its_routes_cases(self):
        fixture = json.loads(Path(os.environ["LEGEND_LITE_API_CASES"]).read_text(encoding="utf-8"))
        cases = [
            c
            for c in fixture["cases"]
            if "python" in c.get("bindings", ["browser", "server", "python"]) and c["op"] in self.ROUTES
        ]
        place = tempfile.TemporaryDirectory()
        self.addCleanup(place.cleanup)
        home = mock.patch.dict(os.environ, {"LEGEND_LITE_HOME": place.name})
        home.start()
        self.addCleanup(home.stop)
        for case in cases:
            with self.subTest(case["name"]):
                engine = Engine(ll.Frames(), pages=Path(place.name) / "pages")
                method, path = self.ROUTES[case["op"]]
                if method == "GET":
                    said = engine.answer("GET", path + case["request"]["id"], "", None)
                else:
                    said = engine.answer("POST", path, "", json.dumps(case["request"]))
                if "refusal" in case:
                    self.assertGreaterEqual(said.status, 400, said.body[:300])
                    self.assertEqual(json.loads(said.body)["kind"], case["refusal"]["kind"])
                else:
                    self.assertEqual(said.status, 200, said.body[:300])
                    self.assertEqual(_json.loads(said.body), case["answer"])


class PlanAndRunRoutes(Served):
    """PLANANDRUN ON PYTHON (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): a page's grid planned against the model
    Python holds and run on its DuckDB in one call (plan-and-run.json), and its dry run (plan.json, the page's compile
    check) -- only over the models this engine serves, no handle crossing, never a dropped call. A question about the
    Pure query alone is lite's own API (/api/lite/v1/<op>); legend-engine's is the edge's (H5)."""

    def plan_and_run(self, steps, model=None):
        return self.post(
            "/plan-and-run.json",
            json.dumps(
                {
                    "lambda": json.dumps(ll.parse("|" + self.table.accessor + steps)),
                    "model": self.table.model if model is None else model,
                    "runtime": self.table.runtime,
                }
            ),
        )

    def test_a_grid_is_planned_over_the_model_held_run_here_and_typed_by_its_plan(self):
        status, headers, body = self.plan_and_run(GROUP)
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.apache.arrow.stream"), body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        plan = rows.schema.metadata[b"legend.plan"].decode()
        self.assertTrue(plan.startswith("OK\n"), plan[:200])
        self.assertEqual(json.loads(plan[3:])["plan"]["root"]["sql"]["statement"], self.table.plan(GROUP).sql)
        self.assertEqual({f.name: f.metadata[b"legend.type"] for f in rows.schema}, {"desk": b"String", "q": b"Float"})
        kept = self.df[self.df.qty > 5]
        self.assertEqual(
            rows.to_pylist(), [{"desk": d, "q": q} for d, q in kept.groupby("desk")["qty"].sum().sort_index().items()]
        )

    def plan_and_run_over(self, model, query):
        return self.post(
            "/plan-and-run.json",
            json.dumps({"lambda": json.dumps(ll.parse(query)), "model": model, "runtime": self.table.runtime}),
        )

    def test_a_model_the_page_writes_over_the_tables_it_serves_is_planned(self):
        # a page's project, say: its own Database, over the frame's table -- every table one Python serves (option (c))
        project = self.table.model.replace("trades::DB", "proj::store::DB")
        status, headers, body = self.plan_and_run_over(project, "|#>{proj::store::DB.trades}#->select(~[desk])")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.apache.arrow.stream"), body[:300])
        self.assertEqual(pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all().num_rows, len(self.df))
        # the frame let go of: its table no longer served, the project no longer read
        self.frames.unregister("trades")
        status, _, body = self.plan_and_run_over(project, "|#>{proj::store::DB.trades}#->select(~[desk])")
        self.assertEqual((refusal(json.loads(body)["plan"]) or ("",))[0], "REQUEST", body)

    def test_a_model_that_declares_a_table_it_does_not_serve_is_refused_and_nothing_runs(self):
        # a table of this database Python never showed the page (a person's own): never read, whatever the model says
        self.frames.connection._con.execute("CREATE TABLE salaries AS SELECT 'Ada' AS name, 100 AS amount")
        self.addCleanup(self.frames.connection._con.execute, "DROP TABLE IF EXISTS salaries")
        model = self.table.model.replace(
            "    Table trades",
            "    Table salaries\n    (\n        name VARCHAR(10),\n        amount BIGINT\n    )\n    Table trades",
            1,
        )
        self.assertIn("Table salaries", model)
        status, headers, body = self.plan_and_run_over(model, "|#>{trades::DB.salaries}#->select(~[name])")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/json"))
        self.assertEqual((refusal(json.loads(body)["plan"]) or ("",))[0], "REQUEST", body)
        # nor one that does not read as a model at all -- refused in the compiler's words, its kind PARSER (2026-11-15,
        # the C5 follow-up's check: until then ERR MODEL, "reads only the tables it serves", naming none)
        status, _, body = self.plan_and_run_over("not a model", "|1")
        self.assertEqual((refusal(json.loads(body)["plan"]) or ("",))[0], "PARSE", body)

    def test_a_plan_and_run_over_a_read_its_failure_ended_is_answered_422_with_its_plan(self):
        # the C4 audit's M4, 2026-11-13: a read a failing query ended refuses its later queries in the database's own
        # error class, so the plan-and-run answers {plan, error} 422 -- until then a RuntimeError, a 500
        from legend_lite._planned import PlannedSql

        with self.frames.reading() as read:
            with self.assertRaises(Exception):  # noqa: B017 -- whichever error the database or the dataframe library raises: the point is that it is not swallowed
                read.execute(PlannedSql("SELECT CAST('x' AS INTEGER) AS n"))
            answer = self.server.engine._plan_and_run(
                json.dumps(
                    {
                        "lambda": json.dumps(ll.parse("|" + self.table.accessor + "->select(~[desk])")),
                        "model": self.table.model,
                        "runtime": self.table.runtime,
                    }
                ),
                over=read,
            )
        self.assertEqual(answer.status, 422, answer.body[:300])
        said = json.loads(answer.body)
        self.assertTrue(said["plan"].startswith("OK\n"), said)
        self.assertRegex(
            said["error"],
            r"^TransactionException: this read ended at the failure that aborted its transaction \(Conversion",
        )

    def test_sql_the_database_refuses_is_answered_with_its_plan(self):
        status, headers, body = self.plan_and_run("->extend(~[n: x|$x.desk->toOne()->parseInteger()])")
        self.assertEqual((status, headers["Content-Type"]), (422, "application/json"), body[:300])
        said = json.loads(body)
        self.assertTrue(said["plan"].startswith("OK\n"), said)
        self.assertIn("Exception", said["error"])

    def test_a_request_that_is_not_one_is_refused(self):
        for body in ("not json", "[]", json.dumps({"lambda": 1, "model": "", "runtime": ""})):
            self.assertEqual(self.post("/plan-and-run.json", body)[0], 400, body)
            self.assertEqual(self.post("/plan.json", body)[0], 400, body)

    def test_the_dry_run_plans_what_plan_and_run_plans_over_the_model_held(self):
        # one planner function for both (the page's check compiles what its run sends): the same query's plan, the same
        # text -- a later edit that splits them fails here, not in review
        lam = json.dumps(ll.parse("|" + self.table.accessor + GROUP))
        asked = {"lambda": lam, "model": self.table.model, "runtime": self.table.runtime}
        status, headers, body = self.post("/plan.json", sent("/plan.json", asked))
        self.assertEqual((status, headers["Content-Type"]), (200, "application/json"), body)
        dry = json.loads(body)["plan"]
        self.assertTrue(dry.startswith("OK\n"), dry)
        _, _, rows = self.post("/plan-and-run.json", json.dumps(asked))
        self.assertEqual(
            dry, pyarrow.ipc.open_stream(pa.py_buffer(rows)).read_all().schema.metadata[b"legend.plan"].decode()
        )
        # never run, and only over the models this engine serves (R4): another is the planner's refusal, in its words
        refused = json.loads(
            self.post(
                "/plan.json", sent("/plan.json", {"lambda": lam, "model": "unserved", "runtime": self.table.runtime})
            )[2]
        )
        # a text that is no model: the compiler's own refusal (2026-11-15, the C5 follow-up's check: until then ERR MODEL)
        self.assertEqual((refusal(refused["plan"]) or ("",))[0], "PARSE", refused)

    def test_the_planner_worker_tunnel_is_gone(self):
        # a page asks Python as it asks any host: legend-engine's own routes, and the host's compute -- never the tab's
        # worker messages carried over HTTP (planner.json, retired 2026-10-15)
        self.assertEqual(self.post("/planner.json", json.dumps({"kind": "pureV1"}))[0], 404)

    def test_the_stores_need_the_token(self):
        # the saved queries and the saved pages (B5, B6): this user's, so only a page holding the engine's token asks
        for path in ("/api/lite/v1/queries/search", "/api/lite/v1/queries/dataCube/search"):
            self.assertEqual(self.post(path, "{}", headers={"Authorization": "Bearer nope"})[0], 401, path)

    def test_both_need_the_token_and_neither_is_a_get(self):
        for path in ("/plan-and-run.json", "/plan.json"):
            self.assertEqual(self.post(path, "{}", headers={"Authorization": "Bearer nope"})[0], 401, path)
            request = urllib.request.Request(
                self.server.url + path, headers={"Authorization": self.server.authorization}
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as r:
                    status = r.status
            except urllib.error.HTTPError as e:
                status = e.code
            self.assertNotEqual(status, 200, path)


class SettingsKept(Served):
    """The page's own settings, kept by Python in this user's folder (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B2): what
    the browser keeps per address, and Python's address is new each run."""

    def get(self, path):
        request = urllib.request.Request(self.server.url + path, headers={"Authorization": self.server.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_kept_whole_and_read_back_by_the_next_page(self):
        from unittest import mock

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        with mock.patch.dict(os.environ, {"LEGEND_LITE_HOME": home.name}):
            self.assertEqual(self.get("/settings.json"), (200, b"null"), "none kept yet")
            values = {"dataCube.grid.rowBuffer": 30, "dataCube.debugger.enableDebugMode": True}
            self.assertEqual(self.post("/settings.json", json.dumps(values))[0], 204)
            self.assertEqual(json.loads(self.get("/settings.json")[1]), values)
            self.assertEqual(json.loads(Path(home.name, "settings", "datacube.json").read_text()), values)
            # a write is whole: the next replaces it
            self.assertEqual(self.post("/settings.json", json.dumps({"dataCube.grid.rowBuffer": 40}))[0], 204)
            self.assertEqual(json.loads(self.get("/settings.json")[1]), {"dataCube.grid.rowBuffer": 40})

    def test_only_an_object_and_only_with_the_token(self):
        for body in ("not json", "[1]", '"x"'):
            self.assertEqual(self.post("/settings.json", body)[0], 400, body)
        self.assertEqual(self.post("/settings.json", "{}", headers={"Authorization": "Bearer nope"})[0], 401)
        request = urllib.request.Request(self.server.url + "/settings.json")
        with self.assertRaises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(request, timeout=30)
        self.assertEqual(refused.exception.code, 401)


class ModelHomeServed(Served):
    """Python's model home (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3): the native library's SDLC and Depot, served at
    /sdlc/api and /depot/api with the engine's token, over a git repository in this user's folder. The routes' answers
    are the SDLC suite's (//datacube:python_model_home_test); here, how the engine serves them."""

    def call(self, method, path, body=None, authorization=None):
        sent = {"Authorization": self.server.authorization if authorization is None else authorization}
        if body is not None:
            sent["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.server.url + path, None if body is None else body.encode("utf-8"), sent, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_a_project_made_and_read_back_in_a_git_repository_of_this_users(self):
        from unittest import mock

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        with mock.patch.dict(os.environ, {"LEGEND_LITE_HOME": home.name}):
            said = json.loads(self.server.engine.answer("GET", "/services.json", "", None).body)
            # home() resolves the folder it is given (macOS's /var is /private/var)
            self.assertEqual(said["modelHome"], {"where": str(Path(home.name, "projects").resolve())})
            status, body = self.call(
                "POST",
                "/sdlc/api/projects",
                json.dumps(
                    {"name": "Python home", "description": "", "groupId": "org.example", "artifactId": "python-home"}
                ),
            )
            self.assertIn(status, (200, 201), body[:300])
            project = json.loads(body)["projectId"]
            status, body = self.call("GET", f"/sdlc/api/projects/{project}")
            self.assertEqual(status, 200, body[:300])
            self.assertEqual(json.loads(body)["name"], "Python home")
            # git's own layout: what `git --git-dir` reads, and `git push` sends anywhere
            self.assertTrue(Path(home.name, "projects", "HEAD").is_file())
            self.assertTrue(Path(home.name, "projects", "refs", "heads").is_dir())

    def test_with_the_token_only_and_delete_for_the_model_home_only(self):
        self.assertEqual(self.call("GET", "/sdlc/api/projects", authorization="Bearer nope")[0], 401)
        self.assertEqual(self.call("GET", "/depot/api/projects", authorization="Bearer nope")[0], 401)
        self.assertEqual(self.call("DELETE", "/settings.json")[0], 405)


class EdgeOff(unittest.TestCase):
    """THE PRODUCT LIBRARY HOLDS NO EDGE (K2, the user's "product root, edge opt-in", 2026-11-18): //native:compiler is
    rooted at lite's own entry points alone -- legend-engine's (pure/v1, execute's halves, the refusal in its envelope,
    its saved-query store) are the engine library's (//native:engine), which no product module loads. Asked of the
    product library's own symbol table, never a list kept by hand; and the product's suites run on it (this lane)."""

    def test_the_product_library_exports_no_edge_entry(self):
        from legend_lite import _loader
        from legend_lite._library import library

        held = sorted(name for name in _loader.ENGINE_ARITY if library().exports(name))
        self.assertEqual(held, [], "legend-engine's entry points are the engine library's, never the product's")
        # the check bites: a product entry is seen
        self.assertTrue(library().exports("lite_query_plan"))
        self.assertFalse(set(_loader.ENGINE_ARITY) & set(_loader._ARITY), "the product's loader names no edge entry")


class TheEdgeSeam(unittest.TestCase):
    """The edge program's H5 (2026-11-17, the orchestrator's D-a): the product's engine answers the calls its EdgeRoutes
    serve through them -- NO_EDGE_ROUTES, which serves none, unless the edge's door gives it another -- never None (G7),
    and never by importing the edge."""

    def test_an_engine_carries_the_products_refusal_and_answers_through_it(self):
        from legend_lite._edge_seam import NO_EDGE_ROUTES

        engine = Engine(ll.Frames())
        self.assertIs(engine.edge, NO_EDGE_ROUTES)
        said = engine.answer("POST", "/no-such-route.json", "", "{}")
        self.assertEqual((said.status, said.body), (404, b"no such path: /no-such-route.json"))

    def test_a_call_its_edge_routes_serve_goes_to_them_and_only_that(self):
        asked = []

        class Routes:
            def serves(self, method, path):
                return path.startswith("/theirs/")

            def answer(self, engine, method, path, query, body):
                asked.append((method, path, query, body))
                return engine_module.Answer(200, "text/plain", b"the routes given")

        from legend_lite import engine as engine_module

        engine = Engine(ll.Frames(), edge=Routes())
        said = engine.answer("GET", "/theirs/x", "a=1", None)
        self.assertEqual(
            (said.status, said.body, asked), (200, b"the routes given", [("GET", "/theirs/x", "a=1", None)])
        )
        # its own routes stay its own, and a call neither serves is the product's refusal
        self.assertEqual(engine.answer("GET", "/services.json", "", None).status, 200)
        said = engine.answer("POST", "/no-such-route.json", "", "{}")
        self.assertEqual((said.status, said.body), (404, b"no such path: /no-such-route.json"))
        self.assertEqual(len(asked), 1)

    def test_none_is_no_edge_routes(self):
        for wrong in (None, object(), "edge"):
            with self.assertRaisesRegex(TypeError, "an engine's edge routes answer what its own routes do not"):
                Engine(ll.Frames(), edge=wrong)


class TheProductsStore(unittest.TestCase):
    """The edge program's H6 (2026-11-17): the saved queries and pages at lite's own route (/api/lite/v1/queries, as the
    server mounts them; the route renamed by the orchestrator's ruling of that day, H6 (b)), answered by the native
    library's PRODUCT entries in lite's words and refused in lite's one error shape ({kind, message}) -- the same rules,
    the same folders, the same records; legend-engine's mount (/api/pure/v1/query) is the edge's, in legend-engine's
    words (legend_lite.edge, H5): a product engine serves no such route."""

    def setUp(self):
        self.place = tempfile.TemporaryDirectory()
        self.addCleanup(self.place.cleanup)
        self.engine = Engine(ll.Frames(), pages=Path(self.place.name) / "pages")
        home = mock.patch.dict(os.environ, {"LEGEND_LITE_HOME": self.place.name})
        home.start()
        self.addCleanup(home.stop)

    def test_a_missing_query_is_refused_in_each_mounts_own_words(self):
        lite = self.engine.answer("GET", "/api/lite/v1/queries/nope", "", None)
        self.assertEqual(
            (lite.status, json.loads(lite.body)),
            (404, {"kind": "notFound", "message": "no saved query has the ID 'nope'"}),
        )
        # legend-engine's mount: none on the product's engine (the edge's, H5)
        engine = self.engine.answer("GET", "/api/pure/v1/query/nope", "", None)
        self.assertEqual((engine.status, engine.body), (404, b"no such path: /api/pure/v1/query/nope"))

    def test_a_route_lites_store_does_not_serve_is_refused_by_name(self):
        # lite's API v1 spec §3: a malformed request, 400 request (PR #118 fix round, 2026-11-18; legend-engine's 404 is
        # the edge's mount's)
        said = self.engine.answer("GET", "/api/lite/v1/queries/dataCube/a/b", "", None)
        self.assertEqual(
            (said.status, json.loads(said.body)),
            (400, {"kind": "request", "message": "no such legend-lite API: /api/lite/v1/queries/dataCube/a/b"}),
        )

    def test_a_page_saved_at_lites_route_is_read_there(self):
        made = self.engine.answer(
            "POST",
            "/api/lite/v1/queries/dataCube",
            "",
            json.dumps({"id": "p-9", "name": "Both", "content": {"kind": "datacube.page"}}),
        )
        self.assertEqual(made.status, 200, made.body)
        read = self.engine.answer("GET", "/api/lite/v1/queries/dataCube/p-9", "", None)
        self.assertEqual((read.status, json.loads(read.body)["name"]), (200, "Both"))
        missing = self.engine.answer("GET", "/api/lite/v1/queries/dataCube/nope", "", None)
        self.assertEqual(
            (missing.status, json.loads(missing.body)),
            (404, {"kind": "notFound", "message": "no saved query has the ID 'nope'"}),
        )

    def test_lites_operations_are_not_its_stores(self):
        # /api/lite/v1/query/<op> is lite's API, never the store's: the store's route is /queries
        said = self.engine.answer("POST", "/api/lite/v1/query/parse", "", json.dumps({"text": "|1 + 2"}))
        self.assertEqual(said.status, 200, said.body)
        self.assertIn("query", json.loads(said.body))


class SavedPagesKept(unittest.TestCase):
    """The page's saved pages (step 2d, B6; at lite's route since H6, 2026-11-17): /api/lite/v1/queries/dataCube, answered by legend-lite's own rules
    in the native library over this user's folder, one file each -- the folder's layout the one its pages were kept in
    before, so they open as they were. The rules themselves are held to DataCube's one suite on every store
    (datacube/test/saved-pages/conformance.ts); these are what only Python's folder can be asked."""

    def setUp(self):
        self.place = tempfile.TemporaryDirectory()
        self.addCleanup(self.place.cleanup)
        self.folder = Path(self.place.name) / "pages"
        self.engine = Engine(ll.Frames(), pages=self.folder)

    def call(self, method, path="", body=None):
        return self.engine.answer(method, "/api/lite/v1/queries/dataCube" + path, "", body)

    def test_a_page_kept_before_the_rules_were_java_opens_as_it_was(self):
        # as the page's own store wrote it through Python (json.dump of its row): its content the page's JSON text
        self.folder.mkdir(parents=True)
        record = {
            "id": "p-1",
            "name": "Q3",
            "content": '{"kind":"datacube.page","dec":12.30}',
            "owner": "this user",
            "createdAt": 1,
            "lastUpdatedAt": 2,
            "lastOpenAt": 2,
        }
        (self.folder / "p-1.json").write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        found = json.loads(self.call("POST", "/search", "{}").body)
        self.assertEqual([(q["id"], q["name"], "content" in q) for q in found], [("p-1", "Q3", False)])
        opened = self.call("GET", "/p-1")
        self.assertEqual(opened.status, 200)
        # its content as written: the decimal's spelling kept
        self.assertIn(b'"content":{"kind":"datacube.page","dec":12.30}', opened.body)
        self.assertEqual(self.call("DELETE", "/p-1").status, 204)
        self.assertEqual(list(self.folder.glob("*.json")), [])

    def test_a_page_saved_is_kept_whole_one_file_each(self):
        made = self.call("POST", "", json.dumps({"id": "p-2", "name": "Kept", "content": {"kind": "datacube.page"}}))
        self.assertEqual(made.status, 200, made.body)
        kept = json.loads((self.folder / "p-2.json").read_text(encoding="utf-8"))
        self.assertEqual((kept["owner"], kept["content"]), ("this user", '{"kind":"datacube.page"}'))
        # every write whole: nothing half-written left beside the pages
        self.assertEqual([p.name for p in self.folder.iterdir() if p.suffix == ".tmp"], [])

    def test_a_file_that_is_not_a_page_is_left_out_and_the_others_open(self):
        self.assertEqual(self.call("POST", "", json.dumps({"id": "good", "name": "G", "content": {}})).status, 200)
        (self.folder / "cut-short.json").write_text('{"id": "cut-sho', encoding="utf-8")
        (self.folder / "a-list.json").write_text("[1, 2]", encoding="utf-8")
        # a page under another file's name (copied by hand): its file's name is its id, so it is not that page
        (self.folder / "copied.json").write_text(
            (self.folder / "good.json").read_text(encoding="utf-8"), encoding="utf-8"
        )
        # and a file whose name is no page's id (a space in it), its row naming that id: get refuses the id, so no list
        # names it
        (self.folder / "a b.json").write_text('{"id": "a b", "name": "S", "content": "{}"}', encoding="utf-8")
        self.assertEqual([q["id"] for q in json.loads(self.call("POST", "/search", "{}").body)], ["good"])
        # asked for by its id, a file that is not a page is not found -- and a page saved under its id replaces it
        self.assertEqual(self.call("GET", "/cut-short").status, 404)
        self.assertEqual(self.call("GET", "/copied").status, 404)
        self.assertEqual(self.call("POST", "", json.dumps({"id": "cut-short", "name": "C", "content": {}})).status, 200)
        self.assertEqual(json.loads(self.call("GET", "/cut-short").body)["name"], "C")

    def test_an_id_that_could_name_another_place_is_refused(self):
        for bad in ("..%2Fescape", ".lock", "a.b", "CON", "nul", "COM1", "lpt9"):
            with self.subTest(id=bad):
                self.assertEqual(self.call("GET", "/" + bad).status, 400)
        self.assertEqual(self.call("POST", "", json.dumps({"id": "../x", "name": "x", "content": {}})).status, 400)
        self.assertFalse((Path(self.place.name) / "x.json").exists())

    @unittest.skipIf(sys.platform == "win32", "a folder private to its owner is a POSIX mode")
    def test_the_folder_is_private_to_this_user(self):
        self.assertEqual(self.call("POST", "", json.dumps({"id": "p", "name": "P", "content": {}})).status, 200)
        self.assertEqual(self.folder.stat().st_mode & 0o777, 0o700)

    def test_one_call_at_a_time_across_its_holders(self):
        # another holder of the folder's lock (another notebook's process takes the same file lock) holds it: a save waits
        order = []
        with locked(self.folder):
            writer = threading.Thread(
                target=lambda: (
                    self.call("POST", "", json.dumps({"id": "w", "name": "W", "content": {}})),
                    order.append("saved"),
                )
            )
            writer.start()
            time.sleep(0.3)
            order.append("released")
        writer.join(timeout=30)
        self.assertEqual(order, ["released", "saved"])

    def test_the_old_calls_are_gone(self):
        # the page's rules were its own then (cube-store.ts's RuleStore), Python keeping its records: no longer
        self.assertEqual(self.engine.answer("POST", "/saved-pages.json", "", '{"call":"all"}').status, 404)

    def test_the_folder_is_this_users_unless_pointed_elsewhere(self):
        import importlib
        from unittest import mock

        home_module = importlib.import_module("legend_lite._home")
        self.addCleanup(setattr, home_module, "_chosen", home_module._chosen)
        home_module._chosen = None
        with mock.patch.dict(os.environ, {"LEGEND_LITE_HOME": self.place.name}):
            self.assertEqual(ll.home(), Path(self.place.name).resolve())
            # a folder the process points itself at wins over the one it was started with
            chosen = Path(self.place.name) / "chosen"
            self.assertEqual(ll.home(chosen), chosen.resolve())
            self.assertEqual(ll.home(), chosen.resolve())
        home_module._chosen = None
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LEGEND_LITE_HOME", None)
            if sys.platform == "darwin":
                self.assertEqual(ll.home(), Path.home() / "Library" / "Application Support" / "legend-lite")
            elif sys.platform.startswith("linux"):
                os.environ["XDG_DATA_HOME"] = self.place.name
                self.assertEqual(ll.home(), Path(self.place.name) / "legend-lite")
        said = json.loads(self.engine.answer("GET", "/services.json", "", None).body)
        self.assertEqual(said["pages"], {"where": str(self.folder), "user": "this user"})


class NoFiles(Served):
    """An engine given no DataCube code to read a page's file with (no headless): it says so, and takes none."""

    def test_it_says_it_takes_no_file_and_refuses_one(self):
        answer = self.server.engine.answer("GET", "/services.json", "", None)
        self.assertFalse(json.loads(answer.body)["files"])
        for path, asked in (
            ("/file-part.json", {"at": 0, "bytes": "YQoxCg=="}),
            ("/file.json", {"upload": "u", "name": "a.csv", "table": "a"}),
        ):
            status, _, body = self.post(path, sent(path, asked))
            self.assertEqual(status, 404, path)
            self.assertIn(b"which this engine was not given", body)


class OnlyItsOwnPage(Served):
    def test_a_burst_of_connections_waits_its_turn(self):
        # the listening queue holds a burst, never socketserver's five: macOS resets a connection the queue has no room
        # for (the engine's comment says when it was seen); the burst itself depends on the machine's load, so the
        # queue's length is what is held here
        self.assertGreaterEqual(self.server._server.request_queue_size, 128)

    def raw(self, method, path, headers, body=b""):
        port = int(self.server.url.rsplit(":", 1)[1])
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
            for name, value in headers.items():
                connection.putheader(name, value)
            connection.endheaders(body)
            r = connection.getresponse()
            return r.status, dict(r.getheaders()), r.read()
        finally:
            connection.close()

    def host(self):
        return self.server.url.removeprefix("http://")

    def test_a_request_without_the_token_is_refused(self):
        tree = json.dumps(ll.parse("|1"))
        for given in ({}, {"Authorization": "Bearer nope"}, {"Authorization": self.server.token}):
            status, _, _ = self.raw(
                "POST",
                "/api/lite/v1/query/print",
                {"Host": self.host(), "Content-Length": str(len(tree))} | given,
                tree.encode(),
            )
            self.assertEqual(status, 401, given)

    def test_a_host_that_is_not_this_machine_is_refused(self):
        port = self.server.url.rsplit(":", 1)[1]
        for host in ("evil.example:" + port, "127.0.0.1:1", "localhost"):
            status, _, _ = self.raw(
                "POST",
                "/api/lite/v1/query/print",
                {"Host": host, "Authorization": self.server.authorization, "Content-Length": "2"},
                b"{}",
            )
            self.assertEqual(status, 403, host)
        asked = json.dumps({"text": "|1"}).encode()
        status, _, _ = self.raw(
            "POST",
            "/api/lite/v1/query/parse",
            {
                "Host": "localhost:" + port,
                "Authorization": self.server.authorization,
                "Content-Length": str(len(asked)),
            },
            asked,
        )
        self.assertEqual(status, 200)

    def test_no_cross_origin_request_is_answered(self):
        status, headers, _ = self.raw(
            "OPTIONS",
            "/api/lite/v1/query/plan",
            {"Host": self.host(), "Origin": "http://evil.example", "Access-Control-Request-Method": "POST"},
        )
        self.assertEqual(status, 405)
        self.assertFalse([h for h in headers if h.lower().startswith("access-control-")], headers)

    def test_a_body_past_the_limit_is_refused_unread(self):
        status, _, _ = self.raw(
            "POST",
            "/api/lite/v1/query/parse",
            {"Host": self.host(), "Authorization": self.server.authorization, "Content-Length": str(64 * 1024 * 1024)},
        )
        self.assertEqual(status, 413)

    def test_a_length_that_is_not_ascii_digits_or_a_body_not_utf8_is_answered(self):
        for length, body, status in (("²", b"", 411), ("2", b"\xff\xfe", 400)):
            got, _, _ = self.raw(
                "POST",
                "/api/lite/v1/query/parse",
                {"Host": self.host(), "Authorization": self.server.authorization, "Content-Length": length},
                body,
            )
            self.assertEqual(got, status, length)

    def test_a_refused_request_s_body_is_read_so_its_answer_arrives(self):
        body = b"x" * 200_000
        status, _, answer = self.raw(
            "POST", "/api/lite/v1/query/parse", {"Host": self.host(), "Content-Length": str(len(body))}, body
        )
        self.assertEqual(status, 401)
        self.assertIn(b"the engine's token", answer)

    def test_closed_it_answers_nothing(self):
        url = self.server.url
        self.server.close()
        self.server = WebServer(Engine(self.frames))  # tearDown closes this one
        request = urllib.request.Request(url + "/api/lite/v1/nope", b"{}", {"Authorization": "Bearer x"})
        with self.assertRaises(urllib.error.URLError) as refused:
            urllib.request.urlopen(request, timeout=5)
        self.assertNotIsInstance(refused.exception, urllib.error.HTTPError)
        self.assertIsInstance(refused.exception.reason, ConnectionRefusedError)

    def test_a_connection_that_sends_nothing_neither_blocks_others_nor_close(self):
        port = int(self.server.url.rsplit(":", 1)[1])
        idle = [socket.create_connection(("127.0.0.1", port)) for _ in range(6)]
        try:
            # more idle connections than the compiler's pool has threads: a request is still answered
            tree = json.dumps({"query": ll.parse("|1")})
            status, _, _ = self.raw(
                "POST",
                "/api/lite/v1/query/print",
                {"Host": self.host(), "Authorization": self.server.authorization, "Content-Length": str(len(tree))},
                tree.encode(),
            )
            self.assertEqual(status, 200)
            started = time.monotonic()
            self.server.close()
            self.assertLess(time.monotonic() - started, 5)
        finally:
            for connection in idle:
                connection.close()
            self.server = WebServer(Engine(self.frames))  # tearDown closes this one


class Site(unittest.TestCase):
    """The site's files (DataCube's built pages), at the engine's origin: to a local Host, under the site only."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR")))
        (self.root / "index.html").write_text("<!doctype html>cube")
        (self.root / "vendor").mkdir()
        (self.root / "vendor" / "planner.wasm").write_bytes(b"\0asm")
        (self.root.parent / "outside.txt").write_text("not the site")
        self.frames = ll.Frames()
        self.server = WebServer(Engine(self.frames, site=self.root))

    def tearDown(self):
        self.server.close()

    def get(self, path, host=None):
        port = int(self.server.url.rsplit(":", 1)[1])
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.putrequest("GET", path, skip_host=True)
            connection.putheader("Host", host or f"127.0.0.1:{port}")
            connection.endheaders()
            r = connection.getresponse()
            return r.status, r.getheader("Content-Type"), r.read()
        finally:
            connection.close()

    def test_its_files_with_their_types(self):
        self.assertEqual(self.get("/"), (200, "text/html; charset=utf-8", b"<!doctype html>cube"))
        self.assertEqual(self.get("/vendor/planner.wasm"), (200, "application/wasm", b"\0asm"))

    def test_nothing_outside_it_nor_a_folder(self):
        for path in (
            "/../outside.txt",
            "/%2e%2e/outside.txt",
            "/vendor/",
            "/vendor",
            "/nope.js",
            "/vendor//planner.wasm",
            "/C:/Windows/win.ini",
            "/C%3A/x",
            "/index.html%00.js",
        ):
            self.assertEqual(self.get(path)[0], 404, path)

    def test_to_a_local_host_only(self):
        port = self.server.url.rsplit(":", 1)[1]
        self.assertEqual(self.get("/", host=f"evil.example:{port}")[0], 403)

    def test_an_engine_without_a_site_serves_no_file(self):
        bare = WebServer(Engine(self.frames))
        try:
            port = int(bare.url.rsplit(":", 1)[1])
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
            connection.request("GET", "/")
            self.assertEqual(connection.getresponse().status, 404)
            connection.close()
        finally:
            bare.close()


class AppsServed(unittest.TestCase):
    """Legend Query and Studio served by Python (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md E): each app's site under its
    name, its config naming this engine's own services, and the link's token traded on the first load for the cookie
    its page's calls then carry."""

    def setUp(self):
        self.sites = Path(tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR")))
        self.kept = {
            k: os.environ.get(k) for k in ("LEGEND_LITE_QUERY_SITE", "LEGEND_LITE_STUDIO_SITE", "LEGEND_LITE_SITE")
        }
        # DataCube's own site, as the session's engine needs one (the wheel's; this test's package has none)
        os.environ["LEGEND_LITE_SITE"] = str(self.sites)
        for app in ("query", "studio"):
            (self.sites / app / "demo").mkdir(parents=True)
            (self.sites / app / "demo" / "index.html").write_text(f"<!doctype html>{app}")
            os.environ[f"LEGEND_LITE_{app.upper()}_SITE"] = str(self.sites / app)
        self.server = WebServer(Engine(ll.Frames()))
        self.port = int(self.server.url.rsplit(":", 1)[1])

    def tearDown(self):
        self.server.close()
        for k, v in self.kept.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def call(self, method, path, headers=None, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            connection.request(method, path, body=body, headers={"Host": f"127.0.0.1:{self.port}", **(headers or {})})
            r = connection.getresponse()
            return r.status, dict(r.getheaders()), r.read()
        finally:
            connection.close()

    def cookie(self, token=None, name=None, site="same-origin"):
        # the page's own call, as the browser marks it
        return {
            "Cookie": f"{name or self.server.cookie}={token or self.server.token}",
            **({"Sec-Fetch-Site": site} if site is not None else {}),
        }

    def test_each_apps_files_under_its_name(self):
        for app in ("query", "studio"):
            status, headers, body = self.call("GET", f"/{app}/demo/index.html")
            self.assertEqual(
                (status, headers["Content-Type"], body),
                (200, "text/html; charset=utf-8", f"<!doctype html>{app}".encode()),
            )
        self.assertEqual(self.call("GET", "/query/../studio/demo/index.html")[0], 404)
        self.assertEqual(self.call("GET", "/queryx/demo/index.html")[0], 404)
        os.environ["LEGEND_LITE_QUERY_SITE"] = str(self.sites / "nowhere")
        self.assertEqual(self.call("GET", "/query/demo/index.html")[0], 404, "no site built: nothing served")

    def test_the_links_token_traded_for_the_cookie_and_the_address_without_it(self):
        status, headers, _body = self.call("GET", f"/studio/demo/index.html?token={self.server.token}")
        self.assertEqual(status, 303)
        self.assertEqual(headers["Location"], "/studio/demo/index.html")
        self.assertEqual(
            headers["Set-Cookie"], f"legend-lite-{self.port}={self.server.token}; Path=/; HttpOnly; SameSite=Strict"
        )
        # any other parameter stays
        self.assertEqual(
            self.call("GET", f"/query/demo/index.html?config=x.json&token={self.server.token}")[1]["Location"],
            "/query/demo/index.html?config=x.json",
        )
        self.assertEqual(self.call("GET", "/query/demo/index.html?token=wrong")[0], 401)

    def test_each_apps_config_names_this_engines_services(self):
        origin = f"http://127.0.0.1:{self.port}"
        status, headers, body = self.call("GET", "/query/demo/config.json")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/json"))
        self.assertEqual(
            json.loads(body),
            {
                "execution": {"kind": "server", "engine": f"{origin}/api"},
                "projects": [],
                "depot": {"sdlc": f"{origin}/sdlc/api", "depot": f"{origin}/depot/api"},
                "vendor": "./vendor/",
            },
        )
        self.assertEqual(
            json.loads(self.call("GET", "/studio/demo/config.json")[2]),
            {
                "sdlc": f"{origin}/sdlc/api",
                "depot": f"{origin}/depot/api",
                "vendor": "./vendor/",
                "worker": "./planner-worker.js",
                "engine": f"{origin}/api",
            },
        )

    # a product route the page's calls carry the cookie to (legend-engine's currentUser is the edge's: N4 of PR #118's
    # fix round, 2026-11-18, tested by test_edge)
    OWN = "/sdlc/api/projects"

    def test_the_apps_calls_carry_the_cookie(self):
        self.assertEqual(self.call("GET", self.OWN), (401, unittest.mock.ANY, unittest.mock.ANY))
        # another engine's cookie (another port's), or this one's with another token: refused
        self.assertEqual(self.call("GET", self.OWN, self.cookie(name="legend-lite-1"))[0], 401)
        self.assertEqual(self.call("GET", self.OWN, self.cookie(token="wrong"))[0], 401)
        status, _, body = self.call("GET", self.OWN, self.cookie())
        self.assertEqual((status, type(json.loads(body))), (200, list))
        # the store and the model home, as the token reads them
        self.assertEqual(self.call("POST", "/api/lite/v1/queries/search", self.cookie(), "{}")[0], 200)
        self.assertEqual(self.call("GET", "/sdlc/api/projects", self.cookie())[0], 200)
        self.assertEqual(self.call("GET", "/sdlc/api/projects")[0], 401)

    def test_the_cookie_admits_the_pages_own_calls_only(self):
        # a browser sends a host's cookies to its every port (SameSite or not): a page of another server on this
        # machine sends it too, and is refused (the audit of E)
        user = self.OWN
        for site in ("same-site", "cross-site", "none"):
            self.assertEqual(self.call("GET", user, self.cookie(site=site))[0], 401, site)
        self.assertEqual(self.call("POST", "/api/lite/v1/queries/search", self.cookie(site="same-site"), "{}")[0], 401)
        # a browser that marks no call: its Origin decides
        own = f"http://127.0.0.1:{self.port}"
        self.assertEqual(self.call("GET", user, {**self.cookie(site=None), "Origin": own})[0], 200)
        for origin in (f"http://127.0.0.1:{self.port + 1}", "null", None):
            headers = {**self.cookie(site=None), **({"Origin": origin} if origin else {})}
            self.assertEqual(self.call("GET", user, headers)[0], 401, origin)

    def test_another_servers_cookies_beside_it(self):
        # the header carries every local server's cookies: one this engine cannot read, or one of its own name with
        # another value, leaves its own readable
        jar = self.cookie()
        for others in ("x=a b", 'x={"a":1}', f"{self.server.cookie}=stale", "bare"):
            for header in (f"{others}; {jar['Cookie']}", f"{jar['Cookie']}; {others}"):
                self.assertEqual(self.call("GET", self.OWN, {**jar, "Cookie": header})[0], 200, header)

    def test_an_apps_path_to_another_host_is_refused_once(self):
        with socket.create_connection(("127.0.0.1", self.port), timeout=30) as raw:
            raw.sendall(b"GET /query/demo/config.json HTTP/1.1\r\nHost: evil.example\r\n\r\n")
            said = b""
            while chunk := raw.recv(65536):
                said += chunk
        self.assertTrue(said.startswith((b"HTTP/1.0 403", b"HTTP/1.1 403")), said[:80])
        self.assertEqual(said.count(b"HTTP/1."), 1, said)

    def test_lites_own_routes_answered_as_its_server_answers_them(self):
        # lite's own API, v1 (the spec's §2, §3, §5; the host train, 2026-11-17): one route per operation
        model = json.dumps({"model": "Class demo::A { x: Integer[1]; y: Nope[1]; }"})
        status, _headers, body = self.call(
            "POST", "/api/lite/v1/model/check", {**self.cookie(), "Content-Type": "application/json"}, model
        )
        self.assertEqual(status, 200, body)
        self.assertEqual([e["kind"] for e in json.loads(body)["errors"]], [ll.compiler.error_kind("COMPILE")])
        self.assertEqual(
            self.call("POST", "/api/lite/v1/model/check", {"Content-Type": "application/json"}, model)[0], 401
        )
        # a refusal is lite's one error shape, its status its kind's
        status, _, body = self.call("POST", "/api/lite/v1/query/parse", self.cookie(), json.dumps({"text": "|1 +"}))
        self.assertEqual((status, json.loads(body)["kind"]), (400, ll.compiler.error_kind("PARSE")))
        # the old route names are gone in one step (ruled 5), refused as a malformed request
        for gone in ("/api/lite/v1/compilation/compile", "/api/lite/v1/nope"):
            status, _, body = self.call("POST", gone, self.cookie(), "{}")
            self.assertEqual((status, json.loads(body)["kind"]), (400, ll.compiler.error_kind("REQUEST")), gone)

    def test_ll_query_and_ll_studio_give_the_apps_links(self):
        from legend_lite import datacube

        kept, datacube._session = datacube._session, datacube._Session()
        try:
            for app, open_app in (("query", ll.query), ("studio", ll.studio)):
                url = open_app(browser=False)
                web = datacube._session._web
                self.assertEqual(url, f"{web.url}/{app}/demo/index.html?token={web.token}")
            # a cube shown and closed leaves the web server to the apps' tabs, which say nothing when they close
            cube = ll.show(pd.DataFrame({"x": [1]}), browser=False, inline=False)
            cube.close()
            self.assertIs(datacube._session._web, web)
            self.assertEqual(sorted(datacube._session.apps), ["query", "studio"])
        finally:
            if datacube._session._web is not None:
                datacube._session._web.close()
            datacube._session = kept


class Cube(Served):
    """cube.json: what DataCube's engine page shows -- a frame's model, runtime and source, asked with the token."""

    def get(self, path, headers=None):
        request = urllib.request.Request(self.server.url + path, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.headers["Content-Type"], r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers["Content-Type"], e.read()

    def test_a_frame_s_cube_as_it_is_now(self):
        status, content_type, body = self.get("/cube.json?table=trades", {"Authorization": self.server.authorization})
        self.assertEqual((status, content_type), (200, "application/json"))
        self.assertEqual(
            json.loads(body),
            {
                "title": "trades",
                "model": self.table.model,
                "runtime": self.table.runtime,
                "source": self.table.source,
                "version": 0,
            },
        )
        # a Live frame read again: a new column, a new model
        self.df["extra"] = 1
        _, _, body = self.get("/cube.json?table=TRADES", {"Authorization": self.server.authorization})
        self.assertIn("extra", json.loads(body)["model"])

    def test_asked_with_the_token_only_and_for_a_frame_it_serves(self):
        self.assertEqual(self.get("/cube.json?table=trades")[0], 401)
        self.assertEqual(self.get("/cube.json?table=nope", {"Authorization": self.server.authorization})[0], 404)


class Page(Cube):
    """page.json: a page it serves (Python's ll.Page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md) -- its document
    and its version -- and version.json for a page: its own version and each of its frames'."""

    DOC: ClassVar[dict] = {
        "kind": "datacube.page",
        "version": 4,
        "name": "Q3",
        "cubes": [{"id": "grid-1", "cube": {"source": {"_type": "frame", "name": "trades", "columns": []}}}],
        "views": [],
        "sheets": [],
    }

    def serve_page(self, name, document):
        """``document`` served as page ``name`` over the frame trades, as the session serves a Python page (its keeper,
        told to serve it), written whole: what these routes answer from, whoever wrote it. Returns its version."""
        keeper = self.server.engine.keeper
        reads = {"frames": ["trades"], "copies": [], "ids": []}
        kept = keeper.current(name)
        version = (
            keeper.keep(name, document, reads, []) if kept is None else keeper.write(name, kept[0], document, reads, [])
        )
        keeper.serve(name, True)
        return version

    def test_a_page_served_its_document_and_version_moving_when_served_again(self):
        auth = {"Authorization": self.server.authorization}
        self.assertEqual(self.get("/page.json?page=q3", auth)[0], 404)
        self.assertEqual(self.serve_page("q3", self.DOC), 1)
        status, content_type, body = self.get("/page.json?page=q3", auth)
        self.assertEqual((status, content_type), (200, "application/json"))
        self.assertEqual(json.loads(body), {"version": 1, "page": self.DOC, "held": [], "hash": page_hash(self.DOC)})
        self.assertEqual(
            json.loads(self.get("/version.json?page=q3", auth)[2]), {"version": 1, "frames": {"trades": 0}}
        )
        # its frame changed: the frame's version moves, the page's not
        self.server.engine.changed("trades")
        self.assertEqual(
            json.loads(self.get("/version.json?page=q3", auth)[2]), {"version": 1, "frames": {"trades": 1}}
        )
        # served again: its version moves
        self.assertEqual(self.serve_page("q3", {**self.DOC, "name": "Q4"}), 2)
        self.assertEqual(json.loads(self.get("/page.json?page=q3", auth)[2])["page"]["name"], "Q4")
        self.server.engine.keeper.serve("q3", False)
        self.assertEqual(self.get("/version.json?page=q3", auth)[0], 404)

    def test_a_page_open_on_it_reads_it_whole_and_a_writer_the_last_of_its_writes_taken(self):
        # (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version) a page open on it follows by reading it
        # whole; asked by a writer, the last of its writes the keeper took comes with the page, read under one lock
        auth = {"Authorization": self.server.authorization}
        keeper = self.server.engine.keeper
        self.serve_page("q3", self.DOC)
        q4 = {**self.DOC, "name": "Q4"}
        self.serve_page("q3", q4)
        said = json.loads(self.get("/page.json?page=q3&follower=tab-1", auth)[2])
        self.assertEqual(
            said, {"version": 2, "page": q4, "held": [], "hash": page_hash(q4)}, "no writer asking: none said"
        )
        keeper.combine = lambda page, held, edits: {
            "page": {**page, "name": "Q5"},
            "held": held,
            "edits": edits,
            "reads": {"frames": ["trades"], "copies": [], "ids": []},
        }
        keeper.settle("q3", 2, [{"page": {"kind": "renamePage", "name": "Q5"}}], writer="w1", seq=3)
        taken = lambda writer: json.loads(self.get(f"/page.json?page=q3&writer={writer}", auth)[2])["taken"]
        self.assertEqual([taken("w1"), taken("w2")], [3, 0], "its own last write; none of another's")
        self.assertEqual(self.get("/page.json?page=q3&writer=a%20b", auth)[0], 400, "a writer said right or refused")
        # a fence (the middle version's audit's 4): answered as the writes were, its writes up to it taken no more
        self.assertEqual(json.loads(self.get("/page.json?page=q3&writer=w2&fence=4", auth)[2])["taken"], 0)
        self.assertEqual(taken("w2"), 4, "fenced")
        for bad in ("writer=w2&fence=0", "writer=w2&fence=x", "fence=4", "writer=w2&fence=%C2%B2"):
            self.assertEqual(self.get(f"/page.json?page=q3&{bad}", auth)[0], 400, bad)
        self.server.engine.keeper.serve("q3", False)
        self.assertEqual(self.get("/page.json?page=q3", auth)[0], 404, "closed: served no more")

    def test_a_page_asked_with_the_token_only(self):
        self.serve_page("q3", self.DOC)
        self.assertEqual(self.get("/page.json?page=q3")[0], 401)
        self.assertEqual(self.get("/version.json?page=q3")[0], 401)

    def test_a_pages_write_that_fails_unexpectedly_says_no_held_value(self):
        # the C5 follow-up's staging check (2026-11-15): a page's write the keeper fails on (DataCube's code, over a page
        # that may name a remote file by its URL) answered its raw words; it is the engine's one unexpected answer now
        engine = self.server.engine
        self.serve_page("q3", self.DOC)
        token = "X-Amz-Signature=held-0123456789"
        self.frames.held_values = lambda: (token,)

        def failing(*_args, **_kw):
            raise RuntimeError(f"DataCube could not rebase the page over https://bucket/f.parquet?{token}")

        written = json.dumps({"edits": [{"page": {"kind": "renamePage", "name": "Q5"}}], "writer": "w1", "seq": 1})
        with mock.patch.object(engine, "_headless", lambda: None), mock.patch.object(engine.keeper, "settle", failing):
            said = engine.answer("POST", "/page.json", "page=q3&version=1", written)
        text = said.body.decode("utf-8")
        self.assertEqual(said.status, 500, text)
        self.assertIn("RuntimeError: DataCube could not rebase the page", text)
        self.assertNotIn("held-0123456789", text)

    def test_an_open_pages_write_is_read_by_datacubes_own_reader(self):
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) an engine with no DataCube code to read a page with keeps no page an
        # open page writes; one with it: test_page.py Live
        engine = self.server.engine
        self.serve_page("q3", self.DOC)
        said = json.dumps({**self.DOC, "name": "Renamed there"})
        self.assertEqual(engine.answer("POST", "/page.json", "page=q3", said).status, 400, "no version named")
        self.assertEqual(engine.answer("POST", "/page.json", "page=q3&version=1", said).status, 404)
        self.assertEqual(engine.answer("POST", "/page.json", "page=nope&version=1", said).status, 404)

    def test_a_pages_numbers_exact_both_ways(self):
        # a calculated column's 12.30 is 12.30, an integer past 2**53 every digit (the protocol's exact JSON)
        lambda_ = {
            "_type": "lambda",
            "body": [{"_type": "decimal", "value": Decimal("12.30")}, {"_type": "integer", "value": 2**70}],
            "parameters": [],
        }
        self.serve_page("q3", {**self.DOC, "lambda": lambda_})
        body = self.get("/page.json?page=q3", {"Authorization": self.server.authorization})[2].decode("utf-8")
        self.assertIn("12.30", body)
        self.assertIn(str(2**70), body)


class WarehouseOnPython(unittest.TestCase):
    """A warehouse table on Python's page (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3), every route of /warehouse.json
    and the plan-and-run over its model, against a stand-in for the driver (legend_lite.warehouse's Warehouse and
    connect): the shapes refused, the sign-in, a table opened once as one person, its Live rows and receipt, each snap
    a copy of its own modelled by DataCube's code and released by its table, the snap limit, and an expired session
    said as one."""

    URL = "https://wh.example"
    OBJECTS: ClassVar[list] = [
        {
            "catalog": "main",
            "databaseType": "DuckDB",
            "schema": "trading",
            "name": "POSITIONS",
            "kind": "table",
            "columns": [
                {
                    "name": "desk",
                    "type": "VARCHAR",
                    "logicalType": "VARCHAR",
                    "precision": None,
                    "scale": None,
                    "notNull": False,
                },
                {
                    "name": "qty",
                    "type": "INTEGER",
                    "logicalType": "INTEGER",
                    "precision": None,
                    "scale": None,
                    "notNull": False,
                },
            ],
        }
    ]

    def setUp(self):
        from legend_lite import warehouse

        self.warehouse = warehouse
        outer = self
        self.count, self.expired, self.principal, self.pulls = 3, False, "rita", 0
        # a stream tampered (the warehouse counts a row more than it sends) or cut short (its read fails after a chunk)
        self.tampered, self.cut = False, False
        # a count the warehouse answers with no row (G7: never read as no rows to copy)
        self.uncounted = False
        # a slow stream: said when its rows begin, held until let go (the final audit's item 3)
        self.streaming, self.slow = threading.Event(), None
        # every statement the warehouse was handed, in order
        self.asked = []

        class Reader:
            def __init__(self, session, catalog="main", clock=None):
                self._session, self.catalog = session, catalog

            session = property(lambda self: self._session)
            principal = property(lambda self: self._session.principal)
            base_url = property(lambda self: self._session.base_url)

            def renew(self, session):
                self._session = session

            def objects(self):
                if outer.expired:
                    raise warehouse.SessionExpired(outer.URL, self.principal)
                return outer.OBJECTS

            def arrow_chunks(self, sql, on_receipt=None, params=()):
                if outer.expired:
                    raise warehouse.SessionExpired(outer.URL, self.principal)
                if on_receipt is not None:
                    on_receipt(
                        {
                            "statementId": "s1",
                            "serverRows": 4 if outer.tampered else 3,
                            "as": self.principal,
                            "where": "the warehouse at wh",
                        }
                    )
                outer.pulls += 1
                outer.asked.append(str(sql))
                rows = (
                    pa.table({"n": pa.array([] if outer.uncounted else [outer.count], pa.int64())})
                    if "COUNT(*)" in str(sql).upper()
                    else pa.table({"desk": ["FX", "EQ", "FX"], "qty": pa.array([10, 5, 3], pa.int32())})
                )
                sink = pa.BufferOutputStream()
                with pyarrow.ipc.new_stream(sink, rows.schema) as w:
                    w.write_table(rows)
                if outer.slow is not None and "COUNT(*)" not in str(sql).upper():
                    outer.streaming.set()
                    outer.slow.wait(60)
                yield sink.getvalue().to_pybytes()
                if outer.cut and "COUNT(*)" not in str(sql).upper():
                    raise warehouse.WarehouseError("the connection was reset", "RESET")

        def connect(url, user, password):
            if password != "pw":
                raise warehouse.WarehouseError("sign-in failed — AUTH_INVALID: wrong user or password", "AUTH_INVALID")
            return warehouse.Session(url, "tok", user, "2099-01-01T00:00:00Z"), outer.OBJECTS

        patches = [mock.patch.object(warehouse, "Warehouse", Reader), mock.patch.object(warehouse, "connect", connect)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.frames = ll.Frames()
        # DataCube's own code over the frames' database, headless: what models a snap's copy (upload.ts modelLanded)
        self.headless = None

        def headless():
            if self.headless is None:
                from legend_lite._headless import Headless, bundle

                self.headless = Headless(bundle(), connection=self.frames.connection)
            return self.headless

        self.addCleanup(lambda: self.headless is not None and self.headless.close())
        self.engine = Engine(self.frames, headless=headless)

    def call(self, said, path="/warehouse.json"):
        a = self.engine.answer("POST", path, "", json.dumps(said))
        return a.status, a.body

    def opened(self, user="rita"):
        self.assertEqual(self.call({"op": "connect", "url": self.URL, "user": user, "password": "pw"})[0], 200)
        status, body = self.call(
            {"op": "table", "url": self.URL, "catalog": "main", "schema": "trading", "name": "POSITIONS"}
        )
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def query(self, m):
        spec = {
            "table": "POSITIONS",
            "schema": "trading",
            "convertible": False,
            "databaseType": "DuckDB",
            "columns": [
                {("dataType" if k == "type" else k): v for k, v in c.items()} for c in self.OBJECTS[0]["columns"]
            ],
        }
        accessor = ll.table_model(spec)["accessor"]
        return json.dumps(ll.parse(f"|{accessor}->select(~[desk, qty])"))

    def plan_and_run(self, m, runtime):
        a = self.engine.answer(
            "POST",
            "/plan-and-run.json",
            "",
            json.dumps({"lambda": self.query(m), "model": m["model"], "runtime": runtime}),
        )
        return a

    def test_the_shapes_refused_and_a_table_only_once_signed_in(self):
        self.assertEqual(self.call({})[0], 400)
        self.assertEqual(self.engine.answer("POST", "/warehouse.json", "", "not json").status, 400)
        self.assertEqual(self.call({"op": "connect", "url": self.URL, "user": "rita"})[0], 400)
        self.assertEqual(self.call({"op": "nope", "url": self.URL})[0], 400)
        status, body = self.call(
            {"op": "table", "url": self.URL, "catalog": "main", "schema": "trading", "name": "POSITIONS"}
        )
        self.assertEqual((status, body.decode()), (401, f"sign in to the warehouse at {self.URL} first"))
        status, body = self.call({"op": "connect", "url": self.URL, "user": "rita", "password": "wrong"})
        self.assertEqual((status, body.decode()), (422, "sign-in failed — AUTH_INVALID: wrong user or password"))

    def test_signed_in_the_session_without_its_token_and_a_table_not_listed_refused(self):
        status, body = self.call({"op": "connect", "url": self.URL, "user": "rita", "password": "pw"})
        said = json.loads(body)
        self.assertEqual((status, said["session"]["token"], said["session"]["principal"]), (200, "", "rita"))
        self.assertEqual(
            self.call({"op": "table", "url": self.URL, "catalog": "main", "schema": "trading", "name": "secret"})[0],
            404,
        )

    def test_live_rows_from_the_warehouse_with_its_receipt(self):
        m = self.opened()
        a = self.plan_and_run(m, m["runtime"])
        self.assertEqual(a.status, 200, a.body)
        rows = pyarrow.ipc.open_stream(a.body).read_all()
        self.assertEqual(
            sorted(zip(rows["desk"].to_pylist(), rows["qty"].to_pylist())), [("EQ", 5), ("FX", 3), ("FX", 10)]
        )
        self.assertEqual(
            json.loads(rows.schema.metadata[b"legend.receipt"]),
            {"plane": "warehouse", "where": "the warehouse at wh", "as": "rita", "statementId": "s1", "serverRows": 3},
        )

    def test_one_person_per_open_table(self):
        self.opened("rita")
        self.assertEqual(self.call({"op": "connect", "url": self.URL, "user": "bob", "password": "pw"})[0], 200)
        status, body = self.call(
            {"op": "table", "url": self.URL, "catalog": "main", "schema": "trading", "name": "POSITIONS"}
        )
        self.assertEqual(status, 409, body)
        self.assertIn("open already as rita", body.decode())

    def test_a_failure_no_route_answers_is_a_500_naming_it(self):
        # the C5 follow-up's audit item 3 (2026-11-15): a snap over a served frame reads the frames as they are now
        # (Frames.serving), outside any route's catch; a live frame failing there closed the socket ("Failed to fetch"),
        # the error on stderr alone. It is a 500 naming it, its words with every held value taken out
        calls = []

        def frame():
            calls.append(1)
            if len(calls) > 1:
                raise RuntimeError("the live frame could not be read: X-Amz-Signature=held-0123456789")
            return pd.DataFrame({"desk": ["FX"], "qty": [1]})

        table = self.frames.register("broken", frame)
        self.frames.held_values = lambda: ("X-Amz-Signature=held-0123456789",)
        pull = json.dumps(ll.parse("|" + table.accessor + "->select(~[desk, qty])"))
        server = WebServer(self.engine)
        self.addCleanup(server.close)
        request = urllib.request.Request(
            server.url + "/snap.json",
            json.dumps({"op": "snap", "model": table.model, "runtime": table.runtime, "pull": pull}).encode("utf-8"),
            {"Content-Type": "application/json", "Authorization": server.authorization},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as r:
                status, body = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read()
        said = body.decode("utf-8")
        self.assertEqual(status, 500, said)
        self.assertIn("RuntimeError: the live frame could not be read", said)
        self.assertNotIn("held-0123456789", said)

    def test_a_served_snaps_count_and_copy_say_no_held_value(self):
        # the C5 follow-up's audit item 1 (2026-11-15): a served snap's count and its copy failed with DuckDB's own words,
        # which repeat a presigned link's URL (its signature) -- answered as they were. Each failure's words now have
        # every value the engine holds taken out
        import duckdb

        from legend_lite import engine as engine_module

        token = "https://bucket.example/f.parquet?X-Amz-Signature=sig-0123456789abcdef"
        self.frames.held_values = lambda: (token,)
        table = self.frames.register("served", pd.DataFrame({"desk": ["FX", "EQ"], "qty": [1, 2]}))
        pull = json.dumps(ll.parse("|" + table.accessor + "->select(~[desk, qty])"))
        asked = json.dumps({"op": "snap", "model": table.model, "runtime": table.runtime, "pull": pull})

        def failing(*_args, **_kw):
            raise duckdb.IOException(f"HTTP Error: 403 (Forbidden) reading {token}")

        for name in ("count_of", "run_write"):
            with self.subTest(failing=name), mock.patch.object(engine_module, name, failing):
                answer = self.engine.answer("POST", "/snap.json", "", asked)
                said = answer.body.decode()
                self.assertEqual(answer.status, 422, said)
                self.assertIn("HTTP Error: 403", said)
                self.assertNotIn("sig-0123456789abcdef", said)
                self.assertNotIn("bucket.example", said)

    def snapped(self, m):
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def copy_rows(self, copy):
        """The copy's rows, planned over Python's own model of it and run on the frames' database."""
        path = copy["source"]["value"]["path"]
        lam = json.dumps(ll.parse(f"|#>{{{path[0]}.{path[1]}}}#->select(~[desk, qty])"))
        a = self.engine.answer(
            "POST",
            "/plan-and-run.json",
            "",
            json.dumps({"lambda": lam, "model": copy["model"], "runtime": copy["runtime"]}),
        )
        return a.status, a.body

    def test_a_warehouse_snap_lands_exactly_through_its_planned_stream(self):
        # the user's ruling of 2026-11-11: the copy is a planned load whose source is the warehouse's stream, its rows
        # and types exactly the warehouse's, its count the copy's own as its database counted it
        m = self.opened()
        copy = self.snapped(m)
        self.assertEqual((copy["table"], copy["rowCount"], copy["pulledBy"]["serverRows"]), ("snap", 3, 3))
        con = self.frames.connection._con
        self.assertEqual(
            con.execute(
                "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = 'snap' ORDER BY column_index"
            ).fetchall(),
            [("desk", "VARCHAR"), ("qty", "INTEGER")],
        )
        self.assertEqual(
            sorted(con.execute("SELECT desk, qty FROM main.snap").fetchall()), [("EQ", 5), ("FX", 3), ("FX", 10)]
        )

    def test_a_tampered_or_cut_warehouse_stream_rolls_back_and_leaves_no_copy(self):
        m = self.opened()
        exists = lambda t: self.frames.connection._con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [t]
        ).fetchone()[0]
        self.tampered = True
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(status, 422, body)
        self.assertIn(
            "the stream from 'warehouse' landed 3 rows in main.snap, where its plane counted 4: nothing of it is kept",
            body.decode(),
        )
        self.assertEqual(exists("snap"), 0)
        self.tampered, self.cut = False, True
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(status, 422, body)
        self.assertIn("the connection was reset", body.decode())
        self.assertEqual(exists("snap_2"), 0)
        self.cut = False
        self.assertEqual(self.snapped(m)["rowCount"], 3, "a whole stream lands again")

    def test_each_snap_a_copy_of_its_own_read_through_its_own_model_and_released_by_its_table(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3: the warehouse table's own name was every grid's copy, so a
        # second grid got the first's rows as they were, and the copy was read through the table's model
        m = self.opened()
        first = self.snapped(m)
        second = self.snapped(m)
        self.assertEqual(
            (first["table"], second["table"], self.pulls),
            ("snap", "snap_2", 4),
            "each grid pulls and lands its own copy (a count and a pull each)",
        )
        self.assertEqual((first["rowCount"], first["pulledBy"]["as"]), (3, "rita"))
        self.assertNotEqual(first["model"], m["model"], "a copy is read through a model of its own")
        status, body = self.copy_rows(first)
        self.assertEqual(status, 200, body)
        rows = pyarrow.ipc.open_stream(body).read_all()
        self.assertEqual(
            sorted(zip(rows["desk"].to_pylist(), rows["qty"].to_pylist())), [("EQ", 5), ("FX", 3), ("FX", 10)]
        )
        # a copy held, described for a page following a snap made in Python (§18.8): its table and model, as made
        status, body = self.call({"op": "held", "table": "snap"}, "/snap.json")
        self.assertEqual(status, 200, body)
        held = json.loads(body)
        self.assertEqual((held["table"], held["model"], held["source"]), ("snap", first["model"], first["source"]))
        self.assertEqual(self.call({"op": "release", "table": "snap"}, "/snap.json")[0], 200)
        self.assertEqual(self.call({"op": "held", "table": "snap"}, "/snap.json")[0], 404, "let go: held no more")
        self.assertEqual(self.call({"op": "held", "table": "POSITIONS"}, "/snap.json")[0], 404, "no copy a snap made")
        self.assertEqual(self.copy_rows(second)[0], 200, "the other grid still reads its own")
        self.assertEqual(
            self.frames.connection._con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'snap'"
            ).fetchone()[0],
            0,
            "the copy is dropped",
        )
        self.assertIsNone(self.frames.file_table("snap"), "and served no more")
        # only a copy a snap made is let go of this way; one let go already is gone (a late release, a tab's closing
        # grid's after Python let its page's copy go: §18.8), its name never given to another copy
        self.assertEqual(self.call({"op": "release", "table": "snap"}, "/snap.json")[0], 200)
        self.assertEqual(self.call({"op": "release", "table": "POSITIONS"}, "/snap.json")[0], 404)
        self.assertEqual(self.snapped(m)["table"], "snap_3", "a new copy takes no name a copy had")
        self.assertEqual(self.call({"op": "release", "table": "snap"}, "/snap.json")[0], 200)
        self.assertEqual(
            self.call({"op": "held", "table": "snap_3"}, "/snap.json")[0], 200, "a late release lets no other copy go"
        )
        # nor is it a page's file to drop
        self.assertEqual(
            self.engine.answer("POST", "/forget.json", "", json.dumps({"name": "x", "table": "snap_2"})).status, 404
        )
        # a copy a page the keeper keeps reads (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): the keeper's -- its maker's
        # release keeps it -- and it goes with the last page that reads it
        # (what a page reads comes with each write, as DataCube says it: headless pageReads)
        reads = {"frames": [], "copies": ["snap_2"], "ids": []}
        self.engine.keeper.keep("p", {"name": "p"}, reads, [])
        self.engine.keeper.keep("q", {"name": "q"}, reads, [])
        status, body = self.call({"op": "release", "table": "snap_2"}, "/snap.json")
        self.assertEqual(
            (status, json.loads(body)), (200, {"kept": "a page reads it: it goes when nothing reads it any more"})
        )
        self.assertEqual(self.copy_rows(second)[0], 200, "kept")
        self.engine.keeper.forget("p")
        self.engine.sweep()
        self.assertEqual(self.call({"op": "held", "table": "snap_2"}, "/snap.json")[0], 200, "another page reads it")
        self.engine.keeper.write("q", 1, {"name": "q, live"}, {"frames": [], "copies": [], "ids": []}, [])
        self.engine.sweep()
        self.assertEqual(
            self.call({"op": "held", "table": "snap_2"}, "/snap.json")[0], 404, "let go with the last page"
        )

    def test_a_copy_lives_until_every_page_open_on_it_moved_past_it(self):
        # the keeper's known limit 6, closed (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): a page open on another -- a
        # tab, a widget -- at a version that reads a copy still reads it, its queries and the commands it follows by,
        # until it says it shows a later one (as it asks, follows and writes), or says nothing for FOLLOWED_FOR seconds
        m = self.opened()
        copy = self.snapped(m)["table"]
        keeper = self.engine.keeper
        keeper.keep("p", {"name": "p"}, {"frames": [], "copies": [copy], "ids": []}, [])
        keeper.serve("p", True)

        def asked(path, query):
            return self.engine.answer("GET", path, query, None).status

        def held():
            return self.call({"op": "held", "table": copy}, "/snap.json")[0]

        self.assertEqual(asked("/version.json", "page=p&follower=tab-1&at=1"), 200)
        # Python goes live: version 2 reads no copy
        keeper.write("p", 1, {"name": "p, live"}, {"frames": [], "copies": [], "ids": []}, [])
        self.engine.sweep()
        self.assertEqual(held(), 200, "the open page shows version 1, which reads it")
        self.assertEqual(asked("/version.json", "page=p&follower=tab-1"), 200, "following: no version said")
        self.assertEqual(self.call({"op": "release", "table": copy}, "/snap.json")[0], 200)
        self.assertEqual(held(), 200, "its maker's release keeps it: the keeper's")
        self.assertEqual(asked("/page.json", "page=p&follower=tab-1"), 200)
        self.assertEqual(held(), 200, "reading the page: the version it said last stands")
        self.assertEqual(asked("/version.json", "page=p&follower=tab-1&at=2"), 200)
        self.assertEqual(held(), 404, "moved past it: let go as it said so")
        # who it is, and the version it shows, said right or refused
        self.assertEqual(asked("/version.json", "page=p&at=2"), 400)
        self.assertEqual(asked("/version.json", "page=p&follower=a%20b&at=2"), 400)
        self.assertEqual(asked("/version.json", "page=p&follower=tab-1&at=two"), 400)
        self.assertEqual(asked("/page.json", "page=p"), 200, "a request that names no follower says nothing")

    def test_the_dry_run_plans_a_table_as_its_run_does_and_refuses_the_runtime_its_run_refuses(self):
        # plan.json, plan-and-run's planning half (the one-app design §15.10): never run, the run's plan, and the run's
        # refusal of a runtime (the table's rule, refused_runtime) word for word
        m = self.opened()

        def dry(runtime):
            a = self.engine.answer(
                "POST",
                "/plan.json",
                "",
                sent("/plan.json", {"lambda": self.query(m), "model": m["model"], "runtime": runtime}),
            )
            self.assertEqual(a.status, 200, a.body)
            return json.loads(a.body)["plan"]

        planned = dry(m["runtime"])
        self.assertEqual(self.pulls, 0, "a dry run reads nothing from the warehouse")
        run = self.plan_and_run(m, m["runtime"])
        self.assertEqual(run.status, 200, run.body)
        self.assertEqual(planned, pyarrow.ipc.open_stream(run.body).read_all().schema.metadata[b"legend.plan"].decode())
        # another runtime: refused by both, in the same words
        refused = dry("other::Runtime")
        self.assertIn("is read on its warehouse, through its own runtime", refused)
        self.assertEqual(refused, json.loads(self.plan_and_run(m, "other::Runtime").body)["plan"])

    def test_a_copy_that_fails_leaves_nothing(self):
        m = self.opened()
        made = self.engine._headless

        class Refusing:
            def __init__(self, inner):
                self.inner = inner

            def call(self, function, *args, **kw):
                if function == "landed":
                    raise RuntimeError("no model of it")
                return self.inner.call(function, *args, **kw)

        self.engine._headless = lambda: Refusing(made())
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(status, 422, body)
        self.assertIn("could not be copied", body.decode())
        self.assertEqual(
            self.frames.connection._con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'snap'"
            ).fetchone()[0],
            0,
            "no copy left",
        )
        self.assertIsNone(self.frames.file_table("snap"))

    def test_a_frames_grid_snaps_too_a_copy_of_its_base_as_the_frame_was(self):
        # one Snap for every grid (§18.3): a Live frame read afresh by every query, its grid's copy keeping its rows
        data = {"df": pd.DataFrame({"desk": ["FX", "EQ"], "qty": [1, 2]})}
        desks = self.frames.register("desks", lambda: data["df"])
        lam = json.dumps(ll.parse(f"|{desks.accessor}->select(~[desk, qty])"))
        status, body = self.call(
            {"op": "snap", "model": desks.model, "runtime": desks.runtime, "pull": lam}, "/snap.json"
        )
        self.assertEqual(status, 200, body)
        copy = json.loads(body)
        self.assertEqual((copy["table"], copy["rowCount"], "pulledBy" in copy), ("snap", 2, False))
        data["df"] = pd.DataFrame({"desk": ["FX", "EQ", "IR"], "qty": [1, 2, 3]})
        self.assertEqual(desks.execute("->select(~[desk])").num_rows, 3, "the frame, read afresh")
        status, rows = self.copy_rows(copy)
        self.assertEqual(status, 200, rows)
        self.assertEqual(pyarrow.ipc.open_stream(rows).read_all().num_rows, 2, "its copy, as it was snapped")
        self.assertEqual(self.call({"op": "release", "table": "snap"}, "/snap.json")[0], 200)
        # a model reading a table this engine does not serve is not snapped
        secret = ll.table_model(
            {
                "table": "secret",
                "convertible": True,
                "databaseType": "DuckDB",
                "columns": [
                    {
                        "name": "desk",
                        "dataType": "VARCHAR",
                        "logicalType": "VARCHAR",
                        "precision": None,
                        "scale": None,
                        "notNull": False,
                    }
                ],
            }
        )
        status, body = self.call(
            {"op": "snap", "model": secret["model"], "runtime": secret["runtime"], "pull": lam}, "/snap.json"
        )
        self.assertEqual(status, 404, body)

    def test_an_engine_without_datacubes_code_keeps_no_copy(self):
        self.engine = Engine(self.frames)
        m = self.opened()
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(status, 404, body)
        self.assertIn("keeps no copy", body.decode())

    def test_the_snap_limit_in_the_tabs_words(self):
        m = self.opened()
        self.count = 10_000_001
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(
            (status, body.decode()),
            (422, "10,000,001 rows exceeds the 10,000,000 row snap limit. Narrow the filter, or stay live."),
        )

    def test_a_second_read_and_snap_are_served_while_a_slow_warehouse_snap_streams(self):
        # the final audit's item 3 (2026-11-12): the warehouse snap streamed holding the frames' lock and DataCube's
        # turn, so a slow warehouse held every read of the frames and every call into DataCube's code meanwhile
        m = self.opened()
        frame = self.frames.register("books", pa.table({"title": ["a", "b"], "pages": pa.array([10, 20], pa.int64())}))
        self.slow = threading.Event()
        snapped: dict = {}
        streaming = threading.Thread(
            target=lambda: snapped.update(
                answer=self.call(
                    {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
                )
            )
        )
        streaming.start()
        try:
            self.assertTrue(self.streaming.wait(60), "the snap never began streaming")
            served: dict = {}

            def second():
                pull = json.dumps(ll.parse("|" + frame.accessor + "->select(~[title, pages])"))
                served["read"] = self.engine.answer(
                    "POST",
                    "/plan-and-run.json",
                    "",
                    json.dumps({"lambda": pull, "model": frame.model, "runtime": frame.runtime}),
                )
                served["snap"] = self.call(
                    {"op": "snap", "model": frame.model, "runtime": frame.runtime, "pull": pull}, "/snap.json"
                )

            reader = threading.Thread(target=second)
            reader.start()
            reader.join(60)
            self.assertFalse(reader.is_alive(), "a read of the frames waited on the warehouse snap streaming")
            self.assertEqual(served["read"].status, 200, served["read"].body[:300])
            status, body = served["snap"]
            self.assertEqual((status, json.loads(body)["rowCount"]), (200, 2), body)
            self.assertTrue(streaming.is_alive(), "the warehouse snap was still streaming meanwhile")
            # its name is reserved while it streams: the frame's copy took another
            self.assertNotEqual(json.loads(body)["table"], "snap")
        finally:
            self.slow.set()
            streaming.join(60)
        status, body = snapped["answer"]
        self.assertEqual((status, json.loads(body)["table"], json.loads(body)["rowCount"]), (200, "snap", 3), body)

    def test_a_snaps_count_is_the_planners_plan_of_the_pulls_size(self):
        # the final audit's item 1 (2026-11-12): the pull's statement was handed back to the planner as a host
        # operation's query to count; the count is now the planner's own plan of the pull's size()
        from legend_lite.engine import _count_of

        m = self.opened()
        pull = self.query(m)
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": pull}, "/snap.json"
        )
        self.assertEqual(status, 200, body)
        counted = ll.plan(m["model"], json.loads(_count_of(pull)), m["runtime"])
        self.assertEqual(self.asked[0], counted.sql)
        self.assertIn("COUNT(*)", counted.sql)
        with self.assertRaises(ValueError) as refused:
            _count_of(json.dumps({"_type": "lambda", "parameters": [], "body": [{"_type": "integer", "value": 1}] * 2}))
        self.assertEqual(str(refused.exception), "a snap's pull is one expression, not 2")

    def test_a_count_the_warehouse_did_not_answer_copies_nothing(self):
        # G7 (the final audit's item 4, 2026-11-12): a count of no row was read as 0 rows -- under the limit, copied on
        m = self.opened()
        self.uncounted = True
        status, body = self.call(
            {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
        )
        self.assertEqual(
            (status, body.decode()),
            (422, "the snap could not be counted: the warehouse answered 0 row(s), not its rows' count"),
        )
        self.assertEqual(self.pulls, 1, "the count alone was read: nothing streamed")

    def test_an_expired_session_said_as_one(self):
        m = self.opened()
        self.expired = True
        a = self.plan_and_run(m, m["runtime"])
        said = json.loads(a.body)
        self.assertEqual((a.status, said["expired"]), (401, {"baseUrl": self.URL, "principal": "rita"}))
        self.assertEqual(
            self.call(
                {"op": "snap", "model": m["model"], "runtime": m["runtime"], "pull": self.query(m)}, "/snap.json"
            )[0],
            401,
        )
        self.assertEqual(
            self.call({"op": "table", "url": self.URL, "catalog": "main", "schema": "trading", "name": "POSITIONS"})[0],
            401,
        )
