"""The engine (legend_lite.engine): legend-engine's pure/v1 API over frames, as DataCube's remote client calls it --
parse, print and a query's types answered by legend-lite's server code, execute in upstream's Arrow format with
the rows DuckDB computes over the frame as it is then; the refusals in the engine's error shape; and only the
engine's own page answered (its token, a local Host, no cross-origin answer)."""

import http.client
import json
from decimal import Decimal
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
from unittest import mock
from pathlib import Path
import urllib.error
import urllib.request

import pandas as pd
import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite.engine import Engine, NativePlanner, WebServer
from legend_lite._home import THIS_USER, locked

GROUP = "->filter(x|$x.qty > 5)->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])->sort(~desk->ascending())"


def trades():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'desk': ['FX', 'EQ', 'FX', 'RATES', 'EQ'],
        'qty': [10.5, 20.0, 30.25, 1.0, 7.5],
    })


class Served(unittest.TestCase):
    """An engine over one Live frame, and requests as DataCube's client sends them."""

    def setUp(self):
        self.df = trades()
        self.frames = ll.Frames()
        self.table = self.frames.register('trades', self.df)
        self.server = WebServer(Engine(self.frames))

    def tearDown(self):
        self.server.close()

    def post(self, path, body, content_type='application/json', headers=None):
        """The engine's answer to a POST: status, headers, body bytes."""
        sent = {'Content-Type': content_type, 'Authorization': self.server.authorization} | (headers or {})
        request = urllib.request.Request(self.server.url + path, body.encode('utf-8'), sent, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    def query(self, steps):
        """A cube query as the client sends it: over the table, then ->from(runtime) (fromRuntime)."""
        return ll.parse('|' + self.table.accessor + steps + '->from(' + self.table.runtime + ')')

    def execute_input(self, tree, model=None):
        return json.dumps({
            'clientVersion': 'vX_X_X',
            'function': tree,
            'model': {'_type': 'text', 'code': self.table.model if model is None else model},
            'context': {'_type': 'BaseExecutionContext', 'queryTimeOutInSeconds': 60, 'enableConstraints': True},
        })

    def arrow(self, steps):
        status, headers, body = self.post('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                          self.execute_input(self.query(steps)))
        self.assertEqual(status, 200, body[:300])
        return headers, read_arrow(body)


def read_arrow(body):
    """An ARROW_IPC answer read as upstream writes it: one zstd frame around an Arrow IPC stream."""
    with pa.input_stream(pa.py_buffer(body), compression='zstd') as stream:
        return pyarrow.ipc.open_stream(stream).read_all()


class Calls(Served):
    def test_parse_print_and_types_are_the_compilers(self):
        text = '|' + self.table.accessor + GROUP
        status, headers, body = self.post('/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false',
                                          text, 'text/plain')
        self.assertEqual((status, headers['Content-Type']), (200, 'application/json'))
        tree = json.loads(body)
        self.assertEqual(tree, ll.parse(text))

        status, headers, body = self.post('/api/pure/v1/grammar/jsonToGrammar/lambda?renderStyle=STANDARD', json.dumps(tree))
        self.assertEqual((status, headers['Content-Type']), (200, 'text/plain'))
        self.assertEqual(body.decode('utf-8'), ll.print_tree(tree, 'STANDARD'))

        status, _, body = self.post('/api/pure/v1/compilation/lambdaRelationType',
                                    json.dumps({'lambda': tree, 'model': {'_type': 'text', 'code': self.table.model}}))
        self.assertEqual(status, 200, body)
        self.assertEqual([(c['name'], c['genericType']['rawType']['fullPath']) for c in json.loads(body)['columns']],
                         [('desk', 'String'), ('q', 'Float')])

    def test_execute_answers_upstreams_arrow_with_the_rows_duckdb_computes(self):
        headers, rows = self.arrow(GROUP)
        self.assertEqual((headers['Content-Type'], headers['x-legend-response-format']), ('application/json', 'FormatNotSet'))
        self.assertEqual(rows.to_pylist(), self.table.execute(GROUP).to_pylist())
        self.assertEqual(rows.to_pylist(), [{'desk': 'EQ', 'q': 27.5}, {'desk': 'FX', 'q': 40.75}])
        metadata = {k.decode(): v.decode() for k, v in rows.schema.metadata.items()}
        self.assertEqual(list(metadata), ['legend.builder', 'legend.activities', 'legend.columns'])
        self.assertEqual(json.loads(metadata['legend.columns']), ['desk', 'q'])
        self.assertEqual(json.loads(metadata['legend.builder']), {'_type': 'tdsBuilder', 'columns': [
            # the relational spellings as legend-engine writes them (legend-lite's server's builder)
            {'name': 'desk', 'type': 'String', 'relationalType': 'VARCHAR(1024)'},
            {'name': 'q', 'type': 'Float', 'relationalType': 'FLOAT'},
        ]})
        [activity] = json.loads(metadata['legend.activities'])
        self.assertEqual(list(activity), ['comment', 'sql'])
        self.assertEqual(activity['sql'], self.table.plan(GROUP).sql)

    def test_an_empty_answer_is_its_columns_and_no_batch_as_upstreams(self):
        status, _, body = self.post('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                    self.execute_input(self.query("->filter(x|$x.desk == 'NONE')")))
        self.assertEqual(status, 200, body[:300])
        with pa.input_stream(pa.py_buffer(body), compression='zstd') as stream:
            reader = pyarrow.ipc.open_stream(stream)
            self.assertEqual((reader.schema.names, list(reader)), (['id', 'desk', 'qty'], []))

    def test_live_the_next_query_reads_the_frame_as_it_is_then(self):
        _, before = self.arrow(GROUP)
        self.df.loc[1, 'qty'] = 100.0
        _, after = self.arrow(GROUP)
        self.assertNotEqual(after.to_pylist(), before.to_pylist())
        self.assertEqual(after.to_pylist(), [{'desk': 'EQ', 'q': 107.5}, {'desk': 'FX', 'q': 40.75}])

    def test_requests_at_once_are_each_answered(self):
        answers = [None] * 12

        def ask(i):
            # a request that fails says how in its place, rather than leaving it None
            try:
                answers[i] = self.arrow(GROUP)[1].to_pylist()
            except Exception as failure:
                answers[i] = repr(failure)

        threads = [threading.Thread(target=ask, args=(i,)) for i in range(len(answers))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(answers, [[{'desk': 'EQ', 'q': 27.5}, {'desk': 'FX', 'q': 40.75}]] * len(answers))
        self.assertEqual(ll.compiler.library().unfreed(), 0)


    def test_a_burst_of_connections_waits_its_turn(self):
        # the listening queue holds a burst, never socketserver's five: macOS resets a connection the queue has no room
        # for (the engine's comment says when it was seen); the burst itself depends on the machine's load, so the
        # queue's length is what is held here
        self.assertGreaterEqual(self.server._server.request_queue_size, 128)


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
PARTY_QUERY = ('|demo::party::Party.all()->project(~[name: p|$p.name])->sort(~name->ascending())'
               '->from(demo::party::PartyMapping, demo::party::Runtime)')


class ProjectQueries(Served):
    """A project's query run on Python (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): Query or Studio served
    here sends a model none of the frames wrote, with its own Data rows; Python makes that project's database inside its
    DuckDB (the native library's statements), runs the query there on a cursor of its own, and the frames stay as they
    were."""

    def project_input(self, model=PARTY):
        return json.dumps({'clientVersion': 'vX_X_X', 'function': ll.parse(PARTY_QUERY),
                           'model': {'_type': 'text', 'code': model},
                           'context': {'_type': 'BaseExecutionContext', 'queryTimeOutInSeconds': 60}})

    def test_a_projects_query_answers_its_own_rows(self):
        status, _, body = self.post('/api/pure/v1/execution/execute', self.project_input())
        self.assertEqual(status, 200, body)
        self.assertEqual([r['values'] for r in json.loads(body)['result']['rows']],
                         [['Halberd Securities'], ['Meridian Capital']])
        status, _, body = self.post('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC', self.project_input())
        self.assertEqual(status, 200, body[:300])
        self.assertEqual(read_arrow(body).column('name').to_pylist(), ['Halberd Securities', 'Meridian Capital'])
        # one database for the project, asked twice; beside the frames, which answer as before
        self.assertEqual(len(self.server.engine.projects.names), 1)
        _, rows = self.arrow('->select(~[desk])->distinct()->sort(~desk->ascending())')
        self.assertEqual(rows.column('desk').to_pylist(), ['EQ', 'FX', 'RATES'])
        # nothing of the project's beside the frames: its table is in its own database alone
        main = self.frames.connection.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database() AND table_name = 'PARTY'")
        self.assertEqual(main.fetchone()[0], 0)

    def test_each_version_its_own_database(self):
        later = PARTY.replace('2,Halberd Securities', '3,Kestrel Partners')
        for model, want in ((PARTY, [['Halberd Securities'], ['Meridian Capital']]),
                            (later, [['Kestrel Partners'], ['Meridian Capital']]),
                            (PARTY, [['Halberd Securities'], ['Meridian Capital']])):
            status, _, body = self.post('/api/pure/v1/execution/execute', self.project_input(model))
            self.assertEqual(status, 200, body)
            self.assertEqual([r['values'] for r in json.loads(body)['result']['rows']], want)
        self.assertEqual(len(self.server.engine.projects.names), 2)

    def test_a_page_plans_and_runs_a_projects_query_on_its_own_rows(self):
        # a saved query opened on Python's page (C5): planned over the project's model held beside its database, its
        # mapping in the query, and run there -- and its dry run (the page's compile check) planned over that model too.
        # Its rows are its project's because the request says so (``project``, F28)
        asked = {'lambda': json.dumps(ll.parse(PARTY_QUERY)), 'model': PARTY, 'runtime': 'demo::party::Runtime'}
        status, headers, body = self.post('/plan-and-run.json', json.dumps({**asked, 'project': True}))
        self.assertEqual((status, headers['Content-Type']), (200, 'application/vnd.apache.arrow.stream'), body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        self.assertEqual(rows.column('name').to_pylist(), ['Halberd Securities', 'Meridian Capital'])
        status, _, body = self.post('/plan.json', json.dumps({**asked, 'project': True}))
        self.assertEqual(status, 200, body)
        self.assertTrue(json.loads(body)['plan'].startswith('OK\n'), body)
        self.assertEqual(len(self.server.engine.projects.names), 1)
        # not said: the same model reads only the tables this engine serves -- refused naming the one it does not, never
        # read from the project's own rows in their place (F28), by the run and the dry run alike
        for path in ('/plan-and-run.json', '/plan.json'):
            status, _, body = self.post(path, json.dumps(asked))
            self.assertEqual(status, 200, body)
            self.assertRegex(json.loads(body)['plan'], r'reads only the tables it serves.*it reads PARTY\.PARTY, which it '
                                                       r'does not serve now', path)

    def test_kept_for_the_process_and_named_by_what_it_holds(self):
        for _ in range(2):
            self.assertEqual(self.post('/api/pure/v1/execution/execute', self.project_input())[0], 200)
        # the same rows, another class beside them: the same database, no second one made
        with_a_class = PARTY.replace('###Relational', 'Class demo::party::Other { x: Integer[1]; }\n\n###Relational', 1)
        self.assertEqual(self.post('/api/pure/v1/execution/execute', self.project_input(with_a_class))[0], 200)
        self.assertEqual(len(self.server.engine.projects.names), 1)

    def test_a_data_element_over_an_undeclared_table_is_refused_by_name(self):
        bad = PARTY.replace('PARTY.PARTY:\n', 'PARTY.NOPE:\n')
        status, _, body = self.post('/api/pure/v1/execution/execute', self.project_input(bad))
        self.assertEqual(status, 500, body)
        self.assertIn('fills PARTY.NOPE, which no Database of the model declares', json.loads(body)['message'])


class Refusals(Served):
    def error(self, path, body, content_type='application/json'):
        status, headers, raw = self.post(path, body, content_type)
        self.assertEqual(headers['Content-Type'], 'application/json')
        return status, json.loads(raw)

    def test_a_query_that_does_not_compile_is_the_engines_compilation_error(self):
        status, body = self.error('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                  self.execute_input(self.query('->select(~[nope])')))
        self.assertEqual((status, body['errorType'], body['status']), (500, 'COMPILATION', 'error'))

    def test_a_model_the_engine_does_not_serve_is_refused(self):
        other = self.table.model.replace('trades::DB', 'other::DB')
        status, body = self.error('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                  self.execute_input(self.query(''), model=other))
        self.assertEqual(status, 500)
        self.assertIn('the models it serves', body['message'])

    def test_execute_in_json_answers_as_legend_lites_server(self):
        # legend-engine's JSON too (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): DuckDB writes the rows as the server's
        # database writes them, the compiler arranges them with the server's own writers
        status, _, body = self.post('/api/pure/v1/execution/execute',
                                    self.execute_input(self.query('->select(~[desk, qty])->sort(~qty->ascending())')))
        self.assertEqual(status, 200, body)
        answer = json.loads(body)
        self.assertEqual(answer['builder']['_type'], 'tdsBuilder')
        self.assertEqual([c['name'] for c in answer['builder']['columns']], ['desk', 'qty'])
        self.assertEqual(answer['activities'][0]['_type'], 'relational')
        self.assertEqual(answer['result']['columns'], ['desk', 'qty'])
        self.assertEqual([r['values'] for r in answer['result']['rows']],
                         [[d, q] for d, q in sorted(zip(self.df['desk'], self.df['qty']), key=lambda r: r[1])])
        # a refusal in the engine's shape, as the Arrow answer's
        status, refused = self.error('/api/pure/v1/execution/execute', self.execute_input(self.query('->select(~[nope])')))
        self.assertEqual((status, refused['errorType']), (500, 'COMPILATION'))

    def test_the_database_refusing_is_answered_in_the_engines_shape(self):
        status, body = self.error('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                  self.execute_input(self.query('->extend(~[n: x|$x.desk->toOne()->parseInteger()])')))
        self.assertEqual((status, body['code'], body['status']), (500, -1, 'error'))
        self.assertNotIn('errorType', body)
        self.assertIn('Conversion', body['message'])

    def test_a_path_the_api_does_not_serve_is_404_in_the_engines_shape(self):
        status, body = self.error('/api/pure/v1/nope', '{}')
        self.assertEqual((status, body['message']), (404, 'no such legend-engine API in legend-lite: /api/pure/v1/nope'))


class Planes(Served):
    """Where the engine plans (R3): one planner, Python's own, which every call the compiler answers goes through, and
    which services.json names for the page."""

    def test_it_plans_on_pythons_own_planner_and_says_so(self):
        said = json.loads(self.server.engine.answer('GET', '/services.json', '', None).body)
        local = {'word': 'local', 'label': 'Plan local (in Python: the native library)'}
        self.assertEqual((said['plane'], said['planes']), (local, [local]))

    def test_every_call_goes_through_the_planner_it_was_made_with(self):
        asked = []

        class Counting(NativePlanner):
            def answer(self, path, query, body):
                asked.append(path)
                return super().answer(path, query, body)

            def execute_plan(self, body, models):
                asked.append('execute')
                return super().execute_plan(body, models)

        self.server.close()
        # the one place an engine makes its planner, patched for this engine alone
        with mock.patch('legend_lite.engine.NativePlanner', Counting):
            self.server = WebServer(Engine(self.frames))
        self.arrow(GROUP)
        self.post('/api/pure/v1/grammar/grammarToJson/lambda', '|1')
        self.assertEqual(asked, ['execute', '/api/pure/v1/grammar/grammarToJson/lambda'])


class PlanAndRunRoutes(Served):
    """PLANANDRUN ON PYTHON (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): a page's grid planned against the model
    Python holds and run on its DuckDB in one call (plan-and-run.json), and its dry run (plan.json, the page's compile
    check) -- only over the models this engine serves, no handle crossing, never a dropped call. A question about the
    Pure query alone is legend-engine's own route (/api/pure/v1/...), as every Legend server answers it."""

    def plan_and_run(self, steps, model=None):
        return self.post('/plan-and-run.json', json.dumps({
            'lambda': json.dumps(ll.parse('|' + self.table.accessor + steps)),
            'model': self.table.model if model is None else model,
            'runtime': self.table.runtime,
        }))

    def test_a_grid_is_planned_over_the_model_held_run_here_and_typed_by_its_plan(self):
        status, headers, body = self.plan_and_run(GROUP)
        self.assertEqual((status, headers['Content-Type']), (200, 'application/vnd.apache.arrow.stream'), body[:300])
        rows = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        plan = rows.schema.metadata[b'legend.plan'].decode()
        self.assertTrue(plan.startswith('OK\n'), plan[:200])
        self.assertEqual(json.loads(plan[3:])['sql'], self.table.plan(GROUP).sql)
        self.assertEqual({f.name: f.metadata[b'legend.type'] for f in rows.schema}, {'desk': b'String', 'q': b'Float'})
        kept = self.df[self.df.qty > 5]
        self.assertEqual(rows.to_pylist(), [{'desk': d, 'q': q} for d, q in kept.groupby('desk')['qty'].sum().sort_index().items()])

    def plan_and_run_over(self, model, query):
        return self.post('/plan-and-run.json', json.dumps({
            'lambda': json.dumps(ll.parse(query)), 'model': model, 'runtime': self.table.runtime}))

    def test_a_model_the_page_writes_over_the_tables_it_serves_is_planned(self):
        # a page's project, say: its own Database, over the frame's table -- every table one Python serves (option (c))
        project = self.table.model.replace('trades::DB', 'proj::store::DB')
        status, headers, body = self.plan_and_run_over(project, '|#>{proj::store::DB.trades}#->select(~[desk])')
        self.assertEqual((status, headers['Content-Type']), (200, 'application/vnd.apache.arrow.stream'), body[:300])
        self.assertEqual(pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all().num_rows, len(self.df))
        # the frame let go of: its table no longer served, the project no longer read
        self.frames.unregister('trades')
        status, _, body = self.plan_and_run_over(project, '|#>{proj::store::DB.trades}#->select(~[desk])')
        self.assertTrue(json.loads(body)['plan'].startswith('ERR\nMODEL\n'), body)

    def test_a_model_that_declares_a_table_it_does_not_serve_is_refused_and_nothing_runs(self):
        # a table of this database Python never showed the page (a person's own): never read, whatever the model says
        self.frames.connection.execute("CREATE TABLE salaries AS SELECT 'Ada' AS name, 100 AS amount")
        self.addCleanup(self.frames.connection.execute, 'DROP TABLE IF EXISTS salaries')
        model = self.table.model.replace('    Table trades', '    Table salaries\n    (\n        name VARCHAR(10),\n'
                                         '        amount BIGINT\n    )\n    Table trades', 1)
        self.assertIn('Table salaries', model)
        status, headers, body = self.plan_and_run_over(model, '|#>{trades::DB.salaries}#->select(~[name])')
        self.assertEqual((status, headers['Content-Type']), (200, 'application/json'))
        self.assertTrue(json.loads(body)['plan'].startswith('ERR\nMODEL\n'), body)
        # nor one that does not read as a model at all
        status, _, body = self.plan_and_run_over('not a model', '|1')
        self.assertTrue(json.loads(body)['plan'].startswith('ERR\nMODEL\n'), body)

    def test_sql_the_database_refuses_is_answered_with_its_plan(self):
        status, headers, body = self.plan_and_run("->extend(~[n: x|$x.desk->toOne()->parseInteger()])")
        self.assertEqual((status, headers['Content-Type']), (422, 'application/json'), body[:300])
        said = json.loads(body)
        self.assertTrue(said['plan'].startswith('OK\n'), said)
        self.assertIn('Exception', said['error'])

    def test_a_request_that_is_not_one_is_refused(self):
        for body in ('not json', '[]', json.dumps({'lambda': 1, 'model': '', 'runtime': ''})):
            self.assertEqual(self.post('/plan-and-run.json', body)[0], 400, body)
            self.assertEqual(self.post('/plan.json', body)[0], 400, body)

    def test_the_dry_run_plans_what_plan_and_run_plans_over_the_model_held(self):
        # one planner function for both (the page's check compiles what its run sends): the same query's plan, the same
        # text -- a later edit that splits them fails here, not in review
        lam = json.dumps(ll.parse('|' + self.table.accessor + GROUP))
        asked = json.dumps({'lambda': lam, 'model': self.table.model, 'runtime': self.table.runtime})
        status, headers, body = self.post('/plan.json', asked)
        self.assertEqual((status, headers['Content-Type']), (200, 'application/json'), body)
        dry = json.loads(body)['plan']
        self.assertTrue(dry.startswith('OK\n'), dry)
        _, _, rows = self.post('/plan-and-run.json', asked)
        self.assertEqual(dry, pyarrow.ipc.open_stream(pa.py_buffer(rows)).read_all().schema.metadata[b'legend.plan'].decode())
        # never run, and only over the models this engine serves (R4): another is the planner's refusal, in its words
        refused = json.loads(self.post('/plan.json', json.dumps({
            'lambda': lam, 'model': 'unserved', 'runtime': self.table.runtime}))[2])
        self.assertTrue(refused['plan'].startswith('ERR\nMODEL\n'), refused)

    def test_the_planner_worker_tunnel_is_gone(self):
        # a page asks Python as it asks any host: legend-engine's own routes, and the host's compute -- never the tab's
        # worker messages carried over HTTP (planner.json, retired 2026-10-15)
        self.assertEqual(self.post('/planner.json', json.dumps({'kind': 'pureV1'}))[0], 404)

    def test_the_stores_need_the_token(self):
        # the saved queries and the saved pages (B5, B6): this user's, so only a page holding the engine's token asks
        for path in ('/api/pure/v1/query/search', '/api/pure/v1/query/dataCube/search'):
            self.assertEqual(self.post(path, '{}', headers={'Authorization': 'Bearer nope'})[0], 401, path)

    def test_both_need_the_token_and_neither_is_a_get(self):
        for path in ('/plan-and-run.json', '/plan.json'):
            self.assertEqual(self.post(path, '{}', headers={'Authorization': 'Bearer nope'})[0], 401, path)
            request = urllib.request.Request(self.server.url + path, headers={'Authorization': self.server.authorization})
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
        request = urllib.request.Request(self.server.url + path, headers={'Authorization': self.server.authorization})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_kept_whole_and_read_back_by_the_next_page(self):
        from unittest import mock
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        with mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': home.name}):
            self.assertEqual(self.get('/settings.json'), (200, b'null'), 'none kept yet')
            values = {'dataCube.grid.rowBuffer': 30, 'dataCube.debugger.enableDebugMode': True}
            self.assertEqual(self.post('/settings.json', json.dumps(values))[0], 204)
            self.assertEqual(json.loads(self.get('/settings.json')[1]), values)
            self.assertEqual(json.loads(Path(home.name, 'settings', 'datacube.json').read_text()), values)
            # a write is whole: the next replaces it
            self.assertEqual(self.post('/settings.json', json.dumps({'dataCube.grid.rowBuffer': 40}))[0], 204)
            self.assertEqual(json.loads(self.get('/settings.json')[1]), {'dataCube.grid.rowBuffer': 40})

    def test_only_an_object_and_only_with_the_token(self):
        for body in ('not json', '[1]', '"x"'):
            self.assertEqual(self.post('/settings.json', body)[0], 400, body)
        self.assertEqual(self.post('/settings.json', '{}', headers={'Authorization': 'Bearer nope'})[0], 401)
        request = urllib.request.Request(self.server.url + '/settings.json')
        with self.assertRaises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(request, timeout=30)
        self.assertEqual(refused.exception.code, 401)


class ModelHomeServed(Served):
    """Python's model home (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3): the native library's SDLC and Depot, served at
    /sdlc/api and /depot/api with the engine's token, over a git repository in this user's folder. The routes' answers
    are the SDLC suite's (//datacube:python_model_home_test); here, how the engine serves them."""

    def call(self, method, path, body=None, authorization=None):
        sent = {'Authorization': self.server.authorization if authorization is None else authorization}
        if body is not None:
            sent['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.server.url + path, None if body is None else body.encode('utf-8'), sent,
                                         method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_a_project_made_and_read_back_in_a_git_repository_of_this_users(self):
        from unittest import mock
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        with mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': home.name}):
            said = json.loads(self.server.engine.answer('GET', '/services.json', '', None).body)
            # home() resolves the folder it is given (macOS's /var is /private/var)
            self.assertEqual(said['modelHome'], {'where': str(Path(home.name, 'projects').resolve())})
            status, body = self.call('POST', '/sdlc/api/projects', json.dumps({
                'name': 'Python home', 'description': '', 'groupId': 'org.example', 'artifactId': 'python-home'}))
            self.assertIn(status, (200, 201), body[:300])
            project = json.loads(body)['projectId']
            status, body = self.call('GET', f'/sdlc/api/projects/{project}')
            self.assertEqual(status, 200, body[:300])
            self.assertEqual(json.loads(body)['name'], 'Python home')
            # git's own layout: what `git --git-dir` reads, and `git push` sends anywhere
            self.assertTrue(Path(home.name, 'projects', 'HEAD').is_file())
            self.assertTrue(Path(home.name, 'projects', 'refs', 'heads').is_dir())

    def test_with_the_token_only_and_delete_for_the_model_home_only(self):
        self.assertEqual(self.call('GET', '/sdlc/api/projects', authorization='Bearer nope')[0], 401)
        self.assertEqual(self.call('GET', '/depot/api/projects', authorization='Bearer nope')[0], 401)
        self.assertEqual(self.call('DELETE', '/settings.json')[0], 405)


class SavedPagesKept(unittest.TestCase):
    """The page's saved pages (step 2d, B6): upstream's /api/pure/v1/query/dataCube, answered by legend-lite's own rules
    in the native library over this user's folder, one file each -- the folder's layout the one its pages were kept in
    before, so they open as they were. The rules themselves are held to DataCube's one suite on every store
    (datacube/test/saved-pages/conformance.ts); these are what only Python's folder can be asked."""

    def setUp(self):
        self.place = tempfile.TemporaryDirectory()
        self.addCleanup(self.place.cleanup)
        self.folder = Path(self.place.name) / 'pages'
        self.engine = Engine(ll.Frames(), pages=self.folder)

    def call(self, method, path='', body=None):
        return self.engine.answer(method, '/api/pure/v1/query/dataCube' + path, '', body)

    def test_a_page_kept_before_the_rules_were_java_opens_as_it_was(self):
        # as the page's own store wrote it through Python (json.dump of its row): its content the page's JSON text
        self.folder.mkdir(parents=True)
        record = {'id': 'p-1', 'name': 'Q3', 'content': '{"kind":"datacube.page","dec":12.30}', 'owner': 'this user',
                  'createdAt': 1, 'lastUpdatedAt': 2, 'lastOpenAt': 2}
        (self.folder / 'p-1.json').write_text(json.dumps(record, ensure_ascii=False), encoding='utf-8')
        found = json.loads(self.call('POST', '/search', '{}').body)
        self.assertEqual([(q['id'], q['name'], 'content' in q) for q in found], [('p-1', 'Q3', False)])
        opened = self.call('GET', '/p-1')
        self.assertEqual(opened.status, 200)
        # its content as written: the decimal's spelling kept
        self.assertIn(b'"content":{"kind":"datacube.page","dec":12.30}', opened.body)
        self.assertEqual(self.call('DELETE', '/p-1').status, 204)
        self.assertEqual(list(self.folder.glob('*.json')), [])

    def test_a_page_saved_is_kept_whole_one_file_each(self):
        made = self.call('POST', '', json.dumps({'id': 'p-2', 'name': 'Kept', 'content': {'kind': 'datacube.page'}}))
        self.assertEqual(made.status, 200, made.body)
        kept = json.loads((self.folder / 'p-2.json').read_text(encoding='utf-8'))
        self.assertEqual((kept['owner'], kept['content']), ('this user', '{"kind":"datacube.page"}'))
        # every write whole: nothing half-written left beside the pages
        self.assertEqual([p.name for p in self.folder.iterdir() if p.suffix == '.tmp'], [])

    def test_a_file_that_is_not_a_page_is_left_out_and_the_others_open(self):
        self.assertEqual(self.call('POST', '', json.dumps({'id': 'good', 'name': 'G', 'content': {}})).status, 200)
        (self.folder / 'cut-short.json').write_text('{"id": "cut-sho', encoding='utf-8')
        (self.folder / 'a-list.json').write_text('[1, 2]', encoding='utf-8')
        # a page under another file's name (copied by hand): its file's name is its id, so it is not that page
        (self.folder / 'copied.json').write_text((self.folder / 'good.json').read_text(encoding='utf-8'), encoding='utf-8')
        # and a file whose name is no page's id (a space in it), its row naming that id: get refuses the id, so no list
        # names it
        (self.folder / 'a b.json').write_text('{"id": "a b", "name": "S", "content": "{}"}', encoding='utf-8')
        self.assertEqual([q['id'] for q in json.loads(self.call('POST', '/search', '{}').body)], ['good'])
        # asked for by its id, a file that is not a page is not found -- and a page saved under its id replaces it
        self.assertEqual(self.call('GET', '/cut-short').status, 404)
        self.assertEqual(self.call('GET', '/copied').status, 404)
        self.assertEqual(self.call('POST', '', json.dumps({'id': 'cut-short', 'name': 'C', 'content': {}})).status, 200)
        self.assertEqual(json.loads(self.call('GET', '/cut-short').body)['name'], 'C')

    def test_an_id_that_could_name_another_place_is_refused(self):
        for bad in ('..%2Fescape', '.lock', 'a.b', 'CON', 'nul', 'COM1', 'lpt9'):
            with self.subTest(id=bad):
                self.assertEqual(self.call('GET', '/' + bad).status, 400)
        self.assertEqual(self.call('POST', '', json.dumps({'id': '../x', 'name': 'x', 'content': {}})).status, 400)
        self.assertFalse((Path(self.place.name) / 'x.json').exists())

    @unittest.skipIf(sys.platform == 'win32', 'a folder private to its owner is a POSIX mode')
    def test_the_folder_is_private_to_this_user(self):
        self.assertEqual(self.call('POST', '', json.dumps({'id': 'p', 'name': 'P', 'content': {}})).status, 200)
        self.assertEqual(self.folder.stat().st_mode & 0o777, 0o700)

    def test_one_call_at_a_time_across_its_holders(self):
        # another holder of the folder's lock (another notebook's process takes the same file lock) holds it: a save waits
        order = []
        with locked(self.folder):
            writer = threading.Thread(target=lambda: (
                self.call('POST', '', json.dumps({'id': 'w', 'name': 'W', 'content': {}})), order.append('saved')))
            writer.start()
            time.sleep(0.3)
            order.append('released')
        writer.join(timeout=30)
        self.assertEqual(order, ['released', 'saved'])

    def test_the_old_calls_are_gone(self):
        # the page's rules were its own then (cube-store.ts's RuleStore), Python keeping its records: no longer
        self.assertEqual(self.engine.answer('POST', '/saved-pages.json', '', '{"call":"all"}').status, 404)

    def test_the_folder_is_this_users_unless_pointed_elsewhere(self):
        from unittest import mock
        import importlib
        home_module = importlib.import_module('legend_lite._home')
        self.addCleanup(setattr, home_module, '_chosen', home_module._chosen)
        home_module._chosen = None
        with mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': self.place.name}):
            self.assertEqual(ll.home(), Path(self.place.name).resolve())
            # a folder the process points itself at wins over the one it was started with
            chosen = Path(self.place.name) / 'chosen'
            self.assertEqual(ll.home(chosen), chosen.resolve())
            self.assertEqual(ll.home(), chosen.resolve())
        home_module._chosen = None
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LEGEND_LITE_HOME', None)
            if sys.platform == 'darwin':
                self.assertEqual(ll.home(), Path.home() / 'Library' / 'Application Support' / 'legend-lite')
            elif sys.platform.startswith('linux'):
                os.environ['XDG_DATA_HOME'] = self.place.name
                self.assertEqual(ll.home(), Path(self.place.name) / 'legend-lite')
        said = json.loads(self.engine.answer('GET', '/services.json', '', None).body)
        self.assertEqual(said['pages'], {'where': str(self.folder), 'user': 'this user'})


class NoFiles(Served):
    """An engine given no DataCube code to read a page's file with (no headless): it says so, and takes none."""

    def test_it_says_it_takes_no_file_and_refuses_one(self):
        answer = self.server.engine.answer('GET', '/services.json', '', None)
        self.assertFalse(json.loads(answer.body)['files'])
        for path, asked in (('/file-part.json', {'at': 0, 'bytes': 'YQoxCg=='}), ('/file.json', {'upload': 'u', 'name': 'a.csv', 'table': 'a'})):
            status, _, body = self.post(path, json.dumps(asked))
            self.assertEqual(status, 404, path)
            self.assertIn(b'which this engine was not given', body)


class OnlyItsOwnPage(Served):
    def raw(self, method, path, headers, body=b''):
        port = int(self.server.url.rsplit(':', 1)[1])
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
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
        return self.server.url.removeprefix('http://')

    def test_a_request_without_the_token_is_refused(self):
        tree = json.dumps(ll.parse('|1'))
        for given in ({}, {'Authorization': 'Bearer nope'}, {'Authorization': self.server.token}):
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': self.host(), 'Content-Length': str(len(tree))} | given, tree.encode())
            self.assertEqual(status, 401, given)

    def test_a_host_that_is_not_this_machine_is_refused(self):
        port = self.server.url.rsplit(':', 1)[1]
        for host in ('evil.example:' + port, '127.0.0.1:1', 'localhost'):
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': host, 'Authorization': self.server.authorization, 'Content-Length': '2'}, b'{}')
            self.assertEqual(status, 403, host)
        status, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false',
                                {'Host': 'localhost:' + port, 'Authorization': self.server.authorization,
                                 'Content-Length': '2'}, b'|1')
        self.assertEqual(status, 200)

    def test_no_cross_origin_request_is_answered(self):
        status, headers, _ = self.raw('OPTIONS', '/api/pure/v1/execution/execute', {
            'Host': self.host(), 'Origin': 'http://evil.example', 'Access-Control-Request-Method': 'POST'})
        self.assertEqual(status, 405)
        self.assertFalse([h for h in headers if h.lower().startswith('access-control-')], headers)

    def test_a_body_past_the_limit_is_refused_unread(self):
        status, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                {'Host': self.host(), 'Authorization': self.server.authorization,
                                 'Content-Length': str(64 * 1024 * 1024)})
        self.assertEqual(status, 413)

    def test_a_length_that_is_not_ascii_digits_or_a_body_not_utf8_is_answered(self):
        for length, body, status in (('²', b'', 411), ('2', b'\xff\xfe', 400)):
            got, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                 {'Host': self.host(), 'Authorization': self.server.authorization,
                                  'Content-Length': length}, body)
            self.assertEqual(got, status, length)

    def test_a_refused_request_s_body_is_read_so_its_answer_arrives(self):
        body = b'x' * 200_000
        status, _, answer = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                     {'Host': self.host(), 'Content-Length': str(len(body))}, body)
        self.assertEqual(status, 401)
        self.assertIn(b"the engine's token", answer)

    def test_closed_it_answers_nothing(self):
        url = self.server.url
        self.server.close()
        self.server = WebServer(Engine(self.frames))  # tearDown closes this one
        request = urllib.request.Request(url + '/api/pure/v1/nope', b'{}', {'Authorization': 'Bearer x'})
        with self.assertRaises(urllib.error.URLError) as refused:
            urllib.request.urlopen(request, timeout=5)
        self.assertNotIsInstance(refused.exception, urllib.error.HTTPError)
        self.assertIsInstance(refused.exception.reason, ConnectionRefusedError)

    def test_a_connection_that_sends_nothing_neither_blocks_others_nor_close(self):
        port = int(self.server.url.rsplit(':', 1)[1])
        idle = [socket.create_connection(('127.0.0.1', port)) for _ in range(6)]
        try:
            # more idle connections than the compiler's pool has threads: a request is still answered
            tree = json.dumps(ll.parse('|1'))
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': self.host(), 'Authorization': self.server.authorization,
                                     'Content-Length': str(len(tree))}, tree.encode())
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
        self.root = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
        (self.root / 'index.html').write_text('<!doctype html>cube')
        (self.root / 'vendor').mkdir()
        (self.root / 'vendor' / 'planner.wasm').write_bytes(b'\0asm')
        (self.root.parent / 'outside.txt').write_text('not the site')
        self.frames = ll.Frames()
        self.server = WebServer(Engine(self.frames, site=self.root))

    def tearDown(self):
        self.server.close()

    def get(self, path, host=None):
        port = int(self.server.url.rsplit(':', 1)[1])
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
        try:
            connection.putrequest('GET', path, skip_host=True)
            connection.putheader('Host', host or f'127.0.0.1:{port}')
            connection.endheaders()
            r = connection.getresponse()
            return r.status, r.getheader('Content-Type'), r.read()
        finally:
            connection.close()

    def test_its_files_with_their_types(self):
        self.assertEqual(self.get('/'), (200, 'text/html; charset=utf-8', b'<!doctype html>cube'))
        self.assertEqual(self.get('/vendor/planner.wasm'), (200, 'application/wasm', b'\0asm'))

    def test_nothing_outside_it_nor_a_folder(self):
        for path in ('/../outside.txt', '/%2e%2e/outside.txt', '/vendor/', '/vendor', '/nope.js', '/vendor//planner.wasm',
                     '/C:/Windows/win.ini', '/C%3A/x', '/index.html%00.js'):
            self.assertEqual(self.get(path)[0], 404, path)

    def test_to_a_local_host_only(self):
        port = self.server.url.rsplit(':', 1)[1]
        self.assertEqual(self.get('/', host=f'evil.example:{port}')[0], 403)

    def test_an_engine_without_a_site_serves_no_file(self):
        bare = WebServer(Engine(self.frames))
        try:
            port = int(bare.url.rsplit(':', 1)[1])
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
            connection.request('GET', '/')
            self.assertEqual(connection.getresponse().status, 404)
            connection.close()
        finally:
            bare.close()


class AppsServed(unittest.TestCase):
    """Legend Query and Studio served by Python (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md E): each app's site under its
    name, its config naming this engine's own services, and the link's token traded on the first load for the cookie
    its page's calls then carry."""

    def setUp(self):
        self.sites = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
        self.kept = {k: os.environ.get(k) for k in ('LEGEND_LITE_QUERY_SITE', 'LEGEND_LITE_STUDIO_SITE', 'LEGEND_LITE_SITE')}
        # DataCube's own site, as the session's engine needs one (the wheel's; this test's package has none)
        os.environ['LEGEND_LITE_SITE'] = str(self.sites)
        for app in ('query', 'studio'):
            (self.sites / app / 'demo').mkdir(parents=True)
            (self.sites / app / 'demo' / 'index.html').write_text(f'<!doctype html>{app}')
            os.environ[f'LEGEND_LITE_{app.upper()}_SITE'] = str(self.sites / app)
        self.server = WebServer(Engine(ll.Frames()))
        self.port = int(self.server.url.rsplit(':', 1)[1])

    def tearDown(self):
        self.server.close()
        for k, v in self.kept.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def call(self, method, path, headers=None, body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        try:
            connection.request(method, path, body=body, headers={'Host': f'127.0.0.1:{self.port}', **(headers or {})})
            r = connection.getresponse()
            return r.status, dict(r.getheaders()), r.read()
        finally:
            connection.close()

    def cookie(self, token=None, name=None, site='same-origin'):
        # the page's own call, as the browser marks it
        return {'Cookie': f'{name or self.server.cookie}={token or self.server.token}',
                **({'Sec-Fetch-Site': site} if site is not None else {})}

    def test_each_apps_files_under_its_name(self):
        for app in ('query', 'studio'):
            status, headers, body = self.call('GET', f'/{app}/demo/index.html')
            self.assertEqual((status, headers['Content-Type'], body), (200, 'text/html; charset=utf-8',
                                                                       f'<!doctype html>{app}'.encode()))
        self.assertEqual(self.call('GET', '/query/../studio/demo/index.html')[0], 404)
        self.assertEqual(self.call('GET', '/queryx/demo/index.html')[0], 404)
        os.environ['LEGEND_LITE_QUERY_SITE'] = str(self.sites / 'nowhere')
        self.assertEqual(self.call('GET', '/query/demo/index.html')[0], 404, 'no site built: nothing served')

    def test_the_links_token_traded_for_the_cookie_and_the_address_without_it(self):
        status, headers, body = self.call('GET', f'/studio/demo/index.html?token={self.server.token}')
        self.assertEqual(status, 303)
        self.assertEqual(headers['Location'], '/studio/demo/index.html')
        self.assertEqual(headers['Set-Cookie'],
                         f'legend-lite-{self.port}={self.server.token}; Path=/; HttpOnly; SameSite=Strict')
        # any other parameter stays
        self.assertEqual(self.call('GET', f'/query/demo/index.html?config=x.json&token={self.server.token}')[1]['Location'],
                         '/query/demo/index.html?config=x.json')
        self.assertEqual(self.call('GET', '/query/demo/index.html?token=wrong')[0], 401)

    def test_each_apps_config_names_this_engines_services(self):
        origin = f'http://127.0.0.1:{self.port}'
        status, headers, body = self.call('GET', '/query/demo/config.json')
        self.assertEqual((status, headers['Content-Type']), (200, 'application/json'))
        self.assertEqual(json.loads(body), {'execution': {'kind': 'server', 'engine': f'{origin}/api'}, 'projects': [],
                                            'depot': {'sdlc': f'{origin}/sdlc/api', 'depot': f'{origin}/depot/api'},
                                            'vendor': './vendor/'})
        self.assertEqual(json.loads(self.call('GET', '/studio/demo/config.json')[2]), {
            'sdlc': f'{origin}/sdlc/api', 'depot': f'{origin}/depot/api', 'vendor': './vendor/',
            'worker': './planner-worker.js', 'engine': f'{origin}/api'})

    def test_the_apps_calls_carry_the_cookie(self):
        self.assertEqual(self.call('GET', '/api/server/v1/currentUser'), (401, unittest.mock.ANY, unittest.mock.ANY))
        # another engine's cookie (another port's), or this one's with another token: refused
        self.assertEqual(self.call('GET', '/api/server/v1/currentUser', self.cookie(name='legend-lite-1'))[0], 401)
        self.assertEqual(self.call('GET', '/api/server/v1/currentUser', self.cookie(token='wrong'))[0], 401)
        status, _, body = self.call('GET', '/api/server/v1/currentUser', self.cookie())
        self.assertEqual((status, json.loads(body)), (200, THIS_USER))
        # the store, the model home and pure/v1, as the token reads them
        self.assertEqual(self.call('POST', '/api/pure/v1/query/search', self.cookie(), '{}')[0], 200)
        self.assertEqual(self.call('GET', '/sdlc/api/projects', self.cookie())[0], 200)
        self.assertEqual(self.call('GET', '/sdlc/api/projects')[0], 401)

    def test_the_cookie_admits_the_pages_own_calls_only(self):
        # a browser sends a host's cookies to its every port (SameSite or not): a page of another server on this
        # machine sends it too, and is refused (the audit of E)
        user = '/api/server/v1/currentUser'
        for site in ('same-site', 'cross-site', 'none'):
            self.assertEqual(self.call('GET', user, self.cookie(site=site))[0], 401, site)
        self.assertEqual(self.call('POST', '/api/pure/v1/query/search', self.cookie(site='same-site'), '{}')[0], 401)
        # a browser that marks no call: its Origin decides
        own = f'http://127.0.0.1:{self.port}'
        self.assertEqual(self.call('GET', user, {**self.cookie(site=None), 'Origin': own})[0], 200)
        for origin in (f'http://127.0.0.1:{self.port + 1}', 'null', None):
            headers = {**self.cookie(site=None), **({'Origin': origin} if origin else {})}
            self.assertEqual(self.call('GET', user, headers)[0], 401, origin)

    def test_another_servers_cookies_beside_it(self):
        # the header carries every local server's cookies: one this engine cannot read, or one of its own name with
        # another value, leaves its own readable
        jar = self.cookie()
        for others in ('x=a b', 'x={"a":1}', f'{self.server.cookie}=stale', 'bare'):
            for header in (f'{others}; {jar["Cookie"]}', f'{jar["Cookie"]}; {others}'):
                self.assertEqual(self.call('GET', '/api/server/v1/currentUser', {**jar, 'Cookie': header})[0], 200, header)

    def test_an_apps_path_to_another_host_is_refused_once(self):
        with socket.create_connection(('127.0.0.1', self.port), timeout=30) as raw:
            raw.sendall(b'GET /query/demo/config.json HTTP/1.1\r\nHost: evil.example\r\n\r\n')
            said = b''
            while chunk := raw.recv(65536):
                said += chunk
        self.assertTrue(said.startswith(b'HTTP/1.0 403') or said.startswith(b'HTTP/1.1 403'), said[:80])
        self.assertEqual(said.count(b'HTTP/1.'), 1, said)

    def test_lites_own_routes_answered_as_its_server_answers_them(self):
        model = json.dumps({'_type': 'text', 'code': 'Class demo::A { x: Integer[1]; y: Nope[1]; }'})
        status, headers, body = self.call('POST', '/api/lite/v1/compilation/compile',
                                          {**self.cookie(), 'Content-Type': 'application/json'}, model)
        self.assertEqual(status, 200, body)
        self.assertEqual(len(json.loads(body)['errors']), 1)
        self.assertEqual(self.call('POST', '/api/lite/v1/compilation/compile', {'Content-Type': 'application/json'},
                                   model)[0], 401)
        self.assertEqual(self.call('POST', '/api/lite/v1/nope', self.cookie(), '{}')[0], 404)

    def test_ll_query_and_ll_studio_give_the_apps_links(self):
        from legend_lite import datacube
        kept, datacube._session = datacube._session, datacube._Session()
        try:
            for app, open_app in (('query', ll.query), ('studio', ll.studio)):
                url = open_app(browser=False)
                web = datacube._session._web
                self.assertEqual(url, f'{web.url}/{app}/demo/index.html?token={web.token}')
            # a cube shown and closed leaves the web server to the apps' tabs, which say nothing when they close
            cube = ll.show(pd.DataFrame({'x': [1]}), browser=False, inline=False)
            cube.close()
            self.assertIs(datacube._session._web, web)
            self.assertEqual(sorted(datacube._session.apps), ['query', 'studio'])
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
                return r.status, r.headers['Content-Type'], r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers['Content-Type'], e.read()

    def test_a_frame_s_cube_as_it_is_now(self):
        status, content_type, body = self.get('/cube.json?table=trades', {'Authorization': self.server.authorization})
        self.assertEqual((status, content_type), (200, 'application/json'))
        self.assertEqual(json.loads(body), {'title': 'trades', 'model': self.table.model, 'runtime': self.table.runtime,
                                            'source': self.table.source, 'version': 0})
        # a Live frame read again: a new column, a new model
        self.df['extra'] = 1
        _, _, body = self.get('/cube.json?table=TRADES', {'Authorization': self.server.authorization})
        self.assertIn('extra', json.loads(body)['model'])

    def test_asked_with_the_token_only_and_for_a_frame_it_serves(self):
        self.assertEqual(self.get('/cube.json?table=trades')[0], 401)
        self.assertEqual(self.get('/cube.json?table=nope', {'Authorization': self.server.authorization})[0], 404)


class Page(Cube):
    """page.json: a page it serves (Python's ll.Page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md) -- its document
    and its version -- and version.json for a page: its own version and each of its frames'."""

    DOC = {'kind': 'datacube.page', 'version': 4, 'name': 'Q3',
           'cubes': [{'id': 'grid-1', 'cube': {'source': {'_type': 'frame', 'name': 'trades', 'columns': []}}}],
           'views': [], 'sheets': []}

    def serve_page(self, name, document, commands=None):
        """``document`` served as page ``name`` over the frame trades, as the session serves a Python page (its keeper,
        told to serve it), written whole -- moved by ``commands``, when they say what moved it: what these routes answer
        from, whoever wrote it. Returns its version."""
        keeper = self.server.engine.keeper
        reads = {'frames': ['trades'], 'copies': [], 'ids': []}
        kept = keeper.current(name)
        version = (keeper.keep(name, document, reads, []) if kept is None
                   else keeper.write(name, kept[0], document, reads, commands, []))
        keeper.serve(name, True)
        return version

    def test_a_page_served_its_document_and_version_moving_when_served_again(self):
        auth = {'Authorization': self.server.authorization}
        self.assertEqual(self.get('/page.json?page=q3', auth)[0], 404)
        self.assertEqual(self.serve_page('q3', self.DOC), 1)
        status, content_type, body = self.get('/page.json?page=q3', auth)
        self.assertEqual((status, content_type), (200, 'application/json'))
        self.assertEqual(json.loads(body), {'version': 1, 'page': self.DOC, 'held': []})
        self.assertEqual(json.loads(self.get('/version.json?page=q3', auth)[2]), {'version': 1, 'frames': {'trades': 0}})
        # its frame changed: the frame's version moves, the page's not
        self.server.engine.changed('trades')
        self.assertEqual(json.loads(self.get('/version.json?page=q3', auth)[2]), {'version': 1, 'frames': {'trades': 1}})
        # served again: its version moves
        self.assertEqual(self.serve_page('q3', {**self.DOC, 'name': 'Q4'}), 2)
        self.assertEqual(json.loads(self.get('/page.json?page=q3', auth)[2])['page']['name'], 'Q4')
        self.server.engine.keeper.serve('q3', False)
        self.assertEqual(self.get('/version.json?page=q3', auth)[0], 404)

    def test_a_page_open_at_a_version_asks_what_moved_it_since_and_is_told_the_commands_or_the_page(self):
        # (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md) each serve keeps the commands that moved the page from the version
        # before; a page open at version v asks since=v: the commands while every version since has them, else the page
        auth = {'Authorization': self.server.authorization}
        engine = self.server.engine
        asked = lambda since: json.loads(self.get(f'/page.json?page=q3&since={since}', auth)[2])
        self.serve_page('q3', self.DOC)
        self.assertEqual(asked(1), {'version': 1, 'commands': []}, 'nothing moved it')
        self.assertEqual(asked(0), {'version': 1, 'page': self.DOC, 'held': []},
                         'first served: no version before it it was moved from')
        sheet = {'page': {'kind': 'addSheet', 'id': 'sheet-2', 'name': None}}
        group = {'grid': 'grid-1', 'command': {'kind': 'group', 'rows': ['desk']}}
        self.serve_page('q3', {**self.DOC, 'name': 'Q4'}, [sheet])
        self.serve_page('q3', {**self.DOC, 'name': 'Q5'}, [group])
        self.assertEqual(asked(1), {'version': 3, 'commands': [sheet, group]}, "each version's commands, in order")
        self.assertEqual(asked(2), {'version': 3, 'commands': [group]})
        # a change by no command: a page open before it opens the page again
        self.serve_page('q3', {**self.DOC, 'name': 'Q6'})
        self.assertEqual(asked(2)['page']['name'], 'Q6')
        self.assertEqual(asked(4), {'version': 4, 'commands': []})
        self.assertEqual(self.get('/page.json?page=q3&since=x', auth)[0], 400)
        # closed and shown again: kept as it was (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md), so a tab still open on its
        # earlier showing follows by the commands since; served again unchanged, no new version
        self.server.engine.keeper.serve('q3', False)
        self.assertEqual(self.get('/page.json?page=q3&since=4', auth)[0], 404, 'closed: served no more')
        self.serve_page('q3', self.DOC, [sheet])
        self.assertEqual(asked(4), {'version': 5, 'commands': [sheet]})
        self.assertEqual(self.serve_page('q3', self.DOC, [sheet]), 5)
        # the commands of its last 64 versions are kept: a page open at an older one opens it again
        for n in range(70):
            self.serve_page('q3', {**self.DOC, 'name': f'Q{n}'}, [sheet])
        self.assertIn('page', asked(5))
        self.assertEqual(len(asked(75 - 63)['commands']), 63)

    def test_a_page_asked_with_the_token_only(self):
        self.serve_page('q3', self.DOC)
        self.assertEqual(self.get('/page.json?page=q3')[0], 401)
        self.assertEqual(self.get('/version.json?page=q3')[0], 401)

    def test_an_open_pages_write_is_read_by_datacubes_own_reader(self):
        # (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) an engine with no DataCube code to read a page with keeps no page an
        # open page writes; one with it: test_page.py Live
        engine = self.server.engine
        self.serve_page('q3', self.DOC)
        said = json.dumps({**self.DOC, 'name': 'Renamed there'})
        self.assertEqual(engine.answer('POST', '/page.json', 'page=q3', said).status, 400, 'no version named')
        self.assertEqual(engine.answer('POST', '/page.json', 'page=q3&version=1', said).status, 404)
        self.assertEqual(engine.answer('POST', '/page.json', 'page=nope&version=1', said).status, 404)

    def test_a_pages_numbers_exact_both_ways(self):
        # a calculated column's 12.30 is 12.30, an integer past 2**53 every digit (the protocol's exact JSON)
        engine = self.server.engine
        lambda_ = {'_type': 'lambda', 'body': [{'_type': 'decimal', 'value': Decimal('12.30')},
                                               {'_type': 'integer', 'value': 2 ** 70}], 'parameters': []}
        self.serve_page('q3', {**self.DOC, 'lambda': lambda_})
        body = self.get('/page.json?page=q3', {'Authorization': self.server.authorization})[2].decode('utf-8')
        self.assertIn('12.30', body)
        self.assertIn(str(2 ** 70), body)


class WarehouseOnPython(unittest.TestCase):
    """A warehouse table on Python's page (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3), every route of /warehouse.json
    and the plan-and-run over its model, against a stand-in for the driver (legend_lite.warehouse's Warehouse and
    connect): the shapes refused, the sign-in, a table opened once as one person, its Live rows and receipt, each snap
    a copy of its own modelled by DataCube's code and released by its table, the snap limit, and an expired session
    said as one."""

    URL = 'https://wh.example'
    OBJECTS = [{'catalog': 'main', 'databaseType': 'DuckDB', 'schema': 'trading', 'name': 'POSITIONS', 'kind': 'table',
                'columns': [
                    {'name': 'desk', 'type': 'VARCHAR', 'logicalType': 'VARCHAR', 'precision': None, 'scale': None,
                     'notNull': False},
                    {'name': 'qty', 'type': 'INTEGER', 'logicalType': 'INTEGER', 'precision': None, 'scale': None,
                     'notNull': False}]}]

    def setUp(self):
        from legend_lite import warehouse
        self.warehouse = warehouse
        outer = self
        self.count, self.expired, self.principal, self.pulls = 3, False, 'rita', 0

        class Reader:
            def __init__(self, session, catalog='main', clock=None):
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

            def arrow_chunks(self, sql, on_receipt=None):
                if outer.expired:
                    raise warehouse.SessionExpired(outer.URL, self.principal)
                if on_receipt is not None:
                    on_receipt({'statementId': 's1', 'serverRows': 3, 'as': self.principal, 'where': 'the warehouse at wh'})
                outer.pulls += 1
                rows = (pa.table({'n': pa.array([outer.count], pa.int64())}) if sql.startswith('SELECT count(*)')
                        else pa.table({'desk': ['FX', 'EQ', 'FX'], 'qty': pa.array([10, 5, 3], pa.int32())}))
                sink = pa.BufferOutputStream()
                with pyarrow.ipc.new_stream(sink, rows.schema) as w:
                    w.write_table(rows)
                yield sink.getvalue().to_pybytes()

        def connect(url, user, password):
            if password != 'pw':
                raise warehouse.WarehouseError('sign-in failed — AUTH_INVALID: wrong user or password', 'AUTH_INVALID')
            return warehouse.Session(url, 'tok', user, '2099-01-01T00:00:00Z'), outer.OBJECTS

        patches = [mock.patch.object(warehouse, 'Warehouse', Reader), mock.patch.object(warehouse, 'connect', connect)]
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

    def call(self, said, path='/warehouse.json'):
        a = self.engine.answer('POST', path, '', json.dumps(said))
        return a.status, a.body

    def opened(self, user='rita'):
        self.assertEqual(self.call({'op': 'connect', 'url': self.URL, 'user': user, 'password': 'pw'})[0], 200)
        status, body = self.call({'op': 'table', 'url': self.URL, 'catalog': 'main', 'schema': 'trading', 'name': 'POSITIONS'})
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def query(self, m):
        spec = {'table': 'POSITIONS', 'schema': 'trading', 'convertible': False, 'databaseType': 'DuckDB',
                'columns': [{**c, 'dataType': c['type']} for c in self.OBJECTS[0]['columns']]}
        accessor = ll.table_model(spec)['accessor']
        return json.dumps(ll.parse(f'|{accessor}->select(~[desk, qty])'))

    def plan_and_run(self, m, runtime):
        a = self.engine.answer('POST', '/plan-and-run.json', '', json.dumps(
            {'lambda': self.query(m), 'model': m['model'], 'runtime': runtime}))
        return a

    def test_the_shapes_refused_and_a_table_only_once_signed_in(self):
        self.assertEqual(self.call({})[0], 400)
        self.assertEqual(self.engine.answer('POST', '/warehouse.json', '', 'not json').status, 400)
        self.assertEqual(self.call({'op': 'connect', 'url': self.URL, 'user': 'rita'})[0], 400)
        self.assertEqual(self.call({'op': 'nope', 'url': self.URL})[0], 400)
        status, body = self.call({'op': 'table', 'url': self.URL, 'catalog': 'main', 'schema': 'trading', 'name': 'POSITIONS'})
        self.assertEqual((status, body.decode()), (401, f'sign in to the warehouse at {self.URL} first'))
        status, body = self.call({'op': 'connect', 'url': self.URL, 'user': 'rita', 'password': 'wrong'})
        self.assertEqual((status, body.decode()), (422, 'sign-in failed — AUTH_INVALID: wrong user or password'))

    def test_signed_in_the_session_without_its_token_and_a_table_not_listed_refused(self):
        status, body = self.call({'op': 'connect', 'url': self.URL, 'user': 'rita', 'password': 'pw'})
        said = json.loads(body)
        self.assertEqual((status, said['session']['token'], said['session']['principal']), (200, '', 'rita'))
        self.assertEqual(self.call({'op': 'table', 'url': self.URL, 'catalog': 'main', 'schema': 'trading',
                                    'name': 'secret'})[0], 404)

    def test_live_rows_from_the_warehouse_with_its_receipt(self):
        m = self.opened()
        a = self.plan_and_run(m, m['runtime'])
        self.assertEqual(a.status, 200, a.body)
        rows = pyarrow.ipc.open_stream(a.body).read_all()
        self.assertEqual(sorted(zip(rows['desk'].to_pylist(), rows['qty'].to_pylist())), [('EQ', 5), ('FX', 3), ('FX', 10)])
        self.assertEqual(json.loads(rows.schema.metadata[b'legend.receipt']),
                         {'plane': 'warehouse', 'where': 'the warehouse at wh', 'as': 'rita', 'statementId': 's1',
                          'serverRows': 3})

    def test_one_person_per_open_table(self):
        self.opened('rita')
        self.assertEqual(self.call({'op': 'connect', 'url': self.URL, 'user': 'bob', 'password': 'pw'})[0], 200)
        status, body = self.call({'op': 'table', 'url': self.URL, 'catalog': 'main', 'schema': 'trading', 'name': 'POSITIONS'})
        self.assertEqual(status, 409, body)
        self.assertIn('open already as rita', body.decode())

    def snapped(self, m):
        status, body = self.call({'op': 'snap', 'model': m['model'], 'runtime': m['runtime'], 'pull': self.query(m)}, '/snap.json')
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def copy_rows(self, copy):
        """The copy's rows, planned over Python's own model of it and run on the frames' database."""
        path = copy['source']['value']['path']
        lam = json.dumps(ll.parse(f'|#>{{{path[0]}.{path[1]}}}#->select(~[desk, qty])'))
        a = self.engine.answer('POST', '/plan-and-run.json', '', json.dumps(
            {'lambda': lam, 'model': copy['model'], 'runtime': copy['runtime']}))
        return a.status, a.body

    def test_each_snap_a_copy_of_its_own_read_through_its_own_model_and_released_by_its_table(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3: the warehouse table's own name was every grid's copy, so a
        # second grid got the first's rows as they were, and the copy was read through the table's model
        m = self.opened()
        first = self.snapped(m)
        second = self.snapped(m)
        self.assertEqual((first['table'], second['table'], self.pulls), ('snap', 'snap_2', 4),
                         'each grid pulls and lands its own copy (a count and a pull each)')
        self.assertEqual((first['rowCount'], first['pulledBy']['as']), (3, 'rita'))
        self.assertNotEqual(first['model'], m['model'], 'a copy is read through a model of its own')
        status, body = self.copy_rows(first)
        self.assertEqual(status, 200, body)
        rows = pyarrow.ipc.open_stream(body).read_all()
        self.assertEqual(sorted(zip(rows['desk'].to_pylist(), rows['qty'].to_pylist())), [('EQ', 5), ('FX', 3), ('FX', 10)])
        # a copy held, described for a page following a snap made in Python (§18.8): its table and model, as made
        status, body = self.call({'op': 'held', 'table': 'snap'}, '/snap.json')
        self.assertEqual(status, 200, body)
        held = json.loads(body)
        self.assertEqual((held['table'], held['model'], held['source']), ('snap', first['model'], first['source']))
        self.assertEqual(self.call({'op': 'release', 'table': 'snap'}, '/snap.json')[0], 200)
        self.assertEqual(self.call({'op': 'held', 'table': 'snap'}, '/snap.json')[0], 404, 'let go: held no more')
        self.assertEqual(self.call({'op': 'held', 'table': 'POSITIONS'}, '/snap.json')[0], 404, 'no copy a snap made')
        self.assertEqual(self.copy_rows(second)[0], 200, 'the other grid still reads its own')
        self.assertEqual(self.frames.connection.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'snap'").fetchone()[0], 0, 'the copy is dropped')
        self.assertIsNone(self.frames.file_table('snap'), 'and served no more')
        # only a copy a snap made is let go of this way; one let go already is gone (a late release, a tab's closing
        # grid's after Python let its page's copy go: §18.8), its name never given to another copy
        self.assertEqual(self.call({'op': 'release', 'table': 'snap'}, '/snap.json')[0], 200)
        self.assertEqual(self.call({'op': 'release', 'table': 'POSITIONS'}, '/snap.json')[0], 404)
        self.assertEqual(self.snapped(m)['table'], 'snap_3', 'a new copy takes no name a copy had')
        self.assertEqual(self.call({'op': 'release', 'table': 'snap'}, '/snap.json')[0], 200)
        self.assertEqual(self.call({'op': 'held', 'table': 'snap_3'}, '/snap.json')[0], 200, 'a late release lets no other copy go')
        # nor is it a page's file to drop
        self.assertEqual(self.engine.answer('POST', '/forget.json', '', json.dumps({'name': 'x', 'table': 'snap_2'})).status,
                         404)
        # a copy a page the keeper keeps reads (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): the keeper's -- its maker's
        # release keeps it -- and it goes with the last page that reads it
        # (what a page reads comes with each write, as DataCube says it: headless pageReads)
        reads = {'frames': [], 'copies': ['snap_2'], 'ids': []}
        self.engine.keeper.keep('p', {'name': 'p'}, reads, [])
        self.engine.keeper.keep('q', {'name': 'q'}, reads, [])
        status, body = self.call({'op': 'release', 'table': 'snap_2'}, '/snap.json')
        self.assertEqual((status, json.loads(body)), (200, {'kept': 'a page reads it: it goes when nothing reads it any more'}))
        self.assertEqual(self.copy_rows(second)[0], 200, 'kept')
        self.engine.keeper.forget('p')
        self.engine.sweep()
        self.assertEqual(self.call({'op': 'held', 'table': 'snap_2'}, '/snap.json')[0], 200, 'another page reads it')
        self.engine.keeper.write('q', 1, {'name': 'q, live'}, {'frames': [], 'copies': [], 'ids': []}, None, [])
        self.engine.sweep()
        self.assertEqual(self.call({'op': 'held', 'table': 'snap_2'}, '/snap.json')[0], 404, 'let go with the last page')

    def test_a_copy_lives_until_every_page_open_on_it_moved_past_it(self):
        # the keeper's known limit 6, closed (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): a page open on another -- a
        # tab, a widget -- at a version that reads a copy still reads it, its queries and the commands it follows by,
        # until it says it shows a later one (as it asks, follows and writes), or says nothing for FOLLOWED_FOR seconds
        m = self.opened()
        copy = self.snapped(m)['table']
        keeper = self.engine.keeper
        keeper.keep('p', {'name': 'p'}, {'frames': [], 'copies': [copy], 'ids': []}, [])
        keeper.serve('p', True)

        def asked(path, query):
            return self.engine.answer('GET', path, query, None).status

        def held():
            return self.call({'op': 'held', 'table': copy}, '/snap.json')[0]

        self.assertEqual(asked('/version.json', 'page=p&follower=tab-1&at=1'), 200)
        # Python goes live: version 2 reads no copy
        keeper.write('p', 1, {'name': 'p, live'}, {'frames': [], 'copies': [], 'ids': []}, None, [])
        self.engine.sweep()
        self.assertEqual(held(), 200, 'the open page shows version 1, which reads it')
        self.assertEqual(asked('/version.json', 'page=p&follower=tab-1'), 200, 'following: no version said')
        self.assertEqual(self.call({'op': 'release', 'table': copy}, '/snap.json')[0], 200)
        self.assertEqual(held(), 200, "its maker's release keeps it: the keeper's")
        self.assertEqual(asked('/page.json', 'page=p&since=1&follower=tab-1'), 200)
        self.assertEqual(held(), 200, 'following from version 1')
        self.assertEqual(asked('/version.json', 'page=p&follower=tab-1&at=2'), 200)
        self.assertEqual(held(), 404, 'moved past it: let go as it said so')
        # who it is, and the version it shows, said right or refused
        self.assertEqual(asked('/version.json', 'page=p&at=2'), 400)
        self.assertEqual(asked('/version.json', 'page=p&follower=a%20b&at=2'), 400)
        self.assertEqual(asked('/version.json', 'page=p&follower=tab-1&at=two'), 400)
        self.assertEqual(asked('/page.json', 'page=p&since=2'), 200, 'a request that names no follower says nothing')

    def test_the_dry_run_plans_a_table_as_its_run_does_and_refuses_the_runtime_its_run_refuses(self):
        # plan.json, plan-and-run's planning half (the one-app design §15.10): never run, the run's plan, and the run's
        # refusal of a runtime (the table's rule, refused_runtime) word for word
        m = self.opened()

        def dry(runtime):
            a = self.engine.answer('POST', '/plan.json', '', json.dumps(
                {'lambda': self.query(m), 'model': m['model'], 'runtime': runtime}))
            self.assertEqual(a.status, 200, a.body)
            return json.loads(a.body)['plan']

        planned = dry(m['runtime'])
        self.assertEqual(self.pulls, 0, 'a dry run reads nothing from the warehouse')
        run = self.plan_and_run(m, m['runtime'])
        self.assertEqual(run.status, 200, run.body)
        self.assertEqual(planned, pyarrow.ipc.open_stream(run.body).read_all().schema.metadata[b'legend.plan'].decode())
        # another runtime: refused by both, in the same words
        refused = dry('other::Runtime')
        self.assertIn('is read on its warehouse, through its own runtime', refused)
        self.assertEqual(refused, json.loads(self.plan_and_run(m, 'other::Runtime').body)['plan'])

    def test_a_copy_that_fails_leaves_nothing(self):
        m = self.opened()
        made = self.engine._headless

        class Refusing:
            def __init__(self, inner):
                self.inner = inner

            def call(self, function, *args, **kw):
                if function == 'landed':
                    raise RuntimeError('no model of it')
                return self.inner.call(function, *args, **kw)

        self.engine._headless = lambda: Refusing(made())
        status, body = self.call({'op': 'snap', 'model': m['model'], 'runtime': m['runtime'], 'pull': self.query(m)}, '/snap.json')
        self.assertEqual(status, 422, body)
        self.assertIn('could not be copied', body.decode())
        self.assertEqual(self.frames.connection.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'snap'").fetchone()[0], 0, 'no copy left')
        self.assertIsNone(self.frames.file_table('snap'))

    def test_a_frames_grid_snaps_too_a_copy_of_its_base_as_the_frame_was(self):
        # one Snap for every grid (§18.3): a Live frame read afresh by every query, its grid's copy keeping its rows
        data = {'df': pd.DataFrame({'desk': ['FX', 'EQ'], 'qty': [1, 2]})}
        desks = self.frames.register('desks', lambda: data['df'])
        lam = json.dumps(ll.parse(f'|{desks.accessor}->select(~[desk, qty])'))
        status, body = self.call({'op': 'snap', 'model': desks.model, 'runtime': desks.runtime, 'pull': lam}, '/snap.json')
        self.assertEqual(status, 200, body)
        copy = json.loads(body)
        self.assertEqual((copy['table'], copy['rowCount'], 'pulledBy' in copy), ('snap', 2, False))
        data['df'] = pd.DataFrame({'desk': ['FX', 'EQ', 'IR'], 'qty': [1, 2, 3]})
        self.assertEqual(desks.execute('->select(~[desk])').num_rows, 3, 'the frame, read afresh')
        status, rows = self.copy_rows(copy)
        self.assertEqual(status, 200, rows)
        self.assertEqual(pyarrow.ipc.open_stream(rows).read_all().num_rows, 2, 'its copy, as it was snapped')
        self.assertEqual(self.call({'op': 'release', 'table': 'snap'}, '/snap.json')[0], 200)
        # a model reading a table this engine does not serve is not snapped
        secret = ll.table_model({'table': 'secret', 'convertible': True, 'databaseType': 'DuckDB', 'columns': [
            {'name': 'desk', 'dataType': 'VARCHAR', 'logicalType': 'VARCHAR', 'precision': None, 'scale': None,
             'notNull': False}]})
        status, body = self.call({'op': 'snap', 'model': secret['model'], 'runtime': secret['runtime'], 'pull': lam},
                                 '/snap.json')
        self.assertEqual(status, 404, body)

    def test_an_engine_without_datacubes_code_keeps_no_copy(self):
        self.engine = Engine(self.frames)
        m = self.opened()
        status, body = self.call({'op': 'snap', 'model': m['model'], 'runtime': m['runtime'], 'pull': self.query(m)}, '/snap.json')
        self.assertEqual(status, 404, body)
        self.assertIn('keeps no copy', body.decode())

    def test_the_snap_limit_in_the_tabs_words(self):
        m = self.opened()
        self.count = 10_000_001
        status, body = self.call({'op': 'snap', 'model': m['model'], 'runtime': m['runtime'], 'pull': self.query(m)}, '/snap.json')
        self.assertEqual((status, body.decode()), (422, '10,000,001 rows exceeds the 10,000,000 row snap limit. Narrow the '
                                                          'filter, or stay live.'))

    def test_an_expired_session_said_as_one(self):
        m = self.opened()
        self.expired = True
        a = self.plan_and_run(m, m['runtime'])
        said = json.loads(a.body)
        self.assertEqual((a.status, said['expired']), (401, {'baseUrl': self.URL, 'principal': 'rita'}))
        self.assertEqual(self.call({'op': 'snap', 'model': m['model'], 'runtime': m['runtime'], 'pull': self.query(m)}, '/snap.json')[0], 401)
        self.assertEqual(self.call({'op': 'table', 'url': self.URL, 'catalog': 'main', 'schema': 'trading',
                                    'name': 'POSITIONS'})[0], 401)
