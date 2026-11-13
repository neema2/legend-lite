"""DATACUBE'S ENGINE CONTRACT ON PYTHON (legend_lite/contract.py; docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, step 1,
2026-11-13): the door's calls and refusals by name, R3a (a model DataCube sends holds no connection or runtime; the
engine owns its runtimes), the execution id minted once per plan run -- and engine-client's conformance suite (the
contract's R7) run in Python's V8, against Python's engine, through the V8 bridge (engine-wire.ts bridge: the host's
``engine`` call, answered by Engine.answer_over over the call's read)."""

import base64
import functools
import http.server
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc

from unittest import mock

from legend_lite import compiler, contract, projects
from legend_lite._planned import plan_from_answer
from legend_lite._headless import Headless, bundle
from legend_lite.engine import Engine
from legend_lite.frames import Frames

ROWS = pa.table({'id': [1, 2, 3, 4], 'name': ['a', 'b', 'c', 'd']})
CSV = 'id,name\n1,a\n2,b\n3,c\n'
SENTINEL = 'SENTINEL-held-4f1c'


class _Served_Files:
    """A folder served over HTTP on 127.0.0.1: what a remote source reads (a URL's query string ignored)."""

    def __init__(self):
        self.folder = tempfile.TemporaryDirectory()
        Path(self.folder.name, 'remote.csv').write_text(CSV, encoding='utf-8')

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
                                                      functools.partial(Quiet, directory=self.folder.name))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = f'http://127.0.0.1:{self.server.server_address[1]}'

    def close(self):
        self.server.shutdown()
        self.folder.cleanup()


class _Served(unittest.TestCase):
    """A frame served by an engine, and its model as the contract hands it DataCube (R3a (c))."""

    def setUp(self):
        self.frames = Frames()
        self.table = self.frames.register('conformance_t', ROWS)
        self.engine = Engine(self.frames)
        self.model, self.runtime = self.engine.contract.served(self.table.model, self.table.runtime)
        self.query = {'_type': 'lambda', 'parameters': [], 'body': [self.table.source]}

    def call(self, name, body):
        said = self.engine.contract.answer(name, json.dumps(body))
        if said.content_type == 'application/json':
            return said.status, json.loads(said.body) if said.body else None
        return said.status, said.body

    def session(self):
        return self.call('open', {})[1]['session']


class TheDoorTest(_Served):

    def test_capabilities_state_the_contract_and_what_this_engine_does(self):
        status, caps = self.call('capabilities', {})
        self.assertEqual(status, 200)
        self.assertEqual(caps['contract'], contract.VERSION)
        self.assertEqual(caps['engine'], {'name': 'Python', 'databaseType': 'DuckDB'})
        self.assertTrue(caps['calls']['query'] and caps['calls']['typeOf'])
        self.assertEqual(caps['calls']['write'], {'modes': ['create', 'overwrite', 'append', 'deleteAppend', 'merge']})
        self.assertEqual(caps['calls']['addSource'], {'sources': ['upload', 'held', 'url', 'remote', 'query'],
                                                      'how': ['live', 'snap']})

    def test_a_model_handed_to_datacube_holds_no_connection_or_runtime_and_names_its_runtime(self):
        self.assertNotIn('###Connection', self.model)
        self.assertNotIn('###Runtime', self.model)
        self.assertEqual(self.runtime, self.table.runtime)

    def test_a_model_declaring_a_connection_or_an_unknown_runtime_is_refused_by_name(self):
        s = self.session()
        status, said = self.call('typeOf', {'session': s, 'ask': {'model': self.table.model, 'runtime': self.runtime,
                                                                   'query': self.query}})
        self.assertEqual((status, said['error']['kind']), (403, 'refused'))
        self.assertIn("a model declaring a connection", said['error']['message'])
        status, said = self.call('query', {'session': s, 'ask': {'model': self.model, 'runtime': 'x::RT', 'query': self.query}})
        self.assertEqual(said['error']['what'], "the runtime 'x::RT'")

    def test_a_query_answers_arrow_with_its_facts_and_never_the_plan(self):
        s = self.session()
        status, body = self.call('query', {'session': s, 'ask': {'model': self.model, 'runtime': self.runtime,
                                                                  'query': self.query}})
        self.assertEqual(status, 200)
        table = pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all()
        meta = table.schema.metadata
        self.assertNotIn(b'legend.plan', meta, 'no plan crosses into DataCube (R4)')
        facts = json.loads(meta[contract.META])
        self.assertEqual(facts['schema'], [{'name': 'id', 'type': 'Integer'}, {'name': 'name', 'type': 'String'}])
        self.assertTrue(facts['executionId'])
        self.assertIn('SELECT', facts['explain'])
        self.assertEqual(table.num_rows, 4)

    def test_each_call_names_an_open_session_and_an_unknown_call_is_refused(self):
        status, said = self.call('typeOf', {'session': 'session-none', 'ask': {}})
        self.assertEqual(said['error']['what'], "session 'session-none'")
        status, said = self.call('nope', {})
        self.assertEqual(said['error']['what'], "the call 'nope'")
        s = self.session()
        self.call('close', {'session': s})
        status, said = self.call('parse', {'session': s, 'text': '|1'})
        self.assertEqual(said['error']['kind'], 'refused')

    def write(self, s, target, mode):
        return self.call('write', {'session': s, 'ask': {'box': {'model': self.model, 'runtime': self.runtime,
                                                                 'query': self.query}, 'target': {'table': target},
                                                         'mode': mode}})

    def rows(self, table):
        return self.frames.connection._con.execute(f'SELECT id, name FROM {table} ORDER BY id').fetchall()

    def test_write_is_one_call_in_each_mode_the_engines_transaction_and_its_own_execution_id(self):
        s = self.session()
        status, said = self.write(s, 'out_t', 'create')
        self.assertEqual((status, said['rowsWritten']), (200, 4))
        self.assertEqual(self.rows('out_t'), [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')])
        self.assertEqual(self.write(s, 'out_t', 'append')[1]['rowsWritten'], 4)
        self.assertEqual(len(self.rows('out_t')), 8)
        self.assertEqual(self.write(s, 'out_t', 'deleteAppend')[1]['rowsWritten'], 4)
        self.assertEqual(len(self.rows('out_t')), 4)
        self.assertEqual(self.write(s, 'out_t', {'merge': {'keys': ['id']}})[1]['rowsWritten'], 4)
        self.assertEqual(self.rows('out_t'), [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')])
        first, second = self.write(s, 'out_t', 'overwrite')[1], self.write(s, 'out_t', 'overwrite')[1]
        self.assertNotEqual(first['executionId'], second['executionId'])
        # a create of a table there already: the database's words, nothing of it kept, its execution id
        status, said = self.write(s, 'out_t', 'create')
        self.assertEqual((status, said['error']['kind']), (422, 'run'))
        self.assertTrue(said['error']['executionId'])
        self.assertEqual(len(self.rows('out_t')), 4)

    def test_write_derives_its_tables_and_refuses_the_engines_own_and_a_write_in_a_read(self):
        s = self.session()
        status, said = self.write(s, 'conformance_t', 'append')
        self.assertEqual(said['error']['what'], "the table 'conformance_t'")
        status, said = self.call('write', {'session': s, 'ask': {'box': {'model': self.model, 'runtime': self.runtime,
                                                                        'query': self.query}, 'target': {'table': 'x'},
                                                                'mode': 'append', 'sources': ['conformance_t']}})
        self.assertEqual(said['error']['kind'], 'refused', 'the box\'s tables are the engine\'s to derive, never sent')
        said = self.engine.contract.answer('write', json.dumps({'session': s, 'ask': {
            'box': {'model': self.model, 'runtime': self.runtime, 'query': self.query}, 'target': {'table': 'x'},
            'mode': 'create'}}), over=object())
        self.assertIn('a write made in a read', json.loads(said.body)['error']['message'])

    def add(self, s, source, how):
        return self.call('addSource', {'session': s, 'source': source, 'how': how})

    def test_an_upload_is_a_handle_described_queried_and_dropped(self):
        s = self.session()
        status, said = self.add(s, {'upload': {'name': 'up.csv', 'bytes': base64.b64encode(CSV.encode()).decode()}}, 'snap')
        self.assertEqual(status, 200, said)
        handle = said['handle']
        self.assertTrue(handle['handle'].startswith('h-') and said['executionId'] and said['size'] == len(CSV))
        self.assertNotIn('###Connection', said['model'])
        status, described = self.call('describe', {'session': s, 'handle': handle})
        self.assertEqual(described['columns'], [{'name': 'id', 'type': 'Integer'}, {'name': 'name', 'type': 'String'}])
        query = {'_type': 'lambda', 'parameters': [], 'body': [said['relation']]}
        status, body = self.call('query', {'session': s, 'ask': {'model': said['model'], 'runtime': said['runtime'],
                                                                  'query': query}})
        self.assertEqual(pyarrow.ipc.open_stream(pa.py_buffer(body)).read_all().num_rows, 3)
        self.assertEqual(self.call('drop', {'session': s, 'handle': handle}), (200, {}))
        self.assertEqual(self.call('describe', {'session': s, 'handle': handle})[1]['error']['kind'], 'refused')
        status, said = self.call('query', {'session': s, 'ask': {'model': said['model'], 'runtime': said['runtime'],
                                                                  'query': query}})
        self.assertIn('no runtime this engine owns', said['error']['message'])

    def test_a_held_file_is_named_by_the_handle_the_engine_issued(self):
        s = self.session()
        folder = Path(tempfile.mkdtemp())
        (folder / 'held.csv').write_text(CSV, encoding='utf-8')
        held = self.engine.contract.hold(str(folder / 'held.csv'))
        self.assertNotIn(str(folder), json.dumps(held))
        status, said = self.add(s, {'held': held}, 'snap')
        self.assertEqual(status, 200, said)
        for foreign in ({'handle': str(folder / 'held.csv')}, {'handle': 'held-none'}):
            self.assertEqual(self.add(s, {'held': foreign}, 'snap')[1]['error']['kind'], 'refused')
        self.assertIn("a file added 'live'", self.add(s, {'held': held}, 'live')[1]['error']['message'])

    def test_a_secret_is_write_only_bound_by_name_and_never_echoed(self):
        served = _Served_Files()
        self.addCleanup(served.close)
        s = self.session()
        said = self.engine.contract.answer('bindSecret', json.dumps({'session': s, 'name': 'r_url', 'fields': {
            'URL': f'{served.base}/remote.csv?sig={SENTINEL}'}}))
        self.assertEqual((said.status, said.body), (204, b''), 'nothing answered')
        status, added = self.add(s, {'remote': {'kind': 'bearer', 'ref': f'{served.base}/remote.csv', 'secret': 'r_url'}},
                                 'live')
        self.assertEqual(status, 200, added)
        self.assertNotIn(SENTINEL, json.dumps(added))
        query = {'_type': 'lambda', 'parameters': [], 'body': [added['relation']]}
        status, body = self.call('query', {'session': s, 'ask': {'model': added['model'], 'runtime': added['runtime'],
                                                                  'query': query}})
        self.assertEqual(status, 200, body)
        self.assertNotIn(SENTINEL.encode(), body)
        # a remote file that is not there: the database's words, the secret taken out of them
        self.call('bindSecret', {'session': s, 'name': 'gone_url', 'fields': {'URL': f'{served.base}/gone.csv?sig={SENTINEL}'}})
        status, said = self.add(s, {'remote': {'kind': 'bearer', 'ref': f'{served.base}/gone.csv', 'secret': 'gone_url'}},
                                'live')
        self.assertEqual(said['error']['kind'], 'run')
        self.assertNotIn(SENTINEL, json.dumps(said))
        self.assertIn('<redacted>', said['error']['message'])
        # a secret this session holds none of, and a field no secret has: refused by name, no value said
        self.assertIn("the secret 'none'", self.add(s, {'remote': {'kind': 'bearer', 'ref': 'x.csv', 'secret': 'none'}},
                                                     'live')[1]['error']['message'])
        status, said = self.call('bindSecret', {'session': s, 'name': 'bad', 'fields': {'PASSWORD': SENTINEL}})
        self.assertNotIn(SENTINEL, json.dumps(said))
        # dropped with its session: another session holds none of it
        self.call('close', {'session': s})
        t = self.session()
        self.assertIn("the secret 'r_url'", self.add(t, {'remote': {'kind': 'bearer', 'ref': 'x.csv', 'secret': 'r_url'}},
                                                      'live')[1]['error']['message'])

    def test_a_querys_rows_snapped_and_a_closed_sessions_sources_dropped(self):
        s = self.session()
        status, said = self.add(s, {'query': {'model': self.model, 'runtime': self.runtime, 'query': self.query}}, 'snap')
        self.assertEqual(status, 200, said)
        table = said['name']
        self.assertEqual(self.frames.connection._con.execute(f'SELECT count(*) FROM {table}').fetchone(), (4,))
        self.call('close', {'session': s})
        self.assertEqual(self.frames.connection._con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'").fetchone(), (0,))

    def test_a_stamped_plans_stamp_is_bound_to_its_doors_execution_id(self):
        """Step 3b (G10, G11: a stamped plan runs the same on every host, never refused on one): the real output plan
        made to read its execution's stamp -- its final count `SELECT ? AS value`, a `#stamp` slot -- so the write answers
        the trace comment the runner bound in place of a count: the database's text names the id the DOOR minted."""
        real = compiler.output_plan

        def stamped(request):
            plan = json.loads(real(request))
            last = plan['root']['steps'][-1]
            self.assertEqual(last['_type'], 'tdsResult')
            last['sql'] = {**last['sql'], 'statement': 'SELECT ? AS value',
                           'slots': [{'parameter': '#stamp0', 'binding': {'_type': 'one'}}]}
            return plan_from_answer(json.dumps(plan))

        s = self.session()
        with mock.patch.object(compiler, 'output_plan', stamped):
            status, said = self.write(s, 'stamped_t', 'create')
        error = said['error']
        self.assertEqual(error['kind'], 'run')
        self.assertIn(f'executionTraceID\\" : \\"{error["executionId"]}', error['message'])

    def test_the_runner_refuses_a_stamped_plan_with_no_id_and_a_writes_own_stamp(self):
        plan = json.loads(compiler.output_plan({
            'databaseType': 'DuckDB', 'tables': [{'table': 'conformance_t', 'pkg': 'conformance_t', 'convertible': True,
                                                  'columns': self.engine.contract._side.catalog(None, 'conformance_t')}],
            'output': {'name': 'never_t', 'mode': 'create'}, 'rows': json.dumps(self.query)}))
        last = plan['root']['steps'][-1]
        last['sql'] = {**last['sql'], 'statement': 'SELECT ? AS value',
                       'slots': [{'parameter': '#stamp0', 'binding': {'_type': 'one'}}]}
        with self.assertRaisesRegex(projects.PlanRefused, "'#stamp0' reads its execution's stamp, and the run was given no "
                                                          'id by its door'):
            projects.run_plan(plan_from_answer(json.dumps(plan)), lambda sql, params: [], 'DuckDB')
        first = plan['root']['steps'][0]
        first['sql'] = {**first['sql'], 'stamp': '#stamp0'}
        with self.assertRaisesRegex(projects.PlanRefused, 'a write or effect statement binds its own execution-trace stamp'):
            projects.run_plan(plan_from_answer(json.dumps(plan)), lambda sql, params: [], 'DuckDB', execution='x')

    def test_parameters_are_refused_by_name_until_step_6(self):
        s = self.session()
        status, said = self.call('query', {'session': s, 'ask': {'model': self.model, 'runtime': self.runtime,
                                                                  'query': self.query, 'parameters': {'p': {}}}})
        self.assertEqual(said['error']['what'], "a query's parameters")


class ConformanceSuiteTest(_Served):
    """ENGINE-CLIENT'S CONFORMANCE SUITE, against Python headless, through the V8 bridge: every case passes."""

    def test_python_headless_conforms(self):
        script = Path(os.environ['LEGEND_LITE_ENGINE_CONFORMANCE']).read_text(encoding='utf-8')
        served = _Served_Files()
        self.addCleanup(served.close)
        held_file = Path(served.folder.name, 'held.csv')
        held_file.write_text(CSV, encoding='utf-8')
        fixture = {
                   'upload': {'name': 'up.csv', 'text': CSV, 'rows': 3}, 'sentinel': SENTINEL,
                   'held': self.engine.contract.hold(str(held_file)), 'served': {'base': served.base},'model': self.model, 'runtime': self.runtime, 'source': self.table.source,
                   'columns': [{'name': 'id', 'type': 'Integer'}, {'name': 'name', 'type': 'String'}],
                   'rowCount': ROWS.num_rows, 'textColumn': 'name', 'keyColumn': 'id', 'ownTable': 'conformance_t',
                   'store': 'conformance_t::DB', 'writeTarget': 'conformance_out'}
        with Headless(bundle(), connection=self.frames.connection, reading=self.frames.reading_now) as h:
            h.answers = lambda: self.engine.answer_over
            h.execute(script)
            # in no read of the frames: a query reads in a read of its own, a write is made in none (step 2)
            results = json.loads(h.evaluate(f'legendLiteConformance.run({json.dumps(json.dumps(fixture))})'))
        for r in results:
            print(f"{'ok  ' if r['ok'] else 'FAIL'} {r['name']}{'' if r['ok'] else ': ' + r.get('error', '')}", flush=True)
        self.assertGreaterEqual(len(results), 19)
        self.assertEqual([r for r in results if not r['ok']], [])
        self.assertEqual([r for r in results if r.get('notRun')], [], 'every case runs against Python headless')


if __name__ == '__main__':
    unittest.main()
