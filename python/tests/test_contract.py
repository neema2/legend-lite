"""DATACUBE'S ENGINE CONTRACT ON PYTHON (legend_lite/contract.py; docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, step 1,
2026-11-13): the door's calls and refusals by name, R3a (a model DataCube sends holds no connection or runtime; the
engine owns its runtimes), the execution id minted once per plan run -- and engine-client's conformance suite (the
contract's R7) run in Python's V8, against Python's engine, through the V8 bridge (engine-wire.ts bridge: the host's
``engine`` call, answered by Engine.answer_over over the call's read)."""

import json
import os
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc

from legend_lite import contract
from legend_lite._headless import Headless, bundle
from legend_lite.engine import Engine
from legend_lite.frames import Frames

ROWS = pa.table({'id': [1, 2, 3, 4], 'name': ['a', 'b', 'c', 'd']})


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
            return said.status, json.loads(said.body)
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
        self.assertNotIn('write', caps['calls'], 'write is step 2')

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

    def test_parameters_are_refused_by_name_until_step_6(self):
        s = self.session()
        status, said = self.call('query', {'session': s, 'ask': {'model': self.model, 'runtime': self.runtime,
                                                                  'query': self.query, 'parameters': {'p': {}}}})
        self.assertEqual(said['error']['what'], "a query's parameters")


class ConformanceSuiteTest(_Served):
    """ENGINE-CLIENT'S CONFORMANCE SUITE, against Python headless, through the V8 bridge: every case passes."""

    def test_python_headless_conforms(self):
        script = Path(os.environ['LEGEND_LITE_ENGINE_CONFORMANCE']).read_text(encoding='utf-8')
        fixture = {'model': self.model, 'runtime': self.runtime, 'source': self.table.source,
                   'columns': [{'name': 'id', 'type': 'Integer'}, {'name': 'name', 'type': 'String'}],
                   'rowCount': ROWS.num_rows, 'textColumn': 'name'}
        with Headless(bundle(), connection=self.frames.connection, reading=self.frames.reading_now) as h:
            h.answers = lambda: self.engine.answer_over
            h.execute(script)
            with self.frames.reading():
                results = json.loads(h.evaluate(f'legendLiteConformance.run({json.dumps(json.dumps(fixture))})'))
        for r in results:
            print(f"{'ok  ' if r['ok'] else 'FAIL'} {r['name']}{'' if r['ok'] else ': ' + r.get('error', '')}", flush=True)
        self.assertGreaterEqual(len(results), 9)
        self.assertEqual([r for r in results if not r['ok']], [])


if __name__ == '__main__':
    unittest.main()
