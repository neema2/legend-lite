"""LEGEND-ENGINE COMPATIBILITY, AT THE EDGE (the edge program's H5, 2026-11-17; its groundwork the same day): the edge's
own door composes an engine with the edge's routes (``legend_lite.edge.engine``), as Java's EdgeServer composes the
server -- the one place the two meet. legend-engine's ``pure/v1`` API, its execute (Arrow and JSON) and its saved-query
store at ``/api/pure/v1/query`` are served there, and only there: a product engine answers none of them. This target
alone depends on //python:edge; the product's tests run without it (they are imported here for their fixtures only)."""

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite import edge
from legend_lite._edge_seam import NO_EDGE_ROUTES, EdgeRoutes
from legend_lite.engine import Engine, WebServer

import test_engine
from test_engine import GROUP, PARTY, PARTY_QUERY, trades


def read_arrow(body):
    """An ARROW_IPC answer read as upstream writes it: one zstd frame around an Arrow IPC stream."""
    with pa.input_stream(pa.py_buffer(body), compression='zstd') as stream:
        return pyarrow.ipc.open_stream(stream).read_all()


class TheEdgesDoor(unittest.TestCase):

    def test_the_door_composes_an_engine_with_the_edges_routes(self):
        engine = edge.engine(ll.Frames())
        self.assertIsInstance(engine.edge, edge.EngineRoutes)
        self.assertIsInstance(engine.edge, EdgeRoutes)
        self.assertIsNot(engine.edge, NO_EDGE_ROUTES)
        # legend-engine's routes are the edge's: its POSTs, and its store whatever the method
        self.assertTrue(engine.edge.serves('POST', '/api/pure/v1/grammar/grammarToJson/lambda'))
        self.assertTrue(engine.edge.serves('GET', '/api/pure/v1/query/q-1'))
        self.assertFalse(engine.edge.serves('GET', '/api/pure/v1/grammar/grammarToJson/lambda'))
        self.assertFalse(engine.edge.serves('POST', '/api/lite/v1/query/parse'))
        # a call no route serves is refused as the product refuses it
        said = engine.answer('POST', '/no-such-route.json', '', '{}')
        self.assertEqual((said.status, said.body), (404, b'no such path: /no-such-route.json'))

    def test_the_door_gives_the_routes_itself(self):
        with self.assertRaisesRegex(TypeError, "the edge's door gives the engine its routes itself"):
            edge.engine(ll.Frames(), edge=NO_EDGE_ROUTES)

    def test_a_product_engine_serves_none_of_legend_engines_routes(self):
        engine = Engine(ll.Frames())
        for method, path in (('POST', '/api/pure/v1/grammar/grammarToJson/lambda'),
                             ('POST', '/api/pure/v1/execution/execute'), ('GET', '/api/pure/v1/query/q-1')):
            said = engine.answer(method, path, '', '|1')
            self.assertEqual((said.status, said.body), (404, f'no such path: {path}'.encode()), path)


class EdgeOn(unittest.TestCase):
    """THE ENGINE LIBRARY, OPT-IN (K2, the user's "product root, edge opt-in", 2026-11-18): legend-engine's entry
    points are //native:engine's (libcompiler_engine, the legend-lite-engine wheel), loaded by the edge's key alone;
    its routes answer byte-exact as before (this lane's legend-engine tests, below). Without it, the edge's door is
    refused by name."""

    def test_the_engine_library_exports_every_edge_entry(self):
        from legend_lite import _loader
        from legend_lite.edge._engine import engine_library
        self.assertEqual(sorted(n for n in _loader.ENGINE_ARITY if not engine_library().exports(n)), [])
        self.assertNotEqual(engine_library().path, ll.compiler.library().path, 'a library of its own')

    def test_without_the_engine_library_the_door_is_refused_by_name(self):
        import builtins
        import sys
        from legend_lite import _loader
        real_import = builtins.__import__

        def no_engine_package(name, *args, **kwargs):
            if name == 'legend_lite_engine':
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        with mock.patch.dict(os.environ, {'LEGEND_LITE_ENGINE_LIBRARY': ''}), \
                mock.patch.object(_loader, '_engine', None), mock.patch.dict(sys.modules), \
                mock.patch.object(builtins, '__import__', no_engine_package):
            sys.modules.pop('legend_lite_engine', None)
            with self.assertRaisesRegex(edge.EngineMissing, r'legend-engine needs the legend-lite\[engine\] extra'):
                edge.engine(ll.Frames())


class EdgeServed(test_engine.Served):
    """An engine over one Live frame composed at the edge's door, and requests as a legend-engine client sends them."""

    def setUp(self):
        self.df = trades()
        self.frames = ll.Frames()
        self.table = self.frames.register('trades', self.df)
        self.server = WebServer(edge.engine(self.frames))

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


class UnexpectedFailures(EdgeServed):
    def test_an_executes_unexpected_failure_says_no_held_value(self):
        calls = []

        def frame():
            calls.append(1)
            if len(calls) > 1:
                raise RuntimeError('the live frame could not be read: X-Amz-Signature=held-0123456789')
            return trades()

        self.frames.register('broken', frame)
        # a value the engine holds (a bearer URL's): never in an answer's words
        held = 'X-Amz-Signature=held-0123456789'
        original = self.frames.held_values
        self.frames.held_values = lambda: (*original(), held)
        status, _, body = self.post('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                    self.execute_input(self.query('->select(~[desk])')))
        # answered by the API's own catch (the server's refusal), its words with the held value taken out
        said = body.decode('utf-8')
        self.assertIn('RuntimeError: the live frame could not be read', said)
        self.assertNotIn('held-0123456789', said)


class Calls(EdgeServed):
    def test_parse_print_and_types_are_the_compilers(self):
        text = '|' + self.table.accessor + GROUP
        status, headers, body = self.post('/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false',
                                          text, 'text/plain')
        self.assertEqual((status, headers['Content-Type']), (200, 'application/json'))
        tree = json.loads(body)
        self.assertEqual(tree, ll.parse(text))

        status, headers, body = self.post('/api/pure/v1/grammar/jsonToGrammar/lambda?renderStyle=STANDARD', json.dumps(tree))
        self.assertEqual((status, headers['Content-Type']), (200, 'text/plain'))
        self.assertEqual(body.decode('utf-8'), ll.print_tree(tree, 'standard'))

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


class ProjectQueries(EdgeServed):
    """A project's query run through legend-engine's execute (C5): Query or Studio as a legend-engine client sends a
    model none of the frames wrote, with its own Data rows; Python makes that project's database inside its DuckDB and
    runs the query there on a cursor of its own, the frames as they were."""

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
        main = self.frames.connection._con.execute(
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


class Refusals(EdgeServed):
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


class TheEnginesStore(unittest.TestCase):
    """legend-engine's saved-query store at its own mount (/api/pure/v1/query, its pages at .../dataCube), in
    legend-engine's words: the same rules over the same folders as lite's route, so a record saved at one is read at
    the other."""

    def setUp(self):
        self.place = tempfile.TemporaryDirectory()
        self.addCleanup(self.place.cleanup)
        self.engine = edge.engine(ll.Frames(), pages=Path(self.place.name) / 'pages')
        home = mock.patch.dict(os.environ, {'LEGEND_LITE_HOME': self.place.name})
        home.start()
        self.addCleanup(home.stop)

    def test_a_missing_query_is_refused_in_legend_engines_words(self):
        said = self.engine.answer('GET', '/api/pure/v1/query/nope', '', None)
        self.assertEqual((said.status, json.loads(said.body)), (404, {'message': "Can't find query with ID 'nope'"}))
        self.assertEqual(self.engine.answer('PATCH', '/api/pure/v1/query/nope', '', None).status, 405)

    def test_a_page_saved_at_lites_route_is_the_one_record_both_mounts_read(self):
        made = self.engine.answer('POST', '/api/lite/v1/queries/dataCube', '',
                                  json.dumps({'id': 'p-9', 'name': 'Both', 'content': {'kind': 'datacube.page'}}))
        self.assertEqual(made.status, 200, made.body)
        for mount in ('/api/lite/v1/queries/dataCube', '/api/pure/v1/query/dataCube'):
            read = self.engine.answer('GET', mount + '/p-9', '', None)
            self.assertEqual((read.status, json.loads(read.body)['name']), (200, 'Both'), mount)

    def test_its_store_needs_the_token(self):
        server = WebServer(self.engine)
        self.addCleanup(server.close)
        for path in ('/api/pure/v1/query/search', '/api/pure/v1/query/dataCube/search'):
            request = urllib.request.Request(server.url + path, b'{}', {'Authorization': 'Bearer nope'}, method='POST')
            with self.assertRaises(urllib.error.HTTPError) as refused:
                urllib.request.urlopen(request, timeout=30)
            self.assertEqual(refused.exception.code, 401, path)
            request = urllib.request.Request(server.url + path, b'{}', {'Authorization': server.authorization},
                                             method='POST')
            with urllib.request.urlopen(request, timeout=30) as r:
                self.assertEqual(r.status, 200, path)


if __name__ == '__main__':
    unittest.main()
