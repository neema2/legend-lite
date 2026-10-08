"""The engine (legend_lite.engine): legend-engine's pure/v1 API over frames, as DataCube's remote client calls it --
parse, print and a query's types answered by legend-lite's server code, execute in upstream's Arrow format with
the rows DuckDB computes over the frame as it is then; the refusals in the engine's error shape; and only the
engine's own page answered (its token, a local Host, no cross-origin answer)."""

import http.client
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
import urllib.error
import urllib.request

import pandas as pd
import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite.engine import Engine

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
        self.engine = Engine(self.frames)

    def tearDown(self):
        self.engine.close()

    def post(self, path, body, content_type='application/json', headers=None):
        """The engine's answer to a POST: status, headers, body bytes."""
        sent = {'Content-Type': content_type, 'Authorization': self.engine.authorization} | (headers or {})
        request = urllib.request.Request(self.engine.url + path, body.encode('utf-8'), sent, method='POST')
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
            answers[i] = self.arrow(GROUP)[1].to_pylist()

        threads = [threading.Thread(target=ask, args=(i,)) for i in range(len(answers))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(answers, [[{'desk': 'EQ', 'q': 27.5}, {'desk': 'FX', 'q': 40.75}]] * len(answers))
        self.assertEqual(ll.compiler.library().unfreed(), 0)


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

    def test_execute_in_json_is_refused_naming_the_format_served(self):
        status, body = self.error('/api/pure/v1/execution/execute', self.execute_input(self.query('')))
        self.assertEqual(status, 500)
        self.assertIn('ARROW_IPC', body['message'])

    def test_the_database_refusing_is_answered_in_the_engines_shape(self):
        status, body = self.error('/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC',
                                  self.execute_input(self.query('->extend(~[n: x|$x.desk->toOne()->parseInteger()])')))
        self.assertEqual((status, body['code'], body['status']), (500, -1, 'error'))
        self.assertNotIn('errorType', body)
        self.assertIn('Conversion', body['message'])

    def test_a_path_the_api_does_not_serve_is_404_in_the_engines_shape(self):
        status, body = self.error('/api/pure/v1/nope', '{}')
        self.assertEqual((status, body['message']), (404, 'no such legend-engine API in legend-lite: /api/pure/v1/nope'))


class OnlyItsOwnPage(Served):
    def raw(self, method, path, headers, body=b''):
        port = int(self.engine.url.rsplit(':', 1)[1])
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
        return self.engine.url.removeprefix('http://')

    def test_a_request_without_the_token_is_refused(self):
        tree = json.dumps(ll.parse('|1'))
        for given in ({}, {'Authorization': 'Bearer nope'}, {'Authorization': self.engine.token}):
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': self.host(), 'Content-Length': str(len(tree))} | given, tree.encode())
            self.assertEqual(status, 401, given)

    def test_a_host_that_is_not_this_machine_is_refused(self):
        port = self.engine.url.rsplit(':', 1)[1]
        for host in ('evil.example:' + port, '127.0.0.1:1', 'localhost'):
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': host, 'Authorization': self.engine.authorization, 'Content-Length': '2'}, b'{}')
            self.assertEqual(status, 403, host)
        status, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false',
                                {'Host': 'localhost:' + port, 'Authorization': self.engine.authorization,
                                 'Content-Length': '2'}, b'|1')
        self.assertEqual(status, 200)

    def test_no_cross_origin_request_is_answered(self):
        status, headers, _ = self.raw('OPTIONS', '/api/pure/v1/execution/execute', {
            'Host': self.host(), 'Origin': 'http://evil.example', 'Access-Control-Request-Method': 'POST'})
        self.assertEqual(status, 405)
        self.assertFalse([h for h in headers if h.lower().startswith('access-control-')], headers)

    def test_a_body_past_the_limit_is_refused_unread(self):
        status, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                {'Host': self.host(), 'Authorization': self.engine.authorization,
                                 'Content-Length': str(64 * 1024 * 1024)})
        self.assertEqual(status, 413)

    def test_a_length_that_is_not_ascii_digits_or_a_body_not_utf8_is_answered(self):
        for length, body, status in (('²', b'', 411), ('2', b'\xff\xfe', 400)):
            got, _, _ = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                 {'Host': self.host(), 'Authorization': self.engine.authorization,
                                  'Content-Length': length}, body)
            self.assertEqual(got, status, length)

    def test_a_refused_request_s_body_is_read_so_its_answer_arrives(self):
        body = b'x' * 200_000
        status, _, answer = self.raw('POST', '/api/pure/v1/grammar/grammarToJson/lambda',
                                     {'Host': self.host(), 'Content-Length': str(len(body))}, body)
        self.assertEqual(status, 401)
        self.assertIn(b"the engine's token", answer)

    def test_closed_it_answers_nothing(self):
        url = self.engine.url
        self.engine.close()
        self.engine = Engine(self.frames)  # tearDown closes this one
        request = urllib.request.Request(url + '/api/pure/v1/nope', b'{}', {'Authorization': 'Bearer x'})
        with self.assertRaises(urllib.error.URLError) as refused:
            urllib.request.urlopen(request, timeout=5)
        self.assertNotIsInstance(refused.exception, urllib.error.HTTPError)
        self.assertIsInstance(refused.exception.reason, ConnectionRefusedError)

    def test_a_connection_that_sends_nothing_neither_blocks_others_nor_close(self):
        port = int(self.engine.url.rsplit(':', 1)[1])
        idle = [socket.create_connection(('127.0.0.1', port)) for _ in range(6)]
        try:
            # more idle connections than the compiler's pool has threads: a request is still answered
            tree = json.dumps(ll.parse('|1'))
            status, _, _ = self.raw('POST', '/api/pure/v1/grammar/jsonToGrammar/lambda',
                                    {'Host': self.host(), 'Authorization': self.engine.authorization,
                                     'Content-Length': str(len(tree))}, tree.encode())
            self.assertEqual(status, 200)
            started = time.monotonic()
            self.engine.close()
            self.assertLess(time.monotonic() - started, 5)
        finally:
            for connection in idle:
                connection.close()
            self.engine = Engine(self.frames)  # tearDown closes this one


class Site(unittest.TestCase):
    """The site's files (DataCube's built pages), at the engine's origin: to a local Host, under the site only."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
        (self.root / 'index.html').write_text('<!doctype html>cube')
        (self.root / 'vendor').mkdir()
        (self.root / 'vendor' / 'planner.wasm').write_bytes(b'\0asm')
        (self.root.parent / 'outside.txt').write_text('not the site')
        self.frames = ll.Frames()
        self.engine = Engine(self.frames, site=self.root)

    def tearDown(self):
        self.engine.close()

    def get(self, path, host=None):
        port = int(self.engine.url.rsplit(':', 1)[1])
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
        port = self.engine.url.rsplit(':', 1)[1]
        self.assertEqual(self.get('/', host=f'evil.example:{port}')[0], 403)

    def test_an_engine_without_a_site_serves_no_file(self):
        bare = Engine(self.frames)
        try:
            port = int(bare.url.rsplit(':', 1)[1])
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
            connection.request('GET', '/')
            self.assertEqual(connection.getresponse().status, 404)
            connection.close()
        finally:
            bare.close()


class Cube(Served):
    """cube.json: what DataCube's engine page shows -- a frame's model, runtime and source, asked with the token."""

    def get(self, path, headers=None):
        request = urllib.request.Request(self.engine.url + path, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, r.headers['Content-Type'], r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers['Content-Type'], e.read()

    def test_a_frame_s_cube_as_it_is_now(self):
        status, content_type, body = self.get('/cube.json?table=trades', {'Authorization': self.engine.authorization})
        self.assertEqual((status, content_type), (200, 'application/json'))
        self.assertEqual(json.loads(body), {'title': 'trades', 'model': self.table.model, 'runtime': self.table.runtime,
                                            'source': self.table.source})
        # a Live frame read again: a new column, a new model
        self.df['extra'] = 1
        _, _, body = self.get('/cube.json?table=TRADES', {'Authorization': self.engine.authorization})
        self.assertIn('extra', json.loads(body)['model'])

    def test_asked_with_the_token_only_and_for_a_frame_it_serves(self):
        self.assertEqual(self.get('/cube.json?table=trades')[0], 401)
        self.assertEqual(self.get('/cube.json?table=nope', {'Authorization': self.engine.authorization})[0], 404)
