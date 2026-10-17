"""Grids headless (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §4): a page's grid run by DataCube's own code in V8
-- ``grid.view()``, what the grid shows as DataCube computes it; ``grid.relation``, its rows at their finest level as
Pure, as Create a Class makes them; ``grid.to_pandas()``, every one of those rows from the frames' database -- each
checked against pandas computing the same, in the same cell, with no browser."""

import base64
import json
import os
import unittest
from pathlib import Path
from decimal import Decimal

import pandas as pd
import pyarrow.ipc

import legend_lite as ll
from legend_lite import datacube
from legend_lite._headless import Headless, bundle
from legend_lite.engine import Engine

TRADES = pd.DataFrame({
    'region': ['EMEA', 'EMEA', 'AMER', 'APAC', 'AMER', 'EMEA'],
    'desk': ['Rates', 'FX', 'Rates', 'FX', 'FX', 'Rates'],
    'notional': [100.0, 250.5, 75.25, 10.0, 40.0, 1.5],
    'qty': [1, 2, 3, 4, 5, 6],
})


def column(view, name):
    """A view's column of values, by its name."""
    at = [c['name'] for c in view.columns].index(name)
    return [row[at] for row in view.rows]


def groups(view, measure):
    """A grouped view's level-1 groups: label -> the measure's value."""
    labels, values = column(view, '__tree'), column(view, measure)
    return {labels[i]: values[i] for i, t in enumerate(view.tree) if t['level'] == 1}


class Grids(unittest.TestCase):
    def setUp(self):
        self.page = ll.Page('Headless trades')

    def test_a_grouped_grid_shows_what_pandas_computes(self):
        grid = self.page.grid(TRADES, name='trades_grouped', rows=['region'], measures={'notional': 'sum', 'qty': 'sum'})
        view = grid.view()
        # as on screen: the groups' labels first, then the columns, each aggregated
        self.assertEqual([c['name'] for c in view.columns], ['__tree', 'desk', 'notional', 'qty'])
        expected = TRADES.groupby('region')[['notional', 'qty']].sum()
        self.assertEqual(groups(view, 'qty'), expected.qty.to_dict())
        for region, notional in groups(view, 'notional').items():
            self.assertIsInstance(notional, float)
            self.assertAlmostEqual(notional, expected.notional[region])
        # a column not measured shows its one value, or none when the group holds several (uniqueValueOnly)
        desks = {r: (d.iloc[0] if d.nunique() == 1 else None) for r, d in TRADES.groupby('region').desk}
        self.assertEqual(groups(view, 'desk'), desks)
        self.assertTrue(all(t['group'] and not t['expanded'] for t in view.tree))
        self.assertEqual(view.truncated, [])
        self.assertIn('groupBy', view.query)
        self.assertIn('GROUP BY', view.sql.upper())

    def test_a_grid_exports_as_its_export_menu_makes_the_file(self):
        # the screen's own core (export-render.ts), run headless: each format DataCube's Export menu offers, with the
        # grid's formats (a sum past a thousand, written with its separator where a person reads it) and its heatmap
        import csv
        import io
        import pandas as pd
        frame = pd.DataFrame({'region': ['EMEA', 'EMEA', 'APAC'], 'qty': [1200, 34, 5000]})
        grid = self.page.grid(frame, name='trades_exported', rows=['region'], measures={'qty': 'sum'})
        grid.configure({'columns': {'qty': {'heatmap': {'from': '#ffffff', 'to': '#ff0000'}}}})
        # CSV: the raw values, each group's sum on its own row
        rows = list(csv.reader(io.StringIO(grid.export('csv'))))
        header, body = rows[0], rows[1:]
        self.assertIn('qty', header)
        region, qty = header.index('region'), header.index('qty')
        self.assertEqual({r[region]: r[qty] for r in body}, {'EMEA': '1234', 'APAC': '5000'}, rows)
        # formatted where a person reads it: the separator, as the screen draws it
        text, html = grid.export('text'), grid.export('html')
        self.assertIn('1,234', text)
        self.assertIn('5,000', html)
        # the heatmap's colours, as the grid draws them: the top of the scale its `to`
        self.assertRegex(html.lower(), r'background(-color)?:\s*(#ff0000|rgb\(255,\s*0,\s*0\))')
        self.assertTrue(grid.export('excel').startswith(b'PK'), 'a workbook is a zip')
        self.assertTrue(grid.export('pdf').startswith(b'%PDF'))
        with self.assertRaisesRegex(ValueError, "'csv', 'excel', 'html', 'text' or 'pdf'"):
            grid.export('docx')
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(grid.export('excel', to=Path(folder) / 'trades.xlsx').read_bytes()[:2], b'PK')
            # the file's bytes as the export wrote them, on every system (CSV's own CRLF kept, never doubled)
            written = grid.export('csv', to=Path(folder) / 'trades.csv')
            self.assertEqual(written.read_bytes(), grid.export('csv').encode('utf-8'))

    def test_a_filter_and_a_calculated_column_reach_the_relation_and_its_rows(self):
        grid = self.page.grid(TRADES, name='trades_shaped')
        grid.filter(('region', 'equal', 'EMEA'))
        grid.calculate('big', '$x.notional->toOne() > 50')
        grid.group('desk')
        relation = grid.relation
        # the finest level the grid shows: its rows filtered and extended, grouped by its innermost group, as Create a
        # Class makes a class of them
        for part in ("filter(x|$x.region == 'EMEA')", 'big:x|$x.notional->toOne() > 50', 'groupBy(~[desk]'):
            self.assertIn(part, relation)
        rows = grid.to_pandas().set_index('desk').sort_index()
        emea = TRADES[TRADES.region == 'EMEA'].assign(big=lambda d: d.notional > 50)
        one = lambda d: d.iloc[0] if d.nunique() == 1 else None  # noqa: E731 -- uniqueValueOnly
        # a Boolean with nulls stays a boolean (pandas' nullable type, frames.to_pandas): its null pandas' NA
        expected = emea.groupby('desk').agg(region=('region', one), notional=('notional', 'sum'), qty=('qty', 'sum'),
                                            big=('big', one)).sort_index().astype({'big': 'boolean'})
        self.assertEqual(sorted(rows.columns), sorted(expected.columns))
        pd.testing.assert_frame_equal(rows[list(expected.columns)], expected, check_dtype=False)

    def test_a_flat_grid_shows_its_rows_and_its_relation_is_them(self):
        grid = self.page.grid(TRADES, name='trades_flat')
        view = grid.view()
        self.assertEqual([c['name'] for c in view.columns], list(TRADES.columns))
        self.assertEqual(sorted(column(view, 'qty')), sorted(TRADES.qty))
        self.assertNotIn('groupBy', grid.relation)
        self.assertEqual(view.to_pandas().shape, TRADES.shape)
        pd.testing.assert_frame_equal(grid.to_pandas().sort_values('qty').reset_index(drop=True), TRADES, check_dtype=False)

    def test_its_rows_and_its_view_keep_an_integer_with_nulls_an_integer(self):
        # typed by the plan (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15.3a): an Integer stays an integer, nulls and
        # all, in what the grid shows and in its rows -- never 1.0
        frame = pd.DataFrame({'desk': ['FX', 'EQ'], 'qty': pd.array([1, None], dtype='Int64')})
        grid = self.page.grid(frame, name='trades_nulls')
        for out in (grid.view().to_pandas(), grid.to_pandas()):
            self.assertEqual(str(out.qty.dtype), 'Int64')
            self.assertEqual(sorted(out.qty.dropna().tolist()), [1])
            self.assertEqual(int(out.qty.isna().sum()), 1)

    def test_an_integer_past_64_bits_stays_exact_in_its_view(self):
        frame = pd.DataFrame({'k': ['a', 'a'], 'n': [2 ** 63 - 1, 2 ** 63 - 1]})
        out = self.page.grid(frame, name='trades_wide', rows=['k'], measures={'n': 'sum'}).view().to_pandas()
        self.assertEqual(out.n.tolist()[-1], 2 * (2 ** 63 - 1))

    def test_its_rows_read_a_live_frame_once(self):
        # one query, one read of the frame as it is then: a function frame called once (Frames.serving holds it still)
        calls = []
        grid = self.page.grid(lambda: calls.append(1) or TRADES, name='trades_called')
        before = len(calls)
        grid.to_pandas()
        self.assertEqual(len(calls) - before, 1)

    def test_a_grid_changed_in_python_is_viewed_as_it_is_now(self):
        grid = self.page.grid(TRADES, name='trades_changing', rows=['desk'], measures={'qty': 'sum'})
        self.assertEqual(groups(grid.view(), 'qty'), TRADES.groupby('desk').qty.sum().to_dict())
        grid.filter(('qty', 'greaterThan', 2))
        self.assertEqual(groups(grid.view(), 'qty'), TRADES[TRADES.qty > 2].groupby('desk').qty.sum().to_dict())

    def test_its_configuration_is_folded_in_as_on_screen(self):
        # the row cap, a hidden column and a column's aggregate: what the tab folds into its query and lays out
        flat = self.page.grid(TRADES, name='trades_capped')
        flat.configure({'maxRows': 2, 'columns': {'desk': {'hidden': True}}})
        view = flat.view()
        self.assertEqual(len(view.rows), 2, 'the row cap, as on screen')
        self.assertEqual(view.total_rows, len(TRADES), 'and how many rows there are in all')
        self.assertNotIn('desk', [c['name'] for c in view.columns], 'a hidden column, not shown')
        grouped = self.page.grid(TRADES, name='trades_aggregated', rows=['desk'])
        grouped.configure({'columns': {'qty': {'aggregateFn': 'max'}}})
        self.assertEqual(groups(grouped.view(), 'qty'), TRADES.groupby('desk').qty.max().to_dict())
        self.assertIn('max()', grouped.relation)
        rows = grouped.to_pandas().set_index('desk').qty.to_dict()
        self.assertEqual(rows, TRADES.groupby('desk').qty.max().to_dict())

    def test_a_grid_read_while_another_thread_registers_frames_does_not_wait_on_it(self):
        # the session's lock and the frames' lock, taken in one order by both: a grid read beside frames registered
        import threading
        grid = self.page.grid(TRADES, name='trades_threads', rows=['desk'], measures={'qty': 'sum'})
        failures: list[BaseException] = []

        def register() -> None:
            try:
                for i in range(20):
                    self.page.grid(TRADES, name=f'trades_beside_{i}')
            except BaseException as e:  # noqa: BLE001 -- reported below
                failures.append(e)
        other = threading.Thread(target=register)
        other.start()
        for _ in range(20):
            self.assertEqual(groups(grid.view(), 'qty'), TRADES.groupby('desk').qty.sum().to_dict())
        other.join(timeout=60)
        self.assertFalse(other.is_alive(), 'the registering thread finished')
        self.assertEqual(failures, [])

    def test_called_inside_a_running_event_loop_as_a_notebook_cell_is(self):
        # Jupyter runs a cell inside its own asyncio loop: the headless engine runs on a loop of its own, on its own
        # thread, so a synchronous call from inside a running loop answers, and never touches that loop
        import asyncio
        grid = self.page.grid(TRADES, name='trades_in_a_loop', rows=['desk'], measures={'qty': 'sum'})

        async def cell():
            running = asyncio.get_running_loop()
            view = grid.view()
            self.assertIs(asyncio.get_running_loop(), running)
            return groups(view, 'qty')
        self.assertEqual(asyncio.run(cell()), TRADES.groupby('desk').qty.sum().to_dict())

    def test_numbers_cross_exactly(self):
        exact = pd.DataFrame({'id': [9007199254740993, 1], 'x': [float('nan'), float('inf')]})
        view = self.page.grid(exact, name='exact_numbers').view()
        ids = column(view, 'id')
        self.assertEqual(sorted(ids), [1, 9007199254740993], 'an integer past 2^53, exact')
        # pandas' NaN is a missing value as the frame is registered (Arrow reads it so), infinity a float
        self.assertEqual(sorted(column(view, 'x'), key=str), [None, float('inf')])



class Files(unittest.TestCase):
    """A page's file read into the frames' database by DataCube's own code (the engine's file.json;
    docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md, step 2b), as the tab reads one into DuckDB-WASM: a table of its own,
    the model DataCube's code wrote for it served beside the frames', and let go of again."""

    def setUp(self):
        # the session's frames and its DataCube code, as a shown page's engine has them (with no site to serve here)
        self.frames = datacube._session.frames
        self.engine = Engine(self.frames, headless=datacube._session.headless)

    def send(self, path, body):
        return self.engine.answer('POST', path, '', json.dumps(body))

    def upload(self, data, size=4 * 1024 * 1024):
        """A file's bytes sent as the page sends them (file-part.json): a part at a time, each saying where it starts."""
        upload, at = None, 0
        for start in range(0, max(len(data), 1), size):
            part = data[start:start + size]
            answer = self.send('/file-part.json', {**({'upload': upload} if upload else {}), 'at': at,
                                                   'bytes': base64.b64encode(part).decode()})
            self.assertEqual(answer.status, 200, answer.body[:300])
            upload, at = json.loads(answer.body)['upload'], at + len(part)
            self.assertEqual(json.loads(answer.body)['size'], at)
        return upload

    def file(self, name, text, table, size=4 * 1024 * 1024):
        return self.send('/file.json', {'upload': self.upload(text.encode(), size), 'name': name, 'table': table})

    def read(self, name, text, table):
        """A file read: what arrived, as upload.ts says it; let go of when the test ends."""
        answer = self.file(name, text, table)
        self.assertEqual(answer.status, 200, answer.body[:300])
        said = json.loads(answer.body)
        self.addCleanup(self.send, '/forget.json', {'name': name, 'table': said['table']})
        return said

    def tables(self, name):
        return self.frames.connection.execute(
            'SELECT count(*) FROM duckdb_tables() WHERE lower(table_name) = lower(?)', [name]).fetchone()[0]

    def test_the_engine_says_it_takes_files(self):
        answer = self.engine.answer('GET', '/services.json', '', None)
        self.assertTrue(json.loads(answer.body)['files'])

    def test_a_csv_is_read_into_its_table_its_model_served_then_let_go_of(self):
        answer = self.file('books.csv', 'title,pages\nDune,412\nEmma,474\n', 'files_books')
        self.assertEqual(answer.status, 200, answer.body[:300])
        said = json.loads(answer.body)
        self.assertEqual((said['table'], said['rowCount'], said['fileName']), ('files_books', 2, 'books.csv'))
        rows = self.frames.connection.execute('SELECT title, pages FROM files_books ORDER BY pages').fetchall()
        self.assertEqual(rows, [('Dune', 412), ('Emma', 474)])
        with self.frames.serving() as models:
            self.assertEqual(models['files_books'], said['model'])
        # its last grid gone: its table dropped by DataCube's code, its model served no more
        self.assertEqual(self.send('/forget.json', {'name': 'books.csv', 'table': 'files_books'}).status, 204)
        with self.frames.serving() as models:
            self.assertNotIn('files_books', models)
        self.assertEqual(self.tables('files_books'), 0)

    def test_two_pages_asking_one_name_each_get_a_table_of_their_own(self):
        first = self.read('data.csv', 'a\n1\n', 'files_data')
        second = self.read('data.csv', 'a\n2\n3\n', 'files_data')
        self.assertEqual((first['table'], second['table']), ('files_data', 'files_data_2'))
        # the first page letting its file go drops its own table, never the other page's
        self.assertEqual(self.send('/forget.json', {'name': 'data.csv', 'table': 'files_data'}).status, 204)
        self.assertEqual(self.frames.connection.execute('SELECT count(*) FROM files_data_2').fetchone()[0], 2)
        with self.frames.serving() as models:
            self.assertIn('files_data_2', models)

    def test_a_files_table_named_False_is_read_by_a_query(self):
        # Pure's literals are `true` and `false` as spelled: `False` names a file's table (frames.reading_file), and
        # SQL's FALSE, a keyword in any case, is quoted where a query planned over the file's model reads it
        said = self.read('f.csv', 'a\n1\n2\n', 'False')
        self.assertEqual(said['table'], 'False')
        query = json.dumps({'_type': 'lambda', 'body': [said['source']], 'parameters': []})
        answer = self.send('/plan-and-run.json', {'lambda': query, 'model': said['model'], 'runtime': said['runtime']})
        self.assertEqual(answer.status, 200, answer.body[:300])
        self.assertEqual(sorted(pyarrow.ipc.open_stream(answer.body).read_all()['a'].to_pylist()), [1, 2])

    def test_a_file_comes_a_part_at_a_time_however_large(self):
        # a CSV larger than any one call may be (16 MiB), in the page's 4 MiB parts; and one in parts of three bytes,
        # a character split between two of them
        rows = 700_000
        big = 'id,name\n' + ''.join(f'{i},row number {i:08d}\n' for i in range(rows))
        self.assertGreater(len(big), 16 * 1024 * 1024)
        said = self.read('big.csv', big, 'files_big')
        self.assertEqual(said['rowCount'], rows)
        small = self.send('/file.json', {'upload': self.upload('w\nJosé\nZoë\n'.encode(), 3), 'name': 'w.csv',
                                         'table': 'files_parts'})
        self.assertEqual(small.status, 200, small.body[:300])
        self.addCleanup(self.send, '/forget.json', {'name': 'w.csv', 'table': 'files_parts'})
        self.assertEqual(self.frames.connection.execute('SELECT w FROM files_parts ORDER BY w').fetchall(),
                         [('José',), ('Zoë',)])

    def test_a_part_out_of_order_or_of_no_upload_is_refused_never_written(self):
        first = json.loads(self.send('/file-part.json', {'at': 0, 'bytes': base64.b64encode(b'a\n1\n').decode()}).body)
        upload = first['upload']
        # sent again, or skipping ahead: refused, the file as it was
        for at in (0, 2, 9):
            self.assertEqual(self.send('/file-part.json', {'upload': upload, 'at': at, 'bytes': 'Mgo='}).status, 409, at)
        self.assertEqual(json.loads(self.send('/file-part.json', {'upload': upload, 'at': 4, 'bytes': 'Mgo='}).body)['size'], 6)
        self.assertEqual(self.send('/file-part.json', {'upload': 'nope', 'at': 0, 'bytes': 'Mgo='}).status, 404)
        # an upload begins at its first byte; a part's bytes are base64; its position a count of bytes
        for body in ({'at': 3, 'bytes': 'Mgo='}, {'at': 0, 'bytes': '%%%'}, {'at': -1, 'bytes': ''}, {'at': True, 'bytes': ''},
                     {'at': 0}):
            self.assertEqual(self.send('/file-part.json', body).status, 400, body)
        # read once: its parts are gone, and the upload with them
        answer = self.send('/file.json', {'upload': upload, 'name': 'p.csv', 'table': 'files_once'})
        self.assertEqual(answer.status, 200, answer.body[:300])
        self.addCleanup(self.send, '/forget.json', {'name': 'p.csv', 'table': 'files_once'})
        self.assertEqual(self.frames.connection.execute('SELECT a FROM files_once ORDER BY a').fetchall(), [(1,), (2,)])
        self.assertEqual(self.send('/file.json', {'upload': upload, 'name': 'p.csv', 'table': 'files_again'}).status, 404)
        self.assertEqual(self.send('/file-part.json', {'upload': upload, 'at': 6, 'bytes': ''}).status, 404)

    def test_a_name_held_by_a_frame_or_another_table_is_not_taken(self):
        self.frames.register('files_frame', pd.DataFrame({'a': [1]}))
        self.addCleanup(self.frames.unregister, 'files_frame')
        self.frames.connection.execute('CREATE TABLE files_mine AS SELECT 1 AS a')
        self.addCleanup(self.frames.connection.execute, 'DROP TABLE IF EXISTS files_mine')
        self.frames.connection.execute('CREATE TEMP TABLE files_temp AS SELECT 1 AS a')
        self.addCleanup(self.frames.connection.execute, 'DROP TABLE IF EXISTS temp.files_temp')
        for asked in ('files_frame', 'files_mine', 'files_temp'):
            with self.subTest(asked=asked):
                self.assertEqual(self.read('x.csv', 'a\n1\n', asked)['table'], f'{asked}_2')
        # and what is not a name, or is legend-lite's own, is refused
        for asked in ('not a name', 'true', '__legend_lite_files_frame'):
            with self.subTest(asked=asked):
                self.assertEqual(self.file('x.csv', 'a\n1\n', asked).status, 400)
        # a page drops only a table a file of its was read into
        self.assertEqual(self.send('/forget.json', {'name': 'x.csv', 'table': 'files_mine'}).status, 404)
        self.assertEqual(self.tables('files_mine'), 1)
        self.assertEqual(self.frames['files_frame'].name, 'files_frame')

    def test_a_frame_cannot_take_a_files_name_nor_does_show_choose_one(self):
        self.read('frame.csv', 'a\n1\n', 'files_shown')
        with self.assertRaisesRegex(ValueError, "a page's file was read into a table named 'files_shown'"):
            self.frames.register('files_shown', pd.DataFrame({'a': [1]}))
        self.assertTrue(self.frames.taken('files_shown'))

    def test_a_header_a_person_wrote_is_read_into_a_model_the_compiler_takes(self):
        header = '"a ""quoted"" one",b}c,"line\nbreak",plain'
        said = self.read('odd.csv', f'{header}\n1,2,3,4\n', 'files_odd')
        names = [r[0] for r in self.frames.connection.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = 'files_odd' ORDER BY column_index").fetchall()]
        self.assertEqual(names, ['a "quoted" one', 'b}c', 'line\nbreak', 'plain'])
        # the model the writer wrote for those names compiles, and types the source with them (it is served, and only
        # served models are run)
        columns = ll.relation_type(said['model'], {'_type': 'lambda', 'parameters': [], 'body': [said['source']]})
        self.assertEqual([c.name for c in columns], names)

    def test_a_drop_that_fails_leaves_the_table_served_to_be_let_go_of_again(self):
        self.read('keep.csv', 'a\n1\n', 'files_keep')
        real = datacube._session.headless()

        class Refusing:
            def call(self, function, *args):
                if function == 'forget':
                    raise RuntimeError('the drop failed')
                return real.call(function, *args)
        refusing = Engine(self.frames, headless=Refusing)
        answer = refusing.answer('POST', '/forget.json', '', json.dumps({'name': 'keep.csv', 'table': 'files_keep'}))
        self.assertEqual(answer.status, 422, answer.body[:300])
        with self.frames.serving() as models:
            self.assertIn('files_keep', models)
        self.assertEqual(self.tables('files_keep'), 1)

    def test_a_file_duckdb_cannot_read_leaves_nothing_under_its_name(self):
        answer = self.file('bad.parquet', 'not a parquet file', 'files_bad')
        self.assertEqual(answer.status, 422, answer.body[:300])
        self.assertEqual(self.tables('files_bad'), 0)
        with self.frames.serving() as models:
            self.assertNotIn('files_bad', models)

    def test_without_its_bundle_reading_a_file_says_so_never_hides_it(self):
        import os
        from unittest import mock
        missing = os.path.join(os.sep, 'no', 'such', 'bundle.js')
        engine = Engine(self.frames, headless=lambda: Headless(bundle()))
        with mock.patch.dict(os.environ, {'LEGEND_LITE_DATACUBE': missing}):
            # given DataCube's code, the engine offers files; the bundle missing is said where a file is read
            self.assertTrue(json.loads(engine.answer('GET', '/services.json', '', None).body)['files'])
            answer = engine.answer('POST', '/file-part.json', '', json.dumps({'at': 0, 'bytes': 'YQoxCg=='}))
            self.assertEqual(answer.status, 500)
            self.assertIn(f"DataCube's code is not at {missing}", answer.body.decode('utf-8'))



class Remote(unittest.TestCase):
    """A page's remote file read through a view in the frames' database (the engine's remote.json; step 2b): by
    DataCube's own mount, through the httpfs extension the wheel carries, read here from a server on this machine --
    nothing is downloaded. A URL that is not a remote one is refused before anything runs."""

    @classmethod
    def setUpClass(cls):
        import functools
        import http.server
        import tempfile
        import threading
        import pyarrow as pa
        import pyarrow.parquet as pq

        cls.folder = tempfile.TemporaryDirectory()
        pq.write_table(pa.table({'desk': ['FX', 'EQ', 'FX'], 'qty': [1, 2, 3]}), f'{cls.folder.name}/trades.parquet')
        Path(cls.folder.name, 'desks.csv').write_text('desk,head\nFX,Ana\nEQ,Ben\n')
        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass
        handler = functools.partial(Quiet, directory=cls.folder.name)
        cls.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f'http://127.0.0.1:{cls.server.server_address[1]}'
        # Iceberg tables (G5): one whose every file is served, and three naming a file on this machine where its data
        # file, its manifest and its manifest list would be -- none of which exist, so a read of one would fail (and a
        # manifest that cannot be read ends the process inside DuckDB 1.5.5's iceberg_scan): each is refused first
        import iceberg_table
        served = Path(cls.folder.name)
        # named as a page names one: its table's URL marked #iceberg (remote.ts inferFormat)
        cls.iceberg = iceberg_table.write(served, 'trades_iceberg', cls.base, [[('FX', 1), ('EQ', 2)], [('FX', 3)]]) + '#iceberg'
        here = cls.iceberg_here = served / 'not-served'
        cls.iceberg_local = {
            what: iceberg_table.write(served, f'local_{what}', cls.base, [[('FX', 1)]], **{f'{what}_url': str(here / what)})
            + '#iceberg' for what in ('data', 'manifest', 'list')}

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.folder.cleanup()

    def setUp(self):
        self.frames = datacube._session.frames
        self.engine = Engine(self.frames, headless=datacube._session.headless)

    def mount(self, url, name='remote'):
        return self.engine.answer('POST', '/remote.json', '', json.dumps({'url': url, 'name': name}))

    def test_the_engine_says_it_reads_remote_files(self):
        self.assertTrue(json.loads(self.engine.answer('GET', '/services.json', '', None).body)['remote'])

    def test_a_remote_parquet_and_csv_are_read_through_views_their_models_served(self):
        for url, name, count in ((f'{self.base}/trades.parquet', 'remote_trades', 3), (f'{self.base}/desks.csv', 'remote_desks', 2)):
            with self.subTest(url=url):
                answer = self.mount(url, name)
                self.assertEqual(answer.status, 200, answer.body[:300])
                model = json.loads(answer.body)['model']
                self.assertEqual(self.frames.connection.execute(f'SELECT count(*) FROM {name}').fetchone()[0], count)
                with self.frames.serving() as models:
                    self.assertEqual(models[name], model)
        # read with the extension the wheel carries, from its folder: none was downloaded
        httpfs = self.frames.connection.execute(
            "SELECT loaded, install_path FROM duckdb_extensions() WHERE extension_name = 'httpfs'").fetchone()
        self.assertTrue(httpfs[0])
        # as the system compares paths (Windows: whatever the case of the drive letter, either slash), links not
        # followed (the runfiles are links on macOS and Linux)
        carried = os.path.normcase(os.path.abspath(os.environ['LEGEND_LITE_DUCKDB_EXTENSIONS']))
        self.assertTrue(os.path.normcase(os.path.abspath(httpfs[1])).startswith(carried + os.sep), httpfs[1])

    def test_a_remote_iceberg_table_is_read_through_a_view_with_the_extensions_carried(self):
        answer = self.mount(self.iceberg, 'remote_iceberg')
        self.assertEqual(answer.status, 200, answer.body[-600:])
        rows = self.frames.connection.execute('SELECT desk, qty FROM remote_iceberg ORDER BY qty').fetchall()
        self.assertEqual(rows, [('FX', 1), ('EQ', 2), ('FX', 3)])
        with self.frames.serving() as models:
            self.assertEqual(models['remote_iceberg'], json.loads(answer.body)['model'])
        carried = os.path.normcase(os.path.abspath(os.environ['LEGEND_LITE_DUCKDB_EXTENSIONS']))
        for extension in ('iceberg', 'avro'):
            with self.subTest(extension=extension):
                loaded, path = self.frames.connection.execute(
                    'SELECT loaded, install_path FROM duckdb_extensions() WHERE extension_name = ?', [extension]).fetchone()
                self.assertTrue(loaded)
                self.assertTrue(os.path.normcase(os.path.abspath(path)).startswith(carried + os.sep), path)

    def test_an_iceberg_table_naming_a_file_on_this_machine_is_refused_before_any_view_reads_it(self):
        for what, url in self.iceberg_local.items():
            with self.subTest(names=what):
                answer = self.mount(url, f'iceberg_{what}')
                self.assertEqual(answer.status, 400, answer.body[:300])
                self.assertIn(b'every file it names is a remote one', answer.body)
                self.assertIn(repr(str(self.iceberg_here / what)), answer.body.decode('utf-8'))
                self.assertFalse(self.frames.taken(f'iceberg_{what}'))

    def test_a_url_that_is_not_remote_is_refused_before_anything_runs(self):
        local = Path(self.folder.name, 'desks.csv')
        port = self.base.rsplit(':', 1)[1]
        for url in (str(local), local.as_uri(), 'C:\\Users\\x\\secret.csv', 'ftp://127.0.0.1/x.csv', 'http://', '~/.ssh/id_rsa',
                    # what Python's own reading of a URL takes for http, which DuckDB reads from this machine instead
                    f'HTTP://127.0.0.1:{port}/desks.csv', f' http://127.0.0.1:{port}/desks.csv',
                    f'ht\ttp://127.0.0.1:{port}/desks.csv', f'http://127.0.0.1:{port}/desks.csv\n',
                    f'http://127.0.0.1:{port}/desks.csv\x7f', 'azure://account/c/x.parquet'):
            with self.subTest(url=url):
                answer = self.mount(url, 'remote_refused')
                self.assertEqual(answer.status, 400, answer.body[:300])
                self.assertIn(b'never from this machine', answer.body)
        self.assertFalse(self.frames.taken('remote_refused'))

    def test_a_duckdb_the_extensions_are_not_for_is_said_by_name(self):
        import tempfile
        from unittest import mock
        with tempfile.TemporaryDirectory() as other:
            (Path(other) / 'v0.0.1' / 'osx_arm64').mkdir(parents=True)
            with mock.patch.dict(os.environ, {'LEGEND_LITE_DUCKDB_EXTENSIONS': other}):
                answer = self.mount(f'{self.base}/trades.parquet', 'remote_version')
                self.assertEqual(answer.status, 404)
                self.assertIn(b'for duckdb 0.0.1', answer.body)

    def test_a_database_of_the_persons_own_is_left_as_they_keep_it(self):
        import duckdb
        own = duckdb.connect()
        engine = Engine(ll.Frames(connection=own), headless=datacube._session.headless)
        self.assertFalse(json.loads(engine.answer('GET', '/services.json', '', None).body)['remote'])
        self.assertEqual(engine.answer('POST', '/remote.json', '', json.dumps(
            {'url': f'{self.base}/trades.parquet', 'name': 'remote_own'})).status, 404)
        self.assertEqual(own.execute("SELECT current_setting('autoinstall_known_extensions')").fetchone()[0], True)

    def test_a_remote_view_is_not_let_go_of_as_a_files_table(self):
        self.assertEqual(self.mount(f'{self.base}/desks.csv', 'remote_kept').status, 200)
        answer = self.engine.answer('POST', '/forget.json', '', json.dumps({'name': 'desks.csv', 'table': 'remote_kept'}))
        self.assertEqual(answer.status, 404)
        self.assertEqual(self.frames.connection.execute('SELECT count(*) FROM remote_kept').fetchone()[0], 2)

    def test_without_the_extensions_the_engine_reads_no_remote_file(self):
        from unittest import mock
        with mock.patch.dict(os.environ, {'LEGEND_LITE_DUCKDB_EXTENSIONS': os.path.join(os.sep, 'no', 'such', 'folder')}):
            self.assertFalse(json.loads(self.engine.answer('GET', '/services.json', '', None).body)['remote'])
            self.assertEqual(self.mount(f'{self.base}/trades.parquet', 'remote_none').status, 404)


if __name__ == '__main__':
    unittest.main()
