"""Python's engine for a browser test (//datacube:python_engine_test, beside it): the cube corpus's rows shown as a
Live frame (show()), with DataCube's site, and two notebook cubes over it (DataCube widgets: the rows, and three of
them). It prints one JSON line -- the engine's address and token, the cube's link, the frame's model, runtime and
source, the rows (the page loads the same rows into the tab's DuckDB), and each widget's script and state -- then takes
the test's commands until its standard input closes: `update`, `columns`, `widget-update`, `page <json>` (a page of
its frames served, as `q3`), `page-again` (that page served again, its second sheet renamed), `desks-update` (the second
frame a row more), `py-page` (a page built with ll.Page and shown; answered `done py-page <its link as JSON>`),
`py-page-sheet` (a sheet added to it in Python), `py-page-group` and `py-page-regroup` (its first grid grouped by desk,
then by region, in Python), `py-page-rename` and `py-page-name-back` (it renamed in Python, and named back),
`py-page-snap`, `py-page-more` and `py-page-live` (its first grid snapped in Python, its frame a LATAM row more, the grid
live again), `py-page-copies` (answered `done py-page-copies <each grid's snapped, as the page says it, as JSON>`),
`show-snapped` (a frame shown with snap=True; answered `done show-snapped <its link as JSON>`), `copies-held`
(answered `done copies-held <the copies Python's engine holds, as JSON>`),
`widget-page-sheet` (a sheet added to the notebook's page),
`py-page-desks-wider` (the page's desks frame, read Live, a column more in place, where Python does not look) and
`py-page-desks-columns` (answered `done py-page-desks-columns <its desks grid's columns, as the page has them, as JSON>`),
`py-page-version` (answered `done py-page-version <the version its keeper has, as JSON>`),
`show-closing` (a frame of two rows shown, `closing`; answered `done show-closing <its link as JSON>`), `close-closing`
(that frame's cube closed in Python), `py-page-read` (answered `done py-page-read <its sheets' names, as
the page has them, as JSON>`), `py-documents` (answered `done py-documents <pages Python writes, each its JSON
text, as JSON>`: for DataCube's reader to read) -- each answered `done <command>` -- and `widget <name> <message>`, a message from a widget's page, as
a notebook's channel brings it. What a widget sends its page is printed as it goes: `sent <json>` (its buffers in
base64) and `trait <json>` (a property set). A second frame, `desks`, is served Live from the start, for the page.

    engine_fixture --site <DataCube's built site>
"""

import argparse
import base64
import contextlib
import functools
import http.server
import tempfile
import io
import json
import os
import re
import sys
import threading
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import legend_lite as ll
from legend_lite import datacube
from legend_lite.notebook import DataCube, PageCube

_said = threading.Lock()


def say(line: str) -> None:
    """One line of output, whole: widgets answer on threads of their own."""
    with _said:
        sys.stdout.write(line + '\n')
        sys.stdout.flush()


def channel(cube: DataCube | PageCube, name: str) -> None:
    """The widget's channel, as the test carries it: what it sends its page, printed, under the widget's ``name``."""
    cube.send = lambda content, buffers=None: say('sent ' + json.dumps({
        'widget': name, 'content': content, 'buffers': [base64.b64encode(bytes(b)).decode() for b in buffers or []]}))
    cube.observe(lambda change: say('trait ' + json.dumps({'widget': name, 'name': change['name'],
                                                           'value': change['new']})), names=['version', 'versions'])

# The corpus's rows (datacube/test/live-snap/page.ts's): unique on (book, year, qtr); NULLs in the measures and in a
# dimension.
ROWS = [
    ('EMEA', 'Rates', 'B1', 2023, 'Q1', 100.5, 1.25, 10), ('EMEA', 'Rates', 'B1', 2024, 'Q2', 200.25, -3.5, 20),
    ('EMEA', 'FX', 'B3', 2023, 'Q1', 50.0, 0.5, 5), ('EMEA', 'FX', 'B3', 2023, 'Q3', 75.0, 2.0, 6),
    ('AMER', 'Rates', 'B2', 2024, 'Q3', 300.75, 7.0, 30), ('AMER', 'FX', 'B4', 2023, 'Q4', None, None, 7),
    ('AMER', 'Credit', 'B5', 2022, 'Q1', 12.5, 0.25, None), ('APAC', 'Credit', 'B6', 2024, 'Q1', 75.0, 2.0, 3),
    ('APAC', 'Rates', 'B7', 2022, 'Q2', 10.0, 0.0, 1), (None, 'Rates', 'B8', 2024, 'Q4', 42.0, -1.0, 4),
]
SCHEMA = pa.schema([
    ('region', pa.string()), ('desk', pa.string()), ('book', pa.string()), ('year', pa.int32()),
    ('qtr', pa.string()), ('notional', pa.float64()), ('pnl', pa.float64()), ('qty', pa.int32()),
])


def python_documents(frame: pa.Table, written: dict, desks: dict) -> list[str]:
    """Pages as Python writes them, each its JSON text, for DataCube's own reader to read: built with every kind of
    edit Python makes -- filters of every kind of value, a calculated column with an exact decimal, charts beside and
    below their grid and one removed, a stack, a layout in the document's own words -- a frozen chart kept with its grid
    gone, and DataCube's own page (the test's) loaded, changed in Python and written again."""
    built = ll.Page('Built')
    grid = built.grid(frame, name='doc_trades', rows=['region'], measures={'notional': 'sum'}, sort=[('notional', 'desc')],
                      filter=[('notional', 'greaterThan', Decimal('12.30')), ('year', 'in', [2023, 2024]),
                              ('region', 'notEqual', 'APAC'), ('qtr', 'isNotEmpty')])
    grid.calculate('scaled', '$x.notional->toOne() * 1.50D')
    charts = [grid.chart(mark, x='region') for mark in ('bar', 'line', 'area', 'pie', 'scatter')]
    # a KPI: its number against a target, its trend by year
    grid.chart('kpi', x='year', options={'target': 500, 'better': 'higher'}, title='Notional')
    built.remove(charts[1])
    other = built.grid(lambda: desks['now'], name='doc_desks')
    built.stack(other, charts[4])
    laid = built.sheet('Laid out')
    laid.add(charts[2], charts[3])
    laid.layout_from({'kind': 'bands', 'fit': False, 'bands': [{'height': 1.2, 'node': {'split': 'column', 'parts': [
        {'node': {'tile': charts[2].id}, 'size': 0.4}, {'node': {'tile': charts[3].id}, 'size': 0.6}]}}]})
    frozen = ll.Page('Frozen')
    held = frozen.grid(frame, name='doc_frozen', measures={'qty': 'sum'})
    held.chart('bar', x='desk', frozen=True)
    frozen.grid(lambda: desks['now'], name='doc_desks_2')
    frozen.remove(held)
    loaded = ll.Page.load(written, frames={'trades': frame, 'desks': lambda: desks['now']})
    first = loaded.grids[0]
    first.group('desk')
    loaded.stack(first, first.chart('line', x='desk'))
    return [page.to_json() for page in (built, frozen, loaded)]


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--site', required=True)
    os.environ['LEGEND_LITE_SITE'] = arguments.parse_args().site
    frame = pa.Table.from_pylist([dict(zip(SCHEMA.names, row)) for row in ROWS], schema=SCHEMA)
    # show(), as a person calls it -- but the browser is the test's (it opens the link itself)
    cube = ll.show(frame, name='trades', browser=False)
    table = datacube._session.frames['trades']
    web = datacube._session.web()
    # a second frame for a page of two (read Live: an update is the next query's)
    desks = {'now': pa.table({'desk': ['Rates', 'FX', 'Credit'], 'head': ['Ana', 'Ben', 'Cy']})}
    datacube._session.register(lambda: desks['now'], 'desks')
    served_page: dict = {}

    def served_frames(doc: dict) -> list[str]:
        # the frames the test's page reads: each of its cubes over one (the test sends no join)
        return sorted({c['cube']['source']['name'] for c in doc.get('cubes', [])})

    def serve_whole(doc: dict) -> None:
        # the test's page written whole into the session's keeper and served, as no Python page writes one (a page
        # replaced whole, by no command): what a tab open on it follows by reading it whole, as it follows every move
        keeper = web.engine.keeper
        reads = {'frames': served_frames(doc), 'copies': [], 'ids': []}
        kept = keeper.current('q3')
        if kept is None:
            keeper.keep('q3', doc, reads, [])
        else:
            keeper.write('q3', kept[0], doc, reads, [])
        keeper.serve('q3', True)
    # a remote file a page reads from Python (step 2b): a Parquet file served by a server on this machine, read by
    # Python's DuckDB through the httpfs extension the wheel carries -- nothing leaves the machine
    remote = tempfile.TemporaryDirectory()
    import pyarrow.parquet as pq
    pq.write_table(pa.table({'book': ['Dune', 'Emma', 'Ulysses'], 'pages': [412, 474, 730]}), f'{remote.name}/books.parquet')

    class Quiet(http.server.SimpleHTTPRequestHandler):
        # read by Python's DuckDB and by the tab's DuckDB-WASM (L12's remote-file proof, page.ts): a page at the engine's
        # origin asks across origins, and DuckDB reads a Parquet file by ranges of it
        def log_message(self, *_):
            pass

        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Headers', 'Range')
            self.send_header('Access-Control-Expose-Headers', 'Content-Range, Content-Length, Accept-Ranges')
            self.send_header('Accept-Ranges', 'bytes')
            super().end_headers()

        def do_OPTIONS(self):
            self.send_response(204)
            self.end_headers()

        def do_GET(self):
            asked = re.fullmatch(r'bytes=(\d+)-(\d*)', self.headers.get('Range') or '')
            path = self.translate_path(self.path)
            if asked is None or not os.path.isfile(path):
                return super().do_GET()
            with open(path, 'rb') as f:
                body = f.read()
            start = int(asked[1])
            end = min(int(asked[2]) if asked[2] else len(body) - 1, len(body) - 1)
            if start >= len(body) or end < start:
                # a range the file has no byte of
                self.send_response(416)
                self.send_header('Content-Range', f'bytes */{len(body)}')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            self.send_response(206)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Range', f'bytes {start}-{end}/{len(body)}')
            self.send_header('Content-Length', str(end - start + 1))
            self.end_headers()
            self.wfile.write(body[start:end + 1])
    files = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=remote.name))
    threading.Thread(target=files.serve_forever, daemon=True).start()
    # and an Iceberg table the same server serves (G5), read through the iceberg extension the wheel carries
    import iceberg_table
    shelves = iceberg_table.write(Path(remote.name), 'shelves', f'http://127.0.0.1:{files.server_address[1]}',
                                  [[('Atlas', 5), ('Borealis', 7)], [('Cygnus', 9)]])
    # two notebook cubes, as ll.DataCube(df) makes them: the page shows both and fetches DataCube's module once
    widgets = {'nb': DataCube(frame, name='nb'), 'nb2': DataCube(frame.slice(0, 3), name='nb2')}
    # and a notebook's PAGE (ll.Page, shown under a cell): a grid grouped by region, and its chart -- whose code (and
    # ECharts) the page fetches over the channel when it first draws (the widget's lazy chunks)
    notebook_page = ll.Page('Notebook page')
    nb_grid = notebook_page.grid(frame, name='nbpage', rows=['region'], measures={'notional': 'sum'})
    nb_grid.chart('bar')
    # and one of batch 3's kinds: its drawing and the rarer ECharts charts are chunks importing chunks, through the
    # loader's hook, over the channel
    nb_grid.chart('funnel', x='region')
    widgets['nbp'] = PageCube(notebook_page)
    for name, widget in widgets.items():
        channel(widget, name)
    say(json.dumps({
        'url': web.url,
        'authorization': web.authorization,
        'link': cube.url,
        'remote': f'http://127.0.0.1:{files.server_address[1]}/books.parquet',
        'iceberg': f'{shelves}#iceberg',
        'model': table.model,
        'runtime': table.runtime,
        'source': table.source,
        'columns': SCHEMA.names,
        'rows': [list(row) for row in ROWS],
        # each a page's widget: a frame's cube too, a page of one grid over it (P12)
        'widgets': {name: {'esm': w._esm, 'state': {'_module': w._module, 'height': w.height, 'page_key': w.page_key,
                                                    'versions': w.versions}} for name, w in widgets.items()},
    }))
    # the test's commands, one a line: `update` -- one row more; `columns` -- a column more (a new model);
    # `widget-update` -- the first widget's frame a row more; `widget <name> <message>` -- a widget page's message.
    # Closed whatever happens, so the process ends with its input
    try:
        for line in sys.stdin:
            command = line.strip()
            if command.startswith('widget '):
                _, name, message = command.split(' ', 2)
                widgets[name]._received(widgets[name], json.loads(message), [])
                continue
            if command == 'update':
                cube.update(pa.concat_tables([frame, frame.slice(0, 1)]))
            elif command == 'columns':
                cube.update(frame.append_column('trader', pa.array([f't{i}' for i in range(frame.num_rows)])))
            elif command == 'widget-update':
                widgets['nb'].update(pa.concat_tables([frame, frame.slice(0, 1)]))
            elif command == 'widget-page-sheet':
                notebook_page.sheet('Added in Python')
            elif command.startswith('page '):
                served_page.update(json.loads(command[len('page '):]))
                serve_whole(served_page)
                command = 'page'
            elif command == 'page-again':
                served_page['sheets'][1]['name'] = 'Desks (v2)'
                serve_whole(served_page)
            elif command == 'py-page':
                # a page built in Python (ll.Page), as a person builds one, and shown (the test opens the tab)
                pypage = ll.Page('Built in Python')
                pygrid = pypage.grid(frame, name='pytrades', rows=['region'], measures={'notional': 'sum'})
                pygrid.chart('bar', x='region', y=[('notional', 'sum')], title='By region')
                # its desks grid reads a frame of its own, Live: changed in place by py-page-desks-wider alone
                pydesks = {'now': desks['now']}
                pypage.sheet('Desks').add(pypage.grid(lambda: pydesks['now'], name='pydesks'))
                # (show() says its link to a person, on standard output: here the test's line is the answer)
                with contextlib.redirect_stdout(io.StringIO()):
                    shown_page = ll.show(pypage, browser=False)
                command = f'py-page {json.dumps(shown_page.url)}'
            elif command == 'py-page-sheet':
                pypage.sheet('Added in Python')
            elif command == 'py-page-group':
                pygrid.group('desk')
            elif command == 'py-page-regroup':
                pygrid.group('region')
            elif command == 'py-page-rename':
                pypage.name = 'Renamed in Python'
            elif command == 'py-page-name-back':
                pypage.name = 'Built in Python'
            elif command == 'show-closing':
                with contextlib.redirect_stdout(io.StringIO()):
                    closing = ll.show(frame.slice(0, 2), name='closing', browser=False)
                command = f'show-closing {json.dumps(closing.url)}'
            elif command == 'close-closing':
                closing.close()
            elif command == 'py-documents':
                command = f'py-documents {json.dumps(python_documents(frame, served_page, desks))}'
            elif command == 'py-page-desks-wider':
                # (P11) a column more, in place: nothing in Python looks at the frame, so the page in Python holds the
                # cube as it was until the open page, opened again, tells it the cube as it opens now
                pydesks['now'] = pydesks['now'].append_column('floor', pa.array(
                    [f'f{i}' for i in range(pydesks['now'].num_rows)]))
            elif command == 'py-page-desks-columns':
                held = next(c['cube'] for c in pypage.to_dict()['cubes'] if c['cube']['source']['name'] == 'pydesks')
                command = f"py-page-desks-columns {json.dumps([c['name'] for c in held['source']['columns']])}"
            elif command == 'py-page-hash':
                # the keeper's hash of the page (W17): what every tab open on it must hash to
                command = f'py-page-hash {json.dumps(datacube._session.keeper.hash_of(pypage._key))}'
            elif command == 'py-page-version':
                command = f'py-page-version {json.dumps(datacube._session.keeper.current(pypage._key)[0])}'
            elif command == 'py-page-read':
                # the page as it is open now: one copy, a write from the open page Python's at once
                command = f'py-page-read {json.dumps([s.name for s in pypage.sheets])}'
            elif command == 'py-page-join':
                # a join of two of its grids' frames (page.join, §18.3), on its first sheet: the open page opens it
                # through Python, which has its compiler and its frames (the page has neither of its own here)
                mine = pypage.grid(frame, name='pytrades')
                theirs = next(g for g in pypage.grids if g.frames == ['pydesks'])
                pypage.join(mine, theirs, {'form': 'join', 'kind': 'left', 'prefix': 'd_', 'keep': None,
                                           'on': {'rows': [{'left': 'desk', 'op': '==', 'right': 'desk'}]}},
                            title='Trades with their desks', sheet=pypage.sheets[0])
            elif command == 'py-page-snap':
                # its first grid's rows read once, in Python (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): the page's
                # snap command, which the open page follows by reading Python's copy
                pygrid.snap()
            elif command == 'py-page-more':
                # its frame a row more, of a region it had none of: a live grid over it shows LATAM, a snapped one not
                pygrid.update(pa.concat_tables([frame, pa.Table.from_pylist([dict(zip(SCHEMA.names, (
                    'LATAM', 'Rates', 'B9', 2024, 'Q2', 1.0, 0.5, 2)))], schema=SCHEMA)]))
            elif command == 'py-page-live':
                pygrid.live()
            elif command == 'py-page-copies':
                # the copies its grids read, the open page's writes among them (a snap made there)
                command = f'py-page-copies {json.dumps({g.id: g.snapped for g in pypage.grids})}'
            elif command == 'show-snapped':
                # ll.show(df, snap=True): its page's grid read once, by its Snap, in Python (P12)
                with contextlib.redirect_stdout(io.StringIO()):
                    once = ll.show(frame.slice(0, 4), name='snapped', browser=False, snap=True)
                command = f'show-snapped {json.dumps(once.url)}'
            elif command == 'copies-held':
                # the snaps' copies Python's engine holds, by table
                command = f'copies-held {json.dumps(sorted(datacube._session.engine()._copies))}'
            elif command == 'py-page-save':
                # saved into this user's saved pages, as DataCube's Save keeps a page (B6): Open… lists it
                command = f'py-page-save {json.dumps(pypage.save())}'
            elif command == 'desks-update':
                desks['now'] = pa.table({'desk': ['Rates', 'FX', 'Credit', 'Equity'], 'head': ['Ana', 'Ben', 'Cy', 'Di']})
                web.engine.changed('desks')
            say(f'done {command}')
    finally:
        for widget in widgets.values():
            widget.close()
        cube.close()
        files.shutdown()
        remote.cleanup()


if __name__ == '__main__':
    main()
