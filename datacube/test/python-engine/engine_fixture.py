"""Python's engine for a browser test (//datacube:python_engine_test, beside it): the cube corpus's rows shown as a
Live frame (show()), with DataCube's site, and two notebook cubes over it (DataCube widgets: the rows, and three of
them). It prints one JSON line -- the engine's address and token, the cube's link, the frame's model, runtime and
source, the rows (the page loads the same rows into the tab's DuckDB), and each widget's script and state -- then takes
the test's commands until its standard input closes: `update`, `columns`, `widget-update`, `page <json>` (a page of
its frames served, as `q3`), `page-again` (that page served again, its second sheet renamed), `desks-update` (the second
frame a row more), `py-page` (a page built with ll.Page and shown; answered `done py-page <its link as JSON>`),
`py-page-sheet` (a sheet added to it in Python), `widget-page-sheet` (a sheet added to the notebook's page), `py-page-read` (answered `done py-page-read <its sheets' names, as
page.read() says them, as JSON>`), `py-documents` (answered `done py-documents <pages Python writes, each its JSON
text, as JSON>`: for DataCube's reader to read) -- each answered `done <command>` -- and `widget <name> <message>`, a message from a widget's page, as
a notebook's channel brings it. What a widget sends its page is printed as it goes: `sent <json>` (its buffers in
base64) and `trait <json>` (a property set). A second frame, `desks`, is served Live from the start, for the page.

    engine_fixture --site <DataCube's built site>
"""

import argparse
import base64
import contextlib
import io
import json
import os
import sys
import threading
from decimal import Decimal

import pyarrow as pa

import legend_lite as ll
from legend_lite import datacube
from legend_lite.frames import LIVE
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
    datacube._session.register(lambda: desks['now'], 'desks', LIVE)
    served_page: dict = {}
    # two notebook cubes, as ll.DataCube(df) makes them: the page shows both and fetches DataCube's module once
    widgets = {'nb': DataCube(frame, name='nb'), 'nb2': DataCube(frame.slice(0, 3), name='nb2')}
    # and a notebook's PAGE (ll.Page, shown under a cell): a grid grouped by region
    notebook_page = ll.Page('Notebook page')
    notebook_page.grid(frame, name='nbpage', rows=['region'], measures={'notional': 'sum'})
    widgets['nbp'] = PageCube(notebook_page)
    for name, widget in widgets.items():
        channel(widget, name)
    say(json.dumps({
        'url': web.url,
        'authorization': web.authorization,
        'link': cube.url,
        'model': table.model,
        'runtime': table.runtime,
        'source': table.source,
        'columns': SCHEMA.names,
        'rows': [list(row) for row in ROWS],
        'widgets': {name: {'esm': w._esm, 'state': {'_module': w._module, 'height': w.height, **(
            {'table': w.table, 'version': w.version} if isinstance(w, DataCube)
            else {'page_key': w.page_key, 'versions': w.versions})}} for name, w in widgets.items()},
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
                web.engine.serve_page('q3', served_page)
                command = 'page'
            elif command == 'page-again':
                served_page['sheets'][1]['name'] = 'Desks (v2)'
                web.engine.serve_page('q3', served_page)
            elif command == 'py-page':
                # a page built in Python (ll.Page), as a person builds one, and shown (the test opens the tab)
                pypage = ll.Page('Built in Python')
                grid = pypage.grid(frame, name='pytrades', rows=['region'], measures={'notional': 'sum'})
                grid.chart('bar', x='region', y=[('notional', 'sum')], title='By region')
                pypage.sheet('Desks').add(pypage.grid(lambda: desks['now'], name='pydesks'))
                # (show() says its link to a person, on standard output: here the test's line is the answer)
                with contextlib.redirect_stdout(io.StringIO()):
                    shown_page = ll.show(pypage, browser=False)
                command = f'py-page {json.dumps(shown_page.url)}'
            elif command == 'py-page-sheet':
                pypage.sheet('Added in Python')
            elif command == 'py-documents':
                command = f'py-documents {json.dumps(python_documents(frame, served_page, desks))}'
            elif command == 'py-page-read':
                command = f'py-page-read {json.dumps([s.name for s in pypage.read().sheets])}'
            elif command == 'desks-update':
                desks['now'] = pa.table({'desk': ['Rates', 'FX', 'Credit', 'Equity'], 'head': ['Ana', 'Ben', 'Cy', 'Di']})
                web.engine.changed('desks')
            say(f'done {command}')
    finally:
        for widget in widgets.values():
            widget.close()
        cube.close()


if __name__ == '__main__':
    main()
