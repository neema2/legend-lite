"""Python's engine for a browser test (//datacube:python_engine_test, beside it): the cube corpus's rows shown as a
Live frame (show()), with DataCube's site, and two notebook cubes over it (DataCube widgets: the rows, and three of
them). It prints one JSON line -- the engine's address and token, the cube's link, the frame's model, runtime and
source, the rows (the page loads the same rows into the tab's DuckDB), and each widget's script and state -- then takes
the test's commands until its standard input closes: `update`, `columns`, `widget-update` (each answered `done
<command>`), and `widget <name> <message>`, a message from a widget's page, as a notebook's channel brings it. What a
widget sends its page is printed as it goes: `sent <json>` (its buffers in base64) and `trait <json>` (a property set).

    engine_fixture --site <DataCube's built site>
"""

import argparse
import base64
import json
import os
import sys
import threading

import pyarrow as pa

import legend_lite as ll
from legend_lite import datacube
from legend_lite.notebook import DataCube

_said = threading.Lock()


def say(line: str) -> None:
    """One line of output, whole: widgets answer on threads of their own."""
    with _said:
        sys.stdout.write(line + '\n')
        sys.stdout.flush()


def channel(cube: DataCube) -> None:
    """The widget's channel, as the test carries it: what it sends its page, printed."""
    cube.send = lambda content, buffers=None: say('sent ' + json.dumps({
        'widget': cube.name, 'content': content, 'buffers': [base64.b64encode(bytes(b)).decode() for b in buffers or []]}))
    cube.observe(lambda change: say('trait ' + json.dumps({'widget': cube.name, 'name': change['name'],
                                                           'value': change['new']})), names=['version'])

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


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--site', required=True)
    os.environ['LEGEND_LITE_SITE'] = arguments.parse_args().site
    frame = pa.Table.from_pylist([dict(zip(SCHEMA.names, row)) for row in ROWS], schema=SCHEMA)
    # show(), as a person calls it -- but the browser is the test's (it opens the link itself)
    cube = ll.show(frame, name='trades', browser=False)
    table = datacube._session.frames['trades']
    web = datacube._session.web()
    # two notebook cubes, as ll.DataCube(df) makes them: the page shows both and fetches DataCube's module once
    widgets = {'nb': DataCube(frame, name='nb'), 'nb2': DataCube(frame.slice(0, 3), name='nb2')}
    for widget in widgets.values():
        channel(widget)
    say(json.dumps({
        'url': web.url,
        'authorization': web.authorization,
        'link': cube.url,
        'model': table.model,
        'runtime': table.runtime,
        'source': table.source,
        'columns': SCHEMA.names,
        'rows': [list(row) for row in ROWS],
        'widgets': {name: {'esm': w._esm, 'state': {'_module': w._module, 'table': w.table, 'version': w.version,
                                                    'height': w.height}} for name, w in widgets.items()},
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
            say(f'done {command}')
    finally:
        for widget in widgets.values():
            widget.close()
        cube.close()


if __name__ == '__main__':
    main()
