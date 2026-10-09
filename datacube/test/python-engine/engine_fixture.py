"""Python's engine for a browser test (//datacube:python_engine_test, beside it): the cube corpus's rows shown as a
Live frame (show()), with DataCube's site. It prints one JSON line -- the engine's address and token, the cube's link,
the frame's model, runtime and source, and the rows (the page loads the same rows into the tab's DuckDB) -- then
takes the test's commands (`update`, `columns`) until its standard input closes.

    engine_fixture --site <DataCube's built site>
"""

import argparse
import json
import os
import sys

import pyarrow as pa

import legend_lite as ll
from legend_lite import datacube

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
    engine = datacube._session.engine()
    print(json.dumps({
        'url': engine.url,
        'authorization': engine.authorization,
        'link': cube.url,
        'model': table.model,
        'runtime': table.runtime,
        'source': table.source,
        'columns': SCHEMA.names,
        'rows': [list(row) for row in ROWS],
    }), flush=True)
    # the test's commands, one a line: `update` -- one row more; `columns` -- a column more (a new model). Closed
    # whatever happens, so the process ends with its input
    try:
        for line in sys.stdin:
            if line.strip() == 'update':
                cube.update(pa.concat_tables([frame, frame.slice(0, 1)]))
            elif line.strip() == 'columns':
                cube.update(frame.append_column('trader', pa.array([f't{i}' for i in range(frame.num_rows)])))
            print(f'done {line.strip()}', flush=True)
    finally:
        cube.close()


if __name__ == '__main__':
    main()
