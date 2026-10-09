"""An interactive Python with legend_lite ready, to try DataCube on a dataframe:

    bazel run //python:repl

It is the repository's own Python and pinned packages (duckdb, pyarrow, pandas, polars), with the compiler's native
library and DataCube's built pages found in this binary's runfiles -- nothing is installed and nothing of the machine's
is used. `ll` (legend_lite) and `pd` (pandas) are imported, and `trades` is a small sample DataFrame:

    >>> cube = ll.show(trades)          # DataCube opens in the browser
    >>> trades.loc[0, 'qty'] = 999      # then click in the cube (or cube.refresh()) to see it
    >>> cube.update(trades.head(3))     # a new frame: the open page shows it by itself
"""

import code
import os

from python.runfiles import runfiles

files = runfiles.Create()
for name in ('LEGEND_LITE_LIBRARY', 'LEGEND_LITE_SITE'):
    if name not in os.environ:
        # the library and the site come by `bazel run`, which gives the binary its env (BUILD.bazel)
        raise SystemExit('start it with `bazel run //python:repl`: it finds the library and the site through that')
    path = files.Rlocation(os.environ[name])
    if not path or not os.path.exists(path):
        raise SystemExit(f'{name}: {os.environ[name]} is not in the runfiles')
    os.environ[name] = path

import pandas as pd  # noqa: E402

import legend_lite as ll  # noqa: E402

trades = pd.DataFrame({
    'region': ['EMEA', 'EMEA', 'AMER', 'AMER', 'APAC', 'APAC', 'EMEA', 'AMER'],
    'desk': ['Rates', 'FX', 'Rates', 'Credit', 'FX', 'Rates', 'Credit', 'FX'],
    'year': [2023, 2024, 2023, 2024, 2023, 2024, 2024, 2023],
    'notional': [100.5, 200.25, 300.75, 50.0, 75.0, 10.0, 42.0, 12.5],
    'qty': [10, 20, 30, 5, 6, 1, 4, 7],
})

code.interact(
    banner='legend-lite: `ll` and `pd` are imported, and `trades` is a sample DataFrame.\n'
           '  cube = ll.show(trades)   -- DataCube on it, in the browser\n'
           'At this prompt a change shows on the cube\'s next click (or cube.refresh()); Ctrl-D quits.',
    local={'ll': ll, 'pd': pd, 'trades': trades},
    exitmsg='',
)
