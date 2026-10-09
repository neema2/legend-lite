# A notebook's cube in a real JupyterLab: the manual check (2026-10-08)

The Bazel tests hold the notebook cube's parts: the Python widget's calls and version (`//python:notebook_test`), and
its loader and module in the pinned Chromium with a stand-in for anywidget's model (`//datacube:python_engine_test`).
This check ran the whole thing once in a real JupyterLab, to see what those stand-ins cannot show: anywidget's own
front end loading the loader, ipywidgets' channel, the cell outputs, and JupyterLab's keyboard shortcuts. It is not a
test. Running JupyterLab in a test would bring about 70 packages; whether to do that is open (the design,
`docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`, "In a notebook").

**What ran.** The wheel from `bazel build //python:wheel` (legend-lite 0.1.0, macOS arm64), installed with its
`notebook` and `pandas` extras and JupyterLab into a fresh virtual environment: JupyterLab 4.6.4, anywidget 0.11.0,
ipywidgets 8.1.9, Python 3.12. The server ran on 127.0.0.1 with a token and `--LabApp.expose_app_in_browser=True`; the
browser was the repository's pinned Chromium (chromium-headless-shell 1243), driven by `check.mjs` with Playwright. The
notebook is `check.ipynb`: a DataFrame; `cube = ll.show(df)`; `ll.show(df, name='again')` as a cell's last line; an
in-place change (`df.loc[0, 'qty'] = 999.5`); and `cube.update(df.head(2))`.

**What showed (on a fresh kernel).**

| Check | Result |
|---|---|
| both cubes show the frame's 4 rows, under their cells | ok |
| outputs per cell, all five run | `[0, 1, 1, 0, 0]`: one cube under each `show()`, none under the change or the update |
| the in-place change shows after its cell (the after-cell nudge) | ok: 999.50 in both cubes |
| `cube.update(df.head(2))` shows in the first cube | ok: 2 rows |
| arrow keys in a cube move its grid's selection, not the notebook's active cell | ok: the cell stayed 1 |
| the status bar names where the rows came from | "the engine at kernel" |

The first run found one fault, fixed before this record: `cube.update(df)` returned the cube, so a cell ending in it
displayed a second copy. `update` now returns nothing, in a tab too (`//python:notebook_test` holds it). The page
errors the run printed ("Canceled future for create_subshell_request") are JupyterLab's own, from restarting the
kernel the check starts with.

**To run it again.** Install the wheel as above, start `jupyter lab` with the flags above and this folder's notebook in
its root, then `PLAYWRIGHT=<the repository's bazel-bin/datacube/node_modules/playwright/index.mjs> node check.mjs <a
Chromium executable>` (the port and token are the script's: 8899, `checktoken`).
