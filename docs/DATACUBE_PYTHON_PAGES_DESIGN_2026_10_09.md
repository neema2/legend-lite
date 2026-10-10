# DataCube pages from Python: everything the page does, built in code (design, 2026-10-09)

**Status: agreed with the user, 2026-10-09** (§5 records the decisions). The user (2026-10-09): "python config to set up full pages with multi
grids/visuals and multiple tabs -- basically everything you can do from UI should be able to be configured from
python". Builds on `docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md` (`ll.show(df)`, the engine in Python, the notebook
cube) and `docs/DATACUBE_PAGES_DESIGN_2026_10_09.md` (pages, sheets, stacks; its phase 4, "Python pages").

## 1. The idea in one paragraph

A DataCube page already has one exact description: the saved page document (`datacube.page` version 3 -- its grids'
cubes, their charts, its sheets, their layouts, its stacks). The UI writes it and reads it. **Python builds the same
document**, through a small typed API, and DataCube opens it exactly as it opens a saved page. Nothing is described
twice, so nothing can drift: whatever the page can hold, Python can say, because it says it in the page's own words.
And it goes both ways: a page designed in the UI and exported (Export > Page File) loads in Python with its frames
bound, and a page built in Python can be adjusted in the UI and saved again.

## 2. What a developer types

```python
import legend_lite as ll

page = ll.Page("Q3 review")

# a grid over a frame, grouped, pivoted, filtered, sorted, with its measures -- as the grid's own panels set them
trades = page.grid(trades_df, name="trades",
                   rows=["region", "desk"], columns=["year"],
                   measures={"notional": "sum", "pnl": "sum"},
                   filter=[("region", "notEqual", "APAC")],
                   sort=[("notional", "desc")])
trades.calculate("margin", "$x.pnl / $x.notional")        # a calculated column, in Pure, checked by the compiler

# charts of it, as Insert > Visualization makes them (every mark and option the chart's Options window has)
by_desk = trades.chart("bar", x="desk", y=[("notional", "sum")], split="region", title="Notional by desk")
trend = trades.chart("line", x="year", y=[("pnl", "sum")], frozen=True)

# sheets, and where the tiles sit on them
summary = page.sheet("Summary")            # the page's first sheet is page.sheets[0]; this adds a second
summary.add(by_desk, trend)                # the charts on Summary; the grid stays on the first sheet
summary.arrange("side-by-side")            # the layout picker's shapes, by name
page.stack(by_desk, trend)                 # or: two tiles in one place, as tabs

ll.show(page)                              # under the cell in a notebook, else a browser tab
page.save("q3.page.json")                  # the page document, as Export > Page File writes it
again = ll.Page.load("q3.page.json", frames={"trades": trades_df})
```

The handles stay live, as `show(df)`'s do: `trades.update(new_df)` shows a new frame; a change to the page object
after it is shown (a chart added, a sheet renamed) is sent to the open page.

## 3. How it fits together

- **The page document is the contract.** Python writes version 3 (`page-document.ts`); the browser reads it with its
  one reader, which already refuses a malformed page by name. Python adds no second set of rules: its builder makes
  well-formed documents by construction (each tile on one sheet, every view placed, a stack of two or more), and what
  needs a compiler is checked by the compiler, below.
- **A grid over a frame** is a cube whose source is the frame, by its name (a new source kind in `cube-document.ts`,
  `{ _type: 'frame', name }`, beside a file's and a warehouse table's). Python's engine serves the frames as it does
  for `show(df)`; a page of several grids is several frames on one engine.
- **The browser side** hosts a page, not one cube: the engine page (`demo/engine.ts`, a browser tab) and the notebook
  widget (`demo/widget.ts`) put a `PageApp` (`page/page-app.ts`) where they now put one `CubeApp`, every grid in
  remote-run mode over its frame. So a Python page has the sheets, stacks, layouts and menus the app has.
- **Pure where the UI writes Pure.** A calculated column is a Pure expression, as the grid's own editor takes it; the
  native library parses it and types it against the frame's model when Python adds it, so a mistake fails in Python,
  at that line, with the compiler's message -- not later in the page. Filters are the filter editor's conditions
  (column, operator, value, and/or groups), written as tuples.
- **Everything, by two routes.** The typed API covers what a page is usually built from (grids, their groupings,
  pivots, measures, filters, sorts, calculated columns, formats; charts and their options; sheets, layouts, stacks,
  names). Anything else the UI can set is reachable in the document's own words -- `trades.configure({...})` takes the
  grid's configuration keys as its saved cube writes them, `chart.options({...})` a chart spec's -- and a page built in
  the UI loads as is. So coverage is complete from the first step, and the typed helpers grow where they help.

- **Live, both ways.** The engine serves a page's document beside its frames (`page.json`, as `cube.json` serves one
  cube's). When Python changes a shown page, the page's version moves and the open page reopens the new document,
  staying on the sheet it shows; what was changed in the page meanwhile is replaced -- and `page.read()` takes the open
  page's document back into Python first, to keep it. A frame updated (`grid.update(df)`) only re-queries, as now.

## 4. Order

1. **The page over an engine** (TypeScript): the frame source kind; the engine page and the notebook widget host a
   page of grids over frames, sheets and stacks included. Proven by opening a hand-written page document of two frames
   and two sheets in the pinned Chromium, in a tab and as a widget.
2. **`ll.Page` in Python**: grids, charts, sheets, layouts and stacks; `show`, `save`, `load`; the handles live after
   showing. Proven by Python tests of the documents written, and a browser check that a page built in Python opens
   with what it said.
3. **The round trip**: a page saved in the UI, loaded in Python, saved again: the same document. And the examples in
   the package's README.
4. **Then the charts** (the user, 2026-10-09: "all the charts -- look at echarts examples"), each one reachable from
   Python the moment the UI has it, since Python writes the same chart spec.

## 5. Decisions (the user, 2026-10-09)

1. **Calculated columns in Pure, filters as the filter editor's conditions in tuples** -- what DataCube saves, the
   compiler checking each in Python at the line that adds it -- rather than a Python-flavoured mini-language that would
   be translated (a second language to keep exact).
2. **Objects with methods** (`page.grid(...)`, `grid.chart(...)`, `page.sheet(...)`), each with its document form
   (`to_dict()`), discoverable with a notebook's tab completion -- rather than the document written as nested dicts.
3. **Live edits after showing**: a change to a shown page's objects is sent to the open page, as `update()` is now --
   rather than a page fixed once shown.

## 6. Step 1, where (before the first edit)

The page over an engine, in TypeScript: `datacube/src/cube-document.ts` (the frame source kind), `datacube/demo/engine.ts`
and `demo/widget.ts` (a page, not one cube, when the engine serves one), `demo/engine-cube.ts` (a grid's maker over a
frame, shared), `src/page/page-app.ts` if the page needs a host hook, and their tests; `python/legend_lite/engine.py`
(`page.json`) for the browser check. Step 2: `python/legend_lite/page.py` (new: `Page`, `Grid`, `Chart`, `Sheet`),
`datacube.py` and `notebook.py` (`show(page)`), `python/tests/test_page.py`.
