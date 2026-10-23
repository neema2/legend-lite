# The DataCube tour: a playbook

The tour drives DataCube in a real browser the way a person would. It runs the round-3 flows one after another on a
single page. Every step is checked and photographed, and the run writes a gallery (`index.html`) next to the pictures,
so you can see what each check saw. A step that fails is photographed too, which shows where it went wrong.

The user asked for it on 2026-10-17: "definitely do the screenshot tour - maybe save the playbook for that so we can
run it from time to time as well in the heavy lane". The script is `datacube/demo/tour.mjs`.

It sits between two existing kinds of harness. `shots.mjs` takes pictures and checks nothing. The `verify-*` harnesses
check and take no pictures, and they run on every push. The tour does both, and it is manual.

## Running it

| How | Command | Where the gallery goes |
|---|---|---|
| As a test, on the Chromium that Bazel pins | `bazel test --nozip_undeclared_test_outputs //datacube:tour_test` | `bazel-testlogs/datacube/tour_test/test.outputs/index.html` |
| By hand | `bazel run //datacube:tour -- --out datacube-tour` | `datacube-tour/index.html`, relative to the directory you ran it from |
| With the heavy suite | `bazel test //gates:heavy` | as for the test |

- **The flag:** without `--nozip_undeclared_test_outputs`, Bazel zips the outputs into `outputs.zip`.
- **The tab:** both forms serve the built site on 127.0.0.1 and drive one tab.
- **Run time:** a few minutes, most of it the example pages.
- **The result:** the run exits 1 if any step fails, prints one line per step, and ends with the gallery's path.
- **Hand runs:** `bazel run` uses Playwright's own Chromium. Install it once with `bazel run //datacube:install_browser`.

## When to run it

- Before a DataCube change lands that touches the grid's settings, the charts, saving or sharing.
- From time to time, through `//gates:heavy`. The tour is manual and no per-push lane runs it.
- When someone wants to see the product rather than read about it. The gallery can go in a PR or be shared as a page.

Nothing is lost by its being manual. Each check it makes is also made by a per-push harness:

| Tour steps | Per-push harness |
|---|---|
| Grid edits and charts | `verify_charts_test` |
| Saving and sharing | `verify_cubes_test` |
| Undo, sorts and calculated columns | `verify_features_test` |

The tour's job is the pictures, and running the flows in order on one page.

## The steps, and what to look at

Each step is one section of the gallery. A step's line says what the check found.

1. **A page over the sample trades.** New, then Blank Page, then "Add a data source", then the picker's Examples: the
   trades sample, as a person starts a page. The demo's own first grid knows no source, so a page built on it cannot
   be saved or shared; a page over a sample from the picker can.
2. **Grouped from the tool panel.** region, desk, then book, each double-clicked. The grid shows a three-level tree.
3. **A row group taken off by its chip.** book's `×` is clicked. Check that the grid regroups and the chip is gone.
4. **Column Properties: a column's Aggregation set to average.** This is the round-3 model: a grid's aggregates are
   its columns' Aggregation (`docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md` §3.5). There are two pictures: the editor
   before Apply, and the grid after it. That column's numbers are now averages.
5. **A sort, from a cell's menu.** Sort, then Descending, on a cell of that column. The groups reorder, largest first.
6. **A calculated column.** Extended Columns, then Add New Column…. It is a formula over the numeric column, and the
   compiler checks it live. There are two pictures: the editor with its check, and the grid with the new column.
7. **Insert, then Visualization.** A chart that follows the grid, with its pill reading Dynamic.
8. **The Aggregation changed again.** The following chart plots its column at the new Aggregation.
9. **Two values of one column.** The grid is first left with one row group and no pivot, because a split draws one
   value per series. The chart is then frozen and a second value of the same column is added under another
   aggregate, and the chart is made Dynamic again. Both values stay. This is the B2 fix: a following chart never
   folds one value onto another. There are two pictures, frozen and Dynamic, each with two series.
10. **Show Grid on a frozen chart.** The chart is shown in a grid of its own grouping, which it follows from then on,
    plotting exactly the values it had (the B1 rule: each value read back from the column that carries it). The
    page's Undo then takes the step back. There are two pictures: the chart beside its own grid, then the page undone.
11. **Undo and Redo.** The grid's own menu. Undo changes the grid, and Redo brings it back exactly. The query's epoch,
    a counter every new request bumps, is left out of the comparison.
12. **Save As, reload, reopen.** The page is saved, the tab reloaded, and the page reopened from Open…. The grid's row
    groups and every chart come back as saved.
13. **Share….** The link opens in a fresh tab with the same grid and charts. The picture is of the fresh tab.
14. **An older page file refused.** The page is exported, its cube marked version 1, and imported. Open… says the file
    is no longer read, and the grid on screen is unchanged. The user ruled on 2026-10-17 that no older document is
    read.
15. **The example page `charts-dashboard`.** Every chart on every sheet draws. There is one picture per sheet.
16. **The example page `charts-kinds`.** As for 15.
17. **No page errors on the way.**

## When a step fails

- **Read the failure:** the step's line, in the gallery and in the log, says what was seen and what was wanted.
- **Look at the picture:** it was taken at the moment of failure. A menu still open, a window in the way or an error in
  the status bar usually explains it.
- **Check the flow first:** the steps share one page, so a failure early on can leave later steps on an unexpected
  page. Fix the first failure first.
- **Reproduce it:** if a flow is broken in the product, the same flow in its per-push harness (table above) should
  fail too. Add the case there, so it is checked on every push and not only when the tour runs.

## Changing the tour

A step is `step(title, fn)`. `fn` drives the page and returns what it saw, or throws to say why not. It is given
`shot(target, label)` to take a picture mid-step, for example with a window open. A step that takes no picture is
photographed when it ends.

- **Selectors:** the helpers use the same selectors as `verify-charts.mjs` and `verify-cubes.mjs`. If one of those
  changes, change it here too.
- **Waiting:** never use a fixed sleep (G-11). Wait on `settle()`, which watches the page signal, or on a condition.
- **New flows:** when a round adds one, add a step for it and a line in the list above.
