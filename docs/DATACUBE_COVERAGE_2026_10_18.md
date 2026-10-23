# DataCube coverage, 2026-10-18: where no test reaches

This is the command program's stage 1a, part 4 (`docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md` §11). It is a
report, not a gate. Run it again with `node datacube/tools/coverage.mjs <out>` after
`bazel build //wasm:planner_dir //engine-client:duckdb_extensions`. The script's header says what it writes.

## The numbers

Measured over DataCube's 137 Node test files, on branch `claude/datacube-command-tests`:

- **Lines:** 94.2% of the source files the tests load (44,179 of 46,892).
- **Branches:** 86.6% (10,572 of 12,207).
- **Not counted:** the browser harnesses (`demo/verify-*.mjs`) and Python's suite.

About 13 test files fail outside Bazel. They need something only their target gives, such as a built bundle, a scanner's
file list or a warehouse, and the script lists them.

## No Node test loads these

16 files. They are reached elsewhere:

| Files | Reached by |
|---|---|
| `src/headless/` grid, model, data-in, host, calls, entry, and the installers | Python's suite (`//python:headless_test`, `engine_test`) |
| `src/app/datacube-app.ts`, `src/app/sources.ts`, `src/app/services.ts` | the browser harnesses (`verify_app`, `verify_cubes`, `verify_features`, the tour) |
| `src/services/browser.ts`, `browser-basics.ts`, `src/file-handles.ts`, `src/embed.ts` | the browser harnesses |
| `src/infer.ts` | nothing to run: it declares interfaces only. |

The headless page and layout calls are loaded by `test/page-document.test.ts`, so they appear in the next table.

## The least covered of the files the tests load

| File | Lines | Branches | What is dark, and does it matter |
|---|---|---|---|
| `cube-store.ts` | 44% | 2 of 6 | The tab's IndexedDB store. The browser harnesses save and open through it. Low risk. |
| `model-actions.ts` | 45% | 3 of 8 | Create a Class… and its siblings. Python's model tests reach most of it. |
| `services/python-host.ts` | 62% | 26 of 52 | The notebook host's calls. The notebook tests reach it, in part. |
| `create-class.ts` | 66% | 2 of 8 | The class window's naming rules. Worth unit tests. |
| `ui/chart-form.ts` | 72% | 49 of 111 | **The chart options form, which the command program will route through commands.** Tests come first here. |
| `chart-render.ts` | 80% | 13 of 26 | The ECharts option builder's rarer marks. |
| `ui/chart-panel.ts` | 82% | 67 of 102 | **The charts panel's follow, freeze and Update flows**, where #47's B1 and B2 lived. Tests come first here. |
| `app.ts` | 87% | 707 of 921 | **The screen's edit handlers, which become commands.** The parity cases are where its gaps get covered. |

## What to do with it

The command program will rewrite how three places edit: `app.ts`'s handlers, `chart-panel.ts` and `chart-form.ts`.
Each needs tests on its dark branches before its commands land, by the same rule as stage 1a.

The parity file covers the grid only for now: both of its drivers compare a page's first cube, never its views. A
chart edit becomes a parity case once the drivers compare the page's views too. That comes before the chart commands.
