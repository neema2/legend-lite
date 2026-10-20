// The page's bundle, against its budget (plan F7). A grid-only page must not download ECharts:
// the chart renderer is reached only by a dynamic import, the first time a chart draws
// (ui/chart-panel.ts), so esbuild puts it in a chunk the bundle does not import at startup.
// And the bundle a grid loads has a size budget, so growth is a decision, not an accident.

import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { gzipSync } from 'node:zlib';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { specifiersOf } from '../demo/widget-loader.ts';

// the bundle and its chunk directory, side by side as esbuild wrote them (BUILD.bazel names bundle.js in BUNDLE)
const DEMO = dirname(runfileFromEnv('BUNDLE'));
const CHUNKS = join(DEMO, 'chunks-bundle');

/** One file of a bundle as esbuild's record says (BUILD.bazel: its <bundle>-meta.json beside it): the modules it holds
 *  code of, and what it imports, statically and dynamically. The bundles are minified, so no file says in a comment
 *  which module it holds: the record does. */
interface Output { readonly modules: readonly string[]; readonly statics: readonly string[]; readonly dynamics: readonly string[] }

/** Every JavaScript file of a bundle, by its path, as its metafile records it. */
function outputsOf(bundle: string): ReadonlyMap<string, Output> {
  const meta = JSON.parse(readFileSync(join(dirname(bundle), `${basename(bundle, '.js')}-meta.json`), 'utf8')) as {
    outputs: Record<string, { inputs: Record<string, { bytesInOutput: number }>; imports?: { path: string; kind: string }[] }>;
  };
  // the record's paths are the package's, where esbuild ran: demo/bundle.js, demo/chunks-bundle/...
  const pkg = dirname(dirname(bundle));
  const imported = (out: { imports?: { path: string; kind: string }[] }, kind: string): string[] =>
    (out.imports ?? []).filter((i) => i.kind === kind).map((i) => join(pkg, i.path));
  return new Map(Object.entries(meta.outputs).filter(([path]) => path.endsWith('.js')).map(([path, out]) => [join(pkg, path), {
    // a module that is only imported, its own code none, is not held here
    modules: Object.entries(out.inputs).filter(([, i]) => i.bytesInOutput > 0).map(([m]) => m),
    statics: imported(out, 'import-statement'),
    dynamics: imported(out, 'dynamic-import'),
  }]));
}

const PAGE = outputsOf(join(DEMO, 'bundle.js'));

/** Whether a file of a bundle holds code of `module`: ECharts (any of its modules), or one of DataCube's by its path in
 *  the package (`src/chart-render.ts`; the record names it from wherever esbuild ran, so by its end). */
function holds(file: string, module: string, outputs: ReadonlyMap<string, Output> = PAGE): boolean {
  const out = outputs.get(file);
  assert.ok(out, `${file} is in esbuild's record`);
  return out.modules.some((m) => module === 'echarts' ? m.includes('node_modules/echarts/') : m === module || m.endsWith(`/datacube/${module}`));
}

/** The chunks beside the page's bundle, as the record names them. */
const chunksOf = (outputs: ReadonlyMap<string, Output> = PAGE): string[] => [...outputs.keys()].filter((f) => dirname(f) === CHUNKS);

/** Every file a page loads before any chart: its bundle and what it imports statically, transitively. */
function startup(bundle = join(DEMO, 'bundle.js'), outputs: ReadonlyMap<string, Output> = PAGE): string[] {
  const seen = new Set<string>();
  const walk = (file: string): void => {
    if (seen.has(file)) return;
    seen.add(file);
    const out = outputs.get(file);
    assert.ok(out, `${file} is in esbuild's record`);
    out.statics.forEach(walk);
  };
  walk(bundle);
  return [...seen];
}

/** The page's grid-only download, gzipped, in bytes: what it costs before any chart. Raise it on purpose.
 *  350,000 -> 352,000 (2026-10-02, store types step 7): Postgres's catalog rules -- DataCube writes a
 *  Postgres table's model by Postgres's own rules (generated/catalog-facts.ts), about 0.5 KB gzipped.
 *  Not lowered with the TypeScript writer's removal (2026-10-08, the model written by legend-lite's
 *  module): a budget is raised on purpose, and measured down when the user asks.
 *  352,000 -> 368,000 (2026-10-09, DataCube pages phase 2, docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §6): every page
 *  is a page of its own from the start, its one grid a tile on its board -- the page (page/page-app.ts), the board
 *  (page/cube-page.ts, layout/band-board.ts) and the charts' panel load at startup, where phase 1 fetched them with the
 *  first chart (startup then ~333 KB). Measured 364,791. The layouts are still fetched when first opened, ECharts
 *  when a chart first draws.
 *  368,000 -> 378,000 (2026-10-09, DataCube pages phase 3a, sheets, the design's §7): the sheet tabs in the page's bar
 *  (ui/sheet-tabs.ts), a board per sheet and Move to Sheet (page/cube-page.ts), the name box (page/page-app.ts), all
 *  on screen from the start. Measured 375,751, main before it 367,729 (+8,022).
 *  378,000 -> 381,000 (2026-10-09, DataCube pages phase 3b, stacked tiles, the design's §7.4): a stack in the layout
 *  (layout/bands.ts) and its tabs on the board (layout/band-board.ts), at startup as the board is. Measured 378,297,
 *  sheets before it 375,928 (+2,369).
 *  381,000 -> 383,000 (2026-10-10, DataCube charts batch 1's KPI, landing before the batch's page filters: the KPI's
 *  drawing (chart-option.ts kpiDrawing), its queries (chart-spec.ts kpiQueries) and its form in the charts' panel, at
 *  startup as that panel is. Measured 381,890, main before it under its 381,000.
 *  383,000 -> 386,000 (2026-10-10, DataCube model step 2, Create a Class…, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15):
 *  a grid's menu entry and its checks (query.ts classLambda, storeTableOf, writesClasses), the planner's route
 *  (lite-planner.ts promoteRelationToModel), the page's model read and written with the page (page-document.ts version
 *  4, its tables written for the grids the page holds), each grid's table kept by name on reopening and reserved while
 *  a page opens, and the page file refused while its classes are only here (demo/boot.ts), at startup; the window, the
 *  page's model and the SDLC client are fetched the first time a class is made (src/create-class.ts, its own chunk).
 *  Measured 385,251 with the audit's fixes, 384,750 before them; main before it 381,890 (+3,361).
 *  386,000 -> 387,000 (2026-10-11, DataCube model step 2's second landing, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 *  §15.6a: Create an Association… and Create a Data Space…): the page's Model menu and a grid's entry and their handlers
 *  (demo/boot.ts), the routes' request (model-writer.ts modelRequestBody, which the planner sends), the facts' query
 *  (query.ts duplicateLambda) and each grid's class read and written with the page (page-document.ts), at startup; the
 *  windows, the actions, the names and the SDLC client are chunks fetched the first time one is asked for
 *  (src/model-actions.ts, src/model-names.ts: imported by nothing the page loads at startup). Measured 386,448, main
 *  before it 385,251 (+1,197).
 *  387,000 -> 389,000 (2026-10-11, DataCube one app, step 1, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §12): the tab's
 *  services (src/services/browser.ts: the planner, the database, the page store, the query store, the model home, the
 *  warehouse and the browser, built once), which the app reads everything through (1a, +520); the app building its own
 *  windows -- Open…'s list, Share, the Generated Pure & SQL -- whose markup was index.html's, not counted here (1c);
 *  the dead branches of a planner that opens no file gone. Measured 388,056, main before it 386,536 (+1,520).
 *  389,000 -> 391,000 (2026-10-12, DataCube one app, step 2a: Python's pages open the app): what the app does for a host
 *  whose rows are its own -- the frame as a source, a grid's rows in the host's shape, following another editor of the
 *  page and reporting to it (src/app/datacube-app.ts, src/app/sources.ts), the picker's sections as the services offer
 *  them, and the status line's look moved into the app's styles. Measured 390,035, step 1 before it 388,056 (+1,979).
 *  391,000 -> 392,000 (2026-10-12, DataCube one app, PlanAndRun, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): every
 *  host's grid through one runner -- PlanAndRun over a host's plan-and-run (runner.ts, the tab's localHost), the plan's
 *  request and reading shared by every host (lite-planner.ts planRequest, readPlan). Measured 391,138, phase 2 before
 *  it 390,817 (+321).
 *  392,000 -> 393,000 (2026-10-12, the program's B1, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md): the model actions over
 *  the host's services, never the page's own planner and engine -- the tab's model writing (services/browser.ts
 *  tabModelWriting) and a grid's rows where they are planned and run (src/app/datacube-app.ts rowsOf). Measured 392,082,
 *  main before it 391,846 (+236: the session setup's 708 since 391,138 came with PR #31, under the budget).
 *  393,000 -> 394,000 (2026-10-13, the program's D): a grid's export is one core for the screen's Export menu and
 *  Python's grid.export() (src/export-render.ts: the renderer, the formats and the heatmaps moved out of app.ts into a
 *  module of their own, which the headless bundle calls too). Measured 393,014, B4 before it 392,804 (+210).
 *  394,000 -> 395,000 (2026-10-13, the program's D5: the page spells no SQL): the demo's trades made in the tab as
 *  Arrow data and put in by DuckDB's Arrow reader, where a SQL statement generated them (demo/boot.ts generateTrades;
 *  engine-client's arrow-ipc.ts arrowIpcStream -- Arrow's table and vector makers; its writer was already here,
 *  DuckDB-WASM's). Measured 394,429, D5's step 5 before it 393,598 (+831).
 *  395,000 -> 397,000 (2026-10-14, DataCube charts batch 2, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.1-§3.2): the
 *  chart spec's version 2 and its reader (chart-spec.ts specProblem: a saved page's every chart is read at startup),
 *  the presets the chart's query expands (calc-presets.ts) and their fields in the charts' panel. The drawing itself
 *  (chart-option.ts) moved OUT of startup in the same step -- it comes with ECharts the first time a chart draws -- so
 *  the rest of batch 2's drawing grows the chart's chunk, not this. Measured 395,616 (399,865 before the drawing moved
 *  out), main before it 394,429.
 *  397,000 -> 399,000 (2026-10-14, DataCube charts batch 2, steps 5-8): the charts' panel's new fields at startup, as the
 *  panel is -- more values on one chart (each its mark and axis), reference lines, a hover's and a label's measures.
 *  Measured 398,519, step 4 before it 395,616.
 *  399,000 -> 401,000 (2026-10-14, batch 2 step 10): drilling a chart down the grid's groupings (chart-spec.ts
 *  drilledView, drillDown; the panel's path above the chart). Measured 399,861. The charts' panel loads with the board
 *  at startup, so every field it gains is here: loading it with a page's first chart instead (cube-page.ts openChart,
 *  synchronous today) is a follow-up of its own.
 *  Kept at 401,000 (2026-10-14, batch 2 step 12): the charts' options form moved out of startup into its own chunk
 *  (ui/chart-form.ts, 4,822 bytes, fetched the first time a chart's options are shown), so the fields batch 2 added and
 *  every later batch adds grow that chunk. Measured 398,792 with step 12's options in it, 401,306 before the move.
 *  401,000 -> 403,000 (2026-10-14, batch 2 step 14): the sales sample (samples.ts salesCsv: the picker's samples are at
 *  startup) and the Open window's Examples list (examples/catalog.ts: names only -- each example page is a chunk of its
 *  own, built when picked). Measured 401,224.
 *  403,000 -> 406,000 (2026-10-14, batch 3 step 1): the spec's seventeen new kinds -- their words, what each refuses by
 *  name, and the figures the database computes for a waterfall, a Pareto and a pie's inner ring (chart-spec.ts,
 *  calc-presets.ts), at startup as the spec's reader is; their drawing is a chunk. Measured 404,401.
 *  406,000 -> 281,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's
 *  "does minify buy anything if already zipped?" -- it does, about a third): the same code, a third smaller gzipped,
 *  and about half the text the browser parses (the page's script 764,104 bytes at 2d24022, 411,165 minified). Measured
 *  278,151; unminified, the same tree 405,798 (the chart rules written once -- chart-spec.ts readChart and chartFrom --
 *  at startup with the spec's reader).
 *  281,000 -> 283,500 (2026-10-17, PR neema2/legend-lite#47 round 3, docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md):
 *  one way to edit a grid -- grid-edits.ts, the screen's grouping, pivot, sort and Column Properties rules in one module
 *  the panels, the header menu, the drag zones and Python all call, with their refusals in words, and measuresAsColumns
 *  (a cube saved with a measures list converted on open; Open in Grid's values as Column Properties). Measured 282,270;
 *  278,151 on 2026-10-15, before #47's rounds 2 and 3. */
const BUDGET = 283_500;

/** The engine page's (demo/engine.html: DataCube on Python's engine, which runs its queries) startup download,
 *  gzipped: no DuckDB-WASM and no compiler. Set 2026-10-08 at its first measure (288,876 bytes) and the app's headroom
 *  (about 4%), so its growth is a decision too.
 *  300,000 -> 380,000 (2026-10-12, DataCube one app, step 2a, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md R1): the page
 *  is DataCube's app over Python's services, and `ll.show(df)` a page of one grid -- so the page shell, its sheets, the
 *  app (its sources, the source picker, the saved pages' windows) are at startup, where one cube's tab loaded the
 *  grid alone and fetched the page's code only for a page. The design's "the engine page's startup holds" (§10) did
 *  not survive R1: the page itself is what grew. What the tab's startup carries, less the planner's and DuckDB's
 *  clients. Measured 366,792; its first measure, one cube's tab, 288,876 (2026-10-08).
 *  380,000 -> 381,000 (2026-10-14, DataCube charts batch 2): the grid's budget's note of the same date -- the chart
 *  spec's version 2 reader and its presets at startup, the drawing moved out. Measured 380,118.
 *  381,000 -> 384,000 (2026-10-14, DataCube charts batch 2, steps 5-8): the grid's budget's note of the same date.
 *  Measured 383,019.
 *  384,000 -> 386,000 (2026-10-14, batch 2 step 10): as the grid's. Measured 384,369.
 *  386,000 -> 388,000 (2026-10-14, batch 2 steps 12-14 and its audit): the sales sample and the Examples list (the
 *  grid's note of step 14), and the reader's checks of a chart's option words. Measured 387,083.
 *  388,000 -> 391,000 (2026-10-14, batch 3 step 1): as the grid's. Measured 389,154.
 *  391,000 -> 270,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's
 *  "does minify buy anything if already zipped?" -- it does, about a third). Measured 266,978; unminified, the same
 *  tree 390,443.
 *  270,000 -> 271,000 (2026-10-18, neema2/legend-lite#57 and its audit: a header click sorts (grid-edits.ts clickSort,
 *  clickTreeSort), the header's marks from the order the query applies (headerSorts), and a child group's aggregate
 *  refused as a sort). Measured 270,114. */
const ENGINE_BUDGET = 271_000;

/** What a page's first chart fetches beyond its startup, gzipped: ECharts (its chunk) and the drawing (chart-option.ts).
 *  Set 2026-10-14 at its first measure (DataCube charts batch 2, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §5: "the
 *  chart chunk gets a budget of its own beside the grid's"), raised with each batch on purpose. Measured 368,423: the
 *  ECharts chunk 361,342 (bar, line, scatter, pie, heatmap and treemap with their components, as registered in
 *  chart-render.ts) and the drawing's.
 *  369,000 -> 392,000 (2026-10-14, batch 2 steps 5-8): ECharts' mark lines, areas and points (a chart's reference
 *  lines, bands and pinned extremes) and axis breaks registered, and the drawing of a value per mark and axis, the
 *  grid's formats and the layers. Measured 390,828.
 *  392,000 -> 408,000 (2026-10-14, batch 2 step 9): ECharts' data zoom (a long series' slider, zooming inside the chart)
 *  and the time axis's drawing. Measured 406,575.
 *  408,000 -> 433,000 (2026-10-14, batch 2 step 11): ECharts' brush and the toolbox it starts from (step 12's toolbox
 *  buttons use it too). Measured 431,396.
 *  433,000 -> 436,000 (2026-10-14, batch 2's audit): the brush taking its range by value and the database's edges
 *  (chart-spec.ts AXIS_UPPER), and a crosshair that relays only the tooltip (chart-render.ts, in place of ECharts'
 *  connect). Measured 433,874.
 *  436,000 -> 447,000 (2026-10-14, batch 3 step 1, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): ECharts' charts and
 *  components registered each by its own module (chart-render.ts), not through the index, so batch 3's rarer charts are
 *  a chunk of their own -- what the first chart and the kinds' chunk both use is a third chunk, and three files
 *  compress less well than one; and a pie in two rings or half a ring (chart-option.ts). Measured 445,666.
 *  447,000 -> 301,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's
 *  "does minify buy anything if already zipped?" -- it does, about a third). Measured 298,386; unminified, the same
 *  tree 446,304. */
const CHART_BUDGET = 301_000;

/** Batch 3's kinds (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): their drawing (chart-kinds.ts) and the rarer ECharts
 *  charts they draw with (chart-render-kinds.ts), fetched with the first chart of one of them, beyond the first chart's
 *  own. */
// a chunk's file name, whichever separator the platform's paths use (Windows': backslashes)
const isKinds = (file: string): boolean => /[\\/]chart-(?:render-)?kinds-[^\\/]+\.js$/.test(file);
/** Set 2026-10-14 at its first measure (batch 3 step 1), 92,574: the drawing 7,823 and the charts 84,751, with room for
 *  batch 3's later kinds (small multiples, a bubble).
 *  100,000 -> 66,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's
 *  "does minify buy anything if already zipped?" -- it does, about a third): batch 3's kinds all in. Measured 62,426;
 *  unminified, the same tree 95,848. */
const KINDS_BUDGET = 66_000;

/** The charts' options form (ui/chart-form.ts), fetched the first time a chart's options are shown: set 2026-10-14 at
 *  its first measure, 4,872, with room for batch 3's fields.
 *  8,000 -> 6,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's "does
 *  minify buy anything if already zipped?" -- it does, about a third). Measured 4,858 (a chunk that small gains
 *  little); unminified, the same tree 5,793. */
const FORM_BUDGET = 6_000;

/** An example page's builder (src/examples/charts-dashboard.ts), fetched when it is picked: set 2026-10-14 at its first
 *  measure, 3,264.
 *  6,000 -> 4,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's "does
 *  minify buy anything if already zipped?" -- it does, about a third). Measured 2,739; unminified, the same tree 3,301.
 */
const EXAMPLE_BUDGET = 4_000;

/** A notebook's cube (legend_lite.notebook.DataCube): DataCube's module and its styles (widget.js, widget.css),
 *  gzipped, which the widget's loader fetches over the kernel's channel once per notebook page. Set 2026-10-08 at its
 *  first measure (461,746 bytes: ECharts within, as the module is one file) and about 4% headroom. And the loader
 *  itself, which anywidget sends with every widget: small.
 *  480,000 -> 500,000 (2026-10-09, Python pages step 1, docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): a notebook's
 *  cube can be a whole page (demo/engine-page.ts, the page shell with its sheets and stacks), in the one file -- a module
 *  from a blob URL loads no chunk. Measured 489,067 (+11,532 over main, by widget.js and widget.css alone).
 *  500,000 -> 560,000 (2026-10-12, DataCube one app, step 2a, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §10): the module
 *  is DataCube's app over Python's services, every window the app has inside the one file -- what are chunks in the
 *  tab (the saved pages' windows, the model actions, the saved queries' client) are not lazy here. As the design said
 *  it would be, measured and raised with its reason; a loader that fetches chunks over the channel is its own piece,
 *  on a measurement. Measured 536,970 (widget.js 519,659, widget.css 17,311).
 *  560,000 -> 568,000 (2026-10-14, DataCube charts batch 2, steps 5-8): ECharts' markers and axis breaks and the charts'
 *  new drawing, in the one file (a chart chunk in the tab). Fetching chart code lazily over the channel is batch 2's
 *  item 13 (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3), measured and asked of the user before it is built. Measured
 *  566,503.
 *  568,000 -> 579,000 (2026-10-14, batch 2 step 9): the data zoom, as in the chart's chunk. Measured 577,285.
 *  579,000 -> 598,000 (2026-10-14, batch 2 step 11): the brush and the toolbox, as in the chart's chunk. Measured
 *  596,247.
 *  598,000 -> 604,000 (2026-10-14, batch 2 steps 12-14): the options form, the toolbox's CSV, the example page and the
 *  sales sample -- chunks in the tab, one file here. Measured 602,438.
 *  604,000 -> 606,000 (2026-10-14, batch 2's audit): the chart chunk's note of the same date. Measured 604,278.
 *  606,000 -> 290,000 (2026-10-14, batch 2 step 13, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): the module has lazy
 *  chunks (widget-chunks/), which the loader fetches over the channel when the cube first needs each and imports with
 *  their specifiers rewritten -- ECharts and the chart code, the page's sheets, the saved pages' windows, the model
 *  actions. What a cube fetches before it shows: the module, the chunks it imports statically and its styles, measured
 *  281,797 (all the chunks 410,466). */
const WIDGET_BUDGET = 290_000;
const LOADER_BUDGET = 10_000;

describe('the page loads ECharts only when a chart draws', () => {
  it('esbuild\'s record names every file the bundle wrote, the chunks beside it and no other', () => {
    assert.deepEqual(chunksOf().sort(), readdirSync(CHUNKS).map((f) => join(CHUNKS, f)).sort());
  });

  it('no file the page loads at startup contains ECharts', () => {
    for (const f of startup()) assert.ok(!holds(f, 'echarts'), `${f} carries ECharts`);
  });

  it('ECharts is in a chunk beside the bundle, reached by a dynamic import', () => {
    const lazy = chunksOf().filter((f) => holds(f, 'src/chart-render.ts'));
    assert.equal(lazy.length, 1, 'one chunk holds the chart renderer');
    // from the page's own chunk (page/cube-page.ts, itself fetched when a cube first gets a page), or the bundle
    const importers = [...PAGE].filter(([, out]) => out.dynamics.includes(lazy[0]!));
    assert.ok(importers.length > 0, 'something imports the chart renderer, dynamically');
  });

  it('the layouts (ui/layout-picker.ts) are fetched the first time they are opened, not at startup', () => {
    for (const f of startup()) assert.ok(!holds(f, 'src/ui/layout-picker.ts'), `${f} carries the layout picker`);
    assert.equal(chunksOf().filter((f) => holds(f, 'src/ui/layout-picker.ts')).length, 1, 'one chunk holds the layout picker');
  });

  it(`the first chart fetches at most ${CHART_BUDGET.toLocaleString()} bytes more, gzipped: ECharts and the drawing`, () => {
    // what a chart adds to the page, beside the grid's own budget (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §5)
    const chart = chunksOf().filter((f) => (holds(f, 'echarts') || holds(f, 'src/chart-option.ts')) && !isKinds(f));
    assert.ok(chart.length >= 1, 'the chart\'s chunks are found');
    const size = chart.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= CHART_BUDGET, `a first chart fetches ${size.toLocaleString()} bytes gzipped, over its budget of ${CHART_BUDGET.toLocaleString()}`);
  });

  it(`batch 3's kinds are chunks of their own, fetched with the first of them: at most ${KINDS_BUDGET.toLocaleString()} bytes more`, () => {
    for (const f of startup()) assert.ok(!isKinds(f) && !holds(f, 'src/chart-kinds.ts'), `${f} carries batch 3's kinds`);
    const kinds = chunksOf().filter(isKinds);
    assert.equal(kinds.length, 2, 'the kinds\' drawing and their ECharts charts, a chunk each');
    assert.ok(kinds.length >= 1, 'the kinds\' chunks are found');
    const size = kinds.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= KINDS_BUDGET, `batch 3's kinds fetch ${size.toLocaleString()} bytes gzipped, over their budget of ${KINDS_BUDGET.toLocaleString()}`);
  });

  it(`the options form and an example page are chunks of their own, each within its budget`, () => {
    const sized = (module: string): number => chunksOf().filter((f) => holds(f, module))
      .reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    for (const f of startup()) {
      assert.ok(!holds(f, 'src/ui/chart-form.ts') && !holds(f, 'src/examples/charts-dashboard.ts'), `${f} carries the form or an example`);
    }
    const form = sized('src/ui/chart-form.ts');
    const example = sized('src/examples/charts-dashboard.ts');
    assert.ok(form > 0 && form <= FORM_BUDGET, `the options form is ${form.toLocaleString()} bytes gzipped (budget ${FORM_BUDGET.toLocaleString()})`);
    assert.ok(example > 0 && example <= EXAMPLE_BUDGET, `the example page is ${example.toLocaleString()} bytes gzipped (budget ${EXAMPLE_BUDGET.toLocaleString()})`);
  });

  it(`a grid-only page downloads at most ${BUDGET.toLocaleString()} bytes of script, gzipped`, () => {
    const size = startup().reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= BUDGET, `the grid's script is ${size.toLocaleString()} bytes gzipped, over its budget of ${BUDGET.toLocaleString()}`);
  });

  it(`the engine page downloads at most ${ENGINE_BUDGET.toLocaleString()} bytes of script, gzipped, and no ECharts`, () => {
    const engine = runfileFromEnv('ENGINE_BUNDLE');
    const outputs = outputsOf(engine);
    const files = startup(engine, outputs);
    for (const f of files) assert.ok(!holds(f, 'echarts', outputs), `${f} carries ECharts`);
    const size = files.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= ENGINE_BUDGET,
      `the engine page's script is ${size.toLocaleString()} bytes gzipped, over its budget of ${ENGINE_BUDGET.toLocaleString()}`);
  });
});

describe('a notebook\'s cube is one module and its lazy chunks, fetched over the channel, and its loader small', () => {
  const module = runfileFromEnv('WIDGET');
  const styles = join(dirname(module), 'widget.css');
  const chunks = join(dirname(module), 'widget-chunks');
  /** What a minified file imports statically (`from"./x.js"`, `import"./x.js"`), resolved; and what it imports
   *  dynamically (`import("./x.js")`). */
  const imports = (file: string, dynamic: boolean): string[] =>
    [...readFileSync(file, 'utf8').matchAll(dynamic ? /\bimport\s*\(\s*"(\.[^"]+)"/g : /\bfrom\s*"(\.[^"]+)"|\bimport\s*"(\.[^"]+)"/g)]
      .map((m) => join(dirname(file), (m[1] ?? m[2])!));
  /** The module and what it imports statically, transitively: what the loader fetches before a cube shows. */
  const atStart = (): string[] => {
    const seen = new Set<string>();
    const walk = (file: string): void => {
      if (seen.has(file)) return;
      seen.add(file);
      imports(file, false).forEach(walk);
    };
    walk(module);
    return [...seen];
  };

  it('the module imports only its chunks: the loader rewrites those, a module from a blob URL resolving none', () => {
    for (const file of [module, ...readdirSync(chunks).map((f) => join(chunks, f))]) {
      for (const target of [...imports(file, false), ...imports(file, true)]) {
        assert.equal(dirname(target), chunks, `${file} imports ${target}, not a chunk`);
      }
    }
  });

  it('the loader rewrites exactly the imports esbuild wrote, by kind, in every file of the module (its metafile)', () => {
    // the loader reads each file's specifiers with a pattern over esbuild's minified output; esbuild's own record says
    // what each output imports: a missed form would be a chunk that never loads in a notebook, a false match broken code
    const demo = dirname(module);
    const meta = JSON.parse(readFileSync(join(demo, 'widget-meta.json'), 'utf8')) as {
      outputs: Record<string, { imports?: { path: string; kind: string }[] }>;
    };
    const outputs = Object.entries(meta.outputs).filter(([path]) => path.endsWith('.js'));
    assert.ok(outputs.length > 10, 'the module and its chunks');
    const local = (path: string): string => path.replace(/^demo\//, '');
    for (const [path, out] of outputs) {
      const name = local(path);
      const expected = (out.imports ?? []).map((i) => `${i.kind === 'dynamic-import' ? 'dynamic' : 'static'} ${local(i.path)}`).sort();
      const read = specifiersOf(readFileSync(join(demo, name), 'utf8'), name).map((s) => `${s.dynamic ? 'dynamic' : 'static'} ${s.target}`).sort();
      assert.deepEqual([...new Set(read)], [...new Set(expected)], name);
      for (const line of expected) assert.match(line, / widget-chunks\//, `${name} imports only chunks, which the loader rewrites`);
    }
  });

  it('ECharts is not fetched before a chart draws: in a chunk the module imports dynamically', () => {
    for (const f of atStart()) assert.ok(!readFileSync(f, 'utf8').includes('echarts'), `${f} carries ECharts`);
    const lazy = readdirSync(chunks).map((f) => join(chunks, f)).filter((f) => readFileSync(f, 'utf8').includes('echarts'));
    assert.ok(lazy.length > 0, 'a chunk holds ECharts');
  });

  it(`the module, what it imports at start and its styles are at most ${WIDGET_BUDGET.toLocaleString()} bytes gzipped, with no DuckDB-WASM`, () => {
    const text = [module, ...readdirSync(chunks).map((f) => join(chunks, f))].map((f) => readFileSync(f, 'utf8')).join('\n');
    // DuckDB-WASM's code, by what a page that can start it must name: its package, its workers and its modules
    // (duckdb-browser-*.worker.js, duckdb-eh.wasm), its client. Not the word: since 2026-10-12 (DataCube one app, step
    // 2a) the module is the app, whose examples and Remote section say in words what DuckDB does with a file
    assert.ok(!/duckdb-wasm|duckdb-(?:browser|eh|mvp|coi)\b|AsyncDuckDB/i.test(text), 'the notebook cube runs nothing in the page: no DuckDB-WASM');
    const size = atStart().reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0) + gzipSync(readFileSync(styles)).length;
    assert.ok(size <= WIDGET_BUDGET,
      `a notebook cube's module is ${size.toLocaleString()} bytes gzipped, over its budget of ${WIDGET_BUDGET.toLocaleString()}`);
  });

  it('its styles fetch nothing: a notebook page has no site to fetch a font or an image from', () => {
    const css = readFileSync(styles, 'utf8');
    assert.ok(css.includes('.dc-grid'), 'DataCube\'s styles');
    assert.ok(!/url\((?!data:)|@font-face/.test(css), 'no font or image fetched by URL');
  });

  it(`the loader anywidget sends with every widget is at most ${LOADER_BUDGET.toLocaleString()} bytes`, () => {
    const size = readFileSync(runfileFromEnv('WIDGET_LOADER')).length;
    assert.ok(size <= LOADER_BUDGET, `the loader is ${size.toLocaleString()} bytes, over its budget of ${LOADER_BUDGET.toLocaleString()}`);
  });
});
