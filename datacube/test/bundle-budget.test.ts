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

/** The chunks a page's first chart fetches: its renderer's and its drawing's, with all they import. */
function firstChart(): ReadonlySet<string> {
  const fetched = new Set<string>();
  const visit = (f: string): void => {
    if (fetched.has(f)) return;
    fetched.add(f);
    for (const s of PAGE.get(f)?.statics ?? []) visit(s);
  };
  [...PAGE.keys()].filter((f) => dirname(f) === CHUNKS && (holds(f, 'src/chart-render.ts') || holds(f, 'src/chart-option.ts'))).forEach(visit);
  return fetched;
}

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
 *  278,151 on 2026-10-15, before #47's rounds 2 and 3.
 *  283,500 -> 285,500 (2026-10-18, the command program's stage 1, step 3: docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md):
 *  the screen applies its edits as grid commands (grid-commands.ts applyGridCommand, the one function Python's calls are
 *  applied by), so it carries every command's rule -- the configure command's checks and setting lists, the measures
 *  command's measureAsColumn -- though it builds only some; and #57's header clicks (no bump of their own here).
 *  Measured 284,959.
 *  285,500 -> 286,500 (2026-10-18, stage 1's step 3b and its review): the Properties window's Apply as grid commands
 *  (grid-commands.ts draftCommands, the three-way merge it replaced gone), commands told as the cube keeps them
 *  (app.ts #followCommands), and a command read from JSON field by field (its filter by its operators' operands).
 *  Measured 285,529.
 *  286,500 -> 288,500 (2026-10-18, the command program's stage 2, the roadmap run's phase 2:
 *  docs/DATACUBE_PAGE_STATE_2026_10_18.md): a page of its own has one page state and page-level undo -- page-state.ts,
 *  CubePage.sync putting the screen at a step (sheets, tiles made again, titles, specs, layouts), PageApp keeping the
 *  state and each grid's maker, the screen telling a grid and a chart added and a chart's spec. The page commands'
 *  rules stay out (applyToState lives beside them, not in the state). Measured 287,626.
 *  288,500 -> 289,500 (2026-10-18, the command program's stage 3 and its audit: docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md):
 *  DataCube's app follows a page Python serves by its commands (app/live-follow.ts: which way a move is followed, and
 *  each grid closed or frame moved refused so the page opens again), with its page name and "changed since saved".
 *  Imported, not fetched: a chunk of its own costs more than it saves (its shared code split out with it). Measured
 *  288,919.
 *  289,500 -> 290,000 (2026-10-18, chart batch 4, the roadmap run's phase 5: docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md
 *  Batch 4): the quartile aggregates (their labels and the compiler's offers for them) and the box plot's rules in
 *  chart-spec.ts, which the grid carries for its charts' panel; the box plot's drawing is chart-kinds.ts's chunk.
 *  Measured 289,651 before the phase's audit fixes.
 *  290,000 -> 291,000 (2026-10-18, chart batch 5, the roadmap run's phase 6: the design note's Batch 5): the maps' rules
 *  in chart-spec.ts (a map reads a country's code or name; a bubble map's places, `at`) and the panel's routing to
 *  them; their drawing, shapes and ECharts are chunks of their own (MAPS_BUDGET). Measured 290,341.
 *  291,000 -> 292,000 (2026-10-18, batch 3's leftovers, the roadmap run's phase 7: a Marimekko's, a waffle's and a
 *  Gantt's rules and the database's layouts for them, calc-presets.ts, in the grid's chart rules). Measured 291,127.
 *  292,000 -> 296,500 (2026-10-19, the model plan's step 3, dashboards on classes: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 *  §16): the page's filters at startup -- their reader in the page document's (page-filters.ts), the page holding them
 *  with their undo, each grid's lens on its queries (runner.ts lensed, cube.ts and app.ts setLens, its status bar's
 *  readout), a chart's click taken as one -- and a class as a source (cube-document.ts, app/sources.ts openedClass). The
 *  filter bar, the class source's rules and the page commands are chunks fetched with a page's first class grid or
 *  filter. Measured 296,324; 297,275 before the bar and the class helper were made lazy.
 *  296,500 -> 297,000 (2026-10-19, step 3's audit: a class's cube opened after the others and left out when its rows
 *  are not here, filter edits one after another, a chart's mark split between the page and its grid, Open in Grid's
 *  grid given the page's filters as they change). Measured 296,653.
 *  297,000 -> 301,500 (2026-10-20, the model plan's step 4, combining in prep: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 *  §17): every cube document reads a join (join-how.ts, cube-document.ts readJoinSource), a grid's place says the tables
 *  it reads, the Join… entry and its two stubs, the planner's tablesModel. Join…'s flow, the window, a join's relation
 *  and its opening are one chunk fetched with the first join (app/join-app.ts); with them at startup it measured about
 *  1,300 more. Most of the rest is esbuild's: a lazy module sharing startup code splits it into more chunks (18 -> 22
 *  files), each gzipped alone. Measured 301,329.
 *  301,500 -> 302,000 (2026-10-20, design §18 step 2, Snap by query: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3):
 *  each snap a copy of its own -- the page's snap target making one (app/sources.ts snapInto), modelled as a file read
 *  in is (upload.ts modelLanded), the cube's copy runner made and let go with each snap -- and remote files offered a
 *  snap. Measured 301,561.
 *  302,000 -> 303,000 (2026-10-20, §18's independent audit, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): a snapped
 *  grid's runner reads its copy wherever a query reads its live base (runner.ts rebased: a chart's, Ad Hoc mode's, a
 *  drill-through's query was refused on the copy), one snap at a time, a closed grid's copy let go as it lands, and a
 *  host's saved join opened by the host (sources.ts openedHostJoin). Measured 302,679.
 *  303,000 -> 303,500 (2026-10-20, design §18.8 step A, Snap as page state: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 *  §18.8): the page document's `copies` -- the copy each snapped grid reads -- read strictly by the page's reader at
 *  open (page-document.ts readGridCopy) and kept for the cubes a page holds as it is written (copiesFor). Measured
 *  303,359.
 *  303,500 -> 305,000 (2026-10-20, design §18.8 step B: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): the page follows
 *  its other editor's snap and live commands -- a grid reading a copy its host holds (cube.ts adopt, HostSnaps adopt,
 *  forget, handOver), the page writing down each grid's copy (page-app.ts #copies) and following them (PageApp.follow),
 *  a saved page opening live, each snapped grid said (datacube-app.ts openSaved, openedLive) -- and a frame read once as
 *  it opens (ll.show(df, snap=True): CubeApp.snapRows). Measured 304,504.
 *  305,000 -> 305,500 (2026-10-20, the page keeper's step 2.3: docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): the tab's
 *  channel to the page's keeper (app/live-channel.ts) -- the edits made on screen, each grid's and the page's, written
 *  with the page; one queue of writes and follows -- and a page of no grid written and read live (PageApp.document's
 *  `empty`, readPage's). Measured 305,133.
 *  305,500 -> 306,500 (2026-10-21, the one API program's step S1, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md: every edit
 *  of a grid and of its page a command -- the grid's new commands (an open group, Collapse All, a calculated column
 *  edited and taken out, a chart's mark), the page's (a chart's selection and own query), a saved page's tiles it
 *  could not open kept as placeholders). Measured 306,226.
 *  306,500 -> 307,000 (2026-10-21, step S1's P11: a cube opened again over a host's frames told as the page's
 *  `rebuilt` -- its rule (page-commands.ts), whether it is another (cube-document.ts cubeTaken), told as a host's page
 *  opens (datacube-app.ts openLive)). Measured 306,571.
 *  307,000 -> 307,500 (2026-10-21, step S1's audit round and step S2's G3 and V5: `rebuilt` applied by one rule over the
 *  source as it is now (cube-document.ts cubeOpenedAgain), what a reopened page tells (rebuiltCommands), a retry by its
 *  commands, a saved tree's expand level). Measured 307,284.
 *  307,500 -> 308,000 (2026-10-21, step S3's P14: the board says what the hand did, a stack and an arrangement told as
 *  their own commands -- band-board.ts LayoutMove, cube-page.ts #tellMove). Measured 307,520.
 *  308,000 -> 314,500 (2026-10-21, step S3's core, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md "the board produced by the
 *  commands": the page now applies its own edits, so it carries page-commands.ts -- the page commands' rules, their
 *  reader, followedPage, cubeOpenedAgain -- where it fetched them with another editor's first command; the plan's S3
 *  bullet names this raise). Measured 314,140.
 *  314,500 -> 315,500 (2026-10-21, step S3's audit round, the plan's F6-F17: a cube alone's board kept as its grid's own
 *  changes land, its ids held across its lifetimes, a detached chart's Update told and its query read by sync, an editing
 *  grid drawn in a band of its own, every grid's document said). Measured 314,975.
 *  315,500 -> 316,000 (2026-10-22, step S5's B8, B14, B6 and B5: setChart's refusal of a detached chart told to follow,
 *  the one "nothing to save" sentence and the one page-file rule, DataCube's, which Python says and asks too). Measured
 *  315,525.
 *  316,000 -> 319,000 (2026-10-22, step S8, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md "screen code builds no command":
 *  every gesture's meaning in one module the screen's controls and Python's grid.gesture share -- grid-gestures.ts, its
 *  refusals in words, about 2.8 KB gzipped, and page-gestures.ts, 0.8 KB -- app.ts and the menu losing their copies,
 *  about 1 KB; making the gestures load later would put the selection-off path behind an await, F93's order). Measured
 *  318,627.
 *  319,000 -> 321,000 (2026-10-23, the user's bulletproofing round: a chart's three states -- Dynamic, Pinned, Frozen --
 *  and the filter a frozen chart keeps (chart-spec.ts chartState, ChartSpec.kept, its pill's menu), a part fetched after
 *  the site updated said in words (lazily.ts), a tile added under the one it came from (bands.ts add), and the samples
 *  the start page and the join example are written down as (sample-trades.ts sampleTradesCsv, samples.ts's desk trades
 *  and countries) -- the samples are the source picker's, which every page carries). Measured 320,489.
 *  321,000 -> 322,500 (2026-10-23, the user's right-click Join ▸ and one Create ▸ in every menu: the quick joins built as
 *  the menu opens -- each grid's keys suggested (join-how.ts suggestedKeys and quickJoins, moved from the join's own
 *  chunk), the menu's entries (app/join-menu.ts) -- Create ▸'s one list (menu.ts createItems), a grid's status-bar note
 *  (app.ts note); the join itself, its window and its flow still fetched with the first join). Measured 321,971.
 *  322,500 -> 323,500 (2026-10-23, the grid's menu button gone, its right-click its menu -- Edit ▸, Tile ▸, Properties'
 *  Dimensions tab's Group Rows by and Start Ad Hoc Analysis -- and a long press as the right-click on a touch screen,
 *  an iPad's Safari firing none (ui/long-press.ts), installed with every grid and page). Measured 322,712.
 *  323,500 -> 325,500 (2026-10-24, the BI and Excel parity program's P1, docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md:
 *  the grid's Ctrl+Arrow, Ctrl+A and mouse drag selection, the rows behind a cell as a grid of their own with their
 *  caption, Expand All, the keyboard shortcuts' list and window, pinned columns' offsets, copies headed as exports
 *  are). Measured 325,228.
 *  325,500 -> 329,000 (2026-10-24, the program's P2: the Filter Values window -- a column's values with their counts, a
 *  search, (Select All) -- the date ranges relative to today, Top and Bottom N, and the menu entries that offer them).
 *  Measured 328,453.
 *  329,000 -> 330,000 (2026-10-24, P2's Show As as Excel's Show Values As: a measure at its parent's or the grand
 *  total's grain, the column editor's Parent or Grand Total, % of parent row, % running total, rank from the smallest).
 *  Measured 329,704.
 *  330,000 -> 332,500 (2026-10-24, P5's class tree, step 1: a class's grid reads its related classes -- the class tree read
 *  from the model's elements (class-tree.ts, in the grid's module: its menu offers one hop of it), the relation rewritten
 *  with each related column, the header menu's Add Related Column). Measured 332,202.
 *  332,500 -> 334,000 (2026-10-24, P5's step 2, measures declared in the model: the header menu's Add Model Measure and
 *  the grid's offer of them (class-tree.ts measuresOf, app.ts); the measures' reader travels with the class source's
 *  chunk and the steps that add one are a chunk of their own, fetched with the first (model-measure-commands.ts) --
 *  with both at startup it measured 335,041). Measured 333,553.
 *  334,000 -> 339,500 (2026-10-24, P3's conditional formats, Excel's: a data bar, an icon by the third of the range, a
 *  rule's colours, drawn by the grid as it paints a cell (conditional.ts, grid.ts), the menu's Conditional Formatting
 *  and the column's Properties' editor of them, each setting judged by configure (the commands' chunk, at startup: a
 *  grid applies commands as it opens) and drawn in the HTML export -- the editor is in the Properties chunk every grid
 *  loads at startup). Measured 339,239.
 *  339,500 -> 342,500 (2026-10-24, P3's Excel number format codes and date patterns: format-code.ts reads a code into
 *  its sections and writes a cell by it -- in the grid's formatter, which writes every cell at once, so at startup --
 *  with its colour, the Properties' Format Code field and its judging on configure and on a saved page's open).
 *  Measured 342,025.
 *  342,500 -> 343,500 (2026-10-24, P3's groups of a column's values: ValueGroups read, judged and planned (grid-commands,
 *  query), the menu's Group Values and Edit Groups, the window's opening -- the window itself a chunk of its own,
 *  fetched with the first one opened (ui/group-values.ts)). Measured 342,844.
 *  343,500 -> 344,500 (2026-10-24, P2's aggregates of two columns and Count: six aggregates -- their compiler facts per
 *  type in the generated offer table (offers.ts reads it at startup), the SECOND_COLUMN table every rule asks, their
 *  labels -- the Properties' second column named by its role, and Count's gesture and menu). Measured 344,036.
 *  344,500 -> 346,000 (2026-10-24, P4's text tiles: a text view read, written and merged (page-document, page-commands,
 *  page-rebase), the board's tile placed, synced and exported, Create ▸ Text Box, its styles -- the words' reader, drawing
 *  and editor a chunk of their own, fetched with the first text tile drawn (page-text.ts)). Measured 345,834.
 *  346,000 -> 347,000 (2026-10-24, the user on an iPad: a long press's menu opened its first submenu by itself, and
 *  columns could not be dragged or resized -- a long press's menu says so to the stylesheet until the next touch, waits
 *  on what the browser drags for the finger to lift, and is never a resize grip's (ui/long-press.ts); Count moved from the
 *  right-click menu to Column Properties' buttons). Measured 346,360. */
const BUDGET = 347_000;

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
 *  refused as a sort). Measured 270,114.
 *  271,000 -> 272,500 (2026-10-18, the command program's stage 1, step 3: as the grid's -- the screen applies its edits
 *  as grid commands, by the one function Python's calls are applied by). Measured 272,184.
 *  272,500 -> 273,500 (2026-10-18, stage 1's step 3b and its review: as the grid's). Measured 272,761.
 *  273,500 -> 275,500 (2026-10-18, the command program's stage 2: page-level undo, as the grid's). Measured 274,868.
 *  275,500 -> 278,500 (2026-10-18, the command program's stage 3, the roadmap run's phase 3:
 *  docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): this is the page Python shows, which now follows Python's changes by
 *  their commands -- the live page's changes (python-host.ts, the grid command reader with them) and the app applying
 *  them (datacube-app.ts followLive). The page commands' rules come only with the first commands (a chunk of their
 *  own: page-ids.ts keeps the screen's ids apart from them). Measured 277,563.
 *  278,500 -> 279,000 (2026-10-18, chart batch 4: the quartiles and the box plot's rules -- the grid's note of the same
 *  date). Measured 278,735 before the phase's audit fixes.
 *  279,000 -> 280,000 (2026-10-18, chart batch 5: the maps' rules -- the grid's note of the same date). Measured
 *  279,416.
 *  280,000 -> 281,000 (2026-10-18, batch 3's leftovers -- the grid's note of the same date). Measured 280,228.
 *  281,000 -> 285,000 (2026-10-19, the model plan's step 3: the page's filters and a class as a source -- the grid's note
 *  of the same date). Measured 284,804.
 *  285,000 -> 285,500 (2026-10-19, step 3's audit -- the grid's note of the same date). Measured 285,174.
 *  285,500 -> 290,000 (2026-10-20, the model plan's step 4, combining in prep -- the grid's note of the same date).
 *  Measured 289,795.
 *  290,000 -> 290,500 (2026-10-20, design §18 step 2, Snap by query -- the grid's note of the same date). Measured
 *  290,349.
 *  290,500 -> 291,000 (2026-10-20, design §18 step 4, a class of any base: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 *  §18.3): what a class is made over read at startup, before its window is fetched, so a grid that cannot be one is
 *  refused in words there (query.ts classBasis, tablesOf: every table a grid's rows read, a join's each), and the page's
 *  model keeping a list of tables per grid. Measured 290,648.
 *  291,000 -> 292,000 (2026-10-20, §18's independent audit -- the grid's note of the same date; this page also asks
 *  Python to open a page.join grid, opened.json). Measured 291,599.
 *  292,000 -> 292,500 (2026-10-20, design §18.8 step A -- the grid's note of the same date). Measured 292,296.
 *  292,500 -> 294,000 (2026-10-20, design §18.8 step B -- the grid's note of the same date; this page also asks Python
 *  for a copy it holds, held). Measured 293,533.
 *  294,000 -> 294,500 (2026-10-20, the page keeper's step 2.3 -- the grid's note of the same date). Measured 294,287.
 *  294,500 -> 295,500 (2026-10-21, the one API program's step S1, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md: every edit
 *  of a grid a command -- each calculated column the screen makes read whole (C1), a group opened and Collapse All (G1,
 *  G2), a calculated column edited and taken out (G4, G5) -- read, applied and told by grid-commands.ts, which the page
 *  loads at start). Measured 295,261.
 *  295,500 -> 296,000 (2026-10-21, step S1's P1-P3: the page's rename, a grid's own Snap and Go Live told as page
 *  commands, its copy settled only once its move holds). Measured 295,526.
 *  296,000 -> 296,500 (2026-10-21, step S1's P8-P9: a detached chart's own query told, a saved page's tiles it could not
 *  open kept as placeholders -- the grid's note of the same date). Measured 296,329.
 *  296,500 -> 297,000 (2026-10-21, step S1's P11 -- the grid's note of the same date). Measured 296,697.
 *  297,000 -> 297,500 (2026-10-21, step S2's G3 and V5: configure's tree rule, a saved tree's expand level and closed
 *  groups written and read). Measured 297,040.
 *  297,500 -> 298,000 (2026-10-21, step S3's P14 -- the grid's note of the same date; step S2's V1 V2, followedPage on
 *  every path a cube changes, came in under the last raise). Measured 297,661.
 *  298,000 -> 304,500 (2026-10-21, step S3's core -- the grid's note of the same date). Measured 304,314.
 *  304,500 -> 305,000 (2026-10-21, step S3's G8 and P7: a step's selection put back by its grid's own refilter, each
 *  sheet's layout as the page has it kept apart from the board's drawing). Measured 304,513.
 *  305,000 -> 305,500 (2026-10-21, step S3's audit round -- the grid's note of the same date). Measured 305,177.
 *  305,500 -> 306,000 (2026-10-21, step S4, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md "the host applies the commands":
 *  the page Python serves written as its edits or, undone or redone, whole (a restore), the ids it has held read with it,
 *  and Open..., in place and Blank Page leaving it for a page of the tab's own (D1), said on a blank page's card).
 *  Measured 305,535.
 *  306,000 -> 306,500 (2026-10-22, step S5's B8, B14, B6, B5, Z18, B4 and B13 -- the grid's note of the same date, and
 *  the saved page it is kept with the page and told (savedAs), so either editor saves over one record). Measured
 *  306,214.
 *  306,500 -> 310,000 (2026-10-22, step S8 -- the grid's note of the same date). Measured 309,694.
 *  310,000 -> 311,000 (2026-10-23, the bulletproofing round -- the grid's note of the same date). Measured 310,535.
 *  311,000 -> 312,500 (2026-10-23, the right-click Join ▸ and Create ▸ -- the grid's note of the same date). Measured
 *  312,050.
 *  312,500 -> 313,500 (2026-10-23, the grid's menu button gone and a long press as the right-click -- the grid's note of
 *  the same date). Measured 312,770.
 *  313,500 -> 314,000 (2026-10-23, KPIs as stat tiles, with Show Grid on every chart kind (PR #69) merged beside them:
 *  the tile's lines and the headline format the grid's formatter writes -- the notebook widget's note of the same
 *  date). Measured 313,533.
 *  314,000 -> 315,500 (2026-10-24, the BI and Excel parity program's P1 -- the grid's note of the same date). Measured
 *  315,361.
 *  315,500 -> 319,000 (2026-10-24, the program's P2 -- the grid's note of the same date). Measured 318,756.
 *  319,000 -> 320,500 (2026-10-24, P2's Show As -- the grid's note of the same date). Measured 320,130.
 *  320,500 -> 323,000 (2026-10-24, P5's class tree, step 1 -- the grid's note of the same date). Measured 322,762.
 *  323,000 -> 325,000 (2026-10-24, P5's step 2, model measures -- the grid's note of the same date). Measured 324,518.
 *  325,000 -> 330,000 (2026-10-24, P3's conditional formats -- the grid's note of the same date). Measured 329,866.
 *  330,000 -> 333,000 (2026-10-24, P3's format codes -- the grid's note of the same date). Measured 332,665.
 *  333,000 -> 334,500 (2026-10-24, P3's groups of values -- the grid's note of the same date). Measured 333,810.
 *  334,500 -> 335,500 (2026-10-24, P2's aggregates of two columns and Count -- the grid's note of the same date).
 *  Measured 334,962.
 *  335,500 -> 337,000 (2026-10-24, P4's text tiles -- the grid's note of the same date). Measured 336,754.
 *  337,000 -> 338,000 (2026-10-24, a long press on an iPad and Count in Column Properties -- the grid's note of the same
 *  date). Measured 337,265. */
const ENGINE_BUDGET = 338_000;

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
 *  tree 446,304.
 *  301,000 -> 303,000 (2026-10-18, chart batch 5, the roadmap run's phase 6): the ECharts code a first chart shares with
 *  the maps' chunk is a file of its own (three files compress less well than two), and the drawing grew with batches 4
 *  and 5's words; the measure counts only the chunks a first chart fetches (one only batch 3's kinds and the maps share
 *  is theirs). Measured 302,497. */
const CHART_BUDGET = 303_000;

/** Batch 3's kinds (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): their drawing (chart-kinds.ts) and the rarer ECharts
 *  charts they draw with (chart-render-kinds.ts), fetched with the first chart of one of them, beyond the first chart's
 *  own. */
// a chunk's file name, whichever separator the platform's paths use (Windows': backslashes)
const isKinds = (file: string): boolean => /[\\/]chart-(?:render-)?kinds-[^\\/]+\.js$/.test(file);
/** Set 2026-10-14 at its first measure (batch 3 step 1), 92,574: the drawing 7,823 and the charts 84,751, with room for
 *  batch 3's later kinds (small multiples, a bubble).
 *  100,000 -> 66,000 (2026-10-15, the bundles minified, their names kept: datacube/BUILD.bazel _BUNDLES; the user's
 *  "does minify buy anything if already zipped?" -- it does, about a third): batch 3's kinds all in. Measured 62,426;
 *  unminified, the same tree 95,848.
 *  66,000 -> 68,000 (2026-10-18, the roadmap run's phases 5-7: the box plot, the click's period end on these kinds,
 *  batch 3's leftovers -- a Marimekko, a waffle, a Gantt -- and ECharts code these kinds share only with the maps,
 *  counted here since phase 6's audit). Measured 66,807. */
const KINDS_BUDGET = 68_000;

const isMaps = (file: string): boolean => /[\\/]chart-(?:render-)?maps-[^\\/]+\.js$/.test(file);
/** Set 2026-10-18 at its first measure (chart batch 5, the roadmap run's phase 6), 75,526: the drawing with the world's
 *  countries (Natural Earth 1:110m, prepared by tools/maps/prepare.py: about 60,000 of it) and ECharts' map chart and
 *  geo component, a chunk each. */
const MAPS_BUDGET = 78_000;

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
 *  281,797 (all the chunks 410,466).
 *  290,000 -> 291,000 (2026-10-18, the command program's stage 1: the screen applies its edits as grid commands, the
 *  Properties window's Apply among them -- the grid's notes of the same date). Measured 290,232.
 *  291,000 -> 293,000 (2026-10-18, the command program's stage 2: page-level undo -- the grid's notes of the same date).
 *  Measured 292,353.
 *  293,000 -> 296,000 (2026-10-18, the command program's stage 3: a notebook's page follows Python's changes by their
 *  commands -- the engine page's note of the same date). Measured 295,033.
 *  296,000 -> 296,500 (2026-10-18, chart batch 4: the quartiles and the box plot's rules -- the grid's note of the same
 *  date). Measured 296,208 before the phase's audit fixes.
 *  296,500 -> 297,500 (2026-10-18, chart batch 5: the maps' rules -- the grid's note of the same date). Measured
 *  296,883.
 *  297,500 -> 298,500 (2026-10-18, batch 3's leftovers -- the grid's note of the same date). Measured 297,692.
 *  298,500 -> 302,600 (2026-10-19, the model plan's step 3: the page's filters and a class as a source -- the grid's
 *  note of the same date). Measured 302,450.
 *  302,600 -> 303,000 (2026-10-19, step 3's audit -- the grid's note of the same date). Measured 302,803.
 *  303,000 -> 307,800 (2026-10-20, the model plan's step 4, combining in prep -- the grid's note of the same date).
 *  Measured 307,630.
 *  307,800 -> 308,300 (2026-10-20, design §18 step 2, Snap by query -- the grid's note of the same date). Measured
 *  308,163.
 *  308,300 -> 308,800 (2026-10-20, design §18 step 4, a class of any base -- the engine page's note of the same date).
 *  Measured 308,467.
 *  308,800 -> 309,800 (2026-10-20, §18's independent audit -- the engine page's note of the same date). Measured
 *  309,429.
 *  309,800 -> 310,300 (2026-10-20, design §18.8 step A -- the grid's note of the same date). Measured 310,121.
 *  310,300 -> 311,800 (2026-10-20, design §18.8 step B -- the engine page's note of the same date). Measured 311,364.
 *  311,800 -> 312,500 (2026-10-20, the page keeper's step 2.3 -- the grid's note of the same date). Measured 312,118.
 *  312,500 -> 313,500 (2026-10-21, the one API program's step S1 -- the engine page's note of the same date). Measured
 *  313,083.
 *  313,500 -> 314,500 (2026-10-21, step S1's P1-P9 -- the grid's note of the same date). Measured 314,150.
 *  314,500 -> 315,000 (2026-10-21, step S1's P11 -- the grid's note of the same date). Measured 314,514.
 *  315,000 -> 315,500 (2026-10-21, step S1's audit round and step S2's G3 and V5 -- the grid's note of the same date).
 *  Measured 315,233.
 *  315,500 -> 316,000 (2026-10-21, step S3's P14 and step S2's audit round: a stack and an arrangement told as their own
 *  commands, a selection judged when its filter moves -- the grid's note of the same date). Measured 315,526.
 *  316,000 -> 322,500 (2026-10-21, step S3's core -- the grid's note of the same date). Measured 322,107.
 *  322,500 -> 323,500 (2026-10-21, step S3's audit round -- the grid's note of the same date). Measured 322,960.
 *  323,500 -> 324,000 (2026-10-22, step S5's B8, B14 and B6: setChart's refusal of a detached chart told to follow, and
 *  the one "nothing to save" sentence, DataCube's, which Python says too). Measured 323,533.
 *  324,000 -> 324,500 (2026-10-22, step S5's B5, Z18, B4 and B13 -- the engine page's note of the same date). Measured
 *  324,039.
 *  324,500 -> 328,000 (2026-10-22, step S8 -- the grid's note of the same date). Measured 327,491.
 *  328,000 -> 329,500 (2026-10-23, the bulletproofing round -- the grid's note of the same date). Measured 328,843.
 *  329,500 -> 331,000 (2026-10-23, the right-click Join ▸ and Create ▸ -- the grid's note of the same date). Measured
 *  330,421.
 *  331,000 -> 332,000 (2026-10-23, the grid's menu button gone and a long press as the right-click -- the grid's note of
 *  the same date). Measured 331,244.
 *  332,000 -> 332,500 (2026-10-23, KPIs as stat tiles: the tile's headline, meter and change lines in the chart panel,
 *  their styles, and the headline format the grid's formatter writes -- the drawing itself is the chart chunk's).
 *  Measured 332,006.
 *  332,500 -> 334,500 (2026-10-24, the BI and Excel parity program's P1 -- the grid's note of the same date). Measured
 *  334,149.
 *  334,500 -> 338,000 (2026-10-24, the program's P2 -- the grid's note of the same date). Measured 337,670.
 *  338,000 -> 339,500 (2026-10-24, P2's Show As -- the grid's note of the same date). Measured 339,031.
 *  339,500 -> 342,000 (2026-10-24, P5's class tree, step 1 -- the grid's note of the same date). Measured 341,696.
 *  342,000 -> 344,000 (2026-10-24, P5's step 2, model measures -- the grid's note of the same date). Measured 343,433.
 *  344,000 -> 349,000 (2026-10-24, P3's conditional formats -- the grid's note of the same date). Measured 348,827.
 *  349,000 -> 352,000 (2026-10-24, P3's format codes -- the grid's note of the same date). Measured 351,631.
 *  352,000 -> 353,500 (2026-10-24, P3's groups of values -- the grid's note of the same date). Measured 352,770.
 *  353,500 -> 354,500 (2026-10-24, P2's aggregates of two columns and Count -- the grid's note of the same date).
 *  Measured 353,929.
 *  354,500 -> 356,000 (2026-10-24, P4's text tiles -- the grid's note of the same date). Measured 355,922.
 *  356,000 -> 357,000 (2026-10-24, a long press on an iPad and Count in Column Properties -- the grid's note of the same
 *  date). Measured 356,493. */
const WIDGET_BUDGET = 357_000;
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
    // ECharts' chunks and the drawing's that a first chart fetches (firstChart): a chunk only batch 3's kinds and the
    // maps share is theirs, counted with the kinds
    const fetched = firstChart();
    const chart = chunksOf().filter((f) => (holds(f, 'echarts') || holds(f, 'src/chart-option.ts')) && !isKinds(f) && !isMaps(f)
      && fetched.has(f));
    assert.ok(chart.length >= 1, 'the chart\'s chunks are found');
    const size = chart.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= CHART_BUDGET, `a first chart fetches ${size.toLocaleString()} bytes gzipped, over its budget of ${CHART_BUDGET.toLocaleString()}`);
  });

  it(`batch 3's kinds are chunks of their own, fetched with the first of them: at most ${KINDS_BUDGET.toLocaleString()} bytes more`, () => {
    for (const f of startup()) assert.ok(!isKinds(f) && !holds(f, 'src/chart-kinds.ts'), `${f} carries batch 3's kinds`);
    const kinds = chunksOf().filter(isKinds);
    assert.equal(kinds.length, 2, 'the kinds\' drawing and their ECharts charts, a chunk each');
    // with them, the ECharts code they share only with the maps' chunks: fetched with the first of either, counted here
    // (the phase 6 audit: a chunk in no budget)
    const shared = chunksOf().filter((f) => holds(f, 'echarts') && !isKinds(f) && !isMaps(f) && !firstChart().has(f));
    for (const f of shared) {
      assert.ok([...PAGE].every(([importer, out]) => !out.statics.includes(f) || isKinds(importer) || isMaps(importer)),
        `${f} holds ECharts code a first chart does not fetch, and is fetched by something other than the kinds or the maps`);
    }
    const size = [...kinds, ...shared].reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= KINDS_BUDGET, `batch 3's kinds fetch ${size.toLocaleString()} bytes gzipped, over their budget of ${KINDS_BUDGET.toLocaleString()}`);
  });

  it(`batch 5's maps are chunks of their own, fetched with the first of them: at most ${MAPS_BUDGET.toLocaleString()} bytes more`, () => {
    for (const f of startup()) assert.ok(!isMaps(f) && !holds(f, 'src/chart-maps.ts') && !holds(f, 'src/maps/world-110m.ts'), `${f} carries the maps`);
    const maps = chunksOf().filter(isMaps);
    assert.equal(maps.length, 2, 'the maps\' drawing with their shapes, and their ECharts chart, a chunk each');
    // the shapes come with the drawing alone: in no chunk a first chart or batch 3's kinds fetch
    assert.deepEqual(chunksOf().filter((f) => holds(f, 'src/maps/world-110m.ts')).map((f) => isMaps(f)), [true]);
    const size = maps.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= MAPS_BUDGET, `batch 5's maps fetch ${size.toLocaleString()} bytes gzipped, over their budget of ${MAPS_BUDGET.toLocaleString()}`);
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
