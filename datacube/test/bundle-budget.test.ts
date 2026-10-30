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
 *  right-click menu to Column Properties' buttons). Measured 346,360.
 *  347,000 -> 355,500 (2026-10-24, main's D19 merged into this branch: its own steps there, 323,500 -> 332,000 ->
 *  333,500 -- the grid's and the page's total diffs, the converge on the keeper's page, the page hash, its audits'
 *  rounds, each signed off by the user (D9) -- now on top of the parity program's). Measured 355,159.
 *  355,500 -> 356,500 (2026-10-25, the flow mode's M1 step 1, docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1: a grid over
 *  another grid's rows read and written (cube-document.ts RowsSource), its live input resolved against its page, a page
 *  whose link goes nowhere or round refused as it is read, its grids opened after what they read (live-inputs.ts) --
 *  every page read asks it, so it is no chunk of its own; the canvas will be). Measured 356,085.
 *  356,500 -> 357,000 (2026-10-25, the flow mode's M1 step 2: a box opened in the tab -- sources.ts reopened's arm, where
 *  it is opened (by the host on its page, else here), each grid's start kept for a box after it, what a grid reads
 *  resolved as it opened (GridPlace.reads); the opening itself is the join's chunk's (app/join-sources.ts openedRows)).
 *  Measured 356,620.
 *  357,000 -> 363,500 (2026-10-25, the flow mode's M1 steps 3-4, docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4: a sheet's
 *  layout a board's or a flow's -- read and written (page-document.ts), the flow's commands and their rules
 *  (page-commands.ts addFlow, placeBox, layout/flow.ts), the rebase's arms -- and the page's flow sheets: the board's
 *  boxes, a document each until shown, the one shown made and closed (page/cube-page.ts), the flow board's state
 *  (layout/flow-board.ts), Create ▸ Flow, the host's box opener and palette tools. Every page read and every board asks
 *  them, so they load with the page; the canvas, its palette and what a box says it does are a chunk of their own
 *  (layout/flow-canvas.ts, flow-boxes.ts), fetched with the first flow). Measured 363,119.
 *  363,500 -> 364,000 (2026-10-25, the flow mode's M1 steps 5 and after: a grid's windows that set it docked where its
 *  host docks them (app.ts dock, openStep), a box's settings opened with it, a file dropped onto a flow, a box refused
 *  drawn so -- the drawing itself the canvas's chunk still). Measured 363,615.
 *  364,000 -> 365,000 (2026-10-25, the flow mode's M2: setInput -- a box linked, frozen, a removal's freeze undone -- its
 *  sides read off a source (cube-document.ts inputOn), a join's inputs live, a tool put on a line; the canvas's lines and
 *  ports its own chunk still). Measured 364,792.
 *  365,000 -> 366,000 (2026-10-25, the flow mode's M2 step 3 and M3's first tools: setJoin, a Join box's settings
 *  routed to its window, a box passing on the columns its input gains; its window and its words a chunk of their own
 *  (join-flow.ts, join-words.ts), the tools' words the canvas's). Measured 365,559.
 *  366,000 -> 367,500 (2026-10-25, the flow mode's M7 first steps: Tidy Up's rule and its gesture, which Python's
 *  flow.tidy asks too (page-gestures.ts tidyCommands, layout/flow.ts arranged), a box duplicated; a box's menu and the
 *  palette's find the canvas's chunk). Measured 366,976.
 *  367,500 -> 368,000 (2026-10-25, the flow mode's Sample: a grid's Sample window opened by its step, docked; the form
 *  a chunk of its own, ui/sample-form.ts). Measured 367,587.
 *  368,000 -> 368,500 (2026-10-25, the flow mode's comments: a text tile on a flow placed as a box is, drawn as a note,
 *  left alone by Tidy Up and Run; the note's drawing the canvas's chunk). Measured 368,285.
 *  368,500 -> 369,500 (2026-10-25, the flow mode's DateTime: a period column read off its expression to say a box's
 *  tool (calc-presets.ts grainOf), the window that sets it opened by its step, docked; the form a chunk of its own,
 *  ui/date-form.ts). Measured 369,121.
 *  369,500 -> 370,000 (2026-10-25, the flow mode's Text Input: the palette's tool and the step that adds an Input over the
 *  table typed in; the window a chunk of its own, ui/text-input-dialog.ts). Measured 369,521.
 *  370,000 -> 370,500 (2026-10-25, the flow mode's text tools -- Text To Columns, RegEx, Data Cleansing: the three windows
 *  opened by their steps and docked; their columns' rule (text-tool-columns.ts), the windows (ui/text-tool-form.ts) and
 *  the box words (text-tools.ts) chunks of their own, none fetched at startup). Measured 370,114.
 *  370,500 -> 371,500 (2026-10-25, the flow mode's Unique and its many boxes at once: a box's rows the columns it shows
 *  (live-inputs.ts gridInput's omit, join-open.ts inputRows), and the page's copy, paste, move and removal of several
 *  boxes as one step (page/cube-page.ts); picking them, the band and the group's drag the canvas's chunk). Measured
 *  370,931.
 *  371,500 -> 372,500 (2026-10-25, the flow mode's containers: a comment framed on a flow -- the flow's frame rules
 *  (layout/flow.ts frameProblem, framedBy), placeBox's frame (page-commands.ts) and its reading (page-document.ts), the
 *  page's Add, Size, Fold and Move Container (page/cube-page.ts); their drawing the canvas's chunk). Measured 372,152.
 *  372,500 -> 374,000 (2026-10-25, the flow mode's Output: a box's output and its rule (layout/flow.ts setOutput), the
 *  setOutput command and its reading (page-commands.ts, page-document.ts), the page's Write now and Run writing it
 *  (page/cube-page.ts), its rows streamed to CSV (app.ts writeOutput, export.ts toCsv imported as it was, not fetched
 *  again: fetching a module the page has split its shared code into two more startup files, +2,200 bytes, measured and
 *  undone). Measured 373,513.
 *  374,000 -> 375,000 (2026-10-25, the flow mode's Multi-Field Formula: the palette's tool, its columns read back off
 *  their shapes to say a box (multi-field.ts multiFieldOf), the step that opens its window (app.ts); its window and its
 *  commands chunks of their own, ui/multi-field-form.ts and multi-field-columns.ts, fetched with the first). Measured
 *  374,121.
 *  375,000 -> 376,000 (2026-10-25, the flow mode's Transpose, Select and Sample's other ways: their windows' openers
 *  (app.ts), and what a box hands on -- its columns shown, a Summarize's keys and aggregates, a Select's renames -- one
 *  rule now (box-columns.ts) that live-inputs.ts reads at startup; the rules and the windows chunks of their own).
 *  Measured 375,039.
 *  376,000 -> 376,500 (2026-10-25, the flow mode's Summarize window: its opener (app.ts), and the read of a grid's
 *  columns as a Summarize has them beside the offers it reads (grid-edits.ts summaryOf, summaryActions); the commands'
 *  rule and the window chunks of their own -- the rule reading the snapshot alone, as one that imported the grid's edits
 *  split their startup chunk in two, +1,362 bytes, measured and undone). Measured 376,147.
 *  376,500 -> 377,000 (2026-10-25, the flow mode's Join L and R: a live input naming a Join's port resolved to its side's
 *  rows matched against the other's (live-inputs.ts gridInput, join-how.ts unmatchedOn), read and written with a copy
 *  (cube-document.ts JoinInput.unmatched), a join set again held to its readers (page-commands.ts setJoin) -- read when a
 *  page is, so at startup; the anti-joins themselves join-open.ts's, and the ports' drawing the canvas's chunk). Measured
 *  376,805.
 *  377,000 -> 377,500 (2026-10-25, the flow mode's Find Replace: its window's opener (app.ts); the rule, the window and a
 *  box's words chunks of their own, find-replace.ts, ui/find-replace-form.ts and the canvas's). Measured 377,079.
 *  377,500 -> 378,000 (2026-10-25, the flow mode's Count Records: its step, applied as its box is made (app.ts), its
 *  rule a chunk of its own, count-records.ts). Measured 377,577.
 *  378,000 -> 379,000 (2026-10-25, the flow mode's Union of any number: a join's inputs after its right read, written
 *  and walked at startup (cube-document.ts JoinSource.more, joinInputs; live-inputs.ts; page-commands.ts setJoin's more,
 *  setInput's next side; page-between.ts, page-rebase.ts), and a Union's input taken off (page-gestures.ts
 *  takeOffCommands, flow.takeOff); its ports and settings the canvas's chunk). Measured 378,619.
 *  379,000 -> 380,000 (2026-10-25, the flow mode's Code view and Date Filter: a grid's last query printed (app.ts code,
 *  copyText); the Date Filter's rule (date-ranges.ts datesBetweenConditions, grid-gestures.ts filter.dates), its read-back
 *  (page-filters.ts datesKept: beside a Filter's words, so no startup file of its own -- in its own module, read by the
 *  canvas's chunk, it was one, 1,413 bytes) and its window's opener (app.ts); the window a chunk of its own). Measured
 *  379,602.
 *  380,000 -> 381,000 (2026-10-25, the flow mode's Test and a Test that stops a run: its opener (app.ts #openTest), a
 *  grid's first row read and a run writing its Outputs after every box, held back by a failing stopping Test
 *  (page/cube-page.ts firstRowOf, #runFlow), the box's mode read, written and set (page-document.ts, layout/flow.ts
 *  setStops, page-commands.ts setStops); its rule, window and marks chunks of their own; no startup file added, measured).
 *  Measured 380,296.
 *  381,000 -> 382,000 (2026-10-25, the flow mode's Cross Tab with fixed columns, FL2: a pivot's values pinned -- its
 *  command and rule (grid-commands.ts pinPivot, grid-edits.ts pinPivot, a pivot set again taking another's off), the
 *  gesture that types them (grid-gestures.ts pivot.pin), its undo (grid-between.ts), a pinned pivot's rows for the box
 *  after it (query.ts crossTabRows) and Load values (app.ts pinPivotValues, page/cube-page.ts onPinValues); the strip
 *  that asks it the canvas's chunk; no startup file added, measured). Measured 381,541.
 *  382,000 -> 383,000 (2026-10-25, the flow mode's Cross Tab missing values (FL3) and Custom Filter (FL12): the values a
 *  pin lacks asked as its rows land and awaited by a run (query.ts unpinnedValuesLambda, app.ts lackingPivotValues,
 *  page/cube-page.ts #askLacking, layout/flow-board.ts lacking), a draft's columns typed with nothing run (cube.ts
 *  typed) and the Custom Filter's opener (app.ts); its rule, its window and the canvas's marks chunks of their own; no
 *  startup file added, measured). Measured 382,146.
 *  383,000 -> 385,000 (2026-10-26, the flow mode's Pure box, FL12: a box's rows a function of its input's, in Pure -- its
 *  source's function read and judged with the page (cube-document.ts RowsSource.pure, pure-box.ts pureProblem), set and
 *  read as a page command (page-commands.ts setPure; page-between.ts, page-rebase.ts), its window's opener and the rule
 *  it shares with headless (app.ts #openPure, pure-box.ts pureFrom), a relation typed with nothing run (cube.ts
 *  relationColumns), and pure-protocol's top-down rewrite; the window a chunk of its own; no startup file added, measured.
 *  Not tried: all but the reader's check in a lazy module of its own -- some 600 bytes by estimate, and a lazy module
 *  importing that check would split it into a startup file of its own, as Summarize's rule split one). Measured 384,564.
 *  385,000 -> 395,500 (2026-10-26, main merged into the flow branch: its own steps there, 355,500 -> 356,000 -> 357,500 ->
 *  358,500 -> 359,500 -> 360,500 -> 361,500 -> 365,000 -> 365,500 -- the UI waves the user asked for (a measure's numbers
 *  right-aligned, the page's ≡ by purpose), the one tile header, the grid's status bar, the text box as a Markdown editor
 *  and its files (setTextFiles), the menus' one design language, the chrome in one icon set and a chart's options as a
 *  window beside it, each in main's history with its own measure -- now on top of the flow mode's; no startup file
 *  added, measured). Measured 395,242.
 *  395,500 -> 406,000 (2026-10-26, the timeseries branch, PR #74, merged into the flow branch: its own steps there,
 *  365,500 -> 375,000 -> 376,000 -- the timeseries tile and the timeseries review's G4 (a removed grid's cube kept while a
 *  timeseries reads it), then the one rule of the cubes a page holds and a timeseries' states, each in that branch's
 *  history with its own measure (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md) -- now on top of the flow mode's; no
 *  startup file added, measured). Measured 405,574.
 *  406,000 -> 408,500 (2026-10-26, the flow mode review's fixes, docs/DATACUBE_FLOW_MODE_REVIEW_2026_10_26.md: a box's
 *  rows handed on in its sorts' order and the one Output writer (query.ts handedOnLambda, flow-output.ts, fetched with
 *  the page: lazily, it was a chunk that split the startup files), a Cross Tab's values read once (query.ts
 *  pivotValuesOf), the page gestures Python shares (flow.place, flow.duplicate), a stopping box never judged
 *  (flow-board.ts runStoppers); the checks' readers kept out (setStops reads a Test's shape alone); no startup file
 *  added -- 33 as before, measured by esbuild's record). Measured 408,032.
 *  408,500 -> 409,500 (2026-10-26, the flow mode's roadmap: C13, a join's one of theirs per key (join-how.ts's reader of
 *  `once`), a Profile's top values (layout/flow-canvas.ts #drawProfile), and a Cross Tab's Add to the cast
 *  (flow-canvas.ts #paintPin, app.ts addPivotValues): no startup file added). Measured 408,617.
 *  409,500 -> 410,500 (2026-10-26, the flow mode's Cache, FL10: a box that copies its rows when it runs -- its copy
 *  handed to the page as the box closes and released as its chain changes (cube.ts handOver, app.ts handOverCopy and
 *  snapNow, page/cube-page.ts #cacheKept, cacheCopies), a snapped input opened by its copy (live-inputs.ts copyOf,
 *  join-open.ts openedInputLeaves), its command read and written (page-commands.ts setCache, page-document.ts, layout/
 *  flow.ts setCache), and Add to the cast's gesture (grid-gestures.ts pivot.addValues, the ledger's L11: the screen
 *  decided the longer pin); the checkbox the canvas's chunk; no startup file added, measured). Measured 409,602.
 *  410,500 -> 411,500 (2026-10-26, the flow mode's smaller tool gaps, #74: an Imputation by its median (snapshot.ts's
 *  window function, query.ts's arm), a DateTime written as text (date-tools.ts dateFormat), a Fuzzy Match's score
 *  (join-how.ts's reader and scoreRefusal), a RegEx's Tokenize (text-tools.ts's reader), an Oversample (row-sample.ts's
 *  rule and its read-back), and a cached box with nothing to copy said so (app.ts heldHere): no startup file added,
 *  measured). Measured 410,882.
 *  411,500 -> 413,000 (2026-10-26, the flow mode's Output into a table, M6/FL13: an Output's table kind, its rule and its
 *  one reader (layout/flow.ts outputProblem, readOutput: the page's and its command's), the grid's write of one and the
 *  tab's writer handed to it (app.ts writeOutput, app/datacube-app.ts tableWriter), the planner's execution plan
 *  (lite-planner.ts executionPlan) and the engine's hold (duckdb.ts exclusive); the writer and the plan runner a chunk of
 *  their own, fetched with the first table written; no startup file added, measured). Measured 412,379.
 *  413,000 -> 414,500 (2026-10-26, the flow mode's Dynamic Replace, #74: a box's read-back and words (dynamic-replace.ts,
 *  the canvas's, as multi-field.ts is), the grid's window opener and its rows read for another box (app.ts
 *  #openDynamicReplace, handedOnRows; flow-output.ts handedOnTable), the page's read of a rules box (page/cube-page.ts
 *  #readRules); its rules and commands and its window chunks of their own, fetched with the first). Measured 414,089.
 *  414,500 -> 417,500 (2026-10-26, the flow mode's M8, a page's parameters: their rule, read and judged where the page's
 *  commands and document are (page-parameters.ts: setParameter, readPage), their lens on every grid's queries and their
 *  binding where a source is resolved (page-app.ts #lensOf, live-inputs.ts resolvedSource), Create ▸ Parameter and the
 *  flow's palette's; the parameters bar a chunk of its own, fetched with the first parameter). Measured 416,977.
 *  417,500 -> 420,500 (2026-10-26, the flow mode's M8, standard macros: their rule, read where the page's document and
 *  commands are (macros.ts: setMacro, readPage), a box's Save as Macro form and the palette's Macro menu (layout/
 *  flow-canvas.ts #macroForm, #macroMenu), the page's save -- each box read in turn -- and use (page/cube-page.ts
 *  #saveMacro, #useMacro), a grid's own steps as a function (app.ts ownFunction)). Measured 419,896.
 *  420,500 -> 421,500 (2026-10-26, the flow mode's M8, Dynamic Input: its rule, where the page's commands and gestures
 *  are (dynamic-input.ts pickCommands: page-app.ts pickInput, page-gestures.ts 'flow.pick' -- Python's grid.pick), an
 *  append's pick read and said (join-how.ts readJoinHow, join-words.ts); its settings' Keep the rows of the canvas's
 *  chunk (layout/flow-canvas.ts #paintPick)). Measured 420,901.
 *  421,500 -> 422,500 (2026-10-26, its audit: a step's inverse carrying the page's parameters and macros and a box's
 *  cache (page-between.ts byName, setCache), a grid's pick written as the page holds it (join-how.ts heldHow), a
 *  parameter's type read by its family (engine-client/src/types.ts, page-parameters.ts)). Measured 421,550.
 *  422,500 -> 423,500 (2026-10-26, the flow mode's M8, Block Until Done, Message and the run history: a box's blocking
 *  and its message, read where the page's commands and document are (layout/flow.ts readMessage: setBlocks, setMessage,
 *  readPage), a run in stages with its Messages said (page/cube-page.ts #runFlow: runStages, messageSaid), its log kept
 *  (layout/flow-board.ts logs); their settings and the log's history the canvas's chunk). Measured 422,951.
 *  423,500 -> 425,000 (2026-10-26, the flow mode's roadmap audit, #77: a join's sides bound with the page's parameters
 *  and a lambda's own shadowing them (page-parameters.ts boundIn, parametersRead), a cache's chain keyed by what its box
 *  is made of and let go live (layout/flow-board.ts chain, page/cube-page.ts staleCaches, liveNow), a told layout held
 *  to its boxes' settings' rules (page-document.ts flowSheetProblem, page-commands.ts stopsProblem), a box's settings
 *  carried by a move, a paste and a duplicate (page-gestures.ts settingsCommands)). Measured 424,587.
 *  On main meanwhile, from the same base (the timeseries work, PR #79 merging it in):
 *  408,500 -> 409,500 (2026-10-26, the timeseries follow-ups rebased onto the flow mode, docs/TIMESERIES_REVIEW_2026_10_25.md
 *  §10: the converge holds and opens a cube kept off the board for a tile (app/live-converge.ts cubeThere, cubesHeld), and
 *  a timeseries frozen from the page's cubes, an unopened one's as saved (cube-page.ts heldFilter, page-app.ts
 *  unopenedCube); no startup file added). Measured 408,946.
 *  409,500 -> 410,800 (2026-10-26, the timeseries' next phases' 1, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: the
 *  spec's reader of a tile's align and of gs-quant's remaining functions' signatures (timeseries-spec.ts TsAlign,
 *  manyArgs), and the monthly macro sample's generator beside the others (samples.ts macroCsv); no startup file added).
 *  Measured 410,159.
 *  410,800 -> 411,500 (2026-10-26, the timeseries' next phases' 2 and 3, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md
 *  §11: the spec's reader of a pane upside down and its ends, a subtitle and a note, and a tile's view -- its mode, its
 *  series, a seasonality's years, a histogram's bins, a curve's dates (timeseries-spec.ts TsView) -- which a saved page
 *  is read by as it opens; no startup file added). Measured 410,869.
 *  411,500 -> 413,400 (2026-10-26, the user: "try as pure function ... can be datacube pure scripts" -- DataCube's Pure
 *  library (pure-library.ts: datacube::ts::mean, the returns, volatility), its text with the planner, which takes it
 *  after the model of a query that calls it (lite-planner.ts #over): a person's own query may call it too; no startup
 *  file added). Measured 412,861.
 *  413,400 -> 414,000 (2026-10-27, Phase 5's drawings, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: the spec's
 *  reader of a person's own line and note -- each point a date and a value (timeseries-spec.ts TsPoint) -- which a saved
 *  page is read by as it opens; no startup file added). Measured 413,496.
 *  425,000 / 414,000 -> 430,500 (2026-10-28, main merged into the flow mode's PR #79: the two chains above, each grown
 *  from 408,500 apart -- the flow mode's roadmap and main's timeseries work -- in one bundle; no startup file added by
 *  the merge). Measured 430,054. */
const BUDGET = 430_500;

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
 *  and Open..., in place and New Page leaving it for a page of the tab's own (D1), said on a blank page's card).
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
 *  date). Measured 337,265.
 *  338,000 -> 344,000 (2026-10-24, main's D19 merged into this branch: its own steps there, 314,000 -> 324,000 -- the
 *  grid-only page's note of the same date). Measured 343,438.
 *  344,000 -> 345,000 (2026-10-25, the flow mode's M1 step 1 -- the grid-only page's note of the same date). Measured
 *  344,379.
 *  345,000 -> 352,000 (2026-10-25, the flow mode's M1 steps 3-4 -- the grid-only page's note of the same date). Measured
 *  351,377.
 *  352,000 -> 353,500 (2026-10-25, the flow mode's M2 -- the grid-only page's note of the same date). Measured 353,109.
 *  353,500 -> 354,500 (2026-10-25, the flow mode's M2 step 3 and M3's first tools -- the grid-only page's note of the
 *  same date). Measured 353,829.
 *  354,500 -> 355,500 (2026-10-25, the flow mode's M7 first steps -- the grid-only page's note of the same date).
 *  Measured 355,267.
 *  355,500 -> 356,000 (2026-10-25, the flow mode's M5 and M6 first steps: a box's count kept for its chain, Run showing
 *  each box in turn; the drawing the canvas's chunk). Measured 355,733.
 *  356,000 -> 356,500 (2026-10-25, the flow mode's False port: a live input's port read and resolved, a box made where
 *  its tool or line was let go). Measured 356,148.
 *  356,500 -> 357,000 (2026-10-25, the flow mode's comments -- the grid-only page's note of the same date). Measured
 *  356,556.
 *  357,000 -> 357,500 (2026-10-25, the flow mode's DateTime -- the grid-only page's note of the same date). Measured
 *  357,399.
 *  357,500 -> 358,000 (2026-10-25, the flow mode's copy and paste and its toolbar's Undo and Redo: the page keeps what
 *  a box's Ctrl+C copied and pastes it as one step, and tells the canvas what Undo and Redo would do; the buttons and
 *  the keys the canvas's chunk). Measured 357,632.
 *  358,000 -> 358,500 (2026-10-25, the flow mode's text tools -- the grid-only page's note of the same date). Measured
 *  358,355.
 *  358,500 -> 359,000 (2026-10-25, the flow mode's Unique, and a box's rows the columns it shows: a live input of a flow's
 *  box leaves out the columns its box hides (live-inputs.ts gridInput, cube-document.ts JoinInput.omit, join-open.ts
 *  inputRows -- startup modules); Unique's rule and its window chunks of their own). Measured 358,659.
 *  359,000 -> 359,500 (2026-10-25, the flow mode's many boxes at once -- the grid-only page's note of the same date).
 *  Measured 359,191.
 *  359,500 -> 361,000 (2026-10-25, the flow mode's containers -- the grid-only page's note of the same date; the Profile's
 *  and the keys' wiring of the canvas's options among them). Measured 360,446.
 *  361,000 -> 362,500 (2026-10-25, the flow mode's Output -- the grid-only page's note of the same date). Measured
 *  361,799.
 *  362,500 -> 363,000 (2026-10-25, the flow mode's Multi-Field Formula and Append Fields: the Multi-Field's opener
 *  (app.ts), and a Join box's tool read off its join to set it again (app/datacube-app.ts, join-how.ts isEveryPair).
 *  Measured 362,544.
 *  363,000 -> 364,000 (2026-10-25, the flow mode's Transpose, Select and Sample's other ways -- the grid-only page's note
 *  of the same date). Measured 363,365.
 *  364,000 -> 365,000 (2026-10-25, the flow mode's in place and a box's correlations: the in-place rule beside what a box
 *  hands on (box-columns.ts inPlaceCommand, inPlaceOf, read at startup through live-inputs.ts), the Profile asking its
 *  measures' correlations (app.ts); the tools and the matrix's drawing their chunks). Measured 364,174.
 *  365,000 -> 365,500 (2026-10-25, the flow mode's Summarize window and a Join's L and R -- the grid-only page's notes of
 *  the same date). Measured 365,150.
 *  365,500 -> 366,000 (2026-10-25, the flow mode's Find Replace and a box's Chart: the Find Replace window's opener
 *  (app.ts) and the page's chart of a box (page/cube-page.ts chartOf, its ChartPanel the page's own already); the
 *  window, the rule and the results pane's drawing chunks of their own). Measured 365,531.
 *  366,000 -> 367,500 (2026-10-25, the flow mode's Union of any number -- the grid-only page's note of the same date;
 *  the append of any number opened over one model besides, join.ts and join-open.ts the engine page's at startup).
 *  Measured 366,940.
 *  367,500 -> 368,500 (2026-10-25, the flow mode's Code view and Date Filter -- the grid-only page's note of the same
 *  date). Measured 367,918.
 *  368,500 -> 369,500 (2026-10-25, the flow mode's Test and a Test that stops a run -- the grid-only page's note of the
 *  same date). Measured 368,637.
 *  369,500 -> 370,500 (2026-10-25, the flow mode's Cross Tab with fixed columns -- the grid-only page's note of the same
 *  date). Measured 369,849.
 *  370,500 -> 371,500 (2026-10-26, the flow mode's Cross Tab missing values, Custom Filter, Dry Run and New Box After:
 *  the grid-only page's notes of 2026-10-25 for the first two; a run that writes nothing (page/cube-page.ts #runFlow's
 *  dry) and a box's menu gesture made in a box after it (ui/menu.ts newBoxItems, app.ts onStepAfter, page/cube-page.ts
 *  #stepAfter); no startup file added, measured). Measured 370,956.
 *  371,500 -> 373,500 (2026-10-26, the flow mode's Pure box -- the grid-only page's note of the same date). Measured
 *  372,937.
 *  373,500 -> 383,500 (2026-10-26, main merged into the flow branch: its own steps there, 344,000 -> 345,500 -> 347,000 ->
 *  348,000 -> 348,500 -> 349,500 -> 353,000 -> 353,500 -- the grid-only page's note of the same date). Measured 383,307.
 *  383,500 -> 394,000 (2026-10-26, the timeseries branch, PR #74, merged into the flow branch: its own steps there,
 *  353,500 -> 363,000 -> 364,000 -- the grid-only page's note of the same date). Measured 393,657.
 *  394,000 -> 396,500 (2026-10-26, the flow mode review's fixes -- the grid-only page's note of the same date). Measured
 *  396,140.
 *  396,500 -> 397,000 (2026-10-26, the flow mode's roadmap, C13: a join's one of theirs per key -- join.ts firstOfEach,
 *  the engine page's at startup, and join-how.ts's reader of `once`; the Join window's checkbox lazy, no startup file
 *  added). Measured 396,543.
 *  397,000 -> 398,500 (2026-10-26, the flow mode's Cache, FL10, and Add to the cast's gesture -- the grid-only page's note
 *  of the same date -- with the Cross Tab's Add to the cast before it (app.ts addPivotValues)). Measured 397,758.
 *  398,500 -> 399,500 (2026-10-26, the flow mode's smaller tool gaps, #74 -- the grid-only page's note of the same date).
 *  Measured 398,986.
 *  399,500 -> 401,000 (2026-10-26, the flow mode's Output into a table, M6/FL13 -- the grid-only page's note of the same
 *  date). Measured 400,300.
 *  401,000 -> 402,500 (2026-10-26, the flow mode's Dynamic Replace, #74 -- the grid-only page's note of the same date).
 *  Measured 402,020.
 *  402,500 -> 405,500 (2026-10-26, the flow mode's M8, a page's parameters -- the grid-only page's note of the same
 *  date). Measured 404,894.
 *  405,500 -> 408,500 (2026-10-26, the flow mode's M8, standard macros -- the grid-only page's note of the same date).
 *  Measured 407,818.
 *  408,500 -> 409,500 (2026-10-26, the flow mode's M8, Dynamic Input -- the grid-only page's note of the same date).
 *  Measured 408,808.
 *  409,500 -> 411,000 (2026-10-26, the flow mode's M8: Dynamic Input's audit, then Block Until Done, Message and the run
 *  history -- the grid-only page's notes of the same date). Measured 410,880.
 *  411,000 -> 413,000 (2026-10-26, the flow mode's roadmap audit, #77 -- the grid-only page's note of the same date).
 *  Measured 412,503.
 *  On main meanwhile, from the same base (the timeseries work, PR #79 merging it in):
 *  396,500 -> 397,600 (2026-10-26, the timeseries follow-ups rebased onto the flow mode -- the grid-only page's note of
 *  the same date). Measured 397,097.
 *  397,600 -> 398,900 (2026-10-26, the timeseries' next phases' 1 -- the grid-only page's note of the same date).
 *  Measured 398,329.
 *  398,900 -> 399,600 (2026-10-26, the timeseries' next phases' 2 and 3 -- the grid-only page's note of the same date).
 *  Measured 399,033.
 *  399,600 -> 399,900 (2026-10-26, the user: "how exactly to build that chart programmatically from TS and python" --
 *  the spec reader's expression text writes a basket's parameters by name, and writes an expression to read back as
 *  itself (timeseries-expr.ts exprSource), which a saved page's reader carries; the code itself is a chunk of the Query
 *  window's). Measured 399,611.
 *  399,900 -> 401,500 (2026-10-26, DataCube's Pure library with the planner -- the grid-only page's note of the same
 *  date). Measured 400,965.
 *  401,500 -> 402,100 (2026-10-27, Phase 5's drawings -- the grid-only page's note of the same date). Measured 401,506.
 *  413,000 / 402,100 -> 418,500 (2026-10-28, main merged into the flow mode's PR #79 -- the grid-only page's note of the
 *  same date). Measured 417,953. */
const ENGINE_BUDGET = 418_500;

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
 *  is theirs). Measured 302,497.
 *  303,000 -> 303,500 (2026-10-25, the flow mode's DateTime): no chart code changed -- the chunks a first chart fetches
 *  name the main bundle's exports and its files, and those moved with the grid-only page's note of the same date.
 *  Measured 303,003.
 *  The same 303,000 -> 303,500 on the timeseries branch (2026-10-25, the timeseries tile): the charts' ECharts handed to a timeseries tile's drawing
 *  (chart-render.ts canvasChart), so the two share one build of it and a first chart's fetch is otherwise as it was.
 *  Measured 303,007.
 *  Both kept at 303,500 when the timeseries branch, PR #74, merged into the flow branch (2026-10-26). Measured
 *  303,020. */
const CHART_BUDGET = 303_500;

/** What a first timeseries tile fetches beyond the page's startup and a first chart's (the charts' ECharts, which it
 *  draws over): its expression language (timeseries-expr.ts), its queries and its drawing (timeseries-query.ts,
 *  timeseries-option.ts), its viewer (ui/timeseries-view.ts) and its renderer (timeseries-render.ts). Set 2026-10-25 at
 *  its first measure (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md), 29,593, with room for its next functions.
 *  31,000 -> 33,000 (2026-10-25, candles: ohlc's words, its query, its drawing and hover, and a candle of a column made by
 *  the series editor -- the candlestick chart itself with the rarer kinds, above). Measured 31,228.
 *  33,000 -> 34,500 (2026-10-25, the review's fixes, docs/TIMESERIES_REVIEW_2026_10_25.md: the query compiler written
 *  part by part, each operand on its own dates (Q1), the periods a year inferred (Q3), levels apart from units (Q2);
 *  the tile's edits one rule, timeseries-edit.ts, which Python asks too (A2) -- the headroom kept to ~2% where it was
 *  5.7%, as the review's G8 asked). Measured 33,858.
 *  34,500 -> 35,000 (2026-10-26, the second audit's fixes (a level shifted by a number no level: timeseries-expr.ts
 *  levelOf) and a timeseries frozen: the filter each source keeps read by every query (timeseries-query.ts, filterRead),
 *  the viewer's setState). Measured 34,547.
 *  35,000 -> 35,500 (2026-10-26, the second audit's numbers: a range's start the value as of it (S3), an operand of one
 *  instrument unsplit (S2), prices from initial (S5), a combination's periods a year (S4), compare over its start's size
 *  and no returns' figures at or below zero (S1); and its silent gaps said -- a split line of no frequency, a level at or
 *  below zero off a log axis, the statistics' blanks (P-2)). Measured 35,157.
 *  35,500 -> 36,000 (2026-10-26, the second audit's one-API fixes: one judge of a change for the viewer and Python
 *  (timeseries-edit.ts judgedEdit), the rows' notes one rule for the drawing and Python's frame (timeseries-query.ts
 *  seriesNotes), the catalog's ramp and beta said where they differ, a tick never "-0"). Measured 35,705.
 *  36,000 -> 36,900 (2026-10-26, the tile in PR #73's design, the user's ask: its series editor and Settings a window
 *  beside it among the page's windows (ui/timeseries-view.ts #open over ui/window.ts, which the charts' chunk already
 *  brings), its Properties for its right-click -- the headroom ~2%, as the review's G8 asked). Measured 36,189.
 *  36,900 -> 37,800 (2026-10-26, the review's follow-ups, docs/TIMESERIES_REVIEW_2026_10_25.md §10: growth annualized
 *  only over a year, and said (N3); a candle's series computed on their own dates, then each period's first, highest,
 *  lowest and last (N4, timeseries-query.ts candle); a combination's grids opened together once a round of asking, and
 *  no statistics asked while nothing shows them (ui/timeseries-view.ts Opened, #statistics) -- the headroom ~2%).
 *  Measured 37,063.
 *  37,800 -> 46,800 (2026-10-26, the expert functions the user asked for, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md
 *  §5.3½, and their review's fixes (docs/TIMESERIES_REVIEW_2026_10_25.md §11): windows and lags of a calendar period
 *  (timeseries-query.ts #calendar, #valueBack), a level's returns' risk and the panel's Risk tab, a rolling regression,
 *  the Drawdowns and Correlations tabs and their queries, exponential averages in closed form (Writer.ewm), percentiles
 *  by each window's rows (ofWindow, ofLine), conditions shaded (timeseries-option.ts markArea), the catalog's words for
 *  each, the range and blank rules shared with Python (rangeWindow, statBlanks) -- the headroom ~2%). Measured 45,854.
 *  46,800 -> 52,600 (2026-10-26, the next phases' 1, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: two series'
 *  dates combined by the tile's align (timeseries-query.ts aligned, datesOf, onto), interpolate, and gs-quant's
 *  remaining functions -- the date parts, if, not, compare, filter, product, geometrically_aggregate, weighted_sum,
 *  smooth_spikes, excess_returns, vol_swap_volatility, consecutive, union, append, prepend, first, last, count, trend and
 *  seasonally_adjusted (decomposed) -- with their catalog words; the headroom ~2%). Measured 51,518.
 *  52,600 -> 56,600 (2026-10-26, the next phases' 2, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: an instrument
 *  swapped across the tile (timeseries-edit.ts swapped), the Data tab and its Copy (timeseries-option.ts timeseriesTable),
 *  a picture's heading and its export (tsHeading; timeseries-render.ts exportUrl -- ECharts' SVG painter a chunk of its
 *  own, fetched with an SVG alone), a pane upside down and its ends by hand, the Query window, a parameter's slider --
 *  the headroom ~2%). Measured 55,530.
 *  56,600 -> 66,400 (2026-10-26, the next phases' 3, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: a tile's other
 *  views -- a scatter and its regression, seasonality against the years before, a histogram and its normal, a curve
 *  across a split's key (timeseries-query.ts scatterQuery, seasonalityQuery, histogramQuery, curveQuery, and their
 *  drawing, timeseries-views.ts) -- the Calendar tab and its query (calendarQuery), and each view's Settings and its
 *  ask (ui/timeseries-view.ts #viewAsk, #fetchView); the headroom ~2%. The views' drawing, about 2,500 of it, could be a
 *  chunk of its own fetched with the first view, if it grows). Measured 65,119.
 *  66,400 -> 68,700 (2026-10-26, the Phase 2 and Phase 3 reviews' fixes, docs/TIMESERIES_REVIEW_2026_10_25.md §13-§14: a
 *  drag a try until OK (ui/timeseries-view.ts #tried, #leaving), a picture's legend and height (timeseries-option.ts
 *  tsPictureHeight), the Data tab's day keys, the calendar's months by number, every histogram bin, a curve's lines of
 *  earlier days said (timeseries-views.ts curveNotes), the views' series by mode (viewChoices), the chart busy by its
 *  rounds -- the headroom ~2%). Measured 67,342.
 *  68,700 -> 72,000 (2026-10-26, the next phases' 4, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11: an analyst's
 *  packs -- fifteen functions' signatures and words (timeseries-spec.ts, timeseries-expr.ts) and their queries
 *  (timeseries-query.ts analystCalls, basket, the Dickey-Fuller regression): technicals of a candle's columns, a series
 *  against a benchmark, a total return, a rebalanced basket, a lead, a reversion -- the headroom ~2%). Measured
 *  70,558.
 *  72,000 -> 73,500 (2026-10-26, the user: "show all the pure queries used to create a timeseries chart" -- every
 *  question a tile asks by one rule, each written by one function (timeseries-query.ts tileAsks, askQuery), and the
 *  Query window listing them all, each as it was sent with its SQL (ui/timeseries-view.ts #showQueries, #runAsk) -- the
 *  headroom ~2%). Measured 72,007.
 *  73,500 -> 75,300 (2026-10-26, the user: "try as pure function ... can be datacube pure scripts" -- a tile's mean,
 *  returns and volatility written as calls of DataCube's Pure library (timeseries-query.ts Writer.library), and the Query
 *  window's Library tab showing it (ui/timeseries-view.ts #libraryPanel) -- the headroom ~2%). Measured 73,797.
 *  75,300 -> 89,000 (2026-10-27, the library review: DataCube's Pure library, 128 functions, every function of the
 *  catalog, is a chunk of its own, src/pure-library-text.ts, fetched with the first query that calls it -- a tile's
 *  every query does -- so it is counted here, and checked off every page's startup, where it had been the planner's):
 *  measured 87,233, the library's text 16,264 of it, the tile's own code 70,969 (the calendar forms' writing gone into
 *  the library); the headroom ~2%.
 *  89,000 -> 91,500 (2026-10-27, Phase 6's D4, the user: "be able to plot trades" -- marks from a grid's rows, an
 *  annotation of their own: their query (timeseries-query.ts marksQuery, the `marks` ask), their reader and drawing
 *  (timeseries-option.ts tsMarks, the scatter of triangles, a line at a date), the viewer asking them beside the bounds
 *  and Settings adding them (ui/timeseries-view.ts #marksFields) -- the headroom ~2%). Measured 89,600.
 *  91,500 -> 95,000 (2026-10-27, Phase 5: the measure (a span's figures, ui/timeseries-view.ts #paintMeasure), shade
 *  from a column (#shadeFields), and a person's drawings -- the Draw mode, its box, a line's two clicks and a note's text
 *  (#draws, #pointed, #paintDraw), their drawing per pane (timeseries-option.ts `drawings:`), the renderer's click read
 *  in a pane's axes (timeseries-render.ts) -- the headroom ~2%). Measured 93,025.
 *  95,000 -> 97,000 (2026-10-27, Phase 5: a trend projected -- its query (timeseries-query.ts projectionQuery, the
 *  `projection` ask), its reading and dashed drawing (timeseries-option.ts tsProjected), the viewer asking it beside the
 *  series and Settings' Project (ui/timeseries-view.ts #projectionFields) -- and the replay (#replay) -- the headroom
 *  ~2%). Measured 95,068.
 *  97,000 -> 100,000 (2026-10-28, the focused Pure audit's conversion, the user: "convert all, match gsquant" -- the
 *  tile's tables and views written as the library's functions: performanceStatistics, drawdownsOfLine, runs,
 *  calendarReturns, histogramOfLine, seasonalRange, regression, fitAt, correlationOf, periodsPerYear in its text, the
 *  writer's inline maths gone -- the headroom ~2%). Measured 98,043.
 *  100,000 -> 102,000 (2026-10-28, the user: "write the query in a way that compile time checks the parameters" and
 *  "a generic snippet ... to just create exactly that tile in their own datacube" -- the library's ten enumerations and
 *  the last views as its functions (summaryStatistics, seasonality, projection, tradedValue, linearAt, periodIndex, in
 *  its text), the writer's enumeration values (timeseries-query.ts libraryOption), the program of a page of its own
 *  (timeseries-code.ts) and each formula's Pure under it (ui/timeseries-view.ts #codePanel), docs/TIMESERIES_REVIEW_2026_10_25.md
 *  §19-§20 -- the headroom ~2%). Measured 100,296. */
const TIMESERIES_BUDGET = 102_000;

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
 *  counted here since phase 6's audit). Measured 66,807.
 *  68,000 -> 70,000 (2026-10-25, the timeseries tile's candles, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md):
 *  ECharts' candlestick registered with these rarer charts, fetched by a timeseries tile when it first draws candles, so
 *  neither a first chart nor a first timeseries carries it. Measured 69,759. */
const KINDS_BUDGET = 70_000;

const isMaps = (file: string): boolean => /[\\/]chart-(?:render-)?maps-[^\\/]+\.js$/.test(file);
/** A chunk of ECharts' SVG painter (chart-render.ts svgChart): fetched with a timeseries' picture as SVG alone. */
const isSvgPainter = (file: string): boolean => (PAGE.get(file)?.modules ?? [])
  .some((m) => /node_modules\/zrender\/lib\/svg\/(?:Painter|graphic|patch)\.js$/.test(m) || m.includes('node_modules/echarts/lib/renderer/installSVGRenderer'));
/** Set 2026-10-26 at its first measure (the timeseries' next phases' 2: a picture as SVG, for a page or a deck), the
 *  painter's chunks -- its installer and zrender's SVG drawing -- with room for an ECharts bump. Measured 9,354. */
const SVG_BUDGET = 9_600;
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
 *  date). Measured 356,493.
 *  357,000 -> 363,000 (2026-10-24, main's D19 merged into this branch: its own steps there, 332,500 -> 342,500 -- the
 *  grid-only page's note of the same date). Measured 362,642.
 *  363,000 -> 364,500 (2026-10-25, the flow mode's M1 step 1 -- the grid-only page's note of the same date). Measured
 *  363,850.
 *  364,500 -> 372,500 (2026-10-25, the flow mode's M1 steps 3-4 -- the grid-only page's note of the same date; the
 *  notebook's cube carries the page's styles, a flow sheet's among them). Measured 371,848.
 *  372,500 -> 373,000 (2026-10-25, the flow mode's M1 steps 5 and after -- the grid-only page's note of the same date;
 *  the settings pane's styles among them). Measured 372,674.
 *  373,000 -> 374,500 (2026-10-25, the flow mode's M2 -- the grid-only page's note of the same date; a flow's lines'
 *  and ports' styles among them). Measured 374,344.
 *  374,500 -> 375,500 (2026-10-25, the flow mode's M2 step 3 and M3's first tools -- the grid-only page's note of the
 *  same date; the palette's group headings' styles among them). Measured 375,094.
 *  375,500 -> 377,000 (2026-10-25, the flow mode's M7 first steps -- the grid-only page's note of the same date; the
 *  canvas's toolbar and the palette's find field's styles among them). Measured 376,663.
 *  377,000 -> 377,500 (2026-10-25, the flow mode's M5 and M6 first steps -- the engine page's note of the same date; the
 *  Run button's and a box's count's styles among them). Measured 377,210.
 *  377,500 -> 378,000 (2026-10-25, the flow mode's False port -- the engine page's note of the same date; a Filter's T
 *  and F ports' styles among them). Measured 377,821.
 *  378,000 -> 378,500 (2026-10-25, the flow mode's run log: its panel's styles; its drawing the canvas's chunk). Measured
 *  378,157.
 *  378,500 -> 379,000 (2026-10-25, the flow mode's comments -- the grid-only page's note of the same date; a note's
 *  styles among them). Measured 378,609.
 *  379,000 -> 380,000 (2026-10-25, the flow mode's DateTime -- the grid-only page's note of the same date). Measured
 *  379,446.
 *  380,000 -> 380,500 (2026-10-25, the flow mode's Text Input -- the grid-only page's note of the same date; the table's
 *  window's styles among them). Measured 380,187.
 *  380,500 -> 381,000 (2026-10-25, the flow mode's text tools -- the grid-only page's note of the same date; their
 *  windows' styles and the Parse group's colour among them). Measured 380,836.
 *  381,000 -> 381,500 (2026-10-25, the flow mode's Unique -- the engine page's note of the same date; its U and D ports'
 *  styles among them). Measured 381,179.
 *  381,500 -> 382,000 (2026-10-25, the flow mode's many boxes at once -- the grid-only page's note of the same date; the
 *  picked boxes' ring and the band's styles among them). Measured 381,752.
 *  382,000 -> 382,500 (2026-10-25, the flow mode's Profile: the results pane's Rows and Profile and the profile's table's
 *  styles; its query and its reading a chunk of their own, profile.ts, its drawing the canvas's chunk). Measured
 *  382,213.
 *  382,500 -> 384,000 (2026-10-25, the flow mode's containers -- the grid-only page's note of the same date; a container's
 *  frame, head, fold and corner, and the keys' panel, styles among them). Measured 383,671.
 *  384,000 -> 385,500 (2026-10-25, the flow mode's Output -- the grid-only page's note of the same date; its settings'
 *  styles among them). Measured 385,054.
 *  385,500 -> 386,500 (2026-10-25, the flow mode's Multi-Field Formula -- the grid-only page's note of the same date).
 *  Measured 385,655.
 *  386,500 -> 387,500 (2026-10-25, the flow mode's Transpose, Select and Sample's other ways -- the grid-only page's note
 *  of the same date; the Select's and the Transpose's windows' styles among them). Measured 386,857.
 *  387,500 -> 388,500 (2026-10-25, the flow mode's minimap, Fuzzy Match, Select's retype and a box's correlations: the
 *  minimap's and the correlation matrix's styles, the Join window's edits-apart field and the Select's type choice;
 *  their code the canvas's and the windows' chunks). Measured 387,738.
 *  388,500 -> 389,500 (2026-10-25, the flow mode's Summarize window and a Join's L and R -- the grid-only page's notes of
 *  the same date; the window's rows and a Join's three outputs' styles among them). Measured 389,004.
 *  389,500 -> 390,000 (2026-10-25, the flow mode's Find Replace, a box's Chart and a Select's column order: a box's
 *  order handed on (box-columns.ts boxOrder, read at startup through live-inputs.ts; cube-document.ts JoinInput.order),
 *  the Chart's and the Select's arrows' styles among them). Measured 389,693.
 *  390,000 -> 391,500 (2026-10-25, the flow mode's Union of any number -- the grid-only page's and the engine page's
 *  notes of the same date; a Union's numbered ports and its settings' rows among the styles). Measured 391,082.
 *  391,500 -> 392,500 (2026-10-25, the flow mode's Code view, Create Samples and command palette: a grid's last query
 *  printed (app.ts code, copyText) at startup; the Code view's, the palette's and the two-output ports' styles among the
 *  module's; their code the canvas's chunk). Measured 391,532.
 *  392,500 -> 393,500 (2026-10-25, the flow mode's Date Filter and Test: the Date Filter's rule and read-back and its
 *  window's opener -- the grid-only page's note of the same date -- the Test's opener (app.ts #openTest) and a grid's
 *  first row read (page/cube-page.ts firstRowOf); a Test's marks and its window's styles; its rule, its window and its
 *  marks' code chunks of their own). Measured 392,624.
 *  393,500 -> 395,000 (2026-10-25, the flow mode's Test count of rows and Cross Tab with fixed columns: an empty grid's
 *  row read (page/cube-page.ts firstRowOf), a pivot's values pinned and a pinned pivot's rows -- the grid-only page's
 *  note of the same date -- and the Cross Tab strip's styles). Measured 394,407.
 *  395,000 -> 396,000 (2026-10-25, the flow mode's Cross Tab missing values and Custom Filter -- the grid-only page's
 *  note of the same date -- and the Custom Filter window's styles). Measured 395,109.
 *  396,000 -> 398,000 (2026-10-26, the flow mode's Pure box -- the grid-only page's note of the same date -- and its
 *  window's styles). Measured 397,684.
 *  398,000 -> 410,000 (2026-10-26, main merged into the flow branch: its own steps there, 363,000 -> 365,000 -> 366,500 ->
 *  367,500 -> 368,500 -> 370,000 -> 373,500 -> 374,000 -> 374,500 -- the grid-only page's note of the same date, and
 *  their styles). Measured 409,703.
 *  410,000 -> 421,500 (2026-10-26, the timeseries branch, PR #74, merged into the flow branch: its own steps there,
 *  374,500 -> 385,000 -> 386,000 -- the grid-only page's note of the same date, and the timeseries tile's styles).
 *  Measured 421,003.
 *  421,500 -> 424,000 (2026-10-26, the flow mode review's fixes -- the grid-only page's note of the same date). Measured
 *  423,483.
 *  424,000 -> 425,000 (2026-10-26, the flow mode's roadmap: C13, a join's one of theirs per key -- the engine page's note
 *  of the same date -- and a Profile's top values, its column opened (layout/flow-canvas.ts #drawProfile, profile.ts
 *  topValuesLambda): one module, everything in it). Measured 424,104.
 *  425,000 -> 426,000 (2026-10-26, the flow mode's Cache, FL10, and Add to the cast -- the engine page's note of the same
 *  date -- and the Cache's styles). Measured 425,324.
 *  426,000 -> 427,000 (2026-10-26, the flow mode's smaller tool gaps, #74 -- the engine page's note of the same date --
 *  and the score's name field's style). Measured 426,601.
 *  427,000 -> 428,500 (2026-10-26, the flow mode's Output into a table, M6/FL13 -- the engine page's note of the same
 *  date -- with the headless lane's writer beside it: one module, everything in it). Measured 427,946.
 *  428,500 -> 430,000 (2026-10-26, the flow mode's Dynamic Replace, #74 -- the engine page's note of the same date --
 *  with the headless lane's rules read beside it). Measured 429,681.
 *  430,000 -> 433,000 (2026-10-26, the flow mode's M8, a page's parameters -- the engine page's note of the same date --
 *  with the headless lane's lens and binding beside it). Measured 432,624.
 *  433,000 -> 436,000 (2026-10-26, the flow mode's M8, standard macros -- the engine page's note of the same date -- with
 *  the headless lane's gridFunction and macroComposed beside it). Measured 435,556.
 *  436,000 -> 437,000 (2026-10-26, the flow mode's M8, Dynamic Input -- the engine page's note of the same date -- with
 *  the headless lane's 'flow.pick' beside it). Measured 436,538.
 *  437,000 -> 438,000 (2026-10-26, Dynamic Input's audit -- the engine page's note of the same date -- with the headless
 *  lane's heldHow beside it). Measured 437,193.
 *  438,000 -> 439,000 (2026-10-26, Block Until Done, Message and the run history -- the engine page's note of the same
 *  date -- with the headless lane's flowRun beside it). Measured 438,768.
 *  439,000 -> 441,000 (2026-10-26, the flow mode's roadmap audit, #77 -- the engine page's note of the same date -- with
 *  the headless lane's 'flow.imported' and its joins bound with the page's parameters beside it). Measured 440,385.
 *  On main meanwhile, from the same base (the timeseries work, PR #79 merging it in):
 *  424,000 -> 425,100 (2026-10-26, the timeseries follow-ups rebased onto the flow mode -- the grid-only page's note of
 *  the same date -- and the statistics panel's tabs' and the correlation matrix's styles). Measured 424,538.
 *  425,100 -> 426,400 (2026-10-26, the timeseries' next phases' 1 -- the grid-only page's note of the same date).
 *  Measured 425,778.
 *  426,400 -> 427,100 (2026-10-26, the timeseries' next phases' 2 and 3 -- the grid-only page's note of the same date).
 *  Measured 426,627.
 *  427,100 -> 427,400 (2026-10-26, the user: "show all the pure queries used to create a timeseries chart" -- the Query
 *  window's styles for each question, app.css .dc-ts-ask). Measured 427,157.
 *  427,400 -> 429,200 (2026-10-26, DataCube's Pure library with the planner -- the grid-only page's note of the same
 *  date). Measured 428,652.
 *  429,200 -> 429,900 (2026-10-27, Phase 5's drawings -- the grid-only page's note of the same date -- and the drawing
 *  box's style, app.css .dc-ts-draw). Measured 429,265.
 *  441,000 / 429,900 -> 446,500 (2026-10-28, main merged into the flow mode's PR #79 -- the grid-only page's note of the
 *  same date). Measured 446,209. */
const WIDGET_BUDGET = 446_500;
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

  it(`a first timeseries tile fetches at most ${TIMESERIES_BUDGET.toLocaleString()} bytes more than the startup and a first chart, gzipped`, () => {
    // its viewer and its renderer, and what they import statically: none of it at startup, and the charts' ECharts its own
    const at = new Set(startup());
    const chart = firstChart();
    for (const f of at) assert.ok(!holds(f, 'src/ui/timeseries-view.ts') && !holds(f, 'src/timeseries-expr.ts') && !holds(f, 'src/timeseries-query.ts'),
      `${f} carries the timeseries viewer`);
    const fetched = new Set<string>();
    const visit = (f: string): void => {
      if (fetched.has(f)) return;
      fetched.add(f);
      for (const s of PAGE.get(f)?.statics ?? []) visit(s);
    };
    // and DataCube's Pure library (pure-library-text.ts), a chunk of its own fetched with the first query that calls it --
    // a tile's every query does -- never at startup (the library review, 2026-10-27)
    for (const f of at) assert.ok(!holds(f, 'src/pure-library-text.ts'), `${f} carries the Pure library's text`);
    chunksOf().filter((f) => holds(f, 'src/ui/timeseries-view.ts') || holds(f, 'src/timeseries-render.ts')
      || holds(f, 'src/pure-library-text.ts')).forEach(visit);
    for (const f of fetched) assert.ok(!holds(f, 'echarts') || chart.has(f), `${f}: ECharts a first chart does not fetch`);
    // ECharts' SVG painter a chunk of its own, fetched with a picture's export as SVG alone (chart-render.ts svgChart)
    for (const f of [...at, ...chart, ...fetched]) assert.ok(!isSvgPainter(f), `${f} carries the SVG painter`);
    const own = [...fetched].filter((f) => !at.has(f) && !chart.has(f));
    assert.ok(own.length >= 2, 'the timeseries chunks are found');
    const size = own.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= TIMESERIES_BUDGET, `a first timeseries fetches ${size.toLocaleString()} bytes gzipped, over its budget of ${TIMESERIES_BUDGET.toLocaleString()}`);
  });

  it(`batch 3's kinds are chunks of their own, fetched with the first of them: at most ${KINDS_BUDGET.toLocaleString()} bytes more`, () => {
    for (const f of startup()) assert.ok(!isKinds(f) && !holds(f, 'src/chart-kinds.ts'), `${f} carries batch 3's kinds`);
    const kinds = chunksOf().filter(isKinds);
    assert.equal(kinds.length, 2, 'the kinds\' drawing and their ECharts charts, a chunk each');
    // with them, the ECharts code they share only with the maps' chunks: fetched with the first of either, counted here
    // (the phase 6 audit: a chunk in no budget)
    const shared = chunksOf().filter((f) => holds(f, 'echarts') && !isKinds(f) && !isMaps(f) && !isSvgPainter(f) && !firstChart().has(f));
    for (const f of shared) {
      assert.ok([...PAGE].every(([importer, out]) => !out.statics.includes(f) || isKinds(importer) || isMaps(importer)),
        `${f} holds ECharts code a first chart does not fetch, and is fetched by something other than the kinds or the maps`);
    }
    const size = [...kinds, ...shared].reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= KINDS_BUDGET, `batch 3's kinds fetch ${size.toLocaleString()} bytes gzipped, over their budget of ${KINDS_BUDGET.toLocaleString()}`);
  });

  it(`ECharts' SVG painter is chunks of its own, fetched with a picture as SVG alone: at most ${SVG_BUDGET.toLocaleString()} bytes`, () => {
    const painter = chunksOf().filter(isSvgPainter);
    assert.ok(painter.length >= 1, 'the SVG painter is a chunk of the page');
    // reached by a dynamic import alone: no file fetches it with itself
    for (const [importer, out] of PAGE) {
      for (const f of out.statics) assert.ok(!isSvgPainter(f) || isSvgPainter(importer), `${importer} fetches ${f}, the SVG painter, with itself`);
    }
    const size = painter.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= SVG_BUDGET, `the SVG painter fetches ${size.toLocaleString()} bytes gzipped, over its budget of ${SVG_BUDGET.toLocaleString()}`);
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
    // the Pure library's text a chunk of its own, fetched with the first query that calls it (2026-10-27)
    for (const f of files) assert.ok(!holds(f, 'src/pure-library-text.ts', outputs), `${f} carries the Pure library's text`);
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
