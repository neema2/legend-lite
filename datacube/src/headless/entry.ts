// The headless bundle's entry: what Python's host loads into V8 (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md
// §3), built as one script whose exports are the global `legendLiteHeadless`. Its first two imports put the web layer
// in place before any module that needs it as it loads.

import './install-web.ts';
import './install-url.ts';

import { REQUEST_KINDS, type PlannerWorker } from '../../../engine-client/src/legend/planner-protocol.ts';
import { hostCalls, hostDatabase, hostWorker, type HostDatabase } from './host.ts';
import {
  chartOwnGrid, cubeLeaves, gridAddFilter, gridApply, gridBaseOn, gridChart, gridColumn, gridCube, gridExport,
  gridFilter, gridGesture, gridMeasure, gridOpened, gridPreset, leafCube, gridRelation, gridSetUp, gridSnapPull, gridView, snapRefused, sourceCube,
} from './grid.ts';
import { forget, ingest, landed, mount, remoteAccess, tableFor } from './data-in.ts';
import { modelAssociation, modelClass, modelDataSpace } from './model.ts';
import { layoutBands, layoutTiles } from './layout.ts';
import {
  chartDrill, chartPick, cubeOpensAgain, gridCopied, pageGesture, pageNoneOpened, gridSnapped, nameGiven, pageApply, pageFile, pageFilter, pageNew, pageOfRecord, pageOpenedLive, pageRead,
  pageReads, pageRebase, pageRecord, pageSaved, pageSearch, pageWithCube, pageWritten,
} from './page.ts';
import { joinCube, joinQuestions, joinSides } from './join.ts';

/** The request kinds this bundle's planner sends: the host checks, before anything runs, that it answers each (§7). */
export { REQUEST_KINDS };

/** A grid, headless (§4): its view, its rows as a relation, its export (D), a chart of it
 *  as DataCube makes and judges one (chart-spec.ts chartFrom and chartProblems: written once, which Python asks), and
 *  its cube over a frame as DataCube makes one, new or rebuilt over the frame as it is now (cube-document.ts
 *  rebuilt), and a calculated column added to it, its filter set as the screen's editors do, and the grid edited as the
 *  screen edits one (gridApply: grid-commands.ts, the commands its panels, menu and drag zones build). What a cube reads
 *  (cubeLeaves), its base on the host -- over the host's frames, or its copy, snapped (gridBaseOn: the one rule, Z15)
 *  -- and a saved cube opened over them as it is now (gridOpened): a frame's, or a join's of frames
 *  (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3). */
export {
  cubeLeaves, gridApply, gridBaseOn, gridChart, gridCube, gridExport, gridFilter, gridOpened, leafCube,
  gridRelation, gridSnapPull, gridView,
};

/** One condition added to a grid's filter, as a cell's Add Filter adds one (gridAddFilter: Python's grid.add_filter), and
 *  a grid's copy, as Copy of Grid makes it (gridCopied: Python's grid.copy). */
export { gridAddFilter, gridCopied };

/** Any column the column editor makes -- a window, a child aggregate, an explode, at either level (gridColumn: Python's
 *  grid.calculate, window, child_aggregate, explode) -- and a preset as the header menu adds one (gridPreset: grid.grain,
 *  show_as, bins). */
export { gridColumn, gridPreset };
/** Any gesture a person makes on a grid, by the one function the screen's control calls (gridGesture: Python's
 *  grid.gesture, and toggle_sort, toggle_group_sort, dimension, drill, drill_up, add_group ...: docs/
 *  DATACUBE_ONE_API_PLAN_2026_10_21.md S8), and measures made by a person's own steps (gridMeasure: grid.measure). */
export { gridGesture, gridMeasure };

/** A new grid's cube over a class, a saved query or a warehouse table, as the tab adds one (sourceCube: Python's
 *  `page.grid(cls=...)`, `page.grid(query=...)`, `page.grid(warehouse=...)`; X4). */
export { sourceCube };

/** Why a snap past its host's row limit is refused: the tab's sentence, which Python's engine says too (Z16). And a new
 *  grid's query as the commands that make it, in a person's order (gridSetUp: Python's `page.grid(...)`, Z8). */
export { gridSetUp, snapRefused };

/** Data in (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §4.3): a file read into the host's database by upload.ts, its
 *  table dropped again, and a remote file mounted as a view (its extensions and keys put in place first, where the host
 *  checks what it names). */
export { forget, ingest, landed, mount, remoteAccess, tableFor };

/** The model actions, headless (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D4, Z4-Z7): Create a Class… of a grid, Create
 *  an Association… and Create a Data Space… of the page, as the windows make them, into a project through the host's
 *  fetch -- each answering the page commands of its record of the page. (The model a page keeps for the cubes it still
 *  holds is the page commands' rule: pageApply.) */
export { modelAssociation, modelClass, modelDataSpace };

/** A sheet's layout read by DataCube's own rules (layout/bands.ts): its tiles, and bands of tiles as the layout
 *  picker's shape -- what Python's pages ask, so they walk no layout by rules of their own (a layout is edited by page
 *  commands: pageApply). */
export { layoutBands, layoutTiles };

/** A page document read as DataCube reads it (its own version, every rule held) and written as DataCube writes
 *  it: what Python's pages ask when one is loaded or read back (pageRead), and saved or written as a file, refused as
 *  the screen's Save refuses it (pageSaved), as the page store keeps it saved under a name (pageRecord: the Save window's
 *  record), or as a page file (pageFile: Export ▸ Page File's rule). And a new page as
 *  DataCube starts one: what Python's ll.Page() asks. And a
 *  page edited by commands (page-commands.ts): what Python's page edits send. */
export { pageApply, pageFile, pageNew, pageOpenedLive, pageRead, pageRecord, pageSaved };
/** A page saved in the page store as it opens (pageOfRecord: the tab's Open…, Python's Page.open); the saved pages
 *  searched as Open… searches them (pageSearch: Python's Page.saved). */
export { pageOfRecord, pageSearch };
/** Whether a page's cube opens again over its source as it is now (cubeOpensAgain: a snapped grid's does not); why a
 *  page none of whose cubes opened is not opened (pageNoneOpened: the tab's Open…'s rule, Python's load's). */
export { cubeOpensAgain, pageNoneOpened };
/** A chart's mark selected, as a click selects one (chartPick: Python's chart.select), and a chart drilled down into a
 *  mark or back up, as a double-click and the crumbs drill (chartDrill: chart.drill, chart.drill_up). */
export { chartDrill, chartPick };
/** A gesture on a page, by the one function the screen's gesture calls (pageGesture: Python's chart.grid()'s steps;
 *  docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8). */
export { pageGesture };
/** A chart's grid shown, as Show Grid shows it: the one it follows, or one of its own grouping made (chartOwnGrid:
 *  Python's chart.grid(); chart-spec.ts chartGrid, the screen's rule). */
export { chartOwnGrid };
/** A grid's edit on a page: the page with its cube as the edit left it, its charts following it (V1, V2). */
export { pageWithCube };
/** The page's keeper (Python's: docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) asks DataCube what it never reads itself: a
 *  page written whole -- a restore, an undo's or a redo's, or a page loaded -- read as DataCube reads a page
 *  (pageWritten), what a page reads (pageReads), and a writer's edits applied to the page as it is now (pageRebase:
 *  page-rebase.ts, the one rule for it; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4). */
export { pageReads, pageRebase, pageWritten };

/** A page filter on a class's property, made as the screen's Add a Filter makes one (a setPageFilter command, for
 *  pageApply): what Python's page.filter asks. */
export { pageFilter };

/** A grid's Snap as the page command that says it (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): what its base
 *  query's pull is (gridSnapPull, above) and, its host having copied it, the copy written down as the screen writes
 *  it -- what Python's grid.snap() asks, so the page's record of a copy is DataCube's. */
export { gridSnapped };

/** A page's, a tile's or a sheet's name as a person gives it, by DataCube's one rule: what Python's names ask. */
export { nameGiven };

/** A join of two of a page's cubes over the host's tables (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.4): the join's
 *  grid's cube document, for the page command addGrid, what the join will do before it is made, and its two sides as the
 *  Join window shows them -- the tab's Join…, with no screen, and its answers for a tab on the host's page (B11). */
export { joinCube, joinQuestions, joinSides };

/** What DataCube's code runs on in this host: its planner's worker, and the database. */
export interface Headless {
  readonly worker: () => PlannerWorker;
  readonly database: HostDatabase;
}

/** The planner's worker and the database, over the host's calls (`legendLiteHost`, defined before the bundle loads). */
export function start(): Headless {
  const calls = hostCalls();
  return { worker: () => hostWorker(calls), database: hostDatabase(calls) };
}
