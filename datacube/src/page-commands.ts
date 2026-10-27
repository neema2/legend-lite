// A PAGE EDIT AS A VALUE (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md; the command program's stage 1, its page half): each
// edit of a page's structure -- its sheets, its tiles, their titles, their places -- is a PageCommand, plain JSON, and
// one pure function applies it to the page's document and the ids it has held. The rules the screen kept inline and
// Python copied (the homework's §8) are here once: ids and titles, what goes with a removed tile, where a moved or new
// tile goes, names. No DOM, no planner: what needs either (a cube's document, a chart's spec) is made before the
// command is built, as a grid command's calculated column is parsed first (grid-commands.ts).

import { fieldNames, type CommandFields, type MissingFields } from './command-fields.ts';
import { followCube, readChart, type ChartSpec } from './chart-spec.ts';
import {
  CubeDocumentError, cubeOpenedAgain, inputOn, isLive, moreIndex, readCube, readCubeInput, readCubeSource, withInputOn, type CubeDocument,
  type CubeInput, type CubeSource, type InputPort, type InputSide, type JoinInput, type JoinSource, type RowsSource,
} from './cube-document.ts';
import { ProtocolError, readLambda, type Lambda } from '../../pure-protocol/src/index.ts';
import { pureProblem } from './pure-box.ts';
import {
  MAX_COLUMNS, NEW_SHEET, NotALayout, add, arrange, asSaved, remove, stackTiles, type Bands, type Preset,
} from './layout/bands.ts';
import { EMPTY_FLOW, FlowRefusal, moveBox, placeAfter, removeBox, setOutput, setStops, type Flow, type Frame, type Output } from './layout/flow.ts';
import {
  PageDocumentError, keptFor, layoutTiles, readGridCopy, readLayout, stillHeld, unnamedLabel, writePageOf, type ChartView, type FlowLayout,
  type GridCopy, type GridView, type PageDocument, type PageLayout, type PageSheet, type PageView, type TextView, type TimeseriesView,
} from './page-document.ts';
import { frozenFrom, gridInput, liveGrids, liveProblem } from './live-inputs.ts';
import { givenName } from './page-names.ts';
import { readJoinHow, type JoinHow } from './join-how.ts';
import type { PageModelRef } from './page-model.ts';
import { chartTitle, gridTitle, idShape, nextPageId, textTitle, timeseriesTitle } from './page-ids.ts';
import { textProblem } from './page-text-rule.ts';
import { readTimeseries, type TimeseriesSpec } from './timeseries-spec.ts';
import { readTextFiles, type TextFile } from './text-files.ts';
import { namedProperties, type PageFilter } from './page-filters.ts';
import { readFilterNode, type GridCommand } from './grid-commands.ts';
import type { PageState } from './page-state.ts';
import { CubeRefusal, sameValue, withoutConditions, type CubeShape, type FilterNode } from './snapshot.ts';

// -- the page's model (page-model.ts keeps its record; its rules are here, the commands' own) --

/** The page's model made by its first model action: its project, DataCube's workspace, the package its classes go in,
 *  no tables kept yet. Refused when the page has one: a page has one model. */
export function modelMade(ref: PageModelRef | undefined, project: string, workspace: string, pkg: string): PageModelRef {
  if (ref !== undefined) throw new CubeRefusal(`the page's model is ${ref.project} already: a page has one model`);
  return { project, workspace, package: pkg, tables: {} };
}

/** The page pinned to `revision`, the one its last model action saved. */
export function modelPinned(ref: PageModelRef | undefined, revision: string): PageModelRef {
  const has = need(ref);
  return has.revision === revision ? has : { ...has, revision };
}

/** The page's model keeping, for the grid `cube`, its leaves' `tables` (in their order) and `cls`, its class -- null
 *  leaving either as it is. */
export function modelKept(ref: PageModelRef | undefined, cube: string, tables: readonly string[] | null, cls: string | null):
  PageModelRef {
  let has = need(ref);
  if (tables !== null && JSON.stringify(has.tables[cube]) !== JSON.stringify(tables)) {
    has = { ...has, tables: { ...has.tables, [cube]: [...tables] } };
  }
  if (cls !== null && has.classes?.[cube] !== cls) has = { ...has, classes: { ...has.classes, [cube]: cls } };
  return has;
}

function need(ref: PageModelRef | undefined): PageModelRef {
  if (!ref) throw new CubeRefusal('the page has no model yet: its first model action makes it');
  return ref;
}

/** A page as its commands edit it: its document, and every tile and sheet id it has held -- none is given out again,
 *  so a handle a host kept never meets a new tile under its old one's id. */
export interface PageDraft {
  readonly page: PageDocument;
  readonly held: readonly string[];
}

/** One edit of a page's structure. An id left out of one that makes a tile or a sheet is allocated (nextPageId). */
export type PageCommand =
  | { readonly kind: 'renamePage'; readonly name: string }
  /** The page saved as the page store's record `record` (null: that record deleted): whichever editor saves it next saves
   *  over it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B13). No step of its history, as a rename is none. */
  | { readonly kind: 'savedAs'; readonly record: string | null }
  | { readonly kind: 'addSheet'; readonly id?: string; readonly name: string | null }
  /** A flow sheet (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4): a sheet drawn as a canvas of boxes, each a grid that does
   *  one thing -- empty, after the page's other sheets. */
  | { readonly kind: 'addFlow'; readonly id?: string; readonly name: string | null }
  /** A box of a flow sheet moved to (`x`, `y`) on its canvas (layout/flow.ts moveBox) -- and, a comment's, `frame`: its
   *  size as a container (Alteryx's Tool Container: docs/DATACUBE_FLOW_MODE_2026_10_24.md M7), folded or not; null, none
   *  (a comment again); left out, the frame it has. */
  | { readonly kind: 'placeBox'; readonly tile: string; readonly x: number; readonly y: number; readonly frame?: Frame | null }
  | { readonly kind: 'renameSheet'; readonly sheet: string; readonly name: string | null }
  | { readonly kind: 'moveSheet'; readonly sheet: string; readonly to: number }
  | { readonly kind: 'removeSheet'; readonly sheet: string }
  | { readonly kind: 'renameTile'; readonly tile: string; readonly title: string | null }
  /**
   * A grid of a cube of its own (its document made first), on `sheet` -- a band of its own at the sheet's bottom (since
   * 2026-10-23 `near`, the tile it came from, no longer places it: kept on the wire, which every editor writes). Its
   * document null: a grid whose source the page cannot write down (the screen's, over rows it cannot name), its view
   * held with no cube -- a page that cannot be saved, as the screen's Save says.
   */
  | {
    readonly kind: 'addGrid'; readonly id?: string; readonly document: CubeDocument | null; readonly sheet: string;
    readonly near: string | null; readonly title: string | null; readonly columns: number | null;
  }
  /** A chart of grid `near`'s cube (its spec made first, chart-spec.ts chartFrom), under that grid, beside the charts
   *  already there while that row has room. */
  | {
    readonly kind: 'addChart'; readonly id?: string; readonly near: string; readonly spec: ChartSpec;
    readonly title: string | null; readonly columns: number | null;
  }
  /** A chart's spec, whole (made first by chart-spec.ts editChart, the options form's rule). */
  | { readonly kind: 'setChart'; readonly chart: string; readonly spec: ChartSpec }
  /** A text tile (page-text.ts) on `sheet`, a band of its own at its bottom, its words judged by textProblem. */
  | {
    readonly kind: 'addText'; readonly id?: string; readonly sheet: string; readonly text: string; readonly title: string | null;
    readonly columns: number | null;
  }
  /** A text tile's words, whole (its editor's OK). */
  | { readonly kind: 'setText'; readonly tile: string; readonly text: string }
  /**
   * A TIMESERIES TILE (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §7) of the grids its spec's sources name, on
   * `sheet`: under `near` (the tile it came from, on that sheet), beside the charts already there while that row has
   * room; near none, a band of its own at the sheet's bottom. `kept`: cubes its sources read that no grid shows, held
   * for the tile as `document` says, reading `copy` (null: their live rows) -- made, or the one of that id replaced --
   * as a removed grid's cube is while a tile reads it (the review's G4, docs/TIMESERIES_REVIEW_2026_10_25.md; the
   * owner, 2026-10-25: kept, as a frozen chart's is). What an undo says (page-between.ts), so the tile it brings back
   * reads the very cube it read, a cube never held without a tile reading it.
   */
  | {
    readonly kind: 'addTimeseries'; readonly id?: string; readonly sheet: string; readonly near: string | null;
    readonly spec: TimeseriesSpec; readonly title: string | null; readonly columns: number | null; readonly kept?: readonly KeptCube[];
  }
  /** A timeseries tile's spec, whole (as its viewer made it); `kept` as addTimeseries's. */
  | { readonly kind: 'setTimeseries'; readonly tile: string; readonly spec: TimeseriesSpec; readonly kept?: readonly KeptCube[] }
  /** A text tile's files, whole (text-files.ts: pointed to by name, size and fingerprint, never copied): opened or dropped
   *  on it, or one taken off by its tab's x. */
  | { readonly kind: 'setTextFiles'; readonly tile: string; readonly files: readonly TextFile[] }
  /** A chart reading another grid's cube from now on (D7: Show Grid made the grid of its own grouping) -- its selection
   *  taken off the cube it read, and that cube let go when no view reads it any more (a detached chart's). */
  | { readonly kind: 'followGrid'; readonly chart: string; readonly grid: string }
  /** A chart reading a cube of its own from now on -- detached, as a frozen chart whose grid went is (its selection and
   *  its page filter off, as followGrid takes them; frozen, as a detached chart is): `cube`, the kept cube's id -- one no
   *  grid shows: made, or replaced with `document`, which its other charts then read too -- the id it had, so an undo
   *  puts the chart back on the very cube it read (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W19, 2026-10-24: the inverse
   *  of Show Grid on a detached chart, and of a detached chart's removal). The cube it read is let go when no view reads
   *  it any more. */
  | { readonly kind: 'detach'; readonly chart: string; readonly cube: string; readonly document: CubeDocument; readonly copy?: GridCopy | null }
  /** A chart's selection, the mark clicked last as its conditions (none: cleared) -- the chart's alone: the conditions
   *  on its grid's filter are that grid's `refilter`, told with it (grid-commands.ts). */
  | { readonly kind: 'select'; readonly chart: string; readonly conditions: readonly FilterNode[] }
  /** A tile onto another sheet, at its bottom; onto its own, nothing. `columns`: how many fit side by side there. */
  | { readonly kind: 'moveTile'; readonly tile: string; readonly sheet: string; readonly columns: number | null }
  | { readonly kind: 'stack'; readonly tiles: readonly string[] }
  | { readonly kind: 'removeTile'; readonly tile: string }
  | { readonly kind: 'arrange'; readonly sheet: string; readonly preset: string; readonly order: readonly string[] | null }
  /** A sheet's layout, whole: a drag, a resize, a drop on another tile's middle -- every tile of the sheet in it once. */
  | { readonly kind: 'layout'; readonly sheet: string; readonly layout: PageLayout }
  /**
   * A page filter (page-filters.ts, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2), set: the one of its id replaced,
   * else added -- its id allocated when left out. Its class one a grid of the page reads; `from`, the chart whose click
   * made it, or null.
   */
  | {
    readonly kind: 'setPageFilter'; readonly id?: string; readonly class: string; readonly condition: FilterNode;
    readonly from: string | null;
  }
  /** A page filter taken off. */
  | { readonly kind: 'removePageFilter'; readonly filter: string }
  /**
   * Grid `grid` reads `copy`, a copy of its base query its host has made (Snap: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
   * §18.8), in place of the one it read, if any (its host lets that one go). Not a step of the page's history: the
   * plane is where the grid's rows are, not what it shows.
   */
  | { readonly kind: 'snap'; readonly grid: string; readonly copy: GridCopy }
  /** Grid `grid` reads its live base again (Go Live): its copy, which its host lets go, off the page. */
  | { readonly kind: 'live'; readonly grid: string }
  /** A grid's cube opened again over its source as it is now: `source` written as it is now (a frame's columns moved),
   *  the cube opened over it by one rule whatever it is now (cube-document.ts cubeOpenedAgain) -- made by whichever
   *  side opened it again (the frames' host, or a tab opening its page). */
  | { readonly kind: 'rebuilt'; readonly cube: string; readonly source: CubeSource }
  /**
   * A grid's input set (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2: a line drawn on a flow, a box frozen or linked again,
   * a removal's freeze undone): grid `grid` reads, on `side` -- another grid's rows' one input, a join's left or right, a
   * Union's more1, more2, ... (its next one, one past its last, an input added) --
   * `input`: live, `{grid}`, a grid of the page as it is now; a copy of what a grid was; or, null, a copy of what its live
   * input is now (Freeze). Its query as it is: what its input answers now is read when it opens again (a new column
   * hidden, a gone one's parts left out: cube-document.ts cubeOpenedAgain). Refused when it would read its own rows.
   */
  | { readonly kind: 'setInput'; readonly grid: string; readonly side: InputSide; readonly input: CubeInput | null }
  /** A join's how, whole (a flow's Join or Union box set again by the Join window: docs/DATACUBE_FLOW_MODE_2026_10_24.md
   *  M2), and a Union's inputs after its right (`more`, whole: an input added on the canvas, one taken off; [] none; left
   *  out, as they are): its left and right as they are, its columns read again when it opens (cube-document.ts
   *  cubeOpenedAgain). Refused when a join of two would take more, or one would read its own rows. */
  | { readonly kind: 'setJoin'; readonly grid: string; readonly how: JoinHow; readonly more?: readonly CubeInput[] }
  /** A flow's Pure box set (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12; pure-box.ts): grid `grid`, over another grid's
   *  rows, its rows `pure`'s function of its input's -- a lambda of one parameter, its input's rows, its body one
   *  expression that reads it -- or (null) its input's as they are. Its query as it is, its columns read again when it
   *  opens (cube-document.ts cubeOpenedAgain), the compiler typing its function as a relation then. Refused on a grid over
   *  no other grid's rows, one that reads its snap, and one that is so already. */
  | { readonly kind: 'setPure'; readonly grid: string; readonly pure: Lambda | null }
  /** A flow's box set to write its rows to a file (an Output, docs/DATACUBE_FLOW_MODE_2026_10_24.md M6: when told to,
   *  and when the flow runs), or (null) no longer. */
  | { readonly kind: 'setOutput'; readonly grid: string; readonly output: Output | null }
  /** A flow's Test box set to stop a run when its checks fail, before the run writes any Output (FL8's refuse: Python's
   *  grid.stops_run), or (false) to warn: the run writes on. */
  | { readonly kind: 'setStops'; readonly grid: string; readonly stops: boolean }
  /** The page's model made by its first model action (Create a Class…, a Data Space): its project, DataCube's workspace
   *  in it, the package its classes go in (modelMade, above). */
  | { readonly kind: 'modelInto'; readonly project: string; readonly workspace: string; readonly package: string }
  /** The page pinned to the revision its last model action saved (modelPinned, above). */
  | { readonly kind: 'modelPin'; readonly revision: string }
  /** The page's model keeping a grid's tables and its class -- null leaving either as it is (modelKept, above). */
  | { readonly kind: 'modelKeep'; readonly cube: string; readonly tables: readonly string[] | null; readonly class: string | null };

export const PAGE_COMMAND_KINDS = [
  'renamePage', 'savedAs', 'addSheet', 'addFlow', 'placeBox', 'renameSheet', 'moveSheet', 'removeSheet', 'renameTile', 'addGrid',
  'addChart', 'setChart',
  'addText', 'setText', 'addTimeseries', 'setTimeseries', 'setTextFiles', 'followGrid', 'detach', 'select', 'moveTile', 'stack', 'removeTile',
  'arrange', 'layout', 'setPageFilter', 'removePageFilter', 'snap', 'live', 'rebuilt', 'setInput', 'setJoin', 'setPure', 'setOutput', 'setStops',
  'modelInto', 'modelPin', 'modelKeep',
] as const satisfies readonly PageCommand['kind'][];

/** Each page command's fields, by kind (`?` one it may leave out): what readPageCommand takes, and what Python's generated
 *  builders write (D17). */
export const PAGE_COMMAND_FIELDS = {
  renamePage: ['name'], savedAs: ['record'], addSheet: ['id?', 'name'], addFlow: ['id?', 'name'], placeBox: ['tile', 'x', 'y', 'frame?'],
  renameSheet: ['sheet', 'name'], moveSheet: ['sheet', 'to'],
  removeSheet: ['sheet'], renameTile: ['tile', 'title'], addGrid: ['id?', 'document', 'sheet', 'near', 'title', 'columns'],
  addChart: ['id?', 'near', 'spec', 'title', 'columns'], setChart: ['chart', 'spec'], addText: ['id?', 'sheet', 'text', 'title', 'columns'],
  setText: ['tile', 'text'], setTextFiles: ['tile', 'files'], addTimeseries: ['id?', 'sheet', 'near', 'spec', 'title', 'columns', 'kept?'],
  setTimeseries: ['tile', 'spec', 'kept?'], followGrid: ['chart', 'grid'], detach: ['chart', 'cube', 'document', 'copy?'],
  select: ['chart', 'conditions'], moveTile: ['tile', 'sheet', 'columns'], stack: ['tiles'], removeTile: ['tile'],
  arrange: ['sheet', 'preset', 'order'], layout: ['sheet', 'layout'], setPageFilter: ['id?', 'class', 'condition', 'from'],
  removePageFilter: ['filter'], snap: ['grid', 'copy'], live: ['grid'], rebuilt: ['cube', 'source'], setInput: ['grid', 'side', 'input'], setJoin: ['grid', 'how', 'more?'], setPure: ['grid', 'pure'], setOutput: ['grid', 'output'], setStops: ['grid', 'stops'],
  modelInto: ['project', 'workspace', 'package'], modelPin: ['revision'], modelKeep: ['cube', 'tables', 'class'],
} as const satisfies CommandFields<PageCommand>;
// every field of every kind listed: a field the list leaves out is a type error naming it
const pageFieldsWhole: [MissingFields<PageCommand, typeof PAGE_COMMAND_FIELDS>] extends [never] ? true
  : MissingFields<PageCommand, typeof PAGE_COMMAND_FIELDS> = true;
void pageFieldsWhole;

/** A cube held for the timeseries tiles that read it, no grid showing it (addTimeseries's `kept`). */
export interface KeptCube {
  readonly cube: string;
  readonly document: CubeDocument;
  readonly copy: GridCopy | null;
}

/** What a command applied gives: the page, and the command as applied (its id allocated, when it made something). */
export interface PageApplied {
  readonly draft: PageDraft;
  readonly command: PageCommand;
}

// ---------------------------------------------------------------- ids and titles (page-ids.ts: the screen's too)

export { chartTitle, gridTitle, idTag, nextPageId, textTitle, timeseriesTitle } from './page-ids.ts';

// ---------------------------------------------------------------- applying

const named = (part: 'page' | 'tile' | 'sheet', text: string): string | undefined => {
  const given = givenName(part, text);
  if ('refused' in given) throw new CubeRefusal(given.refused);
  return given.name;
};

const saved = (layout: Bands): PageLayout => ({ kind: 'bands', ...asSaved(layout) });

/** Whether `q` is a Test's (checks.ts checksOf's first question: grouped by its column of one value, `ChecksAll`, as
 *  `x|'checks'`) -- read here without the checks' readers, which a page that has no Test never fetches
 *  (test/bundle-budget.test.ts); held to checks.ts's by test/flow-sheet.test.ts, a Test it makes set to stop a run. */
function testShaped(q: Pick<CubeShape, 'derived' | 'rows'>): boolean {
  if (q.rows.length !== 1 || q.rows[0] !== 'ChecksAll') return false;
  const all = q.derived.find((d) => d.name === 'ChecksAll')?.lambda?.body[0] as { _type?: string; value?: unknown } | undefined;
  return all?._type === 'string' && all.value === 'checks';
}

/** A flow op of layout/flow.ts, its refusal in its words. */
function flowed(op: () => Flow): FlowLayout {
  try {
    return { kind: 'flow', ...op() };
  } catch (e) {
    if (e instanceof FlowRefusal) throw new CubeRefusal(e.message);
    throw e;
  }
}

/** A tile put on a sheet: on a flow, a box after `after` (layout/flow.ts placeAfter: Alteryx's tool dropped on a box's
 *  output); on a board, as `onBoard` places it (bands.ts add, as each command's own rule asks). */
function placedOn(layout: PageLayout, tile: string, after: string | undefined, onBoard: (bands: Bands) => Bands): PageLayout {
  return layout.kind === 'flow' ? flowed(() => placeAfter(layout, tile, after)) : laid(() => onBoard(layout));
}

/** A tile taken off a sheet's layout: a flow's box, a board's tile by its bands' rule. */
function takenOff(layout: PageLayout, tile: string): PageLayout {
  return layout.kind === 'flow' ? { kind: 'flow', ...removeBox(layout, tile) } : laid(() => remove(layout, tile));
}

/** A command a flow sheet takes none of, refused in words: what goes there is a box (a grid), placed on its canvas. */
function onBoardOnly(sheet: PageSheet, what: string): void {
  if (sheet.layout.kind === 'flow') throw new CubeRefusal(`a flow sheet holds boxes, each a grid: ${what} goes on a board sheet`);
}

/** A layout op of bands.ts, its refusal in its words. */
function laid(op: () => Bands): PageLayout {
  try {
    return saved(op());
  } catch (e) {
    if (e instanceof NotALayout) throw new CubeRefusal(e.message);
    throw e;
  }
}

function sheetOf(page: PageDocument, tile: string): PageSheet | undefined {
  return page.sheets.find((s) => layoutTiles(s.layout).includes(tile));
}

function sheetNamed(page: PageDocument, id: string): PageSheet {
  const sheet = page.sheets.find((s) => s.id === id);
  if (!sheet) throw new CubeRefusal(`'${id}' is not a sheet of the page (${page.sheets.map((s) => s.id).join(', ')})`);
  return sheet;
}

function viewNamed(page: PageDocument, id: string): PageView {
  const view = page.views.find((v) => v.id === id);
  if (!view) throw new CubeRefusal(`'${id}' is not a tile of the page`);
  return view;
}

/**
 * A GRID OF THE PAGE AS A JOIN READS IT (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5): its result as the page has it
 * -- its cube's source and query -- under the name it shows, the title a person gave its tile, else what it reads. The
 * tab's Join… and Python's page.join ask it alike (join-flow.ts, headless joinCube). Refused in words: no tile of the
 * page, or a chart. The one rule of a grid as an input (live-inputs.ts gridInput, a live input's too): its source
 * resolved, so a join's copy of a grid over another grid's rows holds no live input (docs/DATACUBE_FLOW_MODE_2026_10_24.md
 * FL6).
 */
export function joinInputOf(page: PageDocument, grid: string): JoinInput {
  const view = viewNamed(page, grid);
  if (view.kind !== 'grid') throw new CubeRefusal(`'${grid}' is ${kindWords(view)}: a join is of two grids`);
  return gridInput(page, grid);
}

const withSheet = (page: PageDocument, id: string, change: (s: PageSheet) => PageSheet): PageDocument =>
  ({ ...page, sheets: page.sheets.map((s) => (s.id === id ? change(s) : s)) });

/**
 * The ids no command may give out: every one the page has held, its cubes' (a cube kept for a detached chart has no
 * view of its own: its id is taken all the same), and its filters' while it has them, whatever `held` lost (a page
 * opened again holds its tiles' ids only). page-between.ts asks it too, as it names a tab's commands under its tag.
 */
export function takenIds(draft: PageDraft): string[] {
  return [...draft.held, ...draft.page.cubes.map((c) => c.id), ...(draft.page.filters ?? []).map((f) => f.id)];
}

/**
 * A fresh id for a command that makes one, or the one it names -- refused when it is taken (takenIds).
 */
function freshId(draft: PageDraft, given: string | undefined, kind: 'grid' | 'chart' | 'text' | 'timeseries' | 'sheet' | 'filter'): string {
  const taken = takenIds(draft);
  if (given === undefined) return nextPageId(taken, kind);
  if (taken.includes(given)) throw new CubeRefusal(`'${given}' is an id the page has held already`);
  if (!idShape(kind).test(given)) throw new CubeRefusal(`a ${kind}'s id is ${kind}-N or ${kind}-<tag>-N, not '${given}'`);
  return given;
}

/** The page written again by its own writer (page-document.ts writePageOf): its name each cube's too, its model kept
 *  for the cubes it holds, a chart's click filter for the charts it holds. */
function rewritten(page: PageDocument,
  change: Partial<Pick<PageDocument, 'name' | 'cubes' | 'views' | 'sheets' | 'filters' | 'copies'>>): PageDocument {
  const next = { ...page, ...change };
  return writePageOf({
    name: next.name, cubes: next.cubes, views: { views: next.views, sheets: next.sheets },
    ...(next.model ? { model: next.model } : {}), ...(next.filters ? { filters: next.filters } : {}),
    ...(next.copies ? { copies: next.copies } : {}), ...(next.record !== undefined ? { record: next.record } : {}),
    ...(next.unknown ? { unknown: next.unknown } : {}),
  });
}

/** The grid view `grid` names, or a refusal: a chart reads its grid's rows, and snaps with it. */
/** What a view is, in a refusal's words: a grid, a chart, a text tile. */
const kindWords = (view: PageView): string => (view.kind === 'text' ? 'a text tile' : view.kind === 'timeseries' ? 'a timeseries tile' : `a ${view.kind}`);

/**
 * A timeseries' spec as a command gives it, and the page holding the cubes the command keeps for it (`kept`), or a
 * refusal: read by the tile's one reader (timeseries-spec.ts readTimeseries); each cube kept one a source reads, no grid
 * shows, and no tile or sheet is called -- made, or the one of its id replaced, its copy as given (no grid snaps or goes
 * live for it); then each source's cube one the page holds -- a grid's, or one kept for the tiles that read it, its grid
 * removed (the review's G4, docs/TIMESERIES_REVIEW_2026_10_25.md: the page reopened read a cube it no longer held,
 * when an id it had once held was a source). No other id is a source.
 */
function timeseriesSpec(draft: PageDraft, given: TimeseriesSpec, kept: readonly KeptCube[] | undefined):
  { readonly spec: TimeseriesSpec; readonly draft: PageDraft } {
  const spec = readTimeseries(JSON.parse(JSON.stringify(given)));
  if (typeof spec === 'string') throw new CubeRefusal(spec);
  let { page } = draft;
  for (const k of kept ?? []) {
    if (!spec.sources.some((s) => s.cube === k.cube)) throw new CubeRefusal(`no source of the timeseries reads ${k.cube}: a cube is kept for the tiles that read it`);
    if (page.views.some((v) => v.kind === 'grid' && v.cube === k.cube)) throw new CubeRefusal(`a grid shows ${k.cube}: its cube is the grid's`);
    if (page.views.some((v) => v.id === k.cube) || page.sheets.some((s) => s.id === k.cube)) {
      throw new CubeRefusal(`'${k.cube}' is a tile or a sheet of the page, not a cube`);
    }
    const cubes = page.cubes.some((c) => c.id === k.cube)
      ? page.cubes.map((c) => (c.id === k.cube ? { id: c.id, cube: k.document } : c))
      : [...page.cubes, { id: k.cube, cube: k.document }];
    const copies = { ...(page.copies ?? {}) };
    if (k.copy === null) delete copies[k.cube];
    else copies[k.cube] = k.copy;
    const { copies: _had, ...bare } = page;
    page = rewritten(bare, { cubes, ...(Object.keys(copies).length > 0 ? { copies } : {}) });
  }
  const held = new Set([...page.cubes.map((c) => c.id), ...page.views.flatMap((v) => (v.kind === 'grid' ? [v.cube] : []))]);
  for (const s of spec.sources) {
    if (!held.has(s.cube)) throw new CubeRefusal(`source ${s.alias} reads '${s.cube}', which is no cube of the page`);
  }
  return { spec, draft: { page, held: [...new Set([...draft.held, ...(kept ?? []).map((k) => k.cube)])] } };
}

function gridNamed(page: PageDocument, grid: string): GridView {
  const view = viewNamed(page, grid);
  if (view.kind !== 'grid') throw new CubeRefusal(`'${grid}' is ${kindWords(view)}: ${view.kind === 'chart' ? 'its grid snaps, and the chart reads what it reads' : 'a grid snaps'}`);
  return view;
}

/**
 * A PAGE FILTER as `setPageFilter` sets it, or why not: its class one a grid of the page reads (a class source), every
 * property it names one that class's grids have, its chart one of the page's over that class.
 */
function pageFilter(page: PageDocument, id: string, command: Extract<PageCommand, { kind: 'setPageFilter' }>): PageFilter {
  const sources = page.cubes.map((c) => c.cube.source).filter((s) => s._type === 'class' && s.class === command.class);
  const name = command.class.slice(command.class.lastIndexOf(':') + 1);
  if (sources.length === 0) throw new CubeRefusal(`no grid of the page reads ${command.class}: a page filter is on a class a grid reads`);
  const has = new Set(sources.flatMap((s) => s.columns.map((c) => c.name)));
  const missing = [...namedProperties(command.condition)].filter((p) => !has.has(p));
  if (missing.length > 0) {
    throw new CubeRefusal(`${name} has no ${missing.join(', ')} (its grids show ${[...has].join(', ')})`);
  }
  if (command.from !== null) {
    const chart = page.views.find((v) => v.id === command.from);
    if (chart?.kind !== 'chart') throw new CubeRefusal(`'${command.from}' is no chart of the page`);
    const over = page.cubes.find((c) => c.id === chart.cube)?.cube.source;
    if (over?._type !== 'class' || over.class !== command.class) throw new CubeRefusal(`'${command.from}' is no chart of a grid over ${name}`);
  }
  return { id, class: command.class, condition: command.condition, ...(command.from !== null ? { from: command.from } : {}) };
}

/** A chart's selection taken off its cube's filter (the screen's #select(id, null)), and off the chart. */
function unselected(page: PageDocument, chart: ChartView): PageDocument {
  const conditions: readonly FilterNode[] = chart.selection ?? [];
  if (conditions.length === 0) return page;
  const cubes = page.cubes.map((c) => {
    if (c.id !== chart.cube) return c;
    const { filter, ...query } = c.cube.query;
    const left = withoutConditions(filter, conditions);
    return { ...c, cube: { ...c.cube, query: left ? { ...query, filter: left } : query } };
  });
  const { selection: _was, ...rest } = chart;
  return { ...page, cubes, views: page.views.map((v) => (v.id === chart.id ? rest : v)) };
}

/**
 * A TILE OFF THE PAGE, as the screen takes one: a grid takes the charts following it, and each frozen one stays,
 * detached -- reading the grid's cube as it was, kept for it -- with no selection of its own (the screen clears it and
 * leaves the kept cube's filter as the grid had it); a chart takes its selection off its grid's filter (the screen's
 * #select(id, null)). A cube stays while a view reads it; the page's model keeps what its cubes still need (keptFor).
 */
function withoutTile(page: PageDocument, tile: string): PageDocument {
  const view = viewNamed(page, tile);
  let next = page;
  const gone = new Set([tile]);
  if (view.kind === 'chart') {
    next = unselected(next, view);
  } else if (view.kind === 'grid') {
    for (const chart of next.views.filter((v): v is ChartView => v.kind === 'chart' && v.cube === view.cube)) {
      if (!chart.spec.frozen) {
        gone.add(chart.id);
      } else if (chart.selection) {
        const { selection: _cleared, ...detached } = chart;
        next = { ...next, views: next.views.map((v) => (v.id === chart.id ? detached : v)) };
      }
    }
  }
  // a grid others read live: each of them reads a copy of what it was from now on (docs/DATACUBE_FLOW_MODE_2026_10_24.md
  // FL6), so no link is left to a grid the page no longer has
  if (view.kind === 'grid' && next.cubes.some((c) => liveGrids(c.cube.source).includes(view.id))) {
    const page0 = next;
    const copy = (port: InputPort | undefined): JoinInput => gridInput(page0, view.id, [], port);
    next = {
      ...next,
      cubes: next.cubes.map((c) => (liveGrids(c.cube.source).includes(view.id)
        ? { ...c, cube: { ...c.cube, source: frozenFrom(c.cube.source, view.id, copy) } } : c)),
    };
  }
  const views = next.views.filter((v) => !gone.has(v.id));
  const cubes = stillHeld(next.cubes, views);
  const sheets = next.sheets.map((s) => {
    let layout: PageLayout = s.layout;
    for (const id of gone) if (layoutTiles(layout).includes(id)) layout = takenOff(layout, id);
    return layout === s.layout ? s : { ...s, layout };
  });
  const left = rewritten({ ...next, ...(next.model ? { model: keptFor(next.model, cubes) } : {}) }, { views, cubes, sheets });
  // a chart's selection off its cube's filter: that cube's other charts judged by the filter now (the S2 audit's 5, F2)
  return view.kind === 'chart' ? followedPage(left, view.cube, page) : left;
}

/**
 * The views and cubes in the order the screen writes them (page/cube-page.ts views(), page-app.ts document()): the grids
 * in the sheets' reading order -- the order a reopened page adds them in -- then the charts, each in the order it came,
 * then the text tiles in reading order; the cubes of the grids in their order, then those kept for detached charts. A
 * page Python edits is written as the screen writes it.
 */
export function inScreenOrder(page: PageDocument): PageDocument {
  const reading = page.sheets.flatMap((s) => layoutTiles(s.layout));
  const at = (id: string): number => (reading.includes(id) ? reading.indexOf(id) : reading.length);
  const inReading = (kind: PageView['kind']): PageView[] => page.views.filter((v) => v.kind === kind).map((v, i) => ({ v, i }))
    .sort((x, y) => at(x.v.id) - at(y.v.id) || x.i - y.i).map((x) => x.v);
  const views = [...inReading('grid'), ...page.views.filter((v) => v.kind === 'chart'), ...inReading('text'), ...inReading('timeseries')];
  const gridOrder = views.flatMap((v) => (v.kind === 'grid' ? [v.cube] : []));
  const place = (id: string): number => (gridOrder.includes(id) ? gridOrder.indexOf(id) : gridOrder.length);
  const cubes = page.cubes.map((c, i) => ({ c, i })).sort((x, y) => place(x.c.id) - place(y.c.id) || x.i - y.i).map((x) => x.c);
  const same = views.every((v, i) => v === page.views[i]) && cubes.every((c, i) => c === page.cubes[i]);
  return same ? page : { ...page, views, cubes };
}

/** A command applied: the page as the screen's rules leave it, or a refusal in their words (CubeRefusal). */
export function applyPageCommand(draft: PageDraft, command: PageCommand): PageApplied {
  const page = draft.page;
  const done = (next: PageDocument, held: readonly string[] = draft.held, applied: PageCommand = command): PageApplied =>
    ({ draft: { page: inScreenOrder(next), held }, command: applied });
  switch (command.kind) {
    case 'renamePage': {
      const name = named('page', command.name);
      if (name === undefined) throw new Error("a page's name is never left to what it shows");
      return done(rewritten(page, { name }));
    }
    case 'savedAs': {
      const { record: _was, ...rest } = page;
      return done(command.record === null ? rest : { ...page, record: command.record });
    }
    case 'addSheet': {
      const id = freshId(draft, command.id, 'sheet');
      const name = command.name === null ? undefined : named('sheet', command.name);
      const sheet: PageSheet = { id, ...(name !== undefined ? { name } : {}), layout: saved(NEW_SHEET) };
      return done({ ...page, sheets: [...page.sheets, sheet] }, [...draft.held, id], { ...command, id });
    }
    case 'addFlow': {
      const id = freshId(draft, command.id, 'sheet');
      const name = command.name === null ? undefined : named('sheet', command.name);
      const sheet: PageSheet = { id, ...(name !== undefined ? { name } : {}), layout: { kind: 'flow', ...EMPTY_FLOW } };
      return done({ ...page, sheets: [...page.sheets, sheet] }, [...draft.held, id], { ...command, id });
    }
    case 'placeBox': {
      const view = viewNamed(page, command.tile);
      const sheet = sheetOf(page, command.tile);
      if (!sheet) throw new CubeRefusal(`'${command.tile}' is on no sheet`);
      const layout = sheet.layout;
      if (layout.kind !== 'flow') throw new CubeRefusal(`'${command.tile}' is a tile of a board, placed by its layout: a box is a flow sheet's`);
      if (command.frame !== undefined && command.frame !== null && view.kind !== 'text') {
        throw new CubeRefusal(`'${command.tile}' is ${kindWords(view)}: a container is a comment`);
      }
      return done(withSheet(page, sheet.id, (s) => ({
        ...s, layout: flowed(() => moveBox(layout, command.tile, command.x, command.y, command.frame)),
      })));
    }
    case 'renameSheet': {
      const sheet = sheetNamed(page, command.sheet);
      const given = command.name === null ? undefined : named('sheet', command.name);
      // a name equal to what the sheet shows without one is none, as a tile's made title is (B2; the S7 audit's P-7: the
      // screen's tab told nothing, Python's pinned it)
      const name = given !== undefined && given === unnamedLabel(page, sheet) ? undefined : given;
      return done(withSheet(page, command.sheet, ({ name: _was, ...s }) => (name !== undefined ? { ...s, name } : s)));
    }
    case 'moveSheet': {
      const sheet = sheetNamed(page, command.sheet);
      if (!Number.isInteger(command.to)) throw new CubeRefusal(`a sheet's place is a whole number, not ${command.to}`);
      const others = page.sheets.filter((s) => s !== sheet);
      const at = Math.max(0, Math.min(others.length, command.to));
      return done({ ...page, sheets: [...others.slice(0, at), sheet, ...others.slice(at)] });
    }
    case 'removeSheet': {
      const sheet = sheetNamed(page, command.sheet);
      if (page.sheets.length === 1) throw new CubeRefusal('a page has one sheet at least: its last is not removed');
      // its grids first: a chart of one, on this sheet or another, goes with it or stays detached
      let next = page;
      const on = (): string[] => layoutTiles(sheetNamed(next, sheet.id).layout);
      for (const tile of on().filter((t) => viewNamed(next, t).kind === 'grid')) next = withoutTile(next, tile);
      for (const tile of on()) next = withoutTile(next, tile);
      return done(rewritten(next, { sheets: next.sheets.filter((s) => s.id !== sheet.id) }));
    }
    case 'renameTile': {
      const view = viewNamed(page, command.tile);
      if (command.title === null && view.kind === 'chart') throw new CubeRefusal('a chart has a title');
      const given = command.title === null ? undefined : named('tile', command.title);
      // a grid or a text named what it was made named keeps no title of its own: it is numbered by its id, as it was
      const made = view.kind === 'grid' ? gridTitle(view.id) : view.kind === 'text' ? textTitle(view.id)
        : view.kind === 'timeseries' ? timeseriesTitle(view.id) : undefined;
      const title = given === made ? undefined : given;
      const views = page.views.map((v) => {
        if (v.id !== view.id) return v;
        if (title !== undefined) return { ...v, title };
        const { title: _was, ...rest } = v as PageView & { title?: string };
        return rest as PageView;
      });
      return done({ ...page, views });
    }
    case 'addGrid': {
      const id = freshId(draft, command.id, 'grid');
      const sheet = sheetNamed(page, command.sheet);
      const given = command.title === null ? undefined : named('tile', command.title);
      // named what it is numbered, it keeps no title of its own, as renameTile leaves one (2026-10-24, found by the
      // undo's random walks: a grid made 'Grid 1' was a page no rename put back)
      const title = given === gridTitle(id) ? undefined : given;
      const view: PageView = { id, kind: 'grid', cube: id, ...(title !== undefined ? { title } : {}) };
      // a grid is a table, which wants the page's width: a band of its own at the bottom of its sheet, never beside the
      // tile it came from, nor between that tile and its charts (the user, 2026-10-23: bands.ts add)
      // on a flow, a box after the one it came from (`near`)
      const placed = withSheet(page, sheet.id, (s) => ({
        ...s, layout: placedOn(s.layout, id, command.near ?? undefined, (bands) => add(bands, id, undefined, command.columns ?? MAX_COLUMNS)),
      }));
      const cubes = command.document === null ? page.cubes : [...page.cubes, { id, cube: command.document }];
      return done(rewritten(placed, { cubes, views: [...page.views, view] }),
        [...draft.held, id], { ...command, id });
    }
    case 'addChart': {
      const grid = viewNamed(page, command.near);
      if (grid.kind !== 'grid') throw new CubeRefusal(`'${command.near}' is ${kindWords(grid)}: a chart is of a grid`);
      const spec = readChart(JSON.parse(JSON.stringify(command.spec)));
      if (typeof spec === 'string') throw new CubeRefusal(spec);
      const id = freshId(draft, command.id, 'chart');
      const title = command.title === null ? chartTitle(id) : named('tile', command.title) ?? chartTitle(id);
      const view: ChartView = { id, kind: 'chart', cube: grid.cube, title, spec };
      const sheet = sheetOf(page, grid.id);
      if (!sheet) throw new CubeRefusal(`'${grid.id}' is on no sheet`);
      onBoardOnly(sheet, 'a chart');
      // under its grid, beside the charts already there while that row has room (bands.ts add)
      const charts = (ids: readonly string[]): boolean => ids.every((t) => page.views.find((v) => v.id === t)?.kind === 'chart');
      const placed = withSheet(page, sheet.id, (s) => ({
        ...s, layout: placedOn(s.layout, id, undefined, (bands) => add(bands, id, grid.id, command.columns ?? MAX_COLUMNS, charts)),
      }));
      // a live chart follows its grid from the start, by the one rule every change of the cube follows it by (V1, P6):
      // a spec made for the grid is where following leaves it; one made for another grid (Python's, a page's own) is
      // regrouped as the grid is, as the next change of the grid would regroup it (D19, 2026-10-24)
      // as applied, its title is the one written (a new chart's number): a redo or a write made again on top, giving the
      // chart a new id, keeps the title the person saw (D19, B12)
      return done(followedPage({ ...placed, views: [...page.views, view] }, grid.cube, page), [...draft.held, id], { ...command, id, title });
    }
    case 'addText': {
      const problem = textProblem(command.text);
      if (problem !== null) throw new CubeRefusal(problem);
      const id = freshId(draft, command.id, 'text');
      // on a flow, a comment: placed as a box is (docs/DATACUBE_FLOW_MODE_2026_10_24.md M7)
      const sheet = sheetNamed(page, command.sheet);
      const given = command.title === null ? undefined : named('tile', command.title);
      const title = given === textTitle(id) ? undefined : given;
      const view: TextView = { id, kind: 'text', text: command.text, ...(title !== undefined ? { title } : {}) };
      // a band of its own at the bottom of its sheet, as a grid is added (bands.ts add): a title or a note is moved where
      // it reads from there
      const placed = withSheet(page, sheet.id, (s) => ({
        ...s, layout: placedOn(s.layout, id, undefined, (bands) => add(bands, id, undefined, command.columns ?? MAX_COLUMNS)),
      }));
      return done({ ...placed, views: [...page.views, view] }, [...draft.held, id], { ...command, id });
    }
    case 'setText': {
      const view = viewNamed(page, command.tile);
      if (view.kind !== 'text') throw new CubeRefusal(`'${command.tile}' is ${kindWords(view)}, not a text tile`);
      const problem = textProblem(command.text);
      if (problem !== null) throw new CubeRefusal(problem);
      return done({ ...page, views: page.views.map((v) => (v.id === view.id ? { ...view, text: command.text } : v)) });
    }
    case 'addTimeseries': {
      const { spec, draft: holding } = timeseriesSpec(draft, command.spec, command.kept);
      const on = holding.page;
      const sheet = sheetNamed(on, command.sheet);
      // a board's tile, as a chart is: a flow sheet holds boxes
      onBoardOnly(sheet, 'a timeseries');
      if (command.near !== null && !layoutTiles(sheet.layout).includes(viewNamed(on, command.near).id)) {
        throw new CubeRefusal(`'${command.near}' is not on ${sheet.id}: a timeseries goes under a tile of its own sheet`);
      }
      const id = freshId(draft, command.id, 'timeseries');
      const given = command.title === null ? undefined : named('tile', command.title);
      const title = given === timeseriesTitle(id) ? undefined : given;
      const view: TimeseriesView = { id, kind: 'timeseries', spec, ...(title !== undefined ? { title } : {}) };
      // under the tile it came from, beside the charts there while the row has room (a chart's place); else a band of
      // its own at the sheet's bottom (a text's)
      const beside = (ids: readonly string[]): boolean => ids.every((t) => {
        const kind = on.views.find((v) => v.id === t)?.kind;
        return kind === 'chart' || kind === 'timeseries';
      });
      const placed = withSheet(on, sheet.id, (s) => ({
        ...s,
        layout: placedOn(s.layout, id, undefined, (bands) => (command.near === null ? add(bands, id, undefined, command.columns ?? MAX_COLUMNS)
          : add(bands, id, command.near, command.columns ?? MAX_COLUMNS, beside))),
      }));
      return done({ ...placed, views: [...on.views, view] }, [...holding.held, id], { ...command, id });
    }
    case 'setTimeseries': {
      const view = viewNamed(page, command.tile);
      if (view.kind !== 'timeseries') throw new CubeRefusal(`'${command.tile}' is ${kindWords(view)}, not a timeseries tile`);
      const { spec, draft: holding } = timeseriesSpec(draft, command.spec, command.kept);
      // a cube kept for the tile alone (its grid removed: G4) let go when it reads it no more, as a removal lets it go
      const on = holding.page;
      const views = on.views.map((v) => (v.id === view.id ? { ...view, spec } : v));
      const cubes = stillHeld(on.cubes, views);
      return done(rewritten({ ...on, ...(on.model ? { model: keptFor(on.model, cubes) } : {}) }, { views, cubes }), holding.held);
    }
    case 'setTextFiles': {
      const view = viewNamed(page, command.tile);
      if (view.kind !== 'text') throw new CubeRefusal(`'${command.tile}' is ${kindWords(view)}, not a text tile`);
      // by the one rule (text-files.ts readTextFiles): a list of named, sized, fingerprinted files of a kind it shows
      const files = readTextFiles(command.files);
      if (typeof files === 'string') throw new CubeRefusal(files);
      const { files: _was, ...rest } = view;
      const next: TextView = files.length > 0 ? { ...rest, files } : rest;
      return done({ ...page, views: page.views.map((v) => (v.id === view.id ? next : v)) });
    }
    case 'setChart': {
      const view = viewNamed(page, command.chart);
      if (view.kind !== 'chart') throw new CubeRefusal(`'${command.chart}' is ${kindWords(view)}, not a chart`);
      const spec = readChart(JSON.parse(JSON.stringify(command.spec)));
      if (typeof spec === 'string') throw new CubeRefusal(spec);
      // a chart detached from its grid (its grid removed: no grid shows its cube) has no grid to follow: it stays frozen,
      // on both sides -- the screen's pill is off for it (B8)
      if (spec.frozen !== true && !page.views.some((v) => v.kind === 'grid' && v.cube === view.cube)) {
        throw new CubeRefusal(`'${command.chart}' is detached -- its grid was removed, and it keeps its own copy of that grid's query -- so it has no grid to follow`);
      }
      // kept as the panel keeps it: a live chart regrouped as its grid is (P6)
      return done(followedPage({ ...page, views: page.views.map((v) => (v.id === view.id ? { ...view, spec } : v)) }, view.cube, page));
    }
    case 'followGrid': {
      const view = viewNamed(page, command.chart);
      if (view.kind !== 'chart') throw new CubeRefusal(`'${command.chart}' is ${kindWords(view)}, not a chart`);
      const grid = viewNamed(page, command.grid);
      if (grid.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(grid)}, not a grid: a chart follows a grid`);
      if (grid.cube === view.cube) throw new CubeRefusal(`'${command.chart}' reads ${command.grid} already`);
      // its selection off the cube it read (as a chart removed takes it off) and its page filter off (a click on nothing
      // takes both: the S7 audit's Y-1), then the chart on the grid's cube; the cube it read let go when no view reads it
      // any more (a detached chart's), and the page's model kept for what is left
      const picked = unselected(page, view);
      const unpicked = picked.filters?.some((f) => f.from === view.id)
        ? { ...picked, filters: picked.filters.filter((f) => f.from !== view.id) } : picked;
      const views = unpicked.views.map((v) => {
        if (v.id !== view.id || v.kind !== 'chart') return v;
        const { selection: _was, ...rest } = v;
        return { ...rest, cube: grid.cube };
      });
      const cubes = stillHeld(unpicked.cubes, views);
      const kept = rewritten({ ...unpicked, ...(unpicked.model ? { model: keptFor(unpicked.model, cubes) } : {}) }, { views, cubes });
      return done(followedPage(kept, grid.cube, page));
    }
    case 'detach': {
      const view = viewNamed(page, command.chart);
      if (view.kind !== 'chart') throw new CubeRefusal(`'${command.chart}' is a grid, not a chart`);
      if (page.views.some((v) => v.kind === 'grid' && v.cube === command.cube)) {
        throw new CubeRefusal(`a grid shows ${command.cube}: a chart beside a grid follows it (followGrid), a detached one reads a cube of its own`);
      }
      if (page.views.some((v) => v.id === command.cube) || page.sheets.some((s) => s.id === command.cube)) {
        throw new CubeRefusal(`'${command.cube}' is a tile or a sheet of the page, not a cube`);
      }
      // its selection off the cube it read and its page filter off, as followGrid takes them (a click on nothing), then the
      // chart on its own cube, frozen (a detached chart has no grid to follow: setChart's rule); that cube made under its
      // id, or the one of that id replaced; the cube it read let go when no view reads it any more; the model kept for
      // what is left
      const picked = unselected(page, view);
      const unpicked = picked.filters?.some((f) => f.from === view.id)
        ? { ...picked, filters: picked.filters.filter((f) => f.from !== view.id) } : picked;
      const views = unpicked.views.map((v) => {
        if (v.id !== view.id || v.kind !== 'chart') return v;
        const { selection: _was, ...rest } = v;
        return { ...rest, cube: command.cube, spec: v.spec.frozen === true ? v.spec : { ...v.spec, frozen: true } };
      });
      const read = stillHeld(unpicked.cubes, views);
      const cubes = read.some((c) => c.id === command.cube)
        ? read.map((c) => (c.id === command.cube ? { id: c.id, cube: command.document } : c))
        : [...read, { id: command.cube, cube: command.document }];
      // the copy the cube reads, as given (null: its live rows; the field absent: as it is, so another chart's cube is
      // not un-snapped by a detach that says nothing of it) -- a detached chart's cube has no grid to snap or go live
      // for it, so the command carries it (the D19 audit's W2 limit, closed; its third round's 4) -- and no copy of a
      // cube let go
      const copies = Object.fromEntries(Object.entries(unpicked.copies ?? {}).filter(([id]) => cubes.some((c) => c.id === id)));
      if (command.copy === null) delete copies[command.cube];
      else if (command.copy !== undefined) copies[command.cube] = command.copy;
      const { copies: _had, ...bare } = unpicked;
      const kept = rewritten({ ...bare, ...(unpicked.model ? { model: keptFor(unpicked.model, cubes) } : {}) },
        { views, cubes, ...(Object.keys(copies).length > 0 ? { copies } : {}) });
      // the cube it read, judged for its other charts (its selection came off that cube's filter), and the cube it reads
      // now followed by every chart on it (the document given is theirs now)
      return done(followedPage(followedPage(kept, view.cube, page), command.cube, page), [...new Set([...draft.held, command.cube])]);
    }
    case 'select': {
      const view = viewNamed(page, command.chart);
      if (view.kind !== 'chart') throw new CubeRefusal(`'${command.chart}' is ${kindWords(view)}, not a chart`);
      const { selection: _was, ...rest } = view;
      const chart: ChartView = command.conditions.length === 0 ? rest : { ...rest, selection: [...command.conditions] };
      return done({ ...page, views: page.views.map((v) => (v.id === view.id ? chart : v)) });
    }
    case 'moveTile': {
      const view = viewNamed(page, command.tile);
      const from = sheetOf(page, command.tile);
      const to = sheetNamed(page, command.sheet);
      // onto its own sheet: nothing, as the screen's Move to Sheet offers only the others
      if (!from || from.id === to.id) return done(page);
      // a box goes from flow to flow, a board's tile from board to board: a grid shown on a board runs, a flow's box is a
      // document until it is shown (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1's design, 3)
      if (from.layout.kind !== to.layout.kind) {
        throw new CubeRefusal(from.layout.kind === 'flow'
          ? `'${command.tile}' is a box of a flow: it moves to another flow sheet, not onto a board`
          : `a flow sheet holds boxes, each a grid that does one step: ${kindWords(view)} stays on a board`);
      }
      const out = withSheet(page, from.id, (s) => ({ ...s, layout: takenOff(s.layout, command.tile) }));
      return done(withSheet(out, to.id, (s) => ({
        ...s, layout: placedOn(s.layout, command.tile, undefined, (bands) => add(bands, command.tile, undefined, command.columns ?? MAX_COLUMNS)),
      })));
    }
    case 'stack': {
      if (new Set(command.tiles).size < 2) throw new CubeRefusal('a stack is two tiles or more');
      for (const t of command.tiles) viewNamed(page, t);
      const sheet = sheetOf(page, command.tiles[0]!);
      if (!sheet || command.tiles.some((t) => sheetOf(page, t) !== sheet)) {
        throw new CubeRefusal('the tiles of a stack are on one sheet (move them there first)');
      }
      const layout = sheet.layout;
      if (layout.kind === 'flow') throw new CubeRefusal("a flow sheet's boxes are not stacked: each is a place of its own on the canvas");
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: laid(() => stackTiles(layout, command.tiles)) })));
    }
    case 'removeTile':
      return done(withoutTile(page, command.tile));
    case 'arrange': {
      const sheet = sheetNamed(page, command.sheet);
      const order = command.order;
      const layout = sheet.layout;
      if (layout.kind === 'flow') throw new CubeRefusal("a board's arrangements are a board's: a flow sheet's boxes are placed on its canvas");
      return done(withSheet(page, sheet.id, () => ({
        ...sheet, layout: laid(() => (order === null ? arrange(layout, command.preset as Preset) : arrange(layout, command.preset as Preset, order))),
      })));
    }
    case 'layout': {
      const sheet = sheetNamed(page, command.sheet);
      // read as a saved page's sheet is (page-document.ts readLayout): its shape, its tiles views of the page, its bands'
      // rules -- a layout the page's reader would refuse never reaches the page
      let read: PageLayout;
      try {
        read = readLayout(command.layout, new Set(page.views.map((v) => v.id)), 'the layout');
      } catch (e) {
        if (e instanceof PageDocumentError) throw new CubeRefusal(e.message);
        throw e;
      }
      // a board's layout is a board's, a flow's a flow's: a sheet is not turned into the other by its layout
      if (read.kind !== sheet.layout.kind) {
        throw new CubeRefusal(`'${sheet.id}' is ${sheet.layout.kind === 'flow' ? 'a flow sheet' : 'a board'}: its layout is ${sheet.layout.kind === 'flow' ? 'boxes' : 'bands'}`);
      }
      const had = [...layoutTiles(sheet.layout)].sort();
      const has = [...layoutTiles(read)].sort();
      if (JSON.stringify(had) !== JSON.stringify(has)) {
        throw new CubeRefusal(`a sheet's layout holds its tiles, each once: ${had.join(', ') || 'none'}, not ${has.join(', ') || 'none'}`);
      }
      const now: PageLayout = read.kind === 'flow' ? read : saved(read);
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: now })));
    }
    case 'setPageFilter': {
      const filters = page.filters ?? [];
      const was = command.id !== undefined ? filters.find((f) => f.id === command.id) : undefined;
      // its own id again replaces it; another given is a fresh one, as a tile's is
      const id = was ? was.id : freshId(draft, command.id, 'filter');
      const filter = pageFilter(page, id, command);
      const next = was ? filters.map((f) => (f.id === id ? filter : f)) : [...filters, filter];
      return done(rewritten(page, { filters: next }), was ? draft.held : [...draft.held, id], { ...command, id });
    }
    case 'removePageFilter': {
      const filters = page.filters ?? [];
      if (!filters.some((f) => f.id === command.filter)) {
        throw new CubeRefusal(`'${command.filter}' is not a filter of the page${filters.length ? ` (${filters.map((f) => f.id).join(', ')})` : ''}`);
      }
      const left = filters.filter((f) => f.id !== command.filter);
      const { filters: _all, ...rest } = page;
      return done(rewritten(left.length > 0 ? { ...rest, filters: left } : rest, {}));
    }
    case 'snap': {
      const view = gridNamed(page, command.grid);
      return done(rewritten(page, { copies: { ...(page.copies ?? {}), [view.cube]: command.copy } }));
    }
    case 'live': {
      const view = gridNamed(page, command.grid);
      const copies = page.copies ?? {};
      if (copies[view.cube] === undefined) throw new CubeRefusal(`'${command.grid}' reads its live rows already`);
      const left = Object.fromEntries(Object.entries(copies).filter(([cube]) => cube !== view.cube));
      const { copies: _all, ...rest } = page;
      return done(rewritten(Object.keys(left).length > 0 ? { ...rest, copies: left } : rest, {}));
    }
    case 'rebuilt': {
      const held = page.cubes.find((c) => c.id === command.cube);
      if (held === undefined) throw new CubeRefusal(`'${command.cube}' is no grid's cube of the page`);
      if (page.copies?.[command.cube] !== undefined) {
        throw new CubeRefusal(`'${command.cube}' reads its snap: its source does not move it until it goes live`);
      }
      const was = held.cube.source;
      const now = command.source;
      if (was._type !== now._type || was.name !== now.name) {
        throw new CubeRefusal(`'${command.cube}' reads ${was.name}: a cube rebuilt reads the same source, not ${now.name}`);
      }
      // opened over the source as it is now, the cube as the page holds it now (another writer's edits kept)
      const { document } = cubeOpenedAgain(held.cube, now);
      return done(followedPage(rewritten(page, { cubes: page.cubes.map((c) => (c.id === command.cube ? { id: c.id, cube: document } : c)) }),
        command.cube, page));
    }
    case 'setInput': {
      const view = viewNamed(page, command.grid);
      if (view.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(view)}: a grid reads another grid's rows`);
      const held = page.cubes.find((c) => c.id === view.cube);
      if (held === undefined) throw new CubeRefusal(`'${command.grid}' shows no cube the page writes down`);
      if (page.copies?.[view.cube] !== undefined) {
        throw new CubeRefusal(`'${command.grid}' reads its snap: its input moves it once it goes live`);
      }
      const source = held.cube.source;
      // a Union's next side (more, one past its last): an input added, where nothing was
      const k = moreIndex(command.side);
      const adds = source._type === 'join' && k !== undefined && k === (source.more?.length ?? 0) + 1;
      const was = adds ? undefined : inputOn(source, command.side);
      let input: CubeInput;
      if (command.input !== null) input = command.input;
      else if (was !== undefined && isLive(was)) input = gridInput(page, was.grid, [], was.port);
      else throw new CubeRefusal(was === undefined ? `'${command.grid}' has no ${command.side} to freeze: it is added live or as a copy`
        : `'${command.grid}' reads a copy already: it is frozen`);
      if (sameValue(was, input)) throw new CubeRefusal(`'${command.grid}' reads ${isLive(input) ? `'${input.grid}'` : 'that copy'} already`);
      const next = rewritten(page, {
        cubes: page.cubes.map((c) => (c.id === held.id ? { id: c.id, cube: { ...c.cube, source: withInputOn(source, command.side, input) } } : c)),
      });
      // a grid of the page, never one that reads its own rows (live-inputs.ts liveProblem: the page's reader's rule)
      const problem = liveProblem(next);
      if (problem !== undefined) throw new CubeRefusal(problem);
      return done(next);
    }
    case 'setJoin': {
      const view = viewNamed(page, command.grid);
      if (view.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(view)}: a join is a grid's`);
      const held = page.cubes.find((c) => c.id === view.cube);
      if (held === undefined) throw new CubeRefusal(`'${command.grid}' shows no cube the page writes down`);
      const source = held.cube.source;
      if (source._type !== 'join') throw new CubeRefusal(`'${command.grid}' reads ${source.name}: it is no join`);
      if (page.copies?.[view.cube] !== undefined) {
        throw new CubeRefusal(`'${command.grid}' reads its snap: its join moves it once it goes live`);
      }
      const more = command.more ?? source.more ?? [];
      if (more.length > 0 && command.how.form !== 'append') {
        throw new CubeRefusal(`'${command.grid}' would join ${2 + more.length} inputs: only a Union takes more than two`);
      }
      if (sameValue(source.how, command.how) && sameValue(source.more ?? [], more)) throw new CubeRefusal(`'${command.grid}' joins so already`);
      const { more: _had, ...two } = source;
      const joined: JoinSource = { ...two, how: command.how, ...(more.length > 0 ? { more } : {}) };
      const next = rewritten(page, {
        cubes: page.cubes.map((c) => (c.id === held.id ? { id: c.id, cube: { ...c.cube, source: joined } } : c)),
      });
      // a box reading its L or R reads it still, an input added is a grid of the page that does not read it (live-inputs.ts
      // liveProblem: the page's reader's rule)
      const problem = liveProblem(next);
      if (problem !== undefined) throw new CubeRefusal(problem);
      return done(next);
    }
    case 'setPure': {
      const view = viewNamed(page, command.grid);
      if (view.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(view)}: a Pure box is a grid's`);
      const held = page.cubes.find((c) => c.id === view.cube);
      if (held === undefined) throw new CubeRefusal(`'${command.grid}' shows no cube the page writes down`);
      const source = held.cube.source;
      if (source._type !== 'rows') throw new CubeRefusal(`'${command.grid}' reads ${source.name}: a Pure box reads another grid's rows`);
      if (page.copies?.[view.cube] !== undefined) {
        throw new CubeRefusal(`'${command.grid}' reads its snap: its Pure moves it once it goes live`);
      }
      const problem = command.pure === null ? undefined : pureProblem(command.pure);
      if (problem !== undefined) throw new CubeRefusal(problem);
      if (sameValue(source.pure ?? null, command.pure)) {
        throw new CubeRefusal(`'${command.grid}' ${command.pure === null ? 'has no Pure' : 'is so already'}`);
      }
      const { pure: _had, ...plain } = source;
      const next: RowsSource = command.pure === null ? plain : { ...plain, pure: command.pure };
      return done(rewritten(page, {
        cubes: page.cubes.map((c) => (c.id === held.id ? { id: c.id, cube: { ...c.cube, source: next } } : c)),
      }));
    }
    case 'setOutput': {
      const view = viewNamed(page, command.grid);
      if (view.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(view)}: an output is a grid's rows`);
      const sheet = sheetOf(page, command.grid);
      const layout = sheet?.layout;
      if (sheet === undefined || layout?.kind !== 'flow') throw new CubeRefusal(`'${command.grid}' is no box of a flow: an output is a flow's`);
      const had = layout.boxes.find((b) => b.tile === command.grid)?.output;
      if (sameValue(had ?? null, command.output)) throw new CubeRefusal(`'${command.grid}' ${command.output === null ? 'writes no file' : `writes ${command.output.name} already`}`);
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: flowed(() => setOutput(layout, command.grid, command.output)) })));
    }
    case 'setStops': {
      const view = viewNamed(page, command.grid);
      if (view.kind !== 'grid') throw new CubeRefusal(`'${command.grid}' is ${kindWords(view)}: a Test is a grid's`);
      const sheet = sheetOf(page, command.grid);
      const layout = sheet?.layout;
      if (sheet === undefined || layout?.kind !== 'flow') throw new CubeRefusal(`'${command.grid}' is no box of a flow: a run is a flow's`);
      const had = layout.boxes.find((b) => b.tile === command.grid)?.stops === true;
      if (had === command.stops) throw new CubeRefusal(`'${command.grid}' ${command.stops ? 'stops a run already' : 'stops no run'}`);
      // what a run is stopped on: a Test's checks, or a fixed Cross Tab's (its every value a column) -- any other box is
      // never judged, so would stop every run (the review of 2026-10-26: it took any box)
      const q = page.cubes.find((c) => c.id === view.cube)?.cube.query;
      const fixed = q !== undefined && q.pivotOn.length === 1 && (q.pivotValues ?? []).length > 0;
      if (command.stops && (q === undefined || (!testShaped(q) && !fixed))) {
        throw new CubeRefusal(`'${command.grid}' checks nothing: a run is stopped by a Test, or a Cross Tab with its columns fixed`);
      }
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: flowed(() => setStops(layout, command.grid, command.stops)) })));
    }
    case 'modelInto':
      return done(rewritten({ ...page, model: modelMade(page.model, command.project, command.workspace, command.package) }, {}));
    case 'modelPin':
      return done(rewritten({ ...page, model: modelPinned(page.model, command.revision) }, {}));
    case 'modelKeep':
      if (!page.cubes.some((c) => c.id === command.cube)) throw new CubeRefusal(`'${command.cube}' is no grid's cube of the page`);
      return done(rewritten({ ...page, model: modelKept(page.model, command.cube, command.tables, command.class) }, {}));
  }
}

/**
 * Commands applied to a page's state (page-state.ts) as one step named `label` -- undo takes them all back -- or, `step`
 * false, as none (a snap or going live: where a grid's rows are, not what the page is); the commands as applied
 * returned; a refusal changes nothing. What every board's owner applies a page edit by (page/page-app.ts, a cube
 * alone's in app.ts).
 */
export function applyToState(state: PageState, commands: readonly PageCommand[], label: string, step = true): PageCommand[] {
  const before = state.draft;
  const { draft, applied } = applyPageCommands(before, commands);
  if (step) state.step(label, applied, before, draft);
  else state.keep(draft);
  return applied;
}

/** Commands applied in order, each as applied: the first refusal stops them, in its words. */
export function applyPageCommands(draft: PageDraft, commands: readonly PageCommand[]): { draft: PageDraft; applied: PageCommand[] } {
  let d = draft;
  const applied: PageCommand[] = [];
  for (const c of commands) {
    const one = applyPageCommand(d, c);
    d = one.draft;
    applied.push(one.command);
  }
  return { draft: d, applied };
}

/**
 * Every id a page holds now: its tiles', its sheets' and its cubes' -- a cube kept for a detached chart has no view, and
 * its id is taken (a draft's `held` starts from these, and grows).
 */
export function idsOf(page: PageDocument): string[] {
  return [...new Set([...page.views.map((v) => v.id), ...page.cubes.map((c) => c.id), ...page.sheets.map((s) => s.id)])];
}

// ---------------------------------------------------------------- reading a command from JSON

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

function fields(o: Record<string, unknown>, kind: string, listed: readonly string[]): void {
  const allowed = fieldNames(listed);
  const odd = Object.keys(o).filter((k) => k !== 'kind' && !allowed.includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a '${kind}' command takes ${allowed.join(', ')}, not ${odd.join(', ')}`);
}

function text(v: unknown, what: string): string {
  if (typeof v !== 'string') throw new CubeRefusal(`${what} is text, not ${JSON.stringify(v)}`);
  return v;
}

const textOrNull = (v: unknown, what: string): string | null => (v === null ? null : text(v, what));

const optionalId = (v: unknown, what: string): { id?: string } => (v === undefined ? {} : { id: text(v, what) });

/**
 * A PAGE AFTER ITS CUBE `cube` CHANGED (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S2: V1, V2, P6), from the page
 * `before` the change, by one rule wherever a cube changes -- a page command's rule, a writer's grid edits made again on
 * top (page-rebase.ts), a host's grid edit (headless pageWithCube) -- and the screen's own following, which is this
 * rule's two halves: each live chart of the cube regrouped as the cube is now (chart-spec.ts followCube: ui/chart-panel.ts
 * refresh), and, when the cube's filter moved, each chart's selection the filter no longer holds taken off (cube-page.ts
 * reconcile). A selection is judged only then: one told before its click's refilter has landed (that grid's change, told
 * when it commits) is still the chart's while the filter catches up (the S2 audit's 1, F1). The cube read as its
 * document holds it: its query.
 */
export function followedPage(page: PageDocument, cube: string, before: PageDocument): PageDocument {
  const held = page.cubes.find((c) => c.id === cube);
  if (held === undefined) return page;
  const shape: CubeShape = { ...held.cube.query, epoch: 0 };
  const was = before.cubes.find((c) => c.id === cube)?.cube.query.filter;
  const judged = JSON.stringify(was ?? null) !== JSON.stringify(shape.filter ?? null);
  let moved = false;
  const views = page.views.map((v) => {
    if (v.kind !== 'chart' || v.cube !== cube) return v;
    const spec = followCube(v.spec, shape);
    const kept = !judged || v.selection === undefined || v.selection.length === 0
      || withoutConditions(shape.filter, v.selection, true) !== null;
    if (spec === v.spec && kept) return v;
    moved = true;
    const { selection: _gone, ...unselected } = v;
    return { ...(kept ? v : unselected), spec };
  });
  return moved ? { ...page, views } : page;
}

/**
 * THE GRIDS' REFILTERS A PAGE PUT AT A STEP OF ITS HISTORY NEEDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md G8): a step
 * holds its charts' selections, and its grids keep their own history -- so a chart brought back with a selection its
 * grid's filter (`filterOf`: the grid as it is now) no longer holds has it put back by that grid's own command, as a
 * click puts it there (G7's rule: the page's select, the grid's refilter). By grid tile, in the page's order.
 */
export function selectionRefilters(page: PageDocument, filterOf: (cube: string) => FilterNode | undefined):
  { readonly grid: string; readonly command: GridCommand }[] {
  return page.views.flatMap((v) => {
    if (v.kind !== 'chart' || v.selection === undefined || v.selection.length === 0) return [];
    const grid = page.views.find((g) => g.kind === 'grid' && g.cube === v.cube);
    if (grid === undefined || withoutConditions(filterOf(v.cube), v.selection, true) !== null) return [];
    return [{ grid: grid.id, command: { kind: 'refilter', remove: [], add: [...v.selection] } as const }];
  });
}

/**
 * WHAT A PAGE OPENED AGAIN TELLS OF ITS CUBES OPENED OVER THEIR FRAMES AS THEY ARE NOW (P11): `opened`, the page as the
 * screen writes it once open; `taken`, the cubes opened as another than the page held (cube-document.ts cubeTaken). Each
 * its `rebuilt`, with its source as it opened -- but a snapped grid's, which reads its copy and is not opened over its
 * frames (`held`'s copies: the page as it was given; the S1 audit's 6).
 */
export function rebuiltCommands(held: PageDocument, opened: PageDocument, taken: readonly string[]): PageCommand[] {
  return taken.flatMap((cube) => {
    const now = opened.cubes.find((c) => c.id === cube);
    if (now === undefined || !opensAgain(held, cube)) return [];
    return [{ kind: 'rebuilt', cube, source: now.cube.source }];
  });
}

/**
 * WHETHER A PAGE'S CUBE OPENS AGAIN OVER ITS SOURCE AS IT IS NOW (a page reopened, a frame served again: P11), the one
 * rule (rebuiltCommands'; Python's, headless cubeOpensAgain -- the S7 audit's Y-2): every cube but a snapped grid's,
 * which reads its copy, never its frames, until it goes live (the S1 audit's 6).
 */
export function opensAgain(page: PageDocument, cube: string): boolean {
  return page.copies?.[cube] === undefined;
}

/** A grid's input, read by the cube document's own reader: what it refuses, refused in its words. */
function inputOf(v: unknown): CubeInput {
  try {
    return readCubeInput(v, "a grid's input");
  } catch (e) {
    if (e instanceof CubeDocumentError) throw new CubeRefusal(e.message);
    throw e;
  }
}

/** A cube's source, read by the cube document's own reader: what it refuses, refused in its words. */
function sourceOf(v: unknown): CubeSource {
  try {
    return readCubeSource(v);
  } catch (e) {
    if (e instanceof CubeDocumentError) throw new CubeRefusal(e.message);
    throw e;
  }
}

/** The fields of a page command a host reads with their exact numbers (a calculated column's 1.10, a join condition's
 *  decimal), as the page's cubes are read: a cube's document, a source, a grid's input (a copy holds a query), a join's
 *  how and a Union's more inputs. */
export const EXACT_FIELD: Partial<Record<PageCommand['kind'], readonly ('document' | 'source' | 'input' | 'how' | 'more' | 'pure')[]>> = {
  addGrid: ['document'], rebuilt: ['source'], setInput: ['input'], setJoin: ['how', 'more'], setPure: ['pure'],
};

/**
 * A grid's cube, read by its own reader (cube-document.ts readCube): what it refuses, refused in its words. Never null
 * from JSON: a grid whose source the page cannot write down is the screen's own, and a page holding one does not read
 * back (Python's page is always written down).
 */
function cubeOf(v: unknown): CubeDocument {
  if (!isObject(v)) throw new CubeRefusal("a grid's cube is its document");
  try {
    return readCube(v);
  } catch (e) {
    if (e instanceof CubeDocumentError) throw new CubeRefusal(e.message);
    throw e;
  }
}

function columnsFrom(v: unknown): number | null {
  if (v === null) return null;
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 1) throw new CubeRefusal(`columns is a whole number, one or more, or null, not ${JSON.stringify(v)}`);
  return v;
}

/** A timeseries command's kept cubes (KeptCube), each a cube's id, its document and its copy or null; none: left out. */
function keptOf(v: unknown): { kept?: readonly KeptCube[] } {
  if (v === undefined) return {};
  if (!Array.isArray(v)) throw new CubeRefusal("a timeseries' kept cubes are a list of {cube, document, copy}");
  return {
    kept: v.map((k): KeptCube => {
      if (!isObject(k)) throw new CubeRefusal(`a kept cube is {cube, document, copy}, not ${JSON.stringify(k)}`);
      const odd = Object.keys(k).filter((f) => !['cube', 'document', 'copy'].includes(f));
      if (odd.length > 0) throw new CubeRefusal(`a kept cube takes cube, document, copy, not ${odd.join(', ')}`);
      if (k['copy'] === undefined) throw new CubeRefusal('a kept cube says the copy it reads, or null for its live rows');
      let copy: GridCopy | null = null;
      if (k['copy'] !== null) {
        try {
          copy = readGridCopy(k['copy']);
        } catch (e) {
          throw new CubeRefusal(e instanceof Error ? e.message : String(e));
        }
      }
      return { cube: text(k['cube'], 'a cube'), document: cubeOf(k['document']), copy };
    }),
  };
}

/**
 * A page command read from JSON (Python's, a live channel's): its kind one this module applies, its fields exactly those
 * the kind takes, each of its type -- a grid's cube read by its own reader -- or refused in words. What a field NAMES (a
 * tile the page has, a chart's spec, a layout's tiles) is the apply's to judge.
 */
export function readPageCommand(given: unknown): PageCommand {
  if (!isObject(given) || typeof given['kind'] !== 'string') {
    throw new CubeRefusal(`a page's command is {kind, ...}, its kind one of ${PAGE_COMMAND_KINDS.join(', ')}, not ${JSON.stringify(given)}`);
  }
  const o = given;
  const kind = o['kind'] as string;
  switch (kind) {
    case 'renamePage':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, name: text(o['name'], "a page's name") };
    case 'savedAs': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const record = textOrNull(o['record'], "a saved page's id");
      if (record === '') throw new CubeRefusal("a saved page's id is not blank");
      return { kind, record };
    }
    case 'addSheet':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, ...optionalId(o['id'], "a sheet's id"), name: textOrNull(o['name'], "a sheet's name") };
    case 'addFlow':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, ...optionalId(o['id'], "a sheet's id"), name: textOrNull(o['name'], "a sheet's name") };
    case 'placeBox':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (typeof o['x'] !== 'number' || typeof o['y'] !== 'number') {
        throw new CubeRefusal(`a box's place is two numbers, not (${JSON.stringify(o['x'])}, ${JSON.stringify(o['y'])})`);
      }
      return { kind, tile: text(o['tile'], 'a box'), x: o['x'], y: o['y'], ...(o['frame'] !== undefined ? { frame: readFrame(o['frame']) } : {}) };
    case 'renameSheet':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, sheet: text(o['sheet'], 'a sheet'), name: textOrNull(o['name'], "a sheet's name") };
    case 'moveSheet':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (typeof o['to'] !== 'number') throw new CubeRefusal(`a sheet's place is a number, not ${JSON.stringify(o['to'])}`);
      return { kind, sheet: text(o['sheet'], 'a sheet'), to: o['to'] };
    case 'removeSheet':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, sheet: text(o['sheet'], 'a sheet') };
    case 'renameTile':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, tile: text(o['tile'], 'a tile'), title: textOrNull(o['title'], "a tile's title") };
    case 'addGrid':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return {
        kind, ...optionalId(o['id'], "a grid's id"), document: cubeOf(o['document']),
        sheet: text(o['sheet'], 'a sheet'), near: textOrNull(o['near'], 'a tile'), title: textOrNull(o['title'], "a tile's title"),
        columns: columnsFrom(o['columns']),
      };
    case 'addChart':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!isObject(o['spec'])) throw new CubeRefusal("a chart's spec is an object");
      return {
        kind, ...optionalId(o['id'], "a chart's id"), near: text(o['near'], 'a grid'), spec: o['spec'] as unknown as ChartSpec,
        title: textOrNull(o['title'], "a tile's title"), columns: columnsFrom(o['columns']),
      };
    case 'setChart':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!isObject(o['spec'])) throw new CubeRefusal("a chart's spec is an object");
      return { kind, chart: text(o['chart'], 'a chart'), spec: o['spec'] as unknown as ChartSpec };
    case 'addTimeseries':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!isObject(o['spec'])) throw new CubeRefusal("a timeseries' spec is an object");
      return {
        kind, ...optionalId(o['id'], "a timeseries tile's id"), sheet: text(o['sheet'], 'a sheet'), near: textOrNull(o['near'], 'a tile'),
        spec: o['spec'] as unknown as TimeseriesSpec, title: textOrNull(o['title'], "a tile's title"), columns: columnsFrom(o['columns']),
        ...keptOf(o['kept']),
      };
    case 'setTimeseries':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!isObject(o['spec'])) throw new CubeRefusal("a timeseries' spec is an object");
      return { kind, tile: text(o['tile'], 'a tile'), spec: o['spec'] as unknown as TimeseriesSpec, ...keptOf(o['kept']) };
    case 'followGrid':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, chart: text(o['chart'], 'a chart'), grid: text(o['grid'], 'a grid') };
    case 'detach': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const document = cubeOf(o['document']);
      if (document === null) throw new CubeRefusal("a detached chart's cube is a document, never null");
      let copy: GridCopy | null | undefined;
      if (o['copy'] === null) copy = null;
      else if (o['copy'] !== undefined) {
        try {
          copy = readGridCopy(o['copy']);
        } catch (e) {
          throw new CubeRefusal(e instanceof Error ? e.message : String(e));
        }
      }
      return { kind, chart: text(o['chart'], 'a chart'), cube: text(o['cube'], 'a cube'), document, ...(copy !== undefined ? { copy } : {}) };
    }
    case 'select':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!Array.isArray(o['conditions'])) throw new CubeRefusal("a chart's selection is a list of conditions");
      return { kind, chart: text(o['chart'], 'a chart'), conditions: o['conditions'].map(readFilterNode) };
    case 'moveTile':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, tile: text(o['tile'], 'a tile'), sheet: text(o['sheet'], 'a sheet'), columns: columnsFrom(o['columns']) };
    case 'stack':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!Array.isArray(o['tiles'])) throw new CubeRefusal('a stack is a list of tiles');
      return { kind, tiles: o['tiles'].map((t) => text(t, 'a tile')) };
    case 'removeTile':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, tile: text(o['tile'], 'a tile') };
    case 'addText':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return {
        kind, ...optionalId(o['id'], "a text tile's id"), sheet: text(o['sheet'], 'a sheet'), text: text(o['text'], "a text tile's text"),
        title: textOrNull(o['title'], "a tile's title"), columns: columnsFrom(o['columns']),
      };
    case 'setText':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, tile: text(o['tile'], 'a tile'), text: text(o['text'], "a text tile's text") };
    case 'setTextFiles': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const files = readTextFiles(o['files']);
      if (typeof files === 'string') throw new CubeRefusal(files);
      return { kind, tile: text(o['tile'], 'a tile'), files };
    }
    case 'arrange': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const order = o['order'];
      if (order !== null && !Array.isArray(order)) throw new CubeRefusal('an arrangement\'s order is a list of tiles, or null');
      return {
        kind, sheet: text(o['sheet'], 'a sheet'), preset: text(o['preset'], "the layout picker's shape"),
        order: order === null ? null : order.map((t) => text(t, 'a tile')),
      };
    }
    case 'layout':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (!isObject(o['layout'])) throw new CubeRefusal("a sheet's layout is {kind: 'bands', fit, bands}");
      return { kind, sheet: text(o['sheet'], 'a sheet'), layout: o['layout'] as unknown as PageLayout };
    case 'setPageFilter':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return {
        kind, ...optionalId(o['id'], "a page filter's id"), class: text(o['class'], "a page filter's class"),
        condition: readFilterNode(o['condition']), from: textOrNull(o['from'], 'the chart a page filter came from'),
      };
    case 'removePageFilter':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, filter: text(o['filter'], 'a page filter') };
    case 'snap': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      let copy: GridCopy;
      try {
        copy = readGridCopy(o['copy']);
      } catch (e) {
        throw new CubeRefusal(e instanceof Error ? e.message : String(e));
      }
      return { kind, grid: text(o['grid'], 'a grid'), copy };
    }
    case 'live':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, grid: text(o['grid'], 'a grid') };
    case 'rebuilt':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, cube: text(o['cube'], "a grid's cube"), source: sourceOf(o['source']) };
    case 'setInput': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const side = o['side'];
      if (side !== 'input' && side !== 'left' && side !== 'right' && (typeof side !== 'string' || moreIndex(side) === undefined)) {
        throw new CubeRefusal(`a grid's input is its input, a join's left or right, or a Union's more1, more2, ..., not ${JSON.stringify(side)}`);
      }
      return { kind, grid: text(o['grid'], 'a grid'), side: side as InputSide, input: o['input'] === null ? null : inputOf(o['input']) };
    }
    case 'setJoin': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const more = o['more'];
      if (more !== undefined && !Array.isArray(more)) throw new CubeRefusal(`a Union's inputs after its right are a list, not ${JSON.stringify(more)}`);
      return {
        kind, grid: text(o['grid'], 'a grid'), how: readJoinHow(o['how']),
        ...(more !== undefined ? { more: (more as unknown[]).map(inputOf) } : {}),
      };
    }
    case 'setPure': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const pure = o['pure'];
      if (pure === null) return { kind, grid: text(o['grid'], 'a grid'), pure: null };
      const problem = pureProblem(pure);
      if (problem !== undefined) throw new CubeRefusal(problem);
      try {
        return { kind, grid: text(o['grid'], 'a grid'), pure: readLambda(pure) };
      } catch (e) {
        if (e instanceof ProtocolError) throw new CubeRefusal(`a Pure box's function: ${e.message}`);
        throw e;
      }
    }
    case 'setOutput': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const out = o['output'];
      if (out !== null && (!isObject(out) || out['format'] !== 'csv' || typeof out['name'] !== 'string' || Object.keys(out).some((k) => k !== 'format' && k !== 'name'))) {
        throw new CubeRefusal(`an output is {format: 'csv', name}, or null, not ${JSON.stringify(out)}`);
      }
      return { kind, grid: text(o['grid'], 'a grid'), output: out === null ? null : { format: 'csv', name: out['name'] as string } };
    }
    case 'setStops': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      if (typeof o['stops'] !== 'boolean') throw new CubeRefusal(`a Test stops a run (true) or warns (false), not ${JSON.stringify(o['stops'])}`);
      return { kind, grid: text(o['grid'], 'a grid'), stops: o['stops'] };
    }
    case 'modelInto':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, project: text(o['project'], "a page model's project"), workspace: text(o['workspace'], "a page model's workspace"),
        package: text(o['package'], "a page model's package") };
    case 'modelPin':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, revision: text(o['revision'], "a page model's revision") };
    case 'modelKeep': {
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      const tables = o['tables'];
      if (tables !== null && !(Array.isArray(tables) && tables.every((t) => typeof t === 'string'))) {
        throw new CubeRefusal(`a grid's tables are a list of names, or null, not ${JSON.stringify(tables)}`);
      }
      return { kind, cube: text(o['cube'], "a grid's cube"), tables: tables as readonly string[] | null,
        class: textOrNull(o['class'], "a grid's class") };
    }
    default:
      throw new CubeRefusal(`'${kind}' is not a page's command: ${PAGE_COMMAND_KINDS.join(', ')}`);
  }
}

/** A container's frame read from JSON (a placeBox's): {w, h, collapsed?}, or null, none -- refused in words. */
function readFrame(raw: unknown): Frame | null {
  if (raw === null) return null;
  if (!isObject(raw) || typeof raw['w'] !== 'number' || typeof raw['h'] !== 'number'
    || (raw['collapsed'] !== undefined && typeof raw['collapsed'] !== 'boolean') || Object.keys(raw).some((k) => !['w', 'h', 'collapsed'].includes(k))) {
    throw new CubeRefusal(`a frame is {w, h, collapsed?} or null, not ${JSON.stringify(raw)}`);
  }
  return { w: raw['w'], h: raw['h'], ...(raw['collapsed'] === true ? { collapsed: true as const } : {}) };
}
