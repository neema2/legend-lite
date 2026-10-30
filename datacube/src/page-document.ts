// A saved page: a cube and the views arranged around it, as one document.
//
// The page WRAPS the cube's own document, by value (cube-document.ts is unchanged,
// and a page holds a cube the way a folder holds a file): the cube is saved and
// reopened exactly as a cube alone is, source reconciled and all. Around it the page
// keeps what only a page has:
//
//   views    what is shown: the cube's grid, and each chart of it -- its title, its
//            spec (plain JSON, ours: chart-spec.ts), frozen or live, and the mark it
//            is filtering the cube to (the conditions are in the cube's filter; the
//            view names them, so the chart can take them off again)
//   sheets   where each view sits: the page's sheets, in order (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md
//            §7.3), each a whole screen with its own bands (layout/bands.ts) -- each band a tree
//            of rows and columns with shares, down to the views' tiles -- and whether it fits
//            its window or scrolls; every view on one sheet.
//
// ONE thing is saved, always the page (user ruling, 2026-09-28): a cube with no charts
// is a page of its grid alone; a cube saved bare before that still opens, as a page of
// its grid (`readSaved`). The same three rules as the cube's document: VERSIONED (a page of
// another version is refused by name: no older one is read, the user, 2026-10-17), UNKNOWN
// FIELDS KEPT (written back verbatim), and a page that cannot be read fails LOUDLY.
//
// A page holds a cube per grid (a page of its own) or one (a cube alone's page); every view
// names its cube.

import { ExactNumber, fromJson, toJson as protocolJson } from '../../pure-protocol/src/index.ts';
import { readChart, type ChartSpec } from './chart-spec.ts';
import { CUBE_KIND, cubeToJson, definitionText, readCube, type CubeDocument, type CubeSource } from './cube-document.ts';
import { type Bands, type Node, NEW_SHEET, add as addTile, problems, tiles as tilesOf } from './layout/bands.ts';
import { flowProblems, flowTiles, readMessage, readOutput, type Flow } from './layout/flow.ts';
import type { PageModelRef } from './page-model.ts';
import { readPageFilters, type PageFilter } from './page-filters.ts';
import { readParameters, type PageParameter } from './page-parameters.ts';
import { readMacros, type PageMacro } from './macros.ts';
import { readLambda } from '../../pure-protocol/src/index.ts';
import { givenName, type NamedPart } from './page-names.ts';
import type { FilterNode } from './snapshot.ts';
import { textProblem } from './page-text-rule.ts';
import { afterWhatTheyRead, liveProblem } from './live-inputs.ts';
import { readTimeseries, type TimeseriesSpec } from './timeseries-spec.ts';
import { readTextFiles, type TextFile } from './text-files.ts';

export const PAGE_KIND = 'datacube.page';
/**
 * 6: the page's model keeps each grid's tables, a list -- a join's leaves' each (2026-10-20,
 * docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3). 5: the page's filters, by a class's property (2026-10-19, §16.2). 4:
 * the page's model, where its classes are (2026-10-10, §15.3). 3: sheets, each its own bands (2026-10-09,
 * docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §7.3). 2: one layout, as bands. 1: tiles on a 12-column grid. Only 6 is read.
 */
export const PAGE_VERSION = 6;
/** The sheet a page of one sheet has: a new page's, and a cube alone's. */
export const FIRST_SHEET = 'sheet-1';

export interface GridView {
  readonly id: string;
  readonly kind: 'grid';
  /** The cube it shows (an id in `PageDocument.cubes`). */
  readonly cube: string;
  readonly title?: string;
}

export interface ChartView {
  readonly id: string;
  readonly kind: 'chart';
  readonly cube: string;
  readonly title: string;
  readonly spec: ChartSpec;
  /** The conditions this chart's last click put on its cube's filter, if any. */
  readonly selection?: readonly FilterNode[];
}

/** A text tile (page-text.ts): a page's title, a note -- its words in a small Markdown, no cube of its own. */
export interface TextView {
  readonly id: string;
  readonly kind: 'text';
  readonly text: string;
  /** Its own title; else it is numbered (page-ids.ts textTitle). */
  readonly title?: string;
  /** The files it shows beside its words (text-files.ts: pointed to, never copied); left out when none. */
  readonly files?: readonly TextFile[];
  /** It shows no cube: read off any view, a text tile's cube is undefined. */
  readonly cube?: never;
}

/**
 * A TIMESERIES TILE (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md): series of any number of the page's grids along one
 * time axis -- its spec's sources name the cubes it reads (a grid's id is its cube's). A source whose grid was removed is
 * kept as it was, its series said to wait for it (undo brings the grid back); the tile shows no one cube of its own.
 */
export interface TimeseriesView {
  readonly id: string;
  readonly kind: 'timeseries';
  readonly spec: TimeseriesSpec;
  /** Its own title; else it is numbered (page-ids.ts timeseriesTitle). */
  readonly title?: string;
  /** It reads its sources' cubes (spec.sources), never one of its own. */
  readonly cube?: never;
}

export type PageView = GridView | ChartView | TextView | TimeseriesView;

/**
 * WHETHER A VIEW READS A CUBE -- a grid's or a chart's its own, a timeseries' each of its sources' (a text tile none): the
 * one rule a page keeps a cube by, so a grid removed while a timeseries reads its cube leaves the cube on the page, as a
 * frozen chart's is kept (the owner, 2026-10-25, the review's G4: the cube went with its grid, the tile after a reopen
 * reading a cube the page no longer held).
 */
export function readsCube(view: PageView, cube: string): boolean {
  return cubesRead(view).includes(cube);
}

/** The cubes a view reads: every kind of tile answers, a new kind not compiling until it does (the second audit's A-B1:
 *  a page's writers read a view's `cube`, which a timeseries has none of, and dropped its cubes). */
export function cubesRead(view: PageView): readonly string[] {
  switch (view.kind) {
    case 'grid':
    case 'chart':
      return [view.cube];
    case 'timeseries':
      return view.spec.sources.map((s) => s.cube);
    case 'text':
      return [];
  }
}

/**
 * THE CUBES A PAGE HOLDS: each its views read (cubesRead) -- a grid's, a detached chart's, a removed grid's a timeseries
 * still reads -- and no other, a cube no view reads let go. The one rule every writer and every page command holds a
 * page's cubes to (a page written out without one is refused: pageContent).
 */
export function cubesHeld(views: readonly PageView[]): ReadonlySet<string> {
  return new Set(views.flatMap(cubesRead));
}

/** Of `cubes`, those `views` still read (cubesHeld): what a change that takes a view off leaves the page holding. */
export function stillHeld<T extends { readonly id: string }>(cubes: readonly T[], views: readonly PageView[]): T[] {
  const held = cubesHeld(views);
  return cubes.filter((c) => held.has(c.id));
}

/** A page written out holding every cube its views read, or the writer's bug said: one without is a file the reader
 *  refuses (the second audit's A-B1, a timeseries' kept cube written off its page). */
function heldWhole(cubes: readonly { readonly id: string }[], views: readonly PageView[]): void {
  const written = new Set(cubes.map((c) => c.id));
  for (const view of views) {
    const missing = cubesRead(view).find((c) => !written.has(c));
    if (missing !== undefined) throw new Error(`the page is written without cube ${missing}, which ${view.kind} ${view.id} reads`);
  }
}

/** Where the views sit: the page's bands (layout/bands.ts), each tile a view's id. */
/** A board sheet's layout: its bands (layout/bands.ts). */
export interface BandsLayout extends Bands {
  readonly kind: 'bands';
}

/** A flow sheet's layout (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4): its boxes on a canvas (layout/flow.ts). */
export interface FlowLayout extends Flow {
  readonly kind: 'flow';
}

/** A sheet's layout: a board of bands, or a flow of boxes. */
export type PageLayout = BandsLayout | FlowLayout;

/** The tiles a sheet's layout holds: a board's in reading order, a flow's in the order its boxes were placed. */
export function layoutTiles(layout: PageLayout): string[] {
  return layout.kind === 'flow' ? flowTiles(layout) : tilesOf(layout);
}

/** A sheet: a whole screen of the page's views, laid out as its bands. */
export interface PageSheet {
  readonly id: string;
  /** Its name, when someone gave it one (else it is named after what it shows). */
  readonly name?: string;
  readonly layout: PageLayout;
}

/** What a cube app shows around its cube: its views, and the sheets they are laid out on (each view on one). */
export interface PageViews {
  readonly views: readonly PageView[];
  readonly sheets: readonly PageSheet[];
}

/** A page of one sheet, laid out as `layout`. */
export const oneSheet = (layout: PageLayout): readonly PageSheet[] => [{ id: FIRST_SHEET, layout }];

/**
 * THE VIEWS A PAGE STARTS WITH, on its one sheet: a new sheet's empty board (bands.ts NEW_SHEET), or with a grid
 * (`grid`: its view's id and its cube's) added to it as any first tile is (bands.ts add: the whole board). The one
 * writer of a starting page: a cube app's page of its grid alone (app.ts pageViews), a cube saved before pages opened as
 * a page (app/datacube-app.ts), and a new page (newPage: Python's and the tab's New Page), whose first grid the same add
 * places.
 */
export function startingViews(grid?: { readonly id: string; readonly cube: string }): PageViews {
  return {
    views: grid ? [{ id: grid.id, kind: 'grid', cube: grid.cube }] : [],
    sheets: oneSheet({ kind: 'bands', ...(grid ? addTile(NEW_SHEET, grid.id) : NEW_SHEET) }),
  };
}

/**
 * A NEW PAGE, the one skeleton (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D11, B9): no cube yet, its one sheet an empty
 * board (startingViews), unnamed ('': shown as "Untitled page", Save suggesting its first sheet's name) unless `name` is
 * given. Python's new page (headless page.ts pageNew) and the tab's New Page (PageApp.clear) both start from it.
 */
export function newPage(name = ''): PageDocument {
  return writePageOf({ name, cubes: [], views: startingViews() });
}

/**
 * WHAT A SHEET IS CALLED (its tab): the name someone gave it, else -- a flow -- "Flow n", else the report title of the
 * first grid it shows (its tiles in reading order; `grid` says of a tile whether it is a grid and its title), else
 * "Sheet n" by its place (`at`, from 0). The tab's sheet tabs ask it of the board, and a page's suggested name of its
 * document (suggestedName).
 */
export function sheetLabel(sheet: { readonly name?: string | undefined; readonly tiles: readonly string[]; readonly flow?: boolean },
  at: number, grid: (tile: string) => { readonly title: string | undefined } | undefined): string {
  if (sheet.name !== undefined) return sheet.name;
  // a flow is its steps, not its first box's source: "Flow N" until someone names it
  if (sheet.flow === true) return `Flow ${at + 1}`;
  const first = sheet.tiles.map(grid).find((g) => g !== undefined);
  return first?.title || `Sheet ${at + 1}`;
}

/**
 * A PAGE NONE OF WHOSE SOURCES OPENED is not opened, said in these words -- the tab's Open…, Python's load (the S7
 * confirmation audit's Y2-2: Python opened a page it could read nothing of): `opened`, how many of its cubes opened.
 */
export function noneOpened(name: string, opened: number): string | undefined {
  return opened === 0 ? `not opened: "${name}" -- none of its sources was opened` : undefined;
}

/**
 * WHAT A PAGE LEAVES OUT WHEN IT OPENS AND A CUBE'S SOURCE CANNOT BE OPENED, the one rule (the tab's Open…, P9; Python's
 * load, headless gridOpened's `leaveOut` -- the S7 audit's H-6: Python refused the whole page): a class whose rows are
 * not read here, a join that no longer joins -- kept as the page saved it, its views saying why, the page's other grids
 * opened. Any other source refuses the open, in words (a file asked for, a frame given).
 */
export function leftOutWhenUnopened(source: CubeSource): boolean {
  return source._type === 'class' || source._type === 'join' || source._type === 'rows';
}

/** What a cube left out as its page opens is said as (leftOutWhenUnopened): its source's name, and why. */
export function leftOutNote(name: string, why: string): string {
  return `left out: ${name} (${why})`;
}

/**
 * WHAT A PAGE IS CALLED when it is saved and its saver names nothing (D11): its own name, else its first sheet's label --
 * as the Save window suggests it (PageApp.suggestedName); '' for a page of no sheet.
 */
export function suggestedName(page: PageDocument): string {
  if (page.name !== '') return page.name;
  const first = page.sheets[0];
  return first === undefined ? '' : labelOf(page, first, first.name);
}

/** What a sheet of `page` shows as its label (sheetLabel), its name `name` (undefined: none of its own): its first grid's
 *  report title, else its place. */
function labelOf(page: PageDocument, sheet: PageSheet, name: string | undefined): string {
  const cubes = new Map(page.cubes.map((c) => [c.id, c.cube]));
  const grids = new Map(page.views.flatMap((v) => (v.kind === 'grid' ? [[v.id, v.cube] as const] : [])));
  return sheetLabel({ name, tiles: layoutTiles(sheet.layout), flow: sheet.layout.kind === 'flow' }, page.sheets.indexOf(sheet), (tile) => {
    const cube = grids.get(tile);
    if (cube === undefined) return undefined;
    const title = cubes.get(cube)?.configuration['reportTitle'];
    return { title: typeof title === 'string' ? title : undefined };
  });
}

/** What a sheet of `page` shows when it has no name of its own (its label, sheetLabel's): a name equal to it is none
 *  (page-commands.ts renameSheet, as renameTile drops a grid's made title: B2; the S7 audit's P-7). */
export function unnamedLabel(page: PageDocument, sheet: PageSheet): string {
  return labelOf(page, sheet, undefined);
}

/**
 * WHAT A PAGE'S MODEL IS NAMED AFTER (its project, package and data space: model-names.ts), D11: its own `name`, else --
 * never named -- what it is called (`suggested`: suggestedName, its first sheet's: its grid's source) without a file's
 * extension (`trades.csv`: trades). The tab's model windows and DataCube's model actions headless ask it.
 */
export function modelPageName(name: string, suggested: string): string {
  return name !== '' ? name : (suggested.replace(/\.[A-Za-z0-9]+$/, '') || 'page');
}

/**
 * A COPY A GRID OF THE PAGE READS (Snap: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): its base query's result, made
 * by the page's host -- the tab's DuckDB, or Python's -- in a table of its own. The page says which copy a grid reads;
 * the copy's model is its host's, looked up by its table (the host modelled it as it landed).
 */
export interface GridCopy {
  readonly table: string;
  /** What it is called, as the grid's plane says it ("Snap 14:02"). */
  readonly label: string;
  /** When it was taken: an ISO instant. */
  readonly takenAt: string;
  readonly rowCount: number;
  /** The pull that made it, when it ran elsewhere (a warehouse): where, and as whom. */
  readonly pulledBy?: CopyReceipt;
}

/**
 * A copy as the page writes it down (§18.8), from what its host made: its table, its rows, when it was taken (its label
 * "Snap 14:02" unless given: engine-client snap.ts defaultLabel), and the plain part of its pull's receipt. The screen's
 * Snap and Python's (headless gridSnapped) write it alike.
 */
export function gridCopyOf(made: {
  readonly table: string; readonly rowCount: number; readonly takenAt: Date; readonly label: string;
  readonly pulledBy?: { readonly plane: CopyReceipt['plane']; readonly where: string; readonly as?: string;
    readonly statementId?: string; readonly serverRows?: number };
}): GridCopy {
  const r = made.pulledBy;
  return {
    table: made.table, label: made.label, takenAt: made.takenAt.toISOString(), rowCount: made.rowCount,
    ...(r ? { pulledBy: {
      plane: r.plane, where: r.where, ...(r.as !== undefined ? { as: r.as } : {}),
      ...(r.statementId !== undefined ? { statementId: r.statementId } : {}),
      ...(r.serverRows !== undefined ? { serverRows: r.serverRows } : {}),
    } } : {}),
  };
}

/** What a copy's pull said of itself (engine-client receipt.ts Receipt: the part a page writes down). */
export interface CopyReceipt {
  readonly plane: 'tab' | 'warehouse' | 'engine';
  readonly where: string;
  readonly as?: string;
  readonly statementId?: string;
  readonly serverRows?: number;
}

export interface PageDocument extends PageViews {
  readonly kind: typeof PAGE_KIND;
  readonly version: number;
  readonly name: string;
  readonly cubes: readonly { readonly id: string; readonly cube: CubeDocument }[];
  /** Where the page's classes are (Create a Class…): its project in the model home; none until its first class. */
  readonly model?: PageModelRef;
  /** Its filters, by a class's property (page-filters.ts): none, none written. */
  readonly filters?: readonly PageFilter[];
  /** Its parameters, the values any Pure on it reads as `$name` (page-parameters.ts, an analytic app's inputs): none,
   *  none written. */
  readonly parameters?: readonly PageParameter[];
  /** Its macros, sub-flows each saved as a Pure function of its input's rows, used as a box (macros.ts): none, none
   *  written. */
  readonly macros?: readonly PageMacro[];
  /** The copy each snapped grid reads, by its cube's id (§18.8): none, none written. */
  readonly copies?: Readonly<Record<string, GridCopy>>;
  /**
   * The saved page it was last saved as, by the page store's id: whichever editor saves it next saves over that record
   * (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B13); none until it is first saved. Never in a page file (pageFile).
   */
  readonly record?: string;
  /** Top-level fields a newer writer added, kept verbatim. */
  readonly unknown?: Readonly<Record<string, unknown>>;
}

/** Whether a saved name holds by givenName's rule (page-names.ts): text, and for a page or a tile not blank. */
const named = (part: NamedPart, value: unknown): value is string =>
  typeof value === 'string' && !('refused' in givenName(part, value));

export class PageDocumentError extends Error {
  constructor(detail: string) {
    super(`cannot read saved page: ${detail}`);
    this.name = 'PageDocumentError';
  }
}

/**
 * What a page with nothing on it is told when it is saved or written as a page file -- the one wording (B14) of the
 * screen's Save and Export ▸ Page File (page-app.ts saveRefusal) and Python's `save` and `to_json` (headless pageSaved):
 * DataCube opens no saved page with no cube (readPage).
 */
export const NOTHING_TO_SAVE = 'There is nothing on this page to save: add a data source first.';

/**
 * A PAGE AS A PAGE FILE -- Export ▸ Page File, and Python's `save(path)` (headless pageFile): its text as DataCube
 * writes it, what a newer writer put in it kept -- or why it is none, in one wording for both (B5): nothing on it
 * (NOTHING_TO_SAVE), or classes kept in a project, which a page file cannot carry yet (§15.3).
 */
export function pageFile(page: PageDocument): { readonly file: string; readonly name: string } | { readonly refused: string } {
  if (page.cubes.length === 0) return { refused: NOTHING_TO_SAVE };
  if (page.model) {
    return { refused: `This page's classes are in ${page.model.project}: a page file cannot carry them yet -- save the `
      + 'page where its classes are kept.' };
  }
  // named as Save names it (D11: an unnamed page, its first sheet's), else 'page' -- the file named the same, by this one
  // rule (the S7 audit's Y-4: the tab named it in its own code, Python not at all)
  const name = suggestedName(page) || 'page';
  const named = page.name === name ? page : writePageOf({
    name, cubes: page.cubes, views: { views: page.views, sheets: page.sheets }, ...(page.model ? { model: page.model } : {}),
    ...(page.filters ? { filters: page.filters } : {}), ...(page.parameters ? { parameters: page.parameters } : {}),
    ...(page.macros ? { macros: page.macros } : {}),
    ...(page.copies ? { copies: page.copies } : {}), ...(page.unknown ? { unknown: page.unknown } : {}),
  });
  // a page file is no saved record: opened elsewhere, its first save is a new one
  const { record: _record, ...file } = named;
  return { file: pageToJson(file), name };
}

const KNOWN = new Set(['kind', 'version', 'name', 'cubes', 'views', 'sheets', 'model', 'filters', 'parameters', 'macros', 'copies', 'record']);

/** The one cube of a step-1 page. */
export const PAGE_CUBE = 'cube';

// ---------------------------------------------------------------- writing

export function writePage(o: {
  readonly name: string;
  readonly cube: CubeDocument;
  readonly views: PageViews;
  readonly model?: PageModelRef;
  readonly filters?: readonly PageFilter[];
  readonly unknown?: Readonly<Record<string, unknown>>;
}): PageDocument {
  const filters = filtersFor(o.filters ?? [], o.views.views);
  return {
    kind: PAGE_KIND,
    version: PAGE_VERSION,
    name: o.name,
    // the page's name is the cube's too: one name, whichever is opened
    cubes: [{ id: PAGE_CUBE, cube: { ...o.cube, name: o.name } }],
    views: o.views.views,
    sheets: o.views.sheets,
    ...(o.model ? { model: keptFor(o.model, [{ id: PAGE_CUBE }]) } : {}),
    ...(filters.length > 0 ? { filters } : {}),
    ...(o.unknown && Object.keys(o.unknown).length > 0 ? { unknown: o.unknown } : {}),
  };
}

/**
 * A PAGE OF SEVERAL CUBES (a page of its own: one cube per grid, each over its own source, and one per grid kept off
 * the board for a detached chart). Each view names its cube by id; a cube no grid view names is a detached chart's, or
 * one a timeseries reads, its grid removed. A page the screen holds as it is now may name a grid's cube it cannot write
 * yet (a grid that cannot say its source); a page written out holds every one (pageContent).
 */
export function writePageOf(o: {
  readonly name: string;
  readonly cubes: readonly { readonly id: string; readonly cube: CubeDocument }[];
  readonly views: PageViews;
  readonly model?: PageModelRef;
  readonly filters?: readonly PageFilter[];
  readonly parameters?: readonly PageParameter[];
  readonly macros?: readonly PageMacro[];
  readonly copies?: Readonly<Record<string, GridCopy>>;
  readonly record?: string;
  readonly unknown?: Readonly<Record<string, unknown>>;
}): PageDocument {
  const filters = filtersFor(o.filters ?? [], o.views.views);
  const copies = copiesFor(o.copies ?? {}, o.cubes);
  return {
    kind: PAGE_KIND,
    version: PAGE_VERSION,
    name: o.name,
    // the page's name is each cube's too: one name, whichever is opened
    cubes: o.cubes.map((c) => ({ id: c.id, cube: { ...c.cube, name: o.name } })),
    views: o.views.views,
    sheets: o.views.sheets,
    ...(o.model ? { model: keptFor(o.model, o.cubes) } : {}),
    ...(filters.length > 0 ? { filters } : {}),
    ...(o.parameters !== undefined && o.parameters.length > 0 ? { parameters: o.parameters } : {}),
    ...(o.macros !== undefined && o.macros.length > 0 ? { macros: o.macros } : {}),
    ...(Object.keys(copies).length > 0 ? { copies } : {}),
    ...(o.record !== undefined ? { record: o.record } : {}),
    ...(o.unknown && Object.keys(o.unknown).length > 0 ? { unknown: o.unknown } : {}),
  };
}

/**
 * A SAVED PAGE AS IT OPENS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): live -- a copy lasts as long as the session
 * that made it, so a page saved with a snapped grid opens with that grid reading its live base -- and each grid that was
 * snapped said. Only the live channel's page (a host serving its page, the page reporting it) keeps its copies.
 */
export function openedLive(page: PageDocument): { readonly page: PageDocument; readonly notes: readonly string[] } {
  if (!page.copies) return { page, notes: [] };
  const { copies, ...rest } = page;
  const notes = Object.entries(copies).map(([cube, copy]) => {
    const name = page.cubes.find((c) => c.id === cube)?.cube.source.name ?? cube;
    return `${name} was snapped (${copy.label}) when the page was saved: it opens live, as a snap lasts only as long as the session that made it`;
  });
  return { page: rest, notes };
}

/** The copies the page's grids read, kept for the cubes it holds: a grid removed takes its copy off the page (its host
 *  lets it go, §18.8). */
export function copiesFor(copies: Readonly<Record<string, GridCopy>>,
  cubes: readonly { readonly id: string }[]): Readonly<Record<string, GridCopy>> {
  const ids = new Set(cubes.map((c) => c.id));
  return Object.fromEntries(Object.entries(copies).filter(([cube]) => ids.has(cube)));
}

/**
 * The page's model as the page writes it: its tables and classes kept for the cubes the page holds, and only those --
 * a grid removed since its class was made takes its table's name and its class with it, as the reader holds every
 * kept name to a cube of the page (the audit's B1: a page written otherwise would never open again). Python's page asks
 * it too, through the page command that removes a tile (page-commands.ts removeTile, headless pageApply): the one rule.
 */
export function keptFor(model: PageModelRef, cubes: readonly { readonly id: string }[]): PageModelRef {
  const ids = new Set(cubes.map((c) => c.id));
  const tables: Record<string, readonly string[]> = {};
  for (const [cube, kept] of Object.entries(model.tables)) if (ids.has(cube)) tables[cube] = kept;
  const classes: Record<string, string> = {};
  for (const [cube, cls] of Object.entries(model.classes ?? {})) if (ids.has(cube)) classes[cube] = cls;
  // its classes likewise: none kept, none written -- the model's own would keep the class of a grid removed, which the
  // reader refuses as it refuses a table's
  const { classes: _was, ...rest } = model;
  return { ...rest, tables, ...(Object.keys(classes).length > 0 ? { classes } : {}) };
}

/**
 * THE PAGE'S MODEL ACROSS AN UNDO OR A REDO (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D5, P15): a model action (Create
 * a Class…, an Association, a Data Space) is no step of the page's history -- what it made in its project cannot be
 * undone -- so the page put back keeps the model as it is `now` (its project, its revision, each grid's tables and
 * class), and a grid it brings back the tables and class it had in the page put back (`restored`); kept for the cubes
 * that page holds (keptFor).
 */
export function modelAcross(restored: PageModelRef | undefined, now: PageModelRef | undefined,
  cubes: readonly { readonly id: string }[]): PageModelRef | undefined {
  if (now === undefined) return restored === undefined ? undefined : keptFor(restored, cubes);
  const classes = { ...restored?.classes, ...now.classes };
  return keptFor({
    ...now, tables: { ...restored?.tables, ...now.tables }, ...(Object.keys(classes).length > 0 ? { classes } : {}),
  }, cubes);
}

/**
 * The page's filters as the page writes them: a chart's click kept only while the page holds that chart -- a chart
 * removed takes the filter its click made with it, as the reader holds every click to a chart of the page.
 */
export function filtersFor(filters: readonly PageFilter[], views: readonly PageView[]): readonly PageFilter[] {
  const charts = new Set(views.filter((v) => v.kind === 'chart').map((v) => v.id));
  return filters.filter((f) => f.from === undefined || charts.has(f.from));
}

/**
 * A FLOW'S BOXES (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1's design, 3): the cubes of `page` a box of a flow sheet
 * shows. Each is a document until its box is shown, so a page opens none of them as it opens (the tab's Open…, its
 * keeper's page converged): each is opened when its box is shown, over the page as it is then.
 */
export function boxCubes(page: Pick<PageDocument, 'views' | 'sheets'>): Set<string> {
  const boxes = new Set(page.sheets.flatMap((s) => (s.layout.kind === 'flow' ? flowTiles(s.layout) : [])));
  return new Set(page.views.flatMap((v) => (v.kind === 'grid' && boxes.has(v.id) ? [v.cube] : [])));
}

/**
 * The order a saved page's cubes are opened in, wherever their tiles sit on the page: a join's after the grids over one
 * source, since it reads the files they read in (a file read once); a class's after every other, since its rows are
 * the tables the page's other grids read in; and a grid over another grid's rows after the grids it reads
 * (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1: live-inputs.ts afterWhatTheyRead), each cube's grid the page's view of it
 * (`views`).
 */
export function openingOrder<T extends { readonly id: string; readonly cube: CubeDocument }>(cubes: readonly T[],
  views: readonly PageView[]): T[] {
  const of = (kind: (t: string) => boolean): T[] => cubes.filter((c) => kind(c.cube.source._type));
  const gridOf = (c: T): string | undefined => views.find((v) => v.kind === 'grid' && v.cube === c.id)?.id;
  return afterWhatTheyRead([...of((t) => t !== 'class' && t !== 'join' && t !== 'rows'), ...of((t) => t === 'join' || t === 'rows'),
    ...of((t) => t === 'class')], gridOf);
}

/** The page as a JSON-ready object: each cube as its own document writes itself -- refused when a view reads a cube it
 *  does not hold (cubesHeld's rule). */
export function pageContent(page: PageDocument): Record<string, unknown> {
  // every way a page leaves as data -- a page file, a saved record, its keeper, a link, Python's -- comes through here
  heldWhole(page.cubes, page.views);
  const { unknown, cubes, ...rest } = page;
  return {
    ...unknown,
    ...rest,
    cubes: cubes.map((c) => ({ id: c.id, cube: fromJson(cubeToJson(c.cube)) })),
  };
}

/** A saved page as the page store keeps it: its id, its name, its content. */
export interface SavedRecord {
  readonly id: string;
  readonly name: string;
  readonly content: Record<string, unknown>;
}

/** Whether `page` saves over the saved page it is (its record, either editor's: B13) rather than as a new one -- the
 *  tab's Save, Python's save (the S7 confirmation audit's Y2-5: Python spelled the choice itself). */
export function savesOver(page: PageDocument): boolean {
  return page.record !== undefined;
}

/**
 * A PAGE SAVED UNDER `name` AS THE PAGE STORE KEEPS IT -- the Save window's record, and Python's `save()`'s (headless
 * pageRecord: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z18): its id, its name, and its content named so -- saved under a
 * name, the page is that name (B4), which its editor then tells as the name box does (renamePage). `name` by the one
 * rule of a page's name (givenName), or why not, in its words.
 */
export function savedRecord(name: string, page: PageDocument, fresh: string): SavedRecord | { readonly refused: string } {
  const given = givenName('page', name);
  if ('refused' in given) return given;
  if (given.name === undefined) throw new Error("a page's name is never left to what it shows");
  // over the record it was last saved as, whichever editor saved it (B13); else a new one, under `fresh`
  const id = page.record ?? fresh;
  return { id, name: given.name, content: pageContent({ ...page, name: given.name, record: id }) };
}

/** A page saved as the Save window saves it -- the tab's Save and Save As, Python's save() and save_as() (headless
 *  pageRecord): over the saved page it is, or, Save As (`asNew`), as a new one under `fresh`, the record it was saved as
 *  left alone (K13); its record (savedRecord) and whether it saves over one (`over`), or why not, in its words. */
export function pageSaving(name: string, page: PageDocument, fresh: string, asNew: boolean):
  { readonly record: SavedRecord; readonly over: boolean } | { readonly refused: string } {
  const { record: _was, ...asItself } = page;
  const saving = asNew ? asItself : page;
  const record = savedRecord(name, saving, fresh);
  return 'refused' in record ? record : { record, over: savesOver(saving) };
}

/** The page as JSON text (exact numbers kept, as the cube's own document keeps them). */
export function pageToJson(page: PageDocument): string {
  return protocolJson(pageContent(page));
}

/**
 * What makes two pages "the same" for "changed since saved": each cube's definition, the
 * views and the sheets (their names, order and layouts). Not the name, not the open rows.
 */
export function pageDefinitionText(page: PageDocument): string {
  // the protocol's writer puts keys in one order: a page read back compares equal
  return protocolJson({
    cubes: page.cubes.map((c) => definitionText(c.cube)),
    views: page.views,
    sheets: page.sheets,
    // its classes' project: made, or a grid's table kept, the page has changed
    ...(page.model ? { model: page.model } : {}),
    // a filter set or taken off, the page has changed
    ...(page.filters ? { filters: page.filters } : {}),
    // a parameter set, its value changed or taken off, the page has changed
    ...(page.parameters ? { parameters: page.parameters } : {}),
    // a macro saved or taken off, the page has changed
    ...(page.macros ? { macros: page.macros } : {}),
  });
}

// ---------------------------------------------------------------- reading

/** A saved document of either kind: a cube alone, or a page around one. */
export type SavedDocument =
  | { readonly kind: 'cube'; readonly cube: CubeDocument }
  | { readonly kind: 'page'; readonly page: PageDocument };

/** Read a saved cube or page, by its kind. */
export function readSaved(input: string | unknown): SavedDocument {
  const raw = typeof input === 'string' ? parse(input) : input;
  if (isObject(raw) && raw['kind'] === PAGE_KIND) return { kind: 'page', page: readPage(raw) };
  return { kind: 'cube', cube: readCube(raw) };
}

/**
 * A SAVED DOCUMENT AS THE PAGE IT OPENS AS (the tab's Open…, Python's Page.open): a page as it is; a cube saved alone,
 * before pages, a page of its one grid, the whole board.
 */
export function savedPage(saved: SavedDocument): PageDocument {
  if (saved.kind === 'page') return saved.page;
  return writePageOf({ name: saved.cube.name, cubes: [{ id: 'cube', cube: saved.cube }], views: startingViews({ id: 'grid', cube: 'cube' }) });
}

/**
 * `empty`: a page of no grid read as one, where a page is written live and not saved -- one Python holds (it starts
 * as one) or a tab has taken its last grid off (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md, known limit 5). A saved
 * page with no cube is never one DataCube opens.
 */
export function readPage(input: string | unknown, options: { readonly empty?: boolean } = {}): PageDocument {
  const raw = typeof input === 'string' ? parse(input) : input;
  if (!isObject(raw)) throw new PageDocumentError('expected an object');
  if (raw['kind'] !== PAGE_KIND) {
    throw new PageDocumentError(`not a saved page (kind ${JSON.stringify(plain(raw['kind']) ?? null)})`);
  }
  const version = plain(raw['version']);
  if (typeof version !== 'number' || version < 1) throw new PageDocumentError('it has no version');
  if (version > PAGE_VERSION) {
    throw new PageDocumentError(`written by a newer version (${version} > ${PAGE_VERSION}); upgrade to open it`);
  }
  if (version < PAGE_VERSION) {
    throw new PageDocumentError(`saved by an older version (${version} < ${PAGE_VERSION}): it is no longer read`);
  }
  if (typeof raw['name'] !== 'string') throw new PageDocumentError("missing 'name'");
  // '' is a page nobody named (D11: shown as "Untitled page"); a name of blanks is none
  if (raw['name'] !== '' && !named('page', raw['name'])) throw new PageDocumentError("the page's name is blank");

  const cubesRaw = raw['cubes'];
  if (!Array.isArray(cubesRaw) || (cubesRaw.length === 0 && options.empty !== true)) throw new PageDocumentError('it has no cube');
  const cubes = cubesRaw.map((c, i) => {
    if (!isObject(c) || typeof c['id'] !== 'string' || !isObject(c['cube'])) {
      throw new PageDocumentError(`cube ${i + 1} is not an id and a cube`);
    }
    if (c['cube']['kind'] !== CUBE_KIND) throw new PageDocumentError(`cube ${c['id']} is not a saved cube`);
    return { id: c['id'], cube: readCube(c['cube']) };
  });
  const ids = new Set(cubes.map((c) => c.id));
  // a view names its cube by id, and the model keeps a table per cube's id: two cubes of one id are one too many
  if (ids.size !== cubes.length) throw new PageDocumentError('two cubes share an id');

  const viewsRaw = plain(raw['views']);
  if (!Array.isArray(viewsRaw)) throw new PageDocumentError("'views' is not a list");
  const views = viewsRaw.map((v) => readView(v, ids));
  const viewIds = new Set(views.map((v) => v.id));
  if (viewIds.size !== views.length) throw new PageDocumentError('two views share an id');

  const sheets = readSheets(plain(raw['sheets']), viewIds);
  // every view has its place, on one sheet: a view the layout leaves out would be put somewhere on opening, and the
  // page read as changed before anyone touched it
  const placed = sheets.flatMap((sheet) => layoutTiles(sheet.layout));
  // a flow's boxes are grids (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL5), its comments text tiles (M7, 2026-10-25): a
  // chart goes on a board
  for (const sheet of sheets) {
    if (sheet.layout.kind !== 'flow') continue;
    const problem = flowSheetProblem(sheet.id, sheet.layout, views);
    if (problem !== undefined) throw new PageDocumentError(problem);
  }
  const twice = placed.filter((id, i) => placed.indexOf(id) !== i);
  if (twice.length > 0) throw new PageDocumentError(`${[...new Set(twice)].join(', ')} on two sheets`);
  const missing = [...viewIds].filter((id) => !placed.includes(id));
  if (missing.length > 0) throw new PageDocumentError(`no sheet has a place for ${missing.join(', ')}`);

  const unknown: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw)) if (!KNOWN.has(k)) unknown[k] = plain(v);
  const model = raw['model'] === undefined ? undefined : readModel(plain(raw['model']), ids);
  const copies = raw['copies'] === undefined ? undefined : readCopies(plain(raw['copies']), ids);
  const record = plain(raw['record']);
  if (record !== undefined && (typeof record !== 'string' || record === '')) {
    throw new PageDocumentError("'record' is not a saved page's id");
  }
  let filters: readonly PageFilter[] | undefined;
  if (raw['filters'] !== undefined) {
    try {
      filters = readPageFilters(plain(raw['filters']), new Set(views.filter((v) => v.kind === 'chart').map((v) => v.id)));
    } catch (e) {
      throw new PageDocumentError(e instanceof Error ? e.message : String(e));
    }
    // none is written as no field: an empty list is no page's
    if (filters.length === 0) throw new PageDocumentError("'filters' is an empty list: a page with none has no field");
  }
  let parameters: readonly PageParameter[] | undefined;
  if (raw['parameters'] !== undefined) {
    try {
      parameters = readParameters(plain(raw['parameters']));
    } catch (e) {
      throw new PageDocumentError(e instanceof Error ? e.message : String(e));
    }
    if (parameters.length === 0) throw new PageDocumentError("'parameters' is an empty list: a page with none has no field");
  }
  let macros: readonly PageMacro[] | undefined;
  if (raw['macros'] !== undefined) {
    try {
      // its functions exact, as a cube's lambdas are read
      macros = readMacros(raw['macros'], (v) => readLambda(v));
    } catch (e) {
      throw new PageDocumentError(e instanceof Error ? e.message : String(e));
    }
    if (macros.length === 0) throw new PageDocumentError("'macros' is an empty list: a page with none has no field");
  }

  const page: PageDocument = {
    kind: PAGE_KIND,
    version: PAGE_VERSION,
    name: raw['name'],
    cubes,
    views,
    sheets,
    ...(model ? { model } : {}),
    ...(filters ? { filters } : {}),
    ...(parameters ? { parameters } : {}),
    ...(macros ? { macros } : {}),
    ...(copies ? { copies } : {}),
    ...(record !== undefined ? { record } : {}),
    ...(Object.keys(unknown).length > 0 ? { unknown } : {}),
  };
  // a grid over another grid's rows reads a grid of this page, and never its own rows (docs/DATACUBE_FLOW_MODE_2026_10_24.md
  // §3.1): a page whose link names no grid of it, or goes round, is no page any editor writes
  const live = liveProblem(page);
  if (live !== undefined) throw new PageDocumentError(live);
  return page;
}

/** The copies the page's snapped grids read, by cube (§18.8), each read strictly: none is written as no field. */
function readCopies(raw: unknown, ids: ReadonlySet<string>): Readonly<Record<string, GridCopy>> {
  if (!isObject(raw)) throw new PageDocumentError("'copies' is not an object");
  const copies: Record<string, GridCopy> = {};
  for (const [cube, copy] of Object.entries(raw)) {
    if (!ids.has(cube)) throw new PageDocumentError(`'copies' holds one for ${cube}, which is no cube of the page`);
    try {
      copies[cube] = readGridCopy(copy);
    } catch (e) {
      throw new PageDocumentError(`the copy of ${cube}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  if (Object.keys(copies).length === 0) throw new PageDocumentError("'copies' is empty: a page with none has no field");
  return copies;
}

/**
 * A copy a grid reads, as a page or a command writes it (§18.8): its table, label, when it was taken (ISO) and its rows,
 * and the pull's receipt when there was one -- each of its type, or refused in words (Error).
 */
export function readGridCopy(raw: unknown): GridCopy {
  if (!isObject(raw)) throw new Error('a copy is {table, label, takenAt, rowCount, pulledBy?}');
  const odd = Object.keys(raw).filter((k) => !['table', 'label', 'takenAt', 'rowCount', 'pulledBy'].includes(k));
  if (odd.length > 0) throw new Error(`a copy has no ${odd.join(', ')}`);
  const { table, label, takenAt, rowCount, pulledBy } = raw;
  if (typeof table !== 'string' || table === '') throw new Error('a copy names its table');
  if (typeof label !== 'string') throw new Error('a copy has a label');
  if (typeof takenAt !== 'string' || Number.isNaN(Date.parse(takenAt))) throw new Error('a copy says when it was taken, as an instant');
  if (typeof rowCount !== 'number' || !Number.isInteger(rowCount) || rowCount < 0) throw new Error('a copy says its rows, a whole number');
  return { table, label, takenAt, rowCount, ...(pulledBy !== undefined ? { pulledBy: readCopyReceipt(pulledBy) } : {}) };
}

function readCopyReceipt(raw: unknown): CopyReceipt {
  if (!isObject(raw)) throw new Error("a copy's pull is {plane, where, as?, statementId?, serverRows?}");
  const odd = Object.keys(raw).filter((k) => !['plane', 'where', 'as', 'statementId', 'serverRows'].includes(k));
  if (odd.length > 0) throw new Error(`a copy's pull has no ${odd.join(', ')}`);
  const { plane, where, as, statementId, serverRows } = raw;
  if (plane !== 'tab' && plane !== 'warehouse' && plane !== 'engine') throw new Error("a copy's pull ran in the tab, a warehouse or an engine");
  if (typeof where !== 'string') throw new Error("a copy's pull says where it ran");
  if (as !== undefined && typeof as !== 'string') throw new Error("a copy's pull says whom it ran as, as text");
  if (statementId !== undefined && typeof statementId !== 'string') throw new Error("a copy's pull names its statement as text");
  if (serverRows !== undefined && (typeof serverRows !== 'number' || !Number.isInteger(serverRows))) {
    throw new Error("a copy's pull counts its rows in a whole number");
  }
  return {
    plane, where, ...(as !== undefined ? { as } : {}), ...(statementId !== undefined ? { statementId } : {}),
    ...(serverRows !== undefined ? { serverRows } : {}),
  };
}

/** The page's model: its project, its workspace, its package, and each grid's table (`ids`: the page's cubes). */
function readModel(raw: unknown, ids: ReadonlySet<string>): PageModelRef {
  if (!isObject(raw)) throw new PageDocumentError("'model' is not an object");
  const { project, workspace, package: pkg, tables, classes, revision } = raw;
  for (const [field, value] of [['project', project], ['workspace', workspace], ['package', pkg]] as const) {
    if (typeof value !== 'string' || value === '') throw new PageDocumentError(`'model' has no ${field}`);
  }
  if (!isObject(tables)) throw new PageDocumentError("'model' has no tables");
  const kept: Record<string, readonly string[]> = {};
  for (const [cube, names] of Object.entries(tables)) {
    if (!ids.has(cube)) throw new PageDocumentError(`'model' keeps tables for ${cube}, which is no cube of the page`);
    if (!Array.isArray(names) || names.length === 0 || !names.every((t) => typeof t === 'string' && t !== '')) {
      throw new PageDocumentError(`'model' keeps no list of table names for ${cube}`);
    }
    kept[cube] = names as string[];
  }
  const made: Record<string, string> = {};
  if (classes !== undefined) {
    if (!isObject(classes)) throw new PageDocumentError("'model' has classes that are not an object");
    for (const [cube, cls] of Object.entries(classes)) {
      if (!ids.has(cube)) throw new PageDocumentError(`'model' keeps a class for ${cube}, which is no cube of the page`);
      if (typeof cls !== 'string' || cls === '') throw new PageDocumentError(`'model' keeps no class name for ${cube}`);
      made[cube] = cls;
    }
  }
  // the pin: none while nothing of the project is saved (its first save failed)
  if (revision !== undefined && (typeof revision !== 'string' || revision === '')) {
    throw new PageDocumentError("'model' has a revision that is no revision's id");
  }
  return {
    project: project as string, workspace: workspace as string, package: pkg as string, tables: kept,
    ...(Object.keys(made).length > 0 ? { classes: made } : {}),
    ...(revision !== undefined ? { revision } : {}),
  };
}

function readSheets(raw: unknown, views: ReadonlySet<string>): readonly PageSheet[] {
  if (!Array.isArray(raw) || raw.length === 0) throw new PageDocumentError("'sheets' is not a list of sheets");
  const ids = new Set<string>();
  return raw.map((sheet, i) => {
    if (!isObject(sheet) || typeof sheet['id'] !== 'string' || sheet['id'].trim() === '') throw new PageDocumentError(`sheet ${i + 1} has no id`);
    const id = sheet['id'];
    if (ids.has(id)) throw new PageDocumentError(`two sheets are ${id}`);
    ids.add(id);
    const name = sheet['name'];
    if (name !== undefined && (typeof name !== 'string' || name.trim() === '')) {
      throw new PageDocumentError(`sheet ${id}'s name is not a name`);
    }
    return {
      id,
      ...(typeof name === 'string' ? { name } : {}),
      layout: readLayout(sheet['layout'], views, `sheet ${id}'s layout`),
    };
  });
}

function readView(v: unknown, cubes: ReadonlySet<string>): PageView {
  if (!isObject(v) || typeof v['id'] !== 'string') throw new PageDocumentError('a view has no id');
  const id = v['id'];
  if (v['kind'] === 'text') {
    // its words by the text tile's one rule (page-text.ts textProblem); no cube
    const problem = textProblem(v['text']);
    if (problem !== null) throw new PageDocumentError(`text ${id}: ${problem}`);
    if (v['title'] !== undefined && !named('tile', v['title'])) throw new PageDocumentError(`text ${id}'s title is blank`);
    // its files by the one rule (text-files.ts readTextFiles); none written, none read
    const files = v['files'] === undefined ? [] : readTextFiles(v['files']);
    if (typeof files === 'string') throw new PageDocumentError(`text ${id}: ${files}`);
    return {
      id, kind: 'text', text: v['text'] as string, ...(typeof v['title'] === 'string' ? { title: v['title'] } : {}),
      ...(files.length > 0 ? { files } : {}),
    };
  }
  if (v['kind'] === 'timeseries') {
    // its spec by the tile's one reader (timeseries-spec.ts readTimeseries); its sources' cubes the page's -- a grid's
    // that was removed kept while the tile reads it (readsCube) -- as a chart's cube is (the review's G4)
    const spec = readTimeseries(v['spec']);
    if (typeof spec === 'string') throw new PageDocumentError(`timeseries ${id} has no timeseries it can draw: ${spec}`);
    const missing = spec.sources.find((s) => !cubes.has(s.cube));
    if (missing) throw new PageDocumentError(`timeseries ${id}'s source ${missing.alias} reads ${missing.cube}, which is no cube of this page`);
    if (v['title'] !== undefined && !named('tile', v['title'])) throw new PageDocumentError(`timeseries ${id}'s title is blank`);
    return { id, kind: 'timeseries', spec, ...(typeof v['title'] === 'string' ? { title: v['title'] } : {}) };
  }
  if (typeof v['cube'] !== 'string' || !cubes.has(v['cube'])) {
    throw new PageDocumentError(`view ${id} shows no cube of this page`);
  }
  if (v['kind'] === 'grid') {
    if (v['title'] !== undefined && !named('tile', v['title'])) {
      throw new PageDocumentError(`grid ${id}'s title is blank`);
    }
    return {
      id,
      kind: 'grid',
      cube: v['cube'],
      ...(typeof v['title'] === 'string' ? { title: v['title'] } : {}),
    };
  }
  if (v['kind'] !== 'chart') throw new PageDocumentError(`view ${id} is of an unknown kind ${JSON.stringify(v['kind'] ?? null)}`);
  if (typeof v['title'] !== 'string') throw new PageDocumentError(`chart ${id} has no title`);
  if (!named('tile', v['title'])) throw new PageDocumentError(`chart ${id}'s title is blank`);
  // its version (chart-spec.ts CHART_VERSION), and every word of the spec's slots one this DataCube knows
  const spec = readChart(v['spec']);
  if (typeof spec === 'string') throw new PageDocumentError(`chart ${id} has no chart it can draw: ${spec}`);
  const selection = v['selection'];
  if (selection !== undefined && !Array.isArray(selection)) {
    throw new PageDocumentError(`chart ${id}'s selection is not a list`);
  }
  return {
    id,
    kind: 'chart',
    cube: v['cube'],
    title: v['title'],
    spec,
    ...(selection && selection.length > 0 ? { selection: selection as FilterNode[] } : {}),
  };
}

/**
 * A layout of bands (`what` names it in what is said): its tiles, each a view of this page, the bands' rules kept. A
 * page's reader reads each sheet's by it, and a page command's layout is read by it too (page-commands.ts `layout`).
 */
export function readLayout(l: unknown, views: ReadonlySet<string>, what: string): PageLayout {
  if (!isObject(l)) throw new PageDocumentError(`${what} is not a layout`);
  if (l['kind'] === 'flow') return readFlow(l, views, what);
  if (l['kind'] !== 'bands') throw new PageDocumentError(`${what} is not a layout of bands or a flow`);
  if (typeof l['fit'] !== 'boolean') throw new PageDocumentError(`${what}'s fit is not true or false`);
  if (!Array.isArray(l['bands'])) throw new PageDocumentError(`${what}'s bands are not a list`);
  const bands = l['bands'].map((b, i) => {
    if (!isObject(b) || typeof b['height'] !== 'number') throw new PageDocumentError(`band ${i + 1} is not a height and its tiles`);
    return { height: b['height'], node: readNode(b['node'], views, `band ${i + 1}`) };
  });
  const layout: Bands = { fit: l['fit'], bands };
  const wrong = problems(layout);
  if (wrong.length > 0) throw new PageDocumentError(`${what} cannot be laid out: ${wrong.join('; ')}`);
  return { kind: 'bands', ...layout };
}

/** What is wrong with flow sheet `id`'s boxes on a page of `views`, in words, if anything: its boxes grids and comments,
 *  a frame a comment's, every setting of a box (an output, its stopping, its cache, its blocking, its message) a grid's.
 *  The page's reader's rule (readPage), and the layout command's (page-commands.ts layout: the roadmap audit of
 *  2026-10-26, a layout gave a comment a cache the reader then refused). */
export function flowSheetProblem(id: string, flow: FlowLayout, views: readonly PageView[]): string | undefined {
  const notGrid = flow.boxes.find((b) => views.find((v) => v.id === b.tile)?.kind === 'chart');
  if (notGrid !== undefined) return `sheet ${id} is a flow of grids and comments: ${notGrid.tile} is a chart`;
  // a frame is a container's, a comment's: a grid is a box, never a container
  const framedGrid = flow.boxes.find((b) => b.frame !== undefined && views.find((v) => v.id === b.tile)?.kind !== 'text');
  if (framedGrid !== undefined) return `${framedGrid.tile} has a frame: a container is a comment, not a grid`;
  // an output is a grid's: a comment writes no rows
  const textOut = flow.boxes.find((b) => b.output !== undefined && views.find((v) => v.id === b.tile)?.kind !== 'grid');
  if (textOut !== undefined) return `${textOut.tile} has an output: an output is a grid's rows, not a comment`;
  const textStops = flow.boxes.find((b) => b.stops === true && views.find((v) => v.id === b.tile)?.kind !== 'grid');
  if (textStops !== undefined) return `${textStops.tile} stops a run: a Test is a grid's, not a comment`;
  const textCache = flow.boxes.find((b) => b.cache === true && views.find((v) => v.id === b.tile)?.kind !== 'grid');
  if (textCache !== undefined) return `${textCache.tile} caches its rows: a cache is a grid's, not a comment`;
  const textSays = flow.boxes.find((b) => (b.blocks === true || b.message !== undefined) && views.find((v) => v.id === b.tile)?.kind !== 'grid');
  if (textSays !== undefined) return `${textSays.tile} blocks a run or says a message: each is a grid's, not a comment`;
  return undefined;
}

/** A flow sheet's layout (layout/flow.ts): its boxes, each a tile of the page at a place, every rule of a flow held. */
function readFlow(l: Record<string, unknown>, views: ReadonlySet<string>, what: string): FlowLayout {
  const extra = Object.keys(l).filter((k) => k !== 'kind' && k !== 'boxes');
  if (extra.length > 0) throw new PageDocumentError(`${what} is a flow of boxes, not ${extra.join(', ')}`);
  if (!Array.isArray(l['boxes'])) throw new PageDocumentError(`${what}'s boxes are not a list`);
  const boxes = l['boxes'].map((b, i) => {
    if (!isObject(b) || typeof b['tile'] !== 'string' || typeof b['x'] !== 'number' || typeof b['y'] !== 'number') {
      throw new PageDocumentError(`box ${i + 1} of ${what} is not a tile and its place (x, y)`);
    }
    if (!views.has(b['tile'])) throw new PageDocumentError(`a box shows no view of this page (${b['tile']})`);
    const odd = Object.keys(b).filter((k) => !['tile', 'x', 'y', 'frame', 'output', 'stops', 'cache', 'blocks', 'message'].includes(k));
    if (odd.length > 0) throw new PageDocumentError(`box ${i + 1} of ${what} is a tile, its place, a frame, an output, its stopping, its cache, its blocking and its message, not ${odd.join(', ')}`);
    if (b['blocks'] !== undefined && b['blocks'] !== true) throw new PageDocumentError(`box ${i + 1} of ${what} blocks a run (true) or is without it, not ${JSON.stringify(b['blocks'])}`);
    const said = b['message'] === undefined ? undefined : readMessage(b['message']);
    if (typeof said === 'string') throw new PageDocumentError(`box ${i + 1} of ${what}'s message: ${said}`);
    if (b['stops'] !== undefined && b['stops'] !== true) throw new PageDocumentError(`box ${i + 1} of ${what} stops a run (true) or is without it, not ${JSON.stringify(b['stops'])}`);
    if (b['cache'] !== undefined && b['cache'] !== true) throw new PageDocumentError(`box ${i + 1} of ${what} caches its rows (true) or is without it, not ${JSON.stringify(b['cache'])}`);
    const o = b['output'];
    const read = o === undefined ? undefined : readOutput(o);
    if (typeof read === 'string') throw new PageDocumentError(`box ${i + 1} of ${what}'s output: ${read}`);
    const output = { ...(read === undefined ? {} : { output: read }),
      ...(b['stops'] === true ? { stops: true as const } : {}), ...(b['cache'] === true ? { cache: true as const } : {}),
      ...(b['blocks'] === true ? { blocks: true as const } : {}), ...(said !== undefined ? { message: said } : {}) };
    const f = b['frame'];
    if (f === undefined) return { tile: b['tile'], x: b['x'], y: b['y'], ...output };
    if (!isObject(f) || typeof f['w'] !== 'number' || typeof f['h'] !== 'number' || (f['collapsed'] !== undefined && f['collapsed'] !== true)
      || Object.keys(f).some((k) => !['w', 'h', 'collapsed'].includes(k))) {
      throw new PageDocumentError(`box ${i + 1} of ${what}'s frame is {w, h, collapsed?}, not ${JSON.stringify(f)}`);
    }
    return { tile: b['tile'], x: b['x'], y: b['y'], frame: { w: f['w'], h: f['h'], ...(f['collapsed'] === true ? { collapsed: true as const } : {}) }, ...output };
  });
  const wrong = flowProblems({ boxes });
  if (wrong.length > 0) throw new PageDocumentError(`${what} cannot be laid out: ${wrong.join('; ')}`);
  return { kind: 'flow', boxes };
}

function readNode(n: unknown, views: ReadonlySet<string>, where: string): Node {
  if (isObject(n) && typeof n['tile'] === 'string') {
    if (!views.has(n['tile'])) throw new PageDocumentError(`a tile shows no view of this page (${n['tile']})`);
    return { tile: n['tile'] };
  }
  // a stack of tiles (the design's §7.4): its tiles in their tabs' order; it reopens on its first tab
  if (isObject(n) && Array.isArray(n['stack'])) {
    const stack = n['stack'].map((t) => {
      if (typeof t !== 'string' || !views.has(t)) throw new PageDocumentError(`${where}'s stack holds no view of this page (${JSON.stringify(t)})`);
      return t;
    });
    return { stack };
  }
  if (!isObject(n) || (n['split'] !== 'row' && n['split'] !== 'column') || !Array.isArray(n['parts'])) {
    throw new PageDocumentError(`${where} is not a tile, a stack or a split of parts`);
  }
  return {
    split: n['split'],
    parts: n['parts'].map((part, i) => {
      if (!isObject(part) || typeof part['size'] !== 'number') throw new PageDocumentError(`${where}, part ${i + 1}, has no size`);
      return { node: readNode(part['node'], views, `${where}, part ${i + 1}`), size: part['size'] };
    }),
  };
}

function parse(text: string): unknown {
  try {
    return fromJson(text);
  } catch (e) {
    throw new PageDocumentError(`not valid JSON (${e instanceof Error ? e.message : String(e)})`);
  }
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof ExactNumber);
}

/** JSON read exactly (fromJson), its numbers plain: what a page's parts but its cubes are read as (their filters, its
 *  layout's shares) -- the headless lens's page filters too (grid.ts lensOf). */
export function plainNumbers(v: unknown): unknown {
  if (v instanceof ExactNumber) return Number(v.text);
  if (Array.isArray(v)) return v.map(plainNumbers);
  if (isObject(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plainNumbers(x)]));
  return v;
}

const plain = plainNumbers;
