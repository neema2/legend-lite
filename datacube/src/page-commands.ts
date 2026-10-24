// A PAGE EDIT AS A VALUE (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md; the command program's stage 1, its page half): each
// edit of a page's structure -- its sheets, its tiles, their titles, their places -- is a PageCommand, plain JSON, and
// one pure function applies it to the page's document and the ids it has held. The rules the screen kept inline and
// Python copied (the homework's §8) are here once: ids and titles, what goes with a removed tile, where a moved or new
// tile goes, names. No DOM, no planner: what needs either (a cube's document, a chart's spec) is made before the
// command is built, as a grid command's calculated column is parsed first (grid-commands.ts).

import { fieldNames, type CommandFields, type MissingFields } from './command-fields.ts';
import { followCube, readChart, type ChartSpec } from './chart-spec.ts';
import {
  CubeDocumentError, cubeOpenedAgain, readCube, readCubeSource, type CubeDocument, type CubeSource, type JoinInput,
} from './cube-document.ts';
import {
  MAX_COLUMNS, NEW_SHEET, NotALayout, add, arrange, asSaved, remove, stackTiles, tiles, type Bands, type Preset,
} from './layout/bands.ts';
import {
  PageDocumentError, keptFor, readGridCopy, readLayout, unnamedLabel, writePageOf, type ChartView, type GridCopy, type GridView,
  type PageDocument, type PageLayout, type PageSheet, type PageView, type TextView,
} from './page-document.ts';
import { givenName } from './page-names.ts';
import type { PageModelRef } from './page-model.ts';
import { chartTitle, gridTitle, nextPageId, textTitle } from './page-ids.ts';
import { textProblem } from './page-text-rule.ts';
import { namedProperties, type PageFilter } from './page-filters.ts';
import { readFilterNode, type GridCommand } from './grid-commands.ts';
import type { PageState } from './page-state.ts';
import { CubeRefusal, withoutConditions, type CubeShape, type FilterNode } from './snapshot.ts';

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
  /** A chart reading another grid's cube from now on (D7: Show Grid made the grid of its own grouping) -- its selection
   *  taken off the cube it read, and that cube let go when no view reads it any more (a detached chart's). */
  | { readonly kind: 'followGrid'; readonly chart: string; readonly grid: string }
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
  /** The page's model made by its first model action (Create a Class…, a Data Space): its project, DataCube's workspace
   *  in it, the package its classes go in (modelMade, above). */
  | { readonly kind: 'modelInto'; readonly project: string; readonly workspace: string; readonly package: string }
  /** The page pinned to the revision its last model action saved (modelPinned, above). */
  | { readonly kind: 'modelPin'; readonly revision: string }
  /** The page's model keeping a grid's tables and its class -- null leaving either as it is (modelKept, above). */
  | { readonly kind: 'modelKeep'; readonly cube: string; readonly tables: readonly string[] | null; readonly class: string | null };

export const PAGE_COMMAND_KINDS = [
  'renamePage', 'savedAs', 'addSheet', 'renameSheet', 'moveSheet', 'removeSheet', 'renameTile', 'addGrid', 'addChart', 'setChart',
  'addText', 'setText', 'followGrid', 'select', 'moveTile', 'stack', 'removeTile', 'arrange', 'layout', 'setPageFilter', 'removePageFilter', 'snap', 'live',
  'rebuilt', 'modelInto', 'modelPin', 'modelKeep',
] as const satisfies readonly PageCommand['kind'][];

/** Each page command's fields, by kind (`?` one it may leave out): what readPageCommand takes, and what Python's generated
 *  builders write (D17). */
export const PAGE_COMMAND_FIELDS = {
  renamePage: ['name'], savedAs: ['record'], addSheet: ['id?', 'name'], renameSheet: ['sheet', 'name'], moveSheet: ['sheet', 'to'],
  removeSheet: ['sheet'], renameTile: ['tile', 'title'], addGrid: ['id?', 'document', 'sheet', 'near', 'title', 'columns'],
  addChart: ['id?', 'near', 'spec', 'title', 'columns'], setChart: ['chart', 'spec'], addText: ['id?', 'sheet', 'text', 'title', 'columns'],
  setText: ['tile', 'text'], followGrid: ['chart', 'grid'],
  select: ['chart', 'conditions'], moveTile: ['tile', 'sheet', 'columns'], stack: ['tiles'], removeTile: ['tile'],
  arrange: ['sheet', 'preset', 'order'], layout: ['sheet', 'layout'], setPageFilter: ['id?', 'class', 'condition', 'from'],
  removePageFilter: ['filter'], snap: ['grid', 'copy'], live: ['grid'], rebuilt: ['cube', 'source'],
  modelInto: ['project', 'workspace', 'package'], modelPin: ['revision'], modelKeep: ['cube', 'tables', 'class'],
} as const satisfies CommandFields<PageCommand>;
// every field of every kind listed: a field the list leaves out is a type error naming it
const pageFieldsWhole: [MissingFields<PageCommand, typeof PAGE_COMMAND_FIELDS>] extends [never] ? true
  : MissingFields<PageCommand, typeof PAGE_COMMAND_FIELDS> = true;
void pageFieldsWhole;

/** What a command applied gives: the page, and the command as applied (its id allocated, when it made something). */
export interface PageApplied {
  readonly draft: PageDraft;
  readonly command: PageCommand;
}

// ---------------------------------------------------------------- ids and titles (page-ids.ts: the screen's too)

export { chartTitle, gridTitle, nextPageId, textTitle } from './page-ids.ts';

// ---------------------------------------------------------------- applying

const named = (part: 'page' | 'tile' | 'sheet', text: string): string | undefined => {
  const given = givenName(part, text);
  if ('refused' in given) throw new CubeRefusal(given.refused);
  return given.name;
};

const saved = (layout: Bands): PageLayout => ({ kind: 'bands', ...asSaved(layout) });

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
  return page.sheets.find((s) => tiles(s.layout).includes(tile));
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
 * page, or a chart.
 */
export function joinInputOf(page: PageDocument, grid: string): JoinInput {
  const view = viewNamed(page, grid);
  if (view.kind !== 'grid') throw new CubeRefusal(`'${grid}' is ${kindWords(view)}: a join is of two grids`);
  const cube = page.cubes.find((c) => c.id === view.cube);
  if (!cube) throw new CubeRefusal(`'${grid}' shows no cube of the page`);
  return { name: view.title ?? cube.cube.source.name, source: cube.cube.source, query: cube.cube.query };
}

const withSheet = (page: PageDocument, id: string, change: (s: PageSheet) => PageSheet): PageDocument =>
  ({ ...page, sheets: page.sheets.map((s) => (s.id === id ? change(s) : s)) });

/**
 * The ids no command may give out: every one the page has held, its cubes' (a cube kept for a detached chart has no
 * view of its own: its id is taken all the same), and its filters' while it has them, whatever `held` lost (a page
 * opened again holds its tiles' ids only). page-rebase.ts asks it too, before a writer's id is given out on top.
 */
export function takenIds(draft: PageDraft): string[] {
  return [...draft.held, ...draft.page.cubes.map((c) => c.id), ...(draft.page.filters ?? []).map((f) => f.id)];
}

/**
 * A fresh id for a command that makes one, or the one it names -- refused when it is taken (takenIds).
 */
function freshId(draft: PageDraft, given: string | undefined, kind: 'grid' | 'chart' | 'text' | 'sheet' | 'filter'): string {
  const taken = takenIds(draft);
  if (given === undefined) return nextPageId(taken, kind);
  if (taken.includes(given)) throw new CubeRefusal(`'${given}' is an id the page has held already`);
  if (!new RegExp(`^${kind}-\\d+$`).test(given)) throw new CubeRefusal(`a ${kind}'s id is ${kind}-N, not '${given}'`);
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
const kindWords = (view: PageView): string => (view.kind === 'text' ? 'a text tile' : `a ${view.kind}`);

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
  const views = next.views.filter((v) => !gone.has(v.id));
  const cubes = next.cubes.filter((c) => views.some((v) => v.cube === c.id));
  const sheets = next.sheets.map((s) => {
    let layout: Bands = s.layout;
    for (const id of gone) if (tiles(layout).includes(id)) layout = remove(layout, id);
    return layout === s.layout ? s : { ...s, layout: saved(layout) };
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
function inScreenOrder(page: PageDocument): PageDocument {
  const reading = page.sheets.flatMap((s) => tiles(s.layout));
  const at = (id: string): number => (reading.includes(id) ? reading.indexOf(id) : reading.length);
  const inReading = (kind: PageView['kind']): PageView[] => page.views.filter((v) => v.kind === kind).map((v, i) => ({ v, i }))
    .sort((x, y) => at(x.v.id) - at(y.v.id) || x.i - y.i).map((x) => x.v);
  const views = [...inReading('grid'), ...page.views.filter((v) => v.kind === 'chart'), ...inReading('text')];
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
      const on = (): string[] => tiles(sheetNamed(next, sheet.id).layout);
      for (const tile of on().filter((t) => viewNamed(next, t).kind === 'grid')) next = withoutTile(next, tile);
      for (const tile of on()) next = withoutTile(next, tile);
      return done(rewritten(next, { sheets: next.sheets.filter((s) => s.id !== sheet.id) }));
    }
    case 'renameTile': {
      const view = viewNamed(page, command.tile);
      if (command.title === null && view.kind === 'chart') throw new CubeRefusal('a chart has a title');
      const given = command.title === null ? undefined : named('tile', command.title);
      // a grid or a text named what it was made named keeps no title of its own: it is numbered by its id, as it was
      const made = view.kind === 'grid' ? gridTitle(view.id) : view.kind === 'text' ? textTitle(view.id) : undefined;
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
      const title = command.title === null ? undefined : named('tile', command.title);
      const view: PageView = { id, kind: 'grid', cube: id, ...(title !== undefined ? { title } : {}) };
      // a grid is a table, which wants the page's width: a band of its own at the bottom of its sheet, never beside the
      // tile it came from, nor between that tile and its charts (the user, 2026-10-23: bands.ts add)
      const placed = withSheet(page, sheet.id, (s) => ({ ...s, layout: laid(() => add(s.layout, id, undefined, command.columns ?? MAX_COLUMNS)) }));
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
      // under its grid, beside the charts already there while that row has room (bands.ts add)
      const charts = (ids: readonly string[]): boolean => ids.every((t) => page.views.find((v) => v.id === t)?.kind === 'chart');
      const placed = withSheet(page, sheet.id, (s) => ({ ...s, layout: laid(() => add(s.layout, id, grid.id, command.columns ?? MAX_COLUMNS, charts)) }));
      return done({ ...placed, views: [...page.views, view] }, [...draft.held, id], { ...command, id });
    }
    case 'addText': {
      const problem = textProblem(command.text);
      if (problem !== null) throw new CubeRefusal(problem);
      const id = freshId(draft, command.id, 'text');
      const sheet = sheetNamed(page, command.sheet);
      const given = command.title === null ? undefined : named('tile', command.title);
      const title = given === textTitle(id) ? undefined : given;
      const view: TextView = { id, kind: 'text', text: command.text, ...(title !== undefined ? { title } : {}) };
      // a band of its own at the bottom of its sheet, as a grid is added (bands.ts add): a title or a note is moved where
      // it reads from there
      const placed = withSheet(page, sheet.id, (s) => ({ ...s, layout: laid(() => add(s.layout, id, undefined, command.columns ?? MAX_COLUMNS)) }));
      return done({ ...placed, views: [...page.views, view] }, [...draft.held, id], { ...command, id });
    }
    case 'setText': {
      const view = viewNamed(page, command.tile);
      if (view.kind !== 'text') throw new CubeRefusal(`'${command.tile}' is ${kindWords(view)}, not a text tile`);
      const problem = textProblem(command.text);
      if (problem !== null) throw new CubeRefusal(problem);
      return done({ ...page, views: page.views.map((v) => (v.id === view.id ? { ...view, text: command.text } : v)) });
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
      const cubes = unpicked.cubes.filter((c) => views.some((v) => v.cube === c.id));
      const kept = rewritten({ ...unpicked, ...(unpicked.model ? { model: keptFor(unpicked.model, cubes) } : {}) }, { views, cubes });
      return done(followedPage(kept, grid.cube, page));
    }
    case 'select': {
      const view = viewNamed(page, command.chart);
      if (view.kind !== 'chart') throw new CubeRefusal(`'${command.chart}' is ${kindWords(view)}, not a chart`);
      const { selection: _was, ...rest } = view;
      const chart: ChartView = command.conditions.length === 0 ? rest : { ...rest, selection: [...command.conditions] };
      return done({ ...page, views: page.views.map((v) => (v.id === view.id ? chart : v)) });
    }
    case 'moveTile': {
      viewNamed(page, command.tile);
      const from = sheetOf(page, command.tile);
      const to = sheetNamed(page, command.sheet);
      // onto its own sheet: nothing, as the screen's Move to Sheet offers only the others
      if (!from || from.id === to.id) return done(page);
      const out = withSheet(page, from.id, (s) => ({ ...s, layout: laid(() => remove(s.layout, command.tile)) }));
      return done(withSheet(out, to.id, (s) => ({ ...s, layout: laid(() => add(s.layout, command.tile, undefined, command.columns ?? MAX_COLUMNS)) })));
    }
    case 'stack': {
      if (new Set(command.tiles).size < 2) throw new CubeRefusal('a stack is two tiles or more');
      for (const t of command.tiles) viewNamed(page, t);
      const sheet = sheetOf(page, command.tiles[0]!);
      if (!sheet || command.tiles.some((t) => sheetOf(page, t) !== sheet)) {
        throw new CubeRefusal('the tiles of a stack are on one sheet (move them there first)');
      }
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: laid(() => stackTiles(s.layout, command.tiles)) })));
    }
    case 'removeTile':
      return done(withoutTile(page, command.tile));
    case 'arrange': {
      const sheet = sheetNamed(page, command.sheet);
      const order = command.order;
      return done(withSheet(page, sheet.id, (s) => ({
        ...s, layout: laid(() => (order === null ? arrange(s.layout, command.preset as Preset) : arrange(s.layout, command.preset as Preset, order))),
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
      const { kind: _kind, ...bands } = read;
      const had = [...tiles(sheet.layout)].sort();
      const has = [...tiles(bands)].sort();
      if (JSON.stringify(had) !== JSON.stringify(has)) {
        throw new CubeRefusal(`a sheet's layout holds its tiles, each once: ${had.join(', ') || 'none'}, not ${has.join(', ') || 'none'}`);
      }
      return done(withSheet(page, sheet.id, (s) => ({ ...s, layout: saved(bands) })));
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
  const { draft, applied } = applyPageCommands(state.draft, commands);
  if (step) state.begin(label);
  state.keep(draft);
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

/** A cube's source, read by the cube document's own reader: what it refuses, refused in its words. */
function sourceOf(v: unknown): CubeSource {
  try {
    return readCubeSource(v);
  } catch (e) {
    if (e instanceof CubeDocumentError) throw new CubeRefusal(e.message);
    throw e;
  }
}

/** The field of a page command a host reads with its exact numbers (a calculated column's 1.10, a join condition's
 *  decimal), as the page's cubes are read: a cube's document, or a source. */
export const EXACT_FIELD: Partial<Record<PageCommand['kind'], 'document' | 'source'>> = {
  addGrid: 'document', rebuilt: 'source',
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
    case 'followGrid':
      fields(o, kind, PAGE_COMMAND_FIELDS[kind]);
      return { kind, chart: text(o['chart'], 'a chart'), grid: text(o['grid'], 'a grid') };
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
