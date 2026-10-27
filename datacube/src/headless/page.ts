// A PAGE DOCUMENT READ AND WRITTEN AS DATACUBE READS AND WRITES ONE, HEADLESS (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md,
// item 1): what Python's pages ask of page-document.ts when one is loaded, read back from the open page, given a sheet's
// layout, saved or written out -- a page of DataCube's own version, every one of its rules held
// (readPage), written as DataCube writes it (pageToJson) -- so Python checks and writes no page by rules of its own.

import { CubeDocumentError, cubeCopied, cubeToJson, readCube } from '../cube-document.ts';
import {
  gridCopyOf, newPage, noneOpened, NOTHING_TO_SAVE, openedLive, PageDocumentError, pageFile as fileOf, pageToJson, readPage,
  pageSaving, readSaved, savedPage, suggestedName, type ChartView, type PageDocument,
} from '../page-document.ts';
import { givenName, type NamedPart } from '../page-names.ts';
import { SAVED_SORTS, savedSearch, type QuerySearchSortBy } from '../cube-store.ts';
import { applyPageCommands, EXACT_FIELD, followedPage, opensAgain, readPageCommand, type PageCommand } from '../page-commands.ts';
import { readGridCommand, type GridCommand } from '../grid-commands.ts';
import { pageReads as readsOf, rebaseEdits, withCube, type PageEdit } from '../page-rebase.ts';
import { CubeRefusal, sameValue, type FilterNode } from '../snapshot.ts';
import { toFilterNode } from '../filter-operators.ts';
import { drillDown, drillLevels, drillRefusal, drillUp, markConditions, namedMark } from '../chart-spec.ts';
import { chartPicked } from '../page-filters.ts';
import { PAGE_GESTURES, isPageGesture } from '../page-gestures.ts';
import { baseOnHost, draftFrom, gridApply, readGivenFilter, untagged, type HeldCopy } from './grid.ts';
import { onHost, type HostTables } from './host-tables.ts';
import { resolvedSource } from '../live-inputs.ts';
import { fromJson, toJson } from '../../../pure-protocol/src/index.ts';
import { defaultLabel } from '../../../engine-client/src/snap.ts';

/**
 * A SAVED FILE (JSON text) AS THE PAGE IT OPENS AS, as the tab's Open… opens a file (page-document.ts readSaved and
 * savedPage; Python's Page.load): a page file, or a Cube File, a page of its one grid (the S7 audit's Y-5: Python
 * refused a Cube File). JSON: `{page}`, the page's text as DataCube writes it (its numbers exact), or `{problems}`, why
 * DataCube does not read it, in its words.
 */
export function pageRead(text: string): string {
  try {
    // a page read as a file or a document is no saved record (B13), as the tab's Open… of a file or text opens one: a
    // page opened by its id holds that record by its savedAs (Python's Page.open) -- the S7 confirmation audit's Y2-3
    const { record: _saved, ...read } = savedPage(readSaved(text));
    return JSON.stringify({ page: pageToJson(read) });
  } catch (e) {
    if (!(e instanceof PageDocumentError) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A PAGE DOCUMENT (JSON text) AS DATACUBE SAVES IT, or writes it as a page file -- what Python's `save` and `to_json`
 * ask: read as an open page is (a page of no grid too, as one shown may be: D2) and refused as the screen's Save and
 * Export ▸ Page File refuse it, in their words, when there is nothing on it (NOTHING_TO_SAVE, B14). JSON: `{page}`, the
 * page's text as DataCube writes it, or `{problems}`, in DataCube's words.
 */
export function pageSaved(text: string): string {
  try {
    const page = readPage(text, { empty: true });
    return JSON.stringify(page.cubes.length === 0 ? { problems: [NOTHING_TO_SAVE] } : { page: pageToJson(page) });
  } catch (e) {
    if (!(e instanceof PageDocumentError) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A PAGE DOCUMENT (JSON text) AS A PAGE FILE -- what Python's `save(path)` writes -- by DataCube's one rule, Export ▸
 * Page File's (page-document.ts pageFile, B5). JSON: `{file}`, the file's text, or `{problems}`, why it is none.
 */
export function pageFile(text: string): string {
  try {
    const made = fileOf(readPage(text, { empty: true }));
    return JSON.stringify('refused' in made ? { problems: [made.refused] } : { file: made.file });
  } catch (e) {
    if (!(e instanceof PageDocumentError) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A PAGE DOCUMENT (JSON text) SAVED UNDER `name` AS THE PAGE STORE KEEPS IT -- what Python's `save()` and `save_as()`
 * send the store -- by the Save window's one rule (page-document.ts pageSaving, Z18, B4, B13 and K13): over the record
 * the page was last saved as, whichever editor saved it, else -- or, Save As (`asNew`), always -- a new one under
 * `fresh`; named `name`, else as the Save window suggests (page-document.ts suggestedName: an unnamed page by its first
 * sheet, D11). JSON: `{record, id, name, over}`, the record's text (its numbers exact), its id, the name it is saved
 * under and whether it saves over the record it is, or `{problems}`, why it is not saved.
 */
export function pageRecord(fresh: string, name: string | null, text: string, asNew: unknown): string {
  if (typeof asNew !== 'boolean') throw new Error('pageRecord: asNew is true or false');
  try {
    const page = readPage(text, { empty: true });
    if (page.cubes.length === 0) return JSON.stringify({ problems: [NOTHING_TO_SAVE] });
    // a name given, else the one the Save window suggests (D11: an unnamed page, its first sheet's)
    const made = pageSaving(name ?? suggestedName(page), page, fresh, asNew);
    if ('refused' in made) return JSON.stringify({ problems: [made.refused] });
    const { record } = made;
    return `{"record":${JSON.stringify(toJson(record))},"id":${JSON.stringify(record.id)},`
      + `"name":${JSON.stringify(record.name)},"over":${JSON.stringify(made.over)}}`;
  } catch (e) {
    if (!(e instanceof PageDocumentError) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A NEW PAGE, as DataCube starts one (page-document.ts newPage, the one skeleton: D11, B9): unnamed when `name` is null
 * ('', as the tab's New Page), else named by DataCube's one rule (givenName) -- so Python writes no page skeleton of
 * its own. JSON: `{page}`, its text as DataCube writes it, or `{problems}`, why the name was refused, in DataCube's words.
 * A page with no cube is shown and followed (D2), never saved (pageSaved): Python's first grid makes it one DataCube
 * saves.
 */
export function pageNew(name: unknown): string {
  if (name === null) return JSON.stringify({ page: pageToJson(newPage()) });
  const given = givenName('page', name);
  if ('refused' in given) return JSON.stringify({ problems: [given.refused] });
  if (given.name === undefined) throw new Error("a page's name is never left to what it shows");
  return JSON.stringify({ page: pageToJson(newPage(given.name)) });
}

const PARTS: readonly NamedPart[] = ['page', 'tile', 'sheet'];

/**
 * A NAME PYTHON GIVES a page, a tile or a sheet, by DataCube's one rule (page-names.ts givenName, which the screen's
 * renames ask too) -- so Python names nothing by a rule of its own. JSON: `{name}` (null: a sheet's blank, none of its
 * own) or `{problems}`, why it was refused, in DataCube's words.
 */
export function nameGiven(part: string, text: unknown): string {
  if (!PARTS.includes(part as NamedPart)) throw new Error(`a name is a page's, a tile's or a sheet's, not a ${part}'s`);
  const given = givenName(part as NamedPart, text);
  return JSON.stringify('refused' in given ? { problems: [given.refused] } : { name: given.name ?? null });
}

/**
 * A PAGE EDITED BY COMMANDS (page-commands.ts: the one function the screen's page edits are judged by --
 * docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md): `page`, its document as Python holds it (JSON, its numbers exact),
 * `held`, every tile and sheet id it has held (a JSON list), and `commands`, a JSON list, each read by readPageCommand
 * (an unknown kind or field refused in words). Applied in order, the first refusal stopping them. JSON:
 * `{page, held, commands}` -- the page as DataCube writes it, the ids held now, the commands as applied (an id a
 * command left out allocated, so Python names its new tile by DataCube's rule) -- or `{problems}`, in DataCube's words.
 */
export function pageApply(page: string, held: string, commands: string): string {
  return applied(page, held, commands);
}

/**
 * A PAGE WITH ITS CUBE `cube` AS A GRID'S EDIT LEFT IT (`document`, gridApply's), and its charts following it -- each live
 * chart regrouped, each selection its filter no longer holds taken off -- by the page's own rule (page-commands.ts
 * followedPage: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md V1, V2): what Python's grid edits keep. JSON: `{page}`, the
 * page as DataCube writes it, or `{problems}`, in DataCube's words.
 */
export function pageWithCube(page: string, cube: string, document: string): string {
  try {
    const doc = heldPage(page);
    if (!doc.cubes.some((c) => c.id === cube)) throw new CubeRefusal(`'${cube}' is no grid's cube of the page`);
    return JSON.stringify({ page: pageToJson(followedPage(withCube(doc, cube, readCube(fromJson(document))), cube, doc)) });
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

function applied(page: string, held: string, commands: string): string {
  let read;
  try {
    const doc = heldPage(page);
    // a grid's cube read from the commands' exact numbers (a calculated column's 1.10 kept), as the page's cubes are
    const exact = fromJson(commands) as unknown[];
    read = (JSON.parse(commands) as unknown[]).map((c, i) => readPageCommand(exactly(c, exact[i])));
    const done = applyPageCommands({ page: doc, held: JSON.parse(held) as string[] }, read);
    return `{"page":${JSON.stringify(pageToJson(done.draft.page))},"held":${JSON.stringify(done.draft.held)},`
      + `"commands":[${done.applied.map((c) => toJson(c)).join(',')}]}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A PAGE FILTER AS PYTHON ASKS FOR ONE (Page.filter; docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2): `given`,
 * `{class, filter, id?}` -- the class by its path or its name, one a grid of the page reads; the condition as Python
 * writes a grid's filter (grid.ts readGivenFilter, its values tagged where JSON has no form for them: D12), built by the
 * filter editor's operands (draftFrom: each operator one its property's type is offered), over the class's properties
 * as the page's grids over it read them (their class sources' columns) -- made the setPageFilter command the screen's
 * Add a Filter makes, for pageApply. JSON: `{command}`, or `{problems}`, in DataCube's words.
 */
export function pageFilter(page: string, given: string): string {
  const doc = heldPage(page);
  const asked = JSON.parse(given) as { class: string; filter: unknown; id?: string };
  const sources = doc.cubes.map((c) => c.cube.source).flatMap((s) => (s._type === 'class' ? [s] : []));
  const over = sources.filter((s) => s.class === asked.class || s.name === asked.class);
  const classes = [...new Set(over.map((s) => s.class))];
  if (classes.length !== 1) {
    const all = [...new Set(sources.map((s) => s.class))];
    return JSON.stringify({ problems: [classes.length === 0
      ? `no grid of the page reads a class ${JSON.stringify(asked.class)}${all.length ? ` (its grids read ${all.join(', ')})` : ' (none reads a class)'}`
      : `${asked.class} names ${classes.join(' and ')}: give the class's path`] });
  }
  const types = new Map<string, string | undefined>(over.flatMap((s) => s.columns.map((c) => [c.name, c.type] as const)));
  const filter = readGivenFilter(asked.filter);
  if (typeof filter === 'string') return JSON.stringify({ problems: [filter] });
  const draft = draftFrom(filter, types);
  if (typeof draft === 'string') return JSON.stringify({ problems: [draft] });
  const condition = toFilterNode(draft, (c) => types.get(c));
  if (condition === null) return JSON.stringify({ problems: ['a page filter lets something through: give it a condition'] });
  return `{"command":${toJson({ kind: 'setPageFilter', ...(asked.id !== undefined ? { id: asked.id } : {}), class: classes[0]!, condition, from: null })}}`;
}

/** A value a mark is named by, as a host gives it (untagged: D12): text, a number, true or false, or null (none). */
function markValue(raw: unknown, what: string): { readonly value: string | number | boolean | null } | string {
  const v = untagged(raw);
  if (v === null || typeof v === 'string' || typeof v === 'boolean' || (typeof v === 'number' && Number.isFinite(v))) return { value: v };
  return `${what} is text, a number, true or false, a date or a timestamp, not ${JSON.stringify(raw)}`;
}

/** The chart `chart` of a page as Python holds it, or why not. */
function chartOf(doc: PageDocument, chart: string): ChartView | string {
  const view = doc.views.find((v) => v.id === chart);
  return view?.kind === 'chart' ? view : `'${chart}' is no chart of the page`;
}

/**
 * WHETHER THE PAGE'S CUBE `cube` OPENS AGAIN OVER ITS SOURCE AS IT IS NOW, as a page opened again takes it (page-commands.ts
 * opensAgain, rebuiltCommands' rule): what Python asks before it opens a cube again over its frames (the S7 audit's Y-2:
 * it kept the rule itself). JSON: true or false.
 */
export function cubeOpensAgain(page: string, cube: string): string {
  return JSON.stringify(opensAgain(heldPage(page), cube));
}

/**
 * A PAGE `name` OPENED WITH `opened` OF ITS CUBES OPENED: why it is not opened, in the tab's words (page-document.ts
 * noneOpened, the tab's Open…'s rule), or null -- what Python's load asks once each cube is opened or left out (the S7
 * confirmation audit's Y2-2). JSON: the words, or null.
 */
export function pageNoneOpened(name: string, opened: number): string {
  return JSON.stringify(noneOpened(name, opened) ?? null);
}

/**
 * A PAGE'S GESTURE AS PYTHON MAKES IT (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8): gesture `name` (page-gestures.ts
 * PAGE_GESTURES: Show Grid's steps, ...) given its arguments by name (`given`, JSON), by the one function the screen's
 * gesture calls: its page commands, for Python to apply as it applies its own -- or `{problems}`, in DataCube's words.
 */
export function pageGesture(name: string, given: string): string {
  if (!isPageGesture(name)) {
    return JSON.stringify({ problems: [`'${name}' is no gesture of a page's: ${Object.keys(PAGE_GESTURES).join(', ')}`] });
  }
  const args: unknown = fromJson(given);
  if (typeof args !== 'object' || args === null || Array.isArray(args)) {
    return JSON.stringify({ problems: [`a gesture's arguments are given by name, not ${given}`] });
  }
  try {
    const commands = PAGE_GESTURES[name](args as Readonly<Record<string, unknown>>);
    return `{"commands":[${commands.map((c) => toJson(c)).join(',')}]}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A CHART'S MARK SELECTED AS PYTHON NAMES ONE (chart.select; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D8): `given`,
 * `{value, split?}` -- the mark's value across and, on a split chart, its series', each tagged where JSON has no form
 * for it (D12) -- or `{value: null}`: the selection cleared. Named as a click names it (chart-spec.ts namedMark) and
 * made the click's conditions (markConditions: a drilled chart's path with them), then what the screen's click does with
 * them (page-filters.ts chartPicked, which page/cube-page.ts #select asks too): on a grid over a class, those on its properties the
 * chart's page filter; the rest the chart's selection -- its grid's `refilter`, the old selection off and the new on, and
 * the page's `select`. Explicit, never toggled (D8): the same mark again is kept. A chart whose grid was removed
 * (detached) filters no grid: refused. JSON: `{grid, commands, page}` -- the grid's tile and its commands (for
 * gridApply), then the page's (for pageApply), in that order, as the screen tells them -- or `{problems}`, in DataCube's
 * words.
 */
export function chartPick(page: string, chart: string, given: string): string {
  const doc = heldPage(page);
  const view = chartOf(doc, chart);
  if (typeof view === 'string') return JSON.stringify({ problems: [view] });
  const grid = doc.views.find((v) => v.kind === 'grid' && v.cube === view.cube);
  if (grid === undefined) return JSON.stringify({ problems: [`'${chart}' is detached (its grid was removed): its marks filter no grid`] });
  const asked = JSON.parse(given) as { value?: unknown; split?: unknown };
  const cube = doc.cubes.find((c) => c.id === view.cube)!.cube;
  let conditions: FilterNode[] | null = null;
  if (asked.value === null) {
    if ('split' in asked) return JSON.stringify({ problems: ['a selection cleared names no mark: it takes no split'] });
  } else {
    const value = markValue(asked.value, "a mark's value across");
    if (typeof value === 'string') return JSON.stringify({ problems: [value] });
    const split = 'split' in asked ? markValue(asked.split, "a mark's series") : undefined;
    if (typeof split === 'string') return JSON.stringify({ problems: [split] });
    const key = namedMark(view.spec, value.value, split);
    if (typeof key === 'string') return JSON.stringify({ problems: [key] });
    conditions = markConditions(view.spec, { ...cube.query, epoch: 0 }, key);
  }
  // what the screen's click does with them, by its one rule (page-filters.ts chartPicked), named and never toggled
  const picked = chartPicked({ source: cube.source, chart, conditions, old: view.selection ?? [], filters: doc.filters ?? [], toggle: false });
  return JSON.stringify({ grid: grid.id, commands: picked.refilter ? [picked.refilter] : [], page: picked.page });
}

/**
 * A CHART DRILLED AS PYTHON DRILLS ONE (chart.drill, chart.drill_up; D8): `move` 'down', into the mark `given` names by
 * its value across (`{value}`, tagged where JSON has no form for it: D12; null the empty one) -- chart-spec.ts
 * drillDown, the screen's double-click's -- or 'up', `{levels}` levels (drillUp, its crumbs'), over the chart's cube as
 * the page holds it (its groupings the levels). JSON: `{command}`, the chart's setChart (for pageApply), or `{problems}`,
 * in DataCube's words.
 */
export function chartDrill(page: string, chart: string, move: string, given: string): string {
  const doc = heldPage(page);
  const view = chartOf(doc, chart);
  if (typeof view === 'string') return JSON.stringify({ problems: [view] });
  const asked = JSON.parse(given) as { value?: unknown; levels?: unknown };
  const spec = view.spec;
  const setChart = (next: typeof spec): string => JSON.stringify({ command: { kind: 'setChart', chart, spec: next } });
  if (move === 'up') {
    const depth = spec.drill?.path.length ?? 0;
    const levels = asked.levels;
    if (typeof levels !== 'number' || !Number.isInteger(levels) || levels < 1) {
      return JSON.stringify({ problems: [`a chart goes up 1 level or more, a whole number, not ${JSON.stringify(levels)}`] });
    }
    if (depth === 0) return JSON.stringify({ problems: ['the chart is not drilled: it is at its top level'] });
    if (levels > depth) {
      return JSON.stringify({ problems: [`the chart is drilled ${depth} level${depth === 1 ? '' : 's'} down: it goes up ${depth} at most`] });
    }
    return setChart(drillUp(spec, depth - levels));
  }
  if (move !== 'down') throw new Error(`a chart drills 'down' or 'up', not ${JSON.stringify(move)}`);
  const shape = { ...doc.cubes.find((c) => c.id === view.cube)!.cube.query, epoch: 0 };
  const refused = drillRefusal(spec, shape);
  if (refused !== null) return JSON.stringify({ problems: [refused] });
  const value = markValue(asked.value, 'the value drilled into');
  if (typeof value === 'string') return JSON.stringify({ problems: [value] });
  const next = drillDown(spec, shape, value.value);
  if (next === null) {
    const levels = drillLevels(spec, shape)!;
    return JSON.stringify({ problems: [`the chart is drilled to ${levels[levels.length - 1]!}, the last of its levels (${levels.join(', ')}): nothing is below it`] });
  }
  return setChart(next);
}

/**
 * A GRID'S COPY AS PYTHON MAKES ONE (grid.copy: Copy of Grid, cube-document.ts cubeCopied): `cube`, the grid's document
 * as the page holds it, as the copy's document -- what Python's addGrid command carries, as the screen's does. JSON:
 * `{cube}`, its text as DataCube writes it, or `{problems}`.
 */
export function gridCopied(cube: string): string {
  try {
    return JSON.stringify({ cube: cubeToJson(cubeCopied(readCube(cube))) });
  } catch (e) {
    if (!(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A SAVED PAGE AS IT OPENS (the tab's Open…: page-document.ts readSaved and savedPage; Python's Page.open): `record`, the
 * page store's record as its GET answers it -- its content a page, or a cube saved alone before pages -- read as the tab
 * reads it, the saved page it is that record. JSON: `{page}`, its text as DataCube writes it, or `{problems}`, in DataCube's words.
 */
export function pageOfRecord(record: string): string {
  try {
    const read = fromJson(record) as { readonly id?: unknown; readonly content?: unknown } | null;
    if (read === null || typeof read !== 'object' || read.content === undefined) {
      return JSON.stringify({ problems: ["not a saved page: the store's record has no content"] });
    }
    if (typeof read.id !== 'string' || read.id === '') return JSON.stringify({ problems: ["not a saved page: the store's record has no id"] });
    // the saved page it is, the record it was opened from, as the tab's Open… holds it -- a cube saved alone, or a page
    // saved before B13, carries none of its own (the S7 audit's Y-8: Python's next save made a second record)
    return JSON.stringify({ page: pageToJson({ ...savedPage(readSaved(read.content)), record: read.id }) });
  } catch (e) {
    if (!(e instanceof PageDocumentError) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A SEARCH OF THE SAVED PAGES AS OPEN… ASKS THE STORE IT (cube-store.ts savedSearch, the tab's library's: K15) -- what
 * Python's `ll.Page.saved()` sends the store: the pages `term` matches, in `sort`'s order (DataCube's spellings, as Open…'s
 * menu: null, its first), the first `limit` (null, as many as the store answers). JSON: `{spec}`, the search's
 * specification, or `{problems}`, what was given that is none, in words.
 */
export function pageSearch(term: unknown, sort: unknown, limit: unknown): string {
  const problems = [
    ...(typeof term === 'string' ? [] : [`a search is text, not ${JSON.stringify(term)}`]),
    ...(sort === null || SAVED_SORTS.some(([s]) => s === sort) ? []
      : [`a sort is one of ${SAVED_SORTS.map(([s]) => s).join(', ')}, not ${JSON.stringify(sort)}`]),
    ...(limit === null || (Number.isInteger(limit) && (limit as number) >= 0) ? []
      : [`a limit is a whole number, 0 or more, not ${JSON.stringify(limit)}`]),
  ];
  return JSON.stringify(problems.length > 0 ? { problems }
    : { spec: savedSearch(term as string, sort as QuerySearchSortBy | null, limit as number | null) });
}

/**
 * A page as Python holds it, read as the page commands take it: written by DataCube's writers (pageNew, pageRead,
 * gridApply), so its parts are read as they are -- a new page has no cube yet, which readPage refuses -- but each cube
 * by its own reader (readCube: a calculated column's numbers exact, every other number plain, as DataCube holds a
 * cube), and the fields a newer writer added kept aside, as the page's own reader keeps them (`unknown`).
 */
/** A page command as read plainly, the fields it carries a cube's document or source in taken from its exact reading
 *  (its numbers as the protocol's, as the page's cubes are read: page-commands.ts EXACT_FIELD). */
export function exactly(plainly: unknown, exact: unknown): unknown {
  const c = plainly as { kind?: unknown } | null;
  const fields = typeof c?.kind === 'string' ? EXACT_FIELD[c.kind as keyof typeof EXACT_FIELD] : undefined;
  const of = exact as Record<string, unknown>;
  return fields === undefined ? plainly
    : { ...(plainly as object), ...Object.fromEntries(fields.filter((f) => of[f] !== undefined).map((f) => [f, of[f]])) };
}

export function heldPage(text: string): PageDocument {
  const { cubes, ...rest } = JSON.parse(text) as Record<string, unknown> & { cubes: { id: string }[] };
  const exact = (fromJson(text) as { cubes: { id: string; cube: unknown }[] }).cubes;
  const { kind, version, name, views, sheets, model, filters, copies, record, ...unknown } = rest;
  return {
    kind, version, name, views, sheets, ...(model !== undefined ? { model } : {}), ...(filters !== undefined ? { filters } : {}),
    // the copies its snapped grids read (§18.8): a field of the page, which its snap and live commands edit
    ...(copies !== undefined ? { copies } : {}),
    // the saved page it is (B13): a field of the page, which the savedAs command edits
    ...(record !== undefined ? { record } : {}),
    cubes: cubes.map((c, i) => ({ id: c.id, cube: readCube(exact[i]!.cube) })),
    ...(Object.keys(unknown).length > 0 ? { unknown } : {}),
  } as unknown as PageDocument;
}

/**
 * A SNAP AS ITS COMMAND (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): grid `grid` reads the copy its host made
 * (`made`: the host's answer -- its table, its rows, and the pull's receipt when it ran elsewhere), taken now, written
 * down as the screen's Snap writes it (page-document.ts gridCopyOf). JSON: the page command, for pageApply.
 */
export function gridSnapped(grid: string, made: string): string {
  const answer = JSON.parse(made) as {
    table: string; rowCount: number;
    pulledBy?: { plane: 'tab' | 'warehouse' | 'engine'; where: string; as?: string; statementId?: string; serverRows?: number };
  };
  const takenAt = new Date();
  const copy = gridCopyOf({
    table: answer.table, rowCount: answer.rowCount, takenAt, label: defaultLabel(takenAt),
    ...(answer.pulledBy ? { pulledBy: answer.pulledBy } : {}),
  });
  return JSON.stringify({ kind: 'snap', grid, copy });
}

/**
 * A SAVED PAGE AS IT OPENS (page-document.ts openedLive, §18.8): a page saved with snapped grids opens live, each said.
 * `text`: a page DataCube reads (pageRead). JSON: `{page, notes}`, the page as its document's text.
 */
export function pageOpenedLive(text: string): string {
  const opened = openedLive(readPage(text));
  return JSON.stringify({ page: pageToJson(opened.page), notes: opened.notes });
}

/** A writer's edits read from JSON, each by its own reader: a page command's (a grid's cube with its exact numbers, as
 *  pageApply reads one), or a grid's by readGridCommand -- as the page's keeper reads a write (pageRebase). */
export function readEdits(text: string): PageEdit[] {
  const exact = fromJson(text) as unknown[];
  const asked = JSON.parse(text) as unknown;
  if (!Array.isArray(asked)) throw new CubeRefusal("a writer's edits are a list");
  return asked.map((e, i) => {
    const edit = e as { page?: unknown; grid?: unknown; command?: unknown } | null;
    if (edit !== null && typeof edit === 'object' && 'page' in edit) {
      const c = edit.page as { kind?: unknown } | null;
      const command: PageCommand = readPageCommand(exactly(c, (exact[i] as { page: unknown }).page));
      return { page: command };
    }
    if (edit !== null && typeof edit === 'object' && typeof edit.grid === 'string') {
      const command: GridCommand = readGridCommand(edit.command);
      return { grid: edit.grid, command };
    }
    throw new CubeRefusal(`an edit is {page: <a page command>} or {grid, command}, not ${JSON.stringify(e)}`);
  });
}

/** What a page reads, as its keeper takes it (page-rebase.ts pageReads). */
const readsJson = (page: PageDocument): string => JSON.stringify(readsOf(page));

/**
 * WHAT A PAGE READS (page-rebase.ts pageReads), for its keeper, which never reads a page itself: `page` as Python holds
 * it. JSON: `{frames, copies, ids}` -- the frames its cubes read, the copies its snapped grids read (their tables),
 * every id it holds.
 */
export function pageReads(page: string): string {
  return readsJson(heldPage(page));
}

/**
 * A WRITER'S EDITS APPLIED TO THE PAGE AS IT IS NOW (page-rebase.ts rebaseEdits), for the page's keeper, which keeps what
 * a write's commands make (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4, H1) -- at the version they were made at there is
 * nothing to combine; at one the page has moved on from, they are made again on top: `page` and `held` as the keeper has
 * them, `edits` as the writer made them (JSON lists), `host` the tables the host serves (grid.ts HostTables: its frames,
 * files and remote files) and `copies` the copies it holds (by table), over which a grid's edit is applied as gridApply
 * applies one -- a snapped grid's over its copy, as the writer applied it. Every id an edit makes is its editor's own
 * (page-ids.ts nextPageId), so nothing is given out afresh (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md). JSON:
 * `{page, held, edits, reads}` -- the page they make (as DataCube writes it), the ids it has held, the edits as applied
 * there, and what the page reads -- or `{problems}`: one does not apply, or a cube they add is over a source the host
 * cannot hold (grid.ts onHost), in DataCube's words, and nothing was applied.
 */
export async function pageRebase(page: string, held: string, edits: string,
  host: HostTables, copies: Readonly<Record<string, HeldCopy>>): Promise<string> {
  try {
    const now = heldPage(page);
    const done = await rebaseEdits({ page: now, held: JSON.parse(held) as string[] }, readEdits(edits),
      async (at, cube, id, commands) => {
        const text = cubeToJson(cube);
        // a snapped grid's edit over its copy, as the writer applied it (baseOnHost: the one rule)
        // over its page as the edit found it: a grid over another grid's rows reads that grid there
        const base = await baseOnHost(text, (at.copies ?? {})[id], host, copies, at, id);
        const said = JSON.parse(await gridApply({ cube: text, ...base } as Parameters<typeof gridApply>[0],
          `[${commands.map((c) => toJson(c)).join(',')}]`)) as { cube?: string; problems?: string[] };
        if (said.problems) throw new CubeRefusal(said.problems.join('; '));
        return readCube(fromJson(said.cube!));
      });
    // what the host holds its cubes over (grid.ts onHost): a cube the write adds, or moves to another source, over one the
    // host cannot base its grid's edits on is refused now, in words -- never kept to refuse its first edit (the S4 audit's
    // 2). A snapped grid reads its copy.
    const was = new Map(now.cubes.map((c) => [c.id, c.cube.source]));
    for (const c of done.page.cubes) {
      if ((done.page.copies ?? {})[c.id] !== undefined) continue;
      // a grid over another grid's rows, held where what it reads is: its link resolved against the page it is on
      const grid = done.page.views.find((v) => v.kind === 'grid' && v.cube === c.id)?.id;
      if (!sameValue(was.get(c.id), c.cube.source)) await onHost(resolvedSource(done.page, c.cube.source, grid), host);
    }
    return `{"page":${JSON.stringify(pageToJson(done.page))},"held":${JSON.stringify(done.held)},`
      + `"edits":[${done.edits.map((e) => toJson(e)).join(',')}],`
      + `"reads":${readsJson(done.page)}}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError) && !(e instanceof PageDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}
