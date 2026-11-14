// A TIMESERIES TILE, HEADLESS (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §7): the spec of what Python tells it,
// made by DataCube's own rules -- its series' #TS texts read by core's reader (the host's legend-lite library:
// timeseries-text.ts readSeriesTexts, A3 step 3), a source's columns by DataCube's defaults, the whole read by the tile's
// one reader -- so Python's page.timeseries keeps no rule of its own (the charts' rule: headless gridChart). The page
// command that adds or sets it is Python's generated builder's, applied by pageApply. Every function that walks a
// series' tree reads the page's texts first (pageRead).

import { fromJson, toJson, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { callsLibrary } from '../pure-library.ts';
import { DATACUBE_LIBRARY } from '../pure-library-text.ts';
import { readPage, type PageDocument } from '../page-document.ts';
import { gridTitle } from '../page-ids.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot } from '../snapshot.ts';
import {
  askQuery, boundsQuery, curveColumns, curveOrder, marksOf, marksQuery, projectionOf, rangeWindow, readsAnchor, seriesNotes, seriesQuery, seriesSources, statBlanks, statName,
  tableSeries, tileAsks, viewSeries,
  TS_CAL_MONTH, TS_CAL_RETURN, TS_CAL_YEAR, TS_CAL_YEAR_RETURN, TS_HIST_COUNT, TS_HIST_HIGH, TS_HIST_LOW, TS_HIST_MEAN, TS_HIST_MID, TS_HIST_N,
  TS_HIST_NORMAL, TS_HIST_P5, TS_HIST_SD, TS_SC_CORR, TS_SC_INTERCEPT, TS_SC_N, TS_SC_R2, TS_SC_SLOPE, TS_SC_X, TS_SC_Y, TS_SEA_HIGH,
  TS_SEA_LATEST, TS_SEA_LOW, TS_SEA_MEAN, TS_SEA_PLACE, TS_SEA_YEAR,
  TS_PJ_END, TS_PJ_END_VALUE, TS_PJ_FROM, TS_PJ_FROM_VALUE, TS_PJ_LAST, TS_PJ_LAST_VALUE, TS_PJ_N, TS_PJ_SLOPE,
  TS_CORR, TS_CORR_A, TS_CORR_B, TS_CORR_N, TS_DATE, TS_DD_DECLINE, TS_DD_DEPTH, TS_DD_PEAK, TS_DD_RANK, TS_DD_RECOVERED,
  TS_DD_RECOVERY, TS_DD_TROUGH, TS_HIGH, TS_LATEST, TS_LOW, TS_LOWER, TS_OPEN, TS_PER_YEAR, TS_ROWS, TS_SPLIT, TS_STAT_LEVEL_LOW,
  TS_STAT_PER_YEAR, TS_STATS, TS_TEXT, TS_UPPER, TS_VALUE, type TsInput, type TsInputs, type TsQuery, type TsTable, type TsWindow,
} from '../timeseries-query.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { HostTables } from './host-tables.ts';
import { eachRelation } from '../join-open.ts';
import { sourcesOnHost, sourcesOpenedOn } from './join.ts';
import { marksCut, marksName, projectionName, TS_CURVE_DATES, TS_MARK_LIMIT, tsProblem, type TimeseriesSpec, type TsSeries, type TsSource, type TsView } from '../timeseries-spec.ts';
import { aliasFor, nextSeriesId, seriesName, sourceOf, splitName, type TsColumn } from '../timeseries-expr.ts';
import { readSeriesTexts, readTyped, TsTextRefusal, writtenText, type TsReader } from '../timeseries-text.ts';
import { termOf } from '../timeseries-builder-wire.ts';
import { BuilderRefusal } from '../timeseries-builder-core.ts';
import { LitePlanner } from '../lite-planner.ts';
import { hostCalls, hostWorker } from './host.ts';
import { codeOfPage, tileCode, TS_CODE_LANGUAGES, type TsCodeLanguage } from '../timeseries-code.ts';
import { editTimeseries, firstPlotOf, judgedEdit, TsEditRefusal, type TsChange, type TsGridColumns } from '../timeseries-edit.ts';

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** A grid of the page by its tile's id: its cube's id and its columns as the cube's query types them. */
function gridOf(page: PageDocument, grid: string): TsGridColumns {
  const view = page.views.find((v) => v.id === grid);
  if (!view || view.kind !== 'grid') throw new Problem(`'${grid}' is no grid of the page`);
  const held = page.cubes.find((c) => c.id === view.cube);
  if (!held) throw new Problem(`'${grid}' shows no cube the page holds`);
  const columns = rowColumns(held.cube.query).map((c) => ({ name: c.name, type: c.type }));
  // titled as the page names its tile (page-ids.ts gridTitle): a source's alias is called after it, as on the screen
  return { cube: view.cube, title: view.title ?? gridTitle(view.id), columns, rows: held.cube.query.rows, filter: held.cube.query.filter };
}

export class Problem extends Error {}

/** Core's reader of a series' text: the host's legend-lite library, one planner of no model made the first time asked. */
let grammar: LitePlanner | undefined;
const reader: TsReader = (text) => (grammar ??= new LitePlanner({ model: '', runtime: '', worker: () => hostWorker(hostCalls()) })).tsItem(text);

/** A page as Python holds it (JSON's value), read, every timeseries tile's texts read by core: what a walk of a series'
 *  tree needs first. */
async function pageRead(raw: unknown, options: { readonly empty?: boolean } = {}): Promise<PageDocument> {
  const doc = readPage(raw, options);
  await Promise.all(doc.views.flatMap((v) => (v.kind === 'timeseries' ? [readSeriesTexts(v.spec.series, reader)] : [])));
  return doc;
}

/** A first plot of a source told by its grid, by the screen's Create ▸ Timeseries' rule (timeseries-edit.ts firstPlotOf:
 *  its price-like value, a line per instrument, a price's rebased) -- refused in its words where the screen refuses
 *  (the review's A3: a grid of a date and no number made a tile of no series). */
function firstOf(doc: PageDocument, told: unknown, source: TsSource | undefined): Pick<TimeseriesSpec, 'series' | 'view'> | undefined {
  if (!isRecord(told) || typeof told['grid'] !== 'string' || source === undefined) return undefined;
  const plot = firstPlotOf(gridOf(doc, told['grid']), source);
  if (typeof plot === 'string') throw new Problem(plot);
  return plot;
}

/** A slot Python told, a word or absent: of any other kind, refused by name (the review's G2: swapped for the default). */
function wordOf(v: unknown, what: string): string | undefined {
  if (v === undefined) return undefined;
  if (typeof v !== 'string') throw new Problem(`${what} is ${JSON.stringify(v)}, not a word`);
  return v;
}

/** The columns each source of `spec` reads, as the page holds its cube (its query's), by alias: what the viewer reads. */
function columnsByAlias(doc: PageDocument, spec: TimeseriesSpec): Map<string, TsColumn[] | undefined> {
  return new Map(spec.sources.map((s) => {
    const held = doc.cubes.find((c) => c.id === s.cube);
    return [s.alias, held ? rowColumns(held.cube.query).map((c) => ({ name: c.name, type: c.type })) : undefined] as const;
  }));
}

/** A spec Python made, judged as the viewer judges its change (timeseries-edit.ts judgedEdit) against the grids it reads:
 *  a problem it makes refused in its words (the review's A5); one the tile had before it (`before`), the tile's still. */
function judged(doc: PageDocument, before: TimeseriesSpec | null, made: unknown): TimeseriesSpec {
  try {
    return judgedEdit(before, made, (s) => columnsByAlias(doc, s));
  } catch (e) {
    if (e instanceof TsEditRefusal) throw new Problem(e.message);
    throw e;
  }
}

/** A series' text as Python gives it, read by core: its canonical text (refused where it is no series, in core's words). */
async function textOf(s: Readonly<Record<string, unknown>>, what: string): Promise<string> {
  if (s['expr'] !== undefined) throw new Problem(`${what}: a series' formula is its #TS text now (text=...), not expr`);
  if (s['term'] !== undefined) {
    // a term of Python's builder (python/legend_lite/timeseries.py), made by this builder's own functions and written in
    // the language (B3's Python twin), then read by core as any text is
    if (s['text'] !== undefined) throw new Problem(`${what} is its text or its term, not both`);
    let text: string;
    try {
      text = writtenText(termOf(s['term']).expr, []);
    } catch (e) {
      if (e instanceof BuilderRefusal || e instanceof TsTextRefusal) throw new Problem(`${what}: ${e.message}`);
      throw e;
    }
    const read = await readTyped(text, reader);
    if ('refused' in read) throw new Problem(`${what}: ${read.refused.message}: ${JSON.stringify(text)}`);
    return read.text;
  }
  const e = s['text'];
  if (typeof e !== 'string') throw new Problem(`${what}'s text is ${JSON.stringify(e ?? null)}, not a #TS expression`);
  const read = await readTyped(e, reader);
  if ('refused' in read) throw new Problem(`${what}: ${read.refused.message}: ${JSON.stringify(e)}`);
  return read.text;
}

/** A TERM OF PYTHON'S BUILDER AS ITS TEXT (B3's twin, python/legend_lite/timeseries.py): `term` (JSON, the wire's:
 *  timeseries-builder-wire.ts) made by this builder's functions, written in the language and read by core. JSON:
 *  `{text}`, or `{problems}` in DataCube's words. */
export async function timeseriesTermText(term: string): Promise<string> {
  try {
    return JSON.stringify({ text: await textOf({ term: JSON.parse(term) as unknown }, 'the term') });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * A TIMESERIES' SPEC AS PYTHON TELLS IT: `given` (JSON) is a spec's own slots, but that each source may name its grid by
 * its tile (`grid`, its cube then the grid's) and leave out its alias, its date and its instrument column (DataCube's
 * rule takes them from the grid's columns: timeseries-spec.ts sourceOf, aliasFor), and each series may give its
 * #TS text (`volatility(px.close['SPX'], w => 63)`, read by core) and leave out its id (the next letter).
 * `page`: the page as Python holds it. JSON: `{spec}` -- read by the tile's one reader -- or `{problems}`, in DataCube's
 * words.
 */
export async function timeseriesMade(page: string, given: string): Promise<string> {
  try {
    const doc = await pageRead(JSON.parse(page));
    const told: unknown = JSON.parse(given);
    if (!isRecord(told)) throw new Problem('a timeseries is an object of its sources and its series');
    const rawSources = told['sources'];
    if (!Array.isArray(rawSources) || rawSources.length === 0) throw new Problem('a timeseries reads one grid at least (sources)');
    const sources: TsSource[] = [];
    for (const [i, raw] of rawSources.entries()) {
      if (!isRecord(raw)) throw new Problem(`source ${i + 1} is not an object`);
      const { grid, ...rest } = raw;
      if (grid === undefined) {
        sources.push(rest as unknown as TsSource);
        continue;
      }
      if (typeof grid !== 'string') throw new Problem(`source ${i + 1}'s grid is not a tile's id`);
      const g = gridOf(doc, grid);
      const alias = wordOf(rest['alias'], `source ${i + 1}'s alias`) ?? aliasFor(g.title, sources.map((s) => s.alias));
      const made = sourceOf(g.columns, g.rows, g.cube, alias);
      const date = wordOf(rest['date'], `source ${alias}'s date`) ?? made?.date;
      if (date === undefined) throw new Problem(`${grid} has no date column to read its series along: say its date`);
      const key = rest['key'] === null ? undefined : wordOf(rest['key'], `source ${alias}'s key`) ?? made?.key;
      const extra = Object.keys(rest).filter((k) => !['alias', 'date', 'key'].includes(k));
      sources.push({ alias, cube: g.cube, date, ...(key !== undefined ? { key } : {}), ...Object.fromEntries(extra.map((k) => [k, rest[k]])) } as TsSource);
    }
    // no series told: DataCube's first of its first grid (defaultTimeseries' rule)
    const first = told['series'] === undefined ? firstOf(doc, rawSources[0], sources[0]) : undefined;
    const rawSeries = told['series'] ?? first?.series ?? [];
    if (!Array.isArray(rawSeries)) throw new Problem("a timeseries' series are a list");
    const series: TsSeries[] = [];
    for (const [i, raw] of rawSeries.entries()) {
      const s = typeof raw === 'string' ? { text: raw } : raw;
      if (!isRecord(s)) throw new Problem(`series ${i + 1} is not an expression or an object`);
      const id = wordOf(s['id'], `series ${i + 1}'s id`) ?? nextSeriesId(series);
      const { term: _term, ...drawn } = s;
      series.push({ ...drawn, id, text: await textOf(s, `series ${id}`) } as unknown as TsSeries);
    }
    const spec = judged(doc, null, { ...told, sources, series, ...(told['view'] === undefined && first?.view ? { view: first.view } : {}) });
    return JSON.stringify({ spec: spec satisfies TimeseriesSpec });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * A TIMESERIES TILE CHANGED, HEADLESS: tile `tile` of `page` (JSON, as Python holds it) changed by `given` (JSON, a
 * timeseries-edit.ts TsChange as Python tells it: a series plotted is its #TS text, read by core; a grid
 * read is named by its tile, `{kind: 'read', grid: 'grid-2'}`) by the tile's one rule, editTimeseries -- the rule its
 * viewer's editors ask -- and judged as timeseriesMade judges a tile. JSON: `{spec}`, for Python's setTimeseries, or
 * `{problems}`, in DataCube's words.
 */
export async function timeseriesEdit(page: string, tile: string, given: string): Promise<string> {
  try {
    const doc = await pageRead(JSON.parse(page));
    const view = doc.views.find((v) => v.id === tile);
    if (!view || view.kind !== 'timeseries') throw new Problem(`'${tile}' is no timeseries tile of the page`);
    const told: unknown = JSON.parse(given);
    if (!isRecord(told) || typeof told['kind'] !== 'string') throw new Problem('a change to a timeseries is an object of its kind');
    let change = told as unknown as TsChange;
    if (told['kind'] === 'plot') {
      const s = told['series'];
      if (!isRecord(s)) throw new Problem('a series plotted is an object of its expression and how it is drawn');
      const id = wordOf(s['id'], "the series' id");
      if (told['replace'] !== undefined && typeof told['replace'] !== 'boolean') throw new Problem("a plot's replace is true or false");
      const { term: _term, ...drawn } = s;
      change = {
        kind: 'plot', series: { ...drawn, text: await textOf(s, `series ${id ?? nextSeriesId(view.spec.series)}`) } as never,
        ...(told['replace'] === true ? { replace: true } : {}),
      };
    } else if (told['kind'] === 'read') {
      const grid = wordOf(told['grid'], 'the grid read');
      if (grid === undefined) throw new Problem("a grid read is named by its tile's id");
      change = { kind: 'read', grid: gridOf(doc, grid) };
    } else if (told['kind'] === 'state') {
      // each source's grid's filter as the page holds its cube now (a removed grid's as it was): what the viewer keeps
      if (told['state'] !== 'dynamic' && told['state'] !== 'frozen') throw new Problem(`a timeseries' state is dynamic or frozen, not ${JSON.stringify(told['state'])}`);
      const filters = new Map(view.spec.sources.flatMap((s) => {
        const held = doc.cubes.find((c) => c.id === s.cube);
        return held ? [[s.cube, held.cube.query.filter] as const] : [];
      }));
      change = { kind: 'state', state: told['state'], filters };
    }
    const spec = judged(doc, view.spec, editTimeseries(view.spec, change));
    return JSON.stringify({ spec: spec satisfies TimeseriesSpec });
  } catch (e) {
    if (e instanceof Problem || e instanceof TsEditRefusal) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/** What a host asks of a series' query (timeseriesQuery): its page, the tile and the series, the window it is read
 *  over, and what the host serves (host-tables.ts HostTables). */
export interface SeriesAsked {
  readonly page: unknown;
  readonly tile: string;
  /** The series asked; absent, the tile's first (seriesOf), its id said in the answer. */
  readonly series?: string;
  /** The first date it is read from (an index's base, compare's), and the last; absent, every date. */
  readonly window?: TsWindow;
  /** The first date shown, where the dates kept are every one (`window` absent): an index's base, compare's, as the
   *  screen draws the tile over its range (the Phase 2 review's 3); absent, the window's first. */
  readonly anchor?: string;
  readonly host: HostTables;
  /** The copies the host holds, by table (Python's held_copies): a snapped grid is read over its copy. */
  readonly copies?: Readonly<Record<string, unknown>>;
  /** Absent, the series' points; else one of its tables over the window (the statistics panel's tabs) -- of
   *  correlations, the series' lines with `with`'s -- or one of its views (the next phases' 3): a scatter of the series
   *  across and `with`'s up, a seasonality, a histogram, a curve, the calendar. */
  readonly table?: TsTable | 'projection';
  readonly with?: string;
  /** A projection's (`table` projection): its place among the tile's annotations (TsProjection), of the series. */
  readonly projection?: number;
  /** A view's own settings told for this ask (a seasonality's years, a histogram's bins, a curve's dates); absent, the
   *  tile's view's, else its defaults -- judged by the tile's reader. */
  readonly years?: number;
  readonly bins?: number;
  readonly dates?: readonly string[];
  /** A series' points as the screen asks them (timeseries-query.ts askQuery: from the window's first date where it reads
   *  one, every date kept, the drawing's limit) -- Python's Timeseries.queries, the screen's Query window's; absent, as
   *  a frame reads them (the window's dates alone, every point). */
  readonly drawn?: boolean;
}

/**
 * WHAT A HOST ASKS OF A TILE'S SERIES, written: its points (seriesQuery, every point of the window) or one of its tables
 * over the window (the screen's own: statsQuery, drawdownsQuery, correlationsQuery), and its answer's words -- the names
 * of the columns it answers with, by role.
 */
export function askedOf(asked: Pick<SeriesAsked, 'table' | 'with' | 'projection' | 'window' | 'anchor' | 'years' | 'bins' | 'dates' | 'drawn'>, inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries): string {
  const window = asked.window ?? {};
  const withOf = (what: string): TsSeries => {
    const other = asked.with === undefined ? undefined : ts.series.find((x) => x.id === asked.with);
    if (!other) throw new Problem(`${what} of ${series.id} is with another series of the tile: ${JSON.stringify(asked.with ?? null)} is none`);
    return other;
  };
  // a view's settings told for this ask: the tile's view with them, judged by its reader as a change of the tile is
  const given: { -readonly [K in 'years' | 'bins' | 'dates']?: TsView[K] } = {};
  if (asked.years !== undefined) given.years = asked.years;
  if (asked.bins !== undefined) given.bins = asked.bins;
  if (asked.dates !== undefined) given.dates = asked.dates;
  const viewed: TimeseriesSpec = Object.keys(given).length === 0 ? ts : { ...ts, view: { ...ts.view, ...given } };
  const problem = viewed === ts ? null : tsProblem(viewed);
  if (problem !== null) throw new Problem(problem.replace(/^its timeseries' /, ''));
  // a view's series named as the screen's view names it: its name, else its expression (timeseries-expr.ts seriesName)
  const named = (s: TsSeries): string => JSON.stringify(seriesName(s));
  // each table's and view's query the screen's own (askQuery, the one function its rounds write them by)
  const asking = (table: TsTable, on: TimeseriesSpec = ts): TsQuery =>
    askQuery(inputs, on, { kind: table, series: series.id, ...(asked.with !== undefined ? { with: asked.with } : {}) }, window);
  switch (asked.table) {
    case undefined: {
      if (asked.drawn === true) {
        const q = askQuery(inputs, ts, { kind: 'series', series: series.id }, window);
        return `"query":${toJson(q.query)},${answerOf(q, series)}`;
      }
      // its first date its base where it reads one (an index, compare), and the window's dates alone kept -- by the
      // database; every point of them (the drawing's limit is the drawing's: the owner, 2026-10-25, the review's P5)
      const from = asked.anchor ?? window.from;
      const q = seriesQuery(inputs, ts, series, readsAnchor(ts, series) && from !== undefined ? { from } : {}, window, null);
      return `"query":${toJson(q.query)},${answerOf(q, series)}`;
    }
    case 'statistics': {
      const q = asking('statistics');
      const columns = Object.fromEntries([['split', TS_SPLIT], ...TS_STATS.map((st) => [st, statName(st)])]);
      // and what says why a level's figures are blank (timeseriesStatNotes: statBlanks, the screen's words)
      const why = { levelLow: TS_STAT_LEVEL_LOW, perYear: TS_STAT_PER_YEAR };
      return `"query":${toJson(q.query)},"table":"statistics","columns":${JSON.stringify(columns)},"why":${JSON.stringify(why)},`
        + `"level":${JSON.stringify(q.level)},"split":${JSON.stringify(q.split ?? null)},"name":${JSON.stringify(series.name ?? null)}`;
    }
    case 'drawdowns': {
      const q = asking('drawdowns');
      const columns = { split: TS_SPLIT, rank: TS_DD_RANK, peak: TS_DD_PEAK, trough: TS_DD_TROUGH, recovered: TS_DD_RECOVERED, depth: TS_DD_DEPTH, decline: TS_DD_DECLINE, recovery: TS_DD_RECOVERY };
      return `"query":${toJson(q.query)},"table":"drawdowns","columns":${JSON.stringify(columns)},"split":${JSON.stringify(q.split ?? null)},"name":${JSON.stringify(series.name ?? null)}`;
    }
    case 'correlations': {
      const other = withOf('a correlation');
      const q = asking('correlations');
      const columns = { a: TS_CORR_A, b: TS_CORR_B, corr: TS_CORR, n: TS_CORR_N };
      return `"query":${toJson(q.query)},"table":"correlations","columns":${JSON.stringify(columns)},"split":${JSON.stringify(series.split ?? null)},"withSplit":${JSON.stringify(other.split ?? null)},`
        + `"name":${JSON.stringify(series.name ?? null)},"withName":${JSON.stringify(other.name ?? null)}`;
    }
    // the views (the next phases' 3): each the screen's own query, its answer's columns by role
    case 'scatter': {
      const other = withOf('a scatter');
      const q = asking('scatter');
      const columns = { date: TS_DATE, x: TS_SC_X, y: TS_SC_Y, n: TS_SC_N, slope: TS_SC_SLOPE, intercept: TS_SC_INTERCEPT, corr: TS_SC_CORR, r2: TS_SC_R2 };
      return `"query":${toJson(q.query)},"table":"scatter","columns":${JSON.stringify(columns)},"name":${named(series)},"withName":${named(other)}`;
    }
    case 'seasonality': {
      const q = asking('seasonality', viewed);
      const columns = { date: TS_DATE, year: TS_SEA_YEAR, place: TS_SEA_PLACE, value: TS_VALUE, low: TS_SEA_LOW, high: TS_SEA_HIGH, mean: TS_SEA_MEAN, latest: TS_SEA_LATEST };
      return `"query":${toJson(q.query)},"table":"seasonality","columns":${JSON.stringify(columns)},"unit":${JSON.stringify(q.unit)},"name":${named(series)}`;
    }
    case 'histogram': {
      const q = asking('histogram', viewed);
      const columns = { low: TS_HIST_LOW, middle: TS_HIST_MID, high: TS_HIST_HIGH, count: TS_HIST_COUNT, normal: TS_HIST_NORMAL, p5: TS_HIST_P5, mean: TS_HIST_MEAN, sd: TS_HIST_SD, n: TS_HIST_N };
      return `"query":${toJson(q.query)},"table":"histogram","columns":${JSON.stringify(columns)},"unit":${JSON.stringify(q.unit)},"name":${named(series)}`;
    }
    case 'curve': {
      const dates = viewed.view?.dates ?? TS_CURVE_DATES;
      const q = asking('curve', viewed);
      const columns = { split: TS_SPLIT, dates: dates.map((d, k) => ({ told: d, ...curveColumns(k) })) };
      return `"query":${toJson(q.query)},"table":"curve","columns":${JSON.stringify(columns)},"split":${JSON.stringify(q.split ?? null)},"name":${named(series)}`;
    }
    case 'calendar': {
      const q = asking('calendar');
      const columns = { split: TS_SPLIT, year: TS_CAL_YEAR, month: TS_CAL_MONTH, return: TS_CAL_RETURN, yearReturn: TS_CAL_YEAR_RETURN };
      return `"query":${toJson(q.query)},"table":"calendar","columns":${JSON.stringify(columns)},"split":${JSON.stringify(q.split ?? null)},"name":${named(series)}`;
    }
    // a trend projected (Phase 5): the screen's own query (askQuery's projectionQuery), its one row's columns by role
    case 'projection': {
      const p = projectionOf(ts, asked.projection);
      if (p.series !== series.id) throw new Problem(`${projectionName(p, asked.projection!)} projects series ${p.series}, not ${series.id}`);
      const q = askQuery(inputs, ts, { kind: 'projection', series: series.id, projection: asked.projection! }, window);
      const columns = {
        from: TS_PJ_FROM, fromValue: TS_PJ_FROM_VALUE, last: TS_PJ_LAST, lastValue: TS_PJ_LAST_VALUE, end: TS_PJ_END, endValue: TS_PJ_END_VALUE,
        slope: TS_PJ_SLOPE, n: TS_PJ_N,
      };
      return `"query":${toJson(q.query)},"table":"projection","columns":${JSON.stringify(columns)},"unit":${JSON.stringify(q.unit)},`
        + `"name":${JSON.stringify(projectionName(p, asked.projection!))}`;
    }
    default:
      // a host's JSON: a word the tile has no table of, refused by name
      throw new Problem(`a timeseries has no table ${JSON.stringify(asked.table)} (statistics, drawdowns, correlations, scatter, seasonality, histogram, curve, calendar, projection)`);
  }
}

/** What a host asks of a source's bounds (timeseriesQuery): the dates it spans and where each range button starts
 *  (timeseries-query.ts boundsQuery) -- the tile's range read from them by timeseriesWindow. */
export interface BoundsAsked {
  readonly page: unknown;
  readonly tile: string;
  readonly table: 'bounds';
  readonly source: string;
  readonly host: HostTables;
  readonly copies?: Readonly<Record<string, unknown>>;
}

/** A source's bounds as a host reads them: its query over the source's grid as the tile reads it. */
export function boundsOf(ts: TimeseriesSpec, alias: string, input: TsInput): string {
  const src = ts.sources.find((x) => x.alias === alias);
  if (!src) throw new Problem(`the tile has no source ${alias}`);
  return `"query":${toJson(boundsQuery(input, src))},"table":"bounds"`;
}

/** What a host asks of marks from a grid's rows (timeseriesQuery): the marks at their place among the tile's
 *  annotations (TsMarks), of their one source's grid. */
export interface MarksAsked {
  readonly page: unknown;
  readonly tile: string;
  readonly table: 'marks';
  readonly marks: number;
  readonly host: HostTables;
  readonly copies?: Readonly<Record<string, unknown>>;
}

/** Marks as a host reads them: their query over their source's grid as the tile reads it (timeseries-query.ts
 *  marksQuery), their answer's columns named, latest first, and their name. */
export function marksAsked(ts: TimeseriesSpec, at: number | undefined, input: TsInput): string {
  const marks = marksOf(ts, at);
  const columns = { date: TS_DATE, value: marks.at !== undefined ? TS_VALUE : null, text: marks.text !== undefined ? TS_TEXT : null };
  return `"query":${toJson(marksQuery(new Map([[marks.source, input]]), ts, marks))},"table":"marks","columns":${JSON.stringify(columns)},`
    + `"name":${JSON.stringify(marksName(marks, at!))},"limit":${TS_MARK_LIMIT},"cut":${JSON.stringify(marksCut(marks, at!))}`;
}

/** A timeseries tile of a page, or a refusal naming what is not. */
function tileOf(page: PageDocument, tile: string): TimeseriesSpec {
  const view = page.views.find((v) => v.id === tile);
  if (!view || view.kind !== 'timeseries') throw new Problem(`'${tile}' is no timeseries tile of the page`);
  return view.spec;
}

/**
 * THE DATES A TILE'S TABLES READ, HEADLESS: tile `tile` of `page` over its range, by the rule its screen's window reads
 * (timeseries-query.ts rangeWindow) -- for Python's Timeseries tables asked of no dates. `bounds`: null, the first ask
 * (JSON `{window}`, or `{bounds}`: the sources whose bounds its range is read from, each asked of timeseriesQuery as
 * `table: "bounds"`); else each such source's bounds as the host read them (by alias, each column's first value), JSON
 * `{window}`. Or `{problems}`.
 */
export async function timeseriesWindow(page: string, tile: string, bounds: string): Promise<string> {
  try {
    const ts = tileOf(await pageRead(JSON.parse(page), { empty: true }), tile);
    const told = JSON.parse(bounds) as unknown;
    if (told === null) {
      // the range's dates its own, or every one: no bounds to read
      if (ts.range === undefined || !('last' in ts.range) || ts.range.last === 'max') return JSON.stringify({ window: rangeWindow(ts.range, []) });
      return JSON.stringify({ bounds: ts.sources.map((x) => x.alias) });
    }
    if (!isRecord(told)) throw new Problem('a tile\'s bounds are by its sources\' aliases');
    const tables = Object.entries(told).map(([alias, columns]): ResultTable => {
      if (!isRecord(columns) || !(TS_LATEST in columns)) throw new Problem(`source ${alias}'s bounds are its bounds query's columns, each its value`);
      return { columns: Object.entries(columns).map(([name, v]) => ({ name, type: 'String', values: [v === null ? null : String(v)] })), rowCount: 1, epoch: 0, elapsedMs: 0 };
    });
    return JSON.stringify({ window: rangeWindow(ts.range, tables) });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * WHY A LEVEL'S STATISTICS ARE BLANK, HEADLESS: each line's words by the rule the screen's statistics panel says them by
 * (timeseries-query.ts statBlanks) -- `asked` JSON `{lines: [{name, low, perYear, cagr}]}` (each the line's own answer's,
 * absent where it has none) -- for Python's Timeseries.statistics, said as warnings. JSON: `{notes}`, or `{problems}`.
 */
export function timeseriesStatNotes(asked: string): string {
  try {
    const a = JSON.parse(asked) as { readonly lines?: unknown };
    if (!Array.isArray(a.lines)) throw new Problem('a table\'s lines are a list');
    const figure = (v: unknown): number | null | undefined => (v === undefined ? undefined : v === null ? null : Number(v));
    return JSON.stringify({
      notes: a.lines.flatMap((l: unknown) => {
        if (!isRecord(l) || typeof l['name'] !== 'string') throw new Problem('a line of a table has its name');
        return statBlanks(l['name'], { low: figure(l['low']), perYear: figure(l['perYear']), cagr: figure(l['cagr']) });
      }),
    });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/** The sources a host's ask reads, by alias: the series', and of a correlation the other's too. */
export function askedSources(asked: Pick<SeriesAsked, 'table' | 'with'>, ts: TimeseriesSpec, series: TsSeries): string[] {
  const other = (asked.table === 'correlations' || asked.table === 'scatter') && asked.with !== undefined ? ts.series.find((x) => x.id === asked.with) : undefined;
  return [...new Set([...seriesSources(ts, series), ...(other ? seriesSources(ts, other) : [])])];
}

/**
 * THE SERIES A TILE'S TABLES READ, HEADLESS: tile `tile` of `page` by the one rule its screen's tabs read
 * (timeseries-query.ts tableSeries) -- for Python's Timeseries tables -- and the series each of its views draws
 * (viewSeries). JSON: `{statistics, drawdowns, correlations, views: {scatter, one, curve, calendar}}` (ids; correlations
 * each a pair, a scatter its x and y), or `{problems}`.
 */
export async function timeseriesTables(page: string, tile: string): Promise<string> {
  try {
    const ts = tileOf(await pageRead(JSON.parse(page), { empty: true }), tile);
    // and the series each view draws, by the rule the screen's views read (timeseries-query.ts viewSeries)
    return JSON.stringify({ ...tableSeries(ts), views: viewSeries(ts) });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * EVERY QUESTION A TILE ASKS THE DATABASE, HEADLESS: tile `tile` of `page`, by the one rule its screen's Query window
 * reads (timeseries-query.ts tileAsks: each source's bounds, each series' points, the view's, the statistics panel's) --
 * for Python's Timeseries.queries, each asked of timeseriesQuery as the screen asks it. JSON: `{asks}` (each its kind,
 * its series, pair or source, its words and what it serves), or `{problems}`.
 */
export async function timeseriesAsks(page: string, tile: string): Promise<string> {
  try {
    return JSON.stringify({ asks: tileAsks(tileOf(await pageRead(JSON.parse(page), { empty: true }), tile)) });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * A TILE AS THE CODE THAT BUILDS IT, HEADLESS: tile `tile` of `page` in `language` (python, typescript), by the one rule
 * its screen's Code shows it by (timeseries-code.ts tileCode) -- for Python's Timeseries.code. JSON: `{code}`, or
 * `{problems}`.
 */
export async function timeseriesCode(page: string, tile: string, language: string): Promise<string> {
  try {
    if (!(TS_CODE_LANGUAGES as readonly string[]).includes(language)) {
      throw new Problem(`a tile's code is written in ${TS_CODE_LANGUAGES.join(' or ')}, not ${JSON.stringify(language)}`);
    }
    const doc = await pageRead(JSON.parse(page), { empty: true });
    const { spec, of } = codeOfPage(doc, tile);
    return JSON.stringify({ code: tileCode(spec, of, language as TsCodeLanguage) });
  } catch (e) {
    if (e instanceof Problem || e instanceof CubeRefusal) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * A CURVE'S KEYS IN ORDER, HEADLESS: `keys` (JSON, a curve's lines' split values as its answer has them) by the rule the
 * screen's curve orders them by (timeseries-query.ts curveOrder: tenors by their span, else their text) -- for Python's
 * Timeseries.curve. JSON: `{order}` (the places of the keys), or `{problems}`.
 */
export function timeseriesCurveOrder(keys: string): string {
  const told: unknown = JSON.parse(keys);
  if (!Array.isArray(told) || !told.every((k) => typeof k === 'string')) return JSON.stringify({ problems: ['a curve\'s keys are its lines\' values, as text'] });
  return JSON.stringify({ order: curveOrder(told as string[]) });
}

/** What a series' query answers with, by role, as a host reads its answer. */
const ANSWER = `"columns":${JSON.stringify({ date: TS_DATE, value: TS_VALUE, split: TS_SPLIT, lower: TS_LOWER, upper: TS_UPPER, open: TS_OPEN, high: TS_HIGH, low: TS_LOW, rows: TS_ROWS, perYear: TS_PER_YEAR })}`;

/** A series' query as a host reads it: its unit, whether a band or candles, its split and its name, beside the query. */
export function answerOf(q: TsQuery, series: TsSeries): string {
  return `${ANSWER},"unit":${JSON.stringify(q.unit)},"band":${JSON.stringify(q.band)},"candles":${JSON.stringify(q.candles === true)},`
    + `"shade":${JSON.stringify(q.shade === true)},"split":${JSON.stringify(q.split ?? null)},"name":${JSON.stringify(series.name ?? null)},`
    + `"series":${JSON.stringify(series.id)}`;
}

/**
 * A SERIES' LINES NAMED, HEADLESS: tile `tile`'s series `series` of `page`, each line by the rule the screen names it by
 * (timeseries-expr.ts splitName: its name, else its expression; a split's each its value beside it) -- `splits` (JSON) a
 * split's values as its answer has them, or null for a series of one line. For Python's Timeseries.table, named as the
 * screen's Data tab names its columns (the Phase 2 review's 3). JSON: `{names}`, or `{problems}`.
 */
export async function timeseriesLineNames(page: string, tile: string, series: string, splits: string): Promise<string> {
  try {
    const { ts, series: s } = seriesOf(await pageRead(JSON.parse(page), { empty: true }), tile, series);
    const told: unknown = JSON.parse(splits);
    if (told !== null && !Array.isArray(told)) throw new Problem('a series\' lines are named by its split\'s values, or null for one line');
    return JSON.stringify({ names: told === null ? [splitName(s, undefined, ts.series)] : told.map((v) => splitName(s, v, ts.series)) });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * WHAT A SERIES' ROWS SAY, HEADLESS: tile `tile`'s series `series` of `page`, its answer's split, counted rows and periods
 * a year as a host read them (`columns`: each a list, or absent where its answer has none) -- said by the rule the
 * viewer's drawing says them by (timeseries-query.ts seriesNotes; the second audit's P-3: Python's frame dropped them).
 * JSON: `{notes}`, or `{problems}`.
 */
export async function timeseriesNotes(asked: string): Promise<string> {
  try {
    const a = JSON.parse(asked) as { readonly page: unknown; readonly tile: string; readonly series?: string; readonly columns: Readonly<Record<string, unknown>> };
    const { ts, series } = seriesOf(await pageRead(a.page, { empty: true }), a.tile, a.series);
    const columns = [[TS_SPLIT, a.columns['split']], [TS_ROWS, a.columns['rows']], [TS_PER_YEAR, a.columns['perYear']]] as const;
    const told = columns.flatMap(([name, values]) => {
      if (values === undefined) return [];
      if (!Array.isArray(values)) throw new Problem(`a series' ${name} is a list of its values`);
      return [{ name, type: 'Float', values: values as Scalar[] }];
    });
    const rowCount = told[0]?.values.length ?? 0;
    if (told.some((c) => c.values.length !== rowCount)) throw new Problem("a series' columns are each as long as its rows");
    const table: ResultTable = { columns: told, rowCount, epoch: 0, elapsedMs: 0 };
    return JSON.stringify({ notes: seriesNotes(ts, series, table, seriesSources(ts, series)[0]) });
  } catch (e) {
    if (e instanceof Problem) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}

/**
 * WHAT A QUERY'S MODEL TAKES (pure-library.ts): `askedOf`'s answer (`"query":...`) calling DataCube's Pure library,
 * `,"library":<its text>` -- which its host plans it over after the model (Python's Timeseries._query) -- else nothing.
 */
export function libraryOf(said: string): string {
  const { query } = fromJson(`{${said}}`) as unknown as { readonly query: ValueSpecification };
  return callsLibrary(query) ? `,"library":${JSON.stringify(DATACUBE_LIBRARY)}` : '';
}

/** A timeseries tile's series of a page, by their ids, or a refusal naming what is not there. */
export function seriesOf(page: PageDocument, tile: string, id: string | undefined): { readonly ts: TimeseriesSpec; readonly series: TsSeries } {
  const view = page.views.find((v) => v.id === tile);
  if (!view || view.kind !== 'timeseries') throw new Problem(`'${tile}' is no timeseries tile of the page`);
  const ts = view.spec;
  // none asked: the tile's first -- DataCube's rule, which a host names in its answer (`series`), never a host's own
  // (the review's §9 A: Python's frame() chose it by a rule of its own)
  if (id === undefined) {
    const first = ts.series[0];
    if (first === undefined) throw new Problem(`${tile} plots no series`);
    return { ts, series: first };
  }
  const series = ts.series.find((x) => x.id === id);
  if (!series) throw new Problem(`${tile} has no series ${id} (it has ${ts.series.map((x) => x.id).join(', ')})`);
  return { ts, series };
}

/**
 * A SERIES' QUERY, HEADLESS (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5): a timeseries tile's series as its viewer
 * asks for it -- timeseries-query.ts seriesQuery over its sources' grids as the page has them (each its source, its
 * calculated columns and its filter), the sources opened over one model of the host's tables they read (join-open.ts
 * openSources, as the tab reads several grids together; one grid read the same way) -- for a host to plan and run where
 * the rows are (Python's Timeseries.frame: the database's every figure, as the screen's). JSON: `{model, runtime,
 * query, columns, unit, band, candles, split, name}` -- the query a lambda as the protocol writes it, `columns` the names
 * of its answer's (timeseries-query.ts: `date`, `value`, a split's `split`, a band's `lower` and `upper`, a candle's
 * `open`, `high` and `low`, its close the value), its rows latest first; `library` beside them where the query calls
 * DataCube's Pure library (libraryOf), planned over after the model -- or `{problems}`, in DataCube's words.
 */
export async function timeseriesQuery(spec: string): Promise<string> {
  try {
    const asked = fromJson(spec) as unknown as SeriesAsked | BoundsAsked | MarksAsked;
    const page = await pageRead(asked.page, { empty: true });
    // marks from a grid's rows: of their one grid, read as that grid is, as a source's bounds are
    if (asked.table === 'marks') {
      const marks = marksOf(tileOf(page, asked.tile), asked.marks);
      const src = tileOf(page, asked.tile).sources.find((x) => x.alias === marks.source)!;
      if (!page.cubes.some((c) => c.id === src.cube)) throw new Problem(`source ${src.alias}'s grid is not on the page (it was removed)`);
      return JSON.stringify({ grid: src.cube });
    }
    // a source's bounds: of its one grid, read as that grid is (headless grid.ts gridTimeseries)
    if (asked.table === 'bounds') {
      const src = tileOf(page, asked.tile).sources.find((x) => x.alias === asked.source);
      if (!src) throw new Problem(`${asked.tile} has no source ${JSON.stringify(asked.source)}`);
      if (!page.cubes.some((c) => c.id === src.cube)) throw new Problem(`source ${src.alias}'s grid is not on the page (it was removed)`);
      return JSON.stringify({ grid: src.cube });
    }
    const { ts, series } = seriesOf(page, asked.tile, asked.series);
    let aliases: string[];
    try {
      aliases = askedSources(asked, ts, series);
    } catch (e) {
      throw new Problem(`series ${series.id}: ${e instanceof Error ? e.message : String(e)}`);
    }
    const cubes = aliases.map((alias) => {
      const src = ts.sources.find((x) => x.alias === alias)!;
      if (!page.cubes.some((c) => c.id === src.cube)) throw new Problem(`source ${alias}'s grid is not on the page (it was removed)`);
      return src.cube;
    });
    // a series of one grid: read as that grid is, over its base (its copy, snapped; its mapping, a class's; the page's
    // filters on it) -- the grid's own path, headless grid.ts gridTimeseries, as the tab reads it through the grid's
    // runner (the review's P1: read live over the grid's leaves, beside every other read of the grid from its copy)
    if (cubes.length === 1) return JSON.stringify({ grid: cubes[0] });
    // several: one model of the host's tables they read, a snapped grid's its copy (join.ts sourcesOnHost)
    const { sources, tables } = await sourcesOnHost(page, cubes, asked.host, asked.copies ?? {});
    const saved = aliases.map((alias, i) => {
      const cube = page.cubes.find((c) => c.id === cubes[i]);
      if (cube === undefined) throw new Problem(`source ${alias} reads ${cubes[i]}, which is no cube of the page`);
      return { alias, query: cube.cube.query };
    });
    const opened = await sourcesOpenedOn(sources, tables);
    // each grid as the page has it -- its calculated columns, its filter -- over its relation of the model
    const inputs: TsInputs = new Map(eachRelation(opened, saved).map(([{ alias, query }, relation]) => {
      const snapshot: CubeSnapshot = { ...query, source: { query: relation }, epoch: 0 };
      return [alias, { snapshot, filter: query.filter }] as const;
    }));
    const said = askedOf(asked, inputs, ts, series);
    return `{"model":${JSON.stringify(opened.model)},"runtime":${JSON.stringify(opened.runtime)},${said}${libraryOf(said)}}`;
  } catch (e) {
    if (e instanceof Problem || e instanceof CubeRefusal) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}
