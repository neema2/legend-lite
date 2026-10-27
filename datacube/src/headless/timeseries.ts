// A TIMESERIES TILE, HEADLESS (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §7): the spec of what Python tells it,
// made by DataCube's own rules -- its expressions read by DataCube's parser, a source's columns by DataCube's defaults,
// the whole read by the tile's one reader -- so Python's page.timeseries keeps no rule of its own (the charts' rule:
// headless gridChart). The page command that adds or sets it is Python's generated builder's, applied by pageApply.

import { fromJson, toJson } from '../../../pure-protocol/src/index.ts';
import { readPage, type PageDocument } from '../page-document.ts';
import { gridTitle } from '../page-ids.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot } from '../snapshot.ts';
import {
  readsAnchor, seriesNotes, seriesQuery, seriesSources, TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_PER_YEAR, TS_ROWS, TS_SPLIT,
  TS_UPPER, TS_VALUE, type TsInputs, type TsQuery, type TsWindow,
} from '../timeseries-query.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { HostTables } from './host-tables.ts';
import { eachRelation } from '../join-open.ts';
import { sourcesOnHost, sourcesOpenedOn } from './join.ts';
import { type TimeseriesSpec, type TsExpr, type TsSeries, type TsSource } from '../timeseries-spec.ts';
import {
  aliasFor, nextSeriesId, parseExpr, sourceOf, TsSyntaxError, type TsColumn
} from '../timeseries-expr.ts';
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

/** An expression as Python gives it: its text, read by DataCube's parser (refused where it is, in DataCube's words), or a tree. */
function exprOf(e: unknown, what: string): TsExpr {
  if (typeof e !== 'string') return e as TsExpr;
  try {
    return parseExpr(e);
  } catch (err) {
    if (err instanceof TsSyntaxError) throw new Problem(`${what}: ${err.message} (at ${err.at + 1}: ${JSON.stringify(e)})`);
    throw err;
  }
}

/**
 * A TIMESERIES' SPEC AS PYTHON TELLS IT: `given` (JSON) is a spec's own slots, but that each source may name its grid by
 * its tile (`grid`, its cube then the grid's) and leave out its alias, its date and its instrument column (DataCube's
 * rule takes them from the grid's columns: timeseries-spec.ts sourceOf, aliasFor), and each series may give its
 * expression as text (`volatility(px:close[SPX], 63)`, read by DataCube's parser) and leave out its id (the next letter).
 * `page`: the page as Python holds it. JSON: `{spec}` -- read by the tile's one reader -- or `{problems}`, in DataCube's
 * words.
 */
export function timeseriesMade(page: string, given: string): string {
  try {
    const doc = readPage(JSON.parse(page));
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
      const s = typeof raw === 'string' ? { expr: raw } : raw;
      if (!isRecord(s)) throw new Problem(`series ${i + 1} is not an expression or an object`);
      const id = wordOf(s['id'], `series ${i + 1}'s id`) ?? nextSeriesId(series);
      series.push({ ...s, id, expr: exprOf(s['expr'], `series ${id}`) } as unknown as TsSeries);
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
 * timeseries-edit.ts TsChange as Python tells it: a series' expression may be its text, read by DataCube's parser; a grid
 * read is named by its tile, `{kind: 'read', grid: 'grid-2'}`) by the tile's one rule, editTimeseries -- the rule its
 * viewer's editors ask -- and judged as timeseriesMade judges a tile. JSON: `{spec}`, for Python's setTimeseries, or
 * `{problems}`, in DataCube's words.
 */
export function timeseriesEdit(page: string, tile: string, given: string): string {
  try {
    const doc = readPage(JSON.parse(page));
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
      change = {
        kind: 'plot', series: { ...s, expr: exprOf(s['expr'], `series ${id ?? nextSeriesId(view.spec.series)}`) } as never,
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
  readonly series: string;
  /** The first date it is read from (an index's base, compare's), and the last; absent, every date. */
  readonly window?: TsWindow;
  readonly host: HostTables;
  /** The copies the host holds, by table (Python's held_copies): a snapped grid is read over its copy. */
  readonly copies?: Readonly<Record<string, unknown>>;
}

/** What a series' query answers with, by role, as a host reads its answer. */
const ANSWER = `"columns":${JSON.stringify({ date: TS_DATE, value: TS_VALUE, split: TS_SPLIT, lower: TS_LOWER, upper: TS_UPPER, open: TS_OPEN, high: TS_HIGH, low: TS_LOW, rows: TS_ROWS, perYear: TS_PER_YEAR })}`;

/** A series' query as a host reads it: its unit, whether a band or candles, its split and its name, beside the query. */
export function answerOf(q: TsQuery, series: TsSeries): string {
  return `${ANSWER},"unit":${JSON.stringify(q.unit)},"band":${JSON.stringify(q.band)},"candles":${JSON.stringify(q.candles === true)},`
    + `"split":${JSON.stringify(q.split ?? null)},"name":${JSON.stringify(series.name ?? null)}`;
}

/**
 * WHAT A SERIES' ROWS SAY, HEADLESS: tile `tile`'s series `series` of `page`, its answer's split, counted rows and periods
 * a year as a host read them (`columns`: each a list, or absent where its answer has none) -- said by the rule the
 * viewer's drawing says them by (timeseries-query.ts seriesNotes; the second audit's P-3: Python's frame dropped them).
 * JSON: `{notes}`, or `{problems}`.
 */
export function timeseriesNotes(asked: string): string {
  try {
    const a = JSON.parse(asked) as { readonly page: unknown; readonly tile: string; readonly series: string; readonly columns: Readonly<Record<string, unknown>> };
    const { ts, series } = seriesOf(readPage(a.page, { empty: true }), a.tile, a.series);
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

/** A timeseries tile's series of a page, by their ids, or a refusal naming what is not there. */
export function seriesOf(page: PageDocument, tile: string, id: string): { readonly ts: TimeseriesSpec; readonly series: TsSeries } {
  const view = page.views.find((v) => v.id === tile);
  if (!view || view.kind !== 'timeseries') throw new Problem(`'${tile}' is no timeseries tile of the page`);
  const ts = view.spec;
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
 * `open`, `high` and `low`, its close the value), its rows latest first -- or `{problems}`, in DataCube's words.
 */
export async function timeseriesQuery(spec: string): Promise<string> {
  try {
    const asked = fromJson(spec) as unknown as SeriesAsked;
    const page = readPage(asked.page, { empty: true });
    const { ts, series } = seriesOf(page, asked.tile, asked.series);
    let aliases: string[];
    try {
      aliases = seriesSources(ts, series);
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
    const window = asked.window ?? {};
    // its first date its base where it reads one (an index, compare), and the window's dates alone kept -- by the
    // database; every point of them (the drawing's limit is the drawing's: the owner, 2026-10-25, the review's P5)
    const q = seriesQuery(inputs, ts, series, readsAnchor(ts, series) && window.from !== undefined ? { from: window.from } : {}, window, null);
    return `{"model":${JSON.stringify(opened.model)},"runtime":${JSON.stringify(opened.runtime)},"query":${toJson(q.query)},${answerOf(q, series)}}`;
  } catch (e) {
    if (e instanceof Problem || e instanceof CubeRefusal) return JSON.stringify({ problems: [e.message] });
    throw e;
  }
}
