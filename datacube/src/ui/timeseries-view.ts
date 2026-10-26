// THE TIMESERIES VIEWER (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §6): a timeseries tile's toolbar, legend, chart,
// statistics and editors, over its spec (timeseries-spec.ts). What it draws is what the database answered
// (timeseries-query.ts: one query per series, D2); what it shows of it -- the dates in view, a line hidden, a colour --
// is the viewer's. A change of the spec is the page's (the host's setSpec: a setTimeseries command); the dates a zoom or
// a pan shows are the person's own look, never saved.
//
// WHEN IT ASKS THE DATABASE (D4: no cache of answers): it keeps the rows of its last answer. A change of the drawing alone
// -- a line's style, its pane, an annotation, a line hidden, the legend -- is drawn from them; a change of what a series
// reads (its expression, its sources, the frequency, compare) asks again; and so does a grid's new view (the host's
// refresh: every DataCube chart redraws when its grid's view lands). A zoom asks again for the series that read the first
// date shown (an index, a change from the start, compare) and for the statistics -- nothing else.
//
// Fetched with the first timeseries tile (a chunk of its own, page/cube-page.ts); ECharts with it (timeseries-render.ts).

import type { Lambda, ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import { isNumeric, isTemporal, isTimeOfDay } from '../../../engine-client/src/types.ts';
import { themeOf } from '../chart-option.ts';
import type { ChartPicture } from '../chart-render.ts';
import { rowColumns, type CubeSnapshot, type FilterNode } from '../snapshot.ts';
import {
  MAX_PANES, TS_RANGES, viewSetting, type TimeseriesSpec, type TsAnnotation, type TsExpr,
  type TsRangeWord, type TsSeries, type TsSource, type TsState, type TsStyleKind,
} from '../timeseries-spec.ts';
import {
  candleOf, columnRef, exprText, firstValue, functionNamed, inlined, nextSeriesId, parseExpr, seriesName,
  sourcesOf, timeseriesProblems, TsSyntaxError, type TsColumn, TS_FUNCTIONS, type TsFamily,
} from '../timeseries-expr.ts';
import {
  editTimeseries, firstTimeseries as firstOf, judgedEdit, TsEditRefusal, type TsChange, type TsGridColumns, type TsSeriesTold,
} from '../timeseries-edit.ts';
import {
  boundsQuery, keyValuesQuery, rangeStartName, readsAnchor, seriesQuery, seriesSources, statName, statsQuery, TS_DATE, TS_LATEST,
  TS_SPLIT, TS_STAT_LEVEL_LOW, TS_STAT_PER_YEAR, TS_VALUE, type TsInput, type TsInputs, type TsQuery, type TsStat, type TsWindow,
} from '../timeseries-query.ts';
import {
  describeTimeseries, dateLabel, timeseriesCsv, timeseriesOption, toNumber, tsLines, valueText, type TsFormat, type TsLine,
  type TsResult,
} from '../timeseries-option.ts';
import type { MountedTimeseries } from '../timeseries-render.ts';
import { lazily } from '../lazily.ts';
import { chromeIcon, closeMark } from './tile-icons.ts';
import { isTextEntry } from '../focus.ts';
import { besideWindow, makeWindow, raiseWindow, screenOf } from './window.ts';

/** A grid a source reads, as it is now: its query, its filter as the tile reads it, its runner and its formats. */
export interface TsGrid {
  /** Its cube's id (its tile's). */
  readonly cube: string;
  readonly title: string;
  /** Its grid removed from the page, its cube kept for the tiles that read it (page-document.ts readsCube): read as it was. */
  readonly kept?: true;
  readonly snapshot: CubeSnapshot;
  readonly filter: FilterNode | undefined;
  run(query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal): Promise<ResultTable>;
  /** A value as the grid writes it in this column. */
  format(value: Scalar, column: string, type: string | undefined): string;
}

/** Grids opened over one model of their tables, for a series that combines them: each cube's source over the model. */
export interface TsTogether {
  readonly relations: ReadonlyMap<string, ValueSpecification>;
  run(query: Lambda): Promise<ResultTable>;
  dispose(): void;
}

/** The grids opened together in one round of asking (a fetch, the window moved: its series and their statistics), by
 *  the cubes they are: each set opened once and put away when the round is done (the review's P9: opened again for
 *  every query, the joint model written again each time). */
type Opened = Map<string, Promise<TsTogether>>;

/** A round's grids opened together, put away. One that could not be opened has nothing to put away: its refusal was
 *  said by every query that asked for it (#run), so it is not said again here. */
function putAway(opened: Opened): void {
  for (const p of opened.values()) void p.then((t) => t.dispose(), () => undefined);
}

/** A series' answer as its query asked it, its dates and its values among its columns: else the series' failure, said on
 *  its line -- never drawn as no points (timeseries-option.ts tsLines reads them). */
function answered(r: TsResult): TsResult {
  for (const c of [TS_DATE, TS_VALUE]) {
    if (!r.table.columns.some((x) => x.name === c)) throw new Error(`the database answered without the series' ${c === TS_DATE ? 'dates' : 'values'}`);
  }
  return r;
}

/** How long a grid's view waits before the tile asks again: a burst of views (a grid expanding row by row) asks once, as
 *  a chart's (ui/chart-panel.ts debounceMs). */
const REFRESH_DEBOUNCE_MS = 150;

/** What the viewer is given by the page it is on. */
export interface TimeseriesHost {
  /** A grid of the page by its cube, as it is now; undefined when no grid shows it (removed). */
  grid(cube: string): TsGrid | undefined;
  /** The page's grids, in its order: what a source may read. */
  grids(): readonly TsGrid[];
  /** A source's filter as the page holds its cube -- its grid's now, or as the page saved it when its grid could not be
   *  opened -- what freezing keeps; undefined when the page holds no cube of it. */
  heldFilter(cube: string): { readonly filter: FilterNode | undefined } | undefined;
  /** The grids of `cubes` opened together over one model of their tables (D2, D5), or a refusal in words. */
  together(cubes: readonly string[]): Promise<TsTogether>;
  /** The tile's spec as the person changed it: the page's (setTimeseries). */
  setSpec(spec: TimeseriesSpec): void;
  /** Where its editors' windows go, among the page's windows (a chart's options' host: CubePageOptions.windowHost). */
  readonly windowHost: HTMLElement;
  /** Its name, as its tile says it: its editors' windows are titled by it. */
  title(): string;
  /** Its renderer (a test's stand-in); else timeseries-render.ts. */
  readonly mount?: (el: HTMLElement, on: MountOn) => MountedTimeseries;
}

/** A source's bound as the database answered it (timeseries-query.ts boundsQuery): its one row's value, null for none. */
function boundsValue(t: ResultTable, name: string): Scalar | null {
  const c = t.columns.find((x) => x.name === name);
  if (!c) throw new Error(`the tile's bounds answered with no ${name}`);
  return c.values[0] ?? null;
}

/** A grid of the page as the tile's edits read it (timeseries-edit.ts TsGridColumns): its columns as its query is now. */
function gridColumns(grid: TsGrid): TsGridColumns {
  return {
    cube: grid.cube, title: grid.title, columns: rowColumns(grid.snapshot).map((c) => ({ name: c.name, type: c.type })), rows: grid.snapshot.rows,
    filter: grid.filter,
  };
}

/** A FIRST TIMESERIES OF A GRID (Create ▸ Timeseries): timeseries-edit.ts firstTimeseries, the rule Python's
 *  page.timeseries asks too, over the grid as it is now. */
export function firstTimeseries(grid: TsGrid): TimeseriesSpec | string {
  return firstOf(gridColumns(grid));
}

/** What the renderer tells the viewer. */
export interface MountOn {
  readonly onZoom: (from: string | undefined, to: string | undefined) => void;
  readonly onDate?: (date: string) => void;
  readonly onResize?: () => void;
}

/** What a spec's queries depend on: when it changes, the database is asked again; else the drawing alone changes. */
function queryKey(ts: TimeseriesSpec): string {
  return JSON.stringify({
    sources: ts.sources, grain: ts.grain ?? null, compare: ts.view?.compare ?? null,
    series: ts.series.map((s) => ({ id: s.id, expr: s.expr, split: s.split ?? null })),
  });
}

/** An element with its class, text and attributes. */
function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls?: string, text?: string,
  attrs: Readonly<Record<string, string>> = {}): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
}

function button(doc: Document, label: string, title: string, onClick: () => void, cls = 'dc-button dc-ts-btn'): HTMLButtonElement {
  const b = el(doc, 'button', cls, label, { type: 'button', title });
  b.addEventListener('click', onClick);
  return b;
}

/** A select's line saying why there is nothing to pick there: not an option. */
function unpicked(doc: Document, why: string): HTMLOptionElement {
  const o = doc.createElement('option');
  o.textContent = why;
  o.disabled = true;
  return o;
}

/** An option of a select, in `doc` (the window's own: a page's windows may float in another document). */
function option(doc: Document, label: string, value: string, selected = false): HTMLOptionElement {
  const o = doc.createElement('option');
  o.textContent = label;
  o.value = value;
  o.selected = selected;
  return o;
}

function select(doc: Document, options: readonly (readonly [string, string])[], value: string, onChange: (v: string) => void,
  title = ''): HTMLSelectElement {
  const s = el(doc, 'select', 'dc-select dc-ts-select', undefined, title ? { title, 'aria-label': title } : {});
  for (const [v, label] of options) s.append(option(doc, label, v, v === value));
  s.value = value;
  s.addEventListener('change', () => onChange(s.value));
  return s;
}

function labelled(doc: Document, label: string, control: HTMLElement): HTMLLabelElement {
  const l = el(doc, 'label', 'dc-ts-field');
  l.append(el(doc, 'span', 'dc-ts-field-label', label), control);
  return l;
}

function download(doc: Document, name: string, href: string): void {
  const a = doc.createElement('a');
  a.href = href;
  a.download = name;
  a.click();
}

const GRAINS: readonly (readonly [string, string])[] = [
  ['', 'As the rows are'], ['day', 'Daily'], ['week', 'Weekly'], ['month', 'Monthly'], ['quarter', 'Quarterly'], ['year', 'Yearly'],
];
const COMPARES: readonly (readonly [string, string])[] = [['', 'Levels'], ['rebase', 'Rebase to 100'], ['percent', '% change']];
const STYLES: readonly (readonly [TsStyleKind, string])[] = [['line', 'Line'], ['area', 'Area'], ['bars', 'Bars'], ['step', 'Steps'], ['dots', 'Dots']];
const FAMILIES: readonly TsFamily[] = ['Returns', 'Level', 'Rolling statistics', 'Risk', 'Relationships', 'Technicals', 'Math'];

/** The statistics table's columns: a statistic and how it is written (by the line's unit, or as a share, a date, a count). */
/** Its editors' window: as a chart's options' is sized (chart-panel.ts CHART_WINDOW), narrower -- one column of fields. */
const TS_WINDOW = { width: 440, height: 640, minWidth: 300, minHeight: 240 } as const;

type StatColumn = { readonly stat: TsStat; readonly label: string; readonly as: 'value' | 'ratio' | 'percent' | 'number' | 'date' | 'count' };
const STAT_COLUMNS: readonly StatColumn[] = [
  { stat: 'first', label: 'First', as: 'value' },
  { stat: 'last', label: 'Last', as: 'value' },
  { stat: 'change', label: 'Change', as: 'value' },
  { stat: 'pct', label: '% chg', as: 'ratio' },
  { stat: 'low', label: 'Low', as: 'value' },
  { stat: 'lowAt', label: 'on', as: 'date' },
  { stat: 'high', label: 'High', as: 'value' },
  { stat: 'highAt', label: 'on', as: 'date' },
  { stat: 'mean', label: 'Mean', as: 'value' },
  { stat: 'std', label: 'Std dev', as: 'value' },
  { stat: 'vol', label: 'Vol (ann.)', as: 'percent' },
  { stat: 'mdd', label: 'Max DD', as: 'ratio' },
  { stat: 'cagr', label: 'CAGR', as: 'ratio' },
  { stat: 'sharpe', label: 'Sharpe', as: 'number' },
  { stat: 'count', label: 'Obs', as: 'count' },
];

/** The statistics panel's Risk tab: a level's returns' risk (the expert statistics, 2026-10-26), each the database's. */
const RISK_COLUMNS: readonly StatColumn[] = [
  { stat: 'vol', label: 'Vol (ann.)', as: 'percent' },
  { stat: 'sharpe', label: 'Sharpe', as: 'number' },
  { stat: 'sortino', label: 'Sortino', as: 'number' },
  { stat: 'calmar', label: 'Calmar', as: 'number' },
  { stat: 'mdd', label: 'Max DD', as: 'ratio' },
  { stat: 'skew', label: 'Skew', as: 'number' },
  { stat: 'kurtosis', label: 'Kurtosis', as: 'number' },
  { stat: 'hit', label: 'Up periods', as: 'ratio' },
  { stat: 'best', label: 'Best', as: 'ratio' },
  { stat: 'worst', label: 'Worst', as: 'ratio' },
  { stat: 'var', label: 'VaR 95%', as: 'ratio' },
  { stat: 'es', label: 'ES 95%', as: 'ratio' },
];

/** The statistics panel's tabs, in their order. */
const STAT_TABS: readonly (readonly [StatsTab, string, string])[] = [
  ['summary', 'Summary', 'Each line\'s first, last, change, extremes, mean and deviation over the dates shown'],
  ['risk', 'Risk', 'Each level\'s returns\' risk over the dates shown: volatility, Sharpe, Sortino, Calmar, drawdown, skew, kurtosis, value at risk'],
];
type StatsTab = 'summary' | 'risk';

export class TimeseriesView {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #host: TimeseriesHost;
  #spec: TimeseriesSpec;
  #bar!: HTMLElement;
  #ranges!: HTMLElement;
  #legend!: HTMLElement;
  #banner!: HTMLElement;
  #canvas!: HTMLElement;
  #stats!: HTMLElement;
  /** The statistics panel's tab shown: a viewing choice, as the zoom is (not the page's: nothing of it is saved). */
  #statsTab: StatsTab = 'summary';
  /** Its editor's content, in its window while one is open. */
  #pop: HTMLElement;
  /** Its editor's window, while one is open; and what lets its listeners go when it closes. */
  #window_: HTMLElement | null = null;
  #closing: AbortController | null = null;
  /** Whether the window open is its Settings (painted again after a refused change). */
  #settings = false;
  #chart: MountedTimeseries | null = null;
  #mounting: Promise<MountedTimeseries | null> | null = null;
  /** Whether ECharts can draw candles (chart-render-kinds.ts registered before the chart was made). */
  #candles = false;
  /** The rows of the last answers, by series, and what each refused. */
  #results = new Map<string, TsResult>();
  #failed = new Map<string, string>();
  #statRows = new Map<string, ResultTable>();
  /** What the database refused of each series' statistics, and of each source's dates (its range buttons' starts): said
   *  in the banner and on the line's row, never shown as figures missing (the review's P4: invariant 4). */
  #statsFailed = new Map<string, string>();
  /** Whether the statistics were asked for the answers drawn: none while nothing shows them (#statistics). */
  #statsAsked = false;
  #bounds = new Map<string, ResultTable>();
  #boundsFailed = new Map<string, string>();
  #lines: TsLine[] = [];
  #notes: string[] = [];
  #problems: string[] = [];
  /** The person's own look: the dates the zoom shows, until a range is chosen again. */
  #zoom: TsWindow | null = null;
  #asked = '';
  /** A grid's view landed: the refetch it waits for (`refresh`). */
  #refreshTimer: ReturnType<typeof setTimeout> | undefined;
  #range = '';
  #gen = 0;
  #zoomTimer: ReturnType<typeof setTimeout> | undefined;
  #disposed = false;

  constructor(root: HTMLElement, host: TimeseriesHost, spec: TimeseriesSpec) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#pop = this.#doc.createElement('div');
    this.#host = host;
    this.#spec = spec;
    this.#range = JSON.stringify(spec.range ?? null);
    this.#build();
    void this.#fetch();
  }

  /** The spec it draws. */
  get spec(): TimeseriesSpec {
    return this.#spec;
  }

  /**
   * THE TILE PUT IN A STATE (its header's state button: page/cube-page.ts): FROZEN, each source keeping its grid's filter
   * as it is now (a removed grid's as it was); DYNAMIC, following them again -- by the tile's one rule, as Python's
   * Timeseries.state is, told as the page's.
   */
  setState(state: TsState): void {
    const filters = new Map(this.#spec.sources.flatMap((s) => {
      const held = this.#host.heldFilter(s.cube);
      return held ? [[s.cube, held.filter] as const] : [];
    }));
    this.#edit({ kind: 'state', state, filters });
  }

  /**
   * The spec as the page has it now (its own change come back, another editor's, an undo): drawn from the rows it has
   * when only the drawing changed, asked again when what a series reads changed (D4). The range changed: the zoom goes.
   */
  update(spec: TimeseriesSpec): void {
    if (JSON.stringify(spec) === JSON.stringify(this.#spec)) return;
    this.#apply(spec);
  }

  /**
   * A grid it reads landed a new view: asked again, as every chart of a grid is (after `delay`, so a burst of views asks
   * once). Never kept because what it reads of the grid looks the same: its rows may have moved under the same query (a
   * frame served again, a file read again), and only the database knows (the owner, 2026-10-25: no results cache; the
   * review's P2, docs/TIMESERIES_REVIEW_2026_10_25.md).
   */
  refresh(delay = REFRESH_DEBOUNCE_MS): void {
    clearTimeout(this.#refreshTimer);
    this.#refreshTimer = setTimeout(() => {
      if (this.#disposed) return;
      this.#asked = '';
      void this.#fetch();
    }, delay);
  }

  /** The chart as drawn now, as a picture (an export of the page); null before it has drawn. */
  picture(): ChartPicture | null {
    return this.#chart?.picture() ?? null;
  }

  dispose(): void {
    this.#disposed = true;
    this.#close();
    clearTimeout(this.#zoomTimer);
    clearTimeout(this.#refreshTimer);
    this.#chart?.dispose();
    this.#chart = null;
  }

  // -- the spec changed ----------------------------------------------------------------------------------------------

  /** The person changed the spec: kept by the page, and drawn (or asked) at once. */
  /** A change kept, judged as Python's is (timeseries-edit.ts judgedEdit): refused for a problem it makes, in its words --
   *  whether it was kept, so an editor whose change was refused stays open, what was typed in it kept (the third
   *  review's 1). */
  #set(spec: TimeseriesSpec): boolean {
    let read: TimeseriesSpec;
    try {
      read = judgedEdit(this.#spec, spec, (s) => this.#columns(s));
    } catch (e) {
      if (!(e instanceof TsEditRefusal)) throw e;
      this.#say([e.message], true);
      return false;
    }
    this.#apply(read);
    this.#host.setSpec(read);
    return true;
  }

  #apply(spec: TimeseriesSpec): void {
    const range = JSON.stringify(spec.range ?? null);
    const moved = range !== this.#range;
    if (moved) {
      this.#range = range;
      this.#zoom = null;
    }
    this.#spec = spec;
    this.#paintBar();
    if (queryKey(spec) !== this.#asked) void this.#fetch();
    else {
      // the drawing alone changed (a line's style, its pane, hidden; an annotation; the legend): drawn from the rows it
      // has (D4) -- and the window moved, what reads its first date asked again, and the statistics
      void this.#draw();
      if (moved) void this.#windowed();
      // the statistics shown again, asked if nothing showed them when the lines were
      else if (!this.#statsAsked && (viewSetting(spec, 'stats') || viewSetting(spec, 'legend'))) void this.#statistics(this.#gen);
      else this.#paintStats();
    }
  }

  // -- the database's answers ----------------------------------------------------------------------------------------

  /** The columns of each source's grid, by alias (undefined: its grid is not on the page). */
  #columns(spec: TimeseriesSpec): Map<string, TsColumn[] | undefined> {
    return new Map(spec.sources.map((s) => {
      const g = this.#host.grid(s.cube);
      return [s.alias, g ? rowColumns(g.snapshot).map((c) => ({ name: c.name, type: c.type })) : undefined] as const;
    }));
  }

  /** A series' own grid's input (`alias`), its rows as the grid has them. */
  #input(alias: string): { readonly input: TsInput; readonly grid: TsGrid } | undefined {
    const src = this.#spec.sources.find((s) => s.alias === alias);
    const grid = src ? this.#host.grid(src.cube) : undefined;
    return grid ? { input: { snapshot: grid.snapshot, filter: grid.filter }, grid } : undefined;
  }

  /**
   * A series' query run where it runs (D2, D5): of one grid, by that grid's runner; of several, over one model of their
   * tables (the host's together).
   */
  async #run(series: TsSeries, make: (inputs: TsInputs) => TsQuery, signal: AbortSignal, opened: Opened): Promise<TsResult> {
    const spec = this.#spec;
    const aliases = seriesSources(spec, series);
    if (aliases.length === 0) throw new Error(`series ${series.id} reads no column of a grid`);
    if (aliases.length === 1) {
      const alias = aliases[0]!;
      const own = this.#input(alias);
      if (!own) throw new Error(`source ${alias}'s grid is not on the page`);
      const query = make(new Map([[alias, own.input]]));
      return { query, table: await own.grid.run(query.query, own.grid.snapshot, signal) };
    }
    const sources = aliases.map((a) => spec.sources.find((s) => s.alias === a)!);
    const grids = sources.map((s) => this.#host.grid(s.cube));
    const missing = sources.find((_, i) => grids[i] === undefined);
    if (missing) throw new Error(`source ${missing.alias}'s grid is not on the page`);
    const together = await this.#together(opened, sources.map((s) => s.cube));
    const inputs: TsInputs = new Map(sources.map((s, i) => {
      const g = grids[i]!;
      const relation = together.relations.get(s.cube);
      if (!relation) throw new Error(`${s.alias} was not opened with the others`);
      return [s.alias, { snapshot: { ...g.snapshot, source: { query: relation } }, filter: g.filter }] as const;
    }));
    const query = make(inputs);
    return { query, table: await together.run(query.query) };
  }

  /** The grids of `cubes` opened together, once a round: by the set they are, whatever order a series reads them in. */
  #together(opened: Opened, cubes: readonly string[]): Promise<TsTogether> {
    const key = JSON.stringify([...cubes].sort());
    let together = opened.get(key);
    if (together === undefined) {
      together = this.#host.together(cubes);
      opened.set(key, together);
    }
    return together;
  }

  /** The window shown: the person's zoom, else the range's -- a period back from the latest date any source has. */
  #window(): TsWindow {
    if (this.#zoom) return this.#zoom;
    const r = this.#spec.range;
    if (!r) return {};
    if ('last' in r) {
      if (r.last === 'max') return {};
      // the source with the latest date of all, by comparing the dates the database gave; its start for the range
      // (the bounds query writes both columns: one missing is a bug, never the whole range shown in its place -- the
      // second audit's G notes; a source of no dates has no latest, and a tile of none shows what there is)
      let best: ResultTable | undefined;
      let latest = '';
      for (const t of this.#bounds.values()) {
        const v = boundsValue(t, TS_LATEST);
        if (v === null) continue;
        const at = String(v);
        if (at > latest) {
          latest = at;
          best = t;
        }
      }
      if (!best) return {};
      const from = boundsValue(best, rangeStartName(r.last));
      return from === null ? {} : { from: String(from).slice(0, 10) };
    }
    return { ...(r.from !== undefined ? { from: r.from } : {}), ...(r.to !== undefined ? { to: r.to } : {}) };
  }

  /** Every series asked again: the bounds, then the series, then the statistics -- the grids a combination reads opened
   *  together once for all of them. */
  async #fetch(): Promise<void> {
    const opened: Opened = new Map();
    try {
      await this.#fetchOver(opened);
    } finally {
      putAway(opened);
    }
  }

  async #fetchOver(opened: Opened): Promise<void> {
    const gen = ++this.#gen;
    const spec = this.#spec;
    this.#asked = queryKey(spec);
    this.#canvas.setAttribute('aria-busy', 'true');
    const problems = timeseriesProblems(spec, this.#columns(spec));
    this.#problems = problems;
    const signal = new AbortController().signal;
    // the bounds of each source on the page (its range buttons' starts, the database's)
    const bounds = new Map<string, ResultTable>();
    const boundsFailed = new Map<string, string>();
    await Promise.all(spec.sources.map(async (s) => {
      const own = this.#input(s.alias);
      if (!own) return;
      try {
        bounds.set(s.alias, await own.grid.run(boundsQuery(own.input, s), own.grid.snapshot, signal));
      } catch (e) {
        // its dates refused: a range from the latest date cannot be found from them -- said (#rangeProblems)
        boundsFailed.set(s.alias, e instanceof Error ? e.message : String(e));
      }
    }));
    if (gen !== this.#gen || this.#disposed) return;
    this.#bounds = bounds;
    this.#boundsFailed = boundsFailed;
    const window = this.#window();
    // the series with no problem of their own (a problem of the tile's stops them all)
    const blocked = (s: TsSeries): string | undefined =>
      problems.find((p) => !p.startsWith('Series ')) ?? problems.find((p) => p.startsWith(`Series ${s.id} `));
    const results = new Map<string, TsResult>();
    const failed = new Map<string, string>();
    await Promise.all(spec.series.map(async (s) => {
      const why = blocked(s);
      if (why !== undefined) {
        failed.set(s.id, why);
        return;
      }
      try {
        results.set(s.id, answered(await this.#run(s, (inputs) => seriesQuery(inputs, spec, s, readsAnchor(spec, s) && window.from ? { from: window.from } : {}), signal, opened)));
      } catch (e) {
        failed.set(s.id, e instanceof Error ? e.message : String(e));
      }
    }));
    if (gen !== this.#gen || this.#disposed) return;
    this.#results = results;
    this.#failed = failed;
    await this.#draw();
    await this.#statistics(gen, opened);
    // busy until its statistics are answered too: a refused one is said by then (the second audit's G notes: a browser
    // check that waited for the drawing alone met none)
    if (gen === this.#gen && !this.#disposed) this.#canvas.removeAttribute('aria-busy');
  }

  /** The window moved (a zoom, a range): the series that read its first date asked again, and the statistics. */
  async #windowed(): Promise<void> {
    const opened: Opened = new Map();
    try {
      await this.#windowedOver(opened);
    } finally {
      putAway(opened);
    }
  }

  async #windowedOver(opened: Opened): Promise<void> {
    const gen = ++this.#gen;
    const spec = this.#spec;
    const window = this.#window();
    const signal = new AbortController().signal;
    const anchored = spec.series.filter((s) => this.#results.has(s.id) && readsAnchor(spec, s));
    if (anchored.length > 0) {
      const again = await Promise.all(anchored.map(async (s) => {
        try {
          return [s.id, answered(await this.#run(s, (inputs) => seriesQuery(inputs, spec, s, window.from ? { from: window.from } : {}), signal, opened))] as const;
        } catch (e) {
          this.#failed.set(s.id, e instanceof Error ? e.message : String(e));
          return undefined;
        }
      }));
      if (gen !== this.#gen || this.#disposed) return;
      for (const r of again) if (r) this.#results.set(r[0], r[1]);
      await this.#draw();
    } else {
      this.#paintLegend();
    }
    await this.#statistics(gen, opened);
  }

  /** Each line's statistics over the window shown (the database's), for the legend's change and the table -- none asked
   *  while neither shows them (the review's follow-up: asked with Stats and the legend both off), asked when one does
   *  (#apply). `opened`: the round's grids opened together, else its own. */
  async #statistics(gen: number, opened?: Opened): Promise<void> {
    if (!viewSetting(this.#spec, 'stats') && !viewSetting(this.#spec, 'legend')) {
      this.#statsAsked = false;
      this.#statRows = new Map();
      this.#statsFailed = new Map();
      this.#paintBanner();
      return;
    }
    if (opened !== undefined) return this.#statisticsOver(gen, opened);
    const own: Opened = new Map();
    try {
      await this.#statisticsOver(gen, own);
    } finally {
      putAway(own);
    }
  }

  async #statisticsOver(gen: number, opened: Opened): Promise<void> {
    const spec = this.#spec;
    const window = this.#window();
    const signal = new AbortController().signal;
    const rows = new Map<string, ResultTable>();
    const failed = new Map<string, string>();
    await Promise.all(spec.series.filter((s) => this.#results.has(s.id) && !this.#results.get(s.id)!.query.band).map(async (s) => {
      try {
        rows.set(s.id, (await this.#run(s, (inputs) => statsQuery(inputs, spec, s, window), signal, opened)).table);
      } catch (e) {
        // refused: said on its row and in the banner, never as figures missing
        failed.set(s.id, e instanceof Error ? e.message : String(e));
      }
    }));
    if (gen !== this.#gen || this.#disposed) return;
    this.#statRows = rows;
    this.#statsFailed = failed;
    this.#statsAsked = true;
    this.#paintLegend();
    this.#paintStats();
    this.#paintBanner();
  }

  // -- drawing -------------------------------------------------------------------------------------------------------

  /** The chart's renderer, made the first time it is needed. */
  async #mount(): Promise<MountedTimeseries | null> {
    if (this.#chart) return this.#chart;
    this.#mounting ??= (async () => {
      const on: MountOn = {
        onZoom: (from, to) => this.#zoomed(from, to),
        onDate: (date) => this.#eventAt(date),
        onResize: () => void this.#draw(),
      };
      let make = this.#host.mount;
      if (make === undefined) {
        // the drawing, over the charts' ECharts (chart-render.ts canvasChart): both fetched together
        const [{ mountTimeseries }, { canvasChart }] = await Promise.all([
          lazily(() => import('../timeseries-render.ts')), lazily(() => import('../chart-render.ts'))]);
        make = (el, o) => mountTimeseries(el, o, canvasChart);
      }
      if (this.#disposed) return null;
      this.#chart = make(this.#canvas, on);
      return this.#chart;
    })();
    return this.#mounting;
  }

  /** A line's values in its grid's format: its source's grid's formatter for its column. */
  #formatOf(line: TsLine): TsFormat | undefined {
    if (line.formatColumn === undefined || line.formatSource === undefined) return undefined;
    const src = this.#spec.sources.find((s) => s.alias === line.formatSource);
    const grid = src ? this.#host.grid(src.cube) : undefined;
    if (!grid) return undefined;
    const column = line.formatColumn;
    const type = rowColumns(grid.snapshot).find((c) => c.name === column)?.type;
    return (v) => grid.format(v, column, type);
  }

  async #draw(): Promise<void> {
    const spec = this.#spec;
    const theme = themeOf(this.#root);
    const { lines, notes } = tsLines(spec, [...this.#results.values()], theme);
    this.#lines = lines;
    this.#notes = notes;
    this.#paintLegend();
    this.#paintBanner();
    // candles are one of ECharts' rarer charts (chart-render-kinds.ts), registered with the first drawn -- before the
    // chart is made: an ECharts chart takes the layouts it knows as it is made, so one made before knew no candle's (a
    // candle drawn on it had no place: "reading 'ends'"), and is made again
    if (lines.some((l) => l.candles) && !this.#candles && this.#host.mount === undefined) {
      await lazily(() => import('../chart-render-kinds.ts'));
      this.#candles = true;
      if (this.#disposed) return;
      if (this.#chart) {
        this.#chart.dispose();
        this.#chart = null;
        this.#mounting = null;
      }
    }
    const chart = await this.#mount();
    if (!chart || this.#disposed) return;
    const option = timeseriesOption(spec, lines, {
      theme, formatOf: (l) => this.#formatOf(l), view: this.#window(), height: chart.height() || this.#canvas.clientHeight || 420,
    });
    chart.show(option, describeTimeseries(spec, lines));
  }

  #zoomed(from: string | undefined, to: string | undefined): void {
    this.#zoom = { ...(from !== undefined ? { from } : {}), ...(to !== undefined ? { to } : {}) };
    this.#paintRanges();
    clearTimeout(this.#zoomTimer);
    this.#zoomTimer = setTimeout(() => void this.#windowed(), 300);
  }

  // -- the parts --------------------------------------------------------------------------------------------------------

  #build(): void {
    const doc = this.#doc;
    this.#root.replaceChildren();
    this.#root.classList.add('dc-ts', 'dc-chartpanel');
    this.#bar = el(doc, 'div', 'dc-ts-bar', undefined, { role: 'toolbar', 'aria-label': 'Timeseries' });
    this.#legend = el(doc, 'div', 'dc-ts-legend', undefined, { role: 'list', 'aria-label': 'Series' });
    this.#banner = el(doc, 'div', 'dc-ts-banner', undefined, { role: 'status' });
    this.#canvas = el(doc, 'div', 'dc-ts-chart', undefined, { role: 'img' });
    this.#stats = el(doc, 'div', 'dc-ts-stats');
    const body = el(doc, 'div', 'dc-ts-body');
    body.append(this.#canvas);
    this.#root.append(this.#bar, this.#legend, this.#banner, body, this.#stats);
    this.#paintBar();
  }

  #paintBar(): void {
    const doc = this.#doc;
    const spec = this.#spec;
    this.#ranges = el(doc, 'div', 'dc-ts-ranges', undefined, { role: 'group', 'aria-label': 'Range' });
    this.#paintRanges();
    const grain = select(doc, GRAINS, spec.grain ?? '', (v) => this.#edit({ kind: 'grain', grain: v === '' ? null : v as NonNullable<TimeseriesSpec['grain']> }), 'Frequency');
    const compare = select(doc, COMPARES, spec.view?.compare ?? '', (v) => this.#edit({ kind: 'view', view: { compare: v === '' ? null : v as 'rebase' | 'percent' } }), 'Compare');
    const log = button(doc, 'Log', 'The main pane on a log scale', () => this.#edit({ kind: 'pane', index: 0, log: !(this.#spec.panes?.[0]?.log ?? false) }));
    log.setAttribute('aria-pressed', String(spec.panes?.[0]?.log === true));
    const stats = button(doc, 'Stats', 'The statistics of every line over the dates shown', () => this.#edit({ kind: 'view', view: { stats: !viewSetting(this.#spec, 'stats') } }));
    stats.setAttribute('aria-pressed', String(viewSetting(spec, 'stats')));
    const add = button(doc, '+ Series', 'Plot another series: an expression, gs-quant\'s functions', () => this.#editSeries(null), 'dc-button dc-primary dc-ts-btn');
    const settings = button(doc, 'Settings', 'Sources, panes, annotations, view and export', () => this.#editSettings());
    const csv = button(doc, 'CSV', 'Download the lines as CSV (the rows the database answered)', () =>
      download(doc, 'timeseries.csv', URL.createObjectURL(new Blob([timeseriesCsv(this.#lines)], { type: 'text/csv' }))));
    const png = button(doc, 'PNG', 'Download the chart as a picture', () => {
      const url = this.#chart?.pngUrl();
      if (url) download(doc, 'timeseries.png', url);
    });
    this.#bar.replaceChildren(this.#ranges, el(doc, 'span', 'dc-ts-gap'), grain, compare, log, stats, add, settings, csv, png);
  }

  #paintRanges(): void {
    const doc = this.#doc;
    const r = this.#spec.range;
    const chosen = this.#zoom ? null : r && 'last' in r ? r.last : r ? 'custom' : 'max';
    this.#ranges.replaceChildren(...TS_RANGES.map(({ word, label }) => {
      const b = button(doc, label, `Show the last ${label === 'Max' ? 'of everything' : label}`, () => this.#setRange(word), 'dc-ts-range');
      b.setAttribute('aria-pressed', String(chosen === word));
      return b;
    }));
    if (this.#zoom || chosen === 'custom') {
      const w = this.#window();
      this.#ranges.append(el(doc, 'span', 'dc-ts-custom', `${w.from ?? '…'} – ${w.to ?? '…'}`));
    }
  }

  #setRange(word: TsRangeWord): void {
    this.#zoom = null;
    const spec = editTimeseries(this.#spec, { kind: 'range', range: { last: word } });
    this.#range = JSON.stringify(spec.range);
    this.#spec = spec;
    this.#host.setSpec(spec);
    this.#paintRanges();
    const w = this.#window();
    this.#chart?.zoomTo(w.from, w.to);
    void this.#windowed();
  }

  /** A change the person made, by the tile's one rule (timeseries-edit.ts editTimeseries, which Python asks too): kept
   *  and drawn, or refused in its words. */
  #edit(change: TsChange): boolean {
    let made: TimeseriesSpec;
    let kept: boolean;
    try {
      made = editTimeseries(this.#spec, change);
      kept = this.#set(made);
    } catch (e) {
      if (!(e instanceof TsEditRefusal)) throw e;
      this.#say([e.message], true);
      kept = false;
    }
    // refused in Settings: its fields shown as the tile has them again, never the refused value (the third review's 1)
    if (!kept && this.#settings) this.#editSettings();
    return kept;
  }

  #paintLegend(): void {
    const doc = this.#doc;
    if (!viewSetting(this.#spec, 'legend')) {
      this.#legend.hidden = true;
      return;
    }
    this.#legend.hidden = false;
    const w = this.#window();
    const chips = this.#lines.map((l) => {
      const chip = el(doc, 'button', `dc-ts-chip${l.series.hidden ? ' dc-ts-off' : ''}`, undefined, {
        type: 'button', role: 'listitem', title: `${seriesName(l.series)}: click to ${l.series.hidden ? 'show' : 'hide'}, double-click to edit`,
      });
      const swatch = el(doc, 'span', 'dc-ts-swatch');
      swatch.style.background = l.color;
      chip.append(swatch, el(doc, 'span', 'dc-ts-chip-name', l.name));
      // its latest value in the window shown: a point the database answered, read
      const at = l.points.findLastIndex((p) => p[1] !== null && (w.to === undefined || p[0].slice(0, 10) <= w.to));
      // a band says its edges there, a line its value
      const said = at < 0 ? undefined : l.band
        ? `${valueText(l, l.lower?.[at] ?? null, this.#formatOf(l))} – ${valueText(l, l.upper?.[at] ?? null, this.#formatOf(l))}`
        : valueText(l, l.points[at]![1], this.#formatOf(l));
      if (said !== undefined) chip.append(el(doc, 'span', 'dc-ts-chip-value', said));
      const pct = this.#stat(l, 'pct');
      if (pct !== null && (l.unit === 'same' || l.unit === 'index')) {
        chip.append(el(doc, 'span', `dc-ts-chip-change ${pct >= 0 ? 'dc-ts-up' : 'dc-ts-down'}`, `${pct >= 0 ? '▲' : '▼'} ${Math.abs(pct * 100).toLocaleString(UI_LOCALE, { maximumFractionDigits: 2, minimumFractionDigits: 2 })}%`));
      }
      chip.addEventListener('click', () => this.#edit({ kind: 'hide', id: l.series.id, hidden: l.series.hidden !== true }));
      chip.addEventListener('dblclick', () => this.#editSeries(l.series.id));
      chip.addEventListener('mouseenter', () => this.#chart?.highlight(l.key));
      chip.addEventListener('mouseleave', () => this.#chart?.highlight(null));
      return chip;
    });
    // a series the database refused: named, with why
    const failed = this.#spec.series.filter((s) => this.#failed.has(s.id)).map((s) => {
      const chip = el(doc, 'button', 'dc-ts-chip dc-ts-failed', undefined, { type: 'button', role: 'listitem', title: this.#failed.get(s.id)! });
      chip.append(el(doc, 'span', 'dc-ts-chip-name', `${s.id}: ${seriesName(s)}`), el(doc, 'span', 'dc-ts-chip-value', '!'));
      chip.addEventListener('click', () => this.#editSeries(s.id));
      return chip;
    });
    this.#legend.replaceChildren(...chips, ...failed);
  }

  #paintBanner(): void {
    const problems = [...this.#problems, ...this.#rangeProblems(), ...this.#statsProblems()];
    const said = [...problems, ...this.#notes];
    this.#banner.hidden = said.length === 0;
    this.#banner.replaceChildren(...said.map((t) => this.#doc.createTextNode(`${t} `)));
    this.#banner.classList.toggle('dc-ts-bad', problems.length > 0);
  }

  /** A range from the latest date whose sources' dates were refused: said, with what is shown instead. */
  #rangeProblems(): string[] {
    const r = this.#spec.range;
    if (!r || !('last' in r) || r.last === 'max' || this.#zoom || this.#boundsFailed.size === 0) return [];
    const refused = [...this.#boundsFailed].map(([alias, why]) => `${alias}'s dates were refused (${why})`).join('; ');
    return [`The ${r.last} range is not known: ${refused}. Shown from ${this.#window().from ?? 'the first date'}.`];
  }

  /** Each series' statistics the database refused, by its id. */
  #statsProblems(): string[] {
    if (!viewSetting(this.#spec, 'stats') && !viewSetting(this.#spec, 'legend')) return [];
    return [...this.#statsFailed].map(([id, why]) => `Series ${id}'s statistics were refused: ${why}`);
  }

  #say(words: readonly string[], bad = false): void {
    this.#banner.hidden = words.length === 0;
    this.#banner.textContent = words.join(' ');
    this.#banner.classList.toggle('dc-ts-bad', bad);
  }

  /** A line's statistic, from its series' statistics query (a split's by its value). */
  #stat(line: TsLine, stat: TsStat): number | null {
    const v = this.#statValue(line, stat);
    return typeof v === 'string' && /^\d{4}-/.test(v) ? null : toNumber(v as Scalar);
  }

  #statValue(line: TsLine, stat: TsStat): Scalar | undefined {
    const t = this.#statRows.get(line.series.id);
    if (!t) return undefined;
    const col = t.columns.find((c) => c.name === statName(stat));
    if (!col) return undefined;
    const splits = t.columns.find((c) => c.name === TS_SPLIT)?.values;
    const at = line.split === undefined ? 0 : splits ? splits.findIndex((v) => v === line.split) : -1;
    return at < 0 ? undefined : col.values[at];
  }

  #paintStats(): void {
    const doc = this.#doc;
    if (!viewSetting(this.#spec, 'stats')) {
      this.#stats.hidden = true;
      return;
    }
    this.#stats.hidden = false;
    const columns = this.#statsTab === 'risk' ? RISK_COLUMNS : STAT_COLUMNS;
    const tabs = el(doc, 'div', 'dc-ts-stats-tabs', undefined, { role: 'tablist' });
    for (const [tab, label, title] of STAT_TABS) {
      const b = button(doc, label, title, () => {
        this.#statsTab = tab;
        this.#paintStats();
      }, 'dc-ts-stats-tab');
      b.setAttribute('role', 'tab');
      b.setAttribute('aria-selected', String(tab === this.#statsTab));
      tabs.append(b);
    }
    const table = el(doc, 'table', 'dc-ts-table');
    const head = el(doc, 'tr');
    head.append(el(doc, 'th', undefined, 'Line'), ...columns.map((c) => el(doc, 'th', undefined, c.label)));
    table.append(head);
    const grain = this.#spec.grain ?? 'day';
    for (const l of this.#lines.filter((x) => !x.band && !x.series.hidden)) {
      const tr = el(doc, 'tr');
      const name = el(doc, 'td', 'dc-ts-table-name');
      const swatch = el(doc, 'span', 'dc-ts-swatch');
      swatch.style.background = l.color;
      name.append(swatch, doc.createTextNode(l.name));
      tr.append(name);
      const refused = this.#statsFailed.get(l.series.id);
      if (refused !== undefined) {
        tr.append(el(doc, 'td', 'dc-ts-table-refused', `Refused: ${refused}`, { colspan: String(columns.length) }));
        table.append(tr);
        continue;
      }
      for (const c of columns) {
        const raw = this.#statValue(l, c.stat);
        const n = toNumber(raw as Scalar);
        let text = '';
        if (raw !== undefined && raw !== null) {
          switch (c.as) {
            case 'value': text = valueText(l, n, this.#formatOf(l)); break;
            case 'ratio': text = valueText({ unit: 'ratio' }, n); break;
            case 'percent': text = valueText({ unit: 'percent' }, n); break;
            case 'number': text = valueText({ unit: 'number' }, n); break;
            case 'count': text = n === null ? '' : n.toLocaleString(UI_LOCALE); break;
            case 'date': text = dateLabel(String(raw), grain); break;
          }
        }
        tr.append(el(doc, 'td', c.as === 'date' ? 'dc-ts-table-date' : 'dc-ts-table-num', text || '—'));
      }
      table.append(tr);
    }
    const window = this.#window();
    const blanks = this.#statBlanks();
    const head_ = el(doc, 'div', 'dc-ts-stats-head');
    head_.append(tabs, doc.createTextNode(`Over ${window.from ?? 'the first date'} to ${window.to ?? 'the latest'}: every figure the database's`));
    this.#stats.replaceChildren(head_, table,
      ...(blanks.length > 0 ? [el(doc, 'div', 'dc-ts-stats-blanks', blanks.join(' '))] : []));
  }

  /**
   * WHY A LEVEL'S FIGURES ARE BLANK, line by line (the second audit's P-2b and S1: a "—" said nothing): no volatility or
   * Sharpe ratio where its dates are no frequency to annualize by; none of its returns' figures where it is at or below
   * zero over the dates shown -- each read off the database's answer, compared, never computed.
   */
  #statBlanks(): string[] {
    const out: string[] = [];
    for (const l of this.#lines.filter((x) => x.level && !x.band && !x.series.hidden && !this.#statsFailed.has(x.series.id))) {
      // the level's own least, before compare took it from its start (a % change from its start is zero there)
      const low = this.#statColumn(l, TS_STAT_LEVEL_LOW);
      if (low !== undefined && low !== null && low <= 0) {
        out.push(`${l.name} is at or below zero over these dates: it has no volatility, drawdown, growth or Sharpe ratio, nor any figure of its returns' risk.`);
        continue;
      }
      if (this.#statColumn(l, TS_STAT_PER_YEAR) === null) {
        out.push(`${l.name}: its dates are no frequency to annualize its volatility and Sharpe ratio by: choose the tile's frequency.`);
      }
      // the database's growth blank above zero: these dates span under a year (the review's N3)
      if (low !== undefined && low !== null && this.#statColumn(l, statName('cagr')) === null) {
        out.push(`${l.name}: these dates span under a year, so its growth is not annualized: its % change is its growth.`);
      }
    }
    return out;
  }

  /** A line's figure of its statistics' answer by its column -- the periods a year it was annualized by, as the database
   *  inferred them; its level's least -- undefined where the answer has no such column (a tile of a frequency: nothing
   *  inferred). */
  #statColumn(l: TsLine, name: string): number | null | undefined {
    const t = this.#statRows.get(l.series.id);
    const col = t?.columns.find((c) => c.name === name);
    if (!t || !col) return undefined;
    const splits = t.columns.find((c) => c.name === TS_SPLIT)?.values;
    const at = l.split === undefined ? 0 : splits ? splits.findIndex((v) => v === l.split) : -1;
    return at < 0 ? undefined : toNumber(col.values[at] as Scalar);
  }

  // -- editing -------------------------------------------------------------------------------------------------------

  /**
   * AN EDITOR AS A WINDOW (the user, 2026-10-26: the timeseries tile in PR #73's design -- a chart's options are a window
   * beside it): beside the tile, on the screen, among the page's windows -- dragged by its title, resized from its edges,
   * its × or Escape putting it away, gone with its tile (its sheet left, moved, removed). One at a time: the series
   * editor and Settings share it, its content replaced.
   */
  #open(title: string, ...content: HTMLElement[]): void {
    const doc = this.#doc;
    const view = doc.defaultView;
    if (view === null) return;
    const host = this.#host.windowHost;
    let win = this.#window_;
    if (!win) {
      win = el(doc, 'div', 'dc-app-overlay dc-app-floating dc-ts-window', undefined, { role: 'dialog', tabindex: '-1', 'aria-label': title });
      const head = el(doc, 'div', 'dc-overlay-head');
      const close = el(doc, 'button', 'dc-overlay-close', undefined, { type: 'button', 'aria-label': 'Close (Escape)', title: 'Close (Escape)' });
      close.append(chromeIcon(doc, 'close'));
      close.addEventListener('click', () => this.#close());
      head.append(el(doc, 'span', 'dc-ts-window-title'), close);
      const scroller = el(doc, 'div', 'dc-overlay-body');
      scroller.append(this.#pop = el(doc, 'div', 'dc-ts-pop'));
      win.append(head, scroller);
      host.append(win);
      this.#window_ = win;
      const closing = new AbortController();
      this.#closing = closing;
      makeWindow(win, head, host, {
        viewport: true,
        spec: besideWindow(this.#root.getBoundingClientRect(), screenOf(view), TS_WINDOW),
        minWidth: TS_WINDOW.minWidth,
        minHeight: TS_WINDOW.minHeight,
        signal: closing.signal,
      });
      const opened = win;
      opened.addEventListener('pointerdown', () => raiseWindow(opened, host));
      // Escape puts it away -- not one typed in a field, where it is the field's (the page's windows' rule, app.ts)
      opened.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape' || event.defaultPrevented || isTextEntry(event.target)) return;
        event.preventDefault();
        this.#close();
      });
      // its tile off the page (its sheet left, moved to another, removed): it goes too, as a chart's options do
      const Watch = (view as Window & { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;
      if (Watch) {
        const watch = new Watch(() => {
          const at = this.#root.getBoundingClientRect();
          if (!this.#root.isConnected || (at.width === 0 && at.height === 0)) this.#close();
        });
        watch.observe(this.#root);
        closing.signal.addEventListener('abort', () => watch.disconnect());
      }
    }
    const named = `${title} \u2014 ${this.#host.title()}`;
    win.querySelector('.dc-ts-window-title')!.textContent = named;
    win.setAttribute('aria-label', named);
    this.#pop.replaceChildren(...content);
    this.#settings = false;
    raiseWindow(win, host);
    // the window itself focused, as a chart's options are: Escape puts it away until a field is entered, where Escape is
    // the field's (the third review's 2: the first field focused, Escape never closed it)
    win.focus({ preventScroll: true });
  }

  /** Its editor's window put away -- the focus, when it was in it, back on its tile. */
  #close(): void {
    const win = this.#window_;
    if (!win) return;
    const root = win.getRootNode() as Document | ShadowRoot;
    const had = root.activeElement !== null && win.contains(root.activeElement);
    this.#closing?.abort();
    this.#closing = null;
    this.#settings = false;
    win.remove();
    this.#window_ = null;
    this.#pop = this.#doc.createElement('div');
    // back on its tile -- its focusable frame, as a chart's options give it back (the third review's 3: the viewer's own
    // element takes no focus, and it fell to the body)
    if (had) this.#root.closest<HTMLElement>('[tabindex]')?.focus({ preventScroll: true });
  }

  /** Its Settings, as its tile's right-click Properties… opens them (page/cube-page.ts). */
  openSettings(): void {
    this.#editSettings();
  }

  /** The series editor: a new one (`id` null) or one of the tile's. */
  #editSeries(id: string | null): void {
    const doc = this.#doc;
    const spec = this.#spec;
    const was = id === null ? undefined : spec.series.find((s) => s.id === id);
    const sid = was?.id ?? nextSeriesId(spec.series);
    const columns = this.#columns(spec);
    const many = spec.sources.length > 1;
    // the expression, read as it is typed, its error said where it is
    const expr = el(doc, 'input', 'dc-input dc-ts-expr', undefined, {
      'aria-label': 'Expression', spellcheck: 'false', placeholder: many ? `${spec.sources[0]?.alias}:close[AAPL]` : 'close[AAPL]',
    });
    expr.value = was ? exprText(was.expr) : '';
    const error = el(doc, 'div', 'dc-ts-error', undefined, { role: 'alert' });
    const reads = el(doc, 'div', 'dc-ts-hint');
    let parsed: TsExpr | undefined;
    const check = (): void => {
      error.textContent = '';
      parsed = undefined;
      if (expr.value.trim() === '') return;
      try {
        parsed = parseExpr(expr.value);
        const read = sourcesOf(spec, inlined(parsed, spec.series.filter((s) => s.id !== sid)));
        reads.textContent = `Reads ${read.join(', ') || 'no grid'}${read.length > 1 ? ': one query, the grids joined on the date' : ''}.`;
      } catch (e) {
        error.textContent = e instanceof TsSyntaxError ? `${e.message} (at ${e.at + 1})` : e instanceof Error ? e.message : String(e);
        reads.textContent = '';
      }
    };
    expr.addEventListener('input', check);
    // building one: a source, a column, an instrument, a line per instrument
    const sourceSel = select(doc, spec.sources.map((s) => [s.alias, `${s.alias} (${this.#host.grid(s.cube)?.title ?? 'its grid is gone'})`] as const),
      spec.sources[0]?.alias ?? '', () => paintPickers(), 'Source');
    const columnSel = select(doc, [], '', () => undefined, 'Column');
    const keySel = select(doc, [['', 'Every row']], '', () => undefined, 'Instrument');
    // a line per value of any text column of the series' first source -- Python's split too (the review's A6: the box
    // made it its source's instrument column, so a series split otherwise was rewritten by an edit)
    const splitSel = el(doc, 'select', 'dc-select dc-ts-select', undefined, { 'aria-label': 'Lines' });
    const paintPickers = (): void => {
      const src = spec.sources.find((s) => s.alias === sourceSel.value);
      const known = src ? columns.get(src.alias) : undefined;
      // a source whose grid is not on the page has no columns to pick: said in its pickers, never an empty list
      const cols = known ?? [];
      const numbers = cols.filter((c) => isNumeric(c.type) && c.name !== src?.date);
      columnSel.replaceChildren(...(known === undefined ? [unpicked(doc, 'Its grid is not on the page')] : numbers.map((c) => option(doc, c.name, c.name))));
      keySel.replaceChildren(option(doc, 'Every row', ''));
      const texts = cols.filter((c) => !isNumeric(c.type) && !isTemporal(c.type)).map((c) => c.name);
      const chosen = splitSel.value !== '' ? splitSel.value : was?.split ?? '';
      splitSel.replaceChildren(option(doc, 'One line', ''), ...[...new Set([...texts, ...(chosen !== '' ? [chosen] : [])])].map((c) => option(doc, `A line per ${c}`, c)));
      splitSel.value = chosen;
      const key = src?.key;
      if (src !== undefined && key !== undefined) {
        void this.#keyValues(src, key).then((values) => {
          for (const v of values) keySel.append(option(doc, v, v));
        }, (e: unknown) => {
          keySel.append(unpicked(doc, `Its instruments were refused: ${e instanceof Error ? e.message : String(e)}`));
        });
      }
    };
    paintPickers();
    const use = button(doc, 'Use', 'Write the expression of this column', () => {
      const src = spec.sources.find((s) => s.alias === sourceSel.value);
      if (!src || columnSel.value === '') return;
      const own = many || src.alias !== spec.sources[0]?.alias ? src.alias : undefined;
      expr.value = exprText(columnRef(columnSel.value, own, keySel.value !== '' ? keySel.value : undefined));
      check();
    });
    // a function around it: gs-quant's, by family
    const fnSel = el(doc, 'select', 'dc-select dc-ts-select', undefined, { 'aria-label': 'Function' });
    fnSel.append(option(doc, 'Apply a function…', ''));
    for (const family of FAMILIES) {
      const group = el(doc, 'optgroup', undefined, undefined, { label: family });
      for (const f of TS_FUNCTIONS.filter((x) => x.family === family)) group.append(option(doc, f.label, f.name));
      fnSel.append(group);
    }
    const about = el(doc, 'div', 'dc-ts-hint');
    fnSel.addEventListener('change', () => {
      const f = functionNamed(fnSel.value);
      if (!f) return;
      // a blank box: the function of the picked source's first value (DataCube's first plot's rule, firstValue), else
      // refused in words -- never a column guessed by name
      let inner = expr.value.trim();
      if (inner === '') {
        const picked = spec.sources.find((s) => s.alias === sourceSel.value);
        const first = picked ? firstValue(columns.get(picked.alias) ?? [], picked) : undefined;
        if (picked === undefined || first === undefined) {
          fnSel.value = '';
          error.textContent = 'Write the series it applies to first, or build one from a column.';
          return;
        }
        inner = exprText(columnRef(first, many ? picked.alias : undefined));
      }
      if (f.candles) {
        // a candle of the column written: its source's open, high, low and close (timeseries-expr.ts candleOf)
        fnSel.value = '';
        let column: TsExpr | undefined;
        try {
          column = parseExpr(inner);
        } catch {
          column = undefined;
        }
        const src = column?.kind === 'column' ? spec.sources.find((s) => s.alias === (column.source ?? spec.sources[0]?.alias)) : undefined;
        const own = src ? columns.get(src.alias) : undefined;
        if (src && own === undefined) {
          error.textContent = `Candles read their grid's open, high and low: ${src.alias}'s grid is not on the page.`;
          return;
        }
        const made = column && own ? candleOf(column, own) : undefined;
        if (made === undefined) {
          error.textContent = 'Candles are of a column whose grid has its open, high and low beside it: write a close (close[AAPL]), or ohlc(open, high, low, close).';
          return;
        }
        expr.value = exprText(made);
        about.textContent = `${f.label}: ${f.about}`;
        check();
        return;
      }
      const other = spec.series.find((s) => s.id !== sid);
      const args = f.series === 2 ? [inner, other ? `$${other.id}` : inner] : [inner];
      const params = f.params.filter((p) => p.name === 'w').map((p) => String(p.default));
      expr.value = `${f.name}(${[...args, ...params].join(', ')})`;
      about.textContent = `${f.label}: ${f.about}${f.params.length ? ` Parameters: ${f.params.map((p) => `${p.name} (${p.about}; default ${p.default})`).join('; ')}.` : ''}${f.differs ? ` Differs from gs-quant's ${f.gsQuant}: ${f.differs}.` : ` gs-quant: ${f.gsQuant}.`}`;
      fnSel.value = '';
      check();
    });
    // how it is drawn
    const name = el(doc, 'input', 'dc-input', undefined, { 'aria-label': 'Name', placeholder: 'Its expression' });
    name.value = was?.name ?? '';
    const pane = select(doc, Array.from({ length: MAX_PANES }, (_, i) => [String(i), i === 0 ? 'Main pane' : `Pane ${i + 1}`] as const), String(was?.pane ?? 0), () => undefined, 'Pane');
    const axis = select(doc, [['', 'Axis by unit'], ['left', 'Left axis'], ['right', 'Right axis']], was?.axis ?? '', () => undefined, 'Axis');
    const kind = select(doc, STYLES, was?.style?.kind ?? 'line', () => undefined, 'Drawn as');
    const width = select(doc, [['1', '1px'], ['2', '2px'], ['3', '3px'], ['4', '4px']], String(was?.style?.width ?? 2), () => undefined, 'Width');
    const dash = select(doc, [['', 'Solid'], ['dashed', 'Dashed'], ['dotted', 'Dotted']], was?.style?.dash ?? '', () => undefined, 'Dash');
    const theme = themeOf(this.#root);
    let color: number | undefined = was?.style?.color;
    const swatches = el(doc, 'div', 'dc-ts-swatches', undefined, { role: 'radiogroup', 'aria-label': 'Colour' });
    const paintSwatches = (): void => {
      swatches.replaceChildren(...[undefined, ...theme.series.map((_, i) => i)].map((slot) => {
        const b = el(doc, 'button', `dc-ts-swatch-pick${slot === color ? ' dc-ts-picked' : ''}`, slot === undefined ? 'A' : '', {
          type: 'button', role: 'radio', 'aria-checked': String(slot === color), title: slot === undefined ? 'By its place' : `Colour ${slot + 1}`,
        });
        if (slot !== undefined) b.style.background = theme.series[slot]!;
        b.addEventListener('click', () => {
          color = slot;
          paintSwatches();
        });
        return b;
      }));
    };
    paintSwatches();
    const ok = button(doc, 'OK', 'Plot it (Enter)', () => {
      check();
      if (!parsed) {
        if (expr.value.trim() === '') error.textContent = 'Write an expression, or build one from a column.';
        return;
      }
      const style: Record<string, unknown> = {
        ...(kind.value !== 'line' ? { kind: kind.value } : {}), ...(color !== undefined ? { color } : {}),
        ...(width.value !== '2' ? { width: Number(width.value) } : {}), ...(dash.value !== '' ? { dash: dash.value } : {}),
      };
      const made: TsSeriesTold = {
        id: sid, expr: parsed,
        ...(name.value.trim() !== '' ? { name: name.value.trim() } : {}),
        ...(splitSel.value !== '' ? { split: splitSel.value } : {}),
        ...(Object.keys(style).length > 0 ? { style } : {}),
        ...(pane.value !== '0' ? { pane: Number(pane.value) } : {}),
        ...(axis.value !== '' ? { axis: axis.value as 'left' | 'right' } : {}),
        ...(was?.hidden ? { hidden: true } : {}),
      };
      if (this.#edit({ kind: 'plot', series: made, ...(was ? { replace: true } : {}) })) this.#close();
    }, 'dc-button dc-primary');
    expr.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') ok.click();
    });
    const remove = button(doc, 'Remove', 'Take this series off', () => {
      if (this.#edit({ kind: 'unplot', id: sid })) this.#close();
    });
    remove.hidden = was === undefined;
    const reference = el(doc, 'details', 'dc-ts-reference');
    reference.append(el(doc, 'summary', undefined, 'Functions (gs-quant\'s names)'));
    const list = el(doc, 'dl', 'dc-ts-functions');
    for (const f of TS_FUNCTIONS) {
      const sig = `${f.name}(${[...Array.from({ length: f.series }, (_, i) => (i === 0 ? 'x' : 'y')), ...f.params.map((p) => `${p.name}=${p.default}`)].join(', ')})`;
      list.append(el(doc, 'dt', undefined, sig), el(doc, 'dd', undefined, `${f.about}${f.differs ? ` (${f.differs})` : ''}`));
    }
    reference.append(list, el(doc, 'p', 'dc-ts-hint', 'Columns: close, px:close, close[AAPL], close[venue=NYSE, AAPL], volume[AAPL].sum. Series: $A. Arithmetic: + - * /.'));
    const builder = el(doc, 'div', 'dc-ts-row');
    builder.append(sourceSel, columnSel, keySel, use);
    const style = el(doc, 'div', 'dc-ts-row');
    style.append(kind, width, dash, pane, axis);
    const actions = el(doc, 'div', 'dc-ts-actions');
    actions.append(remove, el(doc, 'span', 'dc-ts-gap'), button(doc, 'Cancel', 'Leave it as it was (Escape)', () => this.#close()), ok);
    this.#open(was ? `Series ${sid}` : `New series ${sid}`,
      labelled(doc, 'Expression', expr), error, reads, builder, splitSel, fnSel, about,
      labelled(doc, 'Name', name), style, swatches, reference, actions);
    check();
  }

  /** The values of a source's instrument column, the database's (the picker's): refused, in the database's words. */
  async #keyValues(src: TsSource, key: string): Promise<string[]> {
    const own = this.#input(src.alias);
    if (!own) throw new Error(`${src.alias}'s grid is not on the page`);
    const t = await own.grid.run(keyValuesQuery(own.input, src, key), own.grid.snapshot, new AbortController().signal);
    const values = t.columns[0];
    if (values === undefined) throw new Error(`the database answered ${key}'s values with no column`);
    return values.values.map((v) => String(v));
  }

  /** Settings: the sources, the view, the panes, the annotations, a range of two dates, export. */
  #editSettings(): void {
    const doc = this.#doc;
    const spec = this.#spec;
    const parts: HTMLElement[] = [];
    // the sources: each grid, its alias, its date and instrument columns
    parts.push(el(doc, 'div', 'dc-ts-heading', 'Grids it reads'));
    const grids = this.#host.grids();
    for (const s of spec.sources) {
      const grid = this.#host.grid(s.cube);
      const cols = grid ? rowColumns(grid.snapshot) : [];
      const row = el(doc, 'div', 'dc-ts-row');
      const alias = el(doc, 'input', 'dc-input dc-ts-alias', undefined, { 'aria-label': 'Alias' });
      alias.value = s.alias;
      alias.addEventListener('change', () => this.#edit({ kind: 'source', alias: s.alias, rename: alias.value.trim() }));
      const date = select(doc, cols.filter((c) => isTemporal(c.type) && !isTimeOfDay(c.type)).map((c) => [c.name, c.name] as const), s.date,
        (v) => this.#edit({ kind: 'source', alias: s.alias, date: v }), 'Date column');
      const key = select(doc, [['', 'No instruments'], ...cols.filter((c) => !isNumeric(c.type) && !isTemporal(c.type)).map((c) => [c.name, c.name] as const)],
        s.key ?? '', (v) => this.#edit({ kind: 'source', alias: s.alias, key: v === '' ? null : v }), 'Instrument column');
      const where = el(doc, 'span', 'dc-ts-hint', grid === undefined ? 'its grid is not on the page'
        : grid.kept ? `${grid.title} (its grid removed: read as it was)` : grid.title);
      // the tile's own rule says whether it may stop reading this grid, and why not
      let refused: string | undefined;
      try {
        editTimeseries(spec, { kind: 'unread', alias: s.alias });
      } catch (e) {
        if (!(e instanceof TsEditRefusal)) throw e;
        refused = e.message;
      }
      const off = closeMark(button(doc, '×', refused ?? 'Stop reading this grid', () => {
        if (this.#edit({ kind: 'unread', alias: s.alias })) this.#editSettings();
      }, 'dc-ts-close'));
      off.disabled = refused !== undefined;
      row.append(alias, where, date, key, off);
      parts.push(row);
    }
    const others = grids.filter((g) => !spec.sources.some((s) => s.cube === g.cube));
    if (others.length > 0) {
      const addGrid = select(doc, [['', 'Read another grid…'], ...others.map((g) => [g.cube, g.title] as const)], '', (cube) => {
        const g = others.find((x) => x.cube === cube);
        if (!g) return;
        if (this.#edit({ kind: 'read', grid: gridColumns(g) })) this.#editSettings();
      }, 'Read another grid');
      parts.push(addGrid);
    }
    // the view
    parts.push(el(doc, 'div', 'dc-ts-heading', 'View'));
    const toggles = el(doc, 'div', 'dc-ts-row');
    for (const [key, label] of [['legend', 'Legend'], ['lastValues', 'Latest values'], ['navigator', 'Navigator'], ['crosshair', 'Crosshair'], ['stats', 'Statistics']] as const) {
      const box = el(doc, 'input', undefined, undefined, { type: 'checkbox' });
      box.checked = viewSetting(spec, key);
      box.addEventListener('change', () => this.#edit({ kind: 'view', view: { [key]: box.checked } }));
      const l = el(doc, 'label', 'dc-ts-check');
      l.append(box, doc.createTextNode(label));
      toggles.append(l);
    }
    parts.push(toggles);
    // a range of two dates
    const r = spec.range;
    const from = el(doc, 'input', 'dc-input', undefined, { type: 'date', 'aria-label': 'From' });
    const to = el(doc, 'input', 'dc-input', undefined, { type: 'date', 'aria-label': 'To' });
    if (r && !('last' in r)) {
      from.value = r.from ?? '';
      to.value = r.to ?? '';
    }
    const dates = el(doc, 'div', 'dc-ts-row');
    dates.append(el(doc, 'span', 'dc-ts-field-label', 'Dates'), from, to, button(doc, 'Show', 'Show these dates', () => {
      this.#edit({ kind: 'range', range: { ...(from.value ? { from: from.value } : {}), ...(to.value ? { to: to.value } : {}) } });
    }));
    parts.push(dates);
    // the panes in use: each its share of the height, its scale
    const used = [...new Set([0, ...spec.series.map((s) => s.pane ?? 0)])].sort();
    parts.push(el(doc, 'div', 'dc-ts-heading', 'Panes'));
    for (const p of used) {
      const row = el(doc, 'div', 'dc-ts-row');
      const height = el(doc, 'input', 'dc-input dc-ts-small', undefined, { type: 'number', min: '1', max: '10', step: '1', 'aria-label': 'Height' });
      height.value = String(spec.panes?.[p]?.height ?? (p === 0 ? 3 : 1));
      height.addEventListener('change', () => this.#edit({ kind: 'pane', index: p, height: Math.min(10, Math.max(1, Number(height.value) || 1)) }));
      const log = el(doc, 'input', undefined, undefined, { type: 'checkbox' });
      log.checked = spec.panes?.[p]?.log === true;
      log.addEventListener('change', () => this.#edit({ kind: 'pane', index: p, log: log.checked }));
      const l = el(doc, 'label', 'dc-ts-check');
      l.append(log, doc.createTextNode('Log scale'));
      row.append(el(doc, 'span', 'dc-ts-field-label', p === 0 ? 'Main' : `Pane ${p + 1}`), height, l);
      parts.push(row);
    }
    // the annotations
    parts.push(el(doc, 'div', 'dc-ts-heading', 'Annotations'));
    for (const [i, a] of (spec.annotations ?? []).entries()) {
      const row = el(doc, 'div', 'dc-ts-row');
      const said = a.kind === 'level' ? `Level ${a.value}` : a.kind === 'event' ? `Event ${a.date}` : `Period ${a.from} – ${a.to}`;
      row.append(el(doc, 'span', undefined, `${said}${a.kind === 'level' && a.pane ? ` (pane ${a.pane + 1})` : ''}${a.label ? `: ${a.label}` : ''}`), closeMark(button(doc, '×', 'Take it off', () => {
        if (this.#edit({ kind: 'unannotate', index: i })) this.#editSettings();
      }, 'dc-ts-close')));
      parts.push(row);
    }
    const kind = select(doc, [['level', 'A level'], ['event', 'An event'], ['period', 'A period']], 'event', () => paintAdd(), 'Kind');
    const a1 = el(doc, 'input', 'dc-input dc-ts-small');
    const a2 = el(doc, 'input', 'dc-input dc-ts-small', undefined, { type: 'date' });
    const label = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Label', 'aria-label': 'Label' });
    // a level on a pane of its own (an RSI's 70), as Python's annotations say it
    const levelPane = select(doc, used.map((p) => [String(p), p === 0 ? 'Main pane' : `Pane ${p + 1}`] as const), '0', () => undefined, 'Pane');
    const paintAdd = (): void => {
      a1.type = kind.value === 'level' ? 'number' : 'date';
      a1.setAttribute('aria-label', kind.value === 'level' ? 'Value' : kind.value === 'event' ? 'Date' : 'From');
      a2.hidden = kind.value !== 'period';
      levelPane.hidden = kind.value !== 'level';
    };
    paintAdd();
    const addRow = el(doc, 'div', 'dc-ts-row');
    addRow.append(kind, a1, a2, levelPane, label, button(doc, 'Add', 'Draw it over the series', () => {
      const text = label.value.trim();
      const made: TsAnnotation | undefined = kind.value === 'level' && a1.value !== ''
        ? { kind: 'level', value: Number(a1.value), ...(levelPane.value !== '0' ? { pane: Number(levelPane.value) } : {}), ...(text ? { label: text } : {}) }
        : kind.value === 'event' && a1.value !== '' ? { kind: 'event', date: a1.value, ...(text ? { label: text } : {}) }
          : kind.value === 'period' && a1.value !== '' && a2.value !== '' ? { kind: 'period', from: a1.value, to: a2.value, ...(text ? { label: text } : {}) }
            : undefined;
      if (!made) return;
      if (this.#edit({ kind: 'annotate', annotation: made })) this.#editSettings();
    }));
    parts.push(addRow, el(doc, 'div', 'dc-ts-hint', 'Double-click the chart to put an event at that date.'));
    this.#open('Settings', ...parts);
    this.#settings = true;
  }

  /** A double-click on the chart: an event at that date, labelled. */
  #eventAt(date: string): void {
    const doc = this.#doc;
    const label = el(doc, 'input', 'dc-input', undefined, { placeholder: 'What happened (optional)', 'aria-label': 'Label' });
    const add = (): void => {
      const text = label.value.trim();
      if (this.#edit({ kind: 'annotate', annotation: { kind: 'event', date, ...(text ? { label: text } : {}) } })) this.#close();
    };
    label.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') add();
    });
    const actions = el(doc, 'div', 'dc-ts-actions');
    actions.append(el(doc, 'span', 'dc-ts-gap'), button(doc, 'Cancel', 'Escape', () => this.#close()), button(doc, 'Add', 'Mark the date', add, 'dc-button dc-primary'));
    this.#open(`An event on ${dateLabel(date, 'day')}`, label, actions);
  }
}

