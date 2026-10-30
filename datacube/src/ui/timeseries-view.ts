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

import type { EChartsOption } from 'echarts';
import type { Lambda, ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import { hasTimeOfDay, isNumeric, isTemporal, isTimeOfDay } from '../../../engine-client/src/types.ts';
import { themeOf } from '../chart-option.ts';
import type { ChartPicture } from '../chart-render.ts';
import type { RunOutcome } from '../runner.ts';
import { rowColumns, type CubeSnapshot, type FilterNode } from '../snapshot.ts';
import {
  marksName, MAX_HIST_BINS, projectionName, TS_PROJECTION_AHEAD, TS_PROJECTION_WINDOW, MIN_PROJECTION_WINDOW, MAX_PANES, MAX_SEASON_YEARS, MIN_HIST_BINS, TS_CURVE_DATES, TS_HIST_BINS, TS_RANGES, TS_SEASON_YEARS, viewSetting,
  type TimeseriesSpec, type TsAlign, type TsAnnotation, type TsExpr, type TsMarks, type TsMarkShape, type TsMode, type TsPoint, type TsProjection, type TsRangeWord, type TsSeries, type TsSource,
  type TsState, type TsStyleKind,
} from '../timeseries-spec.ts';
import {
  candleOf, columnRef, exprSource, exprText, firstValue, functionNamed, inlined, matchesText, nextSeriesId, parseExpr, parseMatches, seriesName,
  sourcesOf, timeseriesProblems, TsSyntaxError, type TsColumn, TS_FUNCTIONS, type TsFamily, type TsParam,
} from '../timeseries-expr.ts';
import {
  editTimeseries, firstTimeseries as firstOf, instrumentsOf, judgedEdit, TsEditRefusal, type TsChange, type TsGridColumns, type TsSeriesTold,
} from '../timeseries-edit.ts';
import {
  askKey, askQuery, askSources, boundsQuery, keyValuesQuery, marksOf, marksQuery, projectionOf, rangeWindow, readsAnchor, statBlanks, statName, tableSeries, tileAsks,
  viewChoices, viewSeries, TS_DATE, TS_SERVES_WORDS, type TsAsk, type TsAskOf,
  TS_CAL_LOW, TS_CAL_MONTH, TS_CAL_RETURN, TS_CAL_YEAR, TS_CAL_YEAR_RETURN, TS_CORR, TS_CORR_A, TS_CORR_B, TS_DD_DECLINE, TS_DD_DEPTH, TS_DD_PEAK, TS_DD_RANK, TS_DD_RECOVERED, TS_DD_RECOVERY, TS_DD_TROUGH,
  TS_SPLIT, TS_STAT_LEVEL_LOW, TS_STAT_PER_YEAR, TS_VALUE, type TsInput, type TsInputs, type TsQuery, type TsStat, type TsWindow,
} from '../timeseries-query.ts';
import {
  describeTimeseries, dateLabel, drawingsLine, drawingsUnplaced, marksUnplaced, projectionsUnplaced, timeseriesCsv, timeseriesOption, timeseriesTable, timeseriesTsv, toNumber, tsHeading, tsLines,
  tsMarks, tsPictureHeight, tsProjected, valueText, type TsFormat, type TsLine, type TsMarked, type TsProjected, type TsResult,
} from '../timeseries-option.ts';
import type { MountedTimeseries } from '../timeseries-render.ts';
import { curveNotes, curveOption, describeView, histogramOption, scatterOption, seasonalityOption, TS_MONTH_NAMES, type TsViewContext } from '../timeseries-views.ts';
import { lazily } from '../lazily.ts';
import { LIBRARY_PACKAGE, loadLibrary } from '../pure-library.ts';
import type { TsCodeLanguage, TsCodeOf } from '../timeseries-code.ts';
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
  /** A query run by the grid's runner: its rows, and the SQL the database ran for them (the Query window's). */
  run(query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal): Promise<RunOutcome>;
  /** A value as the grid writes it in this column. */
  format(value: Scalar, column: string, type: string | undefined): string;
  /** A query as the compiler prints it, for a person to read (the Query window: the next phases' 2). */
  print(query: Lambda): Promise<string>;
}

/** Grids opened over one model of their tables, for a series that combines them: each cube's source over the model. */
export interface TsTogether {
  readonly relations: ReadonlyMap<string, ValueSpecification>;
  /** A query over the model planned and run: its rows, and its plan's SQL the database ran. */
  run(query: Lambda): Promise<RunOutcome>;
  dispose(): void;
}

/** A query's answer as the viewer keeps it: its rows, and the SQL the database ran for them. */
type Ran = TsResult & { readonly sql: string };

/** Each language's tab in the Query window: its name, and what its program does. */
const CODE_WORDS: Readonly<Record<TsCodeLanguage, { readonly label: string; readonly title: string; readonly hint: string }>> = {
  python: {
    label: 'Python', title: 'The Python that builds this tile',
    hint: 'This tile built in Python (legend_lite): its sheet\'s timeseries over the page\'s grids, as Create ▸ Timeseries makes it, then its '
      + 'points, its statistics and its queries -- tile.code() in Python writes the same.',
  },
  typescript: {
    label: 'TypeScript', title: 'The TypeScript that builds this tile',
    hint: 'This tile built in TypeScript: its spec, and the page command Create ▸ Timeseries sends, applied by DataCube\'s one function '
      + '(page-commands.ts applyPageCommands) -- as Python\'s sheet.timeseries applies it.',
  },
};

/** A question the viewer asked the database, kept for the Query window: the tile and the dates it was asked of, its
 *  query as it was sent, and the SQL the database ran for it. */
type Asked = { readonly spec: TimeseriesSpec; readonly window: TsWindow; readonly query: Lambda; readonly sql: string };

/** A series' calendar of returns as the database answered it (timeseries-query.ts calendarQuery), or its refusal. */
type CalendarAnswer = { readonly id: string; readonly table: ResultTable } | { readonly id: string; readonly failed: string };

/** A view drawn (timeseries-views.ts): its option, and its words for a reader of the screen. */
type ViewDrawn = { readonly option: EChartsOption; readonly described: string };

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
function answered<R extends TsResult>(r: R): R {
  for (const c of [TS_DATE, TS_VALUE]) {
    if (!r.table.columns.some((x) => x.name === c)) throw new Error(`the database answered without the series' ${c === TS_DATE ? 'dates' : 'values'}`);
  }
  return r;
}

/** How long a grid's view waits before the tile asks again: a burst of views (a grid expanding row by row) asks once, as
 *  a chart's (ui/chart-panel.ts debounceMs). */
const REFRESH_DEBOUNCE_MS = 150;
/** A replay's steps from a quarter into the dates shown to the last, and the time between them: about six seconds. */
const REPLAY_STEPS = 60;
const REPLAY_TICK_MS = 100;

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
  /** Where it is on its page, as the code that builds it names it (timeseries-code.ts): its sheet, its own title (null:
   *  its tile's numbered one) and each of `spec`'s sources' grid tiles by cube -- the Query window's Python and
   *  TypeScript. */
  codeOf(spec: TimeseriesSpec): TsCodeOf;
}

/** A source's bound as the database answered it (timeseries-query.ts boundsQuery): its one row's value, null for none. */
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
  /** While measuring: a drag across the chart, its ends in order, `done` once let go (timeseries-render.ts). */
  readonly onSpan?: (from: string, to: string, done: boolean) => void;
  /** While drawing: a click in a pane, its date and its value on the pane's axis (timeseries-render.ts). */
  readonly onPoint?: (pane: number, date: string, value: number) => void;
}

/** What a spec's queries depend on: when it changes, the database is asked again; else the drawing alone changes. */
function queryKey(ts: TimeseriesSpec): string {
  return JSON.stringify({
    sources: ts.sources, grain: ts.grain ?? null, align: ts.align ?? null, compare: ts.view?.compare ?? null,
    series: ts.series.map((s) => ({ id: s.id, expr: s.expr, split: s.split ?? null })),
    // marks from grids' rows are asked too (the Phase 6 review's 1: marks added once drawn were never asked)
    marks: (ts.annotations ?? []).filter((a) => a.kind === 'marks').map(marksKey),
    // and each trend projected (Phase 5)
    projections: (ts.annotations ?? []).filter((a) => a.kind === 'projection').map(projectionKey),
  });
}

/** A projection's answer kept by what it asks -- its series, its window, how far ahead -- as marks' are by what they are;
 *  its label is the drawing's alone (the Phase 5 review's notes: a label renamed asked every series again). */
const projectionKey = (p: TsProjection): string => JSON.stringify([p.series, p.window ?? null, p.ahead ?? null]);

/** Marks' answers kept by what they are, never by their place: an annotation before them taken off leaves them theirs. */
const marksKey = (marks: TsMarks): string => JSON.stringify(marks);

/** What a tile's view asks (the next phases' 3): its mode, its series, its settings -- and the dates shown, for the views
 *  of them (a scatter's, a histogram's). When it changes, the view is asked again; '' along time (no view to ask). */
function viewKey(ts: TimeseriesSpec, window: TsWindow): string {
  const v = ts.view;
  if (v?.mode === undefined) return '';
  const shown = v.mode === 'scatter' || v.mode === 'histogram';
  return JSON.stringify({
    query: queryKey(ts), mode: v.mode, of: v.of ?? null, dates: v.dates ?? null, years: v.years ?? null, bins: v.bins ?? null, window: shown ? window : null,
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

/** A parameter's slider: from its least to its most -- its bounds where it has them, else to four times the larger of
 *  its default and its value (a number of neither bound as far below 0) -- a step of one for a count, of a hundredth of
 *  the span for a number; a bound it must be above or below kept out of reach. */
function sliderRange(p: TsParam, at: number): readonly [number, number, number] {
  const count = p.kind !== 'number';
  const most = p.max ?? p.below ?? Math.max(count ? 10 : 1, 4 * Math.max(Math.abs(Number(p.default)), Math.abs(at)));
  // a number of neither end told (a filter's value, a power): either side of 0, as far down as up (the Phase 2 review's
  // 9: from 0, a filter's -0.02 went no lower)
  const open = !count && p.min === undefined && p.above === undefined && p.max === undefined && p.below === undefined;
  const least = p.min ?? p.above ?? (count ? 1 : open ? -most : 0);
  const step = count ? 1 : Number(((most - least) / 100).toPrecision(3));
  const lo = p.above !== undefined && p.min === undefined ? least + step : least;
  const hi = p.below !== undefined && p.max === undefined ? most - step : most;
  return [Math.min(lo, at), Math.max(hi, at), step];
}

/** A tile's title as a file's name: the characters a file system refuses put aside. */
function fileName(title: string): string {
  return title.replace(/[\\/:*?"<>|\u0000-\u001f]+/g, ' ').trim() || 'timeseries';
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
/** How a tile draws its series (timeseries-spec.ts TsMode), in words. */
const MODES: readonly (readonly [string, string])[] = [
  ['', 'Over time'], ['scatter', 'Scatter'], ['seasonality', 'Seasonality'], ['histogram', 'Histogram'], ['curve', 'Curve'],
];
const STYLES: readonly (readonly [TsStyleKind, string])[] = [['line', 'Line'], ['area', 'Area'], ['bars', 'Bars'], ['step', 'Steps'], ['dots', 'Dots']];
const FAMILIES: readonly TsFamily[] = ['Returns', 'Level', 'Rolling statistics', 'Risk', 'Relationships', 'Technicals', 'Math', 'Dates'];

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
  ['drawdowns', 'Drawdowns', 'Each level\'s deepest drawdowns over the dates shown: from its peak, to its trough, to its recovery'],
  ['correlations', 'Correlations', 'Each pair of lines\' correlation over the dates shown: of a level\'s returns, of anything else as it is'],
  ['data', 'Data', 'Every line\'s value, date by date over the dates shown, as the database answered it -- copied as a spreadsheet pastes it'],
  ['calendar', 'Calendar', 'A level\'s return month by month and year by year, over every date it has: the fund sheet\'s table'],
];
type StatsTab = 'summary' | 'risk' | 'drawdowns' | 'correlations' | 'data' | 'calendar';
/** The most dates the Data tab lists (the latest): Copy takes every one. */
const MAX_DATA_ROWS = 500;
/** The most lines the correlations' matrix has: its first, in the legend's order. */
const MAX_CORRELATED = 12;

/** How two series' dates are combined, in words (timeseries-spec.ts TsAlign). */
const ALIGN_WORDS: readonly (readonly [TsAlign, string])[] = [
  ['intersect', 'The dates both have'],
  ['step', 'Every date, each its last value'],
  ['time', 'Every date, interpolated in time'],
  ['nan', 'Every date, gaps where one has none'],
  ['zero', 'Every date, 0 where one has none'],
];

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
  /** Each level series' deepest drawdowns over the dates shown, as the database answered them (asked when the Drawdowns
   *  tab is shown: lazy Work), and what it refused; undefined until asked for the answers drawn. */
  #drawdowns: Map<string, ResultTable> | undefined;
  #drawdownsFailed = new Map<string, string>();
  #drawdownsAsking = false;
  /** Asked again while an ask was in flight: its answer may be of what was drawn before, so it is asked once more. */
  #drawdownsStale = false;
  /** Each pair of lines' correlation over the dates shown, by their keys (both orders), as the database answered them;
   *  undefined until asked (the Correlations tab shown); what it refused, by its pair's words. */
  #correlations: Map<string, number | null> | undefined;
  #correlationsFailed: string[] = [];
  #correlationsAsking = false;
  #correlationsStale = false;
  /** The view's answer drawn (a mode's, the next phases' 3: timeseries-views.ts) and the series it is of, or what it
   *  refused; what it was asked for (viewKey), and what its answer is of -- drawn only when they are one. */
  #view: (Ran & { readonly of: readonly string[] }) | undefined;
  #viewFailed: string | undefined;
  #viewAsked = '';
  #viewLanded = '';
  /** What the view drawn says a reader should know (a curve's lines of earlier days: curveNotes), in the banner. */
  #viewSaid: string[] = [];
  #viewGen = 0;
  /** The Calendar tab's line, by its key (the person's pick); and its series' calendar as the database answered it, or
   *  its refusal -- asked when the tab is shown (lazy Work), of every date, so asked again only when the rows move. */
  #calendarLine: string | undefined;
  #calendar: CalendarAnswer | undefined;
  #calendarAsking = false;
  #calendarStale = false;
  #calendarGen = 0;
  /** Its editor's content, in its window while one is open. */
  #pop: HTMLElement;
  /** Its editor's window, while one is open; and what lets its listeners go when it closes. */
  #window_: HTMLElement | null = null;
  #closing: AbortController | null = null;
  /** Whether the window open is its Settings (painted again after a refused change). */
  #settings = false;
  /** What the editor open does when it is put away -- closed, or another editor opened in its window: a drag's stop
   *  forgotten and what it tried taken back (the Phase 2 review's 1). */
  #leaving: (() => void) | null = null;
  /** A drag's try drawn (a slider's, the next phases' 2): the tile as the page has it, and as tried -- the person's own
   *  look, never told; every change told is made to the page's (#edit), and the page's own spec takes its place
   *  (update). */
  #tried: { readonly from: TimeseriesSpec; readonly to: TimeseriesSpec } | undefined;
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
  /** What each marks from a grid's rows (TsMarks) answered, by what they are (marksKey), and what was refused of them:
   *  said in the banner, never drawn as no marks. */
  #marks = new Map<string, ResultTable>();
  #marksFailed = new Map<string, string>();
  /** The marks drawn last, as the lines are: what a picture draws. */
  #marked: TsMarked[] = [];
  /** What each trend projected (TsProjection) answered, by what it is (projectionKey), and what was refused of it. */
  #projections = new Map<string, ResultTable>();
  #projectionsFailed = new Map<string, string>();
  /** The projections drawn last: what a picture draws. */
  #projected: TsProjected[] = [];
  /** MEASURING (Phase 5, TradingView's ruler): a drag across the chart is a span, each line's statistics over it the
   *  database's (statsQuery, Python's statistics(start, end) by the one rule) -- its change, its %, its days. */
  #measuring = false;
  #span: TsWindow & { readonly from: string; readonly to: string } | undefined;
  #spanRows = new Map<string, ResultTable>();
  #spanFailed = new Map<string, string>();
  #spanGen = 0;
  #measureBox!: HTMLElement;
  /** DRAWING (Phase 5, TradingView's): a click in a pane is a point put there -- a line's two ends, or a note's place --
   *  told to the tile as an annotation (`annotate`), Python's annotate by the one rule. A line's first end, until its
   *  second; a note's place, until its text. */
  #drawing: 'line' | 'note' | null = null;
  #drawFrom: { readonly pane: number; readonly at: TsPoint } | undefined;
  #noteAt: { readonly pane: number; readonly at: TsPoint } | undefined;
  #drawBox!: HTMLElement;
  /** REPLAY (Phase 5, TradingView's bar replay): the window's end stepped through the dates shown, a quarter in to the
   *  last -- the chart's zoom alone: the lines as computed over every date (a z-score over all of them is not computed
   *  again to each day), shown up to each; the figures of the dates shown asked once it stops (the zoom's own settling).
   *  Stopped by a change of the tile and by a range chosen. The screen's alone: Python's by nature a loop over
   *  frame(end=...). */
  #replayTimer: ReturnType<typeof setInterval> | undefined;
  #lines: TsLine[] = [];
  /** Every question asked of the database, by its ask (timeseries-query.ts askKey): what the Query window shows. A
   *  round's asks of another tile, or of other dates, are not what is drawn: each kept with the tile it was asked of. */
  #ran = new Map<string, Asked>();
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
  /** The rounds asking the database now (#busyOn). */
  #busy = 0;

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
    // the page's own spec: a drag's try under it gone
    const tried = this.#tried !== undefined;
    this.#tried = undefined;
    if (!tried && JSON.stringify(spec) === JSON.stringify(this.#spec)) return;
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
    this.#stopReplay();
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
    const before = this.#spec;
    this.#spec = spec;
    // a replay is of the tile as it was (the Phase 5 review's notes): stopped where it is
    this.#stopReplay();
    this.#paintBar();
    // what drawing asks for, as the tile is now (a compare put on, a view chosen)
    if (this.#drawing !== null) this.#paintDraw();
    // the span measured, of the tile as it is now (the Phase 5 review's 5): put away in a view (no dates), measured again
    // where what its figures read changed, its figures of the tile before never painted over the lines of this one
    if (this.#span !== undefined) {
      if (spec.view?.mode !== undefined) {
        this.#span = undefined;
        this.#spanGen++;
        this.#paintMeasure();
      } else if (queryKey(spec) !== queryKey(before) || JSON.stringify(spec.series.map((x) => x.hidden ?? false)) !== JSON.stringify(before.series.map((x) => x.hidden ?? false))) {
        void this.#measured(this.#span.from, this.#span.to);
      }
    }
    if (queryKey(spec) !== this.#asked) void this.#fetch();
    else {
      // the drawing alone changed (a line's style, its pane, hidden; an annotation; the legend): drawn from the rows it
      // has (D4) -- and the window moved, what reads its first date asked again, and the statistics; the view's own
      // settings moved (its mode, its series), the view asked again alone
      void this.#draw();
      if (!moved && viewKey(spec, this.#window()) !== this.#viewAsked) void this.#viewed();
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

  /** A query of the sources `aliases` run where they run: of one grid, by that grid's runner; of several, over one model
   *  of their tables (the host's together, opened once a round). */
  async #runOn(aliases: readonly string[], make: (inputs: TsInputs) => TsQuery, signal: AbortSignal, opened: Opened): Promise<Ran> {
    const on = await this.#inputsOn(aliases, opened);
    const query = make(on.inputs);
    const { rows, sql } = await on.run(query.query, signal);
    return { query, table: rows, sql };
  }

  /** What a query of the sources `aliases` is written over, and where it runs: of one grid, that grid's own input and
   *  runner; of several, their relations over one model of their tables (the host's together, opened once a round). */
  async #inputsOn(aliases: readonly string[], opened: Opened): Promise<{ readonly inputs: TsInputs; readonly run: (query: Lambda, signal: AbortSignal) => Promise<RunOutcome> }> {
    const spec = this.#spec;
    if (aliases.length === 1) {
      const alias = aliases[0]!;
      const own = this.#input(alias);
      if (!own) throw new Error(`source ${alias}'s grid is not on the page`);
      return { inputs: new Map([[alias, own.input]]), run: (query, signal) => own.grid.run(query, own.grid.snapshot, signal) };
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
    return { inputs, run: (query) => together.run(query) };
  }

  /**
   * ONE OF THE TILE'S QUESTIONS ASKED (timeseries-query.ts tileAsks): its query written by the one function Python's
   * Timeseries.queries writes it by (askQuery) over the dates `window`, run where it runs, and kept -- as it was sent,
   * with the SQL the database ran -- for the Query window.
   */
  async #runAsk(spec: TimeseriesSpec, ask: TsAskOf, window: TsWindow, signal: AbortSignal, opened: Opened): Promise<Ran> {
    const ran = await this.#runOn(askSources(spec, ask), (inputs) => askQuery(inputs, spec, ask, window), signal, opened);
    this.#ran.set(askKey(ask), { spec, window, query: ran.query.query, sql: ran.sql });
    return ran;
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
    // a zoom, else the range's dates by the rule Python's tables read too (timeseries-query.ts rangeWindow)
    return this.#zoom ?? rangeWindow(this.#spec.range, this.#bounds.values());
  }

  /** Every series asked again: the bounds, then the series, then the statistics -- the grids a combination reads opened
   *  together once for all of them. */
  async #fetch(): Promise<void> {
    const opened: Opened = new Map();
    // busy until its statistics are answered too: a refused one is said by then (the second audit's G notes: a browser
    // check that waited for the drawing alone met none)
    const done = this.#busyOn();
    try {
      await this.#fetchOver(opened);
    } finally {
      putAway(opened);
      done();
    }
  }

  /**
   * THE CHART BUSY (its aria-busy) while a round asks the database: each round its own, the last done clearing it (the
   * Phase 3 review's 5: a round superseded left it set, and another's end cleared it while one still asked). What ends
   * this round's.
   */
  #busyOn(): () => void {
    this.#busy += 1;
    this.#canvas.setAttribute('aria-busy', 'true');
    let done = false;
    return () => {
      if (done) return;
      done = true;
      this.#busy -= 1;
      if (this.#busy === 0) this.#canvas.removeAttribute('aria-busy');
    };
  }

  async #fetchOver(opened: Opened): Promise<void> {
    const gen = ++this.#gen;
    const spec = this.#spec;
    this.#asked = queryKey(spec);
    const problems = timeseriesProblems(spec, this.#columns(spec));
    this.#problems = problems;
    const signal = new AbortController().signal;
    // the marks from grids' rows: each its one grid's, as a source's bounds are, asked beside them (no window reads them)
    const marks = new Map<string, ResultTable>();
    const marksFailed = new Map<string, string>();
    const marksAsked = Promise.all((spec.annotations ?? []).map(async (a, i) => {
      if (a.kind !== 'marks') return;
      const own = this.#input(a.source);
      if (!own) {
        marksFailed.set(marksKey(a), `source ${a.source}'s grid is not on the page`);
        return;
      }
      try {
        const query = marksQuery(new Map([[a.source, own.input]]), spec, a);
        const { rows, sql } = await own.grid.run(query, own.grid.snapshot, signal);
        marks.set(marksKey(a), rows);
        this.#ran.set(askKey({ kind: 'marks', marks: i, source: a.source }), { spec, window: {}, query, sql });
      } catch (e) {
        marksFailed.set(marksKey(a), e instanceof Error ? e.message : String(e));
      }
    }));
    // the bounds of each source on the page (its range buttons' starts, the database's)
    const bounds = new Map<string, ResultTable>();
    const boundsFailed = new Map<string, string>();
    await Promise.all(spec.sources.map(async (s) => {
      const own = this.#input(s.alias);
      if (!own) return;
      try {
        const query = boundsQuery(own.input, s);
        const { rows, sql } = await own.grid.run(query, own.grid.snapshot, signal);
        bounds.set(s.alias, rows);
        this.#ran.set(askKey({ kind: 'bounds', source: s.alias }), { spec, window: {}, query, sql });
      } catch (e) {
        // its dates refused: a range from the latest date cannot be found from them -- said (#rangeProblems)
        boundsFailed.set(s.alias, e instanceof Error ? e.message : String(e));
      }
    }));
    await marksAsked;
    if (gen !== this.#gen || this.#disposed) return;
    this.#bounds = bounds;
    this.#boundsFailed = boundsFailed;
    this.#marks = marks;
    this.#marksFailed = marksFailed;
    const window = this.#window();
    const blocked = (s: TsSeries): string | undefined => this.#blocked(s);
    const results = new Map<string, TsResult>();
    const failed = new Map<string, string>();
    // each trend projected, asked beside the series (tileAsks: none under a compare, which the drawing says)
    const projections = new Map<string, ResultTable>();
    const projectionsFailed = new Map<string, string>();
    const projectionsAsked = this.#askProjections(spec, window, signal, opened, projections, projectionsFailed, () => true);
    await Promise.all(spec.series.map(async (s) => {
      const why = blocked(s);
      if (why !== undefined) {
        failed.set(s.id, why);
        return;
      }
      try {
        const { sql: _sql, ...r } = answered(await this.#runAsk(spec, { kind: 'series', series: s.id }, window, signal, opened));
        results.set(s.id, r);
      } catch (e) {
        failed.set(s.id, e instanceof Error ? e.message : String(e));
      }
    }));
    await projectionsAsked;
    if (gen !== this.#gen || this.#disposed) return;
    this.#results = results;
    this.#failed = failed;
    this.#projections = projections;
    this.#projectionsFailed = projectionsFailed;
    // the rows moved: the calendar asked again when its tab is shown
    this.#calendar = undefined;
    this.#calendarGen++;
    await this.#fetchView(opened);
    if (gen !== this.#gen || this.#disposed) return;
    await this.#draw();
    await this.#statistics(gen, opened);
  }

  /** Each trend projected that `which` picks, asked (projectionQuery, over the window's first date where its series reads
   *  one -- an index's, a change's -- as its line is drawn: the Phase 5 review's 1), its answer or its refusal kept by
   *  what it asks. None under a compare (tileAsks: the drawing says so). */
  async #askProjections(spec: TimeseriesSpec, window: TsWindow, signal: AbortSignal, opened: Opened, projections: Map<string, ResultTable>,
    failed: Map<string, string>, which: (s: TsSeries) => boolean): Promise<void> {
    if (spec.view?.compare !== undefined) return;
    await Promise.all((spec.annotations ?? []).map(async (a, i) => {
      if (a.kind !== 'projection') return;
      const s = spec.series.find((x) => x.id === a.series);
      if (s !== undefined && !which(s)) return;
      const why = s === undefined ? `the tile has no series ${a.series}` : this.#blocked(s);
      if (why !== undefined) {
        failed.set(projectionKey(a), why);
        return;
      }
      try {
        projections.set(projectionKey(a), (await this.#runAsk(spec, { kind: 'projection', projection: i, series: a.series }, window, signal, opened)).table);
        failed.delete(projectionKey(a));
      } catch (e) {
        projections.delete(projectionKey(a));
        failed.set(projectionKey(a), e instanceof Error ? e.message : String(e));
      }
    }));
  }

  /** The window moved (a zoom, a range): the series that read its first date asked again, and the statistics. */
  async #windowed(): Promise<void> {
    const opened: Opened = new Map();
    const done = this.#busyOn();
    try {
      await this.#windowedOver(opened);
    } finally {
      putAway(opened);
      done();
    }
  }

  async #windowedOver(opened: Opened): Promise<void> {
    const gen = ++this.#gen;
    const spec = this.#spec;
    const window = this.#window();
    const signal = new AbortController().signal;
    const anchored = spec.series.filter((s) => this.#results.has(s.id) && readsAnchor(spec, s));
    // a view of the dates shown asked again for the dates shown now (a scatter's, a histogram's)
    const viewMoved = viewKey(spec, window) !== this.#viewAsked;
    if (viewMoved) await this.#fetchView(opened);
    if (gen !== this.#gen || this.#disposed) return;
    if (anchored.length > 0) {
      // and each projection of such a series: fit on its values from the first date shown, as its line is drawn
      const projections = new Map(this.#projections);
      const projectionsFailed = new Map(this.#projectionsFailed);
      const projected = this.#askProjections(spec, window, signal, opened, projections, projectionsFailed, (s) => readsAnchor(spec, s));
      const again = await Promise.all(anchored.map(async (s) => {
        try {
          return [s.id, answered(await this.#runAsk(spec, { kind: 'series', series: s.id }, window, signal, opened))] as const;
        } catch (e) {
          this.#failed.set(s.id, e instanceof Error ? e.message : String(e));
          return undefined;
        }
      }));
      await projected;
      if (gen !== this.#gen || this.#disposed) return;
      this.#projections = projections;
      this.#projectionsFailed = projectionsFailed;
      for (const r of again) {
        if (!r) continue;
        const { sql: _sql, ...answer } = r[1];
        this.#results.set(r[0], answer);
      }
      await this.#draw();
    } else if (viewMoved) {
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
    // a band's and a condition's none: drawn, never figures of their own
    await Promise.all(spec.series.filter((s) => this.#results.has(s.id) && !this.#results.get(s.id)!.query.band && !this.#results.get(s.id)!.query.shade).map(async (s) => {
      try {
        rows.set(s.id, (await this.#runAsk(spec, { kind: 'statistics', series: s.id }, window, signal, opened)).table);
      } catch (e) {
        // refused: said on its row and in the banner, never as figures missing
        failed.set(s.id, e instanceof Error ? e.message : String(e));
      }
    }));
    if (gen !== this.#gen || this.#disposed) return;
    this.#statRows = rows;
    this.#statsFailed = failed;
    this.#statsAsked = true;
    // the dates shown or the rows moved: the drawdowns and the correlations too, asked again when their tab is shown
    this.#drawdowns = undefined;
    this.#correlations = undefined;
    this.#paintLegend();
    this.#paintStats();
    this.#paintBanner();
  }

  /** A series' problem that stops it being asked (a problem of the tile's stops them all), in its words. */
  #blocked(s: TsSeries): string | undefined {
    const problems = this.#problems;
    return problems.find((p) => !p.startsWith('Series ')) ?? problems.find((p) => p.startsWith(`Series ${s.id} `));
  }

  /**
   * WHAT THE VIEW ASKS (the next phases' 3): the series it draws -- by the one rule Python's views read
   * (timeseries-query.ts viewSeries) -- and its query, or why there is none to draw, in words.
   */
  #viewAsk(spec: TimeseriesSpec): { readonly of: readonly TsSeries[]; readonly ask: TsAskOf } | string {
    const mode = spec.view?.mode;
    const picked = viewSeries(spec);
    const named = (id: string): TsSeries => {
      const s = spec.series.find((x) => x.id === id);
      if (!s) throw new Error(`the view reads series ${id}, which the tile has not`);
      return s;
    };
    switch (mode) {
      case undefined:
        throw new Error('a tile drawn along time asks no view');
      case 'scatter': {
        if (picked.scatter === undefined) return 'A scatter is of two series of one line: plot another, or pick them in Settings.';
        const [x, y] = [named(picked.scatter[0]), named(picked.scatter[1])];
        return { of: [x, y], ask: { kind: 'scatter', series: x.id, with: y.id } };
      }
      case 'seasonality':
      case 'histogram': {
        if (picked.one === undefined) return `A ${mode} is of a series of one line: plot one, or read one instrument of a split (close[SPX]).`;
        const one = named(picked.one);
        return { of: [one], ask: { kind: mode, series: one.id } };
      }
      case 'curve': {
        if (picked.curve === undefined) return 'A curve is of a series split by a column (its tenors): split one in its editor.';
        const one = named(picked.curve);
        return { of: [one], ask: { kind: 'curve', series: one.id } };
      }
    }
  }

  /**
   * THE VIEW ASKED (a mode's): its answer kept, or its refusal -- said in the banner, never drawn as nothing. Along time,
   * none kept. `opened`: the round's grids opened together, else its own. Whether its answer is the latest asked (a
   * later ask's lands instead).
   */
  async #fetchView(opened?: Opened): Promise<boolean> {
    const spec = this.#spec;
    const window = this.#window();
    const key = viewKey(spec, window);
    const gen = ++this.#viewGen;
    this.#viewAsked = key;
    if (key === '') {
      this.#view = undefined;
      this.#viewFailed = undefined;
      this.#viewLanded = '';
      return true;
    }
    let view: (Ran & { readonly of: readonly string[] }) | undefined;
    let failed: string | undefined;
    const own: Opened = opened ?? new Map();
    try {
      const ask = this.#viewAsk(spec);
      if (typeof ask === 'string') failed = ask;
      else {
        failed = ask.of.map((s) => this.#blocked(s) ?? this.#failed.get(s.id)).find((w) => w !== undefined);
        if (failed === undefined) {
          view = { ...await this.#runAsk(spec, ask.ask, window, new AbortController().signal, own), of: ask.of.map((s) => s.id) };
        }
      }
    } catch (e) {
      failed = e instanceof Error ? e.message : String(e);
    } finally {
      if (opened === undefined) putAway(own);
    }
    if (gen !== this.#viewGen || this.#disposed) return false;
    this.#view = view;
    this.#viewFailed = failed;
    this.#viewLanded = key;
    return true;
  }

  /** The view's own settings moved (its mode, its series, its years): it alone asked again, and drawn. */
  async #viewed(): Promise<void> {
    const done = this.#busyOn();
    try {
      const landed = await this.#fetchView();
      if (!landed || this.#disposed) return;
      await this.#draw();
    } finally {
      done();
    }
  }

  /** The view as an ECharts option, of its answer (timeseries-views.ts), `height` tall, a picture's `title` above it; its
   *  description for a reader of the screen. Undefined with no answer to draw. */
  #viewOption(height: number, title?: string): ViewDrawn | undefined {
    const spec = this.#spec;
    const mode = spec.view?.mode;
    const v = this.#view;
    if (mode === undefined || v === undefined) return undefined;
    const named = (id: string | undefined): string => {
      const s = spec.series.find((x) => x.id === id);
      return s ? seriesName(s) : String(id);
    };
    const line = this.#lines.find((l) => l.series.id === v.of[0]);
    const format = line && line.unit === v.query.unit ? this.#formatOf(line) : undefined;
    const ctx: TsViewContext = {
      theme: themeOf(this.#root), height, ...(title !== undefined ? { title } : {}), format: (n) => valueText({ unit: v.query.unit }, n, format),
    };
    const about = v.of.map((id) => named(id)).join(' against ');
    const described = describeView(mode, about, v.table.rowCount);
    switch (mode) {
      case 'scatter': return { option: scatterOption(spec, v.table, { x: named(v.of[0]), y: named(v.of[1]) }, ctx), described };
      case 'seasonality': return { option: seasonalityOption(spec, v.table, named(v.of[0]), spec.grain, ctx), described };
      case 'histogram': return { option: histogramOption(spec, v.table, named(v.of[0]), ctx), described };
      case 'curve': return { option: curveOption(spec, v.table, spec.view?.dates ?? TS_CURVE_DATES, named(v.of[0]), ctx), described };
    }
  }

  // -- drawing -------------------------------------------------------------------------------------------------------

  /** The chart's renderer, made the first time it is needed. */
  async #mount(): Promise<MountedTimeseries | null> {
    if (this.#chart) return this.#chart;
    this.#mounting ??= (async () => {
      const on: MountOn = {
        onZoom: (from, to) => this.#zoomed(from, to),
        // a date of the time view's (a view's x is no date)
        onDate: (date) => {
          // while measuring or drawing, a double-click is two clicks of theirs, no event (the Phase 5 review's notes)
          if (this.#spec.view?.mode === undefined && !this.#measuring && this.#drawing === null) this.#eventAt(date);
        },
        onResize: () => void this.#draw(),
        onSpan: (from, to, done) => this.#spanned(from, to, done),
        onPoint: (pane, date, value) => this.#pointed(pane, date, value),
      };
      let make = this.#host.mount;
      if (make === undefined) {
        // the drawing, over the charts' ECharts (chart-render.ts canvasChart): both fetched together
        const [{ mountTimeseries }, { canvasChart, svgChart }] = await Promise.all([
          lazily(() => import('../timeseries-render.ts')), lazily(() => import('../chart-render.ts'))]);
        make = (el, o) => mountTimeseries(el, o, canvasChart, svgChart);
      }
      if (this.#disposed) return null;
      this.#chart = make(this.#canvas, on);
      // a chart made again (candles' kinds, or a mode pressed before the first was made) measures and draws as the
      // tile does (the Phase 5 review's 4: a drag neither panned nor measured)
      this.#chart.measure(this.#measuring);
      this.#chart.draw(this.#drawing !== null);
      return this.#chart;
    })();
    return this.#mounting;
  }

  /** Marks' values in their grid's format: their source's grid's formatter for the column they are drawn at. */
  #formatOfMarks(marks: TsMarks): TsFormat | undefined {
    const column = marks.at;
    const src = this.#spec.sources.find((s) => s.alias === marks.source);
    const grid = src ? this.#host.grid(src.cube) : undefined;
    if (column === undefined || !grid) return undefined;
    const type = rowColumns(grid.snapshot).find((c) => c.name === column)?.type;
    return (v) => grid.format(v, column, type);
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
    const marked: TsMarked[] = [];
    for (const [i, a] of (spec.annotations ?? []).entries()) {
      const table = a.kind === 'marks' ? this.#marks.get(marksKey(a)) : undefined;
      if (table === undefined) continue;
      const read = tsMarks(spec, i, table, theme);
      marked.push(read.marked);
      notes.push(...read.notes);
    }
    const projected: TsProjected[] = [];
    for (const [i, a] of (spec.annotations ?? []).entries()) {
      const table = a.kind === 'projection' ? this.#projections.get(projectionKey(a)) : undefined;
      if (table === undefined) continue;
      const read = tsProjected(spec, i, table);
      if (read.projected) projected.push(read.projected);
      notes.push(...read.notes);
    }
    notes.push(...marksUnplaced(spec, lines, marked), ...drawingsUnplaced(spec, lines), ...projectionsUnplaced(spec, lines));
    this.#lines = lines;
    this.#marked = marked;
    this.#projected = projected;
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
    const height = chart.height() || this.#canvas.clientHeight || 420;
    const mode = spec.view?.mode;
    if (mode !== undefined) {
      // a view: drawn when its answer is of what is asked now (until then, what was drawn stays)
      if (this.#viewLanded !== viewKey(spec, this.#window())) return;
      let drawn: ViewDrawn | undefined;
      try {
        drawn = this.#viewOption(height);
        this.#viewSaid = mode === 'curve' && this.#view !== undefined ? curveNotes(this.#view.table, spec.view?.dates ?? TS_CURVE_DATES) : [];
      } catch (e) {
        this.#viewFailed = e instanceof Error ? e.message : String(e);
        this.#viewSaid = [];
      }
      this.#paintBanner();
      chart.show(drawn?.option ?? { series: [] }, drawn?.described ?? `No ${mode}: ${this.#viewFailed ?? 'nothing to draw'}`);
      return;
    }
    const option = timeseriesOption(spec, lines, {
      theme, formatOf: (l) => this.#formatOf(l), formatOfMarks: (m) => this.#formatOfMarks(m), view: this.#window(), height, marks: marked,
      projections: this.#projected,
      ...(this.#measuring ? { measuring: true } : {}), ...(this.#span !== undefined ? { span: this.#span } : {}),
      ...(this.#drawing !== null ? { drawing: true } : {}), ...(this.#drawFrom !== undefined ? { drawFrom: this.#drawFrom } : {}),
    });
    chart.show(option, describeTimeseries(spec, lines));
  }

  /** Measuring on or off: a drag is a span measured, not a pan; off, the span and its figures put away. Drawing, off. */
  #measure(on: boolean): void {
    if (on && this.#drawing !== null) this.#draws(null);
    this.#measuring = on;
    this.#chart?.measure(on);
    if (!on) {
      this.#span = undefined;
      this.#spanGen++;
      this.#paintMeasure();
    }
    this.#paintBar();
    void this.#draw();
  }

  /** A span dragged: shown as it is dragged, and once let go each line's statistics over it asked of the database. */
  #spanned(from: string, to: string, done: boolean): void {
    // a view's x is no date (the Phase 5 review's 5): measured along time alone
    if (!this.#measuring || this.#spec.view?.mode !== undefined) return;
    this.#span = { from, to };
    if (!done) {
      this.#paintMeasure(true);
      return;
    }
    void this.#draw();
    void this.#measured(from, to);
  }

  /** Each line's statistics over the span -- statsQuery, by the rule the statistics panel reads its window by -- the
   *  last span's answers alone. */
  async #measured(from: string, to: string): Promise<void> {
    const gen = ++this.#spanGen;
    const spec = this.#spec;
    const span: TsWindow = { from, to };
    const opened: Opened = new Map();
    const rows = new Map<string, ResultTable>();
    const failed = new Map<string, string>();
    this.#paintMeasure(true);
    try {
      // the lines shown: a hidden one is in no figure of the box
      await Promise.all(tableSeries(spec).statistics.filter((id) => spec.series.find((x) => x.id === id)?.hidden !== true).map(async (id) => {
        const s = spec.series.find((x) => x.id === id)!;
        try {
          const ran = await this.#runOn(askSources(spec, { kind: 'statistics', series: id }), (inputs) => askQuery(inputs, spec, { kind: 'statistics', series: id }, span),
            new AbortController().signal, opened);
          rows.set(s.id, ran.table);
        } catch (e) {
          failed.set(s.id, e instanceof Error ? e.message : String(e));
        }
      }));
    } finally {
      putAway(opened);
    }
    if (gen !== this.#spanGen || this.#disposed) return;
    this.#spanRows = rows;
    this.#spanFailed = failed;
    this.#paintMeasure();
  }

  /** The span's figures under the bar: each line's first and last value, its change and %, its days and observations,
   *  each the database's, in its line's format; asking, said; refused, said in its words. */
  #paintMeasure(asking = false): void {
    const doc = this.#doc;
    const span = this.#span;
    this.#measureBox.hidden = span === undefined;
    if (span === undefined) {
      this.#measureBox.replaceChildren();
      return;
    }
    const head = el(doc, 'div', 'dc-ts-measure-head', `${span.from} → ${span.to}`);
    const rows: HTMLElement[] = [];
    if (asking) {
      rows.push(el(doc, 'div', 'dc-ts-hint', 'Measuring…'));
    } else {
      for (const l of this.#lines.filter((x) => !x.series.hidden && this.#spanRows.has(x.series.id))) {
        const at = (stat: TsStat): number | null => {
          const v = this.#statValue(l, stat, this.#spanRows);
          return typeof v === 'string' && /^\d{4}-/.test(v) ? null : toNumber(v as Scalar);
        };
        const f = this.#formatOf(l);
        const first = at('first');
        const last = at('last');
        const change = at('change');
        const pct = at('pct');
        const days = at('days');
        const count = at('count');
        const text = first === null || last === null ? 'no values in this span'
          : `${valueText(l, first, f)} → ${valueText(l, last, f)} · ${change === null ? '—' : `${change >= 0 ? '+' : ''}${valueText(l, change, f)}`}`
            + `${pct === null ? '' : ` (${pct >= 0 ? '+' : ''}${(pct * 100).toLocaleString(UI_LOCALE, { maximumFractionDigits: 2 })}%)`}`
            + ` · ${days === null ? '—' : days.toLocaleString(UI_LOCALE)} days, ${count === null ? '—' : count.toLocaleString(UI_LOCALE)} obs`;
        const row = el(doc, 'div', 'dc-ts-measure-row');
        row.append(el(doc, 'span', 'dc-ts-measure-name', l.name), el(doc, 'span', undefined, text));
        rows.push(row);
      }
      for (const [id, why] of this.#spanFailed) rows.push(el(doc, 'div', 'dc-ts-hint dc-ts-bad', `Series ${id} was not measured: ${why}`));
    }
    const actions = el(doc, 'div', 'dc-ts-row');
    actions.append(
      button(doc, 'Zoom to it', 'Show these dates alone', () => {
        this.#chart?.zoomTo(span.from, span.to);
        this.#zoomed(span.from, span.to);
      }),
      button(doc, 'Clear', 'Measure another span', () => {
        this.#span = undefined;
        this.#spanGen++;
        this.#paintMeasure();
        void this.#draw();
      }));
    this.#measureBox.replaceChildren(head, ...rows, actions);
  }

  /** Drawing a line or a note, or neither: a click is a point put in a pane, not a pan; off, a point half made put away.
   *  Measuring, off. */
  #draws(kind: 'line' | 'note' | null): void {
    // a point is a day (TsPoint): a tile read by the time of day has none (the Phase 5 review's 6), said
    if (kind !== null && this.#byTime()) {
      this.#say(['Drawing is by the day: this tile reads its dates by the time of day -- give it a frequency (Daily) to draw on it.'], true);
      return;
    }
    if (kind !== null && this.#measuring) this.#measure(false);
    this.#drawing = kind;
    this.#drawFrom = undefined;
    this.#noteAt = undefined;
    this.#chart?.draw(kind !== null);
    this.#paintDraw();
    this.#paintBar();
    void this.#draw();
  }

  /** Whether the tile reads its dates by the time of day: no frequency, a source's date a date-time. */
  #byTime(): boolean {
    const spec = this.#spec;
    if (spec.grain !== undefined) return false;
    const columns = this.#columns(spec);
    return spec.sources.some((src) => hasTimeOfDay(columns.get(src.alias)?.find((c) => c.name === src.date)?.type));
  }

  /** A point clicked while drawing: a line's first end, then its second -- the line told -- or a note's place, its text
   *  asked beside it. Its value as the pane draws it, to six figures (a hand's, not the database's). */
  #pointed(pane: number, date: string, value: number): void {
    if (this.#drawing === null || this.#spec.view?.mode !== undefined) return;
    // a pane whose drawings have no line to be on puts none (drawingsLine), said
    const on = drawingsLine(this.#spec, this.#lines, pane);
    if ('why' in on) {
      this.#say([on.why], true);
      return;
    }
    const at: TsPoint = { date, value: Number(value.toPrecision(6)) };
    const where = pane === 0 ? {} : { pane };
    if (this.#drawing === 'note') {
      this.#noteAt = { pane, at };
      this.#paintDraw();
      return;
    }
    const from = this.#drawFrom;
    // a line's ends in one pane: a second click in another starts the line there
    if (from === undefined || from.pane !== pane) {
      this.#drawFrom = { pane, at };
      this.#paintDraw();
      void this.#draw();
      return;
    }
    this.#drawFrom = undefined;
    this.#paintDraw();
    // the same point twice is no line
    if (from.at.date === at.date && from.at.value === at.value) {
      void this.#draw();
      return;
    }
    const [a, b] = from.at.date <= at.date ? [from.at, at] : [at, from.at];
    if (!this.#edit({ kind: 'annotate', annotation: { kind: 'line', from: a, to: b, ...where } })) void this.#draw();
  }

  /** What drawing asks for, under the bar: which kind, and what a click does next; a note's text once its place is put. */
  #paintDraw(): void {
    const doc = this.#doc;
    const kind = this.#drawing;
    this.#drawBox.hidden = kind === null;
    if (kind === null) {
      this.#drawBox.replaceChildren();
      return;
    }
    const choose = select(doc, [['line', 'A line'], ['note', 'A note']], kind, (v) => this.#draws(v as 'line' | 'note'), 'Draw');
    const said = this.#spec.view?.mode !== undefined ? `A ${this.#spec.view.mode} draws no drawings: drawn along time alone.`
      : this.#spec.view?.compare !== undefined ? 'Drawings are put on the lines\' own values: Compare off to draw.'
        : kind === 'line' ? (this.#drawFrom === undefined ? 'Click where the line starts.' : `From ${this.#drawFrom.at.date}: click where it ends.`)
          : this.#noteAt === undefined ? 'Click where the note goes.' : `At ${this.#noteAt.at.date}:`;
    // each pane drawn whose drawings have no line to be on, and why (drawingsLine)
    const panes = [...new Set(this.#lines.filter((l) => !l.shades).map((l) => l.series.pane ?? 0))].sort((a, b) => a - b);
    const nowhere = this.#spec.view?.compare !== undefined ? [] : panes.flatMap((p) => {
      const on = drawingsLine(this.#spec, this.#lines, p);
      return 'why' in on ? [on.why] : [];
    });
    const parts: HTMLElement[] = [choose, el(doc, 'span', 'dc-ts-hint', [said, ...nowhere].join(' '))];
    const noteAt = this.#noteAt;
    if (kind === 'note' && noteAt !== undefined) {
      const text = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Note', 'aria-label': 'Note' });
      const put = (): void => {
        const words = text.value.trim();
        if (words === '') return;
        this.#noteAt = undefined;
        this.#paintDraw();
        this.#edit({ kind: 'annotate', annotation: { kind: 'note', at: noteAt.at, text: words, ...(noteAt.pane === 0 ? {} : { pane: noteAt.pane }) } });
      };
      text.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') put();
      });
      parts.push(text, button(doc, 'Add', 'Pin the note there', put), button(doc, 'Cancel', 'Put no note there', () => {
        this.#noteAt = undefined;
        this.#paintDraw();
      }));
      queueMicrotask(() => text.focus());
    }
    parts.push(button(doc, 'Done', 'Stop drawing', () => this.#draws(null)));
    const row = el(doc, 'div', 'dc-ts-row');
    row.append(...parts);
    this.#drawBox.replaceChildren(row);
  }

  /** The replay started, or stopped where it is: from a quarter into the dates the lines show to their last, a step a
   *  tick. Under two dates shown, nothing to replay, said. */
  #replay(): void {
    if (this.#replayTimer !== undefined) {
      this.#stopReplay();
      return;
    }
    const w = this.#window();
    const dates = [...new Set(this.#lines.filter((l) => !l.series.hidden && !l.shades).flatMap((l) => l.points.map(([d]) => d)))]
      .filter((d) => (w.from === undefined || d >= w.from) && (w.to === undefined || d <= w.to)).sort();
    if (dates.length < 2) {
      this.#say(['Nothing to replay: the lines show fewer than two dates.'], true);
      return;
    }
    const from = dates[0]!;
    let at = Math.floor(dates.length / 4);
    const step = Math.max(1, Math.ceil((dates.length - 1 - at) / REPLAY_STEPS));
    this.#chart?.zoomTo(from, dates[at]);
    this.#replayTimer = setInterval(() => {
      at = Math.min(dates.length - 1, at + step);
      // done: its end open again, so what is drawn past the last date (a projection) is shown (the Phase 5 review's 2)
      this.#chart?.zoomTo(from, at === dates.length - 1 ? undefined : dates[at]);
      if (at === dates.length - 1) this.#stopReplay();
    }, REPLAY_TICK_MS);
    this.#paintBar();
  }

  #stopReplay(): void {
    if (this.#replayTimer === undefined) return;
    clearInterval(this.#replayTimer);
    this.#replayTimer = undefined;
    if (!this.#disposed) this.#paintBar();
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
    this.#measureBox = el(doc, 'div', 'dc-ts-measure', undefined, { role: 'status', 'aria-label': 'Measured' });
    this.#measureBox.hidden = true;
    this.#drawBox = el(doc, 'div', 'dc-ts-measure dc-ts-draw', undefined, { role: 'group', 'aria-label': 'Drawing' });
    this.#drawBox.hidden = true;
    const body = el(doc, 'div', 'dc-ts-body');
    body.append(this.#canvas);
    this.#root.append(this.#bar, this.#legend, this.#banner, this.#measureBox, this.#drawBox, body, this.#stats);
    this.#paintBar();
  }

  #paintBar(): void {
    const doc = this.#doc;
    const spec = this.#spec;
    this.#ranges = el(doc, 'div', 'dc-ts-ranges', undefined, { role: 'group', 'aria-label': 'Range' });
    this.#paintRanges();
    const grain = select(doc, GRAINS, spec.grain ?? '', (v) => this.#edit({ kind: 'grain', grain: v === '' ? null : v as NonNullable<TimeseriesSpec['grain']> }), 'Frequency');
    const compare = select(doc, COMPARES, spec.view?.compare ?? '', (v) => this.#edit({ kind: 'view', view: { compare: v === '' ? null : v as 'rebase' | 'percent' } }), 'Compare');
    // how its series are drawn: along time, or one of its other views (the next phases' 3)
    const mode = select(doc, MODES, spec.view?.mode ?? '', (v) => this.#edit({ kind: 'view', view: { mode: v === '' ? null : v as TsMode } }), 'View');
    const log = button(doc, 'Log', 'The main pane on a log scale', () => this.#edit({ kind: 'pane', index: 0, log: !(this.#spec.panes?.[0]?.log ?? false) }));
    log.setAttribute('aria-pressed', String(spec.panes?.[0]?.log === true));
    const stats = button(doc, 'Stats', 'The statistics of every line over the dates shown', () => this.#edit({ kind: 'view', view: { stats: !viewSetting(this.#spec, 'stats') } }));
    stats.setAttribute('aria-pressed', String(viewSetting(spec, 'stats')));
    const add = button(doc, '+ Series', 'Plot another series: an expression, gs-quant\'s functions', () => this.#editSeries(null), 'dc-button dc-primary dc-ts-btn');
    const settings = button(doc, 'Settings', 'Sources, panes, annotations, view and export', () => this.#editSettings());
    const csv = button(doc, 'CSV', 'Download the lines as CSV (the rows the database answered)', () =>
      download(doc, 'timeseries.csv', URL.createObjectURL(new Blob([timeseriesCsv(this.#lines)], { type: 'text/csv' }))));
    const png = button(doc, 'PNG', 'Download the chart as a picture, its title, subtitle and note above it, at three times its pixels', () => void this.#export('png'));
    const svg = button(doc, 'SVG', 'Download the chart as SVG, its title, subtitle and note above it: a picture for a page or a deck', () => void this.#export('svg'));
    const swap = button(doc, 'Swap', 'One instrument for another in every series that reads it', () => this.#swap());
    const query = button(doc, 'Query', 'Each series\' query as the compiler prints it, and its SQL', () => void this.#showQueries());
    const measure = button(doc, 'Measure', 'Drag across the chart: each line\'s change, its % and the days between, the database\'s', () => this.#measure(!this.#measuring));
    measure.setAttribute('aria-pressed', String(this.#measuring));
    const replay = button(doc, this.#replayTimer === undefined ? 'Replay' : 'Stop', this.#replayTimer === undefined
      ? 'Step the chart\'s end through its dates: the lines as computed over every date, shown up to each day' : 'Stop the replay where it is', () => this.#replay());
    replay.setAttribute('aria-pressed', String(this.#replayTimer !== undefined));
    const draw = button(doc, 'Draw', 'Click the chart: a line from one point to another, or a note pinned at one', () => this.#draws(this.#drawing === null ? 'line' : null));
    draw.setAttribute('aria-pressed', String(this.#drawing !== null));
    this.#bar.replaceChildren(this.#ranges, el(doc, 'span', 'dc-ts-gap'), grain, compare, mode, log, stats, measure, draw, replay, add, swap, settings, query, csv, png, svg);
  }

  #paintRanges(): void {
    const doc = this.#doc;
    const r = this.#spec.range;
    const chosen = this.#zoom ? null : r && 'last' in r ? r.last : r ? 'custom' : 'max';
    // a seasonality's and a curve's dates their own (every year; the dates asked): the range theirs to read, not the
    // tile's -- said, never pressed to no effect (the Phase 3 review's 12)
    const mode = this.#spec.view?.mode;
    const own = mode === 'seasonality' || mode === 'curve';
    this.#ranges.replaceChildren(...TS_RANGES.map(({ word, label }) => {
      const b = button(doc, label, own ? `A ${mode} reads its own dates: ${mode === 'curve' ? 'the dates asked in Settings' : 'every year, the latest and the years before'}`
        : `Show the last ${label === 'Max' ? 'of everything' : label}`, () => this.#setRange(word), 'dc-ts-range');
      b.setAttribute('aria-pressed', String(chosen === word));
      b.disabled = own;
      return b;
    }));
    if (this.#zoom || chosen === 'custom') {
      const w = this.#window();
      this.#ranges.append(el(doc, 'span', 'dc-ts-custom', `${w.from ?? '…'} – ${w.to ?? '…'}`));
    }
  }

  #setRange(word: TsRangeWord): void {
    this.#stopReplay();
    const tried = this.#untried();
    this.#zoom = null;
    const spec = editTimeseries(this.#spec, { kind: 'range', range: { last: word } });
    // a drag's try set aside: the tile asked again as the page has it
    if (tried !== undefined) {
      this.#apply(spec);
      this.#host.setSpec(spec);
      return;
    }
    this.#range = JSON.stringify(spec.range);
    this.#spec = spec;
    this.#host.setSpec(spec);
    this.#paintRanges();
    const w = this.#window();
    if (spec.view?.mode === undefined) this.#chart?.zoomTo(w.from, w.to);
    void this.#windowed();
  }

  /**
   * THE CHART AS A PICTURE (the next phases' 2: a picture for a page or a deck): the lines as drawn now, over the dates
   * shown, drawn again off the screen with the tile's title, subtitle and note above them -- a PNG at three times its
   * pixels, or SVG -- on the page's own surface, and handed over as a file named by the tile. Refused in words.
   */
  async #export(kind: 'png' | 'svg'): Promise<void> {
    const chart = await this.#mount();
    if (!chart || this.#disposed) return;
    const width = this.#canvas.clientWidth;
    const height = chart.height() || this.#canvas.clientHeight;
    const viewing = this.#spec.view?.mode !== undefined;
    if (!(width > 0 && height > 0) || (viewing ? this.#view === undefined || this.#viewLanded !== viewKey(this.#spec, this.#window()) : this.#lines.length === 0)) {
      this.#say(['There is no chart drawn to make a picture of yet.'], true);
      return;
    }
    const title = this.#host.title();
    const theme = themeOf(this.#root);
    // the picture's panes as tall as on the screen: its heading -- its title, and a time view's legend -- above them, no
    // navigator below (the Phase 2 review's 10: the screen's heading had been counted twice)
    const tall = viewing ? height - tsHeading(this.#spec) + tsHeading(this.#spec, title) : tsPictureHeight(this.#spec, height, title);
    let option: EChartsOption;
    try {
      option = viewing ? this.#viewOption(tall, title)!.option : timeseriesOption(this.#spec, this.#lines, {
        theme, formatOf: (l) => this.#formatOf(l), formatOfMarks: (m) => this.#formatOfMarks(m), view: this.#window(), height: tall, title, picture: true, marks: this.#marked,
      });
    } catch (e) {
      this.#say([`The picture could not be made: ${e instanceof Error ? e.message : String(e)}`], true);
      return;
    }
    let url: string;
    try {
      url = await chart.exportUrl(option, kind, width, tall, theme.surface);
    } catch (e) {
      this.#say([`The picture could not be made: ${e instanceof Error ? e.message : String(e)}`], true);
      return;
    }
    download(this.#doc, `${fileName(title)}.${kind}`, url);
  }

  /**
   * SWAP AN INSTRUMENT (the next phases' 2: Bloomberg's change of security): one the tile's series read for another of
   * its source's instruments -- the database's list of them, or one typed -- in every series that reads it, by the
   * tile's one rule (timeseries-edit.ts, the swap change Python's Timeseries.swap tells too).
   */
  #swap(): void {
    const doc = this.#doc;
    const spec = this.#spec;
    const read = instrumentsOf(spec);
    if (read.length === 0) {
      this.#say(['No series reads an instrument by its source\'s instrument column (close[SPX]): there is none to swap.'], true);
      return;
    }
    const many = spec.sources.length > 1;
    const fromSel = select(doc, read.map((r, i) => [String(i), many ? `${r.alias}: ${r.value}` : r.value] as const), '0', () => paintTo(), 'Instrument');
    const toSel = el(doc, 'select', 'dc-select dc-ts-select', undefined, { 'aria-label': 'For' });
    const typed = el(doc, 'input', 'dc-input', undefined, { 'aria-label': 'Or type one', placeholder: 'Or type one', spellcheck: 'false' });
    const error = el(doc, 'div', 'dc-ts-error', undefined, { role: 'alert' });
    // each paint's own: an answer to a paint before the last dropped (the Phase 2 review's 6: both appended)
    let painted = 0;
    const paintTo = (): void => {
      const mine = ++painted;
      const r = read[Number(fromSel.value)]!;
      const src = spec.sources.find((x) => x.alias === r.alias);
      toSel.replaceChildren(option(doc, 'Pick one…', ''));
      if (src === undefined || src.key === undefined) return;
      void this.#keyValues(src, src.key).then((values) => {
        if (mine !== painted) return;
        for (const v of values) if (v !== r.value) toSel.append(option(doc, v, v));
      }, (e: unknown) => {
        if (mine !== painted) return;
        toSel.append(unpicked(doc, `Its instruments were refused: ${e instanceof Error ? e.message : String(e)}`));
      });
    };
    paintTo();
    const ok = button(doc, 'Swap', 'Read the other instrument in every series that reads this one (Enter)', () => {
      const r = read[Number(fromSel.value)]!;
      const to = typed.value.trim() !== '' ? typed.value.trim() : toSel.value;
      if (to === '') {
        error.textContent = 'Pick the instrument it is swapped for, or type one.';
        return;
      }
      if (this.#edit({ kind: 'swap', from: r.value, to, ...(many ? { alias: r.alias } : {}) })) this.#close();
    }, 'dc-button dc-primary');
    typed.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') ok.click();
    });
    const actions = el(doc, 'div', 'dc-ts-actions');
    actions.append(el(doc, 'span', 'dc-ts-gap'), button(doc, 'Cancel', 'Leave it as it was (Escape)', () => this.#close()), ok);
    this.#open('Swap an instrument', labelled(doc, 'Instrument', fromSel), labelled(doc, 'For', toSel), labelled(doc, 'Or type one', typed), error,
      el(doc, 'div', 'dc-ts-hint', 'Every series that reads it reads the other instead; a name that says it says the other.'), actions);
  }

  /**
   * THE QUERIES (the next phases' 2; every one of them, the user, 2026-10-26: "show all the pure queries used to create a
   * timeseries chart"): each question the tile asks the database, by the one rule Python's Timeseries.queries reads
   * (timeseries-query.ts tileAsks) -- each source's bounds, each series' points, the view's, then the statistics panel's
   * -- each its query as DataCube wrote it, printed by the compiler, and the SQL the database ran for it, each copied
   * with a click, and all of them at once. A question asked is shown as it was sent; one not asked yet (a tab not shown)
   * is written as it would be, and says when it is asked; one refused says why.
   */
  #showQueries(): void {
    const doc = this.#doc;
    const spec = this.#spec;
    const window = this.#window();
    const asks = tileAsks(spec);
    const opened: Opened = new Map();
    const written: { readonly ask: TsAsk; pure?: string; sql?: string }[] = asks.map((ask) => ({ ask }));
    const sections = new Map<'chart' | 'panel', HTMLElement[]>([['chart', []], ['panel', []]]);
    const jobs = asks.map(async (ask, i) => {
      const told = written[i]!;
      const kept = this.#ran.get(askKey(ask));
      const ran = kept !== undefined && kept.spec === spec ? kept : undefined;
      const refused = this.#refusalOf(ask);
      const box = el(doc, 'details', 'dc-ts-ask');
      if (ask.serves === 'chart') box.open = true;
      const status = ran !== undefined ? 'ran' : refused !== undefined ? 'refused' : 'not asked yet';
      const summary = el(doc, 'summary', 'dc-ts-ask-what');
      summary.append(el(doc, 'span', undefined, `${i + 1}. ${ask.what}`), el(doc, 'span', `dc-ts-ask-status dc-ts-ask-${status.replace(/ /g, '-')}`, status));
      const pure = el(doc, 'pre', 'dc-ts-code', 'Writing…');
      if (ran !== undefined) told.sql = ran.sql;
      const sql = el(doc, 'pre', 'dc-ts-code', ran !== undefined ? ran.sql
        : refused !== undefined ? `Refused: ${refused}` : `Not asked yet: ${TS_SERVES_WORDS[ask.serves]} asks for it when shown.`);
      const over = ran !== undefined && (ran.window.from !== undefined || ran.window.to !== undefined)
        ? el(doc, 'div', 'dc-ts-hint', `Asked of the dates ${ran.window.from ?? 'from the first'} to ${ran.window.to ?? 'the last'}.`) : undefined;
      const copies = el(doc, 'div', 'dc-ts-row');
      copies.append(
        button(doc, 'Copy Pure', 'Its query as the compiler prints it', () => {
          if (told.pure === undefined) this.#say([`Query ${i + 1} is not written: ${pure.textContent ?? ''}`], true);
          else void this.#copy(told.pure, `Query ${i + 1}'s Pure copied.`);
        }),
        button(doc, 'Copy SQL', 'The SQL the database ran for it', () => {
          if (told.sql === undefined) this.#say([`Query ${i + 1} has no SQL: ${sql.textContent ?? ''}`], true);
          else void this.#copy(told.sql, `Query ${i + 1}'s SQL copied.`);
        }));
      box.append(summary, ...(over ? [over] : []), el(doc, 'div', 'dc-ts-field-label', 'Pure'), pure, el(doc, 'div', 'dc-ts-field-label', 'SQL'), sql, copies);
      sections.get(ask.serves === 'chart' ? 'chart' : 'panel')!.push(box);
      try {
        // as it was sent, or as it would be: written by the one function every round writes it by (askQuery)
        const query = ran?.query ?? await this.#queryOf(spec, ask, window, opened);
        const grid = this.#printer(spec, ask);
        if (grid === undefined) throw new Error('its grid is not on the page');
        told.pure = await grid.print(query);
        pure.textContent = told.pure;
      } catch (e) {
        pure.textContent = `Not written: ${e instanceof Error ? e.message : String(e)}`;
      }
    });
    void Promise.allSettled(jobs).then(() => putAway(opened));
    const all = (): string => written.map((w, i) => [`// ${i + 1}. ${w.ask.what} -- for ${TS_SERVES_WORDS[w.ask.serves]}`,
      w.pure ?? `// not written`, `-- its SQL`, w.sql ?? `-- not run`].join('\n')).join('\n\n');
    const copyAll = el(doc, 'div', 'dc-ts-row');
    copyAll.append(button(doc, 'Copy all', 'Every query: its Pure and its SQL', () => void this.#copy(all(), `${asks.length} queries copied.`)));
    const queries = el(doc, 'div', 'dc-ts-code-panel');
    queries.append(
      el(doc, 'div', 'dc-ts-hint', `Every question this tile asks the database (${asks.length}): each written in Pure by DataCube, planned by `
        + 'legend-lite and run as the SQL below it. Every figure drawn is one of their answers.'),
      copyAll,
      el(doc, 'div', 'dc-ts-heading', 'Drawing the chart'), ...sections.get('chart')!,
      el(doc, 'div', 'dc-ts-heading', 'The statistics panel'), ...sections.get('panel')!);
    // and the tile as the program that builds it (the user, 2026-10-26: "how exactly to build that chart
    // programmatically from TS and python"): in each language, by the one rule Python's Timeseries.code reads
    const languages = Object.keys(CODE_WORDS) as TsCodeLanguage[];
    const panels: [string, string, HTMLElement][] = [['Queries', 'Every query it asks the database', queries],
      ['Library', 'The Pure functions its queries call by name: DataCube\'s library', this.#libraryPanel()],
      ...languages.map((language): [string, string, HTMLElement] => [CODE_WORDS[language].label, CODE_WORDS[language].title, this.#codePanel(spec, language)])];
    const tabs = el(doc, 'div', 'dc-ts-stats-tabs', undefined, { role: 'tablist' });
    const pick = (i: number): void => {
      panels.forEach(([, , panel], j) => { panel.hidden = j !== i; });
      [...tabs.children].forEach((b, j) => b.setAttribute('aria-selected', String(j === i)));
    };
    panels.forEach(([label, title], i) => {
      const b = button(doc, label, title, () => pick(i), 'dc-ts-stats-tab');
      b.setAttribute('role', 'tab');
      tabs.append(b);
    });
    pick(0);
    this.#open('Queries', tabs, ...panels.map(([, , panel]) => panel));
  }

  /** DATACUBE'S PURE LIBRARY (pure-library.ts; the user, 2026-10-26): the functions its queries call by name
   *  (`timeseries::mean`, ...), as the planner reads them after the model of a query that calls them, copied with a
   *  click. Its text is a chunk of its own (pure-library-text.ts), read as the planner reads it. */
  #libraryPanel(): HTMLElement {
    const doc = this.#doc;
    const panel = el(doc, 'div', 'dc-ts-code-panel');
    const program = el(doc, 'pre', 'dc-ts-code dc-ts-program', 'Reading…');
    let text: string | undefined;
    const copies = el(doc, 'div', 'dc-ts-row');
    copies.append(button(doc, 'Copy Library', 'The library\'s Pure', () => {
      if (text === undefined) this.#say([`The library is not read: ${program.textContent ?? ''}`], true);
      else void this.#copy(text, 'The library\'s Pure copied.');
    }));
    panel.append(
      el(doc, 'div', 'dc-ts-hint', `The functions these queries call by name (${LIBRARY_PACKAGE}::mean, ::volatility, ...): DataCube's own, written `
        + 'in Pure over any table, which legend-lite reads after the model of a query that calls them and expands into it. '
        + 'A query of your own may call them too.'),
      copies, program);
    void loadLibrary().then((library) => {
      text = library;
      program.textContent = library;
    }).catch((e: unknown) => {
      program.textContent = `Not read: ${e instanceof Error ? e.message : String(e)}`;
    });
    return panel;
  }

  /** The tile as the program that builds it in `language` (timeseries-code.ts tileCode, a chunk of its own fetched with
   *  the first Query window), copied with a click; or why it cannot be written, in words. */
  #codePanel(spec: TimeseriesSpec, language: TsCodeLanguage): HTMLElement {
    const doc = this.#doc;
    const words = CODE_WORDS[language];
    const panel = el(doc, 'div', 'dc-ts-code-panel');
    const program = el(doc, 'pre', 'dc-ts-code dc-ts-program', 'Writing…');
    let code: string | undefined;
    const copies = el(doc, 'div', 'dc-ts-row');
    copies.append(button(doc, `Copy ${words.label}`, `The ${words.label} that builds this tile`, () => {
      if (code === undefined) this.#say([`Its ${words.label} is not written: ${program.textContent ?? ''}`], true);
      else void this.#copy(code, `Its ${words.label} copied.`);
    }));
    panel.append(el(doc, 'div', 'dc-ts-hint', words.hint), copies, program);
    void lazily(() => import('../timeseries-code.ts')).then(({ tileCode }) => {
      code = tileCode(spec, this.#host.codeOf(spec), language);
      program.textContent = code;
    }).catch((e: unknown) => {
      program.textContent = `Not written: ${e instanceof Error ? e.message : String(e)}`;
    });
    // each formula with the Pure the tile writes for it (the user's "A", 2026-10-28): a formula is DataCube's shorthand for
    // that query -- written by the one function every round writes it by (askQuery), printed by the compiler
    const asks = tileAsks(spec).filter((a) => a.kind === 'series');
    if (asks.length > 0) {
      panel.append(el(doc, 'div', 'dc-ts-heading', 'Each formula\'s Pure'),
        el(doc, 'div', 'dc-ts-hint', 'Each series above is a formula, DataCube\'s gs-quant shorthand for the Pure query under it, which the database runs.'));
      const opened: Opened = new Map();
      const window = this.#window();
      const jobs = asks.map(async (ask) => {
        const s = spec.series.find((x) => x.id === ask.series);
        const pure = el(doc, 'pre', 'dc-ts-code', 'Writing…');
        panel.append(el(doc, 'div', 'dc-ts-field-label', `${ask.series}  ${s === undefined ? '' : exprSource(s.expr)}`), pure);
        try {
          const grid = this.#printer(spec, ask);
          if (grid === undefined) throw new Error('its grid is not on the page');
          pure.textContent = await grid.print(await this.#queryOf(spec, ask, window, opened));
        } catch (e) {
          pure.textContent = `Not written: ${e instanceof Error ? e.message : String(e)}`;
        }
      });
      void Promise.allSettled(jobs).then(() => putAway(opened));
    }
    return panel;
  }

  /** An ask's query written as a round would write it, without running it: over its grid's input, or several grids'
   *  relations opened together (`opened`, put away by the caller). */
  async #queryOf(spec: TimeseriesSpec, ask: TsAsk, window: TsWindow, opened: Opened): Promise<Lambda> {
    if (ask.kind === 'marks') {
      const marks = marksOf(spec, ask.marks);
      const own = this.#input(marks.source);
      if (own === undefined) throw new Error(`source ${marks.source}'s grid is not on the page`);
      return marksQuery(new Map([[marks.source, own.input]]), spec, marks);
    }
    if (ask.kind === 'bounds') {
      const src = spec.sources.find((x) => x.alias === ask.source);
      const own = src === undefined ? undefined : this.#input(src.alias);
      if (src === undefined || own === undefined) throw new Error(`source ${ask.source ?? ''}'s grid is not on the page`);
      return boundsQuery(own.input, src);
    }
    const on = await this.#inputsOn(askSources(spec, ask), opened);
    return askQuery(on.inputs, spec, ask, window).query;
  }

  /** The grid that prints an ask's query: the first it reads (a print reads the query alone, no model). */
  #printer(spec: TimeseriesSpec, ask: TsAsk): TsGrid | undefined {
    const first = askSources(spec, ask)[0];
    return first === undefined ? undefined : this.#input(first)?.grid;
  }

  /** Why an ask was refused, where the tile has said so: a series', the view's, a source's bounds'. */
  #refusalOf(ask: TsAsk): string | undefined {
    switch (ask.kind) {
      case 'bounds':
        return this.#boundsFailed.get(ask.source!);
      case 'marks':
        return this.#marksFailed.get(marksKey(marksOf(this.#spec, ask.marks)));
      case 'projection':
        return this.#projectionsFailed.get(projectionKey(projectionOf(this.#spec, ask.projection)));
      case 'series':
        return this.#failed.get(ask.series!);
      case 'statistics':
        return this.#statsFailed.get(ask.series!);
      case 'scatter':
      case 'seasonality':
      case 'histogram':
      case 'curve':
        return this.#viewFailed;
      case 'drawdowns':
      case 'correlations':
      case 'calendar':
        return undefined;
    }
  }

  /** A change about to be told made to the tile as the page has it: a drag's try under it set aside (never told with
   *  it), not drawn -- the change draws. The try set aside, if one was drawn. */
  #untried(): { readonly from: TimeseriesSpec; readonly to: TimeseriesSpec } | undefined {
    const tried = this.#tried;
    this.#tried = undefined;
    if (tried !== undefined && this.#spec === tried.to) this.#spec = tried.from;
    return tried;
  }

  /** A change the person made, by the tile's one rule (timeseries-edit.ts editTimeseries, which Python asks too): kept
   *  and drawn, or refused in its words. */
  #edit(change: TsChange): boolean {
    const tried = this.#untried();
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
    // refused after a try: the tile drawn as the page has it
    if (!kept && tried !== undefined) this.#apply(this.#spec);
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
      // a band says its edges there, a condition whether it holds, a line its value
      const said = at < 0 ? undefined : l.band
        ? `${valueText(l, l.lower?.[at] ?? null, this.#formatOf(l))} – ${valueText(l, l.upper?.[at] ?? null, this.#formatOf(l))}`
        : l.shades !== undefined ? (l.points[at]![1] === 1 ? 'holds' : 'does not')
          : valueText(l, l.points[at]![1], this.#formatOf(l));
      if (said !== undefined) chip.append(el(doc, 'span', 'dc-ts-chip-value', said));
      const pct = this.#stat(l, 'pct');
      if (pct !== null && l.level && (l.unit === 'same' || l.unit === 'index')) {
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
    const problems = [...this.#problems, ...this.#rangeProblems(), ...this.#statsProblems(), ...this.#viewProblems(), ...this.#marksProblems(), ...this.#projectionProblems()];
    const viewing = this.#spec.view?.mode !== undefined && this.#viewLanded === viewKey(this.#spec, this.#window());
    const said = [...problems, ...this.#notes, ...(viewing ? this.#viewSaid : [])];
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

  /** Each marks the database refused, by their name. */
  #marksProblems(): string[] {
    return (this.#spec.annotations ?? []).flatMap((a, i) => {
      const why = a.kind === 'marks' ? this.#marksFailed.get(marksKey(a)) : undefined;
      return a.kind === 'marks' && why !== undefined ? [`${marksName(a, i)} were refused: ${why}`] : [];
    });
  }

  /** Each trend projected the database refused, in its words. */
  #projectionProblems(): string[] {
    return (this.#spec.annotations ?? []).flatMap((a, i) => {
      const why = a.kind === 'projection' ? this.#projectionsFailed.get(projectionKey(a)) : undefined;
      return a.kind === 'projection' && why !== undefined ? [`${projectionName(a, i)} was refused: ${why}`] : [];
    });
  }

  /** The view's refusal, said (never drawn as nothing): its mode's, in its words. */
  #viewProblems(): string[] {
    const mode = this.#spec.view?.mode;
    if (mode === undefined || this.#viewFailed === undefined || this.#viewLanded !== viewKey(this.#spec, this.#window())) return [];
    return [`The ${mode} is not drawn: ${this.#viewFailed}`];
  }

  /** Each series' statistics the database refused, by its id. */
  #statsProblems(): string[] {
    if (!viewSetting(this.#spec, 'stats') && !viewSetting(this.#spec, 'legend')) return [];
    return [...this.#statsFailed].map(([id, why]) => `Series ${id}'s statistics were refused: ${why}`);
  }

  /** Text onto the clipboard, `done` said when it is; refused in words where the page has none. */
  async #copy(text: string, done: string): Promise<void> {
    const clipboard = this.#doc.defaultView?.navigator.clipboard;
    if (clipboard === undefined) {
      this.#say(['This page has no clipboard to copy to.'], true);
      return;
    }
    try {
      await clipboard.writeText(text);
      this.#say([done]);
    } catch (e) {
      this.#say([`Not copied: ${e instanceof Error ? e.message : String(e)}`], true);
    }
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

  #statValue(line: TsLine, stat: TsStat, rows: ReadonlyMap<string, ResultTable> = this.#statRows): Scalar | undefined {
    const t = rows.get(line.series.id);
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
    const window = this.#window();
    for (const [tab, label, title] of STAT_TABS) {
      const b = button(doc, label, title, () => {
        this.#statsTab = tab;
        this.#paintStats();
      }, 'dc-ts-stats-tab');
      b.setAttribute('role', 'tab');
      b.setAttribute('aria-selected', String(tab === this.#statsTab));
      tabs.append(b);
    }
    const head_ = el(doc, 'div', 'dc-ts-stats-head');
    head_.append(tabs, doc.createTextNode(this.#statsTab === 'calendar' ? 'Over every date each line has: every figure the database\'s'
      : `Over ${window.from ?? 'the first date'} to ${window.to ?? 'the latest'}: every figure the database's`));
    if (this.#statsTab === 'drawdowns') {
      this.#stats.replaceChildren(head_, ...this.#drawdownsTable());
      return;
    }
    if (this.#statsTab === 'correlations') {
      this.#stats.replaceChildren(head_, ...this.#correlationsTable());
      return;
    }
    if (this.#statsTab === 'data') {
      this.#stats.replaceChildren(head_, ...this.#dataTable());
      return;
    }
    if (this.#statsTab === 'calendar') {
      this.#stats.replaceChildren(head_, ...this.#calendarTable());
      return;
    }
    const table = el(doc, 'table', 'dc-ts-table');
    const head = el(doc, 'tr');
    head.append(el(doc, 'th', undefined, 'Line'), ...columns.map((c) => el(doc, 'th', undefined, c.label)));
    table.append(head);
    const grain = this.#spec.grain ?? 'day';
    for (const l of this.#lines.filter((x) => !x.band && !x.shades && !x.series.hidden)) {
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
    const blanks = this.#statBlanks();
    this.#stats.replaceChildren(head_, table,
      ...(blanks.length > 0 ? [el(doc, 'div', 'dc-ts-stats-blanks', blanks.join(' '))] : []));
  }

  /** The lines the Drawdowns tab shows: of the series the drawdowns are asked of (tableSeries, Python's rule too: every
   *  level, hidden or not, so showing one is no new ask -- the E9 review's 3 and 8), those not hidden. */
  #levelLines(): TsLine[] {
    const asked = new Set(tableSeries(this.#spec).drawdowns);
    return this.#lines.filter((l) => asked.has(l.series.id) && !l.series.hidden);
  }

  /**
   * THE DRAWDOWNS TAB: each level line's deepest drawdowns over the dates shown, as the database answered them
   * (timeseries-query.ts drawdownsQuery) -- asked the first time the tab is shown for the answers drawn.
   */
  #drawdownsTable(): HTMLElement[] {
    const doc = this.#doc;
    const lines = this.#levelLines();
    if (lines.length === 0) return [el(doc, 'div', 'dc-ts-stats-blanks', 'No line is a level: a return, a difference or a deviation has no drawdowns.')];
    if (this.#drawdowns === undefined) {
      void this.#askDrawdowns();
      return [el(doc, 'div', 'dc-ts-stats-blanks', 'Asking the database for each line\'s drawdowns…')];
    }
    const table = el(doc, 'table', 'dc-ts-table');
    const head = el(doc, 'tr');
    head.append(...['Line', '#', 'Peak', 'Trough', 'Recovered', 'Depth', 'Days down', 'Days back'].map((t) => el(doc, 'th', undefined, t)));
    table.append(head);
    const grain = this.#spec.grain ?? 'day';
    const said: string[] = [];
    for (const l of lines) {
      const refused = this.#drawdownsFailed.get(l.series.id);
      const name = (): HTMLElement => {
        const td = el(doc, 'td', 'dc-ts-table-name');
        const swatch = el(doc, 'span', 'dc-ts-swatch');
        swatch.style.background = l.color;
        td.append(swatch, doc.createTextNode(l.name));
        return td;
      };
      if (refused !== undefined) {
        const tr = el(doc, 'tr');
        tr.append(name(), el(doc, 'td', 'dc-ts-table-refused', `Refused: ${refused}`, { colspan: '7' }));
        table.append(tr);
        continue;
      }
      const t = this.#drawdowns.get(l.series.id);
      if (!t) continue;
      const column = (n: string): readonly unknown[] => t.columns.find((c) => c.name === n)?.values ?? [];
      const splits = column(TS_SPLIT);
      const mine = column(TS_DD_RANK).map((_, i) => i).filter((i) => l.split === undefined || splits[i] === l.split);
      if (mine.length === 0) {
        // a level at or below zero has none (its falls no fraction of a peak): said so, read off its statistics (the E9
        // review's 9) -- else it never fell
        const low = this.#statColumn(l, TS_STAT_LEVEL_LOW);
        said.push(low !== undefined && low !== null && low <= 0
          ? `${l.name} is at or below zero over these dates: it has no drawdowns.`
          : `${l.name} has no drawdown over these dates: it never fell below a peak.`);
      }
      for (const i of mine) {
        const tr = el(doc, 'tr');
        const date = (n: string): HTMLElement => {
          const v = column(n)[i];
          return el(doc, 'td', 'dc-ts-table-date', v === null || v === undefined ? 'not yet' : dateLabel(String(v), grain));
        };
        const count = (n: string): HTMLElement => {
          const v = toNumber(column(n)[i] as Scalar);
          return el(doc, 'td', 'dc-ts-table-num', v === null ? '—' : v.toLocaleString(UI_LOCALE));
        };
        tr.append(name(), count(TS_DD_RANK), date(TS_DD_PEAK), date(TS_DD_TROUGH), date(TS_DD_RECOVERED),
          el(doc, 'td', 'dc-ts-table-num', valueText({ unit: 'ratio' }, toNumber(column(TS_DD_DEPTH)[i] as Scalar))), count(TS_DD_DECLINE), count(TS_DD_RECOVERY));
        table.append(tr);
      }
    }
    return [table, ...(said.length > 0 ? [el(doc, 'div', 'dc-ts-stats-blanks', said.join(' '))] : [])];
  }

  /** The series the correlations are asked of: tableSeries' (Python's rule too: every one drawn on an axis but candles,
   *  hidden or not), the first MAX_CORRELATED. */
  #correlatedIds(): string[] {
    return [...new Set(tableSeries(this.#spec).correlations.flat())].slice(0, MAX_CORRELATED);
  }

  /** The lines the Correlations tab shows: of the series asked, those not hidden, the first MAX_CORRELATED. */
  #correlatedLines(): TsLine[] {
    const asked = new Set(this.#correlatedIds());
    return this.#lines.filter((l) => asked.has(l.series.id) && !l.series.hidden).slice(0, MAX_CORRELATED);
  }

  /**
   * THE CORRELATIONS TAB: a matrix of the lines, each cell the pair's correlation over the dates shown, as the database
   * answered it (timeseries-query.ts correlationsQuery) -- asked the first time the tab is shown for the answers drawn.
   * Each cell tinted by its sign and size: drawn, not computed.
   */
  #correlationsTable(): HTMLElement[] {
    const doc = this.#doc;
    const lines = this.#correlatedLines();
    if (lines.length < 2) return [el(doc, 'div', 'dc-ts-stats-blanks', 'A correlation is of two lines: draw another.')];
    if (this.#correlations === undefined) {
      void this.#askCorrelations();
      return [el(doc, 'div', 'dc-ts-stats-blanks', 'Asking the database for each pair of lines\' correlation…')];
    }
    const table = el(doc, 'table', 'dc-ts-table dc-ts-corr');
    const head = el(doc, 'tr');
    head.append(el(doc, 'th', undefined, 'Line'), ...lines.map((l) => el(doc, 'th', undefined, l.name)));
    table.append(head);
    for (const a of lines) {
      const tr = el(doc, 'tr');
      const name = el(doc, 'td', 'dc-ts-table-name');
      const swatch = el(doc, 'span', 'dc-ts-swatch');
      swatch.style.background = a.color;
      name.append(swatch, doc.createTextNode(a.name));
      tr.append(name);
      for (const b of lines) {
        // each line with itself the database's too: none where it has under two values or does not vary (the E9 review's 13)
        const v = this.#correlations.get(`${a.key}|${b.key}`);
        const td = el(doc, 'td', 'dc-ts-table-num', v === undefined || v === null ? '—' : v.toLocaleString(UI_LOCALE, { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
        if (v !== undefined && v !== null && a.key !== b.key) {
          td.classList.add(v >= 0 ? 'dc-ts-corr-pos' : 'dc-ts-corr-neg');
          td.style.setProperty('--dc-ts-corr', String(Math.min(1, Math.abs(v))));
        }
        tr.append(td);
      }
      table.append(tr);
    }
    const said = [...this.#correlationsFailed];
    const asked = new Set(this.#correlatedIds());
    if (new Set(tableSeries(this.#spec).correlations.flat()).size > asked.size
      || this.#lines.filter((l) => asked.has(l.series.id) && !l.series.hidden).length > MAX_CORRELATED) {
      said.push(`The first ${MAX_CORRELATED} lines.`);
    }
    said.push('A level\'s returns are correlated, anything else as it is, over the dates each pair has shown.');
    return [table, el(doc, 'div', 'dc-ts-stats-blanks', said.join(' '))];
  }

  /**
   * THE DATA TAB: every line's figures side by side, a row per date shown (timeseries-option.ts timeseriesTable: the
   * database's points, put by their dates) -- the latest MAX_DATA_ROWS listed, latest first, each in its line's format;
   * Copy takes every date, oldest first, as a spreadsheet pastes it.
   */
  #dataTable(): HTMLElement[] {
    const doc = this.#doc;
    let t: ReturnType<typeof timeseriesTable>;
    try {
      t = timeseriesTable(this.#lines.filter((l) => !l.series.hidden && !l.shades), this.#window());
    } catch (e) {
      // a line of two values a date: said, never a value dropped
      return [el(doc, 'div', 'dc-ts-error', `No table: ${e instanceof Error ? e.message : String(e)}`)];
    }
    if (t.columns.length === 0 || t.rows.length === 0) return [el(doc, 'div', 'dc-ts-stats-blanks', 'No line has a value over these dates.')];
    const table = el(doc, 'table', 'dc-ts-table dc-ts-data');
    const head = el(doc, 'tr');
    head.append(el(doc, 'th', undefined, 'Date'), ...t.columns.map((c) => {
      const th = el(doc, 'th');
      const swatch = el(doc, 'span', 'dc-ts-swatch');
      swatch.style.background = c.line.color;
      th.append(swatch, doc.createTextNode(c.name));
      return th;
    }));
    table.append(head);
    const grain = this.#spec.grain ?? 'day';
    for (const r of t.rows.slice(-MAX_DATA_ROWS).reverse()) {
      const tr = el(doc, 'tr');
      tr.append(el(doc, 'td', 'dc-ts-table-date', dateLabel(r.date, grain)),
        ...r.values.map((v, i) => el(doc, 'td', 'dc-ts-table-num', v === null ? '' : valueText(t.columns[i]!.line, v, this.#formatOf(t.columns[i]!.line)))));
      table.append(tr);
    }
    const copy = button(doc, 'Copy', 'Every date shown, oldest first, as a spreadsheet pastes it', () =>
      void this.#copy(timeseriesTsv(t), `${t.rows.length.toLocaleString(UI_LOCALE)} dates copied, oldest first.`));
    const said = t.rows.length > MAX_DATA_ROWS
      ? `The latest ${MAX_DATA_ROWS.toLocaleString(UI_LOCALE)} of ${t.rows.length.toLocaleString(UI_LOCALE)} dates, latest first: Copy takes every one.`
      : `${t.rows.length.toLocaleString(UI_LOCALE)} dates, latest first.`;
    const bar = el(doc, 'div', 'dc-ts-row');
    bar.append(copy, el(doc, 'span', 'dc-ts-stats-blanks', said));
    // its rows scrolled within the panel, its header kept above them
    const scroll = el(doc, 'div', 'dc-ts-data-scroll');
    scroll.append(table);
    return [bar, scroll];
  }

  /**
   * THE CALENDAR TAB (the next phases' 3: the fund sheet's monthly returns): a level line's return each month and each
   * year, a row a year (the latest first), as the database answered them (timeseries-query.ts calendarQuery) -- asked the
   * first time the tab shows its series. Each cell tinted by its sign and size: drawn, not computed.
   */
  #calendarTable(): HTMLElement[] {
    const doc = this.#doc;
    const ids = new Set(viewSeries(this.#spec).calendar);
    const lines = this.#lines.filter((l) => ids.has(l.series.id) && !l.series.hidden);
    if (lines.length === 0) return [el(doc, 'div', 'dc-ts-stats-blanks', 'No line is a level: a return, a difference or a deviation has no calendar of returns.')];
    const line = lines.find((l) => l.key === this.#calendarLine) ?? lines[0]!;
    const pick = select(doc, lines.map((l) => [l.key, l.name] as const), line.key, (k) => {
      this.#calendarLine = k;
      this.#paintStats();
    }, 'Line');
    const bar = el(doc, 'div', 'dc-ts-row');
    bar.append(pick);
    const asked = this.#calendar;
    if (asked === undefined || asked.id !== line.series.id) {
      void this.#askCalendar(line.series);
      return [bar, el(doc, 'div', 'dc-ts-stats-blanks', 'Asking the database for its months\' returns…')];
    }
    if ('failed' in asked) return [bar, el(doc, 'div', 'dc-ts-error', `Refused: ${asked.failed}`)];
    const t = asked.table;
    // a column the database answered without refused by name, never read as no months (the Phase 3 review's 9)
    const column = (n: string): readonly unknown[] => {
      const c = t.columns.find((x) => x.name === n);
      if (!c) throw new Error(`the database answered without the calendar's ${n}`);
      return c.values;
    };
    let splits: readonly unknown[];
    let years: (number | null)[];
    let months: (number | null)[];
    let returns: (number | null)[];
    let yearly: (number | null)[];
    let lows: (number | null)[];
    try {
      splits = line.split === undefined ? [] : column(TS_SPLIT);
      years = column(TS_CAL_YEAR).map((v) => toNumber(v as Scalar));
      months = column(TS_CAL_MONTH).map((v) => toNumber(v as Scalar));
      returns = column(TS_CAL_RETURN).map((v) => toNumber(v as Scalar));
      yearly = column(TS_CAL_YEAR_RETURN).map((v) => toNumber(v as Scalar));
      lows = column(TS_CAL_LOW).map((v) => toNumber(v as Scalar));
    } catch (e) {
      return [bar, el(doc, 'div', 'dc-ts-error', `Refused: ${e instanceof Error ? e.message : String(e)}`)];
    }
    // its rows by year, each month's return in its place, the year's beside them
    const byYear = new Map<number, { months: (number | null | undefined)[]; year: number | null }>();
    years.forEach((y, i) => {
      if (y === null || (line.split !== undefined && splits[i] !== line.split)) return;
      const row = byYear.get(y) ?? { months: Array.from({ length: 12 }, () => undefined), year: null };
      const m = months[i];
      if (m !== null && m !== undefined && m >= 1 && m <= 12) row.months[m - 1] = returns[i];
      row.year = yearly[i] ?? null;
      byYear.set(y, row);
    });
    if (byYear.size === 0) return [bar, el(doc, 'div', 'dc-ts-stats-blanks', `${line.name} has no month with a level.`)];
    const table = el(doc, 'table', 'dc-ts-table dc-ts-cal');
    const head = el(doc, 'tr');
    head.append(el(doc, 'th', undefined, 'Year'), ...TS_MONTH_NAMES.map((m) => el(doc, 'th', undefined, m)), el(doc, 'th', undefined, 'Year'));
    table.append(head);
    // a month of 10% (a year of 25%) the strongest tint
    const cell = (v: number | null | undefined, full: number): HTMLElement => {
      const td = el(doc, 'td', 'dc-ts-table-num', v === undefined ? '' : valueText({ unit: 'ratio' }, v));
      if (v !== undefined && v !== null && v !== 0) {
        td.classList.add(v > 0 ? 'dc-ts-corr-pos' : 'dc-ts-corr-neg');
        td.style.setProperty('--dc-ts-corr', String(Math.min(1, Math.abs(v) / full)));
      }
      return td;
    };
    for (const [y, row] of [...byYear].sort((a, b) => b[0] - a[0])) {
      const tr = el(doc, 'tr');
      tr.append(el(doc, 'td', 'dc-ts-table-date', String(y)), ...row.months.map((v) => cell(v, 0.1)), cell(row.year, 0.25));
      table.append(tr);
    }
    const said = ['A month\'s return from the month before\'s last level to its own last; a year\'s from the year before\'s last.'
      + ' None where there is no level the month or the year before: the first, and after one missing.'];
    // a level at or below zero at a month's end: a return from it none, said so (the Phase 3 review's 9)
    const low = lows.find((_, i) => line.split === undefined || splits[i] === line.split);
    if (low !== undefined && low !== null && low <= 0) said.push(`${line.name} is at or below zero at a month's end: a return from there is none.`);
    return [bar, table, el(doc, 'div', 'dc-ts-stats-blanks', said.join(' '))];
  }

  /** A level series' calendar asked of the database, once for the rows drawn, then the tab painted again. */
  async #askCalendar(s: TsSeries): Promise<void> {
    if (this.#calendarAsking) {
      this.#calendarStale = true;
      return;
    }
    this.#calendarAsking = true;
    this.#calendarStale = false;
    const gen = this.#calendarGen;
    const spec = this.#spec;
    const opened: Opened = new Map();
    let answer: CalendarAnswer;
    try {
      answer = { id: s.id, table: (await this.#runAsk(spec, { kind: 'calendar', series: s.id }, {}, new AbortController().signal, opened)).table };
    } catch (e) {
      answer = { id: s.id, failed: e instanceof Error ? e.message : String(e) };
    } finally {
      putAway(opened);
      this.#calendarAsking = false;
    }
    if (this.#disposed) return;
    // asked again since (the rows moved, or another line's asked while this was in flight): asked for what is shown now
    if (gen !== this.#calendarGen || this.#calendarStale) {
      this.#calendarStale = false;
      if (this.#statsTab === 'calendar') this.#paintStats();
      return;
    }
    this.#calendar = answer;
    if (this.#statsTab === 'calendar') this.#paintStats();
  }

  /** Each pair of series' lines' correlations asked of the database over the dates shown, once, then painted. */
  async #askCorrelations(): Promise<void> {
    if (this.#correlationsAsking) {
      this.#correlationsStale = true;
      return;
    }
    this.#correlationsAsking = true;
    this.#correlationsStale = false;
    const gen = this.#gen;
    const spec = this.#spec;
    const window = this.#window();
    const signal = new AbortController().signal;
    const opened: Opened = new Map();
    const cells = new Map<string, number | null>();
    const failed: string[] = [];
    try {
      const ids = new Set(this.#correlatedIds());
      const keyOf = (s: TsSeries, split: unknown): string => (s.split === undefined ? s.id : `${s.id}\u0000${String(split)}`);
      const pairs = tableSeries(spec).correlations.filter(([x, y]) => ids.has(x) && ids.has(y))
        .map(([x, y]) => [spec.series.find((s) => s.id === x)!, spec.series.find((s) => s.id === y)!] as const);
      await Promise.all(pairs.map(async ([a, b]) => {
        try {
          const t = (await this.#runAsk(spec, { kind: 'correlations', series: a.id, with: b.id }, window, signal, opened)).table;
          const column = (n: string): readonly unknown[] | undefined => t.columns.find((c) => c.name === n)?.values;
          const corr = column(TS_CORR) ?? [];
          corr.forEach((v, i) => {
            const ka = keyOf(a, column(TS_CORR_A)?.[i]);
            const kb = keyOf(b, column(TS_CORR_B)?.[i]);
            const n = toNumber(v as Scalar);
            cells.set(`${ka}|${kb}`, n);
            cells.set(`${kb}|${ka}`, n);
          });
        } catch (e) {
          failed.push(`${a.id} with ${b.id} was refused: ${e instanceof Error ? e.message : String(e)}`);
        }
      }));
    } finally {
      putAway(opened);
      this.#correlationsAsking = false;
    }
    if (this.#disposed) return;
    // asked again since (a round began, or the tab asked while this was in flight -- the E9 review's 12): this answer may
    // be of what was drawn before, so asked again for what is drawn now, if still shown
    if (gen !== this.#gen || this.#correlationsStale) {
      this.#correlationsStale = false;
      if (this.#statsTab === 'correlations' && this.#correlations === undefined) void this.#askCorrelations();
      return;
    }
    this.#correlations = cells;
    this.#correlationsFailed = failed;
    this.#paintStats();
  }

  /** Each level series' drawdowns asked of the database over the dates shown, once, then the tab painted again. */
  async #askDrawdowns(): Promise<void> {
    if (this.#drawdownsAsking) {
      this.#drawdownsStale = true;
      return;
    }
    this.#drawdownsAsking = true;
    this.#drawdownsStale = false;
    const gen = this.#gen;
    const spec = this.#spec;
    const window = this.#window();
    const signal = new AbortController().signal;
    const opened: Opened = new Map();
    const rows = new Map<string, ResultTable>();
    const failed = new Map<string, string>();
    try {
      const ids = tableSeries(spec).drawdowns;
      await Promise.all(ids.map(async (id) => {
        try {
          rows.set(id, (await this.#runAsk(spec, { kind: 'drawdowns', series: id }, window, signal, opened)).table);
        } catch (e) {
          failed.set(id, e instanceof Error ? e.message : String(e));
        }
      }));
    } finally {
      putAway(opened);
      this.#drawdownsAsking = false;
    }
    if (this.#disposed) return;
    // asked again since (a round began, or the tab asked while this was in flight -- the E9 review's 12): this answer is
    // of what was drawn before -- asked again for what is drawn now, if still shown
    if (gen !== this.#gen || this.#drawdownsStale) {
      this.#drawdownsStale = false;
      if (this.#statsTab === 'drawdowns' && this.#drawdowns === undefined) void this.#askDrawdowns();
      return;
    }
    this.#drawdowns = rows;
    this.#drawdownsFailed = failed;
    this.#paintStats();
  }

  /**
   * WHY A LEVEL'S FIGURES ARE BLANK, line by line (the second audit's P-2b and S1: a "—" said nothing): no volatility or
   * Sharpe ratio where its dates are no frequency to annualize by; none of its returns' figures where it is at or below
   * zero over the dates shown -- each read off the database's answer, compared, never computed.
   */
  #statBlanks(): string[] {
    const out: string[] = [];
    for (const l of this.#lines.filter((x) => x.level && !x.band && !x.series.hidden && !this.#statsFailed.has(x.series.id))) {
      // by the rule Python's statistics say it by (timeseries-query.ts statBlanks)
      out.push(...statBlanks(l.name, { low: this.#statColumn(l, TS_STAT_LEVEL_LOW), perYear: this.#statColumn(l, TS_STAT_PER_YEAR),
        cagr: this.#statColumn(l, statName('cagr')) }));
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
    // the editor it shows put away first
    const leaving = this.#leaving;
    this.#leaving = null;
    leaving?.();
    this.#pop.replaceChildren(...content);
    this.#settings = false;
    raiseWindow(win, host);
    // the window itself focused, as a chart's options are: Escape puts it away until a field is entered, where Escape is
    // the field's (the third review's 2: the first field focused, Escape never closed it)
    win.focus({ preventScroll: true });
  }

  /** Its editor's window put away -- the focus, when it was in it, back on its tile. */
  #close(): void {
    const leaving = this.#leaving;
    this.#leaving = null;
    leaving?.();
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
    // its outermost function's parameters as sliders, painted as the expression is read (`paint` false: a slider moving,
    // which stays the one dragged)
    const sliders = el(doc, 'div', 'dc-ts-sliders');
    const check = (paint = true): void => {
      error.textContent = '';
      parsed = undefined;
      if (expr.value.trim() !== '') {
        try {
          parsed = parseExpr(expr.value);
          const read = sourcesOf(spec, inlined(parsed, spec.series.filter((s) => s.id !== sid)));
          reads.textContent = `Reads ${read.join(', ') || 'no grid'}${read.length > 1 ? ': one query, the grids joined on the date' : ''}.`;
        } catch (e) {
          error.textContent = e instanceof TsSyntaxError ? `${e.message} (at ${e.at + 1})` : e instanceof Error ? e.message : String(e);
          reads.textContent = '';
        }
      }
      if (paint) paintSliders();
    };
    /**
     * A PARAMETER TRIED BY DRAGGING (the next phases' 2: TradingView's inputs): each number of the outermost function
     * its slider, moving the expression's own parameter -- and a series already plotted drawn again when it stops, by the
     * tile's one rule, as its OK would: tried, as the person's own look, not kept -- OK keeps it, one change told (one
     * step to undo); Cancel, Escape, Remove or another editor take it back (the Phase 2 review's 1: each stop was told,
     * and one after Cancel landed still).
     */
    let live: ReturnType<typeof setTimeout> | undefined;
    const paintSliders = (): void => {
      const call = parsed?.kind === 'call' ? parsed : undefined;
      const f = call ? functionNamed(call.fn) : undefined;
      const tunable = call && f ? f.params.filter((p) => p.kind !== 'word' && typeof (call.params?.[p.name] ?? p.default) === 'number') : [];
      sliders.replaceChildren(...tunable.map((p) => {
        const at = Number(call!.params?.[p.name] ?? p.default);
        const [lo, hi, step] = sliderRange(p, at);
        const range = el(doc, 'input', 'dc-ts-slider', undefined, {
          type: 'range', min: String(lo), max: String(hi), step: String(step), 'aria-label': `${f!.label}'s ${p.name}`, title: p.about,
        });
        range.value = String(at);
        const shown = el(doc, 'span', 'dc-ts-slider-value', String(at));
        range.addEventListener('input', () => {
          const v = Number(Number(range.value).toPrecision(6));
          shown.textContent = String(v);
          // the expression as it is now: another slider moved too
          const now = parsed?.kind === 'call' && parsed.fn === call!.fn ? parsed : call!;
          expr.value = exprText({ ...now, params: { ...now.params, [p.name]: v } });
          check(false);
          if (was === undefined) return;
          clearTimeout(live);
          live = setTimeout(() => apply(false), 400);
        });
        const row = el(doc, 'label', 'dc-ts-field dc-ts-slider-row');
        row.append(el(doc, 'span', 'dc-ts-field-label', p.name), range, shown);
        return row;
      }));
    };
    expr.addEventListener('input', () => check());
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
      const second = other ? `$${other.id}` : inner;
      // the expression first, then what the function reads beside it: another series, an if's branches, weights
      const args = f.series === 2 ? [inner, second] : f.series === 3 ? [inner, second, '0'] : f.series === 'many' ? [inner, '1', second, '1'] : [inner];
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
    /** The series as the editor has it, plotted -- the window put away when `close`; tried (a slider's) when not. */
    const apply = (close: boolean): void => {
      check(close);
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
      if (!close) {
        const change: TsChange = { kind: 'plot', series: made, ...(was ? { replace: true } : {}) };
        // tried: judged by the tile's one rule over the tile as the page has it, drawn, told nothing
        const from = this.#tried?.from ?? this.#spec;
        let read: TimeseriesSpec;
        try {
          read = judgedEdit(from, editTimeseries(from, change), (x) => this.#columns(x));
        } catch (e) {
          if (!(e instanceof TsEditRefusal)) throw e;
          this.#say([e.message], true);
          return;
        }
        this.#tried = { from, to: read };
        this.#apply(read);
        return;
      }
      clearTimeout(live);
      if (this.#edit({ kind: 'plot', series: made, ...(was ? { replace: true } : {}) })) this.#close();
    };
    const ok = button(doc, 'OK', 'Plot it (Enter)', () => apply(true), 'dc-button dc-primary');
    expr.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') ok.click();
    });
    const remove = button(doc, 'Remove', 'Take this series off', () => {
      clearTimeout(live);
      if (this.#edit({ kind: 'unplot', id: sid })) this.#close();
    });
    remove.hidden = was === undefined;
    const reference = el(doc, 'details', 'dc-ts-reference');
    reference.append(el(doc, 'summary', undefined, 'Functions (gs-quant\'s names)'));
    const list = el(doc, 'dl', 'dc-ts-functions');
    for (const f of TS_FUNCTIONS) {
      const reads = f.reads !== undefined ? [...f.reads] : f.series === 'many' ? ['x', 'wx', 'y', 'wy', '...'] : f.series === 3 ? ['flags', 'x', 'y']
        : Array.from({ length: f.series }, (_, i) => (i === 0 ? 'x' : 'y'));
      const sig = `${f.name}(${[...reads, ...f.params.map((p) => `${p.name}=${p.default}`)].join(', ')})`;
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
      labelled(doc, 'Expression', expr), error, reads, sliders, builder, splitSel, fnSel, about,
      labelled(doc, 'Name', name), style, swatches, reference, actions);
    // put away (Cancel, Escape, its close, another editor): a drag's stop forgotten, and what it tried taken back
    this.#leaving = () => {
      clearTimeout(live);
      const tried = this.#tried;
      this.#tried = undefined;
      if (tried !== undefined && this.#spec === tried.to) this.#apply(tried.from);
    };
    check();
  }

  /** The values of a source's instrument column, the database's (the picker's): refused, in the database's words. */
  async #keyValues(src: TsSource, key: string): Promise<string[]> {
    const own = this.#input(src.alias);
    if (!own) throw new Error(`${src.alias}'s grid is not on the page`);
    const t = (await own.grid.run(keyValuesQuery(own.input, src, key), own.grid.snapshot, new AbortController().signal)).rows;
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
    // a subtitle and a note on the chart and its pictures (the next phases' 2)
    for (const [key, label, hint] of [['subtitle', 'Subtitle', 'A line under the title'], ['note', 'Note', 'Source: …']] as const) {
      const box = el(doc, 'input', 'dc-input', undefined, { 'aria-label': label, placeholder: hint });
      box.value = spec.view?.[key] ?? '';
      box.addEventListener('change', () => this.#edit({ kind: 'view', view: { [key]: box.value.trim() === '' ? null : box.value.trim() } }));
      parts.push(labelled(doc, label, box));
    }
    // how its series are drawn, and what a view reads (the next phases' 3): by the rule the view reads them by
    // (timeseries-query.ts viewSeries) when none is picked
    const mode = spec.view?.mode;
    const drawn = el(doc, 'div', 'dc-ts-row');
    drawn.append(el(doc, 'span', 'dc-ts-field-label', 'Drawn'),
      select(doc, MODES, mode ?? '', (v) => {
        if (this.#edit({ kind: 'view', view: { mode: v === '' ? null : v as TsMode } })) this.#editSettings();
      }, 'Drawn'));
    parts.push(drawn);
    if (mode !== undefined) parts.push(...this.#viewFields(mode));
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
    // how two series' dates are combined where an operator reads both (gs-quant's Interpolate)
    const align = select(doc, ALIGN_WORDS, spec.align ?? 'intersect',
      (v) => this.#edit({ kind: 'align', align: v === 'intersect' ? null : v as TsAlign }), 'Combining two series');
    const combine = el(doc, 'div', 'dc-ts-row');
    combine.append(el(doc, 'span', 'dc-ts-field-label', 'Combining'), align);
    parts.push(combine);
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
      // upside down, and its axis' ends by hand (the next phases' 2)
      const invert = el(doc, 'input', undefined, undefined, { type: 'checkbox' });
      invert.checked = spec.panes?.[p]?.invert === true;
      invert.addEventListener('change', () => this.#edit({ kind: 'pane', index: p, invert: invert.checked }));
      const inverted = el(doc, 'label', 'dc-ts-check');
      inverted.append(invert, doc.createTextNode('Upside down'));
      const end = (which: 'min' | 'max'): HTMLInputElement => {
        const box = el(doc, 'input', 'dc-input dc-ts-small', undefined, { type: 'number', step: 'any', 'aria-label': which === 'min' ? 'Least' : 'Most', placeholder: which === 'min' ? 'Least' : 'Most' });
        const at = spec.panes?.[p]?.[which];
        box.value = at === undefined ? '' : String(at);
        box.addEventListener('change', () => {
          // what the box cannot read ("5,000": its value then empty), or none an axis ends at (1e400): said, the end
          // kept -- never taken off without a word (the Phase 2 review's 8)
          const label = which === 'min' ? 'Least' : 'Most';
          if (box.validity.badInput) {
            this.#say([`${label}: what was typed is not a number.`], true);
            return;
          }
          const v = box.value.trim() === '' ? null : Number(box.value);
          if (v !== null && !Number.isFinite(v)) {
            this.#say([`${label}: ${box.value.trim()} is no number the axis can end at.`], true);
            const now = this.#spec.panes?.[p]?.[which];
            box.value = now === undefined ? '' : String(now);
            return;
          }
          // refused: Settings painted again as the tile has it (#edit)
          this.#edit({ kind: 'pane', index: p, [which]: v });
        });
        return box;
      };
      row.append(el(doc, 'span', 'dc-ts-field-label', p === 0 ? 'Main' : `Pane ${p + 1}`), height, l, inverted, end('min'), end('max'));
      parts.push(row);
    }
    // the annotations
    parts.push(el(doc, 'div', 'dc-ts-heading', 'Annotations'));
    for (const [i, a] of (spec.annotations ?? []).entries()) {
      const row = el(doc, 'div', 'dc-ts-row');
      const point = (p: TsPoint): string => `${p.date} at ${p.value.toLocaleString(UI_LOCALE)}`;
      const said = a.kind === 'level' ? `Level ${a.value}` : a.kind === 'event' ? `Event ${a.date}` : a.kind === 'period' ? `Period ${a.from} – ${a.to}`
        : a.kind === 'line' ? `Line ${point(a.from)} → ${point(a.to)}` : a.kind === 'note' ? `Note ${point(a.at)}: ${a.text}`
          : a.kind === 'projection' ? `Projection of ${a.series}: its last ${a.window ?? TS_PROJECTION_WINDOW} observations, ${a.ahead ?? TS_PROJECTION_AHEAD} on`
          : `Marks: ${a.source}'s rows${(a.where ?? []).length > 0 ? ` [${matchesText(a.where!)}]` : ''}${a.at !== undefined ? ` at ${a.at}` : ', a line each'}`;
      const pane = (a.kind === 'level' || a.kind === 'marks' || a.kind === 'line' || a.kind === 'note') && a.pane ? ` (pane ${a.pane + 1})` : '';
      const label = a.kind !== 'note' && a.label ? `: ${a.label}` : '';
      row.append(el(doc, 'span', undefined, `${said}${pane}${label}`), closeMark(button(doc, '×', 'Take it off', () => {
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
    parts.push(addRow, el(doc, 'div', 'dc-ts-hint', 'Double-click the chart to put an event at that date; Draw puts a line or a note on it.'));
    parts.push(...this.#marksFields(used), ...this.#shadeFields(), ...this.#projectionFields());
    this.#open('Settings', ...parts);
    this.#settings = true;
  }

  /** Marks from a grid's rows in Settings (TsMarks): the source, the rows it marks (matches typed as a column's), the
   *  column each is drawn at (or a line at its date), what each says, its shape, its pane and its label -- added by the
   *  tile's one rule, refused in its words. */
  #marksFields(used: readonly number[]): HTMLElement[] {
    const doc = this.#doc;
    const spec = this.#spec;
    const columns = this.#columns(spec);
    const source = select(doc, spec.sources.map((s) => [s.alias, s.alias] as const), spec.sources.at(-1)!.alias, () => paint(), 'Marks source');
    const matches = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Rows: AAPL, side="BUY"', 'aria-label': 'Rows marked' });
    const at = el(doc, 'select', 'dc-input dc-ts-small', undefined, { 'aria-label': 'Drawn at' });
    const text = el(doc, 'select', 'dc-input dc-ts-small', undefined, { 'aria-label': 'Says' });
    const shape = select(doc, [['up', '▲ up'], ['down', '▼ down'], ['dot', '● dot']], 'up', () => undefined, 'Shape');
    const pane = select(doc, used.map((p) => [String(p), p === 0 ? 'Main pane' : `Pane ${p + 1}`] as const), '0', () => undefined, 'Marks pane');
    const label = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Label', 'aria-label': 'Marks label' });
    const options = (to: HTMLSelectElement, none: string, names: readonly string[]): void => {
      to.replaceChildren(...[['', none] as const, ...names.map((n) => [n, n] as const)].map(([v, t]) => {
        const o = doc.createElement('option');
        o.value = v;
        o.textContent = t;
        return o;
      }));
    };
    const paint = (): void => {
      const cols = columns.get(source.value) ?? [];
      const date = spec.sources.find((s) => s.alias === source.value)?.date;
      options(at, 'A line at its date', cols.filter((c) => isNumeric(c.type) && c.name !== date).map((c) => c.name));
      options(text, 'Says nothing', cols.filter((c) => c.name !== date).map((c) => c.name));
    };
    paint();
    const add = button(doc, 'Add', 'Mark the rows over the series', () => {
      let where: ReturnType<typeof parseMatches>;
      try {
        where = parseMatches(matches.value);
      } catch (e) {
        this.#say([e instanceof Error ? e.message : String(e)], true);
        return;
      }
      const words = label.value.trim();
      const made: TsAnnotation = {
        kind: 'marks', source: source.value, ...(where.length > 0 ? { where } : {}),
        ...(at.value !== '' ? { at: at.value, shape: shape.value as TsMarkShape, ...(pane.value !== '0' ? { pane: Number(pane.value) } : {}) } : {}),
        ...(text.value !== '' ? { text: text.value } : {}), ...(words ? { label: words } : {}),
      };
      if (this.#edit({ kind: 'annotate', annotation: made })) this.#editSettings();
    });
    const rows = el(doc, 'div', 'dc-ts-row');
    rows.append(source, matches, at, shape, pane, text, label, add);
    return [el(doc, 'div', 'dc-ts-field-label', 'Marks from a grid\'s rows'), rows,
      el(doc, 'div', 'dc-ts-hint', 'Each row the grid has that the rows match, at its date: drawn at a column\'s value (a trade at its price), or a line at its date.')];
  }

  /** Shade from a column in one step (Phase 5; FRED's recession bars): a 0/1 column of a source, its rows matched as a
   *  column's brackets are typed, shaded where it is not 0 -- a condition series, by the tile's one rule (`shade`). */
  #shadeFields(): HTMLElement[] {
    const doc = this.#doc;
    const spec = this.#spec;
    const columns = this.#columns(spec);
    const source = select(doc, spec.sources.map((s) => [s.alias, s.alias] as const), spec.sources[0]!.alias, () => paint(), 'Shade source');
    const column = el(doc, 'select', 'dc-input dc-ts-small', undefined, { 'aria-label': 'Shade column' });
    const matches = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Rows: US', 'aria-label': 'Rows shaded' });
    const name = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Name', 'aria-label': 'Shade name' });
    const paint = (): void => {
      const date = spec.sources.find((s) => s.alias === source.value)?.date;
      column.replaceChildren(...(columns.get(source.value) ?? []).filter((c) => isNumeric(c.type) && c.name !== date).map((c) => {
        const o = doc.createElement('option');
        o.value = c.name;
        o.textContent = c.name;
        return o;
      }));
    };
    paint();
    const add = button(doc, 'Shade', 'Shade every period where the column is not 0', () => {
      // nothing to shade by, said (the Phase 5 review's notes: the button did nothing)
      if (column.value === '') {
        this.#say([columns.get(source.value) === undefined ? `Source ${source.value}'s grid is not on the page.` : `Source ${source.value}'s grid has no number column to shade by.`], true);
        return;
      }
      let where: ReturnType<typeof parseMatches>;
      try {
        where = parseMatches(matches.value);
      } catch (e) {
        this.#say([e instanceof Error ? e.message : String(e)], true);
        return;
      }
      const words = name.value.trim();
      if (this.#edit({ kind: 'shade', column: column.value, source: source.value, ...(where.length > 0 ? { where } : {}), ...(words ? { name: words } : {}) })) this.#editSettings();
    });
    const row = el(doc, 'div', 'dc-ts-row');
    row.append(source, column, matches, name, add);
    return [el(doc, 'div', 'dc-ts-field-label', 'Shade where a column is not 0'), row];
  }

  /** A trend projected (Phase 5): a series of the tile, the observations its line is fit over, how far past its last
   *  date it is drawn on -- the fit the database's (projectionQuery), told by the tile's one rule (`annotate`). */
  #projectionFields(): HTMLElement[] {
    const doc = this.#doc;
    const spec = this.#spec;
    const which = select(doc, spec.series.map((x) => [x.id, `${x.id}: ${seriesName(x)}`] as const), spec.series[0]!.id, () => undefined, 'Projected series');
    const window = el(doc, 'input', 'dc-input dc-ts-small', undefined, {
      type: 'number', min: String(MIN_PROJECTION_WINDOW), step: '1', value: String(TS_PROJECTION_WINDOW), 'aria-label': 'Observations fit',
    });
    const ahead = el(doc, 'input', 'dc-input dc-ts-small', undefined, { value: TS_PROJECTION_AHEAD, 'aria-label': 'Projected ahead' });
    const name = el(doc, 'input', 'dc-input', undefined, { placeholder: 'Label', 'aria-label': 'Projection label' });
    const add = button(doc, 'Project', 'Draw the line fit over its last observations, and on past its last date', () => {
      const n = Number(window.value);
      const words = name.value.trim();
      const made: TsProjection = {
        kind: 'projection', series: which.value,
        ...(n !== TS_PROJECTION_WINDOW ? { window: n } : {}),
        ...(ahead.value.trim() !== TS_PROJECTION_AHEAD ? { ahead: ahead.value.trim() } : {}),
        ...(words ? { label: words } : {}),
      };
      if (this.#edit({ kind: 'annotate', annotation: made })) this.#editSettings();
    });
    const row = el(doc, 'div', 'dc-ts-row');
    row.append(which, window, ahead, name, add);
    return [el(doc, 'div', 'dc-ts-field-label', 'Project a trend: the line fit over its last observations, drawn on'), row];
  }

  /** A view's own fields in Settings: the series it reads (a scatter's x and y), a seasonality's years, a histogram's
   *  bins, a curve's dates -- each a change by the tile's one rule, refused in its words. */
  #viewFields(mode: TsMode): HTMLElement[] {
    const doc = this.#doc;
    const spec = this.#spec;
    const picked = viewSeries(spec);
    const out: HTMLElement[] = [];
    // the series a view can draw (timeseries-query.ts viewChoices: no band, no condition, no candles -- the Phase 3
    // review's 12: candles offered, then refused)
    const can = viewChoices(spec);
    const choices = (ids: readonly string[]): (readonly [string, string])[] =>
      ids.map((id) => spec.series.find((s) => s.id === id)!).map((s) => [s.id, `${s.id}: ${seriesName(s)}`] as const);
    const row = (label: string, ...controls: HTMLElement[]): HTMLElement => {
      const r = el(doc, 'div', 'dc-ts-row');
      r.append(el(doc, 'span', 'dc-ts-field-label', label), ...controls);
      return r;
    };
    if (mode === 'scatter') {
      const [x, y] = picked.scatter ?? ['', ''];
      const one = choices(can.oneLine);
      // a pick kept until its pair is whole (the Phase 3 review's 12: the first lost where the tile had no pair yet)
      let pending: string[] = [x, y];
      const pick = (which: 0 | 1, at: string): HTMLSelectElement => select(doc, [...(at === '' ? [['', 'Pick a series'] as const] : []), ...one], at, (v) => {
        // the other as the tile has it now (a pick before this one kept)
        const pair = [...(viewSeries(this.#spec).scatter ?? pending)];
        pair[which] = v;
        pending = pair;
        if (pair[0] !== '' && pair[1] !== '') this.#edit({ kind: 'view', view: { of: pair } });
      }, which === 0 ? 'Across (x)' : 'Up (y)');
      out.push(row('Across (x)', pick(0, x)), row('Up (y)', pick(1, y)));
    } else {
      const at = mode === 'curve' ? picked.curve : picked.one;
      const one = choices(mode === 'curve' ? can.split : can.oneLine);
      out.push(row('Series', select(doc, [...(at === undefined ? [['', 'Pick a series'] as const] : []), ...one], at ?? '', (v) => {
        if (v !== '') this.#edit({ kind: 'view', view: { of: [v] } });
      }, 'Series')));
    }
    const whole = (key: 'years' | 'bins', label: string, least: number, most: number, at: number): void => {
      const box = el(doc, 'input', 'dc-input dc-ts-small', undefined, { type: 'number', min: String(least), max: String(most), step: '1', 'aria-label': label });
      box.value = String(at);
      box.addEventListener('change', () => {
        // a number the box could not read is said, never taken for none (the Phase 2 review's 8)
        if (box.validity.badInput) {
          this.#say([`${label}: ${JSON.stringify(box.value)} is not a whole number.`], true);
          return;
        }
        this.#edit({ kind: 'view', view: { [key]: box.value.trim() === '' ? null : Number(box.value) } });
      });
      out.push(row(label, box));
    };
    if (mode === 'seasonality') whole('years', 'Years before', 1, MAX_SEASON_YEARS, spec.view?.years ?? TS_SEASON_YEARS);
    if (mode === 'histogram') whole('bins', 'Bins', MIN_HIST_BINS, MAX_HIST_BINS, spec.view?.bins ?? TS_HIST_BINS);
    if (mode === 'curve') {
      const box = el(doc, 'input', 'dc-input', undefined, { 'aria-label': 'Dates', placeholder: 'latest, 1m, 1y, 2024-03-29' });
      box.value = (spec.view?.dates ?? TS_CURVE_DATES).join(', ');
      box.addEventListener('change', () => {
        const told = box.value.split(',').map((d) => d.trim()).filter((d) => d !== '');
        this.#edit({ kind: 'view', view: { dates: told.length === 0 ? null : told } });
      });
      out.push(row('Dates', box), el(doc, 'div', 'dc-ts-hint', 'Each line as of each date: latest, a period back from it (1m, 1y), or a day.'));
    }
    return out;
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

