// THE TIMESERIES VIEWER'S SPEC (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §3): what a timeseries tile plots, in
// plain JSON -- a page view of its own (page-document.ts TimeseriesView), saved, undone, merged and made by Python as
// every tile is.
//
// A tile reads SOURCES -- grids of the page, each by its cube, with its own date column and instrument column -- and
// plots SERIES along one time axis. Each series is an EXPRESSION in the vocabulary of gs-quant's
// timeseries package -- the functions Goldman Sachs' PlotTool Pro exports to its expression language
// (`gs_quant.timeseries`, each marked `@plot_function`): returns, volatility, z-scores, rolling statistics, drawdowns,
// correlation and beta, Bollinger bands. A person types one (`volatility(px:close[AAPL], 63)`) or builds it from the
// menus; either way the spec keeps the tree, and its text is printed from the tree (timeseries-expr.ts `exprText`), so
// the words a person typed and the words saved never drift.
//
// NOTHING HERE COMPUTES A VALUE. An expression is words; timeseries-query.ts writes each node as a column of the
// series' query -- a window over the dates, an expression over the row -- which the planner plans and the database
// computes, as every calculated column of DataCube's is. This file says what is meant and refuses a saved spec that
// is not (tsProblem), in words: it is what a page reads at its start (page-document.ts), so it holds the spec, its
// catalog and its reader alone -- the expression language and the rules against a grid's columns are the viewer's
// (timeseries-expr.ts), fetched with the first timeseries tile.

import type { TimeGrain } from './chart-spec.ts';
import { readFilterNode } from './grid-commands.ts';
import type { FilterNode } from './snapshot.ts';

// ------------------------------------------------------------------ the spec

/** How a period's rows become its one value: the latest row's (a price), the earliest's, or an aggregate (a volume's sum). */
export type TsAggregate = 'last' | 'first' | 'average' | 'sum' | 'min' | 'max';

/** A match on a series' rows: `close[AAPL]` (its source's instrument column) or `close[venue=NYSE]`. */
export interface TsMatch {
  /** The column matched; absent, its source's instrument column (TsSource.key). */
  readonly column?: string;
  readonly value: string | number | boolean;
}

/** What a series plots: a tree of the grid's columns, numbers, other series and functions. */
export type TsExpr =
  /**
   * A column of a source along its date: its rows that match, each period's taken by `fn` (default `last`). `source`:
   * the alias of the source it reads (`px:close`); absent, the tile's first source.
   */
  | {
    readonly kind: 'column'; readonly source?: string; readonly column: string; readonly fn?: TsAggregate;
    readonly where?: readonly TsMatch[];
  }
  /** Another series' expression, by its id: `$A`. */
  | { readonly kind: 'ref'; readonly series: string }
  | { readonly kind: 'number'; readonly value: number }
  | { readonly kind: 'binary'; readonly op: TsOperator; readonly left: TsExpr; readonly right: TsExpr }
  /** A function of the catalog (TS_FUNCTIONS): its series arguments, then its parameters by name. */
  | {
    readonly kind: 'call'; readonly fn: TsFunctionName; readonly args: readonly TsExpr[];
    readonly params?: Readonly<Record<string, number | string>>;
  };

export type TsOperator = '+' | '-' | '*' | '/';

/** How a series is drawn. */
export interface TsStyle {
  /** A line (the default), the area under it, bars from zero, steps, dots; a Bollinger band is a band, OHLC candles. */
  readonly kind?: TsStyleKind;
  /** Its colour: a slot of the palette (0 its first), else its place among the lines. */
  readonly color?: number;
  /** Its line's width, 1 to 4 pixels (default 2). */
  readonly width?: number;
  readonly dash?: 'solid' | 'dashed' | 'dotted';
}

export type TsStyleKind = 'line' | 'area' | 'bars' | 'step' | 'dots';

export interface TsSeries {
  /** Its id -- a capital letter, as `$A` refers to it -- unique in the tile. */
  readonly id: string;
  readonly expr: TsExpr;
  /** Its name in the legend; else its expression's text. */
  readonly name?: string;
  /**
   * One line per value of this column (one per symbol), each its own partition of every window: a column of the source
   * the series reads first (sourcesOf).
   */
  readonly split?: string;
  readonly style?: TsStyle;
  /** The pane it is drawn in, 0 the main one at the top, up to MAX_PANES - 1. */
  readonly pane?: number;
  /** Its value axis in its pane; else by its unit (a second unit takes the right axis). */
  readonly axis?: 'left' | 'right';
  /** Left out of the drawing (its legend entry stays, to show it again). */
  readonly hidden?: boolean;
}

/** A period back from the latest date the series have, or all of them. */
export type TsRangeWord = '1m' | '3m' | '6m' | 'ytd' | '1y' | '2y' | '3y' | '5y' | '10y' | 'max';

/** The window a tile shows: a period back from the latest date (`last`), or between two dates (ISO, either open). */
export type TsRange = { readonly last: TsRangeWord } | { readonly from?: string; readonly to?: string };

/** A pane: its share of the height (the main pane's default 3, the others' 1), and its value axis' scale. */
export interface TsPane {
  readonly height?: number;
  readonly log?: boolean;
}

/** What is drawn over the series: a level, an event's date, a shaded period -- each with a label. */
export type TsAnnotation =
  | { readonly kind: 'level'; readonly value: number; readonly pane?: number; readonly label?: string }
  | { readonly kind: 'event'; readonly date: string; readonly label?: string }
  | { readonly kind: 'period'; readonly from: string; readonly to: string; readonly label?: string };

export type TsAnnotationKind = TsAnnotation['kind'];

/** The viewer's own settings: each off or on, every one with a default (TS_VIEW_DEFAULTS). */
export interface TsView {
  /** Every line from one start: 100 at the first date shown (`rebase`), or its percent change from there (`percent`). */
  readonly compare?: 'rebase' | 'percent';
  /** The legend above the chart, each line's latest value and change beside its name. */
  readonly legend?: boolean;
  /** Each line's latest value at its end. */
  readonly lastValues?: boolean;
  /** The navigator under the chart: a slider over every date, to zoom and pan. */
  readonly navigator?: boolean;
  /** The statistics of every line over the dates shown, under the chart. */
  readonly stats?: boolean;
  /** A crosshair across the panes, with every line's value at the date pointed at. */
  readonly crosshair?: boolean;
}

/**
 * A GRID THE TILE READS: its cube on the page (a grid's, or -- its grid removed -- one kept for the tile, detached), the
 * date column its rows are read along, the column naming each row's instrument, and the alias its expressions call it.
 */
export interface TsSource {
  /** A word, unique in the tile: `px:close`. */
  readonly alias: string;
  /** The cube it reads: the page's (a grid's id is its cube's). */
  readonly cube: string;
  /** Its date column: a StrictDate or a DateTime. */
  readonly date: string;
  /** Its column naming each row's instrument -- a symbol, a ticker, a tenor -- that `close[AAPL]` matches. */
  readonly key?: string;
  /**
   * FROZEN: what it keeps of its grid's query as it was when the tile was frozen -- the grid's filter then (none: it had
   * none) -- so a filter put on the grid since, or a click on another chart, leaves the tile as it was: a frozen chart's
   * rule (chart-spec.ts ChartSpec.kept). Every source of a tile keeps one, or none does (tsStateOf).
   */
  readonly kept?: TsKept;
}

/** What a frozen tile's source keeps of its grid's query (TsSource.kept). */
export interface TsKept {
  readonly filter?: FilterNode;
}

/**
 * A TIMESERIES TILE'S STATE, in a chart's words (chart-spec.ts ChartState): DYNAMIC, following its grids' filters and
 * the page's clicks; FROZEN, each grid's filter kept as it was. No PINNED: a timeseries' grouping is always its own --
 * its dates' periods and its instruments, never its grid's rows -- so there is nothing of its grid's to pin.
 */
export type TsState = 'dynamic' | 'frozen';
/** Its states, in the order its state button offers them. */
export const TS_STATES: readonly TsState[] = ['dynamic', 'frozen'];

/** The state a tile is in: frozen when its sources keep their filters (the reader holds them to all or none). */
export function tsStateOf(spec: TimeseriesSpec): TsState {
  return spec.sources[0]?.kept !== undefined ? 'frozen' : 'dynamic';
}

/** The filter a source's rows are read by: the one it keeps, frozen, else its grid's as it is now (`now`). */
export function filterRead(src: TsSource, now: FilterNode | undefined): FilterNode | undefined {
  return src.kept ? src.kept.filter : now;
}

export interface TimeseriesSpec {
  /** The grids it reads, the first the one an expression reads when it names none. */
  readonly sources: readonly TsSource[];
  /**
   * The ONE frequency of the tile -- each series' values taken to it by its column's aggregate -- so series of
   * different grids share one time axis. Absent: as the rows are.
   */
  readonly grain?: TimeGrain;
  readonly series: readonly TsSeries[];
  /** The window shown first. Absent: every date. */
  readonly range?: TsRange;
  readonly panes?: readonly TsPane[];
  readonly annotations?: readonly TsAnnotation[];
  readonly view?: TsView;
}

/** The view's settings before anyone sets one. */
export const TS_VIEW_DEFAULTS: Required<Omit<TsView, 'compare'>> = {
  legend: true,
  lastValues: true,
  navigator: true,
  stats: false,
  crosshair: true,
};

/** A view setting as the tile has it: told, else its default. */
export function viewSetting(ts: TimeseriesSpec, key: keyof typeof TS_VIEW_DEFAULTS): boolean {
  return ts.view?.[key] ?? TS_VIEW_DEFAULTS[key];
}

/** The most series a tile plots, and the most lines one split series draws (its first values, in order). */
export const MAX_TS_SERIES = 12;
/** The most grids a tile reads. */
export const MAX_TS_SOURCES = 8;
export const MAX_SPLIT_LINES = 20;
/** The most panes a tile stacks: the main one and three under it. */
export const MAX_PANES = 4;
/** The most points a series' query asks for: past it, the latest ones (a cut is said). */
export const TS_POINT_LIMIT = 100_000;

/** The range buttons, in their order, and how each is labelled. */
export const TS_RANGES: readonly { readonly word: TsRangeWord; readonly label: string }[] = [
  { word: '1m', label: '1M' }, { word: '3m', label: '3M' }, { word: '6m', label: '6M' }, { word: 'ytd', label: 'YTD' },
  { word: '1y', label: '1Y' }, { word: '2y', label: '2Y' }, { word: '3y', label: '3Y' }, { word: '5y', label: '5Y' },
  { word: '10y', label: '10Y' }, { word: 'max', label: 'Max' },
];

/** A period back as the compiler's `adjust` takes it: how many of which DurationUnit (ytd and max are their own). */
export const RANGE_STEP: Readonly<Partial<Record<TsRangeWord, readonly [number, 'MONTHS' | 'YEARS']>>> = {
  '1m': [1, 'MONTHS'], '3m': [3, 'MONTHS'], '6m': [6, 'MONTHS'], '1y': [1, 'YEARS'], '2y': [2, 'YEARS'],
  '3y': [3, 'YEARS'], '5y': [5, 'YEARS'], '10y': [10, 'YEARS'],
};

/**
 * Periods in a year at each grain: what annualizes a figure (gs-quant's `_get_annualization_factor`, which reads it off
 * the spacing of the dates -- here the grain says it, so a series with a gap is annualized the same).
 */
export const PERIODS_PER_YEAR: Readonly<Record<TimeGrain, number>> = {
  day: 252, week: 52, month: 12, quarter: 4, year: 1,
};

// ------------------------------------------------------------------ the catalog's signatures

export type TsFunctionName =
  | 'returns' | 'prices' | 'diff' | 'lag' | 'index' | 'rebase' | 'change' | 'percent_change' | 'annualize'
  | 'mean' | 'sum' | 'min' | 'max' | 'range' | 'std' | 'var' | 'zscores' | 'winsorize'
  | 'volatility' | 'drawdown' | 'max_drawdown' | 'sharpe_ratio'
  | 'correlation' | 'beta' | 'cov' | 'alpha' | 'r_squared' | 'hedge_ratio' | 'residual'
  | 'bollinger_bands' | 'rsi' | 'ohlc'
  | 'exponential_moving_average' | 'exponential_std' | 'exponential_volatility' | 'exponential_spread_volatility' | 'macd'
  | 'smoothed_moving_average'
  | 'log' | 'exp' | 'sqrt' | 'abs' | 'power' | 'floor' | 'ceil';

/** A function's parameter as a spec holds it: a whole number, a span (a window, a lag: a whole number of observations
 *  or a calendar period, `3m`), a number, or a word (a kind of return). */
export interface TsParamShape {
  readonly name: string;
  readonly kind: 'whole' | 'span' | 'number' | 'word';
  readonly default: number | string;
  readonly words?: readonly string[];
  /** The least a number may be. */
  readonly min?: number;
  /** What a number must be below (an exponential average's weight: under 1). */
  readonly below?: number;
}

/**
 * A FUNCTION'S SIGNATURE: how many series it reads (one, or two: a correlation's) and its parameters -- what a saved
 * spec is read by (tsProblem). What it is called, what it computes, its units and how it is drawn are the catalog's
 * words (timeseries-expr.ts TS_FUNCTIONS), fetched with the viewer: a page reads its tiles at its start, and needs none
 * of them to.
 */
export interface TsSignature {
  /** One, two (a correlation's), or four (a candle's: its open, high, low and close). */
  readonly series: 1 | 2 | 4;
  readonly params: readonly TsParamShape[];
}

const W = (fallback: number): TsParamShape => ({ name: 'w', kind: 'span', default: fallback, min: 0 });
const OBS: TsParamShape = { name: 'obs', kind: 'span', default: 1, min: 1 };
const RETURN_TYPE: TsParamShape = { name: 'type', kind: 'word', default: 'simple', words: ['simple', 'log', 'absolute'] };
const num = (name: string, fallback: number, min?: number): TsParamShape =>
  ({ name, kind: 'number', default: fallback, ...(min !== undefined ? { min } : {}) });
/** A whole number of observations alone (a recursion's: Wilder's smoothing counts them). */
const whole = (name: string, fallback: number, min = 1): TsParamShape => ({ name, kind: 'whole', default: fallback, min });
/** An exponential average's weight of the one before, 0 to under 1 (gs-quant's beta: 0.75). */
const BETA: TsParamShape = { name: 'beta', kind: 'number', default: 0.75, min: 0, below: 1 };
const one = (...params: TsParamShape[]): TsSignature => ({ series: 1, params });

/** Every function's signature, gs-quant's names (PlotTool Pro's words), in the catalog's order. */
export const TS_SIGNATURES: Readonly<Record<TsFunctionName, TsSignature>> = {
  returns: one(OBS, RETURN_TYPE), prices: one(num('initial', 1), RETURN_TYPE), diff: one(OBS), lag: one(OBS), annualize: one(),
  index: one(num('initial', 1)), rebase: one(num('base', 100)), change: one(), percent_change: one(),
  mean: one(W(22)), sum: one(W(0)), min: one(W(22)), max: one(W(22)), range: one(W(22)), std: one(W(22)), var: one(W(22)),
  zscores: one(W(0)), winsorize: one(num('limit', 2.5, 0)),
  volatility: one(W(22), RETURN_TYPE), drawdown: one(W(0)), max_drawdown: one(W(0)), sharpe_ratio: one(W(252)),
  correlation: { series: 2, params: [W(63)] }, beta: { series: 2, params: [W(63)] }, cov: { series: 2, params: [W(63)] },
  alpha: { series: 2, params: [W(63)] }, r_squared: { series: 2, params: [W(63)] }, hedge_ratio: { series: 2, params: [W(63)] },
  residual: { series: 2, params: [W(63)] },
  bollinger_bands: one(W(20), num('k', 2, 0)), rsi: one(whole('w', 14)), ohlc: { series: 4, params: [] },
  exponential_moving_average: one(BETA), exponential_std: one(BETA), exponential_volatility: one(BETA),
  exponential_spread_volatility: one(BETA), macd: one(whole('m', 12), whole('n', 26), whole('s', 1)),
  smoothed_moving_average: one(whole('w', 14)),
  log: one(), exp: one(), sqrt: one(), abs: one(), power: one(num('y', 2)), floor: one(num('value', 0)), ceil: one(num('value', 0)),
};

/**
 * A CALENDAR PERIOD as a window or a lag takes it (gs-quant's tenor, `helper._to_offset`): a whole number of days, weeks,
 * months or years -- `3m`, `1y` -- as the compiler's `adjust` takes it (a DurationUnit). gs-quant's hours are not taken:
 * a timeseries is read along its dates.
 */
export interface TsTenor {
  readonly n: number;
  readonly unit: 'DAYS' | 'WEEKS' | 'MONTHS' | 'YEARS';
}

const TENOR = /^([1-9][0-9]*)([dwmy])$/;
const TENOR_UNITS: Readonly<Record<string, TsTenor['unit']>> = { d: 'DAYS', w: 'WEEKS', m: 'MONTHS', y: 'YEARS' };

/** A span's calendar period (`3m`), or undefined: a whole number of observations, or no period. */
export function tenorOf(v: number | string): TsTenor | undefined {
  if (typeof v !== 'string') return undefined;
  const m = TENOR.exec(v);
  return m ? { n: Number(m[1]), unit: TENOR_UNITS[m[2]!]! } : undefined;
}

/** How many series a function reads, in words: one, two, or a candle's four. */
export function seriesWords(n: TsSignature['series']): string {
  return n === 1 ? 'one series' : n === 2 ? 'two series' : 'four series (its open, high, low and close)';
}

/** The values a column may take of each period. */
export const AGGREGATES: readonly TsAggregate[] = ['last', 'first', 'average', 'sum', 'min', 'max'];

/** An alias as a source may be called: a word, a letter first. */
export const ALIAS = /^[A-Za-z][A-Za-z0-9_]*$/;

// ------------------------------------------------------------------ the reader

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

const TS_KEYS: Readonly<Record<keyof TimeseriesSpec, true>> = {
  sources: true, grain: true, series: true, range: true, panes: true, annotations: true, view: true,
};
const SOURCE_KEYS: Readonly<Record<keyof TsSource, true>> = { alias: true, cube: true, date: true, key: true, kept: true };
const KEPT_KEYS: Readonly<Record<keyof TsKept, true>> = { filter: true };
const SERIES_KEYS: Readonly<Record<keyof TsSeries, true>> = {
  id: true, expr: true, name: true, split: true, style: true, pane: true, axis: true, hidden: true,
};
const STYLE_KEYS: Readonly<Record<keyof TsStyle, true>> = { kind: true, color: true, width: true, dash: true };
const PANE_KEYS: Readonly<Record<keyof TsPane, true>> = { height: true, log: true };
const VIEW_KEYS: Readonly<Record<keyof TsView, true>> = {
  compare: true, legend: true, lastValues: true, navigator: true, stats: true, crosshair: true,
};
const MATCH_KEYS: Readonly<Record<keyof TsMatch, true>> = { column: true, value: true };
const EXPR_KEYS: { readonly [K in TsExpr['kind']]: Readonly<Record<string, true>> } = {
  column: { kind: true, source: true, column: true, fn: true, where: true },
  ref: { kind: true, series: true },
  number: { kind: true, value: true },
  binary: { kind: true, op: true, left: true, right: true },
  call: { kind: true, fn: true, args: true, params: true },
};
const ANNOTATION_KEYS: { readonly [K in TsAnnotationKind]: Readonly<Record<string, true>> } = {
  level: { kind: true, value: true, pane: true, label: true },
  event: { kind: true, date: true, label: true },
  period: { kind: true, from: true, to: true, label: true },
};
const GRAINS: readonly TimeGrain[] = ['day', 'week', 'month', 'quarter', 'year'];
const STYLE_KINDS: readonly TsStyleKind[] = ['line', 'area', 'bars', 'step', 'dots'];
const DATE = /^\d{4}-\d{2}-\d{2}$/;

function unknownKey(o: Readonly<Record<string, unknown>>, keys: Readonly<Record<string, true>>, what: string): string | null {
  const extra = Object.keys(o).find((k) => !Object.hasOwn(keys, k));
  return extra === undefined ? null : `there is no ${JSON.stringify(extra)} in ${what} (it says ${Object.keys(keys).join(', ')})`;
}

function exprProblem(e: unknown, what: string, depth = 0): string | null {
  if (depth > 64) return `${what} is nested too deep`;
  if (!isRecord(e) || typeof e['kind'] !== 'string' || !Object.hasOwn(EXPR_KEYS, e['kind'])) {
    return `${what} is not an expression (a column, a ref, a number, a binary or a call)`;
  }
  const kind = e['kind'] as TsExpr['kind'];
  const extra = unknownKey(e, EXPR_KEYS[kind], `${what} (a ${kind})`);
  if (extra) return extra;
  switch (kind) {
    case 'column': {
      if (typeof e['column'] !== 'string' || e['column'] === '') return `${what} names no column`;
      if (e['source'] !== undefined && (typeof e['source'] !== 'string' || !ALIAS.test(e['source']))) {
        return `${what}'s source ${JSON.stringify(e['source'])} is not a source's alias (a word)`;
      }
      if (e['fn'] !== undefined && !AGGREGATES.includes(e['fn'] as TsAggregate)) {
        return `${what}'s period value ${JSON.stringify(e['fn'])} is not one of ${AGGREGATES.join(', ')}`;
      }
      const where = e['where'];
      if (where !== undefined) {
        if (!Array.isArray(where)) return `${what}'s matches are not a list`;
        for (const [i, m] of where.entries()) {
          if (!isRecord(m) || !['string', 'number', 'boolean'].includes(typeof m['value'])
            || (m['column'] !== undefined && typeof m['column'] !== 'string')) {
            return `${what}'s match ${i + 1} is not a value (and a column)`;
          }
          const mExtra = unknownKey(m, MATCH_KEYS, `${what}'s match ${i + 1}`);
          if (mExtra) return mExtra;
        }
      }
      return null;
    }
    case 'ref':
      return typeof e['series'] === 'string' && e['series'] !== '' ? null : `${what} names no series`;
    case 'number':
      return typeof e['value'] === 'number' && Number.isFinite(e['value']) ? null : `${what} is not a finite number`;
    case 'binary':
      if (!['+', '-', '*', '/'].includes(e['op'] as string)) return `${what}'s operator ${JSON.stringify(e['op'])} is not +, -, * or /`;
      return exprProblem(e['left'], `${what}'s left side`, depth + 1) ?? exprProblem(e['right'], `${what}'s right side`, depth + 1);
    case 'call': {
      const f = typeof e['fn'] === 'string' && Object.hasOwn(TS_SIGNATURES, e['fn']) ? TS_SIGNATURES[e['fn'] as TsFunctionName] : undefined;
      // a function this DataCube does not compute (gs-quant's recursions among them) is refused when it is typed, in
      // the parser's words (timeseries-expr.ts NOT_BUILT): a saved one is refused here by its name
      if (!f) return `${what} calls ${JSON.stringify(e['fn'] ?? null)}, which this DataCube does not compute`;
      const fn = e['fn'] as TsFunctionName;
      if (!Array.isArray(e['args']) || e['args'].length !== f.series) {
        return `${what}: ${fn} reads ${seriesWords(f.series)}`;
      }
      for (const [i, a] of e['args'].entries()) {
        const p = exprProblem(a, `${fn}'s series ${i + 1}`, depth + 1);
        if (p) return p;
      }
      const params = e['params'];
      if (params !== undefined) {
        if (!isRecord(params)) return `${what}'s parameters are not an object`;
        for (const [name, v] of Object.entries(params)) {
          const q = f.params.find((x) => x.name === name);
          if (!q) return `${fn} has no parameter ${JSON.stringify(name)} (it has ${f.params.map((x) => x.name).join(', ') || 'none'})`;
          if (q.kind === 'word') {
            if (!q.words!.includes(v as string)) return `${fn}'s ${name} ${JSON.stringify(v)} is not one of ${q.words!.join(', ')}`;
          } else if (q.kind === 'span' && typeof v === 'string') {
            if (tenorOf(v) === undefined) return `${fn}'s ${name} ${JSON.stringify(v)} is neither a whole number nor a calendar period (3m, 1y)`;
          } else {
            if (typeof v !== 'number' || !Number.isFinite(v)) return `${fn}'s ${name} ${JSON.stringify(v)} is not a number`;
            if ((q.kind === 'whole' || q.kind === 'span') && !Number.isInteger(v)) return `${fn}'s ${name} ${v} is not a whole number`;
            if (q.min !== undefined && v < q.min) return `${fn}'s ${name} ${v} is below ${q.min}`;
            if (q.below !== undefined && v >= q.below) return `${fn}'s ${name} ${v} is not below ${q.below}`;
          }
        }
      }
      return null;
    }
  }
}

/** The aliases an expression's columns name, each once (a column naming none reads the first source). */
function columnSourcesOf(e: TsExpr): string[] {
  const out = new Set<string>();
  const walk = (x: TsExpr): void => {
    if (x.kind === 'column' && x.source !== undefined) out.add(x.source);
    else if (x.kind === 'binary') {
      walk(x.left);
      walk(x.right);
    } else if (x.kind === 'call') x.args.forEach(walk);
  };
  walk(e);
  return [...out];
}

/** A date as a spec writes it, an ISO day. */
const isDate = (v: unknown): v is string => typeof v === 'string' && DATE.test(v) && !Number.isNaN(Date.parse(v));

/**
 * What stops a saved `ts` slot from being read, in words, or null: every key, word and number, a newer DataCube's
 * refused by name -- never ignored, never drawn as something else (chart-spec.ts specProblem's rule).
 */
export function tsProblem(ts: unknown): string | null {
  if (!isRecord(ts)) return 'its timeseries is not an object';
  const extra = unknownKey(ts, TS_KEYS, 'its timeseries');
  if (extra) return extra;
  const sources = ts['sources'];
  if (!Array.isArray(sources) || sources.length === 0) return 'its timeseries reads no grid (sources)';
  if (sources.length > MAX_TS_SOURCES) return `a timeseries reads at most ${MAX_TS_SOURCES} grids (it reads ${sources.length})`;
  const aliases = new Set<string>();
  for (const [i, src] of sources.entries()) {
    const what = `source ${i + 1}`;
    if (!isRecord(src)) return `${what} is not an object`;
    const srcExtra = unknownKey(src, SOURCE_KEYS, what);
    if (srcExtra) return srcExtra;
    if (typeof src['alias'] !== 'string' || !ALIAS.test(src['alias'])) return `${what}'s alias ${JSON.stringify(src['alias'] ?? null)} is not a word`;
    if (aliases.has(src['alias'])) return `two sources are called ${src['alias']}`;
    aliases.add(src['alias']);
    if (typeof src['cube'] !== 'string' || src['cube'] === '') return `source ${src['alias']} names no cube`;
    if (typeof src['date'] !== 'string' || src['date'] === '') return `source ${src['alias']} names no date column`;
    if (src['key'] !== undefined && (typeof src['key'] !== 'string' || src['key'] === '')) return `source ${src['alias']}'s key is not a column`;
    const kept = src['kept'];
    if (kept !== undefined) {
      if (!isRecord(kept)) return `what source ${src['alias']} keeps ${JSON.stringify(kept)} is not an object`;
      const keptExtra = unknownKey(kept, KEPT_KEYS, `what source ${src['alias']} keeps`);
      if (keptExtra) return keptExtra;
      if (kept['filter'] !== undefined) {
        try {
          readFilterNode(kept['filter']);
        } catch (e) {
          return `the filter source ${src['alias']} keeps is not one: ${e instanceof Error ? e.message : String(e)}`;
        }
      }
    }
  }
  // frozen whole or not at all: a tile half following its grids is no state its button says
  const keeping = sources.filter((src) => isRecord(src) && src['kept'] !== undefined).length;
  if (keeping > 0 && keeping < sources.length) return 'a frozen timeseries keeps every source\'s filter: some of its sources keep none';
  if (ts['grain'] !== undefined && !GRAINS.includes(ts['grain'] as TimeGrain)) {
    return `its timeseries' frequency ${JSON.stringify(ts['grain'])} is not one of ${GRAINS.join(', ')}`;
  }
  const series = ts['series'];
  if (!Array.isArray(series)) return 'its timeseries\' series are not a list';
  if (series.length > MAX_TS_SERIES) return `a timeseries plots at most ${MAX_TS_SERIES} series (it has ${series.length})`;
  const ids = new Set<string>();
  for (const [i, s] of series.entries()) {
    const what = `series ${i + 1}`;
    if (!isRecord(s)) return `${what} is not an object`;
    const sExtra = unknownKey(s, SERIES_KEYS, what);
    if (sExtra) return sExtra;
    if (typeof s['id'] !== 'string' || !/^[A-Z][A-Z0-9]*$/.test(s['id'])) return `${what}'s id ${JSON.stringify(s['id'] ?? null)} is not a capital letter (A, B, ...)`;
    if (ids.has(s['id'])) return `two series are ${s['id']}`;
    ids.add(s['id']);
    const p = exprProblem(s['expr'], `series ${s['id']}`);
    if (p) return p;
    const named = columnSourcesOf(s['expr'] as TsExpr).find((a) => !aliases.has(a));
    if (named !== undefined) return `series ${s['id']} reads ${named}:..., which is none of its sources (${[...aliases].join(', ')})`;
    if (s['name'] !== undefined && typeof s['name'] !== 'string') return `series ${s['id']}'s name is not text`;
    if (s['split'] !== undefined && typeof s['split'] !== 'string') return `series ${s['id']}'s split is not a column`;
    if (s['pane'] !== undefined && !(Number.isInteger(s['pane']) && (s['pane'] as number) >= 0 && (s['pane'] as number) < MAX_PANES)) {
      return `series ${s['id']}'s pane ${JSON.stringify(s['pane'])} is not one of 0 to ${MAX_PANES - 1}`;
    }
    if (s['axis'] !== undefined && s['axis'] !== 'left' && s['axis'] !== 'right') return `series ${s['id']}'s axis ${JSON.stringify(s['axis'])} is not left or right`;
    if (s['hidden'] !== undefined && typeof s['hidden'] !== 'boolean') return `series ${s['id']}'s hidden is not true or false`;
    const style = s['style'];
    if (style !== undefined) {
      if (!isRecord(style)) return `series ${s['id']}'s style is not an object`;
      const stExtra = unknownKey(style, STYLE_KEYS, `series ${s['id']}'s style`);
      if (stExtra) return stExtra;
      if (style['kind'] !== undefined && !STYLE_KINDS.includes(style['kind'] as TsStyleKind)) {
        return `series ${s['id']}'s style ${JSON.stringify(style['kind'])} is not one of ${STYLE_KINDS.join(', ')}`;
      }
      if (style['color'] !== undefined && !(Number.isInteger(style['color']) && (style['color'] as number) >= 0)) {
        return `series ${s['id']}'s colour ${JSON.stringify(style['color'])} is not a slot of the palette (0, 1, ...)`;
      }
      if (style['width'] !== undefined && !(typeof style['width'] === 'number' && style['width'] >= 1 && style['width'] <= 4)) {
        return `series ${s['id']}'s width ${JSON.stringify(style['width'])} is not 1 to 4`;
      }
      if (style['dash'] !== undefined && !['solid', 'dashed', 'dotted'].includes(style['dash'] as string)) {
        return `series ${s['id']}'s dash ${JSON.stringify(style['dash'])} is not solid, dashed or dotted`;
      }
    }
  }
  const range = ts['range'];
  if (range !== undefined) {
    if (!isRecord(range)) return 'its timeseries\' range is not an object';
    if (range['last'] !== undefined) {
      if (Object.keys(range).length !== 1) return 'its timeseries\' range is a period back (last) or two dates (from, to), not both';
      if (!TS_RANGES.some((r) => r.word === range['last'])) return `its timeseries' range ${JSON.stringify(range['last'])} is not one of ${TS_RANGES.map((r) => r.word).join(', ')}`;
    } else {
      const rExtra = unknownKey(range, { from: true, to: true }, 'its timeseries\' range');
      if (rExtra) return rExtra;
      for (const end of ['from', 'to'] as const) {
        if (range[end] !== undefined && !isDate(range[end])) return `its timeseries' range's ${end} ${JSON.stringify(range[end])} is not a date (2024-01-31)`;
      }
      if (typeof range['from'] === 'string' && typeof range['to'] === 'string' && range['from'] > range['to']) {
        return 'its timeseries\' range ends before it starts';
      }
    }
  }
  const panes = ts['panes'];
  if (panes !== undefined) {
    if (!Array.isArray(panes) || panes.length > MAX_PANES) return `its timeseries' panes are not a list of at most ${MAX_PANES}`;
    for (const [i, pane] of panes.entries()) {
      if (!isRecord(pane)) return `pane ${i} is not an object`;
      const pExtra = unknownKey(pane, PANE_KEYS, `pane ${i}`);
      if (pExtra) return pExtra;
      if (pane['height'] !== undefined && !(typeof pane['height'] === 'number' && pane['height'] > 0 && pane['height'] <= 10)) {
        return `pane ${i}'s height ${JSON.stringify(pane['height'])} is not a share from above 0 to 10`;
      }
      if (pane['log'] !== undefined && typeof pane['log'] !== 'boolean') return `pane ${i}'s log is not true or false`;
    }
  }
  const annotations = ts['annotations'];
  if (annotations !== undefined) {
    if (!Array.isArray(annotations)) return 'its timeseries\' annotations are not a list';
    for (const [i, a] of annotations.entries()) {
      const what = `annotation ${i + 1}`;
      if (!isRecord(a) || !Object.hasOwn(ANNOTATION_KEYS, a['kind'] as string)) return `${what} is not a level, an event or a period`;
      const aExtra = unknownKey(a, ANNOTATION_KEYS[a['kind'] as TsAnnotationKind], `${what} (a ${a['kind']})`);
      if (aExtra) return aExtra;
      if (a['label'] !== undefined && typeof a['label'] !== 'string') return `${what}'s label is not text`;
      if (a['kind'] === 'level') {
        if (typeof a['value'] !== 'number' || !Number.isFinite(a['value'])) return `${what} has no value`;
        if (a['pane'] !== undefined && !(Number.isInteger(a['pane']) && (a['pane'] as number) >= 0 && (a['pane'] as number) < MAX_PANES)) {
          return `${what}'s pane is not one of 0 to ${MAX_PANES - 1}`;
        }
      }
      if (a['kind'] === 'event' && !isDate(a['date'])) return `${what}'s date ${JSON.stringify(a['date'] ?? null)} is not a date (2024-01-31)`;
      if (a['kind'] === 'period') {
        if (!isDate(a['from']) || !isDate(a['to'])) return `${what} needs a from and a to, each a date (2024-01-31)`;
        if (a['from'] > a['to']) return `${what} ends before it starts`;
      }
    }
  }
  const view = ts['view'];
  if (view !== undefined) {
    if (!isRecord(view)) return 'its timeseries\' view is not an object';
    const vExtra = unknownKey(view, VIEW_KEYS, 'its timeseries\' view');
    if (vExtra) return vExtra;
    if (view['compare'] !== undefined && view['compare'] !== 'rebase' && view['compare'] !== 'percent') {
      return `its timeseries' compare ${JSON.stringify(view['compare'])} is not rebase or percent`;
    }
    for (const flag of ['legend', 'lastValues', 'navigator', 'stats', 'crosshair'] as const) {
      if (view[flag] !== undefined && typeof view[flag] !== 'boolean') return `its timeseries' ${flag} is not true or false`;
    }
  }
  return null;
}

/**
 * A saved tile's spec as a page reads it (page-document.ts), and as its commands take one (page-commands.ts): the spec,
 * or what stops it from being read, in words (tsProblem). Python's are read the same, headless: one reader.
 */
export function readTimeseries(spec: unknown): TimeseriesSpec | string {
  const problem = tsProblem(spec);
  return problem !== null ? problem : spec as TimeseriesSpec;
}

/** The cubes a tile reads, each once, in its sources' order: what the page keeps its sources' references to. */
export function cubesOf(ts: Pick<TimeseriesSpec, 'sources'>): string[] {
  return [...new Set(ts.sources.map((s) => s.cube))];
}
