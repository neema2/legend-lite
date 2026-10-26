// A TIMESERIES' EXPRESSIONS AND ITS RULES AGAINST ITS GRIDS (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §3-§4): the
// expression language a person types (parseExpr) and the text the spec's tree prints as (exprText); what an
// expression reads, what its figures are and what it inlines; what stops a spec from being drawn over its grids' columns
// (timeseriesProblems); and a first source and a first series of a grid (sourceOf, defaultTimeseries).
//
// Apart from the spec's reader (timeseries-spec.ts, which a page reads at its start): this is the viewer's and the
// headless tile's, fetched with the first timeseries tile drawn (page/cube-page.ts) -- a page with none loads none of it
// (test/bundle-budget.test.ts). Nothing here computes a value either: it reads and writes words.

import { isNumeric, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import {
  AGGREGATES, ALIAS, seriesWords, tenorOf, TS_SIGNATURES, type TimeseriesSpec, type TsAggregate, type TsExpr, type TsFunctionName, type TsMatch,
  type TsOperator, type TsParamShape, type TsSeries, type TsSource,
} from './timeseries-spec.ts';

// ------------------------------------------------------------------ the catalog

/** What a function's figures are, so the axis and the tooltip say them right. */
export type TsUnit =
  /** As its input: a level, a price, the grid's own format. */
  | 'same'
  /** A ratio, written as a percentage: a return, a drawdown. */
  | 'ratio'
  /** Already in percent (gs-quant's volatility: 20% is 20.0). */
  | 'percent'
  /** A plain number: a z-score, a beta, a correlation. */
  | 'number'
  /** An index level: 100 at its start. */
  | 'index';

/** A function's parameter (its signature's, timeseries-spec.ts TS_SIGNATURES) and what it is, in a line. */
export interface TsParam extends TsParamShape {
  readonly about: string;
}

export type TsFamily = 'Returns' | 'Level' | 'Rolling statistics' | 'Risk' | 'Relationships' | 'Technicals' | 'Math';

export interface TsFunction {
  readonly name: TsFunctionName;
  readonly label: string;
  readonly family: TsFamily;
  /** How many series it reads: one, two (a correlation's), or four (a candle's). */
  readonly series: 1 | 2 | 4;
  readonly params: readonly TsParam[];
  /** What its figures are; `input` where it depends on a parameter (returns' type). */
  readonly unit: TsUnit | 'input';
  readonly about: string;
  /** The gs-quant function it is, `module.name`, or DataCube's own where gs-quant has none. */
  readonly gsQuant: string;
  /** Where it differs from gs-quant's, said. */
  readonly differs?: string;
  /** It reads the FIRST VALUE SHOWN (an index's base): computed after the window shown starts (timeseries-query.ts). */
  readonly anchored?: true;
  /** It draws two lines, a band between them (a Bollinger band): nothing may read it. */
  readonly band?: true;
  /** It draws a candle per period, of its four series (an open, a high, a low, a close): nothing may read it. */
  readonly candles?: true;
  /** Other names a person may type for it. */
  readonly aliases?: readonly string[];
  /** The least and the most its figures can be (an RSI's 0 and 100): its axis shows them whole. */
  readonly bounds?: readonly [number, number];
  /**
   * Whether its figures are a LEVEL -- a price, an index: what compare starts from one value and the statistics read a
   * volatility, a drawdown, a growth of (levelOf). `keeps`: a level of a level (a moving average of a price); `makes`: a
   * level of anything (prices compounded from returns, an index). Absent: never a level (a return, a difference, a
   * deviation -- a figure that may be zero or below, whose ratios mean nothing).
   */
  readonly level?: 'keeps' | 'makes';
}

/** A function's words: all of it but its signature, and what each parameter is (`w` and `obs`, `type` said once). */
type TsWords = Omit<TsFunction, 'name' | 'series' | 'params'> & { readonly params?: Readonly<Record<string, string>> };

const PARAM_ABOUT: Readonly<Record<string, string>> = {
  w: 'observations in the window, or a calendar period (3m, 1y: the dates after one that far back, to this one); 0 from the start',
  obs: 'observations back, or a calendar period back (1y: the value as of a year before, its last at or before)',
  type: 'simple: x / x[-obs] - 1; log: ln x - ln x[-obs]; absolute: x - x[-obs]',
};

/** Where every rolling figure's first value differs from gs-quant's: drawn at the observation its window first holds
 *  w, where gs-quant's ramp (apply_ramp, x[w:]) drops that one too. */
const RAMP_DIFFERS = 'a window of w observations drawn from the observation it first holds w (gs-quant\'s ramp starts one observation later); of a calendar period, from its first date that period on, as gs-quant\'s';
/** Where a lag by a calendar period differs: the value as of the date that far back, where gs-quant's lag moves each
 *  date on by the period and keeps only the dates it lands on. */
const OBS_DIFFERS = 'obs a calendar period back is the value as of that date, its last at or before it (gs-quant\'s only on the dates whose date that far back has a value)';
/** Where beta differs on two calendars: each series' returns on its own dates, joined on the dates both have. */
const BETA_DIFFERS = 'on two calendars, over the dates both have (gs-quant takes the covariance over every date either has, over the benchmark\'s own variance)';

/**
 * THE FUNCTIONS' WORDS, gs-quant's names first (PlotTool Pro's words), each with what it computes -- and, where
 * DataCube's differs, how. Every one is written by timeseries-query.ts as columns the database computes.
 */
const WORDS: Readonly<Record<TsFunctionName, TsWords>> = {
  // returns
  returns: { label: 'Returns', family: 'Returns', unit: 'input', about: 'The return over obs observations.', gsQuant: 'econometrics.returns' },
  prices: { level: 'makes',
    // a level compounded from initial, as an index is (the timeseries gallery, 2026-10-25: as its returns' unit, a
    // level of 1,139 read as 113,949.58%)
    label: 'Prices from returns', family: 'Returns', unit: 'index', params: { initial: 'the level it starts from' },
    about: 'A level compounded from a series of returns, from initial: initial times the product of (1 + each return) to date.',
    gsQuant: 'econometrics.prices',
  },
  diff: { label: 'Difference', family: 'Returns', unit: 'same', about: 'The change over obs observations: x - x[-obs].', gsQuant: 'analysis.diff' },
  lag: { level: 'keeps',
    label: 'Lag', family: 'Returns', unit: 'same', about: 'The value obs observations before, or as of a calendar period before.', gsQuant: 'analysis.lag',
  },
  annualize: {
    label: 'Annualize', family: 'Returns', unit: 'same',
    about: 'Times the square root of the periods in a year (252 days, 52 weeks, 12 months, 4 quarters).',
    gsQuant: 'econometrics.annualize', differs: 'with a frequency, the periods in a year are the tile\'s (gs-quant always reads them off the dates, as DataCube does with none)',
  },
  // level
  index: { level: 'makes',
    label: 'Index', family: 'Level', unit: 'index', params: { initial: 'its value as of the first date shown' },
    about: 'Divided by its value as of the first date shown (its last at or before it), times initial.', gsQuant: 'econometrics.index', anchored: true,
  },
  rebase: { level: 'makes',
    label: 'Rebase to 100', family: 'Level', unit: 'index', params: { base: 'its value as of the first date shown' },
    about: 'Its value as of the first date shown (its last at or before it) made base (100): index(x, 100).', gsQuant: 'DataCube (econometrics.index)', anchored: true,
  },
  change: { label: 'Change from start', family: 'Level', unit: 'same', about: 'Less its value as of the first date shown (its last at or before it).', gsQuant: 'econometrics.change', anchored: true },
  percent_change: {
    label: '% change from start', family: 'Level', unit: 'ratio', about: 'Its change from its value as of the first date shown (its last at or before it), as a share of it.',
    gsQuant: 'DataCube (econometrics.index - 1)', anchored: true,
  },
  // rolling statistics
  mean: { level: 'keeps',
    label: 'Moving average', family: 'Rolling statistics', unit: 'same', about: 'The average over the last w observations.',
    gsQuant: 'statistics.mean, technicals.moving_average', aliases: ['moving_average', 'sma', 'ma', 'average'],
    differs: 'w defaults to 22 observations (gs-quant: every observation)',
  },
  sum: {
    label: 'Rolling sum', family: 'Rolling statistics', unit: 'same', about: 'The sum over the last w observations; from the start, a running total.',
    gsQuant: 'statistics.sum_', aliases: ['sum_', 'cumsum'],
  },
  min: { level: 'keeps', label: 'Rolling minimum', family: 'Rolling statistics', unit: 'same', about: 'The least over the last w observations.', gsQuant: 'statistics.min_', aliases: ['min_'], differs: 'w defaults to 22 observations (gs-quant: every observation)' },
  max: { level: 'keeps', label: 'Rolling maximum', family: 'Rolling statistics', unit: 'same', about: 'The most over the last w observations.', gsQuant: 'statistics.max_', aliases: ['max_'], differs: 'w defaults to 22 observations (gs-quant: every observation)' },
  range: {
    label: 'Rolling range', family: 'Rolling statistics', unit: 'same', about: 'The most less the least over the last w observations.',
    gsQuant: 'statistics.range_', aliases: ['range_'],
    differs: 'w defaults to 22 observations (gs-quant: every observation)',
  },
  std: {
    label: 'Rolling standard deviation', family: 'Rolling statistics', unit: 'same', about: 'The sample standard deviation over the last w observations.',
    gsQuant: 'statistics.std', aliases: ['stdev'],
    differs: 'w defaults to 22 observations (gs-quant: every observation)',
  },
  // in its input's units squared: a number, never a ratio's or a price's units (the second audit's N5)
  var: {
    label: 'Rolling variance', family: 'Rolling statistics', unit: 'number', about: 'The sample variance over the last w observations.',
    gsQuant: 'statistics.var', aliases: ['variance'],
    differs: 'w defaults to 22 observations (gs-quant: every observation)',
  },
  zscores: {
    label: 'Z-score', family: 'Rolling statistics', unit: 'number', params: { w: 'observations in the window; 0 every observation' },
    about: 'How many standard deviations from the mean: over the last w observations, or (0) every one.',
    gsQuant: 'statistics.zscores', aliases: ['zscore', 'z'],
  },
  winsorize: { level: 'keeps',
    label: 'Winsorize', family: 'Rolling statistics', unit: 'same', params: { limit: 'standard deviations from the mean it is held within' },
    about: 'Held within limit standard deviations of its mean over every observation.', gsQuant: 'statistics.winsorize',
  },
  // risk
  volatility: {
    label: 'Realized volatility', family: 'Risk', unit: 'percent',
    about: 'The annualized standard deviation of its returns over the last w observations, in percent (20% is 20).',
    gsQuant: 'econometrics.volatility', aliases: ['vol'],
    differs: 'w defaults to 22 observations (gs-quant: the whole series)',
  },
  drawdown: {
    label: 'Drawdown', family: 'Risk', unit: 'ratio', params: { w: 'observations its peak is taken over; 0 from the start' },
    about: 'Below its running peak, as a share of it: x / max(x) - 1.', gsQuant: 'DataCube (econometrics.max_drawdown\'s first step)',
  },
  max_drawdown: {
    label: 'Maximum drawdown', family: 'Risk', unit: 'ratio', about: 'The deepest drawdown over the last w observations (-0.2 for 20%).',
    gsQuant: 'econometrics.max_drawdown',
  },
  sharpe_ratio: {
    label: 'Sharpe ratio', family: 'Risk', unit: 'number',
    about: 'Its annualized mean return over its annualized volatility, over the last w observations.', gsQuant: 'econometrics.sharpe_ratio',
    differs: 'w defaults to 252 observations (gs-quant: every observation); a risk-free rate of zero and the arithmetic mean of simple returns; gs-quant takes excess returns over a currency\'s rate and compounds',
  },
  // relationships
  correlation: {
    label: 'Rolling correlation', family: 'Relationships', unit: 'number',
    about: 'The correlation of the two series\' simple returns over the last w observations, on the dates both have.',
    gsQuant: 'econometrics.correlation', aliases: ['corr'],
    differs: 'w defaults to 63 observations (gs-quant: every observation)',
  },
  beta: {
    label: 'Rolling beta', family: 'Relationships', unit: 'number',
    about: 'The first\'s beta to the second (a benchmark): the covariance of their simple returns over the second\'s variance.',
    gsQuant: 'econometrics.beta',
    differs: 'w defaults to 63 observations (gs-quant: every observation)',
  },
  cov: {
    label: 'Rolling covariance', family: 'Relationships', unit: 'number',
    about: 'The sample covariance of the two series as they are, over the last w observations.', gsQuant: 'statistics.cov',
    differs: 'w defaults to 63 observations (gs-quant: every observation)',
  },
  // a regression of the first on the second over a rolling window (the expert functions, 2026-10-26): DataCube's own
  alpha: {
    label: 'Rolling alpha', family: 'Relationships', unit: 'ratio',
    about: 'The first\'s return above what its beta to the second explains: the mean of its simple returns less beta times '
      + 'the second\'s, annualized (Jensen\'s, a risk-free rate of zero), over the last w observations.',
    gsQuant: 'DataCube (econometrics.beta\'s regression)',
  },
  r_squared: {
    label: 'Rolling R²', family: 'Relationships', unit: 'number', bounds: [0, 1],
    about: 'How much of the first\'s returns the second\'s explain: their correlation squared, over the last w observations.',
    gsQuant: 'DataCube (econometrics.correlation squared)', aliases: ['rsquared', 'r2'],
  },
  hedge_ratio: {
    label: 'Hedge ratio', family: 'Relationships', unit: 'number',
    about: 'How many of the second hedge one of the first: the slope of the first\'s levels on the second\'s (least '
      + 'squares: their covariance over the second\'s variance), over the last w observations.',
    gsQuant: 'DataCube (statistics.cov over statistics.var)',
  },
  residual: {
    label: 'Regression residual', family: 'Relationships', unit: 'same',
    about: 'The first less what its rolling regression on the second gives for it: the spread of a pairs trade, '
      + 'x - (a + b·y) with a and b fitted over the last w observations of the levels.',
    gsQuant: 'DataCube (a pairs trade\'s spread)', aliases: ['spread'],
  },
  // technicals
  bollinger_bands: { level: 'keeps',
    label: 'Bollinger bands', family: 'Technicals', unit: 'same', params: { k: 'standard deviations either side' },
    about: 'A band k standard deviations either side of the moving average over the last w observations.',
    gsQuant: 'technicals.bollinger_bands', band: true, aliases: ['bollinger'],
    differs: 'w defaults to 20 observations (gs-quant: every observation)',
  },
  ohlc: { level: 'keeps',
    label: 'Candles (open, high, low, close)', family: 'Technicals', unit: 'same', candles: true, aliases: ['candles', 'candlestick'],
    about: 'A candle per period: its first open, its highest high, its lowest low and its last close (each a column of the '
      + 'grid, taken so by the period unless its expression says how), green when it closed above its open, red below.',
    gsQuant: 'DataCube (PlotTool Pro\'s candlesticks)',
  },
  rsi: {
    label: 'Relative strength index', family: 'Technicals', unit: 'number',
    about: '100 - 100 / (1 + average gain / average loss) over the last w changes: 0 to 100.',
    gsQuant: 'technicals.relative_strength_index', aliases: ['relative_strength_index'], bounds: [0, 100],
    differs: 'simple averages of the gains and losses (Cutler\'s); gs-quant smooths them (Wilder\'s), a recursion the database\'s windows have not',
  },
  // math
  log: { label: 'Natural log', family: 'Math', unit: 'number', about: 'ln x.', gsQuant: 'algebra.log' },
  exp: { label: 'Exponential', family: 'Math', unit: 'number', about: 'e to the x.', gsQuant: 'algebra.exp' },
  sqrt: { label: 'Square root', family: 'Math', unit: 'same', about: 'The square root.', gsQuant: 'algebra.sqrt' },
  abs: { label: 'Absolute value', family: 'Math', unit: 'same', about: '|x|.', gsQuant: 'algebra.abs_', aliases: ['abs_'] },
  power: { label: 'Power', family: 'Math', unit: 'number', params: { y: 'the power' }, about: 'x to the power y.', gsQuant: 'algebra.power' },
  floor: { level: 'keeps',
    label: 'Floor at a value', family: 'Math', unit: 'same', params: { value: 'the least it may be' },
    about: 'Never below value: the greater of x and value.', gsQuant: 'algebra.floor',
  },
  ceil: { level: 'keeps',
    label: 'Cap at a value', family: 'Math', unit: 'same', params: { value: 'the most it may be' },
    about: 'Never above value: the lesser of x and value.', gsQuant: 'algebra.ceil',
  },
};

/** THE FUNCTIONS: each one's signature (TS_SIGNATURES, what a saved spec is read by) with its words, in the catalog's
 *  order. */
export const TS_FUNCTIONS: readonly TsFunction[] = (Object.keys(WORDS) as TsFunctionName[]).map((name) => {
  const { params: about, ...words } = WORDS[name];
  const sig = TS_SIGNATURES[name];
  const params = sig.params.map((p): TsParam => {
    const said = about?.[p.name] ?? PARAM_ABOUT[p.name];
    if (said === undefined) throw new Error(`${name}'s parameter ${p.name} is said by nothing`);
    return { ...p, about: said };
  });
  // every rolling figure's ramp, and beta's calendar, said where they differ (the second audit's N1, N2: undeclared)
  const differs = [words.differs, sig.params.some((p) => p.name === 'w') ? RAMP_DIFFERS : undefined,
    sig.params.some((p) => p.name === 'obs') ? OBS_DIFFERS : undefined,
    name === 'beta' ? BETA_DIFFERS : undefined].filter((d): d is string => d !== undefined);
  return { name, series: sig.series, params, ...words, ...(differs.length > 0 ? { differs: differs.join('; ') } : {}) };
});

/** The functions by name. */
export const TS_FUNCTION: ReadonlyMap<TsFunctionName, TsFunction> = new Map(TS_FUNCTIONS.map((f) => [f.name, f]));

/**
 * gs-quant's functions DataCube does not compute, each with why: typed, they are refused in these words rather than
 * read as a column of the grid.
 */
export const NOT_BUILT: Readonly<Record<string, string>> = {
  exponential_moving_average: 'an exponential average is a recursion (each value from the one before), which the database\'s windows have not: use mean(x, w)',
  ema: 'an exponential average is a recursion (each value from the one before), which the database\'s windows have not: use mean(x, w)',
  macd: 'MACD is a difference of exponential averages, a recursion the database\'s windows have not',
  exponential_volatility: 'an exponentially weighted volatility is a recursion the database\'s windows have not: use volatility(x, w)',
  exponential_std: 'an exponentially weighted deviation is a recursion the database\'s windows have not: use std(x, w)',
  smoothed_moving_average: 'a smoothed average is a recursion the database\'s windows have not: use mean(x, w)',
  seasonally_adjusted: 'a seasonal decomposition is a model fitted to the series, not a window over it',
  trend: 'a trend from a seasonal decomposition is a model fitted to the series, not a window over it',
  median: 'a rolling median is a window the planner does not write yet: use mean(x, w) or zscores(x, w)',
  percentiles: 'a rolling percentile rank is a window the planner does not write yet',
  excess_returns: 'excess returns need a risk-free rate series: divide by one of your own',
};


/** Every name a person may type, an alias to its function. */
const TYPED_NAMES: ReadonlyMap<string, TsFunction> = new Map(TS_FUNCTIONS.flatMap((f) => [
  [f.name, f] as const, ...(f.aliases ?? []).map((a) => [a, f] as const),
]));

/** The function a typed name means, by its name or an alias. */
export function functionNamed(name: string): TsFunction | undefined {
  return TYPED_NAMES.get(name.toLowerCase());
}

/** A function's parameter as a call has it: told, else its default. */
export function paramOf(call: Extract<TsExpr, { kind: 'call' }>, name: string): number | string {
  const f = TS_FUNCTION.get(call.fn);
  const p = f?.params.find((q) => q.name === name);
  if (!p) throw new Error(`${call.fn} has no parameter ${name}`);
  return call.params?.[name] ?? p.default;
}

// ------------------------------------------------------------------ what an expression reads and is

/** The series an expression reads, by id (its `$A`s), each once. */
export function refsOf(e: TsExpr): string[] {
  const out = new Set<string>();
  const walk = (x: TsExpr): void => {
    if (x.kind === 'ref') out.add(x.series);
    else if (x.kind === 'binary') {
      walk(x.left);
      walk(x.right);
    } else if (x.kind === 'call') x.args.forEach(walk);
  };
  walk(e);
  return [...out];
}

/** A column read as an expression: `column`, of `source` when it is given (absent: the tile's first source), its rows
 *  where its instrument is `instrument` when it is given (absent: every row). */
export function columnRef(column: string, source?: string, instrument?: string): TsExpr {
  return {
    kind: 'column', ...(source !== undefined ? { source } : {}), column, ...(instrument !== undefined ? { where: [{ value: instrument }] } : {}),
  };
}

/** The alias of the source a column of an expression reads: its own, else the tile's first source's. */
export function sourceAlias(ts: Pick<TimeseriesSpec, 'sources'>, e: Extract<TsExpr, { kind: 'column' }>): string | undefined {
  return e.source ?? ts.sources[0]?.alias;
}

/** The sources an expression reads, by alias, in the order it first reads them (refs inlined first: `inlined`). */
export function sourcesOf(ts: Pick<TimeseriesSpec, 'sources'>, e: TsExpr): string[] {
  const out: string[] = [];
  const walk = (x: TsExpr): void => {
    if (x.kind === 'column') {
      const a = sourceAlias(ts, x);
      if (a !== undefined && !out.includes(a)) out.push(a);
    } else if (x.kind === 'binary') {
      walk(x.left);
      walk(x.right);
    } else if (x.kind === 'call') x.args.forEach(walk);
  };
  walk(e);
  return out;
}

/**
 * The columns an expression reads of each source -- its values' and its matches', a match of no column its source's
 * instrument column -- each once, by alias.
 */
export function columnsOf(ts: Pick<TimeseriesSpec, 'sources'>, e: TsExpr): Map<string, string[]> {
  const out = new Map<string, string[]>();
  const add = (alias: string, c: string): void => {
    const cs = out.get(alias) ?? [];
    if (!cs.includes(c)) cs.push(c);
    out.set(alias, cs);
  };
  const walk = (x: TsExpr): void => {
    if (x.kind === 'column') {
      const alias = sourceAlias(ts, x);
      if (alias === undefined) return;
      add(alias, x.column);
      const key = ts.sources.find((src) => src.alias === alias)?.key;
      for (const m of x.where ?? []) {
        const c = m.column ?? key;
        if (c !== undefined) add(alias, c);
      }
    } else if (x.kind === 'binary') {
      walk(x.left);
      walk(x.right);
    } else if (x.kind === 'call') x.args.forEach(walk);
  };
  walk(e);
  return out;
}

/** An alias for a grid of this title: its first word, lower case (`Prices` is `prices`), made unique among `taken`. */
export function aliasFor(title: string | undefined, taken: readonly string[]): string {
  const word = (title ?? '').toLowerCase().replace(/[^a-z0-9_]+/g, ' ').trim().split(' ')[0] ?? '';
  // an alias is read before a colon and a function before a bracket: a word that names a function is still one
  const base = /^[a-z][a-z0-9_]*$/.test(word) ? word.slice(0, 16) : 'grid';
  if (!taken.includes(base)) return base;
  for (let n = 2; ; n++) if (!taken.includes(`${base}${n}`)) return `${base}${n}`;
}

/** A series that cannot be inlined: one reading itself, or a series there is not -- said where it is drawn. */
export class TsInlineError extends Error {}

/** An expression with every `$A` replaced by A's own expression (refs checked first: timeseriesProblems). */
export function inlined(e: TsExpr, series: readonly TsSeries[], seen: readonly string[] = []): TsExpr {
  switch (e.kind) {
    case 'ref': {
      if (seen.includes(e.series)) throw new TsInlineError(`series ${e.series} reads itself: ${[...seen, e.series].map((id) => `$${id}`).join(' reads ')}`);
      const s = series.find((x) => x.id === e.series);
      if (!s) throw new TsInlineError(`there is no series ${e.series}`);
      return inlined(s.expr, series, [...seen, e.series]);
    }
    case 'binary':
      return { ...e, left: inlined(e.left, series, seen), right: inlined(e.right, series, seen) };
    case 'call':
      return { ...e, args: e.args.map((a) => inlined(a, series, seen)) };
    default:
      return e;
  }
}

/** What an expression's figures are (its top function's, or its input's), so it is written and placed by its unit. */
export function unitOf(e: TsExpr): TsUnit {
  switch (e.kind) {
    case 'column':
    case 'ref':
      return 'same';
    case 'number':
      return 'number';
    case 'binary': {
      const l = unitOf(e.left);
      const r = unitOf(e.right);
      // a level times or over a number is still a level; two levels divided are a number (a ratio of prices)
      if (e.op === '+' || e.op === '-') return l === 'number' ? r : l;
      // a ratio times 100 is a percent, a percent over 100 a ratio -- what a person means writing `100 * returns(x)` (the
      // review's Q4: drawn a hundred times too large as a ratio still); any other number keeps its unit
      const by = e.right.kind === 'number' ? { unit: l, n: e.right.value } : e.left.kind === 'number' && e.op === '*' ? { unit: r, n: e.left.value } : undefined;
      if (by !== undefined) {
        // and by a hundredth: a percent times 0.01 is a ratio, a ratio over 0.01 a percent (the second audit's N5)
        const scale = e.op === '*' ? by.n : e.op === '/' ? 1 / by.n : undefined;
        if (by.unit === 'ratio' && scale === 100) return 'percent';
        if (by.unit === 'percent' && scale === 0.01) return 'ratio';
        return by.unit;
      }
      // a number over a series (1 / x), two series multiplied or divided: a number
      return 'number';
    }
    case 'call': {
      const f = TS_FUNCTION.get(e.fn);
      if (!f) throw new Error(`no function ${e.fn} in the catalog (the spec's reader refuses one)`);
      if (f.unit === 'input') return paramOf(e, 'type') === 'absolute' ? unitOf(e.args[0]!) : 'ratio';
      if (f.unit === 'same') return e.args[0] ? unitOf(e.args[0]) : 'same';
      return f.unit;
    }
  }
}

/**
 * WHETHER AN EXPRESSION IS A LEVEL -- a price, an index, a ratio or product of prices: what compare starts from one
 * value, and what the statistics read a volatility, a drawdown, a growth and a Sharpe ratio of. A difference, a change,
 * a deviation, a spread of two levels is none, though it is in its input's units (the review's Q2: `change(close)`
 * rebased to nothing, `diff`'s volatility 2011%). A column is a level; a level times or over a positive number, a level
 * over or times a level, and a sum of levels (a basket) are; a level plus or less a number, a spread, or a level turned
 * negative is none (the second audit's B1); a function's is as its words say (TsFunction.level).
 */
export function levelOf(e: TsExpr): boolean {
  switch (e.kind) {
    case 'column':
    case 'ref':
      return true;
    case 'number':
      return false;
    case 'binary': {
      const l = levelOf(e.left);
      const r = levelOf(e.right);
      const positive = (x: TsExpr): boolean => x.kind === 'number' && x.value > 0;
      // a basket of levels is one; a level shifted by a number, or a spread of two, is none -- its start can be zero or
      // below, which compare would divide by (the second audit's B1: index(close) - 1, rebase(close) - 100)
      if (e.op === '+') return l && r;
      if (e.op === '-') return false;
      // a level scaled by a positive number, a ratio or a product of levels, is one; scaled by a negative one, none
      if (e.op === '/') return l && (r || positive(e.right));
      return (l && (r || positive(e.right))) || (r && positive(e.left));
    }
    case 'call': {
      const f = TS_FUNCTION.get(e.fn);
      if (!f) throw new Error(`no function ${e.fn} in the catalog (the spec's reader refuses one)`);
      return f.level === 'makes' || (f.level === 'keeps' && levelOf(e.args[0]!));
    }
  }
}

/** The least and the most an expression's figures can be: its outermost function's (an RSI's 0 and 100), else none. */
export function boundsOf(e: TsExpr): readonly [number, number] | undefined {
  return e.kind === 'call' ? TS_FUNCTION.get(e.fn)?.bounds : undefined;
}

/** Whether an expression draws a band (a Bollinger band): two lines, nothing reading it. */
export function isBand(e: TsExpr): boolean {
  return e.kind === 'call' && TS_FUNCTION.get(e.fn)?.band === true;
}

/** Whether an expression draws candles (ohlc): a candle per period, nothing reading it. */
export function isCandles(e: TsExpr): boolean {
  return e.kind === 'call' && TS_FUNCTION.get(e.fn)?.candles === true;
}

/** The words a grid's open, high and low columns are named by: a candle of its close is read of them. */
const CANDLE_WORDS: readonly (readonly string[])[] = [['open', 'o'], ['high', 'h', 'hi'], ['low', 'l', 'lo']];

/**
 * A CANDLE OF A COLUMN (the series editor's Candles): `close[AAPL]` made `ohlc(open[AAPL], high[AAPL], low[AAPL],
 * close[AAPL])` -- the same source and matches, the columns its source has by those names (or o, h, l; any case, a
 * prefix or a suffix as the close's own: `close_px` beside `open_px`). Undefined when the expression is no column, or
 * its source has no such three.
 */
export function candleOf(e: TsExpr, columns: readonly TsColumn[]): TsExpr | undefined {
  if (e.kind !== 'column') return undefined;
  const close = e.column.toLowerCase();
  // what the close's name has beside its word (close_px: `_px`), which its open's has too
  const at = ['close', 'last', 'px', 'price'].map((w) => [w, close.indexOf(w)] as const).find(([, i]) => i >= 0);
  const named = (words: readonly string[]): string | undefined => {
    for (const w of words) {
      const want = at ? close.slice(0, at[1]) + w + close.slice(at[1] + at[0].length) : w;
      const hit = columns.find((c) => c.name.toLowerCase() === want) ?? columns.find((c) => c.name.toLowerCase() === w);
      if (hit) return hit.name;
    }
    return undefined;
  };
  const [open, high, low] = CANDLE_WORDS.map(named);
  if (open === undefined || high === undefined || low === undefined) return undefined;
  const of = (column: string): TsExpr => ({ ...e, column });
  return { kind: 'call', fn: 'ohlc', args: [of(open), of(high), of(low), e] };
}

/** What an expression draws that nothing may read, in words: a Bollinger band, candles; undefined, a line. */
export function drawnOnly(e: TsExpr): string | undefined {
  return isBand(e) ? 'a band' : isCandles(e) ? 'candles' : undefined;
}

// ------------------------------------------------------------------ the expression language

/** An expression typed that cannot be read, said where. */
export class TsSyntaxError extends Error {
  /** Where in the text, from 0. */
  readonly at: number;
  constructor(message: string, at: number) {
    super(message);
    this.name = 'TsSyntaxError';
    this.at = at;
  }
}

type Token =
  | { readonly t: 'num'; readonly v: number; readonly at: number }
  | { readonly t: 'id'; readonly v: string; readonly at: number; readonly quoted: boolean }
  | { readonly t: 'str'; readonly v: string; readonly at: number }
  | { readonly t: 'ref'; readonly v: string; readonly at: number }
  | { readonly t: 'op'; readonly v: string; readonly at: number }
  | { readonly t: 'end'; readonly at: number };

function tokenize(text: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  while (i < text.length) {
    const c = text[i]!;
    if (/\s/.test(c)) {
      i++;
      continue;
    }
    const at = i;
    if (/[0-9]/.test(c) || (c === '.' && /[0-9]/.test(text[i + 1] ?? ''))) {
      const m = /^(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?/i.exec(text.slice(i))!;
      // a number run straight into letters is a word -- a tenor, a period: 10Y, 3M
      if (/[A-Za-z_]/.test(text[i + m[0].length] ?? '')) {
        const w = /^[0-9A-Za-z_]+/.exec(text.slice(i))!;
        out.push({ t: 'id', v: w[0], at, quoted: false });
        i += w[0].length;
        continue;
      }
      out.push({ t: 'num', v: Number(m[0]), at });
      i += m[0].length;
      continue;
    }
    if (/[A-Za-z_]/.test(c)) {
      const m = /^[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(i))!;
      out.push({ t: 'id', v: m[0], at, quoted: false });
      i += m[0].length;
      continue;
    }
    if (c === '$') {
      const m = /^\$([A-Za-z][A-Za-z0-9]*)/.exec(text.slice(i));
      if (!m) throw new TsSyntaxError('a $ names a series: $A', at);
      out.push({ t: 'ref', v: m[1]!.toUpperCase(), at });
      i += m[0].length;
      continue;
    }
    if (c === '`' || c === '"' || c === '\'') {
      // a column with a name of its own (`total pnl`), or a value ("AAPL")
      const end = text.indexOf(c, i + 1);
      if (end < 0) throw new TsSyntaxError(`a ${c} is not closed`, at);
      const v = text.slice(i + 1, end);
      out.push(c === '`' ? { t: 'id', v, at, quoted: true } : { t: 'str', v, at });
      i = end + 1;
      continue;
    }
    if ('+-*/(),[]=.:'.includes(c)) {
      out.push({ t: 'op', v: c, at });
      i++;
      continue;
    }
    throw new TsSyntaxError(`"${c}" means nothing here`, at);
  }
  out.push({ t: 'end', at: text.length });
  return out;
}

/**
 * AN EXPRESSION AS A PERSON TYPES IT, read into the tree the spec keeps -- PlotTool Pro's style over gs-quant's names:
 *
 *   close                      the first source's column, each period's latest value
 *   px:close, rates:yield      a column of the source of that alias
 *   close[AAPL]                its rows whose instrument column (the source's key) is AAPL
 *   close[venue=NYSE, AAPL]    and whose venue is NYSE
 *   volume[AAPL].sum           each period's rows summed (last, first, average, sum, min, max)
 *   $A                         series A's expression
 *   returns(close[AAPL], 1, log), volatility($A, 63), correlation($A, $B, 126), zscores($A, w=252)
 *   close[AAPL] / close[MSFT], ($A + $B) / 2, 100 * returns($A)
 *
 * A column whose name is not a word is quoted with backticks (`total pnl`); a value that is not a word or a number with
 * quotes ("BRK.B"). A function's parameters follow its series, by place or by name (w=63, type=log).
 */
export function parseExpr(text: string): TsExpr {
  const toks = tokenize(text);
  let p = 0;
  const peek = (): Token => toks[p]!;
  const next = (): Token => toks[p++]!;
  const isOp = (v: string): boolean => {
    const t = peek();
    return t.t === 'op' && t.v === v;
  };
  const expect = (v: string): void => {
    const t = next();
    if (t.t !== 'op' || t.v !== v) throw new TsSyntaxError(`a "${v}" was expected here`, t.at);
  };

  const sum = (): TsExpr => {
    let left = product();
    while (isOp('+') || isOp('-')) {
      const op = (next() as { v: string }).v as TsOperator;
      left = { kind: 'binary', op, left, right: product() };
    }
    return left;
  };
  const product = (): TsExpr => {
    let left = unary();
    while (isOp('*') || isOp('/')) {
      const op = (next() as { v: string }).v as TsOperator;
      left = { kind: 'binary', op, left, right: unary() };
    }
    return left;
  };
  const unary = (): TsExpr => {
    if (isOp('-')) {
      next();
      const operand = unary();
      return operand.kind === 'number' ? { kind: 'number', value: -operand.value }
        : { kind: 'binary', op: '*', left: { kind: 'number', value: -1 }, right: operand };
    }
    if (isOp('+')) {
      next();
      return unary();
    }
    return primary();
  };
  const value = (): string | number | boolean => {
    const t = next();
    if (t.t === 'num') return t.v;
    if (t.t === 'str') return t.v;
    if (t.t === 'id') return t.v === 'true' ? true : t.v === 'false' ? false : t.v;
    if (t.t === 'op' && t.v === '-' && peek().t === 'num') return -(next() as { v: number }).v;
    throw new TsSyntaxError('a value was expected here (AAPL, "BRK.B", 42)', t.at);
  };
  const column = (name: string, source?: string): TsExpr => {
    const where: TsMatch[] = [];
    let fn: TsAggregate | undefined;
    if (isOp('[')) {
      next();
      do {
        const t = peek();
        const named = (t.t === 'id') && toks[p + 1]?.t === 'op' && (toks[p + 1] as { v: string }).v === '=';
        if (named) {
          next();
          next();
          where.push({ column: (t as { v: string }).v, value: value() });
        } else {
          where.push({ value: value() });
        }
      } while (isOp(',') && next());
      expect(']');
    }
    if (isOp('.')) {
      next();
      const t = next();
      if (t.t !== 'id' || !AGGREGATES.includes(t.v as TsAggregate)) {
        throw new TsSyntaxError(`a period's value is one of ${AGGREGATES.join(', ')}`, t.at);
      }
      fn = t.v as TsAggregate;
    }
    return {
      kind: 'column', ...(source !== undefined ? { source } : {}), column: name, ...(fn !== undefined ? { fn } : {}),
      ...(where.length > 0 ? { where } : {}),
    };
  };
  const call = (name: string, at: number): TsExpr => {
    const f = functionNamed(name);
    if (!f) {
      const why = NOT_BUILT[name.toLowerCase()];
      throw new TsSyntaxError(why ? `${name}: ${why}` : `there is no function ${name}`, at);
    }
    next();
    const args: TsExpr[] = [];
    const params: Record<string, number | string> = {};
    let positional = 0;
    if (!isOp(')')) {
      do {
        const t = peek();
        const named = t.t === 'id' && toks[p + 1]?.t === 'op' && (toks[p + 1] as { v: string }).v === '=';
        if (named) {
          next();
          next();
          const param = f.params.find((q) => q.name === (t as { v: string }).v);
          if (!param) throw new TsSyntaxError(`${f.name} has no parameter ${(t as { v: string }).v} (it has ${f.params.map((q) => q.name).join(', ') || 'none'})`, t.at);
          params[param.name] = paramValue(param);
          continue;
        }
        if (args.length < f.series) {
          args.push(sum());
          continue;
        }
        const param = f.params[positional++];
        if (!param) throw new TsSyntaxError(`${f.name} takes ${seriesWords(f.series)}${f.params.length > 0 ? ` and ${f.params.map((q) => q.name).join(', ')}` : ''}`, t.at);
        params[param.name] = paramValue(param);
      } while (isOp(',') && next());
    }
    expect(')');
    if (args.length < f.series) throw new TsSyntaxError(`${f.name} reads ${seriesWords(f.series)}`, at);
    return { kind: 'call', fn: f.name, args, ...(Object.keys(params).length > 0 ? { params } : {}) };
  };
  const paramValue = (param: TsParam): number | string => {
    const t = next();
    if (param.kind === 'word') {
      const v = t.t === 'id' || t.t === 'str' ? t.v.toLowerCase() : undefined;
      if (v === undefined || !param.words!.includes(v)) throw new TsSyntaxError(`${param.name} is one of ${param.words!.join(', ')}`, t.at);
      return v;
    }
    // a span's calendar period: `3m`, `1y` (the lexer reads a number run into letters as one word)
    if (param.kind === 'span' && t.t === 'id' && !t.quoted) {
      const v = t.v.toLowerCase();
      if (tenorOf(v) === undefined) throw new TsSyntaxError(`${param.name} is a whole number of observations or a calendar period (3m, 1y)`, t.at);
      return v;
    }
    let n: number | undefined;
    if (t.t === 'num') n = t.v;
    else if (t.t === 'op' && t.v === '-' && peek().t === 'num') n = -(next() as { v: number }).v;
    if (n === undefined) throw new TsSyntaxError(param.kind === 'span' ? `${param.name} is a whole number of observations or a calendar period (3m, 1y)` : `${param.name} is a number`, t.at);
    if ((param.kind === 'whole' || param.kind === 'span') && !Number.isInteger(n)) throw new TsSyntaxError(`${param.name} is a whole number`, t.at);
    if (param.min !== undefined && n < param.min) throw new TsSyntaxError(`${param.name} is at least ${param.min}`, t.at);
    return n;
  };
  const primary = (): TsExpr => {
    const t = next();
    if (t.t === 'num') return { kind: 'number', value: t.v };
    if (t.t === 'ref') return { kind: 'ref', series: t.v };
    if (t.t === 'op' && t.v === '(') {
      const inner = sum();
      expect(')');
      return inner;
    }
    if (t.t === 'id') {
      if (!t.quoted && isOp('(')) return call(t.v, t.at);
      // a source's column: `px:close`, `rates:yield[10Y]`
      if (!t.quoted && isOp(':')) {
        if (!ALIAS.test(t.v)) throw new TsSyntaxError(`"${t.v}" is not a source's name`, t.at);
        next();
        const c = next();
        if (c.t !== 'id') throw new TsSyntaxError(`a column of ${t.v} was expected here`, c.at);
        return column(c.v, t.v);
      }
      if (!t.quoted && NOT_BUILT[t.v.toLowerCase()]) throw new TsSyntaxError(`${t.v}: ${NOT_BUILT[t.v.toLowerCase()]}`, t.at);
      return column(t.v);
    }
    if (t.t === 'end') throw new TsSyntaxError('the expression stops before its end', t.at);
    throw new TsSyntaxError(`"${t.t === 'str' ? t.v : (t as { v: string }).v}" was not expected here`, t.at);
  };

  const e = sum();
  const last = peek();
  if (last.t !== 'end') throw new TsSyntaxError('the expression goes on past its end', last.at);
  return e;
}

const WORD = /^[A-Za-z_][A-Za-z0-9_]*$/;
const PRECEDENCE: Readonly<Record<TsOperator, number>> = { '+': 1, '-': 1, '*': 2, '/': 2 };

/** A number as an expression writes it. */
function numberText(n: number): string {
  return Number.isInteger(n) || Math.abs(n) >= 1e-6 ? String(n) : n.toExponential();
}

/** A value of a match as an expression writes it: a word as it is, anything else quoted. */
function valueText(v: string | number | boolean): string {
  if (typeof v !== 'string') return String(v);
  // a word as it is (AAPL, 10Y), unless it would read back as something else: a number, true or false
  return /^[A-Za-z0-9_]+$/.test(v) && !Number.isFinite(Number(v)) && !/^\d+(\.\d*)?e\d*$/i.test(v) && v !== 'true' && v !== 'false'
    ? v : JSON.stringify(v);
}

/** A column's name as an expression writes it: a word as it is, anything else in backticks. */
function columnText(c: string): string {
  return WORD.test(c) && !functionNamed(c) && !AGGREGATES.includes(c as TsAggregate) ? c : `\`${c}\``;
}

/**
 * AN EXPRESSION AS TEXT, the one way it is written (parseExpr reads it back to the same tree): a function's parameters
 * by place, those left at their defaults left off its end.
 */
export function exprText(e: TsExpr, outer = 0): string {
  switch (e.kind) {
    case 'number':
      return numberText(e.value);
    case 'ref':
      return `$${e.series}`;
    case 'column': {
      const where = (e.where ?? []).map((m) => (m.column !== undefined ? `${m.column}=${valueText(m.value)}` : valueText(m.value)));
      return `${e.source !== undefined ? `${e.source}:` : ''}${columnText(e.column)}${where.length > 0 ? `[${where.join(', ')}]` : ''}${e.fn !== undefined ? `.${e.fn}` : ''}`;
    }
    case 'binary': {
      const mine = PRECEDENCE[e.op];
      // a minus or a division binds its right side tighter: a - (b - c), a / (b * c)
      const right = e.op === '-' || e.op === '/' ? mine + 1 : mine;
      const text = `${exprText(e.left, mine)} ${e.op} ${exprText(e.right, right)}`;
      return mine < outer ? `(${text})` : text;
    }
    case 'call': {
      const f = TS_FUNCTION.get(e.fn);
      const args = e.args.map((a) => exprText(a));
      if (f) {
        const told = f.params.map((q) => e.params?.[q.name]);
        let lastTold = told.length - 1;
        while (lastTold >= 0 && (told[lastTold] === undefined || told[lastTold] === f.params[lastTold]!.default)) lastTold--;
        for (let i = 0; i <= lastTold; i++) {
          const v = told[i] ?? f.params[i]!.default;
          args.push(typeof v === 'number' ? numberText(v) : v);
        }
      }
      return `${e.fn}(${args.join(', ')})`;
    }
  }
}

/** A series' name as the legend says it: its own, else its expression's text. */
export function seriesName(s: TsSeries): string {
  return s.name ?? exprText(s.expr);
}

/**
 * A line's name: its series' -- and, one of a split series' lines, its value with it: a named series' after its name
 * ("Yield 10Y"), a plain column's alone ("AAPL"), else before its expression ("AAPL volatility(close, 63)").
 */
export function splitName(s: TsSeries, split: unknown): string {
  if (split === undefined) return seriesName(s);
  if (s.name !== undefined) return `${s.name} ${String(split)}`;
  const plain = s.expr.kind === 'column' && (s.expr.where ?? []).length === 0;
  return plain ? String(split) : `${String(split)} ${seriesName(s)}`;
}

/** The next free series id: the first capital letter no series has (then A2, B2, ...). */
export function nextSeriesId(series: readonly TsSeries[]): string {
  const taken = new Set(series.map((s) => s.id));
  for (let round = 1; ; round++) {
    for (let c = 65; c <= 90; c++) {
      const id = round === 1 ? String.fromCharCode(c) : `${String.fromCharCode(c)}${round}`;
      if (!taken.has(id)) return id;
    }
  }
}

// ------------------------------------------------------------------ against the grid

/** A grid's column as a timeseries reads it: its name and its type as the compiler types it. */
export interface TsColumn {
  readonly name: string;
  readonly type: string | undefined;
}

/** What a tile of no series says where it is drawn: a tile either side may leave (timeseries-edit.ts judgedEdit). */
export const NO_SERIES = 'Add a series to plot.';

/**
 * What stops a timeseries from being drawn over its sources' columns, in words; empty when it can be. `columns`: each
 * source's columns by its alias, as its grid has them now -- undefined for a source whose grid the page cannot give
 * (its cube gone). Each source's date a date, its key there; each series reading what its sources have -- a value a
 * number, its refs there and not round in a circle, of one split, its split a column of the source it reads first --
 * a band nothing reads.
 */
export function timeseriesProblems(ts: TimeseriesSpec, columns: ReadonlyMap<string, readonly TsColumn[] | undefined>): string[] {
  const out: string[] = [];
  const types = new Map<string, Map<string, string | undefined>>();
  for (const src of ts.sources) {
    const cols = columns.get(src.alias);
    if (cols === undefined) {
      // (no "Undo brings it back": a page reopened has no history -- the second audit's A notes)
      out.push(`Source ${src.alias}'s grid is not on the page: Settings reads another grid.`);
      continue;
    }
    const typeOf = new Map(cols.map((c) => [c.name, c.type]));
    types.set(src.alias, typeOf);
    const dateType = typeOf.get(src.date);
    if (!typeOf.has(src.date)) out.push(`Source ${src.alias} has no column '${src.date}' to read its series along.`);
    else if (!isTemporal(dateType) || isTimeOfDay(dateType)) out.push(`Source ${src.alias}'s '${src.date}' is not a date: a timeseries is read along a date or a date-time.`);
    if (src.key !== undefined && !typeOf.has(src.key)) out.push(`Source ${src.alias} has no column '${src.key}' to name its instruments by.`);
  }
  if (ts.series.length === 0) out.push(NO_SERIES);
  for (const s of ts.series) {
    const name = `Series ${s.id}`;
    for (const ref of refsOf(s.expr)) {
      const other = ts.series.find((x) => x.id === ref);
      if (!other) {
        out.push(`${name} reads $${ref}, which there is not.`);
        continue;
      }
      if ((other.split ?? null) !== (s.split ?? null)) out.push(`${name} reads $${ref}, split ${other.split === undefined ? 'by nothing' : `by '${other.split}'`}: a series reads only series split as it is.`);
      const drawn = drawnOnly(other.expr);
      if (drawn !== undefined) out.push(`${name} reads $${ref}, ${drawn}: ${drawn} ${drawn === 'candles' ? 'are' : 'is'} drawn, never read.`);
    }
    let whole: TsExpr;
    try {
      whole = inlined(s.expr, ts.series);
    } catch (e) {
      out.push(`${name}: ${e instanceof Error ? e.message : String(e)}.`);
      continue;
    }
    const read = sourcesOf(ts, whole);
    for (const alias of read) {
      if (!ts.sources.some((src) => src.alias === alias)) out.push(`${name} reads ${alias}:..., which is none of the tile's sources.`);
    }
    for (const [alias, cs] of columnsOf(ts, whole)) {
      const typeOf = types.get(alias);
      if (!typeOf) continue;
      for (const c of cs) if (!typeOf.has(c)) out.push(`${name} reads '${c}', which source ${alias} has not.`);
    }
    if (s.split !== undefined) {
      const first = read[0];
      const typeOf = first === undefined ? undefined : types.get(first);
      const date = ts.sources.find((src) => src.alias === first)?.date;
      if (typeOf && !typeOf.has(s.split)) out.push(`${name} is split by '${s.split}', which source ${first} -- the one it reads first -- has not.`);
      if (s.split === date) out.push(`${name} is split by its own date column.`);
      if (read.length === 0) out.push(`${name} is split by '${s.split}' but reads no column.`);
    }
    const walk = (e: TsExpr, top: boolean): void => {
      if (e.kind === 'column') {
        const alias = sourceAlias(ts, e);
        const typeOf = alias === undefined ? undefined : types.get(alias);
        if (typeOf?.has(e.column) && !isNumeric(typeOf.get(e.column))) out.push(`${name} plots '${e.column}', which is not a number.`);
        const src = ts.sources.find((x) => x.alias === alias);
        if (src && src.key === undefined && (e.where ?? []).some((m) => m.column === undefined)) {
          out.push(`${name} matches a value with no column (close[AAPL]): choose source ${src.alias}'s instrument column (Key).`);
        }
      } else if (e.kind === 'binary') {
        walk(e.left, false);
        walk(e.right, false);
      } else if (e.kind === 'call') {
        if (!top && TS_FUNCTION.get(e.fn)?.band) out.push(`${name} reads a Bollinger band: a band is drawn, never read.`);
        if (!top && TS_FUNCTION.get(e.fn)?.candles) out.push(`${name} reads candles: candles are drawn, never read.`);
        e.args.forEach((a) => walk(a, false));
      }
    };
    walk(whole, true);
  }
  return [...new Set(out)];
}

// ------------------------------------------------------------------ a first source, a first timeseries

/** Words a column's name has when it is a price, most likely first: the default series reads the first that matches. */
const PRICE_WORDS = ['close', 'adj_close', 'price', 'last', 'px', 'value', 'level', 'rate', 'yield', 'spot', 'mid', 'nav'];
/** Words a column's name has when it names the instrument. */
const KEY_WORDS = ['symbol', 'ticker', 'instrument', 'asset', 'security', 'tenor', 'pair', 'name', 'id', 'code', 'series'];

const named = (words: readonly string[], among: readonly TsColumn[]): TsColumn | undefined => {
  for (const w of words) {
    const hit = among.find((c) => c.name.toLowerCase() === w) ?? among.find((c) => c.name.toLowerCase().includes(w));
    if (hit) return hit;
  }
  return undefined;
};

/**
 * A GRID AS A SOURCE: read along its first date column, its instruments named by its first text column the grid groups
 * by, else the first named like one (symbol, ticker, tenor, ...). Null when it has no date.
 */
export function sourceOf(columns: readonly TsColumn[], rows: readonly string[], cube: string, alias: string): TsSource | null {
  const date = columns.find((c) => isTemporal(c.type) && !isTimeOfDay(c.type));
  if (!date) return null;
  const texts = columns.filter((c) => !isNumeric(c.type) && !isTemporal(c.type) && c.name !== date.name);
  const key = texts.find((c) => rows.includes(c.name)) ?? named(KEY_WORDS, texts);
  return { alias, cube, date: date.name, ...(key ? { key: key.name } : {}) };
}

/** The value a first series of a source plots: the first number named like a price (close, price, ...), else its first. */
export function firstValue(columns: readonly TsColumn[], source: TsSource): string | undefined {
  const numbers = columns.filter((c) => isNumeric(c.type) && c.name !== source.date);
  return (named(PRICE_WORDS, numbers) ?? numbers[0])?.name;
}

/**
 * A FIRST TIMESERIES OF A GRID: the grid its one source (sourceOf), its first series that source's price-like value, one
 * line per instrument when it names them. Null when the grid has no date or no number.
 */
export function defaultTimeseries(columns: readonly TsColumn[], rows: readonly string[], cube: string, alias: string): TimeseriesSpec | null {
  const source = sourceOf(columns, rows, cube, alias);
  const first = source ? firstPlot(columns, source) : undefined;
  return source && first ? { sources: [source], ...first } : null;
}

/** Values that are rates -- a yield, an interest rate -- compared as they are: a yield from 100 is no figure. */
const RATE_WORDS = ['rate', 'yield'];

/**
 * A SOURCE'S FIRST PLOT: its price-like value (firstValue), one line per instrument when it names them -- and then, a
 * price, each line from 100 at the first date shown (compare: rebase), so instruments of any price are read against
 * each other (ten lines of one scale, an index's thousands beside a currency's ones, read as one flat line and nine);
 * a rate is drawn as it is. Undefined when it has no number.
 */
export function firstPlot(columns: readonly TsColumn[], source: TsSource): Pick<TimeseriesSpec, 'series' | 'view'> | undefined {
  const value = firstValue(columns, source);
  if (value === undefined) return undefined;
  const rebased = source.key !== undefined && !RATE_WORDS.some((w) => value.toLowerCase().includes(w));
  return {
    series: [{ id: 'A', expr: columnRef(value), ...(source.key ? { split: source.key } : {}) }],
    ...(rebased ? { view: { compare: 'rebase' } } : {}),
  };
}
