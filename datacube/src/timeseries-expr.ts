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
  AGGREGATES, ALIAS, isConditionOp, manyProblem, seriesWords, tenorOf, TS_COMPARISONS, TS_SIGNATURES, type TimeseriesSpec, type TsAggregate, type TsExpr, type TsFunctionName, type TsMatch,
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

export type TsFamily = 'Returns' | 'Level' | 'Rolling statistics' | 'Risk' | 'Relationships' | 'Technicals' | 'Math' | 'Dates';

export interface TsFunction {
  readonly name: TsFunctionName;
  readonly label: string;
  readonly family: TsFamily;
  /** How many series it reads: one, two (a correlation's), three (an if's), four (a candle's), or `many` with their
   *  weights (a weighted sum's). */
  readonly series: 1 | 2 | 3 | 4 | 'many';
  /** What each series is, where not x and y (timeseries-spec.ts TsSignature). */
  readonly reads?: readonly string[];
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
export type TsWords = Omit<TsFunction, 'name' | 'series' | 'params'> & { readonly params?: Readonly<Record<string, string>> };

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
/** Where a classical decomposition differs (the Phase 1 review's 4): gs-quant first fills its series to a calendar
 *  -- business days to every day, a gap with the value before it -- and refuses one of under two cycles. */
const DECOMPOSE_DIFFERS = 'the tile needs a frequency; a daily cycle counts business days (5 a week, 21 a month, 63 a quarter, 252 a year) whatever the rows\' days, where gs-quant fills a daily series to every calendar day (7, 30, 91, 365); a month, a quarter or a week missing is not filled -- each place in the cycle is still the calendar\'s, the moving average reading the observations either side; under two whole cycles, none (gs-quant refuses)';
/** Where beta differs on two calendars: each series' returns on its own dates, joined on the dates both have. */
const BETA_DIFFERS = 'on two calendars, over the dates both have (gs-quant takes the covariance over every date either has, over the benchmark\'s own variance)';

/**
 * THE FUNCTIONS' WORDS, gs-quant's names first (PlotTool Pro's words), each with what it computes -- and, where
 * DataCube's differs, how. Every one is written by timeseries-query.ts as columns the database computes.
 */
// exported for the one test that holds the library's facts to it (A3 step 1: timeseries-spec.test.ts), until the tile
// reads the catalog and this table is deleted (A3 step 5)
export const WORDS: Readonly<Record<TsFunctionName, TsWords>> = {
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
  // by each window's rows (the expert functions, 2026-10-26): a self-join of each date to its window's
  percentiles: {
    label: 'Rolling percentile rank', family: 'Rolling statistics', unit: 'number', bounds: [0, 100],
    params: { w: 'observations in the window, or a calendar period (3m, 1y); 0 every one to it' },
    about: 'Where each value ranks among those of its window, 0 to 100: the share below it, those equal to it counted half (scipy\'s percentileofscore, kind=mean).',
    gsQuant: 'statistics.percentiles', aliases: ['percentile_rank', 'pctrank'],
    differs: 'a series ranked in its own window (gs-quant also ranks a second series\' values in the first\'s)',
  },
  median: { level: 'keeps',
    label: 'Rolling median', family: 'Rolling statistics', unit: 'same',
    about: 'The middle value over the last w observations (of an even count, the mean of the two middle ones).',
    gsQuant: 'statistics.median', differs: 'w defaults to 22 observations (gs-quant: every observation)',
  },
  percentile: { level: 'keeps',
    label: 'Rolling percentile', family: 'Rolling statistics', unit: 'same',
    params: { n: 'the percentile, 0 to 100', w: 'observations in the window, or a calendar period (3m, 1y); 0 every observation' },
    about: 'The n-th percentile of the values over the last w observations, between the two nearest (pandas\' quantile); 0 the whole series\'.',
    gsQuant: 'statistics.percentile', aliases: ['quantile'],
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
  exponential_volatility: {
    label: 'Exponential volatility', family: 'Risk', unit: 'percent', params: { beta: 'the weight of the returns before, 0 to under 1' },
    about: 'The annualized exponentially weighted standard deviation of its returns, in percent (20% is 20).',
    gsQuant: 'technicals.exponential_volatility', aliases: ['ewvol', 'ewma_volatility'],
    differs: 'a return of none between others is passed over, as pandas\' ignore_na (gs-quant\'s weights decay across it)',
  },
  exponential_spread_volatility: {
    label: 'Exponential spread volatility', family: 'Risk', unit: 'same', params: { beta: 'the weight of the changes before, 0 to under 1' },
    about: 'The annualized exponentially weighted standard deviation of its changes, in its own units (a spread\'s, a yield\'s).',
    gsQuant: 'technicals.exponential_spread_volatility',
    differs: 'a change of none between others is passed over, as pandas\' ignore_na (gs-quant\'s weights decay across it)',
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
    label: 'Relative strength index', family: 'Technicals', unit: 'number', params: { w: 'the changes its gains and losses are smoothed over' },
    about: '100 - 100 / (1 + average gain / average loss), each Wilder\'s smoothed average of the last w changes: 0 to 100.',
    gsQuant: 'technicals.relative_strength_index', aliases: ['relative_strength_index'], bounds: [0, 100],
    differs: 'w a whole number of changes (gs-quant also takes a calendar period, its smoothing then over each date\'s count)',
  },
  macd: {
    label: 'MACD', family: 'Technicals', unit: 'same',
    params: { m: 'the short average\'s span', n: 'the long average\'s span', s: 'the span its difference is averaged over (1: as it is)' },
    about: 'The short exponential average less the long one, itself averaged over s -- a span k weighs each new value 2 / (k + 1).',
    gsQuant: 'technicals.macd',
  },
  // exponentially weighted (the expert functions, 2026-10-26): each a recursion, y = beta·y[-1] + (1 - beta)·x, which
  // the database computes in its closed form (timeseries-query.ts Writer.ewm)
  exponential_moving_average: { level: 'keeps',
    label: 'Exponential moving average', family: 'Rolling statistics', unit: 'same',
    params: { beta: 'the weight of the average before, 0 to under 1' },
    about: 'Each value the average before weighed beta and the series 1 - beta: y0 = x0, y = beta·y[-1] + (1 - beta)·x.',
    gsQuant: 'technicals.exponential_moving_average', aliases: ['ema', 'ewma'],
    differs: 'a value of none between others is passed over, as pandas\' ignore_na (gs-quant\'s weights decay across it)',
  },
  exponential_std: {
    label: 'Exponential standard deviation', family: 'Rolling statistics', unit: 'same',
    params: { beta: 'the weight of the observations before, 0 to under 1' },
    about: 'The exponentially weighted standard deviation, bias-corrected (pandas\' ewm(alpha = 1 - beta, adjust=False).std()).',
    gsQuant: 'statistics.exponential_std', aliases: ['ewm_std', 'ewstd'],
    differs: 'a value of none between others is passed over, as pandas\' ignore_na (gs-quant\'s weights decay across it)',
  },
  smoothed_moving_average: { level: 'keeps',
    label: 'Smoothed moving average', family: 'Rolling statistics', unit: 'same', params: { w: 'the observations it is smoothed over' },
    about: 'Wilder\'s: from the mean of w observations, each value (w - 1) parts the one before and one part the series.',
    gsQuant: 'technicals.smoothed_moving_average', aliases: ['smma', 'wilder'],
    differs: 'w defaults to 14 observations (gs-quant: every observation); a whole number of them (gs-quant also takes a calendar period)',
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
  // dates (the next phases' 1, 2026-10-26)
  interpolate: { level: 'keeps',
    label: 'Onto the dates of another', family: 'Dates', unit: 'same',
    params: { method: "intersect: the dates both have; nan: none where x has none; zero: 0 there; step: from x's value as of y's first date (its first, before it starts), each of y's dates x's own value, else the one before among them" },
    about: 'x on the dates of y -- its lines, a split\'s, under every line of y\'s -- by method (gs-quant\'s, its default intersect).',
    gsQuant: 'datetime.interpolate',
  },
  union: { level: 'keeps',
    label: 'One where it has none, the other', family: 'Dates', unit: 'same',
    about: 'Every date either has: x where it has a value, else y (gs-quant\'s union of two: combine_first).', gsQuant: 'datetime.union',
  },
  append: { level: 'keeps',
    label: 'One, then the other after it', family: 'Dates', unit: 'same',
    about: 'x to its last date, then y on the dates after it: a history spliced onto another\'s end.', gsQuant: 'datetime.append',
  },
  prepend: { level: 'keeps',
    label: 'One until the other starts', family: 'Dates', unit: 'same',
    about: 'x before y\'s first date, then y: a newer series with an older history before it.', gsQuant: 'datetime.prepend',
  },
  first: { level: 'keeps',
    label: 'Its first value', family: 'Dates', unit: 'same', about: 'Its first value, on every date it has.', gsQuant: 'analysis.first',
    differs: 'its first value, where gs-quant\'s is its first date\'s even when that is none',
  },
  last: { level: 'keeps',
    label: 'Its last value', family: 'Dates', unit: 'same', about: 'Its last value, on every date it has.', gsQuant: 'analysis.last',
  },
  count: {
    label: 'How many so far', family: 'Dates', unit: 'number', about: 'How many values it has to each date.', gsQuant: 'analysis.count',
  },
  day: { label: 'Day of the month', family: 'Dates', unit: 'number', about: 'The day of the month of each of its dates, 1 to 31.', gsQuant: 'datetime.day' },
  month: { label: 'Month', family: 'Dates', unit: 'number', about: 'The month of each of its dates, 1 to 12.', gsQuant: 'datetime.month' },
  year: { label: 'Year', family: 'Dates', unit: 'number', about: 'The year of each of its dates.', gsQuant: 'datetime.year' },
  quarter: { label: 'Quarter', family: 'Dates', unit: 'number', about: 'The quarter of each of its dates, 1 to 4.', gsQuant: 'datetime.quarter' },
  weekday: { label: 'Day of the week', family: 'Dates', unit: 'number', about: 'The day of the week of each of its dates, 0 (Monday) to 6 (Sunday).', gsQuant: 'datetime.weekday' },
  // algebra and analysis (the next phases' 1, 2026-10-26)
  if: {
    label: 'One or the other, by a condition', family: 'Math', unit: 'same',
    about: 'On the dates of its flags: where they hold (not 0), x; where not, y -- each a series or a number, none where the one taken has none: if(close > mean(close, 50), close, 0).',
    gsQuant: 'algebra.if_', aliases: ['if_'], differs: 'flags other than 0 hold (gs-quant refuses flags but 0 and 1)',
  },
  not: {
    label: 'Not', family: 'Math', unit: 'number', about: 'A condition turned over: 1 where it does not hold, 0 where it does.',
    gsQuant: 'algebra.not_', aliases: ['not_'], differs: 'a value other than 0 holds (gs-quant refuses values but 0 and 1)',
  },
  compare: {
    label: 'Which is greater', family: 'Math', unit: 'number', about: '1 where x is above y, -1 where below, 0 where equal.',
    gsQuant: 'analysis.compare', differs: 'their dates combined as the tile\'s align says (gs-quant\'s method, default step)',
  },
  filter: { level: 'keeps',
    label: 'Drop values', family: 'Math', unit: 'same',
    params: { operator: 'empty: the dates of none; else the values where x is less_than, greater_than, l_equals, g_equals, equals or not_equals value', value: 'what x is compared with' },
    about: 'x without the values the operator matches (gs-quant\'s filter_ removes them; by not_equals, the dates of none too): every figure after it reads the dates left.',
    gsQuant: 'algebra.filter_', aliases: ['filter_'],
  },
  product: {
    label: 'Rolling product', family: 'Rolling statistics', unit: 'number', about: 'The product of the last w values (0: every one to date).',
    gsQuant: 'statistics.product',
  },
  geometrically_aggregate: {
    label: 'Compounded', family: 'Returns', unit: 'same', about: 'Returns compounded to date: the product of (1 + each) less 1.',
    gsQuant: 'algebra.geometrically_aggregate',
  },
  weighted_sum: { level: 'keeps',
    label: 'Weighted average', family: 'Math', unit: 'same',
    about: 'Series weighted, on the dates all have: weighted_sum(x, 0.6, y, 0.4) is (0.6x + 0.4y) / (0.6 + 0.4).',
    gsQuant: 'algebra.weighted_sum', differs: 'each series followed by its weight (gs-quant: a list of series and a list of weights)',
  },
  smooth_spikes: { level: 'keeps',
    label: 'Spikes smoothed', family: 'Math', unit: 'same',
    params: { threshold: 'how far above or below both neighbours a spike is', type: 'percentage: 1 + threshold times both; absolute: threshold beyond both' },
    about: 'A value beyond both its neighbours by the threshold replaced by their mean; its first and last dates dropped.', gsQuant: 'analysis.smooth_spikes',
    differs: 'threshold defaults to 0.5 (gs-quant: no default)',
  },
  excess_returns: { level: 'keeps',
    label: 'Excess returns', family: 'Returns', unit: 'same',
    about: 'x over a benchmark: over a cash index y, x\'s first value times the product of (1 + x\'s return - y\'s); over a rate (a number, 0.05 for 5%), x less the rate accrued on it day by day (actual/360).',
    gsQuant: 'econometrics.excess_returns',
  },
  vol_swap_volatility: {
    label: 'Vol swap volatility', family: 'Risk', unit: 'percent',
    about: 'The realized volatility a variance swap pays on: the root mean square of log returns over the last w (0: every one to date), times the root of 252, in percent.',
    gsQuant: 'econometrics.vol_swap_volatility', differs: 'w defaults to every observation to date (gs-quant: the whole series)',
  },
  consecutive: {
    label: 'Consecutive', family: 'Rolling statistics', unit: 'number',
    params: { direction: 'positive: values above 0; negative: below' },
    about: 'How many values in a row to each date have been above 0 (or below); 0 where it breaks.', gsQuant: 'analysis.consecutive',
  },
  trend: { level: 'keeps',
    label: 'Trend', family: 'Level', unit: 'same',
    params: { method: 'additive: a season of its own size; multiplicative: of its share', freq: 'the cycle: a year, a quarter, a month or a week' },
    about: 'Its trend in a classical decomposition: a moving average centred over one cycle of the tile\'s frequency (a year of months, 12).',
    gsQuant: 'technicals.trend',
    differs: DECOMPOSE_DIFFERS,
  },
  seasonally_adjusted: { level: 'keeps',
    label: 'Seasonally adjusted', family: 'Level', unit: 'same',
    params: { method: 'additive: x less its season; multiplicative: x over its season', freq: 'the cycle: a year, a quarter, a month or a week' },
    about: 'x with its season taken out, a classical decomposition\'s: each place in the cycle\'s mean detrended value, centred, removed.',
    gsQuant: 'technicals.seasonally_adjusted',
    differs: DECOMPOSE_DIFFERS,
  },
  // the analysts' packs (the next phases' 4, 2026-10-26): technicals of a candle's columns (Wilder's, Lane's, Keltner's,
  // Donchian's, Granville's), a series against a benchmark, a total return, a basket, a pair's lead and its reversion
  atr: {
    label: 'Average true range', family: 'Technicals', unit: 'same', params: { w: 'the true ranges smoothed over' },
    about: 'Wilder\'s smoothed average of true ranges, each the most of the high less the low, the high less the close before and the close before less the low: from the w-th, the mean of the w after the first, then ((w − 1)·atr + tr) / w (TA-Lib\'s). At a frequency, each period\'s highest high, lowest low and last close.',
    gsQuant: 'DataCube (Wilder\'s average true range)', aliases: ['average_true_range'],
  },
  stochastic: {
    label: 'Stochastic oscillator (%K)', family: 'Technicals', unit: 'number', params: { w: 'observations its highest high and lowest low are of' },
    about: '100 × (close − the lowest low) / (the highest high − the lowest low) over the last w observations: 0 to 100; none where the high and the low are one.',
    gsQuant: 'DataCube (Lane\'s stochastic oscillator)', aliases: ['stoch', 'stochastic_oscillator'], bounds: [0, 100],
  },
  keltner_channels: { level: 'keeps',
    label: 'Keltner channels', family: 'Technicals', unit: 'same', band: true,
    params: { w: 'the close\'s exponential average\'s span (its weight of the one before (w − 1) / (w + 1))', a: 'the true ranges averaged', k: 'average true ranges either side' },
    about: 'A band k average true ranges (over a) either side of the exponential moving average of the close (of span w, from the first close), drawn once it holds w closes.',
    gsQuant: 'DataCube (Keltner\'s channels, as Raschke\'s)', aliases: ['keltner'],
  },
  donchian_channels: { level: 'keeps',
    label: 'Donchian channels', family: 'Technicals', unit: 'same', band: true,
    about: 'A band from the lowest low to the highest high of the last w observations.',
    gsQuant: 'DataCube (Donchian\'s channels)', aliases: ['donchian'],
  },
  obv: {
    label: 'On-balance volume', family: 'Technicals', unit: 'number',
    about: 'The running total of each period\'s volume, added when the close rose and taken off when it fell: 0 at the first (StockCharts\'; TA-Lib\'s starts at the first volume). At a frequency, each period\'s whole volume.',
    gsQuant: 'DataCube (Granville\'s on-balance volume)', aliases: ['on_balance_volume'],
  },
  vwap: { level: 'keeps',
    label: 'Volume-weighted average price', family: 'Technicals', unit: 'same',
    about: 'Σ price × volume / Σ volume over the last w observations (0: every one to date), each price times its own volume on its own dates; none where they traded none.',
    gsQuant: 'DataCube (a volume-weighted average price)',
  },
  tracking_error: {
    label: 'Tracking error', family: 'Risk', unit: 'percent',
    about: 'The annualized standard deviation of the first\'s simple returns less the benchmark\'s, over the last w observations, in percent.',
    gsQuant: 'DataCube (an active return\'s volatility)',
  },
  information_ratio: {
    label: 'Information ratio', family: 'Risk', unit: 'number',
    about: 'The mean of the first\'s simple returns less the benchmark\'s over their standard deviation, annualized, over the last w observations.',
    gsQuant: 'DataCube (an active return\'s Sharpe ratio)',
  },
  up_capture: {
    label: 'Up capture', family: 'Risk', unit: 'ratio',
    about: 'The first\'s arithmetic mean simple return on the periods the benchmark rose (above 0) over the benchmark\'s mean then, over the last w observations: above 100%, it rose more (Morningstar\'s compounds each).',
    gsQuant: 'DataCube (an up-market capture ratio)',
  },
  down_capture: {
    label: 'Down capture', family: 'Risk', unit: 'ratio',
    about: 'The first\'s arithmetic mean simple return on the periods the benchmark fell (below 0) over the benchmark\'s mean then, over the last w observations: under 100%, it fell less (Morningstar\'s compounds each).',
    gsQuant: 'DataCube (a down-market capture ratio)',
  },
  total_return: { level: 'keeps',
    label: 'Total return', family: 'Returns', unit: 'same',
    about: 'The price with its dividends reinvested, from its first value: each period\'s (price + the dividends since the price before) / the price before, compounded, on the price\'s dates -- the second a series of dividends on their dates (one of a date with no price paid at the next), or a number: a dividend yield a year, accrued day by day.',
    gsQuant: 'DataCube (a total return index)', aliases: ['tr'],
  },
  basket: { level: 'makes',
    label: 'Basket, rebalanced', family: 'Math', unit: 'index',
    params: { rebalance: 'daily: its weights every date; weekly, monthly: every week or month from its first date (the first date on or after)' },
    about: 'Series held in weights, from 100, on the dates all have: basket(x, 0.6, y, 0.4) -- units fixed between rebalances, each a series\' weight of the basket then.',
    gsQuant: 'backtesting.basket_series', differs: 'no costs; each series followed by its weight (gs-quant: a list of series and a list of weights)',
  },
  lead_lag: {
    label: 'Lead-lag correlation', family: 'Relationships', unit: 'number', bounds: [-1, 1],
    params: { lag: 'the second\'s returns this many observations before (negative: after) -- above 0, does the second lead?' },
    about: 'The rolling correlation of the first\'s simple returns with the second\'s lag observations before, each on its own dates, over the last w.',
    gsQuant: 'DataCube (econometrics.correlation of a lag)',
  },
  dickey_fuller: {
    label: 'Dickey-Fuller statistic', family: 'Relationships', unit: 'number',
    about: 'The t-statistic of b in Δx = a + b·x[-1], by least squares over the last w differences (w + 1 values; 0: every one to date): far below 0 it reverts -- at 5%, under about −2.9 (MacKinnon\'s: −2.86 as w grows, −2.97 at 30). Of a spread written with its ratio, dickey_fuller(close[A] - 1.2 * close[B]), its stationarity; a running residual(y, x, 0) is each date\'s own fit, not Engle and Granger\'s one fit of the whole.',
    gsQuant: 'DataCube (statsmodels\' adfuller of no lags, a constant)', aliases: ['adf'],
  },
  half_life: {
    label: 'Half-life of mean reversion', family: 'Relationships', unit: 'number',
    about: '−ln 2 / ln(1 + b), b the slope of Δx = a + b·x[-1] over the last w differences (w + 1 values; 0: every one to date): the observations a gap takes to halve; none where it does not revert (b at or above 0) or overshoots (b at or below −1).',
    gsQuant: 'DataCube (an Ornstein-Uhlenbeck half-life)',
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
  // vol_swap_volatility's first value is gs-quant's own (its Window(n, n - 1): the Phase 1 review's 7)
  const differs = [words.differs, sig.params.some((p) => p.name === 'w' && p.kind === 'span') && name !== 'vol_swap_volatility' ? RAMP_DIFFERS : undefined,
    sig.params.some((p) => p.name === 'obs') ? OBS_DIFFERS : undefined,
    name === 'beta' ? BETA_DIFFERS : undefined].filter((d): d is string => d !== undefined);
  return { name, series: sig.series, ...(sig.reads !== undefined ? { reads: sig.reads } : {}), params, ...words, ...(differs.length > 0 ? { differs: differs.join('; ') } : {}) };
});

/** The functions by name. */
export const TS_FUNCTION: ReadonlyMap<TsFunctionName, TsFunction> = new Map(TS_FUNCTIONS.map((f) => [f.name, f]));

/**
 * gs-quant's functions DataCube does not compute, each with why: typed, they are refused in these words rather than
 * read as a column of the grid.
 */
export const NOT_BUILT: Readonly<Record<string, string>> = {
  smooth_outliers: 'an iterative procedure (rounds of a centred rolling median until no new outlier), which no query states: smooth_spikes takes out a value beyond both its neighbours',
  repeat: 'it makes dates the data does not have: give the tile a frequency, or combine two series with its align',
  date_range: 'the tile\'s dates are its range: set it',
  filter_dates: 'the tile\'s dates are its range: set it',
  bucketize: 'a value per period is the tile\'s frequency: set it',
  align_calendar: 'a holiday calendar is none of the tile\'s sources',
  day_countdown: 'it makes dates the data does not have',
  generate_series: 'random data: read a grid of your own',
  corr_swap_correlation: 'it needs an index\'s constituents and their weights',
  mode: 'the most frequent value of a window: no aggregate every database shares',
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
      // a condition: 1 where it holds, 0 where not
      if (isConditionOp(e.op)) return 'number';
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
      // an if's unit is its branches' (a number branch takes the other's)
      if (e.fn === 'if') return e.args[1]!.kind === 'number' ? unitOf(e.args[2]!) : unitOf(e.args[1]!);
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
      if (isConditionOp(e.op)) return false;
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
      // a level of every series it reads: an if's branches, a splice's two, a weighted sum's series of positive weights
      if (e.fn === 'if') return levelOf(e.args[1]!) && levelOf(e.args[2]!);
      if (e.fn === 'union' || e.fn === 'append' || e.fn === 'prepend') return levelOf(e.args[0]!) && levelOf(e.args[1]!);
      if (e.fn === 'weighted_sum') {
        return e.args.every((a, i) => (i % 2 === 0 ? levelOf(a) : a.kind === 'number' && a.value > 0));
      }
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

/** Whether an expression is a CONDITION -- a comparison, or conditions joined by and, or -- drawn as the periods it
 *  holds, shaded (timeseries-option.ts), its runs the database's (timeseries-query.ts). */
export function isCondition(e: TsExpr): boolean {
  return (e.kind === 'binary' && isConditionOp(e.op)) || (e.kind === 'call' && e.fn === 'not');
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
    // a comparison (a condition): two characters before one
    const two = text.slice(i, i + 2);
    if (two === '<=' || two === '>=' || two === '==' || two === '!=') {
      out.push({ t: 'op', v: two, at });
      i += 2;
      continue;
    }
    if ('+-*/(),[]=.:<>'.includes(c)) {
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

  // conditions: `a or b`, `a and b`, a comparison of two sums -- each 1 where it holds, 0 where not
  const word = (w: string): boolean => {
    const t = peek();
    return t.t === 'id' && !t.quoted && t.v.toLowerCase() === w;
  };
  const either = (): TsExpr => {
    let left = both();
    while (word('or')) {
      next();
      left = { kind: 'binary', op: 'or', left, right: both() };
    }
    return left;
  };
  const both = (): TsExpr => {
    let left = comparison();
    while (word('and')) {
      next();
      left = { kind: 'binary', op: 'and', left, right: comparison() };
    }
    return left;
  };
  const comparison = (): TsExpr => {
    const left = sum();
    const t = peek();
    if (t.t === 'op' && (TS_COMPARISONS as readonly string[]).includes(t.v)) {
      next();
      const right = sum();
      const again = peek();
      if (again.t === 'op' && (TS_COMPARISONS as readonly string[]).includes(again.v)) {
        throw new TsSyntaxError('a comparison compares two sides: join two with and, or', again.at);
      }
      return { kind: 'binary', op: t.v as TsOperator, left, right };
    }
    return left;
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
        if (f.series === 'many' || args.length < f.series) {
          args.push(either());
          continue;
        }
        const param = f.params[positional++];
        if (!param) throw new TsSyntaxError(`${f.name} takes ${seriesWords(f.series, f.reads)}${f.params.length > 0 ? ` and ${f.params.map((q) => q.name).join(', ')}` : ''}`, t.at);
        params[param.name] = paramValue(param);
      } while (isOp(',') && next());
    }
    expect(')');
    const many = f.series === 'many' ? manyProblem(f.name, args) : undefined;
    if (many !== undefined) throw new TsSyntaxError(`${f.name} ${many}`, at);
    if (f.series !== 'many' && args.length < f.series) throw new TsSyntaxError(`${f.name} reads ${seriesWords(f.series, f.reads)}`, at);
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
    if (param.above !== undefined && n <= param.above) throw new TsSyntaxError(`${param.name} is above ${param.above}`, t.at);
    if (param.below !== undefined && n >= param.below) throw new TsSyntaxError(`${param.name} is below ${param.below}`, t.at);
    if (param.max !== undefined && n > param.max) throw new TsSyntaxError(`${param.name} is at most ${param.max}`, t.at);
    return n;
  };
  const primary = (): TsExpr => {
    const t = next();
    if (t.t === 'num') return { kind: 'number', value: t.v };
    if (t.t === 'ref') return { kind: 'ref', series: t.v };
    if (t.t === 'op' && t.v === '(') {
      const inner = either();
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

  const e = either();
  const last = peek();
  if (last.t !== 'end') throw new TsSyntaxError('the expression goes on past its end', last.at);
  return e;
}

const WORD = /^[A-Za-z_][A-Za-z0-9_]*$/;
const PRECEDENCE: Readonly<Record<TsOperator, number>> = {
  'or': 1, 'and': 2, '<': 3, '<=': 3, '>': 3, '>=': 3, '==': 3, '!=': 3, '+': 4, '-': 4, '*': 5, '/': 5,
};

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
  return written(e, outer, false);
}

/** Matches as a person types them inside a column's brackets (`AAPL, side="BUY"`): what reads back as them. */
export function matchesText(where: readonly TsMatch[]): string {
  return where.map((m) => (m.column !== undefined ? `${m.column}=${valueText(m.value)}` : valueText(m.value))).join(', ');
}

/** Matches typed as inside a column's brackets (`AAPL, side="BUY"`), read as a column's are; none for no text. */
export function parseMatches(text: string): TsMatch[] {
  if (text.trim() === '') return [];
  const e = parseExpr(`x[${text}]`);
  if (e.kind !== 'column' || e.column !== 'x' || e.fn !== undefined || e.source !== undefined) {
    throw new TsSyntaxError(`"${text}" is not a list of matches (AAPL, side="BUY")`, 0);
  }
  return [...(e.where ?? [])];
}

/**
 * AN EXPRESSION'S TEXT THAT READS BACK AS ITSELF (the code that builds a tile, timeseries-code.ts): as exprText writes
 * it, but each parameter as it was told -- one told by its place while every one before it is told, else by its name
 * (`returns(close, type=log)`), one not told left out -- so parseExpr gives the very expression again (exprText's own
 * drops a default told and writes a default before a later one told: the same figures, another spec).
 */
export function exprSource(e: TsExpr): string {
  return written(e, 0, true);
}

function written(e: TsExpr, outer: number, exact: boolean): string {
  switch (e.kind) {
    case 'number':
      return numberText(e.value);
    case 'ref':
      return `$${e.series}`;
    case 'column': {
      const where = e.where ?? [];
      return `${e.source !== undefined ? `${e.source}:` : ''}${columnText(e.column)}${where.length > 0 ? `[${matchesText(where)}]` : ''}${e.fn !== undefined ? `.${e.fn}` : ''}`;
    }
    case 'binary': {
      const mine = PRECEDENCE[e.op];
      // a minus or a division binds its right side tighter: a - (b - c), a / (b * c); a comparison both (it is no chain)
      const compares = mine === PRECEDENCE['<'];
      const right = e.op === '-' || e.op === '/' || compares ? mine + 1 : mine;
      const text = `${written(e.left, compares ? mine + 1 : mine, exact)} ${e.op} ${written(e.right, right, exact)}`;
      return mine < outer ? `(${text})` : text;
    }
    case 'call': {
      const f = TS_FUNCTION.get(e.fn);
      const args = e.args.map((a) => written(a, 0, exact));
      const value = (v: number | string): string => (typeof v === 'number' ? numberText(v) : v);
      // a function of many series' parameters by their names: after its series and weights in pairs, a word by its place
      // reads as one more series (the basket's `monthly`)
      const named = f?.series === 'many';
      if (f && exact) {
        // each told as it was: by its place while every one before it is told, then by its name
        let placed = !named;
        for (const q of f.params) {
          const v = e.params?.[q.name];
          if (v === undefined) {
            placed = false;
            continue;
          }
          args.push(placed ? value(v) : `${q.name}=${value(v)}`);
        }
        const unknown = Object.keys(e.params ?? {}).filter((k) => !f.params.some((q) => q.name === k));
        if (unknown.length > 0) throw new Error(`${e.fn} takes no ${unknown.join(', ')}`);
      } else if (f) {
        const told = f.params.map((q) => e.params?.[q.name]);
        let lastTold = told.length - 1;
        while (lastTold >= 0 && (told[lastTold] === undefined || told[lastTold] === f.params[lastTold]!.default)) lastTold--;
        for (let i = 0; i <= lastTold; i++) {
          const v = told[i] ?? f.params[i]!.default;
          if (named && told[i] === undefined) continue;
          args.push(named ? `${f.params[i]!.name}=${value(v)}` : value(v));
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
  // marks from a grid's rows: each column they read one of their source's, the one they are drawn at a number
  for (const [i, a] of (ts.annotations ?? []).entries()) {
    if (a.kind !== 'marks') continue;
    const name = `Marks ${a.label !== undefined ? `"${a.label}"` : i + 1}`;
    const typeOf = types.get(a.source);
    const src = ts.sources.find((x) => x.alias === a.source);
    if (!typeOf || !src) continue;
    for (const c of [a.at, a.text, ...(a.where ?? []).map((m) => m.column ?? src.key)]) {
      if (c !== undefined && !typeOf.has(c)) out.push(`${name} read '${c}', which source ${a.source} has not.`);
    }
    if (a.at !== undefined && typeOf.has(a.at) && !isNumeric(typeOf.get(a.at))) out.push(`${name} are drawn at '${a.at}', which is not a number.`);
    if (src.key === undefined && (a.where ?? []).some((m) => m.column === undefined)) {
      out.push(`${name} match a value with no column ([AAPL]): choose source ${src.alias}'s instrument column (Key).`);
    }
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
