// THE TILE'S WORDS AND THEIR SIGNATURES' SHAPE (docs/TS_DECISIONS_2026_11_07.md §4, the order's steps 1 and 2): the words
// a tile draws, SQL's per point among them, and the signature a word has as a tile holds it -- core's catalog's
// (generated/ts-signatures.ts), a whole line's word holding no window. A module of no imports, so the builder's
// generator (tools/ts-builder/make.mts) reads the tile's words as the tile does.

/** The words a tile draws, in the catalog's order: core's catalog's (generated/ts-signatures.ts) and SQL's per point
 *  (log, power, floor, ...: {@link SCALARS}). */
export const TS_FUNCTION_NAMES = [
  'returns', 'prices', 'diff', 'lag', 'index', 'rebase', 'change', 'percent_change', 'annualize',
  'mean', 'sum', 'min', 'max', 'range', 'std', 'var', 'zscores', 'winsorize',
  'volatility', 'drawdown', 'max_drawdown', 'sharpe_ratio',
  'correlation', 'beta', 'cov', 'alpha', 'r_squared', 'hedge_ratio', 'residual',
  'bollinger_bands', 'rsi', 'ohlc',
  'exponential_moving_average', 'exponential_std', 'exponential_volatility', 'exponential_spread_volatility', 'macd',
  'smoothed_moving_average', 'percentiles', 'median', 'percentile',
  'log', 'exp', 'sqrt', 'abs', 'power', 'floor', 'ceil',
  'if', 'not', 'compare', 'filter', 'product', 'geometrically_aggregate', 'weighted_sum', 'smooth_spikes',
  'excess_returns', 'cash_index', 'vol_swap_volatility', 'consecutive', 'trend', 'seasonally_adjusted',
  'interpolate', 'union', 'append', 'prepend', 'first', 'last', 'count', 'day', 'month', 'year', 'quarter', 'weekday',
  'atr', 'stochastic', 'keltner_channels', 'donchian_channels', 'obv', 'vwap',
  'tracking_error', 'information_ratio', 'up_capture', 'down_capture', 'total_return', 'basket',
  'lead_lag', 'dickey_fuller', 'half_life',
] as const;

export type TsFunctionName = typeof TS_FUNCTION_NAMES[number];

/** A function's parameter as a spec holds it: a whole number, a span (a window, a lag: a whole number of observations
 *  or a calendar period, `3m`), a number, or a word (a kind of return) -- core's catalog's (generated/ts-signatures.ts). */
export interface TsParamShape {
  readonly name: string;
  readonly kind: 'whole' | 'span' | 'number' | 'word';
  /** The language's default, where a call leaves it out (the library's `timeseries::Words.given`, each proved its call
   *  without it: TsIslandTest.everyWordsStatedDefaultIsItsCallWithoutIt); none where every call gives it (a mean's
   *  window: the spec's §7) or the library's call without it is its own (an annualization factor: the frequency's). */
  readonly default?: number | string;
  readonly words?: readonly string[];
  /** The least a number may be. */
  readonly min?: number;
  /** What a number must be above (a deviation's weight of the one before: over 0, or it weighs one value alone). */
  readonly above?: number;
  /** What a number must be below (an exponential average's weight: under 1). */
  readonly below?: number;
  /** The most a number may be (a percentile's: 100). */
  readonly max?: number;
  /** Every call gives it (each of the word's overloads takes it). */
  readonly required: boolean;
  /** A window that takes the whole line, `whole` (a z-score's, a percentile's: the future read, said so), as the tile
   *  holds it: 'whole'. */
  readonly whole?: true;
  /** What a picked word begins it with, where every call gives it, the language has no default for it and gs-quant
   *  none either (the library's `timeseries::Words.begun`: a percentile's n 50). */
  readonly begin?: number | string;
  /** gs-quant's default of the parameter it names alike (gs-quant 2.2.3's, read from gs-quant itself): what a word is
   *  begun with where the language has none (the builders' rule, docs/TS_DECISIONS_2026_11_07.md §4). */
  readonly gsQuant?: number | string;
}

/**
 * A FUNCTION'S SIGNATURE: how many series it reads (one, or two: a correlation's) and its parameters -- what a saved
 * spec is read by (tsProblem). What it is called, what it computes, its units and how it is drawn are the catalog's
 * words (timeseries-expr.ts TS_FUNCTIONS), fetched with the viewer: a page reads its tiles at its start, and needs none
 * of them to.
 */
export interface TsSignature {
  /** One, two (a correlation's), three (an if's condition and its two branches), four (a candle's: its open, high, low
   *  and close), or `many`: series and their weights in pairs (a weighted sum's). */
  readonly series: 1 | 2 | 3 | 4 | 'many';
  /** What each series is, in order, where they are not x and y (a true range's high, low and close): said by its
   *  refusals and its reference. */
  readonly reads?: readonly string[];
  readonly params: readonly TsParamShape[];
}

/** SQL'S WORDS PER POINT, which no word of the library is yet (A4 makes them library words): their parameters gs-quant's
 *  (gs-quant 2.2.3's timeseries.algebra, as inspect.signature reads it: power's y 1), each given in every call -- the
 *  language writes them as SQL's (ln, power). floor and ceil are the library's since 2026-11-08 (register S161). */
export const SCALARS: Readonly<Partial<Record<TsFunctionName, TsSignature>>> = {
  not: { series: 1, params: [] }, log: { series: 1, params: [] }, exp: { series: 1, params: [] },
  sqrt: { series: 1, params: [] }, abs: { series: 1, params: [] },
  power: { series: 1, params: [{ name: 'y', kind: 'number', required: true, gsQuant: 1 }] },
};

/** The words whose window is the whole line, written after the series (`first(x, all)`, `last(x, whole)`): a tile's
 *  holds no window (timeseries-text.ts WHOLE). */
export const WHOLE_LINE: ReadonlySet<TsFunctionName> = new Set(['first', 'last', 'count']);

/** The tile's signatures: SQL's per point its own, every other word core's catalog's (`words`, generated), a whole
 *  line's word without its window. A word the tile draws that the catalog lacks is refused here, as the page loads. */
export function tileSignatures(words: Readonly<Record<string, TsSignature>>): Record<TsFunctionName, TsSignature> {
  return Object.fromEntries(TS_FUNCTION_NAMES.map((name) => {
    const own = SCALARS[name];
    if (own !== undefined) return [name, own];
    const sig = words[name];
    if (sig === undefined) throw new Error(`the tile draws ${name}, which is no word of core's catalog`);
    return [name, WHOLE_LINE.has(name) ? { ...sig, params: sig.params.filter((p) => p.name !== 'w') } : sig];
  })) as Record<TsFunctionName, TsSignature>;
}
