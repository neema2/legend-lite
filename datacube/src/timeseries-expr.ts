// A TIMESERIES' EXPRESSIONS AND ITS RULES AGAINST ITS GRIDS (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §3-§4): what
// an expression reads, what its figures are and what it inlines; what stops a spec from being drawn over its grids' columns
// (timeseriesProblems); and a first source and a first series of a grid (sourceOf, defaultTimeseries).
//
// Apart from the spec's reader (timeseries-spec.ts, which a page reads at its start): this is the viewer's and the
// headless tile's, fetched with the first timeseries tile drawn (page/cube-page.ts) -- a page with none loads none of it
// (test/bundle-budget.test.ts). Nothing here computes a value either, and nothing reads or prints a text: a series'
// text is #TS, read by core alone and written by timeseries-text.ts (docs/TS_DECISIONS_2026_11_07.md §4, the order's
// step 3).

import { isNumeric, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import { NOT_BUILT, textProblem, treeOf, writtenText } from './timeseries-text.ts';
import { WORD_FACTS } from './generated/ts-facts.ts';
import {
  isConditionOp, treeProblem, TS_SIGNATURES, type TimeseriesSpec, type TsExpr, type TsFunctionName, type TsParamShape,
  type TsSeries, type TsSource, paramValue, TS_FUNCTION_NAMES,
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
  annualization_factor: 'the periods a year its figure is annualized by; where not told, the frequency\'s periods a year, else what its dates infer',
};

/** Where every rolling figure's first value differs from gs-quant's: drawn at the observation its window first holds
 *  w, where gs-quant's ramp (apply_ramp, x[w:]) drops that one too. */
const RAMP_DIFFERS = 'a window of w observations drawn from the observation it first holds w (gs-quant\'s ramp starts one observation later); of a calendar period, from its first date that period on, as gs-quant\'s';
/** Where a lag by a calendar period differs: the value as of the date that far back, where gs-quant's lag moves each
 *  date on by the period and keeps only the dates it lands on. */
const OBS_DIFFERS = 'obs a calendar period back is the value as of that date, its last at or before it (gs-quant\'s only on the dates whose date that far back has a value)';
/** Where beta differs on two calendars: each series' returns on its own dates, joined on the dates both have. */
const BETA_DIFFERS = 'on two calendars, over the dates both have (gs-quant takes the covariance over every date either has, over the benchmark\'s own variance)';

/** SQL'S WORDS PER POINT, which no word of the library is yet (A4 makes them library words): their facts the tile's own
 *  (timeseries-spec.ts SCALARS, their signatures). */
const SCALAR_WORDS: Readonly<Partial<Record<TsFunctionName, TsWords>>> = {
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
  not: {
    label: 'Not', family: 'Math', unit: 'number', about: 'A condition turned over: 1 where it does not hold, 0 where it does.',
    gsQuant: 'algebra.not_', aliases: ['not_'], differs: 'a value other than 0 holds (gs-quant refuses values but 0 and 1)',
  },
};

/**
 * THE FUNCTIONS' WORDS, gs-quant's names first (PlotTool Pro's words), each with what it computes -- and, where
 * DataCube's differs, how: the library's own facts (its timeseries::Words tags, generated/ts-facts.ts WORD_FACTS:
 * the order's step 1, 2026-11-07, in place of the tile's hand-kept table), SQL's per point the tile's. Every one is
 * written by timeseries-query.ts as columns the database computes.
 */
export const WORDS: Readonly<Record<TsFunctionName, TsWords>> = Object.fromEntries(TS_FUNCTION_NAMES.map((name) => {
  const words = SCALAR_WORDS[name] ?? WORD_FACTS[name];
  if (words === undefined) throw new Error(`the tile draws ${name}, whose facts the library does not tell`);
  return [name, words];
})) as Record<TsFunctionName, TsWords>;

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
export { NOT_BUILT };


/** Every name a person may type, an alias to its function. */
const TYPED_NAMES: ReadonlyMap<string, TsFunction> = new Map(TS_FUNCTIONS.flatMap((f) => [
  [f.name, f] as const, ...(f.aliases ?? []).map((a) => [a, f] as const),
]));

/** The function a typed name means, by its name or an alias. */
export function functionNamed(name: string): TsFunction | undefined {
  return TYPED_NAMES.get(name.toLowerCase());
}

/** A function's parameter as a call has it: told, else its default -- undefined where neither (an optional the
 *  library has no default for). */
export function paramOf(call: Extract<TsExpr, { kind: 'call' }>, name: string): number | string | undefined {
  return paramValue(call.fn, call.params, name);
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

/** A series' tree with every series it reads inlined: what is computed of it. */
export function wholeOf(s: TsSeries, series: readonly TsSeries[]): TsExpr {
  return inlined(treeOf(s, series), series);
}

/** A series that cannot be inlined: one reading itself, or a series there is not -- said where it is drawn. */
export class TsInlineError extends Error {}

/** An expression with every `$A` replaced by A's own expression (refs checked first: timeseriesProblems). */
export function inlined(e: TsExpr, series: readonly TsSeries[], seen: readonly string[] = []): TsExpr {
  switch (e.kind) {
    case 'ref': {
      if (seen.includes(e.series)) throw new TsInlineError(`series ${e.series} reads itself: ${[...seen, e.series].join(' reads ')}`);
      const s = series.find((x) => x.id === e.series);
      if (!s) throw new TsInlineError(`there is no series ${e.series}`);
      return inlined(treeOf(s, series), series, [...seen, e.series]);
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

/** A series' name as the legend says it: its own, else its text. */
export function seriesName(s: TsSeries): string {
  return s.name ?? s.text;
}

/**
 * A line's name: its series' -- and, one of a split series' lines, its value with it: a named series' after its name
 * ("Yield 10Y"), a plain column's alone ("AAPL"), else before its expression ("AAPL volatility(close, 63)").
 */
export function splitName(s: TsSeries, split: unknown, series: readonly TsSeries[]): string {
  if (split === undefined) return seriesName(s);
  if (s.name !== undefined) return `${s.name} ${String(split)}`;
  const tree = textProblem(s) === null ? treeOf(s, series) : undefined;
  const plain = tree?.kind === 'column' && (tree.where ?? []).length === 0;
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
    // its text as core reads it, judged by the tile's words (timeseries-text.ts): one that is no series, said
    const unread = textProblem(s);
    if (unread !== null) {
      out.push(`${unread}.`);
      continue;
    }
    const tree = treeOf(s, ts.series);
    const wrong = treeProblem(tree, name, ts.sources.map((x) => x.alias));
    if (wrong !== null) {
      out.push(`${wrong}.`);
      continue;
    }
    for (const ref of refsOf(tree)) {
      // every ref is a series of the tile (a name of none reads a column: treeOf), one whose text is no series said
      const other = ts.series.find((x) => x.id === ref)!;
      if (textProblem(other) !== null) continue;
      if ((other.split ?? null) !== (s.split ?? null)) out.push(`${name} reads series ${ref}, split ${other.split === undefined ? 'by nothing' : `by '${other.split}'`}: a series reads only series split as it is.`);
      const drawn = drawnOnly(treeOf(other, ts.series));
      if (drawn !== undefined) out.push(`${name} reads series ${ref}, ${drawn}: ${drawn} ${drawn === 'candles' ? 'are' : 'is'} drawn, never read.`);
    }
    let whole: TsExpr;
    try {
      whole = wholeOf(s, ts.series);
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
    series: [{ id: 'A', text: writtenText(columnRef(value), []), ...(source.key ? { split: source.key } : {}) }],
    ...(rebased ? { view: { compare: 'rebase' } } : {}),
  };
}
