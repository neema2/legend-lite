// A TILE'S SERIES AS THE TIMESERIES LANGUAGE (docs/TS_LANGUAGE_BUILD_2026_10_30.md P5; the language,
// docs/TIMESERIES_LANGUAGE_2026_10_29.md): the #TS{...}# query that answers what the tile's own writer
// (timeseries-query.ts seriesQuery) answers -- each grid a source of the query, bound by `let` and read as `$alias`;
// the tile's frequency its SAMPLE BY, its alignment its FILL; its items the series' own text, written by the one writer
// (timeseries-text.ts statementItem, the order's step 4) -- each column as the statement reads it (its source's name, its
// split's subscript, its period's aggregate under the frequency, ruled 2026-11-08) and a base's first date told; what a
// condition, a bound and a division mean is the language's (registers S160, S161, S151). It writes words: the
// platform's #TS builder writes the Pure.

import { fn, lambda, lit, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { filterNode, sourceWithDerived } from './query.ts';
import { columnType, CubeRefusal } from './snapshot.ts';
import { isBand, isCandles, sourceAlias, sourcesOf, wholeOf } from './timeseries-expr.ts';
import type { TsInputs } from './timeseries-query.ts';
import { filterRead, paramValue, TS_SIGNATURES, writtenParams, type TimeseriesSpec, type TsAggregate, type TsExpr, type TsMatch, type TsSeries } from './timeseries-spec.ts';
import { statementItem, type TsStatementParts } from './timeseries-text.ts';
import type { TimeGrain } from './chart-spec.ts';

/** The answer's column of a series' value; a struct's fields are `ts_value_<field>` (R10). */
export const TS_ITEM = 'ts_value';

/** The functions of a base -- the line's first value, or its value as of the first date shown (`from`, the tile's
 *  anchor). */
const BASED: ReadonlySet<string> = new Set(['index', 'rebase', 'change', 'percent_change']);

/** Whether `e` reads a base: what an anchor changes. */
export function readsBase(e: TsExpr): boolean {
  switch (e.kind) {
    case 'call': return BASED.has(e.fn) || e.args.some(readsBase);
    case 'binary': return readsBase(e.left) || readsBase(e.right);
    default: return false;
  }
}

/** The functions the builder writes itself (TsToPure's along): their window, lag and type by place. */
const OWN: ReadonlySet<string> = new Set(['mean', 'sum', 'min', 'max', 'std', 'var', 'returns', 'diff', 'lag']);

const SAMPLE: Readonly<Record<TimeGrain, string>> = { day: '1d', week: '1w', month: '1mo', quarter: '1q', year: '1y' };

const FILL: Readonly<Record<string, string | undefined>> = {
  intersect: undefined, nan: 'NULL', zero: '0', time: 'LINEAR', step: 'STEP',
};

/** A period's aggregate, as the language names it (R5). */
const TAKEN: Readonly<Record<TsAggregate, string>> = { last: 'last', first: 'first', average: 'avg', sum: 'sum', min: 'min', max: 'max' };

/** How a period takes each series a candle or a technical reads, by its role (the tile's ROLE_TAKEN, CANDLE). */
const ROLE: Readonly<Record<string, TsAggregate>> = { open: 'first', high: 'max', low: 'min', close: 'last', price: 'last', volume: 'sum', dividends: 'sum' };
const CANDLE = ['open', 'high', 'low', 'close'] as const;

/** A decomposition's cycle in periods of the tile's frequency (the platform's TsFunctions' CYCLES, the tile's words). */
const CYCLES: Readonly<Partial<Record<TimeGrain, Readonly<Partial<Record<string, number>>>>>> = {
  day: { week: 5, month: 21, quarter: 63, year: 252 },
  week: { month: 4, quarter: 13, year: 52 },
  month: { quarter: 3, year: 12 },
  quarter: { year: 4 },
};

/** A place in words: first, second ... tenth, then 11th, 21st. */
function ordinal(n: number): string {
  const words = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth'];
  if (n >= 1 && n <= words.length) return words[n - 1]!;
  const tens = n % 100;
  const suffix = tens >= 11 && tens <= 13 ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' } as Record<number, string>)[n % 10] ?? 'th';
  return `${n}${suffix}`;
}

/**
 * WHAT THE TILE REFUSES IN ITS OWN WORDS before a query is written (its frequency, its parameters' words, a series
 * where one is read): the language's refusals say SAMPLE BY and `=>`, which a tile's user never writes.
 */
function refused(ts: TimeseriesSpec, e: Extract<TsExpr, { kind: 'call' }>): void {
  const sig = TS_SIGNATURES[e.fn];
  e.args.forEach((a, i) => {
    // a function's first series, a weighted sum's each (a number elsewhere is a branch's, a rate's: the language's)
    const series = sig.series === 'many' ? i % 2 === 0 : i === 0;
    if (series && a.kind === 'number') throw new CubeRefusal(`${e.fn} reads a series: its ${ordinal(i + 1)} argument is a number`);
    if (isBand(a)) throw new CubeRefusal('a band is drawn, never read');
    if (isCandles(a)) throw new CubeRefusal('candles are drawn, never read');
  });
  const param = (name: string): number | string | undefined => paramValue(e.fn, e.params, name);
  if (e.fn === 'basket') {
    const rebalance = String(param('rebalance'));
    if (!['daily', 'weekly', 'monthly'].includes(rebalance)) throw new CubeRefusal(`a basket rebalances daily, weekly or monthly, not ${JSON.stringify(rebalance)}`);
  }
  if (e.fn === 'trend' || e.fn === 'seasonally_adjusted') {
    const grain = ts.grain;
    const freq = String(param('freq'));
    if (grain === undefined) throw new CubeRefusal(`${e.fn} counts its cycle in the tile's periods: give the tile a frequency (Monthly)`);
    if (CYCLES[grain]?.[freq] === undefined) throw new CubeRefusal(`${e.fn}: a ${freq} is no cycle of ${grain === 'day' ? 'days' : `${grain}s`} -- choose a longer one, or another frequency`);
  }
  if (e.fn === 'dickey_fuller' || e.fn === 'half_life') {
    // w differences (w + 1 values): a fit of a constant and a slope over under 3 has no deviation left to judge it by
    const size = param('w');
    if (typeof size === 'number' && size > 0 && size < 3) throw new CubeRefusal(`${e.fn} over ${size} differences has no deviation left: w is 0 (every one to date) or at least 3`);
  }
}

/** A name as SQL writes it: as it is where it is a plain lower-case word, else quoted. */
function ident(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}

function literal(v: string | number | boolean): string {
  return typeof v === 'string' ? `'${v.replaceAll("'", "''")}'` : typeof v === 'boolean' ? (v ? 'TRUE' : 'FALSE') : String(v);
}

/** A window or a lag as the language writes it: observations, `all` for the tile's 0, a span (`3mo` for its `3m`). */
function span(v: number | string): string {
  if (typeof v === 'number') return v === 0 ? 'all' : String(v);
  // the whole line, its future read (a z-score's, a percentile's: said so, never a default); each date's
  // instruments (13c)
  if (v === 'whole' || v === 'across') return v;
  const m = /^([1-9][0-9]*)([dwmy])$/.exec(v);
  if (!m) throw new CubeRefusal(`a span the tile writes is n then d, w, m or y: ${v}`);
  return `${m[1]}${m[2] === 'm' ? 'mo' : m[2]}`;
}

/** The item the tile's query adds, never none, on every date the series' line has (R6 keeps a row where an item has
 *  a value): its dates whatever its value. */
export const TS_PRESENT = 'ts_present';

/** The functions a period reads its series by their roles (the tile's rolled: a candle's, a technical's). */
const ROLLED: ReadonlySet<string> = new Set(['ohlc', 'atr', 'stochastic', 'keltner_channels', 'donchian_channels', 'obv', 'vwap']);

/** An expression's text and the FROM it reads (`grain`: the periods its columns are taken by; none, as they are). */
interface Written {
  readonly text: (e: TsExpr, role?: TsAggregate) => string;
  readonly from: string;
}

/** The #TS text of `series` of the tile `ts`, each source read as `$<alias>`: its one item `ts_value` -- and, `present`,
 *  `ts_present` beside it. `anchor`: the first date shown, a base's (`from`). Under the tile's frequency a candle or a
 *  technical of computed series (and a VWAP, its traded value each row's) reads them computed on their own dates in a
 *  query of their own, each period then taken by its role (the tile's rolled: the review's N4). */
export function tsQueryText(ts: TimeseriesSpec, series: TsSeries, anchor?: string, present = false): string {
  const expr = wholeOf(series, ts.series);
  const grain = seriesGrain(ts, expr);
  const split = series.split;
  const fill = FILL[ts.align ?? 'intersect'];
  const filled = fill === undefined ? '' : ` FILL(${fill})`;
  // a struct read by a field (R10): a band's upper, a candle's close
  const field = isBand(expr) ? `${TS_ITEM}.upper` : isCandles(expr) ? `${TS_ITEM}.close` : TS_ITEM;
  const also = present ? `, CASE WHEN ${field} IS NULL THEN 0 ELSE 1 END AS ${TS_PRESENT}` : '';
  if (grain !== undefined && expr.kind === 'call' && ROLLED.has(expr.fn)
    && (expr.fn === 'vwap' || expr.args.some((a) => a.kind !== 'column'))) {
    refused(ts, expr);
    const daily = writer(ts, expr, split, undefined, anchor);
    const at = ts.sources.find((s) => s.alias === sourcesOf(ts, expr)[0])!.date;
    const own = (i: number): string => `${DAILY}.ts_a${i}`;
    let items: string[];
    let item: string;
    if (expr.fn === 'vwap') {
      // each period's traded value (each row's price times its volume) and volume, totalled; their ratio over w
      const [p, v] = [daily.text(expr.args[0]!), daily.text(expr.args[1]!)];
      items = [`(${p} * ${v}) AS ts_a0`, `${v} AS ts_a1`];
      const w = span(paramOf(expr, 'w'));
      // #TS's / gives nothing where the divisor is 0 (core's TsSql, S151): no nullif of the tile's own
      item = `(sum(sum(${own(0)}), ${w}) / sum(sum(${own(1)}), ${w}))`;
    } else {
      const roles = expr.fn === 'ohlc' ? CANDLE : TS_SIGNATURES[expr.fn].reads!;
      items = expr.args.map((a, i) => `${daily.text(a)} AS ts_a${i}`);
      item = `${expr.fn}(${[...roles.map((r, i) => `${TAKEN[ROLE[r]!]}(${own(i)})`), ...named(expr, anchor)].join(', ')})`;
    }
    return `WITH ${DAILY} AS TS (SELECT ${items.join(', ')} FROM ${daily.from}${filled}) SELECT ${item} AS ${TS_ITEM}${also}`
      + ` FROM ${DAILY} ON ${ident(at)}${split === undefined ? '' : ` BY ${ident(split)}`} SAMPLE BY ${SAMPLE[grain]}`;
  }
  const w = writer(ts, expr, split, grain, anchor);
  return `SELECT ${w.text(expr)} AS ${TS_ITEM}${also} FROM ${w.from}${grain === undefined ? '' : ` SAMPLE BY ${SAMPLE[grain]}`}${filled}`;
}

/** The #TS item of `series` as `tsQueryText` writes it, `<expression> AS ts_value` -- what a formula box holds of it
 *  (A3 step 2: core reads it back, the one reader). Undefined where the series is a query of its own (a candle or a
 *  technical of computed series under the tile's frequency, a VWAP's): its item reads that query's columns. */
export function tsItemText(ts: TimeseriesSpec, series: TsSeries, anchor?: string): string | undefined {
  const expr = wholeOf(series, ts.series);
  const grain = seriesGrain(ts, expr);
  if (grain !== undefined && expr.kind === 'call' && ROLLED.has(expr.fn)
    && (expr.fn === 'vwap' || expr.args.some((a) => a.kind !== 'column'))) {
    return undefined;
  }
  return `${writer(ts, expr, series.split, grain, anchor).text(expr)} AS ${TS_ITEM}`;
}

/**
 * THE PERIODS A SERIES IS READ BY: the tile's frequency; else, where a column says how a date takes its rows
 * (`volume.sum`, a sector's day of volume: the tile's own `.sum`, `.first`, `.max`), a day -- one value a date, as the
 * aggregate asks (ruling 8: without it a line is its column's rows, several rows on a date several points); else none.
 */
export function seriesGrain(ts: TimeseriesSpec, expr: TsExpr): TimeGrain | undefined {
  if (ts.grain !== undefined) return ts.grain;
  const takes = (e: TsExpr): boolean => e.kind === 'column' ? e.fn !== undefined
    : e.kind === 'call' ? e.args.some(takes) : e.kind === 'binary' ? takes(e.left) || takes(e.right) : false;
  return takes(expr) ? 'day' : undefined;
}

/** The name of a series' own query, its columns computed on their sources' dates. */
const DAILY = 'ts_daily';

/** A parameter of a call, as written or its default. */
function paramOf(e: Extract<TsExpr, { kind: 'call' }>, name: string): number | string {
  const v = paramValue(e.fn, e.params, name);
  if (v === undefined) throw new Error(`${e.fn}'s ${name} is neither told nor defaulted`);
  return v;
}

/** A call's parameters, as the language takes them: the builder's own by place, the catalog's by name (R10), a base's
 *  `from` the tile's anchor. */
function named(e: Extract<TsExpr, { kind: 'call' }>, anchor: string | undefined): string[] {
  // each told, else its default; one of neither left out (the language's own: an annualization factor the frequency's)
  const written = writtenParams(e.fn, e.params).map(([p, v]) =>
    [p.name, p.kind === 'span' ? span(v) : p.kind === 'word' ? literal(String(v)) : String(v)] as const);
  const params = OWN.has(e.fn) ? written.map(([, v]) => v) : written.map(([n, v]) => `${n} => ${v}`);
  if (anchor !== undefined && BASED.has(e.fn)) params.push(`from => ${literal(anchor)}`);
  return params;
}

/** The writing of `expr`'s parts: its columns (each by its period's aggregate under `grain`), its functions, its FROM. */
function writer(ts: TimeseriesSpec, expr: TsExpr, split: string | undefined, grain: TimeGrain | undefined, anchor: string | undefined): Written {
  const read = sourcesOf(ts, expr);
  // each source's BY: the split's column where the series splits on it (its first source), else none -- a match on
  // its instrument written as a condition, so a line of no subscript is its column's rows, as the tile's
  const by = (alias: string): string | undefined => (alias === read[0] ? split : undefined);
  // a match on the split source's instrument where the split is another column (a split by sector, close[SPY]): one
  // line every line reads (the tile's matchesSplit), read from the source again, of no BY
  const one = (alias: string, where: readonly TsMatch[]): boolean => {
    const src = ts.sources.find((s) => s.alias === alias)!;
    const b = by(alias);
    return b !== undefined && src.key !== undefined && src.key !== b && where.some((m) => (m.column ?? src.key) === src.key);
  };
  const ones = new Set<string>();
  const scan = (e: TsExpr): void => {
    if (e.kind === 'column' && one(sourceAlias(ts, e)!, e.where ?? [])) ones.add(sourceAlias(ts, e)!);
    if (e.kind === 'call') e.args.forEach(scan);
    if (e.kind === 'binary') { scan(e.left); scan(e.right); }
  };
  scan(expr);
  const many = read.length + ones.size > 1;
  const column = (e: Extract<TsExpr, { kind: 'column' }>, role?: TsAggregate): string => {
    const alias = sourceAlias(ts, e)!;
    const src = ts.sources.find((s) => s.alias === alias)!;
    const once = one(alias, e.where ?? []);
    const matches = (e.where ?? []).map((m: TsMatch) => {
      const c = m.column ?? src.key;
      if (c === undefined) throw new CubeRefusal(`${alias} has no instrument column to match ${String(m.value)}`);
      return !once && c === by(alias) ? literal(m.value) : `${ident(c)} = ${literal(m.value)}`;
    });
    const named = once ? `${alias}_one` : alias;
    const col = `${many ? `${ident(named)}.` : ''}${ident(e.column)}${matches.length === 0 ? '' : `[${matches.join(', ')}]`}`;
    return grain === undefined ? col : `${TAKEN[e.fn ?? role ?? 'last']}(${col})`;
  };
  // what the tile refuses in its own words, before a word of the statement is written
  const check = (e: TsExpr): void => {
    if (e.kind === 'call') {
      refused(ts, e);
      e.args.forEach(check);
    } else if (e.kind === 'binary') {
      for (const side of [e.left, e.right]) {
        if (isBand(side)) throw new CubeRefusal('a band is drawn, never read');
        if (isCandles(side)) throw new CubeRefusal('candles are drawn, never read');
      }
      check(e.left);
      check(e.right);
    } else if (e.kind === 'ref') {
      throw new Error('a series is read inlined');
    }
  };
  // THE ONE WRITER (the order's step 4): the series' own text (timeseries-text.ts), each column as this statement reads
  // it and a base's first date told
  const parts: TsStatementParts = {
    column: (e, role) => column(e, role === undefined ? undefined : ROLE[role]),
    added: (e) => (anchor !== undefined && BASED.has(e.fn) ? [`from => ${literal(anchor)}`] : []),
  };
  const text = (e: TsExpr, role?: TsAggregate): string => {
    if (e.kind === 'column') return column(e, role);
    check(e);
    return statementItem(e, parts);
  };
  const sources = read.map((alias) => {
    const src = ts.sources.find((s) => s.alias === alias)!;
    const b = by(alias);
    return `$${alias} ${ident(alias)} ON ${ident(src.date)}${b === undefined ? '' : ` BY ${ident(b)}`}`;
  });
  for (const alias of ones) sources.push(`$${alias} ${ident(`${alias}_one`)} ON ${ident(ts.sources.find((s) => s.alias === alias)!.date)}`);
  return { text, from: sources.join(', ') };
}

/** Each source of `read` bound by `let` to its grid's rows (its derived columns too) under the grid's filter as the
 *  tile reads it (filterRead: a frozen tile's own), as the #TS query reads them. */
export function tsLets(inputs: TsInputs, ts: TimeseriesSpec, read: readonly string[]): ValueSpecification[] {
  return read.map((alias) => {
    const input = inputs.get(alias);
    const src = ts.sources.find((s) => s.alias === alias);
    if (!input || !src) throw new CubeRefusal(`source ${alias}'s grid is not open`);
    const s = input.snapshot;
    const filter = filterRead(src, input.filter);
    const rows = sourceWithDerived(s);
    return fn('letFunction', lit.string(alias), (filter === undefined ? rows
      : rows.filter(lambda(['x'], filterNode(filter, 'x', (c) => columnType(s, c))))).node);
  });
}

/** The #TS{...}# island of `text`, as the wire holds it. */
export function tsIsland(text: string): ValueSpecification {
  return { _type: 'classInstance', type: 'TS', value: { ts: text } };
}

/** The #TS query of `series`: each source it reads bound by `let` to its grid's rows (its derived columns too), then the
 *  island over them. */
export function tsSeriesQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, anchor?: string): Lambda {
  const read = sourcesOf(ts, wholeOf(series, ts.series));
  return lambda([], ...tsLets(inputs, ts, read), tsIsland(tsQueryText(ts, series, anchor)));
}
