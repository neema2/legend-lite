// A TIMESERIES' QUERIES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5): each series of a timeseries tile
// (timeseries-spec.ts) as ONE query (D2) -- of its grid, or, a series combining grids, of their tables over one model --
// which the planner plans and the DATABASE computes: every return, volatility, z-score and drawdown a window over the
// dates or an expression over the row, as every calculated column of DataCube's is (calc-presets.ts). Nothing here
// computes a value; it writes what to compute, in Pure (D5).
//
//   1. each source the series reads: its grid's rows -- their source, row-level calculated columns and filter (as the
//      tile reads it) -- and what the series matches of them (close[AAPL])
//   2. each grouped by its period (its date at the tile's frequency: a day, a week, a month...) -- the first by the
//      split too, one line per value -- each column the series reads taken by its period's aggregate: the latest row's
//      value by default (maxBy over the day's number, `dateDiff` from 1970: maxBy orders by a number); the groups of
//      several sources joined on the period
//   3. the dates every column the series reads has (gs-quant's `intersect`), the split's first MAX_SPLIT_LINES values
//   4. each function of the expression, innermost first: its columns over the dates, every window partitioned by the
//      split and ordered by the period; a rolling figure drawn once its window is full (gs-quant's ramp)
//   5. an index or a change from the start reads the value AS OF THE FIRST DATE SHOWN -- the anchor the viewer passes
//      (where its zoom starts), found by the database: the latest period at or before it that has a value, else the
//      first after it
//   6. the period, the split and the value(s), latest first, at most TS_POINT_LIMIT
//
// Every series reads ALL its history (step 5's anchor aside): the window shown is the viewer's, zoomed and panned over
// what came back, so a rolling figure at the window's left edge has its history, as a person expects it to.

import {
  agg, and, asc, collection, colSpec, derive, desc, enumValue, fn, lambda, lit, or, property, variable,
  type AppliedFunction, type ColSpec, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { LIBRARY_ENUMS, type LibraryEnum } from './generated/library-enums.ts';
import { LIBRARY_PACKAGE } from './pure-library.ts';
import { filterNode, literalNode, sourceWithDerived } from './query.ts';
import { columnType, CubeRefusal, type CubeSnapshot, type FilterNode } from './snapshot.ts';
import { WINDOW_ALL_PREFIX } from './grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import { isDay, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import {
  filterRead,
  MAX_SPLIT_LINES, PERIODS_PER_YEAR, RANGE_STEP, tenorOf, TS_MARK_LIMIT, TS_POINT_LIMIT, TS_PROJECTION_AHEAD, TS_PROJECTION_WINDOW, TS_RANGES,
  type TimeseriesSpec, type TsAggregate, type TsMarks, type TsProjection,
  type TsAlign, type TsExpr, type TsMatch, type TsOperator, type TsRange, type TsRangeWord, type TsSeries, type TsSource, type TsTenor,
  isConditionOp, TS_CURVE_DATES, TS_HIST_BINS, TS_SEASON_YEARS, TS_SIGNATURES, type TsFunctionName,
} from './timeseries-spec.ts';
import { inlined, isBand, isCandles, isCondition, levelOf, TsInlineError, paramOf, seriesName, sourceAlias, sourcesOf, splitName, unitOf, type TsUnit } from './timeseries-expr.ts';
import type { TimeGrain } from './chart-spec.ts';

/** The columns a series' query answers with. */
export const TS_DATE = '__ts_date';
export const TS_VALUE = '__ts_value';
export const TS_LOWER = '__ts_lower';
export const TS_UPPER = '__ts_upper';
/** A band's width, upper less lower: the database's, so the drawing stacks the band's fill on its lower edge. */
export const TS_WIDTH = '__ts_width';
/** A candle's open, high and low (its close is TS_VALUE): the period's first open, highest high, lowest low. */
export const TS_OPEN = '__ts_open';
export const TS_HIGH = '__ts_high';
export const TS_LOW = '__ts_low';
export const TS_SPLIT = '__ts_split';
/** What a mark says beside it (TsMarks.text): its row's column, as the database answers it. */
export const TS_TEXT = '__ts_text';
/** A condition's run each date where it holds is in: its first date and its last (none where it does not hold). */
export const TS_FROM = '__ts_from';
export const TS_TO = '__ts_to';
/** How many of the grid's rows each period's value was one of (rows at its latest time): more than one is said. */
export const TS_ROWS = '__ts_rows';
/** The periods a year a series of a tile of no frequency was annualized by, as the database inferred them from its dates
 *  (none: no frequency, said). */
export const TS_PER_YEAR = '__ts_per_year';

/** The columns the bounds query answers with: the latest and earliest dates, and each range button's start. */
export const TS_LATEST = '__ts_latest';
export const TS_EARLIEST = '__ts_earliest';
export const rangeStartName = (w: TsRangeWord): string => `__ts_from_${w}`;

/** The statistics a series' statistics query answers with, by line, in the table's order. */
export const TS_STATS = [
  'count', 'from', 'to', 'days', 'first', 'last', 'change', 'pct', 'low', 'lowAt', 'high', 'highAt', 'mean', 'std', 'vol', 'mdd',
  'cagr', 'sharpe',
  // a level's figures of its returns' risk (the expert statistics, 2026-10-26)
  'sortino', 'calmar', 'skew', 'kurtosis', 'hit', 'best', 'worst', 'var', 'es',
] as const;
/** The statistics only a level has: of its returns, its drawdown and its growth. */
const LEVEL_STATS: readonly TsStat[] = ['vol', 'mdd', 'cagr', 'sharpe', 'sortino', 'calmar', 'skew', 'kurtosis', 'hit', 'best', 'worst', 'var', 'es'];
/** A line's own figures, in the order the library's summaryStatistics writes them. */
const SUMMARY: readonly TsStat[] = ['count', 'from', 'to', 'first', 'last', 'low', 'lowAt', 'high', 'highAt', 'mean', 'std', 'days', 'change', 'pct'];
/** The share of returns a historical value at risk is the quantile of (95%: the worst 5%). */
const VAR_LEVEL = 0.05;
export type TsStat = (typeof TS_STATS)[number];
export const statName = (s: TsStat): string => `__ts_stat_${s}`;
/** The periods a year a level's statistics were annualized by, a tile of no frequency's: the database's, of its dates. */
export const TS_STAT_PER_YEAR = '__ts_stat_per_year';
/** A level's least over the dates shown, before compare: at or below zero, none of its returns' figures (the viewer says
 *  so). */
export const TS_STAT_LEVEL_LOW = '__ts_stat_level_low';

const DURATION = 'meta::pure::functions::date::DurationUnit';
const x = (): ValueSpecification => variable('x');
const col = (n: string): ValueSpecification => property(x(), n);
/** A column's value, `$x.<n>->toOne()`: arithmetic takes one value (an empty one is SQL's null there). */
const one = (n: string): ValueSpecification => fn('toOne', col(n));
const row = (body: ValueSpecification): Lambda => lambda(['x'], body);
const float = (n: number): ValueSpecification => lit.float(n);
const none = (): ValueSpecification => collection([]);
const when = (c: ValueSpecification, then: ValueSpecification, otherwise: ValueSpecification): ValueSpecification =>
  fn('if', c, lambda([], then), lambda([], otherwise));
const notEmpty = (n: string): ValueSpecification => fn('isNotEmpty', col(n));
const op = (name: string, a: ValueSpecification, b: ValueSpecification): ValueSpecification =>
  name === 'divide' ? fn('divide', a, b) : fn(name, collection([a, b]));

/** What a series' query is: the query, the unit its values are in, whether it draws a band, its split. */
export interface TsQuery {
  readonly series: string;
  readonly query: Lambda;
  readonly unit: TsUnit;
  /** Whether its figures are a level (timeseries-expr.ts levelOf): what a log scale and compare may take. */
  readonly level: boolean;
  readonly band: boolean;
  /** It draws a candle per period (ohlc): its answer's TS_OPEN, TS_HIGH, TS_LOW and its close, TS_VALUE. */
  readonly candles?: boolean;
  /** It is a condition, drawn as the periods it holds: its answer's TS_FROM and TS_TO, each date's run's (TS_TO the
   *  line's next date after the run, or its last). */
  readonly shade?: boolean;
  readonly split?: string;
  /** Whether a period's value may be taken from several rows (a series of no instrument): its rows are counted. */
  readonly counted: boolean;
  /** The sources it reads, by alias: one, run where that grid runs; more, a combination over one model of their tables. */
  readonly sources: readonly string[];
}

/** Where the viewer is: the first date shown (an index's base), and for statistics the last. */
export interface TsWindow {
  readonly from?: string;
  readonly to?: string;
}

/** A series' relation as written so far, and what its columns are. */
interface Built {
  readonly rel: Relation;
  readonly period: string;
  readonly periodType: string;
  readonly ord: string;
  /** The sources it reads, by alias, in order. */
  readonly sources: readonly string[];
  readonly value: string;
  readonly lower?: string;
  readonly upper?: string;
  readonly width?: string;
  /** A candle's open, high and low; its close the value. */
  readonly open?: string;
  readonly high?: string;
  readonly low?: string;
  readonly split?: string;
  readonly unit: TsUnit;
  /** Whether its figures are a level (levelOf): what compare rebases, and the statistics read a volatility of. */
  readonly level: boolean;
  /** A level's own value, before compare took it from its start: what its returns' figures are of (a % change from its
   *  start starts at zero -- the third review's 5: its volatility blanked as "at or below zero"). */
  readonly levelValue?: string;
  /** Each period's date: its value's, the period's last observation. */
  readonly at: string;
  readonly rows?: string;
  /** The periods a year it was annualized by, as the database inferred them (a tile of no frequency). */
  readonly perYear?: string;
  /** A condition's run each date where it holds is in: its first date and its last. */
  readonly shadeFrom?: string;
  readonly shadeTo?: string;
  /** What wrote it: a query over it (its statistics) goes on writing, so a column it adds is named after the series' own. */
  readonly writer: Writer;
}

/** The type the compiler gave a column of a grid (its source's or a row-level calculated column's). */
function typeIn(s: CubeSnapshot, column: string): string | undefined {
  return columnType(s, column);
}

/**
 * A date the viewer passes (where its zoom starts or ends: an ISO day), as a literal of the period's type: a day as it
 * is, a date-time at that day's start (an end, its last second, said by its caller).
 */
function dateLiteral(value: string, type: string): ValueSpecification {
  return literalNode(!isDay(type) && /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00` : value, type);
}

/** The compiler's function taking a date to its period's first day. */
const GRAIN_FUNCTION: Readonly<Record<Exclude<TimeGrain, 'day'>, string>> = {
  week: 'firstDayOfWeek', month: 'firstDayOfMonth', quarter: 'firstDayOfQuarter', year: 'firstDayOfYear',
};

/**
 * A GRID A SERIES READS, as its query is written over it: its rows (its snapshot -- its source, its row-level calculated
 * columns) and its filter as the tile reads it. Of a series of one grid, the grid's own snapshot (its plane decides what
 * its source reads: live, or its snapped copy); of a combination, each grid's snapshot with its source the relation of
 * its tables over the one model the combination is planned over (D5: Pure for everything, snapped or not).
 */
export interface TsInput {
  readonly snapshot: CubeSnapshot;
  /** The grid's filter as it is now: a frozen tile's source reads the one it keeps instead (timeseries-spec.ts
   *  filterRead, asked here for every query, so neither host decides it). */
  readonly filter: FilterNode | undefined;
}

/** Each source's input, by its alias. */
export type TsInputs = ReadonlyMap<string, TsInput>;

/** A column's matches as one condition on a row (a match of no column, its source's instrument column). */
function matched(s: CubeSnapshot, key: string | undefined, where: readonly TsMatch[]): ValueSpecification | undefined {
  if (where.length === 0) return undefined;
  const terms = where.map((m) => {
    const c = m.column ?? key;
    if (c === undefined) throw new CubeRefusal('a value is matched with no column: choose the column naming the instruments');
    return fn('equal', col(c), literalNode(m.value, typeIn(s, c)));
  });
  return and(...terms);
}

/** Each column the series reads, once: its source, the column, how a period takes it, and what it matches. */
interface Base {
  readonly key: string;
  readonly alias: string;
  readonly column: string;
  readonly fn: TsAggregate;
  readonly where: readonly TsMatch[];
}

const baseKey = (ts: TimeseriesSpec, n: Extract<TsExpr, { kind: 'column' }>): string =>
  JSON.stringify([sourceAlias(ts, n) ?? '', n.column, n.fn ?? 'last', n.where ?? []]);


/** The column of a part's period (every window's order, the key parts are joined on), its number in time, and its date. */
const PERIOD = '__ts_p0';
const ORD = '__ts_ord';
const AT = '__ts_at';

/**
 * AN OPTION OF THE LIBRARY'S, AS ITS ENUMERATION'S VALUE (`timeseries::Returns.log`; the user, 2026-10-28: a parameter
 * checked when the query compiles, never in its SQL): the type checker refuses a value the enumeration has not, and the
 * static fold takes the branch it names. Its values are the library's own (generated/library-enums.ts, read from its
 * declarations); the tile's reader has checked the word against the same list (the catalog's words), so one not among
 * them is a bug here, said as one.
 */
function libraryOption(enumeration: LibraryEnum, value: string): ValueSpecification {
  const values: readonly string[] = LIBRARY_ENUMS[enumeration];
  if (!values.includes(value)) throw new Error(`${value} is no ${LIBRARY_PACKAGE}::${enumeration} (${values.join(', ')})`);
  return enumValue(`${LIBRARY_PACKAGE}::${enumeration}`, value);
}

/** Each pairwise statistic's function of the library (pure-library.ts): a hedge ratio a beta of levels. */
const PAIRWISE: Readonly<Record<string, string>> = {
  correlation: 'correlation', r_squared: 'rSquared', cov: 'covariance', beta: 'beta', hedge_ratio: 'beta', alpha: 'alpha', residual: 'residual',
};

/** Each benchmark-relative statistic's function of the library (pure-library.ts). */
const BENCHMARKED: Readonly<Record<string, string>> = {
  tracking_error: 'trackingError', information_ratio: 'informationRatio', up_capture: 'upCapture', down_capture: 'downCapture',
};

/**
 * What every part of one series' query shares: the tile, the frequency its periods are of (the tile's; none inside a
 * candle at a frequency, whose series are read on their sources' own dates), the source it reads first (its split's),
 * where the window shown starts, its periods' type (the first part's: another's of another type is refused), and its
 * names -- one count, so a column a part adds is named apart from every other part's it may be joined with.
 */
interface Shared {
  readonly ts: TimeseriesSpec;
  readonly grain: TimeGrain | undefined;
  readonly series: TsSeries;
  readonly inputs: TsInputs;
  readonly split: string | undefined;
  readonly first: string;
  readonly anchor: string | undefined;
  periodType: string | undefined;
  readonly names: { n: number };
}

/** A fresh name of the series' query. */
const freshName = (sh: Shared, what: string): string => `__ts_${what}_${sh.names.n++}`;

/** A window or a lag as a function takes it: a whole number of observations, or a calendar period (`3m`, gs-quant's
 *  tenor). */
type Span = number | TsTenor;
/** `adjust(<date>, -n, <unit>)`: a calendar period before a date, the database's date arithmetic. */
const back = (date: ValueSpecification, t: TsTenor): ValueSpecification => fn('adjust', date, lit.integer(-t.n), enumValue(DURATION, t.unit));
/** `~[a, b]`: column specs of names, as one argument. */
const colSpecs = (names: readonly string[]): ValueSpecification =>
  ({ _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: names.map((name) => ({ name })) } }) as ValueSpecification;

/**
 * A PART OF A SERIES' QUERY, written in steps: a relation of one row per line and period -- a column's own rows (its
 * source's, matched), a function over a part, two parts joined -- each date a part has its own (the review's Q1: a
 * function of one operand never runs over another operand's dates), and a name for each column it adds.
 */
class Writer {
  rel: Relation;
  /** The column its windows are partitioned by: the series' split (a line per value), else one line. */
  readonly part: string;
  /** Whether it is a line per value of the split, or one line every line of the split joins (`close[SPY]`). */
  readonly split: boolean;
  /** Its periods' date: each one's last observation -- the date its value is of, where the window shown is compared. */
  readonly at: string;
  /** Whether its number in time counts seconds (a date-time source's), else days. */
  readonly seconds: boolean;
  readonly shared: Shared;
  /** The columns it carries for the drawing to read, through every join: the rows each period's value was one of, the
   *  periods a year it was annualized by (none: no frequency the dates' spacing is). */
  readonly rows: string[];
  readonly years: string[];
  #perYear: string | undefined;
  /** The tile's frequency's periods a year as a column (perYearColumn). */
  #perYearOfGrain: string | undefined;

  constructor(rel: Relation, o: {
    part: string; split: boolean; at: string; seconds: boolean; rows: readonly string[]; years: readonly string[];
  }, shared: Shared) {
    this.rel = rel;
    this.part = o.part;
    this.split = o.split;
    this.at = o.at;
    this.seconds = o.seconds;
    this.rows = [...o.rows];
    this.years = [...o.years];
    this.shared = shared;
  }

  get ts(): TimeseriesSpec {
    return this.shared.ts;
  }

  fresh(what: string): string {
    return freshName(this.shared, what);
  }

  /**
   * A FUNCTION OF DATACUBE'S PURE LIBRARY over a window of its line (pure-library.ts: the user, 2026-10-26, gs-quant's
   * functions as Pure; the conventions of docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.7):
   * `->timeseries::<name>(over(~<line>, [~period->ascending()], <frame>), ~<value>, <args>, ~<out>)` -- the window the
   * writer's own would be (#over) -- its figure the column it writes.
   */
  library(name: string, what: string, value: string, frame: 'running' | 'all' | 'rest' | number | null, args: readonly ValueSpecification[]): string {
    return this.#libraryCall(name, what, [this.#over(frame), colSpec(value), ...args]);
  }

  /** A function of the library over its line and its dates -- `->timeseries::<name>(~<line>, ~<date>, ~<value>,
   *  <args>, ~<out>)`: a base over the whole line, an as-of join -- the date each period's value is of. */
  libraryOfLine(name: string, what: string, value: string, args: readonly ValueSpecification[]): string {
    return this.#libraryCall(name, what, [colSpec(this.part), colSpec(this.at), colSpec(value), ...args]);
  }

  /** A function of the library over a calendar period back of each date of its line: `libraryOfLine`'s, the period
   *  its n and its unit. */
  libraryOfPeriod(name: string, what: string, value: string, t: TsTenor, args: readonly ValueSpecification[] = []): string {
    return this.libraryOfLine(name, what, value, [...Writer.period(t), ...args]);
  }

  /** Its line and its dates, as the library's functions over a line take them: `~<line>, ~<date>`. */
  lineAndDate(): ValueSpecification[] {
    return [colSpec(this.part), colSpec(this.at)];
  }

  /** A calendar period as the library's period functions take it: its n and its unit. */
  static period(t: TsTenor): ValueSpecification[] {
    return [lit.integer(t.n), enumValue(DURATION, t.unit)];
  }

  /** A function of the library of each row alone: `->timeseries::<name>(<args>, ~<out>)`. */
  libraryOfRow(name: string, what: string, args: readonly ValueSpecification[]): string {
    return this.#libraryCall(name, what, args);
  }

  #libraryCall(name: string, what: string, args: readonly ValueSpecification[]): string {
    return this.libraryOfMany(name, [what], args)[0]!;
  }

  /** A function of the library writing several columns (a band's lower, upper and width), `args` before them: their
   *  names, in its order. */
  libraryOfMany(name: string, whats: readonly string[], args: readonly ValueSpecification[]): string[] {
    const outs = whats.map((what) => this.fresh(what));
    this.rel = this.rel.call(`${LIBRARY_PACKAGE}::${name}`, ...args, ...outs.map((o) => colSpec(o)));
    return outs;
  }

  /**
   * THE MEMBERS AS ROWS (the library's long table, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.7: a row a member and
   * date): of each of `columns` (a member's values, on this writer's rows), its line, its date, its time (ORD), its
   * number, its value and its weight -- the members' rows concatenated.
   */
  members(columns: readonly string[], weights: readonly number[]): { readonly rel: Relation; readonly member: string; readonly value: string; readonly weight: string } {
    const member = this.fresh('mb_m');
    const value = this.fresh('mb_v');
    const weight = this.fresh('mb_w');
    const rel = columns.map((c, i) => this.rel.select([this.part, this.at, ORD, c]).rename(c, value)
      .extend([derive(member, row(lit.integer(i))), derive(weight, row(float(weights[i]!)))]))
      .reduce((all, one_) => all.concatenate(one_));
    return { rel, member, value, weight };
  }

  /** A relation of a row a line and date (a figure of the members', the library's), joined to this writer's by them. */
  joinByDate(other: Relation): void {
    const gp = this.fresh('jd_part');
    const ga = this.fresh('jd_at');
    this.rel = this.rel.join(other.rename(this.part, gp).rename(this.at, ga), 'LEFT', lambda(['l', 'r'], and(
      fn('equal', property(variable('l'), this.part), property(variable('r'), gp)),
      fn('equal', property(variable('l'), this.at), property(variable('r'), ga)))));
  }

  /** The window the library's functions take: the line, its periods in order, a frame (#over's). */
  windowOf(frame: 'running' | 'all' | number | null): AppliedFunction {
    return this.#over(frame);
  }

  /**
   * A STATISTIC OF A CALENDAR PERIOD, the library's (`timeseries::overPeriod`): `stat` of `value` over (date - n unit,
   * date] of each date of its line, none before the line's first value's date that period on (gs-quant's
   * rolling_offset and apply_ramp).
   */
  libraryPeriod(stat: string, what: string, value: string, t: TsTenor): string {
    return this.libraryOfPeriod('overPeriod', what, value, t, [libraryOption('Statistic', stat)]);
  }

  /** A column `n` periods back, or a calendar period back as of that date (gs-quant's lag): the library's. */
  libraryLag(value: string, n: Span): string {
    return typeof n === 'number' ? this.library('lag', 'lag', value, null, [lit.integer(n)]) : this.libraryOfPeriod('lagPeriod', 'lag', value, n);
  }

  /**
   * A BASE OVER THE WHOLE LINE, the library's -- `index`, `change` or `percentChange` of the line's first value, or,
   * with the first date the view shows (the tile's anchor), its `...From` of the value as of it (`valueAt`) -- `args`
   * after the date.
   */
  libraryBased(name: 'index' | 'change' | 'percentChange', what: string, value: string, args: readonly ValueSpecification[] = []): string {
    const anchor = this.shared.anchor;
    if (anchor === undefined) return this.libraryOfLine(name, what, value, args);
    return this.libraryOfLine(`${name}From`, what, value, [dateLiteral(anchor, this.shared.periodType!), ...args]);
  }

  /** A rolled statistic of `value` over `w` periods (0: from the start) or a calendar period, the library's: drawn once
   *  its window holds `ramp` values (a calendar period's, from its line's first value's date that period on). */
  libraryRolled(stat: string, value: string, w: Span, ramp: number): string {
    return typeof w === 'number' ? this.library(stat, stat, value, Writer.frame(w) as 'running' | number, [lit.integer(ramp)])
      : this.libraryPeriod(stat, stat, value, w);
  }

  /** `->extend(~[name: x|body])`. */
  row(what: string, body: ValueSpecification): string {
    const name = this.fresh(what);
    this.rel = this.rel.extend([derive(name, row(body))]);
    return name;
  }

  /** The window every function of the part runs over: its line's partition (and `by`'s, a block of it), the periods
   *  in order, a frame. */
  #over(frame: 'running' | 'all' | 'rest' | number | null, by?: string): AppliedFunction {
    const args: ValueSpecification[] = [
      { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: [{ name: this.part }, ...(by !== undefined ? [{ name: by }] : [])] } },
      collection([asc(PERIOD)]),
    ];
    if (frame === 'running') args.push(fn('rows', fn('unbounded'), lit.integer(0)));
    else if (frame === 'all') args.push(fn('rows', fn('unbounded'), fn('unbounded')));
    else if (frame === 'rest') args.push(fn('rows', lit.integer(0), fn('unbounded')));
    else if (frame !== null) args.push(fn('rows', lit.integer(-(Math.max(1, frame) - 1) || 0), lit.integer(0)));
    return fn('over', ...args);
  }

  /** An aggregate over a frame of the periods: `extend(over(...), ~[name:{p,w,r|<read>}:y|$y-><reduce>()])`. A calendar
   *  period's window is the library's (`windowPeriod`, `overPeriod`): a frame counts rows, never dates. */
  window(what: string, reduce: string, read: ValueSpecification, frame: 'running' | 'all' | 'rest' | number, by?: string): string {
    const name = this.fresh(what);
    this.rel = this.rel.extend([agg(name, lambda(['p', 'w', 'r'], read), lambda(['y'], fn(reduce, variable('y'))))], this.#over(frame, by));
    return name;
  }

  /** A column `n` periods back: `$p->lag($r, n).<column>`; `by`: among the line's rows of the same value of that column.
   *  A calendar period back is the library's (`lagPeriod`: libraryLag). */
  lag(column: string, n: number, by?: string): string {
    const name = this.fresh('lag');
    const call = fn('lag', variable('p'), variable('r'), ...(n !== 1 ? [lit.integer(n)] : []));
    this.rel = this.rel.extend([derive(name, lambda(['p', 'w', 'r'], property(call, column)))], this.#over(null, by));
    return name;
  }

  /** A column the next period on: `$p->lead($r).<column>` (none at the line's last). */
  lead(column: string, n = 1): string {
    const name = this.fresh('lead');
    const call = fn('lead', variable('p'), variable('r'), ...(n !== 1 ? [lit.integer(n)] : []));
    this.rel = this.rel.extend([derive(name, lambda(['p', 'w', 'r'], property(call, column)))], this.#over(null));
    return name;
  }

  /** Each row's place in its line, 1 its first: `$p->rowNumber($r)`. */
  rowNumber(): string {
    const name = this.fresh('rn');
    this.rel = this.rel.extend([derive(name, lambda(['p', 'w', 'r'], fn('rowNumber', variable('p'), variable('r'))))], this.#over(null));
    return name;
  }

  /**
   * AN EXPONENTIALLY WEIGHTED AVERAGE, pandas' `ewm(alpha = 1 - decay, adjust=False).mean()` (gs-quant's
   * exponential_moving_average): the library's (pure-library.ts ewma, its closed form the database's). `seed`: from the
   * value of `seed.value` at the `seed.at`-th value (a Wilder average's first, a mean of w), the series' own from there
   * (ewmaFrom); rows of none have none.
   */
  ewm(x: string, decay: ValueSpecification, seed?: { readonly at: number; readonly value: string }): string {
    return seed === undefined ? this.libraryOfLine('ewma', 'ewm', x, [decay])
      : this.libraryOfLine('ewmaFrom', 'ewm', x, [decay, lit.integer(seed.at), colSpec(seed.value)]);
  }

  /** AN EXPONENTIALLY WEIGHTED STANDARD DEVIATION, pandas' `ewm(alpha = 1 - decay, adjust=False).std()` (gs-quant's
   *  exponential_std): the library's (pure-library.ts ewmStd). */
  ewmStd(x: string, decay: ValueSpecification): string {
    return this.libraryOfLine('ewmStd', 'ewm_std', x, [decay]);
  }

  /** WILDER'S SMOOTHED AVERAGE over w (gs-quant's technicals.smoothed_moving_average): the library's (pure-library.ts
   *  smoothedMean). */
  smoothed(x: string, w_: number): string {
    // a w of 1 or under each value from the line's second, as gs-quant's ramp of 1 has it (the E9 review's nit)
    return this.libraryOfLine('smoothedMean', 'smma', x, [lit.integer(Math.max(w_, 1))]);
  }

  /** A column's value on a frame's rows, as a window reads it. */
  static read(column: string): ValueSpecification {
    return property(variable('r'), column);
  }

  /** `w` periods of `column` (0: from the start), or a calendar period: its frame. */
  static frame(w: Span): 'running' | number | TsTenor {
    return w === 0 ? 'running' : w;
  }

  /** `a / b`, empty where b is zero. */
  ratio(what: string, a: ValueSpecification, b: string): string {
    return this.row(what, when(fn('equal', one(b), lit.integer(0)), none(), fn('divide', a, one(b))));
  }

  /** Returns over `obs` periods, or a calendar period back as of its date, of a type (gs-quant's econometrics.returns):
   *  the library's (pure-library.ts returns, returnsPeriod). */
  returns(a: string, obs: Span, type: string): string {
    const types: readonly string[] = LIBRARY_ENUMS.Returns;
    if (!types.includes(type)) throw new CubeRefusal(`there is no ${JSON.stringify(type)} return (${types.join(', ')})`);
    return typeof obs === 'number' ? this.library('returns', 'ret', a, null, [lit.integer(obs), libraryOption('Returns', type)])
      : this.libraryOfPeriod('returnsPeriod', 'ret', a, obs, [libraryOption('Returns', type)]);
  }

  /**
   * THE PERIODS A YEAR it is annualized by: the tile's frequency's (252 days, 52 weeks, 12 months...); with none, the one
   * the database infers from the part's own dates -- the average days between its periods, as gs-quant's
   * econometrics._get_annualization_factor buckets it (under 2.1 days: 252; 6 to 8: 52; 14 to 17: 26; 25 to 35: 12; 85
   * to 97: 4; 360 to 386: 1; anything else, or dates within a day: none, said by the drawing) -- the owner, 2026-10-25,
   * the review's Q3: monthly levels annualized by 252.
   */
  perYear(): ValueSpecification {
    const grain = this.shared.grain;
    if (grain !== undefined) return lit.integer(PERIODS_PER_YEAR[grain]);
    if (this.#perYear === undefined) {
      // the library's (periodsPerYear: its buckets gs-quant's), over the line's time -- its days, or its seconds
      this.#perYear = this.libraryOfRow('periodsPerYear', 'per_year', [colSpec(this.part), colSpec(ORD), enumValue(DURATION, this.seconds ? 'SECONDS' : 'DAYS')]);
      this.years.push(this.#perYear);
    }
    return one(this.#perYear);
  }

  /** The periods a year as a column (perYear's: the tile's frequency's, else the one its dates infer): what the
   *  library's functions read. */
  perYearColumn(): string {
    const grain = this.shared.grain;
    if (grain === undefined) {
      this.perYear();
      return this.#perYear!;
    }
    this.#perYearOfGrain ??= this.row('per_year', lit.integer(PERIODS_PER_YEAR[grain]));
    return this.#perYearOfGrain;
  }

  /** Simple returns, for the functions gs-quant reads them for (a correlation's, a beta's, a Sharpe ratio's). */
  simple(a: string): string {
    return this.returns(a, 1, 'simple');
  }
}

/** The value a part computes: its column's name, two columns (a band), or four (a candle). */
type Value = { readonly column: string }
  | { readonly lower: string; readonly upper: string; readonly width: string }
  | { readonly open: string; readonly high: string; readonly low: string; readonly close: string };

/** A node of the expression, written: a part and the value it computes over it -- or a constant, numbers alone, the
 *  database's arithmetic wherever it is read. */
type Node = { readonly w: Writer; readonly v: Value } | { readonly constant: ValueSpecification };

/** What only a drawing reads, refused where an expression reads it. */
function unread(v: Value): never {
  throw new CubeRefusal('lower' in v ? 'a band is drawn, never read' : 'candles are drawn, never read');
}

const columnOfValue = (v: Value): string => ('column' in v ? v.column : unread(v));

/** A node as an operand of the row's arithmetic. */
function asOperand(n: Node): ValueSpecification {
  return 'constant' in n ? n.constant : one(columnOfValue(n.v));
}

/** The columns a value is, every one carried through a join. */
const valueColumns = (v: Value): string[] =>
  'column' in v ? [v.column] : 'lower' in v ? [v.lower, v.upper, v.width] : [v.open, v.high, v.low, v.close];

/** How a candle's four series take each period's rows: its first open, its highest high, its lowest low, its last close. */
const CANDLE: readonly TsAggregate[] = ['first', 'max', 'min', 'last'];

/** How a series of each role a function reads (its signature's `reads`) is taken of a period's rows: a high its most, a
 *  low its least, a close or a price its last, a volume or a dividend their total -- a candle's way (CANDLE). */
const ROLE_TAKEN: Readonly<Record<string, TsAggregate>> = { high: 'max', low: 'min', close: 'last', price: 'last', volume: 'sum', dividends: 'sum' };

/** How each series a function reads is taken of a period's rows, by its role; undefined where its series have none (a
 *  series and its benchmark: each its last, as any column). */
function roleTaken(name: TsFunctionName): readonly TsAggregate[] | undefined {
  const reads = TS_SIGNATURES[name].reads;
  if (reads === undefined || !reads.some((r) => r in ROLE_TAKEN)) return undefined;
  return reads.map((r) => {
    const t = ROLE_TAKEN[r];
    if (t === undefined) throw new Error(`${name} reads a ${r}, which no period's rows are taken by`);
    return t;
  });
}

/** An expression with each candle's (ohlc's) plain columns taken by their date as a candle's are (CANDLE) -- unless one
 *  says how itself (`open[AAPL].first`) -- so a day of several rows is its first open, highest high, lowest low and
 *  last close; a period of the tile's frequency then takes its dates' the same way (`candle`). */
function candled(e: TsExpr): TsExpr {
  switch (e.kind) {
    case 'call': {
      const args = e.args.map(candled);
      const taken = e.fn === 'ohlc' ? CANDLE : roleTaken(e.fn);
      if (taken === undefined) return { ...e, args };
      return { ...e, args: args.map((a, i) => (a.kind === 'column' && a.fn === undefined && taken[i] !== undefined ? { ...a, fn: taken[i] } : a)) };
    }
    case 'binary':
      return { ...e, left: candled(e.left), right: candled(e.right) };
    default:
      return e;
  }
}

/** Whether a column's matches pick one value of the split's column (`close[SPY]`, the split by symbol): one line, joined
 *  by every line of the split, not a line of its own (the review's Q5: matched inside each line, every other line went). */
function matchesSplit(b: Base, src: TsSource, split: string): boolean {
  // or its instrument: `close / close[SPY]` split by sector -- SPY's one line divides every sector's (the second audit's
  // S2: split by SPY's own sector, every other sector's line went)
  return b.where.some((m) => { const c = m.column ?? src.key; return c === split || (src.key !== undefined && c === src.key); });
}

/** A part's periods' type, the series' (the first part's): another's of another type is refused -- under a frequency
 *  the tile has, a candle's series read on their own dates (`candle`). */
function periodTyped(sh: Shared, type: string, alias: string): void {
  if (sh.periodType === undefined) sh.periodType = type;
  else if (type !== sh.periodType) {
    throw new CubeRefusal(sh.grain === undefined && sh.ts.grain !== undefined
      ? `series ${sh.series.id}'s candle reads ${sh.first}'s dates and ${alias}'s, of another type, on their own dates: read its series from grids of one date type`
      : `series ${sh.series.id} combines ${sh.first}'s dates with ${alias}'s, of another type: give the tile a frequency (Daily) to read both by day`);
  }
}

/**
 * A COLUMN'S OWN PART (steps 1 and 2): its source's rows -- the grid's filter as the tile reads it, the column's matches,
 * a date and a value on every row -- each row's period at the tile's frequency, grouped by it (and by the split, a column of the
 * source the series reads first, unless it matches one value of it), the column taken by its period's aggregate, the
 * periods it has a value in alone (the review's Q1). A `last` or `first` counts the rows tied at the latest (earliest)
 * time, so the drawing says when one was taken of several (the review's Q7).
 */
function columnPart(sh: Shared, n: Extract<TsExpr, { kind: 'column' }>): Node {
  const alias = sourceAlias(sh.ts, n);
  if (alias === undefined) throw new CubeRefusal(`${n.column} names no source of the tile`);
  const b: Base = { key: baseKey(sh.ts, n), alias, column: n.column, fn: n.fn ?? 'last', where: n.where ?? [] };
  const { src, input } = sourceNamed(sh.ts, sh.inputs, alias);
  const s = input.snapshot;
  const typeOf = (c: string): string | undefined => typeIn(s, c);
  const dateType = typeOf(src.date);
  // its rows with a date and a value: a period dated at its last row with one, the date its value is of (the review's
  // N7: a month whose last row had none was dated at it, its value an earlier day's)
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(src.date)), fn('isNotEmpty', col(b.column))];
  const filter = filterRead(src, input.filter);
  if (filter) conditions.push(filterNode(filter, 'x', typeOf));
  const match = matched(s, src.key, b.where);
  if (match) conditions.push(match);
  let rel = sourceWithDerived(s).filter(row(and(...conditions)));
  const { body, type, at } = periodOf(sh.grain, src.date, dateType);
  periodTyped(sh, type, alias);
  const seconds = !isDay(dateType);
  const ordRow = `__ts_or`;
  const atRow = `__ts_ar`;
  rel = rel.extend([
    derive(PERIOD, row(body)),
    derive(ordRow, row(fn('dateDiff', literalNode('1970-01-01', 'StrictDate'), one(src.date), enumValue(DURATION, seconds ? 'SECONDS' : 'DAYS')))),
    derive(atRow, row(at)),
  ]);
  const split = sh.split !== undefined && alias === sh.first && !matchesSplit(b, src, sh.split);
  const keys = split ? [sh.split!, PERIOD] : [PERIOD];
  const names = { fresh: (what: string): string => freshName(sh, what) };
  const value = names.fresh('base');
  const aggs: ColSpec[] = [
    baseAggregate(value, b, ordRow),
    agg(ORD, row(col(ordRow)), lambda(['y'], fn('max', variable('y')))),
    agg(AT, row(col(atRow)), lambda(['y'], fn('max', variable('y')))),
  ];
  const rows: string[] = [];
  if (b.fn === 'last' || b.fn === 'first') {
    // the rows at the time a `last` takes (a `first`'s earliest): more than one, and which was taken is the database's
    const tie = names.fresh('tie');
    rel = rel.extend([agg(tie, lambda(['p', 'w', 'r'], Writer.read(ordRow)), lambda(['y'], fn(b.fn === 'last' ? 'max' : 'min', variable('y'))))],
      fn('over', { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: keys.map((k) => ({ name: k })) } },
        collection([asc(ordRow)]), fn('rows', fn('unbounded'), fn('unbounded'))));
    const count = names.fresh('rows');
    aggs.push(agg(count, row(when(fn('equal', col(ordRow), col(tie)), lit.integer(1), none())), lambda(['y'], fn('count', variable('y')))));
    rows.push(count);
  }
  rel = rel.groupBy(keys, aggs);
  const part = split ? sh.split! : `${WINDOW_ALL_PREFIX}ts`;
  if (!split) rel = rel.extend([derive(part, row(lit.string('all')))]);
  rel = rel.filter(row(notEmpty(value)));
  return { w: new Writer(rel, { part, split, at: AT, seconds, rows, years: [] }, sh), v: { column: value } };
}

/**
 * PARTS JOINED on the period -- and on their line, both a line per value of the split -- each part's value computed
 * over its own dates first, at the operator or the function that reads them together (the review's Q1). `how` (the
 * tile's `align`, gs-quant's Interpolate): `intersect`, the dates every one has (gs-quant's intersect); else every date
 * any has, each part put onto them (#aligned). A line of the split joined with one line (`close[SPY]`) is that line's
 * per value; each period dated at its latest part's last observation.
 */
function joined(sh: Shared, nodes: readonly { readonly w: Writer; readonly v: Value }[], how: TsAlign = 'intersect'): Writer {
  // the parts of a line per value first: theirs is the line the join is partitioned by
  // every date any has: merged in turn, each read once (#aligned)
  if (how !== 'intersect') return aligned(sh, nodes.map((n) => ({ w: n.w, values: valueColumns(n.v) })), how).w;
  const order = [...nodes].sort((a, b) => Number(b.w.split) - Number(a.w.split));
  let left = order[0]!.w;
  let carried = valueColumns(order[0]!.v);
  for (const right of order.slice(1)) {
    const r = right.w;
    const fresh = (what: string): string => freshName(sh, what);
    const p = fresh('jp');
    const a = fresh('ja');
    const line = left.split && r.split ? fresh('jl') : undefined;
    let rel = r.rel.rename(PERIOD, p).rename(r.at, a);
    if (line !== undefined) rel = rel.rename(r.part, line);
    rel = rel.select([p, a, ...(line !== undefined ? [line] : []), ...valueColumns(right.v), ...r.rows, ...r.years]);
    const on = [fn('equal', property(variable('l'), PERIOD), property(variable('r'), p)),
      ...(line !== undefined ? [fn('equal', property(variable('l'), left.part), property(variable('r'), line))] : [])];
    let both = left.rel.join(rel, 'INNER', lambda(['l', 'r'], and(...on)));
    const at = fresh('at');
    both = both.extend([derive(at, row(when(fn('greaterThanEqual', one(left.at), one(a)), col(left.at), col(a))))]);
    left = new Writer(both, {
      part: left.part, split: left.split, at, seconds: left.seconds,
      rows: [...left.rows, ...r.rows], years: [...left.years, ...r.years],
    }, sh);
    carried = [...carried, ...valueColumns(right.v)];
  }
  return left;
}

/** A part as an alignment puts it onto dates: its writer, the value columns it fills, and those it carries as they are
 *  (a part's has-a-row marks, through the merges after). */
interface Aligned {
  readonly w: Writer;
  readonly values: readonly string[];
  readonly kept?: readonly string[];
}

/** Two parts merged (merged): their writer -- every date either has, by line -- and, for each as it was given, the
 *  column that is 1 where it had a row and its own ORD there. */
interface Merged {
  readonly w: Writer;
  readonly has: readonly [string, string];
  readonly ords: readonly [string, string];
}

/**
 * THE LINES OF THE SPLIT, read off the first source -- its rows as the tile reads them (a date on each, the grid's
 * filter) -- the line names a part of one line is put under (`close[SPY]` beside `close`): a leaf of the query, however
 * deep the parts merged, so a merge of it reads each part once (the Phase 1 review's 1).
 */
function splitLines(sh: Shared): Relation {
  const split = sh.split;
  if (split === undefined) throw new Error('a part of one line is put under the lines of a split: the series has none');
  const { src, input } = sourceNamed(sh.ts, sh.inputs, sh.first);
  const s = input.snapshot;
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(src.date))];
  const filter = filterRead(src, input.filter);
  if (filter) conditions.push(filterNode(filter, 'x', (c) => typeIn(s, c)));
  return sourceWithDerived(s).filter(row(and(...conditions))).select([split]).distinct([split]);
}

/**
 * TWO PARTS MERGED, every date either has (the next phases' 1; rewritten for the Phase 1 review's 1, 2026-10-26): a
 * FULL join on their line and their period -- each part read ONCE, so a nesting of them grows linearly (an as-of join
 * of each and a spine of both read each three times: a basket of six instruments under step was 2 MB of SQL) -- each
 * period dated at its latest part's last observation. A part of one line beside a split's goes under every line of it
 * (splitLines), the lines the split has not dropped. Each part's values, carried columns and kept columns keep their
 * names; none where it has no row.
 */
function merged(sh: Shared, a: Aligned, b: Aligned): Merged {
  // the split's part first: its lines are the merge's
  const swap = b.w.split && !a.w.split;
  const [L, R] = swap ? [b, a] : [a, b];
  const fresh = (what: string): string => freshName(sh, what);
  const part = L.w.part;
  const side = (s: Aligned, under: boolean): { rel: Relation; p: string; per: string; at: string; ord: string; has: string } => {
    const w = s.w;
    const own = [...s.values, ...(s.kept ?? []), ...w.rows, ...w.years];
    let rel = w.rel;
    let p = w.part;
    if (under) {
      // a part of one line under every line of the split, by a key both carry
      const k = fresh('al_k');
      const k2 = fresh('al_k');
      rel = splitLines(sh).extend([derive(k, row(lit.integer(1)))])
        .join(rel.select([PERIOD, w.at, ORD, ...own]).extend([derive(k2, row(lit.integer(1)))]), 'INNER',
          lambda(['l', 'r'], fn('equal', property(variable('l'), k), property(variable('r'), k2))));
      p = part;
    }
    const names = { p: fresh('al_p'), per: fresh('al_per'), at: fresh('al_at'), ord: fresh('al_ord'), has: fresh('al_has') };
    rel = rel.select([p, PERIOD, w.at, ORD, ...own]).rename(p, names.p).rename(PERIOD, names.per).rename(w.at, names.at).rename(ORD, names.ord)
      .extend([derive(names.has, row(lit.integer(1)))]);
    return { rel, ...names };
  };
  const l = side(L, false);
  const r = side(R, L.w.split !== R.w.split);
  const on = lambda(['l', 'r'], and(fn('equal', property(variable('l'), l.p), property(variable('r'), r.p)),
    fn('equal', property(variable('l'), l.per), property(variable('r'), r.per))));
  const either = (x: string, y: string): ValueSpecification => when(fn('isEmpty', col(x)), col(y), col(x));
  const latest = (x: string, y: string): ValueSpecification =>
    when(fn('isEmpty', col(x)), col(y), when(fn('isEmpty', col(y)), col(x), when(fn('greaterThanEqual', one(x), one(y)), col(x), col(y))));
  const at = fresh('al_at');
  const keep = (s: Aligned): string[] => [...s.values, ...(s.kept ?? []), ...s.w.rows, ...s.w.years];
  const rel = l.rel.join(r.rel, 'FULL', on)
    .extend([derive(part, row(either(l.p, r.p))), derive(PERIOD, row(either(l.per, r.per))), derive(at, row(latest(l.at, r.at))),
      derive(ORD, row(latest(l.ord, r.ord)))])
    .select([part, PERIOD, at, ORD, l.ord, r.ord, l.has, r.has, ...keep(L), ...keep(R)]);
  const w = new Writer(rel, {
    part, split: L.w.split, at, seconds: L.w.seconds, rows: [...L.w.rows, ...R.w.rows], years: [...L.w.years, ...R.w.years],
  }, sh);
  if (L.w.split !== R.w.split) {
    // the lines the split has (a line of the source it never reads dropped): the database's QUALIFY, a window's
    const line = w.window('al_line', 'max', Writer.read(l.has), 'all');
    w.rel = w.rel.filter(row(fn('isNotEmpty', col(line))));
  }
  return swap ? { w, has: [r.has, l.has], ords: [r.ord, l.ord] } : { w, has: [l.has, r.has], ords: [l.ord, r.ord] };
}

/**
 * A PART'S VALUES PUT ONTO THE MERGED DATES, each keeping its name (#merged's columns: `ord`, its own ORD where it has a
 * row): `nan`, none where it has none (as merged); `zero`, 0 there; `step`, each date its last value at or before it --
 * before its first, its first (gs-quant's ffill then bfill); `time`, linear in time between its values before and after
 * (none outside them: pandas' limit_area='inside'). Its last and next valued periods its own windows (a running maximum
 * of the periods it has a value in, and the value there) -- over the one relation, nothing read again.
 */
function fill(w: Writer, values: readonly string[], ord: string, how: Exclude<TsAlign, 'intersect'>): void {
  if (how === 'nan' || values.length === 0) return;
  // each column replaced under its own name by the library's (its alignment functions, the platform step, 2026-10-28):
  // the merged one kept under another
  const replaced = (made: readonly (readonly [string, string])[]): void => {
    for (const [v, filled] of made) w.rel = w.rel.rename(v, w.fresh('al_raw')).rename(filled, v);
  };
  if (how === 'zero') {
    replaced(values.map((v) => [v, w.libraryOfRow('fillZero', 'al_zero', [colSpec(v)])] as const));
    return;
  }
  // its last and next valued periods, each once for all its columns (each window a level more the database plans
  // through), its first column saying where it has a row: a part's columns are one row's
  const line = colSpec(w.part);
  const order = colSpec(PERIOD);
  const marker = colSpec(values[0]!);
  const lastP = w.libraryOfRow('lastValued', 'al_lastp', [line, order, marker]);
  const at = (p: string, c: string): string => w.libraryOfRow('valueAt', 'al_v', [line, order, colSpec(p), colSpec(c)]);
  // its carried columns (the rows a value was one of, its periods a year) where it has a row alone: a date filled took
  // none of its rows -- each merge filling every one before it grew the query faster than its parts
  if (how === 'step') {
    const firstP = w.libraryOfRow('firstValued', 'al_firstp', [line, order, marker]);
    const made = values.map((v) => [v, at(lastP, v), at(firstP, v)] as const);
    replaced(made.map(([v, last, first]) => [v, w.libraryOfRow('orElse', 'al_step', [colSpec(last), colSpec(first)])] as const));
  } else {
    const nextP = w.libraryOfRow('nextValued', 'al_nextp', [line, order, marker]);
    const t0 = at(lastP, ord);
    const t1 = at(nextP, ord);
    const made = values.map((v) => [v, at(lastP, v), at(nextP, v)] as const);
    // inside its values alone, at one of its own dates its own value: the library's (linearAt)
    replaced(made.map(([v, a, b]) => [v, w.libraryOfRow('linearAt', 'al_lin', [colSpec(t0), colSpec(a), colSpec(t1), colSpec(b), colSpec(ORD)])] as const));
  }
}

/**
 * EVERY DATE ANY PART HAS (gs-quant's `align` but intersect): the parts merged in turn (#merged: each read once), each
 * part's values put onto the dates by `how` (#fill) -- and, for each part, the column that is 1 where it had a row (an
 * if's flags, an append's x: what keeps a date).
 */
function aligned(sh: Shared, parts: readonly Aligned[], how: Exclude<TsAlign, 'intersect'>):
  { readonly w: Writer; readonly has: readonly string[]; readonly ords: readonly string[] } {
  if (parts.length < 2) throw new Error('an alignment is of two parts or more');
  let acc: Aligned = parts[0]!;
  const has: string[] = [];
  const ords: string[] = [];
  for (const next of parts.slice(1)) {
    const m = merged(sh, acc, next);
    fill(m.w, acc.values, m.ords[0], how);
    fill(m.w, next.values, m.ords[1], how);
    if (has.length === 0) {
      has.push(m.has[0]);
      ords.push(m.ords[0]);
    }
    has.push(m.has[1]);
    ords.push(m.ords[1]);
    // the parts merged before: their has-a-row marks and their own ORDs carried, as they are
    acc = { w: m.w, values: [...acc.values, ...next.values], kept: [...has, ...ords] };
  }
  return { w: acc.w, has, ords };
}

const OPS: Readonly<Record<'+' | '-' | '*' | '/', string>> = { '+': 'plus', '-': 'minus', '*': 'times', '/': 'divide' };

/** A condition's test as the database's: a comparison of the two sides, or two conditions joined (each true unless 0). */
function conditionTest(o: TsOperator, l: ValueSpecification, r: ValueSpecification): ValueSpecification {
  const holds = (x: ValueSpecification): ValueSpecification => fn('not', fn('equal', x, lit.integer(0)));
  switch (o) {
    case '<': return fn('lessThan', l, r);
    case '<=': return fn('lessThanEqual', l, r);
    case '>': return fn('greaterThan', l, r);
    case '>=': return fn('greaterThanEqual', l, r);
    case '==': return fn('equal', l, r);
    case '!=': return fn('not', fn('equal', l, r));
    case 'and': return and(holds(l), holds(r));
    case 'or': return or(holds(l), holds(r));
    case '+': case '-': case '*': case '/':
      throw new Error(`${o} is no condition`);
  }
}

/** A node of the expression, written: its own columns after those of what it reads. */
function write(sh: Shared, e: TsExpr): Node {
  switch (e.kind) {
    case 'number':
      return { constant: Number.isInteger(e.value) ? lit.integer(e.value) : float(e.value) };
    case 'ref':
      throw new Error('a series is read inlined (timeseries-spec.ts inlined)');
    case 'column':
      return columnPart(sh, e);
    case 'binary': {
      const l = write(sh, e.left);
      const r = write(sh, e.right);
      // a condition: 1 where it holds, 0 where not, none where a side has none (the expert functions, 2026-10-26)
      if (isConditionOp(e.op)) {
        if ('constant' in l && 'constant' in r) return { constant: when(conditionTest(e.op, l.constant, r.constant), lit.integer(1), lit.integer(0)) };
        const wc = 'constant' in l ? (r as Exclude<Node, { constant: unknown }>).w
          : 'constant' in r ? l.w : joined(sh, [l, r as Exclude<Node, { constant: unknown }>], sh.ts.align);
        const missing = [l, r].flatMap((x) => ('constant' in x ? [] : [fn('isEmpty', col(columnOfValue(x.v)))]));
        return { w: wc, v: { column: wc.row('cond', when(missing.length === 1 ? missing[0]! : or(...missing), none(),
          when(conditionTest(e.op, asOperand(l), asOperand(r)), lit.integer(1), lit.integer(0)))) } };
      }
      if ('constant' in l && 'constant' in r) return { constant: op(OPS[e.op as '+' | '-' | '*' | '/'], l.constant, r.constant) };
      const w = 'constant' in l ? (r as Exclude<Node, { constant: unknown }>).w
        : 'constant' in r ? l.w : joined(sh, [l, r as Exclude<Node, { constant: unknown }>], sh.ts.align);
      if (e.op === '/') {
        const denominator = 'constant' in r ? w.row('num', r.constant) : columnOfValue(r.v);
        return { w, v: { column: w.ratio('div', asOperand(l), denominator) } };
      }
      return { w, v: { column: w.row('op', op(OPS[e.op as '+' | '-' | '*' | '/'], asOperand(l), asOperand(r))) } };
    }
    case 'call':
      return call(sh, e);
  }
}

/** A function's series argument, written: a part (a function reads a series, never a number alone). */
function seriesArg(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>, i: number): { readonly w: Writer; readonly v: Value } {
  const n = write(sh, e.args[i]!);
  if ('constant' in n) throw new CubeRefusal(`${e.fn} reads a series: its ${ordinal(i + 1)} argument is a number`);
  return n;
}

/** A place in words: first, second ... tenth, then 11th, 21st. */
function ordinal(n: number): string {
  const words = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth'];
  if (n >= 1 && n <= words.length) return words[n - 1]!;
  const tens = n % 100;
  const suffix = tens >= 11 && tens <= 13 ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' } as Record<number, string>)[n % 10] ?? 'th';
  return `${n}${suffix}`;
}

/**
 * A CANDLE (ohlc): its four series joined on their dates. At a frequency, each is computed on its sources' own dates
 * first -- its functions over them, its arithmetic on each one -- and each period then takes the first open, the
 * highest high, the lowest low and the last close of them, the database's (the review's N4: each column was taken by
 * the period first, so a candle of `close / 2` weekly was the week's last, four times over, flat).
 */
function candle(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node {
  const r = rolled(sh, e, CANDLE);
  const [open, high, low, close] = r.columns as [string, string, string, string];
  return { w: r.w, v: { open, high, low, close } };
}

/**
 * A FUNCTION'S SERIES AT THE TILE'S FREQUENCY, EACH BY ITS ROLE (a candle's; the Phase 4 review's 1: a weekly ATR read
 * each week's last high and low, an OBV its last day's volume): under a frequency, its first series -- one a role in
 * `taken` -- each read on its own dates, joined, then each period of the frequency taken by its role (an open its
 * first, a high its most, a low its least, a close its last, a volume its total), and each of `sums` (a column of the
 * series on their own dates, the writer's call that writes it: a price times its volume, the library's tradedValue) its
 * total over the period. Of no frequency, the series as they are, `sums` each a column of them.
 */
function rolled(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>, taken: readonly TsAggregate[],
  sums: readonly ((w: Writer, columns: readonly string[]) => string)[] = []): { readonly w: Writer; readonly columns: readonly string[]; readonly sums: readonly string[] } {
  const own: Shared = sh.grain === undefined ? sh : { ...sh, grain: undefined, periodType: undefined };
  const parts = taken.map((_, i) => seriesArg(own, e, i));
  const inner = joined(own, parts);
  const columns = parts.map((p) => columnOfValue(p.v));
  const made = sums.map((f) => f(inner, columns));
  if (sh.grain === undefined) return { w: inner, columns, sums: made };
  // each period of the tile's frequency, of the dates all have
  const fine = own.periodType!;
  const coarse = periodOf(sh.grain, PERIOD, fine);
  periodTyped(sh, coarse.type, sh.first);
  const p = freshName(sh, 'cp');
  const a = freshName(sh, 'ca');
  const rel = inner.rel
    .filter(row(and(...columns.map((c) => fn('isNotEmpty', col(c))))))
    .extend([derive(p, row(coarse.body)), derive(a, row(periodOf(sh.grain, inner.at, fine).at))]);
  const y = (f: string): Lambda => lambda(['y'], fn(f, variable('y')));
  const by = (t: TsAggregate, c: string, name: string): ColSpec => (t === 'last' || t === 'first'
    ? agg(name, row(fn('rowMapper', col(c), col(ORD))), y(t === 'last' ? 'maxBy' : 'minBy'))
    : agg(name, row(col(c)), y(t)));
  const out = columns.map(() => freshName(sh, 'rv'));
  const totals = made.map(() => freshName(sh, 'rsum'));
  // the most rows a date's value took one of, and the periods a year a function inside annualized by, carried
  const rows = inner.rows.map((c) => [c, freshName(sh, 'rows')] as const);
  const years = inner.years.map((c) => [c, freshName(sh, 'py')] as const);
  const grouped = rel.groupBy([inner.part, p], [
    ...columns.map((c, i) => by(taken[i]!, c, out[i]!)),
    ...made.map((c, i) => by('sum', c, totals[i]!)),
    agg(AT, row(col(a)), y('max')),
    ...[...rows, ...years].map(([from, to]) => agg(to, row(col(from)), y('max'))),
    agg(ORD, row(col(ORD)), y('max')),
  ]).rename(p, PERIOD);
  const w = new Writer(grouped, {
    part: inner.part, split: inner.split, at: AT, seconds: inner.seconds, rows: rows.map(([, c]) => c), years: years.map(([, c]) => c),
  }, sh);
  return { w, columns: out, sums: totals };
}

/** A part, a written argument that is no number. */
type Part = Exclude<Node, { constant: unknown }>;

/** Of written arguments, the parts (no number), each a series. */
const partsOf = (nodes: readonly Node[]): Part[] => nodes.filter((n): n is Part => !('constant' in n));

/**
 * GS-QUANT'S REMAINING FUNCTIONS of several series, or of a series and a number (the next phases' 1, 2026-10-26):
 * if, compare, union, append, prepend, excess_returns, weighted_sum -- or undefined, the function another's.
 */
function otherCalls(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node | undefined {
  const written = (): Node[] => e.args.map((a) => write(sh, a));
  switch (e.fn) {
    case 'if': {
      // x where the flags hold (not 0), y where not, each a series or a number -- on the flags' dates, the branch taken
      // none where it has none, whatever the tile's align (gs-quant's if_: no method; the Phase 1 review's 3)
      const [flags, x, y] = written() as [Node, Node, Node];
      if ('constant' in flags) throw new CubeRefusal('if reads a series of flags first: its first argument is a number');
      const parts = partsOf([flags, x, y]);
      let w = flags.w;
      if (parts.length > 1) {
        const al = aligned(sh, parts.map((n) => ({ w: n.w, values: valueColumns(n.v) })), 'nan');
        w = al.w;
        // the flags are the first part: their dates alone
        w.rel = w.rel.filter(row(fn('isNotEmpty', col(al.has[0]!))));
      }
      // the library's (pure-library.ts ifThen), a number a column of it
      const f = columnOfValue(flags.v);
      const branch = (n: Node): string => ('constant' in n ? w.row('if_k', n.constant) : columnOfValue(n.v));
      return { w, v: { column: w.libraryOfRow('ifThen', 'if', [colSpec(f), colSpec(branch(x)), colSpec(branch(y))]) } };
    }
    case 'compare': {
      const [x, y] = written() as [Node, Node];
      if ('constant' in x && 'constant' in y) {
        return { constant: when(fn('greaterThan', x.constant, y.constant), lit.integer(1), when(fn('lessThan', x.constant, y.constant), lit.integer(-1), lit.integer(0))) };
      }
      const parts = partsOf([x, y]);
      const w = parts.length === 1 ? parts[0]!.w : joined(sh, parts, sh.ts.align);
      // gs-quant's (x > y) * 1 + (x < y) * -1: beside none, neither holds -- 0 (the Phase 1 review's 6); the library's
      // (pure-library.ts compare), a number a column of it
      const side = (n: Node): string => ('constant' in n ? w.row('cmp_k', n.constant) : columnOfValue(n.v));
      return { w, v: { column: w.libraryOfRow('compare', 'cmp', [colSpec(side(x)), colSpec(side(y))]) } };
    }
    case 'union':
    case 'append':
    case 'prepend': {
      // every date either has (union: x where it has a value, else y); gs-quant's dates for a splice -- x's every row to
      // its last, then y's after it (append); x's before y's first row, then y's every row (prepend) -- the other's in
      // between none of them (the Phase 1 review's 2: kept as empty rows, they broke every function after it)
      const x = seriesArg(sh, e, 0);
      const y = seriesArg(sh, e, 1);
      const a = columnOfValue(x.v);
      const b = columnOfValue(y.v);
      const al = aligned(sh, [{ w: x.w, values: [a] }, { w: y.w, values: [b] }], 'nan');
      const w = al.w;
      const [hx, hy] = al.has as [string, string];
      if (e.fn === 'union') return { w, v: { column: w.libraryOfRow('union', 'union', [colSpec(a), colSpec(b)]) } };
      // a splice of the align's rows (which side has one on each date), the library's (pure-library.ts append, prepend)
      return { w, v: { column: w.libraryOfRow(e.fn, e.fn, [...w.lineAndDate(), colSpec(a), colSpec(b), colSpec(hx), colSpec(hy)]) } };
    }
    case 'excess_returns': {
      const x = seriesArg(sh, e, 0);
      const rate = e.args[1]!;
      if (rate.kind === 'number') {
        // over a rate: x less the rate accrued on it day by day, actual/360 -- gs-quant's er_j = er_j-1 + x_j - x_j-1 (1 +
        // rate f_j), summed: x_j - rate * the sum of x_k-1 f_k to date
        // the library's (pure-library.ts excessReturns): a period's days its last observation's time's difference (ORD),
        // in seconds over 86400 of a date-time's -- never its date's (the library review, 2026-10-27)
        const w = x.w;
        return { w, v: { column: w.libraryOfLine('excessReturns', 'er', columnOfValue(x.v),
          [colSpec(ORD), enumValue(DURATION, w.seconds ? 'SECONDS' : 'DAYS'), float(rate.value)]) } };
      }
      // over a cash index: on the dates both have, x's first value times the product of (1 + x's return - y's) -- the
      // library's (excessReturnsOver)
      const b = seriesArg(sh, e, 1);
      const w = joined(sh, [x, b]);
      return { w, v: { column: w.libraryOfLine('excessReturnsOver', 'er', columnOfValue(x.v), [colSpec(columnOfValue(b.v))]) } };
    }
    case 'weighted_sum': {
      // each series and its weight, on the dates all have: their weighted sum over the weights' (gs-quant's intersect) --
      // the library's (pure-library.ts weightedSum), over the members as rows (the design note's §5.7)
      const series = e.args.filter((_, i) => i % 2 === 0).map((_, i) => seriesArg(sh, e, i * 2));
      const weights = e.args.filter((_, i) => i % 2 === 1).map((x) => (x as Extract<TsExpr, { kind: 'number' }>).value);
      const w = joined(sh, series);
      const long = w.members(series.map((p) => columnOfValue(p.v)), weights);
      const out = w.fresh('wsum');
      w.joinByDate(long.rel.call(`${LIBRARY_PACKAGE}::weightedSum`, colSpecs([w.part, w.at]), colSpec(long.value), colSpec(long.weight),
        lit.integer(series.length), colSpec(out)));
      return { w, v: { column: out } };
    }
    default:
      return undefined;
  }
}

/** A window or a lag a function is told: a whole number of observations, or a calendar period (the spec's reader holds
 *  it to one). */
function spanOf(e: Extract<TsExpr, { kind: 'call' }>, name: string): Span {
  const v = paramOf(e, name);
  if (typeof v === 'number') return v;
  const t = tenorOf(v);
  if (t === undefined) throw new CubeRefusal(`${e.fn}'s ${name} ${JSON.stringify(v)} is neither a whole number nor a calendar period (3m, 1y)`);
  return t;
}

/**
 * THE ANALYSTS' PACKS (the next phases' 4, 2026-10-26) of several series: a candle's columns' technicals (Wilder's true
 * range, Lane's stochastic, Keltner's and Donchian's channels, Granville's on-balance volume, a volume-weighted price),
 * a series against a benchmark, a total return, a basket, a pair's lead -- or undefined, the function another's. Each
 * series on its own dates first (a benchmark's returns each on its own, as a correlation's), then joined on the dates
 * all have; every figure the database's.
 */
function analystCalls(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node | undefined {
  const n = (name: string): number => paramOf(e, name) as number;
  /** Two series' simple returns, each on its own dates, on the dates both have one (a pair's, a benchmark's). */
  const returnsOf = (k = 0): { readonly w: Writer; readonly a: string; readonly b: string } => {
    const x = seriesArg(sh, e, 0);
    const y = seriesArg(sh, e, 1);
    const ra = x.w.simple(columnOfValue(x.v));
    const r0 = y.w.simple(columnOfValue(y.v));
    // the second's k observations before (a lag), or after (a lead), on its own dates
    const rb = k === 0 ? r0 : k > 0 ? y.w.lag(r0, k) : y.w.lead(r0, -k);
    const w = joined(sh, [{ w: x.w, v: { column: ra } }, { w: y.w, v: { column: rb } }]);
    // each where both have a value (the library's pairs)
    const [a, b] = w.libraryOfMany('pairs', ['pa', 'pb'], [colSpec(ra), colSpec(rb)]);
    return { w, a: a!, b: b! };
  };
  switch (e.fn) {
    // the technicals, the library's (pure-library.ts): each over a row's high, low and close -- or its close and volume,
    // its price and volume -- each period's at the tile's frequency (rolled)
    case 'atr':
    case 'stochastic':
    case 'keltner_channels': {
      const { w, columns } = rolled(sh, e, roleTaken(e.fn)!);
      const [hi, lo, cl] = columns as [string, string, string];
      const roles = [colSpec(w.part), colSpec(w.at), colSpec(hi), colSpec(lo), colSpec(cl)];
      // a w of 1 or under each value as it is from the line's second (smoothed's)
      if (e.fn === 'atr') return { w, v: { column: w.libraryOfRow('atr', 'atr', [...roles, lit.integer(Math.max(n('w'), 1))]) } };
      if (e.fn === 'stochastic') return { w, v: { column: w.libraryOfRow('stochastic', 'stoch', [...roles, lit.integer(n('w'))]) } };
      // the close's exponential average of span w, k average true ranges over a either side; drawn once the average holds
      // w closes (the Phase 4 review's 7: an EMA 20 of 11 drawn)
      const [lower, upper, width] = w.libraryOfMany('keltnerChannels', ['lower', 'upper', 'width'],
        [...roles, lit.integer(Math.max(n('w'), 1)), lit.integer(Math.max(n('a'), 1)), float(n('k'))]);
      return { w, v: { lower: lower!, upper: upper!, width: width! } };
    }
    case 'donchian_channels': {
      const { w, columns } = rolled(sh, e, roleTaken(e.fn)!);
      const [hi, lo] = columns as [string, string];
      const size = spanOf(e, 'w');
      const [lower, upper, width] = typeof size === 'number'
        ? w.libraryOfMany('donchianChannels', ['lower', 'upper', 'width'], [w.windowOf(Writer.frame(size) as 'running' | number), colSpec(hi), colSpec(lo), lit.integer(size)])
        : w.libraryOfMany('donchianChannelsPeriod', ['lower', 'upper', 'width'],
          [colSpec(w.part), colSpec(w.at), colSpec(hi), colSpec(lo), lit.integer(size.n), enumValue(DURATION, size.unit)]);
      return { w, v: { lower: lower!, upper: upper!, width: width! } };
    }
    case 'obv': {
      // each period's last close and its whole volume
      const { w, columns } = rolled(sh, e, roleTaken(e.fn)!);
      const [cl, vol] = columns as [string, string];
      return { w, v: { column: w.libraryOfRow('obv', 'obv', [colSpec(w.part), colSpec(w.at), colSpec(cl), colSpec(vol)]) } };
    }
    case 'vwap': {
      // each price times its volume on its own dates, the library's (tradedValue), totalled a period (at a frequency, a
      // week's every day's)
      const { w, columns, sums } = rolled(sh, e, roleTaken(e.fn)!, [(iw, [p, v]) => iw.libraryOfRow('tradedValue', 'rs', [colSpec(p!), colSpec(v!)])]);
      const [px, vol] = columns as [string, string];
      const size = spanOf(e, 'w');
      const frame = Writer.frame(size);
      if (typeof size === 'number') {
        return { w, v: { column: w.libraryOfRow('vwap', 'vwap', [w.windowOf(frame as 'running' | number), colSpec(px), colSpec(sums[0]!), colSpec(vol), lit.integer(size)]) } };
      }
      // over a calendar period: its ramp from the first price's date
      return { w, v: { column: w.libraryOfLine('vwapPeriod', 'vwap', px, [colSpec(sums[0]!), colSpec(vol), lit.integer(size.n), enumValue(DURATION, size.unit)]) } };
    }
    case 'tracking_error':
    case 'information_ratio':
    case 'up_capture':
    case 'down_capture': {
      const { w, a, b } = returnsOf();
      const size = spanOf(e, 'w');
      const frame = Writer.frame(size);
      // the library's (pure-library.ts): of a count of periods, a deviation's ramp two at least, a capture's the window's;
      // of a calendar period, from the first return's date that period on
      const capture = e.fn === 'up_capture' || e.fn === 'down_capture';
      const perYear = capture ? [] : [colSpec(w.perYearColumn())];
      if (typeof frame !== 'object') {
        return { w, v: { column: w.libraryOfRow(BENCHMARKED[e.fn]!, e.fn, [w.windowOf(frame), colSpec(a), colSpec(b),
          lit.integer(capture ? (size as number) : Math.max(size as number, 2)), ...perYear]) } };
      }
      return { w, v: { column: w.libraryOfRow(`${BENCHMARKED[e.fn]!}Period`, e.fn, [...w.lineAndDate(), colSpec(a), colSpec(b), ...Writer.period(frame), ...perYear]) } };
    }
    case 'lead_lag': {
      const { w, a, b } = returnsOf(n('lag'));
      const size = spanOf(e, 'w');
      const frame = Writer.frame(size);
      if (typeof frame !== 'object') {
        return { w, v: { column: w.libraryOfRow('leadLag', 'corr', [w.windowOf(frame), colSpec(a), colSpec(b), lit.integer(Math.max(size as number, 2))]) } };
      }
      return { w, v: { column: w.libraryOfRow('leadLagPeriod', 'corr', [...w.lineAndDate(), colSpec(a), colSpec(b), ...Writer.period(frame)]) } };
    }
    case 'total_return': {
      const x = seriesArg(sh, e, 0);
      const px = columnOfValue(x.v);
      const told = e.args[1]!;
      if (told.kind === 'number') {
        // a yield a year, accrued day by day: each period's price over the one before, and the yield over the days since
        // -- the library's (pure-library.ts totalReturn), a period's days its last observation's time's difference (ORD)
        const w = x.w;
        return { w, v: { column: w.libraryOfLine('totalReturn', 'tr', px,
          [colSpec(ORD), enumValue(DURATION, w.seconds ? 'SECONDS' : 'DAYS'), float(told.value)]) } };
      }
      // dividends on their dates: (price + the dividends since the price before) / the price before, on the price's
      // dates -- a dividend of a date the price has none of paid at its next: every date either has (the align's), then
      // the library's (totalReturnOf), which keeps the price's
      const d = seriesArg(sh, e, 1);
      const dv = columnOfValue(d.v);
      const al = aligned(sh, [{ w: x.w, values: [px] }, { w: d.w, values: [dv] }], 'nan');
      const w = al.w;
      return { w, v: { column: w.libraryOfLine('totalReturnOf', 'tr', px, [colSpec(dv)]) } };
    }
    case 'basket':
      return basket(sh, e);
    default:
      return undefined;
  }
}

/**
 * A BASKET (gs-quant's backtesting.basket_series, its costs none): its series held in weights from 100, on the dates
 * all have -- units fixed between rebalances, set at each to the basket's then in its weights. Each date's rebalance
 * period: its own (daily); the weeks, or the months, from the first date (a rebalance the first date on or after each,
 * gs-quant's `min(cal[cal >= date])`; a month's day kept, or its last where it has none, as relativedelta's). Within a
 * period, 1 + Σ w (price / its price at the period's first date - 1); into it, the period before's growth to its first
 * date; the basket 100 times those growths compounded, times its growth within -- every figure the database's, the
 * library's (pure-library.ts basket) over the members as rows.
 */
function basket(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node {
  const series = e.args.filter((_, i) => i % 2 === 0).map((_, i) => seriesArg(sh, e, i * 2));
  const weights = e.args.filter((_, i) => i % 2 === 1).map((x) => (x as Extract<TsExpr, { kind: 'number' }>).value);
  const rebalance = paramOf(e, 'rebalance') as string;
  if (rebalance !== 'daily' && rebalance !== 'weekly' && rebalance !== 'monthly') {
    throw new CubeRefusal(`a basket rebalances daily, weekly or monthly, not ${JSON.stringify(rebalance)}`);
  }
  // the library's (pure-library.ts basket), over the members as rows: a week's days counted by its last observation's
  // time (ORD), in seconds over 86400 of a date-time's
  const w = joined(sh, series);
  const long = w.members(series.map((p) => columnOfValue(p.v)), weights);
  const out = w.fresh('basket');
  w.joinByDate(long.rel.call(`${LIBRARY_PACKAGE}::basket`, colSpec(w.part), colSpec(w.at), colSpec(ORD), colSpec(long.member),
    colSpec(long.value), colSpec(long.weight), lit.integer(series.length), libraryOption('Rebalance', rebalance), enumValue(DURATION, w.seconds ? 'SECONDS' : 'DAYS'),
    colSpec(out)));
  return { w, v: { column: out } };
}

/** A cycle in the observations of a tile's frequency (a classical decomposition's period): undefined, none whole. */
const CYCLES: Readonly<Partial<Record<TimeGrain, Readonly<Partial<Record<string, number>>>>>> = {
  day: { week: 5, month: 21, quarter: 63, year: 252 },
  week: { month: 4, quarter: 13, year: 52 },
  month: { quarter: 3, year: 12 },
  quarter: { year: 4 },
};

function call(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node {
  const n = (name: string): number => paramOf(e, name) as number;
  // a window or a lag: a whole number of observations, or a calendar period (the spec's reader holds it to one)
  const span = (name: string): Span => spanOf(e, name);
  const word = (name: string): string => paramOf(e, name) as string;
  // the pairs and the candle: each series on its own dates, then joined
  if (e.fn === 'correlation' || e.fn === 'beta' || e.fn === 'cov' || e.fn === 'alpha' || e.fn === 'r_squared'
    || e.fn === 'hedge_ratio' || e.fn === 'residual') {
    const a = seriesArg(sh, e, 0);
    const b = seriesArg(sh, e, 1);
    // gs-quant's correlation and beta read each series' returns, taken on its own dates, then aligned (an alpha and an
    // R² too); cov, a hedge ratio and a residual the series as they are (a regression of the levels)
    const levels = e.fn === 'cov' || e.fn === 'hedge_ratio' || e.fn === 'residual';
    const ra = levels ? columnOfValue(a.v) : a.w.simple(columnOfValue(a.v));
    const rb = levels ? columnOfValue(b.v) : b.w.simple(columnOfValue(b.v));
    const w = joined(sh, [{ w: a.w, v: { column: ra } }, { w: b.w, v: { column: rb } }]);
    const size = span('w');
    const frame = Writer.frame(size);
    const of = PAIRWISE[e.fn]!;
    const perYear = e.fn === 'alpha' ? [colSpec(w.perYearColumn())] : [];
    // the library's (pure-library.ts), each over the window's pairs: of a count of periods, its ramp two at least; of a
    // calendar period, from the first pair's date that period on
    if (typeof frame !== 'object') {
      return { w, v: { column: w.libraryOfRow(of, e.fn, [w.windowOf(frame), colSpec(ra), colSpec(rb), lit.integer(Math.max(size as number, 2)), ...perYear]) } };
    }
    return { w, v: { column: w.libraryOfRow(`${of}Period`, e.fn, [...w.lineAndDate(), colSpec(ra), colSpec(rb), ...Writer.period(frame), ...perYear]) } };
  }
  if (e.fn === 'ohlc') return candle(sh, e);
  const special = otherCalls(sh, e) ?? analystCalls(sh, e);
  if (special !== undefined) return special;
  if (e.fn === 'interpolate') {
    // x onto y's dates (gs-quant's datetime.interpolate; the Phase 1 review's 5): every date either has merged, then
    // y's alone kept -- the dates both have (intersect), none where x has none (nan), 0 there (zero); step, as gs-quant's
    // walks the dates asked: the first x's value as of it (its first, before it starts), each after its own value, else
    // the one before among them -- an x date between two asked never read
    const x = seriesArg(sh, e, 0);
    const y = seriesArg(sh, e, 1);
    const xs = valueColumns(x.v);
    const al = aligned(sh, [{ w: x.w, values: xs }, { w: y.w, values: [] }], 'nan');
    const [hx, hy] = al.has as [string, string];
    const method = word('method') as 'intersect' | 'nan' | 'zero' | 'step';
    const m = al.w;
    const w = new Writer(m.rel, { part: m.part, split: m.split, at: m.at, seconds: x.w.seconds, rows: x.w.rows, years: x.w.years }, sh);
    // x's value as of each date of either, before y's alone are kept: a step's first
    const asOf = method === 'step' ? xs.map((c) => w.row('ip_asof', col(c))) : [];
    if (method === 'step') fill(w, asOf, al.ords[0]!, 'step');
    const firstY = method === 'step' ? w.window('ip_first', 'min', Writer.read(w.row('ip_yp', when(fn('isNotEmpty', col(hy)), col(PERIOD), none()))), 'all') : undefined;
    // y's dates kept after the windows above read every date: a filter of a plain column folds under its select's windows
    // (the engine's relational rule, Fold.filterSlot), one reading a window is the database's QUALIFY, after them --
    // y's first date, which every line of y has
    const asked = firstY ?? w.window('ip_first', 'min', Writer.read(w.row('ip_yp', when(fn('isNotEmpty', col(hy)), col(PERIOD), none()))), 'all');
    w.rel = w.rel.filter(row(and(fn('isNotEmpty', col(asked)), fn('isNotEmpty', col(hy)), ...(method === 'intersect' ? [fn('isNotEmpty', col(hx))] : []))));
    if (method === 'zero') fill(w, xs, al.ords[0]!, 'zero');
    if (method === 'step') {
      // its own value where it has one; at the first date asked, its value as of it; then each carried to the next
      xs.forEach((c, i) => {
        const raw = w.fresh('ip_raw');
        w.rel = w.rel.rename(c, raw);
        w.rel = w.rel.extend([derive(c, row(when(fn('isNotEmpty', col(raw)), col(raw),
          when(fn('equal', col(PERIOD), col(firstY!)), col(asOf[i]!), none()))))]);
      });
      fill(w, xs, al.ords[0]!, 'step');
    }
    return { w, v: x.v };
  }
  const x = seriesArg(sh, e, 0);
  const w = x.w;
  const a = columnOfValue(x.v);
  const done = (column: string): Node => ({ w, v: { column } });
  switch (e.fn) {
    case 'returns':
      return done(w.returns(a, span('obs'), word('type')));
    case 'prices':
      // initial at the first date, and every other from there: gs-quant's product (and sum) over the returns to date
      // skips an empty one, as pandas' prod does -- a return of none (the first, which has no date before it) leaves the
      // level where it was (the second audit's S5); the library's (pure-library.ts prices)
      return done(w.libraryOfLine('prices', 'px', a, [float(n('initial')), libraryOption('Returns', word('type'))]));
    // the levels and lags, the library's (pure-library.ts): a lag or a change over a count of periods or a calendar
    // period back, a base over the whole line (its first value, or the value as of the first date shown)
    case 'diff': {
      const obs = span('obs');
      return done(typeof obs === 'number' ? w.library('diff', 'diff', a, null, [lit.integer(obs)]) : w.libraryOfPeriod('diffPeriod', 'diff', a, obs));
    }
    case 'lag':
      return done(w.libraryLag(a, span('obs')));
    case 'annualize':
      return done(w.libraryOfRow('annualize', 'ann', [colSpec(a), colSpec(w.perYearColumn())]));
    case 'index':
    case 'rebase':
      return done(w.libraryBased('index', 'idx', a, [float(n(e.fn === 'index' ? 'initial' : 'base'))]));
    case 'change':
      return done(w.libraryBased('change', 'chg', a));
    case 'percent_change':
      return done(w.libraryBased('percentChange', 'pchg', a));
    // the rolled statistics, the library's (pure-library.ts): over a count of periods (0 from the start) with a ramp
    // of that count, or over a calendar period
    case 'mean':
    case 'sum':
    case 'min':
    case 'max':
    case 'range': {
      const size = span('w');
      return done(w.libraryRolled(e.fn, a, size, typeof size === 'number' ? size : 0));
    }
    case 'std':
    case 'var': {
      const size = span('w');
      return done(w.libraryRolled(e.fn, a, size, typeof size === 'number' ? Math.max(size, 2) : 0));
    }
    // the multi-step windows, the library's (pure-library.ts): over a count of periods (0: every observation, or from
    // the start) with its ramp, or over a calendar period where the library has the function
    case 'zscores': {
      const size = span('w');
      if (typeof size !== 'number') return done(w.libraryOfPeriod('zscoresPeriod', 'z', a, size));
      return done(w.library('zscores', 'z', a, size === 0 ? 'all' : size, [lit.integer(size)]));
    }
    case 'winsorize':
      return done(w.library('winsorize', 'wins', a, 'all', [float(n('limit'))]));
    case 'volatility': {
      const r = w.returns(a, 1, word('type'));
      const size = span('w');
      // over a count of periods (0: from the start): the library's (pure-library.ts volatility), of the returns above
      if (typeof size === 'number') {
        return done(w.library('volatility', 'vol', r, Writer.frame(size) as 'running' | number, [lit.integer(Math.max(size, 2)), colSpec(w.perYearColumn())]));
      }
      // over a calendar period, from the first price's date that period on: gs-quant's returns keep the first price's
      // date (its return none), which its ramp counts from (the E9 review's nit: from the first return's, a date late)
      return done(w.libraryOfLine('volatilityPeriod', 'vol', a, [colSpec(r), lit.integer(size.n), enumValue(DURATION, size.unit), colSpec(w.perYearColumn())]));
    }
    case 'drawdown': {
      const size = span('w');
      if (typeof size !== 'number') return done(w.libraryOfPeriod('drawdownPeriod', 'dd', a, size));
      return done(w.library('drawdown', 'dd', a, Writer.frame(size) as 'running' | number, [lit.integer(size)]));
    }
    case 'max_drawdown': {
      const size = span('w');
      if (typeof size === 'number') return done(w.library('maxDrawdown', 'mdd', a, Writer.frame(size) as 'running' | number, [lit.integer(size)]));
      // over a calendar period: its ramp from the first date the series has, not its drawdowns' (a period later)
      return done(w.libraryOfPeriod('maxDrawdownPeriod', 'mdd', a, size));
    }
    case 'sharpe_ratio': {
      const size = span('w');
      const r = w.simple(a);
      // over a count of periods: the library's, its ramp the deviation's (two at least) and the window's
      if (typeof size === 'number') {
        return done(w.library('sharpeRatio', 'sharpe', r, Writer.frame(size) as 'running' | number,
          [lit.integer(Math.max(size, 2)), colSpec(w.perYearColumn())]));
      }
      // over a calendar period: its ramp from the first price's date, not its returns'
      return done(w.libraryOfLine('sharpeRatioPeriod', 'sharpe', a, [colSpec(r), lit.integer(size.n), enumValue(DURATION, size.unit), colSpec(w.perYearColumn())]));
    }
    case 'bollinger_bands': {
      // the library's: over a count of periods (its ramp the window's and two), or a calendar period
      const size = span('w');
      const k = float(n('k'));
      const [lower, upper, width] = typeof size === 'number'
        ? w.libraryOfMany('bollingerBands', ['lower', 'upper', 'width'], [w.windowOf(Writer.frame(size) as 'running' | number), colSpec(a), lit.integer(size), k])
        : w.libraryOfMany('bollingerBandsPeriod', ['lower', 'upper', 'width'],
          [colSpec(w.part), colSpec(w.at), colSpec(a), lit.integer(size.n), enumValue(DURATION, size.unit), k]);
      return { w, v: { lower: lower!, upper: upper!, width: width! } };
    }
    case 'rsi':
      // gs-quant's (technicals.relative_strength_index): each change's gain and loss, each Wilder's smoothed average over
      // w of them (its first the mean of the w changes after the first, as gs-quant's ramp has it) -- the library's
      return done(w.libraryOfLine('rsi', 'rsi', a, [lit.integer(n('w'))]));
    case 'smoothed_moving_average':
      return done(w.smoothed(a, n('w')));
    case 'dickey_fuller':
    case 'half_life': {
      // Δx = a + b·x[-1] over the window, least squares (the next phases' 4): b their covariance over x[-1]'s variance --
      // its t-statistic Dickey and Fuller's (statsmodels' adfuller of no lags, a constant), its half-life −ln 2 / ln(1 + b)
      const size = span('w');
      // w differences (w + 1 values): a fit of a constant and a slope over under 3 has no deviation left to judge it by
      if (typeof size === 'number' && size > 0 && size < 3) throw new CubeRefusal(`${e.fn} over ${size} differences has no deviation left: w is 0 (every one to date) or at least 3`);
      const frame = Writer.frame(size);
      // the library's (pure-library.ts dickeyFuller, halfLife): of a count of periods, its ramp three at least; of a calendar
      // period, from the first pair's date that period on
      const of = e.fn === 'half_life' ? 'halfLife' : 'dickeyFuller';
      if (typeof frame !== 'object') return done(w.library(of, e.fn, a, frame, [lit.integer(Math.max(size as number, 3))]));
      return done(w.libraryOfPeriod(`${of}Period`, e.fn, a, frame));
    }
    // the self-joins, the library's (pure-library.ts): each row's window its line joined to itself on the rows (or the
    // dates of a calendar period) back
    case 'percentiles': {
      // scipy's percentileofscore(window, value, kind='mean'), gs-quant's: the share of the window below the value, those
      // equal to it counted half, in percent
      const size = span('w');
      return done(typeof size === 'number' ? w.libraryOfLine('percentiles', 'pr', a, [lit.integer(size)])
        : w.libraryOfPeriod('percentilesPeriod', 'pr', a, size));
    }
    case 'median':
    case 'percentile': {
      // pandas' quantile (the database's continuous percentile, between the two nearest): a median its 50th
      const size = span('w');
      // the planner takes the fraction as a literal alone (an aggregate's argument): gs-quant's n of 100 spelled as one
      const q = float(e.fn === 'median' ? 0.5 : n('n') / 100);
      // of every value (percentile's w 0): one aggregate a line, never each row joined to every other (the E9 review's 5:
      // 4,000 rows took 4.5 s)
      if (e.fn === 'percentile' && size === 0) return done(w.libraryOfRow('quantileOfLine', 'pct', [colSpec(w.part), colSpec(a), q]));
      return done(typeof size === 'number' ? w.libraryOfLine('quantile', 'pct', a, [lit.integer(size), q])
        : w.libraryOfPeriod('quantilePeriod', 'pct', a, size, [q]));
    }
    case 'exponential_moving_average':
      return done(n('beta') === 0 ? a : w.ewm(a, float(n('beta'))));
    case 'exponential_std':
      return done(w.ewmStd(a, float(n('beta'))));
    // the exponentials, the library's (pure-library.ts): gs-quant's exponential_volatility (annualize(exponential_std(
    // returns(x), beta)) * 100), exponential_spread_volatility (of diff(x, 1), in its own units) and macd (ewm(span=m) -
    // ewm(span=n), itself ewm(span=s); a span of 1 each value as it is)
    case 'exponential_volatility':
      return done(w.libraryOfLine('exponentialVolatility', 'evol', a, [float(n('beta')), colSpec(w.perYearColumn())]));
    case 'exponential_spread_volatility':
      return done(w.libraryOfLine('exponentialSpreadVolatility', 'esvol', a, [float(n('beta')), colSpec(w.perYearColumn())]));
    case 'macd':
      return done(w.libraryOfLine('macd', 'macd', a, [lit.integer(n('m')), lit.integer(n('n')), lit.integer(n('s'))]));
    // the row maths and the functions along the line, the library's (pure-library.ts)
    case 'log':
    case 'exp':
    case 'sqrt':
    case 'abs':
    case 'not':
      return done(w.libraryOfRow(e.fn, e.fn, [colSpec(a)]));
    case 'power':
      return done(w.libraryOfRow('power', 'pow', [colSpec(a), float(n('y'))]));
    case 'floor':
    case 'ceil':
      return done(w.libraryOfRow(e.fn, e.fn, [colSpec(a), float(n('value'))]));
    case 'filter': {
      // gs-quant's filter_: the values the operator matches removed (empty: the dates of none) -- the rows dropped,
      // after every figure before it is computed over them all (a filter of a window's figure is the database's
      // QUALIFY), so a figure after it reads the dates left
      const how = word('operator');
      const operators: readonly string[] = LIBRARY_ENUMS.FilterOperator;
      if (!operators.includes(how)) throw new CubeRefusal(`there is no ${JSON.stringify(how)} filter (${operators.join(', ')})`);
      w.rel = w.rel.call(`${LIBRARY_PACKAGE}::filterValues`, colSpec(a), libraryOption('FilterOperator', how), float(n('value')));
      return done(a);
    }
    case 'product': {
      const size = span('w');
      const frame = Writer.frame(size);
      // the library's (pure-library.ts product, productPeriod)
      if (typeof frame !== 'object') return done(w.library('product', 'prod_v', a, frame, [lit.integer(size as number)]));
      return done(w.libraryOfPeriod('productPeriod', 'prod_v', a, frame));
    }
    case 'geometrically_aggregate':
      return done(w.libraryOfLine('geometricallyAggregate', 'geo', a, []));
    case 'smooth_spikes':
      // gs-quant's: a value beyond both neighbours (by 1 + threshold times, or threshold beyond) their mean -- beside none
      // no spike (a comparison with NaN never holds: kept, the Phase 1 review's 6); its first and last rows dropped (none)
      return done(w.libraryOfLine('smoothSpikes', 'smooth', a, [float(n('threshold')), libraryOption('ThresholdType', word('type'))]));
    case 'vol_swap_volatility': {
      // gs-quant's: the root mean square of log returns (a mean of zero) over the window, times the root of 252, in percent
      const size = span('w');
      const frame = Writer.frame(size);
      // the library's (pure-library.ts volSwapVolatility, volSwapVolatilityPeriod)
      if (typeof frame !== 'object') return done(w.library('volSwapVolatility', 'vs', a, frame, [lit.integer(size as number)]));
      return done(w.libraryOfPeriod('volSwapVolatilityPeriod', 'vs', a, frame));
    }
    case 'consecutive':
      // a run of values above 0 (below), counted to each date; 0 where it breaks -- each break begins a group
      return done(w.libraryOfLine('consecutive', 'cs', a, [libraryOption('Direction', word('direction'))]));
    case 'trend':
    case 'seasonally_adjusted':
      return done(decomposed(sh, w, a, e.fn, word('method'), word('freq')));
    case 'first':
    case 'last':
      // on every date it has, those of none too (gs-quant's fills its index: the Phase 1 review's 6): the library's
      return done(w.libraryOfLine(e.fn, e.fn, a, []));
    case 'count':
      return done(w.libraryOfLine('count', 'count', a, []));
    case 'day':
    case 'month':
    case 'year':
    case 'quarter':
    case 'weekday':
      // of every date it has, those of none too (gs-quant's: of its index); pandas' 0 for a Monday
      return done(w.libraryOfRow(e.fn, e.fn, [colSpec(w.at)]));
    case 'if':
    case 'compare':
    case 'union':
    case 'append':
    case 'prepend':
    case 'excess_returns':
    case 'weighted_sum':
      throw new Error(`${e.fn} is written by otherCalls`);
    case 'atr':
    case 'stochastic':
    case 'keltner_channels':
    case 'donchian_channels':
    case 'obv':
    case 'vwap':
    case 'tracking_error':
    case 'information_ratio':
    case 'up_capture':
    case 'down_capture':
    case 'total_return':
    case 'basket':
    case 'lead_lag':
      throw new Error(`${e.fn} is written by analystCalls`);
  }
}

/**
 * A CLASSICAL DECOMPOSITION (gs-quant's technicals.trend and seasonally_adjusted, statsmodels' seasonal_decompose):
 * over a cycle of p observations of the tile's frequency, its trend a moving average centred over p (of p + 1, the ends
 * halved, for an even p), none where the window is not whole; each place in the cycle's (its row's number, mod p) mean of
 * the detrended values (x less the trend, or over it), centred (less their mean, or over it): the season. Adjusted, x
 * less its season (over it), where the trend is -- every step the database's.
 */
function decomposed(sh: Shared, w: Writer, a: string, what: 'trend' | 'seasonally_adjusted', method: string, freq: string): string {
  const grain = sh.grain;
  const p = grain === undefined ? undefined : CYCLES[grain]?.[freq];
  if (grain === undefined) throw new CubeRefusal(`${what} counts its cycle in the tile's periods: give the tile a frequency (Monthly)`);
  if (p === undefined) throw new CubeRefusal(`${what}: a ${freq} is no cycle of ${grain === 'day' ? 'days' : `${grain}s`} -- choose a longer one, or another frequency`);
  // the library's (pure-library.ts trend, seasonallyAdjusted): a cycle of p periods, k its reach either side (a
  // window's frame, a literal)
  const k = Math.floor(p / 2);
  if (what === 'trend') return w.libraryOfLine('trend', 'dc_trend', a, [lit.integer(p), lit.integer(k)]);
  // its place in the cycle: the calendar's at a frequency of months, quarters or weeks, the library's (periodIndex) -- a
  // missing one never moving the places after it, as gs-quant's calendar filled first keeps them (the Phase 1 review's
  // 4) -- its row's number at days, a cycle of business days (a place is read modulo the cycle: where it starts counting
  // moves no value to another place)
  const place = grain === 'month' || grain === 'quarter' || grain === 'week'
    ? w.libraryOfRow('periodIndex', 'dc_i', [colSpec(PERIOD), libraryOption('Grain', grain)]) : w.rowNumber();
  return w.libraryOfLine('seasonallyAdjusted', 'dc_sa', a,
    [colSpec(place), lit.integer(p), lit.integer(k), libraryOption('SeasonalModel', method)]);
}

/** A base's aggregate over a period's rows (`ord`: the row's number in time, which `last` and `first` order by). */
function baseAggregate(name: string, b: Base, ord: string): ColSpec {
  const y = variable('y');
  switch (b.fn) {
    case 'last':
    case 'first':
      return agg(name, row(fn('rowMapper', col(b.column), col(ord))), lambda(['y'], fn(b.fn === 'last' ? 'maxBy' : 'minBy', y)));
    case 'average':
    case 'sum':
    case 'min':
    case 'max':
      return agg(name, row(col(b.column)), lambda(['y'], fn(b.fn, y)));
  }
}

/**
 * A source's date taken to the tile's frequency: its period (the day, the week's first day...), the type it has, and the
 * date a row's value is of (`at`: its own day, a date-time's at a frequency) -- each period's latest, the date its value
 * is of. A date of no type the compiler gave is refused (the review's G2: guessed a StrictDate).
 */
function periodOf(grain: TimeGrain | undefined, date: string, dateType: string | undefined): {
  readonly body: ValueSpecification; readonly type: string; readonly at: ValueSpecification;
} {
  if (dateType === undefined) throw new CubeRefusal(`${date} is no column of its grid`);
  if (!isTemporal(dateType) || isTimeOfDay(dateType)) throw new CubeRefusal(`${date} is a ${dateType}, not a date: a timeseries is read along a date`);
  if (grain === undefined || (grain === 'day' && isDay(dateType))) return { body: col(date), type: dateType, at: col(date) };
  const day = isDay(dateType) ? col(date) : fn('datePart', one(date));
  if (grain === 'day') return { body: day, type: 'StrictDate', at: day };
  return { body: fn(GRAIN_FUNCTION[grain], one(date)), type: 'StrictDate', at: day };
}

/** A source of the tile by its alias, and its input, or a refusal naming it. */
function sourceNamed(ts: TimeseriesSpec, inputs: TsInputs, alias: string): { readonly src: TsSource; readonly input: TsInput } {
  const src = ts.sources.find((x) => x.alias === alias);
  if (!src) throw new CubeRefusal(`there is no source ${alias}`);
  const input = inputs.get(alias);
  if (!input) throw new CubeRefusal(`source ${alias}'s grid is not open`);
  return { src, input };
}

/** The type of a source's period at the tile's frequency: what a date the viewer passes back is written as. */
export function periodType(ts: TimeseriesSpec, input: TsInput, src: TsSource): string {
  return periodOf(ts.grain, src.date, typeIn(input.snapshot, src.date)).type;
}

/**
 * A SERIES' RELATION: its expression written part by part (steps 1 to 5, the file's header) -- each column over its own
 * source's rows and dates, each function over its operand's part, the parts joined on the period where an operator or a
 * function reads them together -- and a split's first MAX_SPLIT_LINES lines. `anchor`: the first date shown, where an
 * index starts.
 */
function seriesRelation(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, anchor: string | undefined, names: { n: number } = { n: 0 }): Built {
  const expr = candled(inlined(series.expr, ts.series));
  const read = sourcesOf(ts, expr);
  if (read.length === 0) throw new CubeRefusal(`series ${series.id} reads no column of a grid`);
  // `names`: a query of two series' relations names their columns apart (a scatter's)
  const sh: Shared = { ts, grain: ts.grain, series, inputs, split: series.split, first: read[0]!, anchor, periodType: undefined, names };
  const top = write(sh, expr);
  if ('constant' in top) throw new CubeRefusal(`series ${series.id} reads no column of a grid`);
  const w = top.w;
  const split = series.split;
  if (split !== undefined && !w.split) {
    throw new CubeRefusal(`series ${series.id} is split by ${split}, but reads one ${split}'s rows alone: a line per ${split} needs a column of every one`);
  }
  if (split !== undefined) {
    const rank = `${WINDOW_ALL_PREFIX}ts_line`;
    w.rel = w.rel.extend([derive(rank, lambda(['p', 'w', 'r'], fn('denseRank', variable('p'), variable('w'), variable('r'))))],
      fn('over', collection([asc(split)])));
    w.rel = w.rel.filter(row(fn('lessThanEqual', col(rank), lit.integer(MAX_SPLIT_LINES))));
  }
  let unit = unitOf(expr);
  const level = levelOf(expr);
  let built: { value: string; levelValue?: string; lower?: string; upper?: string; width?: string; open?: string; high?: string; low?: string };
  const v = top.v;
  if ('lower' in v) {
    built = { value: v.upper, lower: v.lower, upper: v.upper, width: v.width };
  } else if ('open' in v) {
    // a candle drawn as the rows are: compare moves no candle (its four prices would need one base each)
    built = { value: v.close, open: v.open, high: v.high, low: v.low };
  } else {
    let value = v.column;
    const own = value;
    // compare: every LEVEL from one start -- a return, a difference, a deviation drawn as it is (the review's Q2) --
    // the library's, as gs-quant's: rebased, x / x0 · 100 (`index`); its % change, x / x0 - 1 (`percentChange`) --
    // x0 its value as of the first date shown (`...From`), else its first (the user, 2026-10-28: "match gsquant", over
    // the second audit's S1, which had read a start below zero rising as a rise)
    const how = ts.view?.compare;
    if (how !== undefined && level) {
      value = how === 'rebase' ? w.libraryBased('index', 'cmp', value, [float(100)]) : w.libraryBased('percentChange', 'cmp', value);
      unit = how === 'rebase' ? 'index' : 'ratio';
    }
    built = { value, ...(value !== own ? { levelValue: own } : {}) };
  }
  // a CONDITION's runs (gaps and islands, the library's `runs`): each date where it holds carries its run's first date
  // and where its shading ends -- the line's next date after it, from which it no longer holds (its own last at the
  // line's end): what the drawing shades, a run of one date a period wide (the E9 review's 4: to its last date, a run of
  // one had no width and every run was a period short)
  let shade: { readonly shadeFrom: string; readonly shadeTo: string } | undefined;
  if (isCondition(expr)) {
    const [shadeFrom, shadeTo] = w.libraryOfMany('runs', ['sh_from', 'sh_to'], [...w.lineAndDate(), colSpec(built.value)]);
    shade = { shadeFrom: shadeFrom!, shadeTo: shadeTo! };
  }
  // the most rows any of its columns took one of, at a period: more than one, and the drawing says so
  const rows = w.rows.length === 0 ? undefined : w.rows.length === 1 ? w.rows[0]!
    // the most of them by the database's max of two (GREATEST): `when(c > most, c, most)` wrote `most` twice, so the
    // query doubled with every part read (eight parts: each first part's count 130 times -- the Phase 1 review's 1)
    : w.row('rows', w.rows.slice(1).reduce<ValueSpecification>((most, c) => fn('max', one(c), most), one(w.rows[0]!)));
  // the periods a year it was annualized by: of a combination, none when any part's is none (the second audit's S4: a
  // part of no frequency, every value none, and the first part's 252 said nothing was wrong)
  const perYear = w.years.length <= 1 ? w.years[0]
    : w.row('per_year_all', when(or(...w.years.map((c) => fn('isEmpty', col(c)))), none(), one(w.years[0]!)));
  return {
    rel: w.rel, period: PERIOD, ord: ORD, at: w.at, periodType: sh.periodType!, ...built, ...(shade ?? {}), ...(split !== undefined ? { split } : {}),
    unit, level, ...(rows !== undefined ? { rows } : {}), ...(perYear !== undefined ? { perYear } : {}), sources: read, writer: w,
  };
}

/** The sources a series reads, by alias, in the order it reads them: one, run where its grid runs; more, a combination. */
export function seriesSources(ts: TimeseriesSpec, series: TsSeries): string[] {
  return sourcesOf(ts, inlined(series.expr, ts.series));
}

/**
 * A SERIES' QUERY: its periods (and split values), latest first, each with its value -- or a band's lower and upper
 * -- at most `limit` of them (TS_POINT_LIMIT, the drawing's), whole dates, and the date the cut crossed at. `view.from`: the first date shown, an index's base (readsAnchor). `clip`: the dates
 * its answer keeps (a host reading a window: Python's Timeseries.frame) -- kept by the database after every value is
 * computed over every date, so a window's first values have their history (a 63-day volatility its 63 days); absent,
 * every date (the viewer's, which zooms over them).
 */
export function seriesQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, view: TsWindow = {}, clip: TsWindow = {},
  limit: number | null = TS_POINT_LIMIT): TsQuery {
  const b = seriesRelation(inputs, ts, series, view.from);
  const out: ColSpec[] = [
    derive(TS_DATE, row(col(b.at))),
    derive(TS_VALUE, row(col(b.value))),
    ...(b.lower !== undefined ? [derive(TS_LOWER, row(col(b.lower)))] : []),
    ...(b.upper !== undefined ? [derive(TS_UPPER, row(col(b.upper)))] : []),
    ...(b.width !== undefined ? [derive(TS_WIDTH, row(col(b.width)))] : []),
    ...(b.open !== undefined ? [derive(TS_OPEN, row(col(b.open))), derive(TS_HIGH, row(col(b.high!))), derive(TS_LOW, row(col(b.low!)))] : []),
    ...(b.shadeFrom !== undefined ? [derive(TS_FROM, row(col(b.shadeFrom))), derive(TS_TO, row(col(b.shadeTo!)))] : []),
    ...(b.split !== undefined ? [derive(TS_SPLIT, row(col(b.split)))] : []),
    ...(b.rows !== undefined ? [derive(TS_ROWS, row(col(b.rows)))] : []),
    ...(b.perYear !== undefined ? [derive(TS_PER_YEAR, row(col(b.perYear)))] : []),
  ];
  let rel = b.rel.extend(out);
  const within: ValueSpecification[] = [];
  if (clip.from !== undefined) within.push(fn('greaterThanEqual', one(b.at), dateLiteral(clip.from, b.periodType)));
  if (clip.to !== undefined) within.push(fn('lessThanEqual', one(b.at), dateLiteral(`${clip.to}${isDay(b.periodType) ? '' : 'T23:59:59'}`, b.periodType)));
  if (within.length > 0) rel = rel.filter(row(and(...within)));
  rel = rel.select(out.map((c) => c.name));
  if (limit !== null) {
    // A CUT KEEPS WHOLE DATES (the review's Q12: a cut at a count of rows kept some of a split's lines at its oldest date
    // and not others): each date's rows kept while the rows of the dates after it are at most `limit` -- the date it
    // crosses at kept too, so a reader knows there are more and draws the dates after it alone -- the database's
    const rn = `${WINDOW_ALL_PREFIX}ts_cut_rn`;
    const first = `${WINDOW_ALL_PREFIX}ts_cut_first`;
    rel = rel.extend([derive(rn, lambda(['p', 'w', 'r'], fn('rowNumber', variable('p'), variable('r'))))], fn('over', collection([desc(TS_DATE)])))
      .extend([agg(first, lambda(['p', 'w', 'r'], Writer.read(rn)), lambda(['y'], fn('min', variable('y'))))], fn('over', colSpecs([TS_DATE])))
      .filter(row(fn('lessThanEqual', col(first), lit.integer(limit + 1))))
      .select(out.map((c) => c.name));
  }
  rel = rel.sort([desc(TS_DATE)]);
  return {
    series: series.id,
    query: rel.lambda(),
    unit: b.unit,
    band: b.lower !== undefined,
    ...(b.open !== undefined ? { candles: true } : {}),
    ...(b.shadeFrom !== undefined ? { shade: true } : {}),
    ...(b.split !== undefined ? { split: b.split } : {}),
    level: b.level,
    counted: b.rows !== undefined,
    sources: b.sources,
  };
}

/**
 * THE DATES A SOURCE SPANS, and where each range button starts: the latest and the earliest date of its grid's rows (its
 * filter as the tile reads it), and each period back from the latest -- `adjust(latest, -1, YEARS)`, the year's first
 * day for YTD -- the DATABASE's date arithmetic, which the viewer only places its zoom at (the source with the latest
 * date of all, found by comparing).
 */
export function boundsQuery(input: TsInput, src: TsSource): Lambda {
  const s = input.snapshot;
  const typeOf = (c: string): string | undefined => typeIn(s, c);
  const all = `${WINDOW_ALL_PREFIX}ts_bounds`;
  let rel = sourceWithDerived(s);
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(src.date))];
  const filter = filterRead(src, input.filter);
  if (filter) conditions.push(filterNode(filter, 'x', typeOf));
  rel = rel.filter(row(and(...conditions)))
    .extend([derive(all, row(lit.string('all')))])
    .groupBy([all], [
      agg(TS_LATEST, row(col(src.date)), lambda(['y'], fn('max', variable('y')))),
      agg(TS_EARLIEST, row(col(src.date)), lambda(['y'], fn('min', variable('y')))),
    ]);
  const starts: ColSpec[] = TS_RANGES.filter((r) => r.word !== 'max').map((r) => {
    const step = RANGE_STEP[r.word];
    const body = step === undefined
      ? fn('firstDayOfYear', one(TS_LATEST))
      : fn('adjust', one(TS_LATEST), lit.integer(-step[0]), enumValue(DURATION, step[1]));
    return derive(rangeStartName(r.word), row(body));
  });
  return rel.extend(starts).select([TS_LATEST, TS_EARLIEST, ...starts.map((c) => c.name)]).lambda();
}

/**
 * MARKS FROM A GRID'S ROWS (TsMarks; Phase 5's events from a grid, Phase 6's trades): each row of its source -- the
 * grid's filter as the tile reads it -- that its matches pick, with a date (and a value where it is drawn at one), at
 * its date as a series' points are (a date-time its day under a frequency: periodOf's `at`), its value and its text the
 * row's own columns: latest first, at most `limit` and one more, so a reader knows when there are more -- the database's.
 */
export function marksQuery(inputs: TsInputs, ts: TimeseriesSpec, marks: TsMarks, limit: number = TS_MARK_LIMIT): Lambda {
  const { src, input } = sourceNamed(ts, inputs, marks.source);
  const s = input.snapshot;
  const typeOf = (c: string): string | undefined => typeIn(s, c);
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(src.date))];
  if (marks.at !== undefined) conditions.push(fn('isNotEmpty', col(marks.at)));
  const filter = filterRead(src, input.filter);
  if (filter) conditions.push(filterNode(filter, 'x', typeOf));
  const match = matched(s, src.key, marks.where ?? []);
  if (match) conditions.push(match);
  const { at } = periodOf(ts.grain, src.date, typeOf(src.date));
  const out: ColSpec[] = [
    derive(TS_DATE, row(at)),
    ...(marks.at !== undefined ? [derive(TS_VALUE, row(col(marks.at)))] : []),
    ...(marks.text !== undefined ? [derive(TS_TEXT, row(col(marks.text)))] : []),
  ];
  // latest first, ties in an order of every column answered: the same rows past the cut whatever the database's own
  // order (the Phase 6 review: same-day rows under a frequency)
  const order = [desc(TS_DATE), ...(marks.at !== undefined ? [desc(TS_VALUE)] : []), ...(marks.text !== undefined ? [asc(TS_TEXT)] : [])];
  return sourceWithDerived(s).filter(row(and(...conditions))).extend(out).select(out.map((c) => c.name))
    .sort(order).limit(limit + 1).lambda();
}

/**
 * WHY A LEVEL'S FIGURES ARE BLANK (the second audit's P-2b and S1: a "—" said nothing), from its statistics' own answer --
 * its least over the dates shown (`low`, before compare took it from its start), its periods a year (`perYear`) and its
 * growth (`cagr`): at or below zero, none of its returns' figures; no frequency, no volatility or Sharpe ratio; under a
 * year, its growth not annualized. Each read off the database's answer, compared, never computed; undefined where its
 * answer has no such column. The one rule the viewer's statistics panel and Python's statistics say it by (the E9
 * review's 10: Python's had said nothing).
 */
export function statBlanks(name: string, f: { readonly low: number | null | undefined; readonly perYear: number | null | undefined; readonly cagr: number | null | undefined }): string[] {
  if (f.low !== undefined && f.low !== null && f.low <= 0) {
    return [`${name} is at or below zero over these dates: it has no volatility, drawdown, growth or Sharpe ratio, nor any figure of its returns' risk.`];
  }
  const out: string[] = [];
  if (f.perYear === null) out.push(`${name}: its dates are no frequency to annualize its volatility and Sharpe ratio by: choose the tile's frequency.`);
  // the database's growth blank above zero: these dates span under a year (the review's N3)
  if (f.low !== undefined && f.low !== null && f.cagr === null) {
    out.push(`${name}: these dates span under a year, so its growth is not annualized: its % change is its growth.`);
  }
  return out;
}

/**
 * THE DATES A TILE'S RANGE SHOWS (its window, a zoom aside): a range of the last of a period from the source with the
 * latest date of all -- found by comparing the dates the database gave in each source's bounds (boundsQuery), its start
 * for the range the database's -- or its own dates; every one for Max, no range, or a tile of no source of dates. The one
 * rule the viewer's window and Python's tables read (the E9 review's 7: Python's had read every date of a 1Y tile).
 */
export function rangeWindow(range: TsRange | undefined, bounds: Iterable<ResultTable>): TsWindow {
  if (!range) return {};
  if ('last' in range) {
    if (range.last === 'max') return {};
    // the bounds query writes both columns: one missing is a bug, never the whole range shown in its place (the second
    // audit's G notes); a source of no dates has no latest
    const value = (t: ResultTable, name: string): unknown => {
      const c = t.columns.find((x) => x.name === name);
      if (!c) throw new Error(`the tile's bounds answered with no ${name}`);
      return c.values[0] ?? null;
    };
    let best: ResultTable | undefined;
    let latest = '';
    for (const t of bounds) {
      const v = value(t, TS_LATEST);
      if (v === null) continue;
      const at = String(v);
      if (at > latest) {
        latest = at;
        best = t;
      }
    }
    if (!best) return {};
    const from = value(best, rangeStartName(range.last));
    return from === null ? {} : { from: String(from).slice(0, 10) };
  }
  return { ...(range.from !== undefined ? { from: range.from } : {}), ...(range.to !== undefined ? { to: range.to } : {}) };
}

/** A series' relation cut to the dates shown (`view`: from, to; either open): each period's value's date, its last
 *  observation (the review's Q6). */
function onlyShown(b: Built, view: TsWindow): void {
  const shown: ValueSpecification[] = [fn('isNotEmpty', col(b.value))];
  if (view.from !== undefined) shown.push(fn('greaterThanEqual', one(b.at), dateLiteral(view.from, b.periodType)));
  if (view.to !== undefined) shown.push(fn('lessThanEqual', one(b.at), dateLiteral(`${view.to}${isDay(b.periodType) ? '' : 'T23:59:59'}`, b.periodType)));
  b.writer.rel = b.writer.rel.filter(row(and(...shown)));
}

/** The columns a series' drawdowns query answers with, a row per drawdown: its peak's date, its trough's and its
 *  recovery's (the next date at or above the peak; none, not recovered), its depth (-0.2 for 20%), the days from the
 *  peak to the trough and from the trough to the recovery, and its rank among the line's (1 the deepest). */
export const TS_DD_PEAK = '__ts_dd_peak';
export const TS_DD_TROUGH = '__ts_dd_trough';
export const TS_DD_RECOVERED = '__ts_dd_recovered';
export const TS_DD_DEPTH = '__ts_dd_depth';
export const TS_DD_DECLINE = '__ts_dd_decline';
export const TS_DD_RECOVERY = '__ts_dd_recovery';
export const TS_DD_RANK = '__ts_dd_rank';
/** The most drawdowns a line's table lists: its deepest. */
export const MAX_DRAWDOWNS = 5;

/**
 * A LEVEL'S DEEPEST DRAWDOWNS over the dates shown, per line (the expert tables, 2026-10-26): each a run from a peak --
 * a date at or above every one before it shown -- to the next such date, its recovery (none: it has not recovered),
 * its trough the least between; the deepest MAX_DRAWDOWNS of the line's, deepest first. Every date, depth and length
 * the library's (drawdownsOfLine), the database's: the peaks a running maximum, each run numbered by a running count of
 * them, grouped, the next run's peak its recovery, ranked. Of the level itself, compared or not; a level at or below zero
 * has none.
 */
export function drawdownsQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, view: TsWindow = {}): TsQuery {
  const b = seriesRelation(inputs, ts, series, view.from);
  if (!b.level) throw new CubeRefusal(`series ${series.id} is no level: a return, a difference or a deviation has no drawdowns`);
  const w = b.writer;
  onlyShown(b, view);
  const lv = b.levelValue ?? b.value;
  // the library's (drawdownsOfLine): each run from a peak to the next, its trough, recovery and depth, the deepest
  // MAX_DRAWDOWNS a line -- its line the split's own column (a split's part is it)
  const out = [TS_DD_PEAK, TS_DD_TROUGH, TS_DD_RECOVERED, TS_DD_DEPTH, TS_DD_DECLINE, TS_DD_RECOVERY, TS_DD_RANK];
  let rel = w.rel.call(`${LIBRARY_PACKAGE}::drawdownsOfLine`, colSpec(w.part), colSpec(b.at), colSpec(lv), lit.integer(MAX_DRAWDOWNS), ...out.map((c) => colSpec(c)));
  if (b.split !== undefined) rel = rel.extend([derive(TS_SPLIT, row(col(b.split)))]);
  rel = rel.select([...(b.split !== undefined ? [TS_SPLIT] : []), ...out]).sort([...(b.split !== undefined ? [asc(TS_SPLIT)] : []), asc(TS_DD_RANK)]);
  return {
    series: series.id, query: rel.lambda(), unit: b.unit, band: false, ...(b.split !== undefined ? { split: b.split } : {}),
    level: true, counted: false, sources: b.sources,
  };
}

/** The columns a correlations query answers with, a row per pair of lines: each line's split value (none: the series'
 *  one line), their correlation over the dates both have shown, and how many dates that is. */
export const TS_CORR_A = '__ts_corr_a';
export const TS_CORR_B = '__ts_corr_b';
export const TS_CORR = '__ts_corr';
export const TS_CORR_N = '__ts_corr_n';

/**
 * THE CORRELATIONS OF TWO SERIES' LINES over the dates shown (the expert tables, 2026-10-26: the statistics panel's
 * Correlations tab): each line of `a` with each of `b` -- of one split series, each pair of its lines once -- their
 * correlation over the periods both have: a level's simple returns (gs-quant's correlation reads returns), anything
 * else as it is; each side cut to the dates shown, then its returns, then the two joined on the period and every pair
 * of lines aggregated -- the database's. A series with itself: each line with itself too (the matrix's diagonal).
 */
export function correlationsQuery(inputs: TsInputs, ts: TimeseriesSpec, a: TsSeries, b: TsSeries, view: TsWindow = {}): TsQuery {
  const names = { n: 0 };
  let periodType: string | undefined;
  const side = (s: TsSeries): { readonly w: Writer; readonly v: string; readonly sources: readonly string[] } => {
    const expr = candled(inlined(s.expr, ts.series));
    if (isBand(expr) || isCandles(expr) || isCondition(expr)) throw new CubeRefusal(`series ${s.id} is drawn, never correlated: a band, candles or a condition`);
    const read = sourcesOf(ts, expr);
    if (read.length === 0) throw new CubeRefusal(`series ${s.id} reads no column of a grid`);
    const sh: Shared = { ts, grain: ts.grain, series: s, inputs, split: s.split, first: read[0]!, anchor: view.from, periodType: undefined, names };
    const node = write(sh, expr);
    if ('constant' in node) throw new CubeRefusal(`series ${s.id} reads no column of a grid`);
    if (s.split !== undefined && !node.w.split) {
      throw new CubeRefusal(`series ${s.id} is split by ${s.split}, but reads one ${s.split}'s rows alone: a line per ${s.split} needs a column of every one`);
    }
    if (periodType === undefined) periodType = sh.periodType;
    else if (sh.periodType !== periodType) throw new CubeRefusal(`series ${a.id} and ${b.id} are of dates of two types: give the tile a frequency (Daily) to read both by day`);
    const w = node.w;
    const own = columnOfValue(node.v);
    const shown: ValueSpecification[] = [fn('isNotEmpty', col(own))];
    if (view.from !== undefined) shown.push(fn('greaterThanEqual', one(w.at), dateLiteral(view.from, sh.periodType!)));
    if (view.to !== undefined) shown.push(fn('lessThanEqual', one(w.at), dateLiteral(`${view.to}${isDay(sh.periodType!) ? '' : 'T23:59:59'}`, sh.periodType!)));
    w.rel = w.rel.filter(row(and(...shown)));
    return { w, v: levelOf(expr) ? w.simple(own) : own, sources: read };
  };
  const A = side(a);
  const la = names.n++;
  const pa = `__ts_corr_pa_${la}`;
  const va = `__ts_corr_va_${la}`;
  const left = A.w.rel.select([A.w.part, PERIOD, A.v]).rename(A.w.part, pa).rename(A.v, va);
  let pairs: Relation;
  let sources = A.sources;
  const bp = `__ts_corr_pb_${names.n}`;
  const bper = `__ts_corr_per_${names.n}`;
  const vb = `__ts_corr_vb_${names.n++}`;
  const on = (also: ValueSpecification[]): Lambda =>
    lambda(['l', 'r'], and(fn('equal', property(variable('l'), PERIOD), property(variable('r'), bper)), ...also));
  if (a.id === b.id) {
    // a series' lines against each other, each pair once (by their values' order), each with itself too: its diagonal
    // the database's, none where it has under two values or no variance (the E9 review's 13: a 1 written in its place)
    const right = A.w.rel.select([A.w.part, PERIOD, A.v]).rename(A.w.part, bp).rename(PERIOD, bper).rename(A.v, vb);
    pairs = left.join(right, 'INNER', on([fn('lessThanEqual', property(variable('l'), pa), property(variable('r'), bp))]));
  } else {
    const B = side(b);
    sources = [...new Set([...A.sources, ...B.sources])];
    const right = B.w.rel.select([B.w.part, PERIOD, B.v]).rename(B.w.part, bp).rename(PERIOD, bper).rename(B.v, vb);
    pairs = left.join(right, 'INNER', on([]));
  }
  // each pair of lines' correlation over the dates both have, the library's (correlationOf: of the pairs where both
  // have a value -- the E9 review's 11 --, none where a side does not vary; a line with itself 1 where it varies, the
  // E9 review's 13)
  let rel = pairs.call(`${LIBRARY_PACKAGE}::correlationOf`, colSpecs([pa, bp]), colSpec(va), colSpec(vb), colSpec(TS_CORR), colSpec(TS_CORR_N));
  // each line by its split value: a series of one line has none to say
  const lines = [...(a.split !== undefined ? [derive(TS_CORR_A, row(col(pa)))] : []), ...(b.split !== undefined ? [derive(TS_CORR_B, row(col(bp)))] : [])];
  if (lines.length > 0) rel = rel.extend(lines);
  rel = rel.select([...(a.split !== undefined ? [TS_CORR_A] : []), ...(b.split !== undefined ? [TS_CORR_B] : []), TS_CORR, TS_CORR_N]);
  return { series: a.id, query: rel.lambda(), unit: 'number', band: false, level: false, counted: false, sources };
}

// -- the views of a tile (the next phases' 3, 2026-10-26: a scatter, seasonality, a histogram, a curve, the calendar):
// each a query of its series' relations, every figure the database's --

/** The tile without its compare: a view that takes its own starts (a seasonality's years) or none (a calendar). */
function uncompared(ts: TimeseriesSpec): TimeseriesSpec {
  if (ts.view?.compare === undefined) return ts;
  const { compare: _taken, ...view } = ts.view;
  return { ...ts, view };
}

/** A window over every row of `rel` by a key every row has (`k`, a constant): the views' figures of the whole. */
const overAll = (k: string): AppliedFunction => fn('over', { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: [{ name: k }] } },
  collection([asc(PERIOD)]), fn('rows', fn('unbounded'), fn('unbounded')));

/** A series a view draws, as it is read: one line (a view of a split series' lines is a curve's), no band, no candles,
 *  no condition -- refused in words, by the view's name. */
function viewLine(inputs: TsInputs, ts: TimeseriesSpec, s: TsSeries, what: string, names: { n: number }, anchor?: string): Built {
  const expr = inlined(s.expr, ts.series);
  if (isBand(expr) || isCandles(expr) || isCondition(expr)) throw new CubeRefusal(`a ${what} draws a series' values: series ${s.id} is a band, candles or a condition`);
  if (s.split !== undefined) throw new CubeRefusal(`a ${what} is of one line: series ${s.id} is a line per ${s.split} -- read one ${s.split} (close[SPX])`);
  return seriesRelation(inputs, ts, s, anchor, names);
}

/** The columns a scatter answers with, a row per date both series have shown (its date, x and y), and on every row the
 *  regression of y on x over them: its slope, its intercept, R², the correlation, how many dates, and the fit at the
 *  least and the most x (the line drawn). */
export const TS_SC_X = '__ts_sc_x';
export const TS_SC_Y = '__ts_sc_y';
export const TS_SC_SLOPE = '__ts_sc_slope';
export const TS_SC_INTERCEPT = '__ts_sc_intercept';
export const TS_SC_R2 = '__ts_sc_r2';
export const TS_SC_CORR = '__ts_sc_corr';
export const TS_SC_N = '__ts_sc_n';
export const TS_SC_X_LOW = '__ts_sc_x_low';
export const TS_SC_X_HIGH = '__ts_sc_x_high';
export const TS_SC_FIT_LOW = '__ts_sc_fit_low';
export const TS_SC_FIT_HIGH = '__ts_sc_fit_high';

/**
 * A SCATTER OF TWO SERIES (Bloomberg's HRA, Macrobond's scatter): a point a date both have shown, y against x, each as
 * the series draws it -- and y's regression on x over those dates, ordinary least squares (the slope their sample
 * covariance over x's variance; the intercept through their means), its R² the correlation squared -- the database's.
 * None of the regression where x does not vary, or under two dates.
 */
export function scatterQuery(inputs: TsInputs, ts: TimeseriesSpec, x: TsSeries, y: TsSeries, view: TsWindow = {}): TsQuery {
  const names = { n: 0 };
  const X = viewLine(inputs, ts, x, 'scatter', names, view.from);
  const Y = viewLine(inputs, ts, y, 'scatter', names, view.from);
  if (X.periodType !== Y.periodType) throw new CubeRefusal(`series ${x.id} and ${y.id} are of dates of two types: give the tile a frequency (Daily) to read both by day`);
  onlyShown(X, view);
  onlyShown(Y, view);
  const fresh = (what: string): string => `__ts_sc_${what}_${names.n++}`;
  const yp = fresh('p');
  const left = X.writer.rel.select([PERIOD, X.at, X.value]).rename(X.at, TS_DATE).rename(X.value, TS_SC_X);
  const right = Y.writer.rel.select([PERIOD, Y.value]).rename(PERIOD, yp).rename(Y.value, TS_SC_Y);
  const k = fresh('k');
  let rel = left.join(right, 'INNER', lambda(['l', 'r'], fn('equal', property(variable('l'), PERIOD), property(variable('r'), yp))))
    .extend([derive(k, row(lit.integer(1)))]);
  const R = Writer.read;
  const all = (what: string, reduce: string, read: ValueSpecification): string => {
    const name = fresh(what);
    rel = rel.extend([agg(name, lambda(['p', 'w', 'r'], read), lambda(['y'], fn(reduce, variable('y'))))], overAll(k));
    return name;
  };
  const n = all('n', 'count', R(TS_SC_X));
  const lo = all('lo', 'min', R(TS_SC_X));
  const hi = all('hi', 'max', R(TS_SC_X));
  // y's regression on x over them, the library's (regression: ordinary least squares), and its fit at the least and the
  // most x (fitAt) -- the line drawn
  const L = (name: string): string => `${LIBRARY_PACKAGE}::${name}`;
  rel = rel.extend([derive(TS_SC_N, row(col(n))), derive(TS_SC_X_LOW, row(col(lo))), derive(TS_SC_X_HIGH, row(col(hi)))])
    .call(L('regression'), overAll(k), colSpec(TS_SC_Y), colSpec(TS_SC_X), lit.integer(2), colSpec(TS_SC_SLOPE), colSpec(TS_SC_INTERCEPT), colSpec(TS_SC_CORR), colSpec(TS_SC_R2))
    .call(L('fitAt'), colSpec(TS_SC_INTERCEPT), colSpec(TS_SC_SLOPE), colSpec(TS_SC_X_LOW), colSpec(TS_SC_FIT_LOW))
    .call(L('fitAt'), colSpec(TS_SC_INTERCEPT), colSpec(TS_SC_SLOPE), colSpec(TS_SC_X_HIGH), colSpec(TS_SC_FIT_HIGH));
  rel = rel.select([TS_DATE, TS_SC_X, TS_SC_Y, TS_SC_N, TS_SC_SLOPE, TS_SC_INTERCEPT, TS_SC_CORR, TS_SC_R2, TS_SC_X_LOW, TS_SC_X_HIGH, TS_SC_FIT_LOW, TS_SC_FIT_HIGH])
    .sort([asc(TS_DATE)]);
  return { series: y.id, query: rel.lambda(), unit: Y.unit, band: false, level: false, counted: false, sources: [...new Set([...X.sources, ...Y.sources])] };
}

/** The columns a projection answers with, one row: the fit at the first of the observations it is fit over and at the
 *  last (their dates), the projection's date `ahead` past the last and the fit there, the line's slope a day, and how
 *  many observations it is fit over. */
export const TS_PJ_FROM = '__ts_pj_from';
export const TS_PJ_FROM_VALUE = '__ts_pj_from_v';
export const TS_PJ_LAST = '__ts_pj_last';
export const TS_PJ_LAST_VALUE = '__ts_pj_last_v';
export const TS_PJ_END = '__ts_pj_end';
export const TS_PJ_END_VALUE = '__ts_pj_end_v';
export const TS_PJ_SLOPE = '__ts_pj_slope';
export const TS_PJ_N = '__ts_pj_n';

/**
 * A TREND PROJECTED (TsProjection; Phase 5): a series' line by ordinary least squares over its last `window`
 * observations -- its value on its time (each date's number of days, or of seconds for date-times), the slope their
 * sample covariance over time's variance, the intercept through their means -- and its fit at the first of them, at
 * the last, and at the date `ahead` past the last (`adjust`): the database's, every figure and every date. Of the line
 * as it is drawn along time: a series read from the first date shown (an index's, a change's) from `anchor`, as its line
 * is (the Phase 5 review's 1: it was fit on its values from its first date of all). Under a compare every line is from
 * the first date shown, so it is refused, as the drawing leaves it out. None of the fit under two observations of
 * different times.
 */
export function projectionQuery(inputs: TsInputs, ts: TimeseriesSpec, p: TsProjection, anchor?: string): TsQuery {
  const s = ts.series.find((x) => x.id === p.series);
  if (!s) throw new CubeRefusal(`a projection of series ${p.series}, which the tile has not`);
  if (ts.view?.compare !== undefined) throw new CubeRefusal('a projection is of a line\'s own values: under Compare its lines are from one start -- Compare off to project it');
  const ahead = tenorOf(p.ahead ?? TS_PROJECTION_AHEAD);
  if (ahead === undefined) throw new CubeRefusal(`a projection's ahead ${JSON.stringify(p.ahead)} is not a calendar period (3m)`);
  const b = viewLine(inputs, ts, s, 'projection', { n: 0 }, anchor);
  const w = b.writer;
  // the library's (projection), every figure of it: the line by least squares of its value on its time over its last
  // `window` values (two at least), its value at the first of them, at the last and at the date `ahead` past the last,
  // and its slope a day (a date-time's time counts seconds)
  const out = [TS_PJ_FROM, TS_PJ_FROM_VALUE, TS_PJ_LAST, TS_PJ_LAST_VALUE, TS_PJ_END, TS_PJ_END_VALUE, TS_PJ_SLOPE, TS_PJ_N];
  const rel = w.rel.call(`${LIBRARY_PACKAGE}::projection`, colSpec(w.part), colSpec(b.at), colSpec(b.ord), colSpec(b.value),
    lit.integer(p.window ?? TS_PROJECTION_WINDOW), lit.integer(ahead.n), enumValue(DURATION, ahead.unit), enumValue(DURATION, w.seconds ? 'SECONDS' : 'DAYS'),
    ...out.map((c) => colSpec(c))).select(out);
  return { series: s.id, query: rel.lambda(), unit: b.unit, band: false, level: b.level, counted: false, sources: b.sources };
}

/** The columns a seasonality answers with, a row per date of its last years: the date's year, its place in the year (a
 *  day of the year, a week, a month or a quarter by the tile's frequency), its value; and, at its place, the least, the
 *  most and the mean of the years before the latest -- with the latest year itself. */
export const TS_SEA_YEAR = '__ts_sea_year';
export const TS_SEA_PLACE = '__ts_sea_place';
export const TS_SEA_LOW = '__ts_sea_low';
export const TS_SEA_HIGH = '__ts_sea_high';
export const TS_SEA_MEAN = '__ts_sea_mean';
export const TS_SEA_LATEST = '__ts_sea_latest';
/** The years before's range at the place, its most less its least: what the drawing shades over its least. */
export const TS_SEA_SPAN = '__ts_sea_span';

/**
 * SEASONALITY (the commodities desk's chart, Bloomberg's SEAG): each of the last `years` years and the latest along its
 * calendar -- its place in the year by the tile's frequency -- and at each place the least, the most and the mean of the
 * years before the latest: the range the latest is read against. Compare (the view's) takes each year from its own start
 * (rebase 100, or its % change): the periods overlaid from their starts. The dates shown are its years', not the range's.
 */
export function seasonalityQuery(inputs: TsInputs, ts: TimeseriesSpec, s: TsSeries, years = ts.view?.years ?? TS_SEASON_YEARS): TsQuery {
  if (ts.grain === 'year') throw new CubeRefusal('a seasonality is of the places in a year: at a yearly frequency a year has one -- read it monthly, weekly or daily');
  const compare = ts.view?.compare;
  // the series as it is, each year taken from its own start below -- never from the window's
  const plain = uncompared(ts);
  const b = viewLine(inputs, plain, s, 'seasonality', { n: 0 });
  const w = b.writer;
  // the library's (seasonality), every figure of it: each date's year and its place in the year by the tile's frequency
  // (a daily series, or one of no frequency, by the week of the year, each year its week's last value -- a day of the
  // year held the years that traded that day alone: weekends and holidays apart year to year, a leap year a day off, the
  // Phase 3 review's 2), each year from its first value under Compare (100 there, x / x0 · 100, or x / x0 - 1: gs-quant's
  // index and percent_change, each year a line of its own), its last years, and each place's range over the years before
  // the latest
  const how = compare !== undefined && b.level ? compare : 'none';
  const rel = w.rel.call(`${LIBRARY_PACKAGE}::seasonality`, colSpec(w.part), colSpec(b.at), colSpec(ORD), colSpec(b.value),
    libraryOption('Grain', ts.grain ?? 'day'), libraryOption('Compare', how), lit.integer(years), colSpec(TS_SEA_YEAR), colSpec(TS_SEA_PLACE), colSpec(TS_VALUE),
    colSpec(TS_SEA_LOW), colSpec(TS_SEA_HIGH), colSpec(TS_SEA_MEAN), colSpec(TS_SEA_SPAN), colSpec(TS_SEA_LATEST))
    .extend([derive(TS_DATE, row(col(b.at)))])
    .select([TS_DATE, TS_SEA_YEAR, TS_SEA_PLACE, TS_VALUE, TS_SEA_LOW, TS_SEA_HIGH, TS_SEA_SPAN, TS_SEA_MEAN, TS_SEA_LATEST])
    .sort([asc(TS_SEA_YEAR), asc(TS_DATE)]);
  const unit = compare === 'rebase' && b.level ? 'index' : compare === 'percent' && b.level ? 'ratio' : b.unit;
  return { series: s.id, query: rel.lambda(), unit, band: false, level: b.level, counted: false, sources: b.sources };
}

/** The columns a histogram answers with, a row per bin a value fell in: its edges, how many fell in it, how many a
 *  normal of the values' mean and deviation would put there; and on every row the values' 5th percentile, mean,
 *  deviation and count. */
export const TS_HIST_LOW = '__ts_hist_low';
export const TS_HIST_HIGH = '__ts_hist_high';
/** A bin's middle: where its bar and its normal's count are drawn. */
export const TS_HIST_MID = '__ts_hist_mid';
export const TS_HIST_COUNT = '__ts_hist_count';
export const TS_HIST_NORMAL = '__ts_hist_normal';
export const TS_HIST_P5 = '__ts_hist_p5';
export const TS_HIST_MEAN = '__ts_hist_mean';
export const TS_HIST_SD = '__ts_hist_sd';
export const TS_HIST_N = '__ts_hist_n';

/**
 * A HISTOGRAM OF A SERIES' VALUES over the dates shown: `bins` of one width from its least to its most (the most in
 * the last), each counted; beside each, the count a normal of the same mean and sample deviation would give it (n times
 * the width times its density at the bin's middle); and the values' 5th percentile -- a return's historical value at
 * risk, marked -- every figure the database's.
 */
export function histogramQuery(inputs: TsInputs, ts: TimeseriesSpec, s: TsSeries, view: TsWindow = {}, bins = ts.view?.bins ?? TS_HIST_BINS): TsQuery {
  const b = viewLine(inputs, ts, s, 'histogram', { n: 0 }, view.from);
  const w = b.writer;
  onlyShown(b, view);
  // the library's (histogramOfLine): the bins, each counted, its normal's count, and the values' 5th percentile, mean,
  // deviation and count -- a row a bin
  const out = [TS_HIST_LOW, TS_HIST_MID, TS_HIST_HIGH, TS_HIST_COUNT, TS_HIST_NORMAL, TS_HIST_P5, TS_HIST_MEAN, TS_HIST_SD, TS_HIST_N];
  const rel = w.rel.call(`${LIBRARY_PACKAGE}::histogramOfLine`, colSpec(w.part), colSpec(b.value), lit.integer(bins), float(VAR_LEVEL), ...out.map((c) => colSpec(c)))
    .select(out).sort([asc(TS_HIST_LOW)]);
  return { series: s.id, query: rel.lambda(), unit: b.unit, band: false, level: b.level, counted: false, sources: b.sources };
}

/** The columns a curve answers with, a row per line (its split value, TS_SPLIT) and for each date asked (its place
 *  among them, `k`): the date as the database found it (the latest date, a period back from it, a day), the line's
 *  value as of then -- its last at or before -- and that value's own date. */
export const curveColumns = (k: number): { readonly asked: string; readonly value: string; readonly date: string } =>
  ({ asked: `__ts_curve_asked_${k}`, value: `__ts_curve_value_${k}`, date: `__ts_curve_date_${k}` });

/**
 * A CURVE (a yield curve, a futures strip): a split series' lines across its key -- its tenors -- as of each date asked:
 * `latest`, the latest date any line has; a period back from it (`1m`, `1y`); a day. Each line its last value at or
 * before the date, and the date it is of: one aggregate a line, every date asked beside the others -- the database's.
 */
export function curveQuery(inputs: TsInputs, ts: TimeseriesSpec, s: TsSeries, dates: readonly string[] = ts.view?.dates ?? TS_CURVE_DATES): TsQuery {
  if (s.split === undefined) throw new CubeRefusal(`a curve is of a line per value of a column (tenors): series ${s.id} is one line -- split it`);
  const expr = inlined(s.expr, ts.series);
  if (isBand(expr) || isCandles(expr) || isCondition(expr)) throw new CubeRefusal(`a curve draws a series' values: series ${s.id} is a band, candles or a condition`);
  // its lines as they are: the tile's compare is the time view's, never each line from its own first value (the Phase 3
  // review's 7)
  const b = seriesRelation(inputs, uncompared(ts), s, undefined);
  const w = b.writer;
  const split = b.split!;
  w.rel = w.rel.filter(row(fn('isNotEmpty', col(b.value))));
  const k = w.fresh('cv_k');
  const latest = w.fresh('cv_latest');
  w.rel = w.rel.extend([derive(k, row(lit.integer(1)))])
    .extend([agg(latest, lambda(['p', 'w', 'r'], Writer.read(b.at)), lambda(['y'], fn('max', variable('y'))))], overAll(k));
  const y = (f: string): Lambda => lambda(['y'], fn(f, variable('y')));
  const aggs = dates.flatMap((d, i) => {
    const t = tenorOf(d);
    // a day asked is the whole of it, on a source of date-times too (the Phase 3 review's 10: its own observations left out)
    const target = w.row('cv_t', d === 'latest' ? col(latest) : t !== undefined ? back(one(latest), t)
      : dateLiteral(isDay(b.periodType) ? d : `${d}T23:59:59`, b.periodType));
    const before = fn('lessThanEqual', one(b.at), one(target));
    const c = curveColumns(i);
    // each line's last value at or before it: the latest by its number in time (ORD: a maxBy's key is a number)
    return [
      agg(c.asked, row(col(target)), y('max')),
      agg(c.date, row(when(before, col(b.at), none())), y('max')),
      agg(c.value, row(fn('rowMapper', col(b.value), when(before, col(ORD), none()))), y('maxBy')),
    ];
  });
  const rel = w.rel.groupBy([split], aggs).extend([derive(TS_SPLIT, row(col(split)))])
    .select([TS_SPLIT, ...dates.flatMap((_, i) => { const c = curveColumns(i); return [c.asked, c.value, c.date]; })])
    .sort([asc(TS_SPLIT)]);
  return { series: s.id, query: rel.lambda(), unit: b.unit, band: false, level: b.level, split, counted: false, sources: b.sources };
}

/** A tenor as a curve's key spells it (`3M`, `10Y`, `1W`): its span in days, to order by (never drawn). */
const KEY_TENOR = /^(\d+(?:\.\d+)?)\s*([DWMY])$/i;
const TENOR_DAYS: Readonly<Record<string, number>> = { D: 1, W: 7, M: 30.4375, Y: 365.25 };
/** A money market's words for its shortest tenors, by their days: overnight, tom-next, spot-next. */
const KEY_WORDS: Readonly<Record<string, number>> = { ON: 1, 'O/N': 1, TN: 2, 'T/N': 2, SN: 3, 'S/N': 3 };

/**
 * A CURVE'S KEYS IN ORDER -- the one rule the screen's curve and Python's read (headless timeseriesCurveOrder): its
 * tenors (`3M`, `2Y`, `10Y`; a money market's `ON`, `TN`, `SN`) by their span, as a yield curve is read (3M before 2Y),
 * then any other key by its text, its numbers as numbers (2 before 10) -- a key no tenor never putting the tenors in
 * text order (the Phase 3 review's 8). The places of `keys`, in that order.
 */
export function curveOrder(keys: readonly string[]): number[] {
  const spans = keys.map((k) => {
    const word = KEY_WORDS[k.trim().toUpperCase()];
    if (word !== undefined) return word;
    const m = KEY_TENOR.exec(k.trim());
    return m ? Number(m[1]) * TENOR_DAYS[m[2]!.toUpperCase()]! : undefined;
  });
  return keys.map((_, i) => i).sort((a, b) => {
    const [x, y] = [spans[a], spans[b]];
    if (x !== undefined && y !== undefined && x !== y) return x - y;
    if ((x === undefined) !== (y === undefined)) return x === undefined ? 1 : -1;
    return keys[a]!.localeCompare(keys[b]!, UI_LOCALE, { numeric: true });
  });
}

/** The columns a calendar answers with, a row per month a line has: its year, its month, its return (from the month
 *  before's last level to its own), and its year's (from the year before's last to its own). */
export const TS_CAL_YEAR = '__ts_cal_year';
export const TS_CAL_MONTH = '__ts_cal_month';
export const TS_CAL_RETURN = '__ts_cal_return';
export const TS_CAL_YEAR_RETURN = '__ts_cal_year_return';
/** The line's least month-end level, on every row: a return from one at or below zero is none. */
export const TS_CAL_LOW = '__ts_cal_low';

/**
 * A LEVEL'S CALENDAR OF RETURNS (the monthly returns table every fund sheet has): each month's simple return, from the
 * month before's last level to its own last, and each year's, from the year before's last to its own -- the first month
 * and the first year none (no level before them). Of every date the line has, each line apart -- the database's.
 */
export function calendarQuery(inputs: TsInputs, ts: TimeseriesSpec, s: TsSeries): TsQuery {
  const expr = inlined(s.expr, ts.series);
  if (isBand(expr) || isCandles(expr) || isCondition(expr)) throw new CubeRefusal(`a calendar of returns is of a level: series ${s.id} is a band, candles or a condition`);
  // months of a level read at least monthly: a quarterly tile's quarters were written under months (the Phase 3 review's 1)
  if (ts.grain === 'quarter' || ts.grain === 'year') {
    throw new CubeRefusal(`a calendar of months reads a level at least monthly: this tile is ${ts.grain === 'quarter' ? 'quarterly' : 'yearly'} -- read it monthly, weekly or daily`);
  }
  const b = seriesRelation(inputs, uncompared(ts), s, undefined);
  if (!b.level) throw new CubeRefusal(`series ${s.id} is no level: a return, a difference or a deviation has no calendar of returns`);
  const w = b.writer;
  // the library's (calendarReturns): each month's return from the month before's last level, each year's from the year
  // before's, the line's least month-end level -- a row a line and month, its line the split's own column
  let rel = w.rel.call(`${LIBRARY_PACKAGE}::calendarReturns`, colSpec(w.part), colSpec(b.at), colSpec(ORD), colSpec(b.value),
    ...[TS_CAL_YEAR, TS_CAL_MONTH, TS_CAL_RETURN, TS_CAL_YEAR_RETURN, TS_CAL_LOW].map((c) => colSpec(c)));
  if (b.split !== undefined) rel = rel.extend([derive(TS_SPLIT, row(col(b.split)))]);
  rel = rel.select([...(b.split !== undefined ? [TS_SPLIT] : []), TS_CAL_YEAR, TS_CAL_MONTH, TS_CAL_RETURN, TS_CAL_YEAR_RETURN, TS_CAL_LOW])
    .sort([...(b.split !== undefined ? [asc(TS_SPLIT)] : []), asc(TS_CAL_YEAR), asc(TS_CAL_MONTH)]);
  return { series: s.id, query: rel.lambda(), unit: 'ratio', band: false, level: false, counted: false, sources: b.sources, ...(b.split !== undefined ? { split: b.split } : {}) };
}

/** The series a tile's views can draw: of one line drawn on an axis (a scatter's, a seasonality's, a histogram's), and
 *  split (a curve's) -- no band, no condition, no candles. Settings' pickers offer these alone. */
export function viewChoices(ts: TimeseriesSpec): { readonly oneLine: readonly string[]; readonly split: readonly string[] } {
  const drawable = tableSeries(ts).statistics.filter((id) => !isCandles(inlined(ts.series.find((x) => x.id === id)!.expr, ts.series)));
  const splitOf = (id: string): boolean => ts.series.find((x) => x.id === id)!.split !== undefined;
  return { oneLine: drawable.filter((id) => !splitOf(id)), split: drawable.filter(splitOf) };
}

/**
 * THE SERIES EACH VIEW DRAWS -- the one rule the screen and Python's views read: the view's `of` where it fits the view
 * (a scatter two series of one line, x then y -- one told, x, the next its y; a seasonality and a histogram one; a
 * curve a split series), else the tile's first it can draw; the calendar every level's but candles'. One `of` serves
 * every mode, so a mode never takes one it cannot draw (the Phase 3 review's 4: a scatter's x handed to a curve).
 * None where the tile has none to draw: said by the view.
 */
export function viewSeries(ts: TimeseriesSpec): {
  readonly scatter?: readonly [string, string]; readonly one?: string; readonly curve?: string; readonly calendar: readonly string[];
} {
  const { oneLine, split } = viewChoices(ts);
  const drawable = new Set([...oneLine, ...split]);
  const of = ts.view?.of ?? [];
  const x = of[0] !== undefined && oneLine.includes(of[0]) ? of[0] : undefined;
  const y = of[1] !== undefined && oneLine.includes(of[1]) ? of[1] : undefined;
  const pairOf: readonly [string, string] | undefined = x !== undefined && y !== undefined ? [x, y]
    : x !== undefined && oneLine.some((id) => id !== x) ? [x, oneLine.find((id) => id !== x)!]
      : oneLine.length >= 2 ? [oneLine[0]!, oneLine[1]!] : undefined;
  const oneOf = x ?? oneLine[0];
  const curveOf = of[0] !== undefined && split.includes(of[0]) ? of[0] : split[0];
  return {
    ...(pairOf !== undefined ? { scatter: pairOf } : {}), ...(oneOf !== undefined ? { one: oneOf } : {}), ...(curveOf !== undefined ? { curve: curveOf } : {}),
    // every level's but a candle's (its four prices no one level)
    calendar: tableSeries(ts).drawdowns.filter((id) => drawable.has(id)),
  };
}

/** The tables of a tile's series (the statistics panel's tabs, and Python's Timeseries tables), and its views'. */
export type TsTable = 'statistics' | 'drawdowns' | 'correlations' | 'scatter' | 'seasonality' | 'histogram' | 'curve' | 'calendar';

/**
 * THE SERIES EACH OF A TILE'S TABLES READS -- the one rule the screen's tabs and Python's tables read: the statistics,
 * every series drawn on an axis (no band, no condition: drawn, never figures of their own); the drawdowns, every
 * level's; the correlations, each pair of those drawn on an axis but candles, once, a split series with itself (its
 * lines against each other). A series that cannot be read (it reads itself, or one there is not) has none: it is said
 * where it is drawn.
 */
export function tableSeries(ts: TimeseriesSpec): { readonly statistics: readonly string[]; readonly drawdowns: readonly string[]; readonly correlations: readonly (readonly [string, string])[] } {
  const read = ts.series.flatMap((s) => {
    try {
      return [{ s, e: inlined(s.expr, ts.series) }];
    } catch (e) {
      if (e instanceof TsInlineError) return [];
      throw e;
    }
  });
  const onAxis = read.filter(({ e }) => !isBand(e) && !isCondition(e));
  const paired = onAxis.filter(({ e }) => !isCandles(e));
  return {
    statistics: onAxis.map(({ s }) => s.id),
    drawdowns: onAxis.filter(({ e }) => levelOf(e)).map(({ s }) => s.id),
    correlations: paired.flatMap(({ s }, i) => paired.slice(i).map(({ s: t }) => [s.id, t.id] as const)),
  };
}

/** Where an ask's answer is shown: the chart itself, or one of the statistics panel's tabs. */
export type TsServes = 'chart' | 'statistics' | 'drawdowns' | 'correlations' | 'calendar';

/** The words of where an answer is shown, for a person reading the queries. */
export const TS_SERVES_WORDS: Readonly<Record<TsServes, string>> = {
  chart: 'the chart', statistics: 'the Summary and Risk tabs', drawdowns: 'the Drawdowns tab',
  correlations: 'the Correlations tab', calendar: 'the Calendar tab',
};

/**
 * ONE QUESTION A TILE ASKS THE DATABASE: a source's bounds (its dates: the range buttons' starts), a series' points, or
 * one of its tables or views -- of a series (`series`), of a pair (`with`), or of a source (`source`) -- in words
 * (`what`), and where its answer is shown (`serves`).
 */
export interface TsAsk {
  readonly kind: 'bounds' | 'series' | 'marks' | 'projection' | TsTable;
  readonly series?: string;
  readonly with?: string;
  readonly source?: string;
  /** Marks': their place among the tile's annotations. */
  readonly marks?: number;
  /** A projection's: its place among the tile's annotations. */
  readonly projection?: number;
  readonly what: string;
  readonly serves: TsServes;
}

/** What an ask is of: what its query is built from, without its words. */
export type TsAskOf = Pick<TsAsk, 'kind' | 'series' | 'with' | 'source' | 'marks' | 'projection'>;

/** An ask's key: what it is of, the same for every round that asks it. */
export const askKey = (a: TsAskOf): string =>
  JSON.stringify([a.kind, a.series ?? null, a.with ?? null, a.source ?? null, ...(a.marks !== undefined ? [a.marks] : []),
    ...(a.projection !== undefined ? [a.projection] : [])]);

/** The marks an ask is of, by their place among the tile's annotations, or a refusal naming it. */
export function marksOf(ts: TimeseriesSpec, at: number | undefined): TsMarks {
  const a = at === undefined ? undefined : ts.annotations?.[at];
  if (a?.kind !== 'marks') throw new CubeRefusal(`the tile has no marks at ${JSON.stringify(at ?? null)}`);
  return a;
}

/** The projection an ask is of, by its place among the tile's annotations, or a refusal naming it. */
export function projectionOf(ts: TimeseriesSpec, at: number | undefined): TsProjection {
  const a = at === undefined ? undefined : ts.annotations?.[at];
  if (a?.kind !== 'projection') throw new CubeRefusal(`the tile has no projection at ${JSON.stringify(at ?? null)}`);
  return a;
}

/**
 * EVERY QUESTION A TILE ASKS THE DATABASE -- the one rule the screen's Query window and Python's Timeseries.queries
 * read (the user, 2026-10-26: "show all the pure queries used to create a timeseries chart"): to draw the chart, each
 * source's bounds, each series' points (a hidden one's too: its chip says it) and the view's (a mode's); for the
 * statistics panel, each line's statistics, each level's drawdowns, each pair's correlation and each level's calendar
 * of returns -- each by the rule its own drawing reads (tableSeries, viewSeries). Each ask's query is askQuery's.
 */
export function tileAsks(ts: TimeseriesSpec): TsAsk[] {
  const named = (id: string): string => {
    const s = ts.series.find((x) => x.id === id);
    return s === undefined ? id : `${id} (${seriesName(s)})`;
  };
  const tables = tableSeries(ts);
  const views = viewSeries(ts);
  const asks: TsAsk[] = [
    ...ts.sources.map((s): TsAsk => ({
      kind: 'bounds', source: s.alias, serves: 'chart',
      what: `Source ${s.alias}'s dates: its latest and earliest, where each range button starts`,
    })),
    ...ts.series.map((s): TsAsk => ({
      kind: 'series', series: s.id, serves: 'chart', what: `Series ${named(s.id)}: its points${s.hidden === true ? ' (hidden)' : ''}`,
    })),
    ...(ts.annotations ?? []).flatMap((a, i): TsAsk[] => (a.kind !== 'marks' ? [] : [{
      kind: 'marks', marks: i, source: a.source, serves: 'chart',
      what: `Marks ${a.label !== undefined ? `"${a.label}"` : i + 1}: source ${a.source}'s rows${a.at !== undefined ? ` at their ${a.at}` : ''}`,
    }])),
    // a projection of a line's own values: none under a compare (projectionQuery's refusal; the drawing says so)
    ...(ts.view?.compare !== undefined ? [] : (ts.annotations ?? []).flatMap((a, i): TsAsk[] => (a.kind !== 'projection' ? [] : [{
      kind: 'projection', projection: i, series: a.series, serves: 'chart',
      what: `Projection ${a.label !== undefined ? `"${a.label}"` : i + 1}: series ${named(a.series)}'s trend over its last ${a.window ?? TS_PROJECTION_WINDOW} observations, ${a.ahead ?? TS_PROJECTION_AHEAD} on`,
    }]))),
  ];
  const mode = ts.view?.mode;
  if (mode === 'scatter' && views.scatter !== undefined) {
    asks.push({ kind: 'scatter', series: views.scatter[0], with: views.scatter[1], serves: 'chart', what: `The scatter: ${named(views.scatter[0])} across, ${named(views.scatter[1])} up, and the fit` });
  } else if ((mode === 'seasonality' || mode === 'histogram') && views.one !== undefined) {
    asks.push({ kind: mode, series: views.one, serves: 'chart', what: `The ${mode} of ${named(views.one)}` });
  } else if (mode === 'curve' && views.curve !== undefined) {
    asks.push({ kind: 'curve', series: views.curve, serves: 'chart', what: `The curve of ${named(views.curve)} across its lines` });
  }
  asks.push(
    ...tables.statistics.map((id): TsAsk => ({ kind: 'statistics', series: id, serves: 'statistics', what: `Series ${named(id)}: its statistics over the dates shown` })),
    ...tables.drawdowns.map((id): TsAsk => ({ kind: 'drawdowns', series: id, serves: 'drawdowns', what: `Series ${named(id)}: its deepest drawdowns over the dates shown` })),
    ...tables.correlations.map(([a, b]): TsAsk => ({
      kind: 'correlations', series: a, with: b, serves: 'correlations',
      what: a !== b ? `The correlation of ${named(a)} and ${named(b)}`
        : ts.series.find((x) => x.id === a)?.split !== undefined ? `Series ${named(a)}: its lines' correlations with each other` : `Series ${named(a)}: its correlation with itself`,
    })),
    ...views.calendar.map((id): TsAsk => ({ kind: 'calendar', series: id, serves: 'calendar', what: `Series ${named(id)}: its returns each month and each year` })),
  );
  return asks;
}

/** The sources an ask reads, by alias: its series', and its pair's too. */
export function askSources(ts: TimeseriesSpec, ask: TsAskOf): string[] {
  if (ask.kind === 'bounds') return [ask.source!];
  if (ask.kind === 'marks') return [marksOf(ts, ask.marks).source];
  const of = (id: string | undefined): TsSeries[] => {
    if (id === undefined) return [];
    const s = ts.series.find((x) => x.id === id);
    if (!s) throw new Error(`the tile has no series ${id}`);
    return [s];
  };
  const read = [...new Set([...of(ask.series), ...of(ask.with)].flatMap((s) => seriesSources(ts, s)))];
  if (read.length === 0) throw new CubeRefusal(`series ${ask.series ?? ''} reads no column of a grid`);
  return read;
}

/**
 * AN ASK'S QUERY, as the screen asks it -- the one function the viewer's every round and Python's Timeseries.queries
 * build each query by: a series' points from the first date shown where it reads one (an index's base, compare's), every
 * date kept, the drawing's limit; a table or a view over the dates shown (`window`), where it reads them. A source's
 * bounds are boundsQuery's, of its one grid.
 */
export function askQuery(inputs: TsInputs, ts: TimeseriesSpec, ask: TsAskOf, window: TsWindow): TsQuery {
  const named = (id: string | undefined): TsSeries => {
    const s = id === undefined ? undefined : ts.series.find((x) => x.id === id);
    if (!s) throw new Error(`the tile has no series ${JSON.stringify(id ?? null)}`);
    return s;
  };
  if (ask.kind === 'bounds') throw new Error('a source\'s bounds are boundsQuery\'s, of its one grid');
  if (ask.kind === 'marks') throw new Error('marks are marksQuery\'s, of their one grid');
  const s = named(ask.series);
  switch (ask.kind) {
    case 'series':
      return seriesQuery(inputs, ts, s, readsAnchor(ts, s) && window.from ? { from: window.from } : {});
    case 'statistics':
      return statsQuery(inputs, ts, s, window);
    case 'drawdowns':
      return drawdownsQuery(inputs, ts, s, window);
    case 'correlations':
      return correlationsQuery(inputs, ts, s, named(ask.with), window);
    case 'scatter':
      return scatterQuery(inputs, ts, s, named(ask.with), window);
    case 'seasonality':
      return seasonalityQuery(inputs, ts, s);
    case 'histogram':
      return histogramQuery(inputs, ts, s, window);
    case 'curve':
      return curveQuery(inputs, ts, s);
    case 'calendar':
      return calendarQuery(inputs, ts, s);
    case 'projection':
      return projectionQuery(inputs, ts, projectionOf(ts, ask.projection), readsAnchor(ts, s) && window.from ? window.from : undefined);
  }
}

/**
 * A SERIES' STATISTICS over the dates shown (`view`: from, to; either open), per line: how many, the first and the last
 * date, the first and the last value and the change between them, the least and the most (and when), the mean and the
 * standard deviation; for a level, its annualized volatility (of its log returns, in percent), its deepest drawdown, its
 * compound annual growth and its Sharpe ratio (a risk-free rate of zero) -- every one the database's, over what is shown.
 */
export function statsQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, view: TsWindow = {}): TsQuery {
  const b = seriesRelation(inputs, ts, series, view.from);
  // the series' own writer: a writer of its own would name a lag of the statistics as the series named its own
  const w = b.writer;
  const v = b.value;
  // a level's volatility and Sharpe ratio, a difference's or a deviation's none (the review's Q2); annualized by the
  // periods a year of every date the series has (a tile of no frequency: inferred, before the window shown is cut)
  const level = b.level;
  const inferred = level && ts.grain === undefined;
  if (level) w.perYear();
  onlyShown(b, view);
  // a level's figures of its returns, its drawdown and its growth: of the level itself, compared or not -- the library's
  // (performanceStatistics) over the line as it is shown, one row a line: its volatility of its log returns, its Sharpe
  // ratio, its deepest drawdown, its compound annual growth (over a calendar year or more alone: a return of under a year
  // is never annualized -- the review's N3 -- its % change its growth, and the viewer says so), and its returns' risk --
  // their moments about their mean (skew, kurtosis), their downside (Sortino), their share above zero, their worst 5% (a
  // historical value at risk, the quantile the database interpolates, and the mean of the returns at or below it: an
  // expected shortfall), their best and worst -- each none of a level at or below zero anywhere shown, never one of the
  // part above zero, never a sign turned (the second audit's S1), and the viewer says why (its statistics' blanks: the
  // level's least, and the periods a year it annualized by, the call's own)
  const lv = b.levelValue ?? v;
  const S = statName;
  const key = b.split ?? w.part;
  const FIGURES: readonly TsStat[] = ['vol', 'sharpe', 'mdd', 'cagr', 'skew', 'kurtosis', 'sortino', 'hit', 'var', 'es', 'best', 'worst'];
  let figures: Relation | undefined;
  let figureLine: string | undefined;
  if (level) {
    const rLog = w.returns(lv, 1, 'log');
    const r = w.simple(lv);
    // its periods a year a column of it first (a frequency's a literal one): the call's table is the writer's after it
    const py = w.perYearColumn();
    figureLine = w.fresh('stat_fl');
    figures = w.rel.call(`${LIBRARY_PACKAGE}::performanceStatistics`, colSpec(w.part), colSpec(b.at), colSpec(b.ord), colSpec(lv), colSpec(r), colSpec(rLog),
      colSpec(py), float(VAR_LEVEL), ...FIGURES.map((st) => colSpec(S(st))), colSpec(TS_STAT_PER_YEAR), colSpec(TS_STAT_LEVEL_LOW)).rename(w.part, figureLine);
  }
  // the line's own figures, the library's (summaryStatistics), one row a line: a candle's least and most its lows' and
  // highs' (the review's Q9), a line's its values'; its days, change and % change (gs-quant's x / x0 - 1, as Compare's
  // and percent_change's: the user, 2026-10-28, "match gsquant", over the review's Q11)
  let rel = w.rel.call(`${LIBRARY_PACKAGE}::summaryStatistics`, colSpec(key), colSpec(b.at), colSpec(b.ord), colSpec(v), colSpec(b.low ?? v), colSpec(b.high ?? v),
    ...SUMMARY.map((st) => colSpec(S(st))));
  // the library's figures beside the line's own: one join of two aggregates of the line, each a row a line
  if (figures !== undefined) {
    rel = rel.join(figures, 'LEFT', lambda(['l', 'r'], fn('equal', property(variable('l'), key), property(variable('r'), figureLine!))));
  }
  // Calmar: its growth over its deepest drawdown's depth, the library's -- of a growth (a year or more) and a drawdown
  // below zero
  if (figures !== undefined) rel = rel.call(`${LIBRARY_PACKAGE}::calmarRatio`, colSpec(S('cagr')), colSpec(S('mdd')), colSpec(S('calmar')));
  const kept = [...TS_STATS.filter((st) => level || !LEVEL_STATS.includes(st)).map(S),
    ...(inferred ? [TS_STAT_PER_YEAR] : []), ...(level ? [TS_STAT_LEVEL_LOW] : [])];
  if (b.split !== undefined) rel = rel.extend([derive(TS_SPLIT, row(col(b.split)))]);
  rel = rel.select([...(b.split !== undefined ? [TS_SPLIT] : []), ...kept]);
  return {
    series: series.id,
    query: rel.lambda(),
    unit: b.unit,
    band: false,
    ...(b.split !== undefined ? { split: b.split } : {}),
    level: b.level,
    counted: false,
    sources: b.sources,
  };
}

/**
 * WHAT A SERIES' ROWS SAY A READER SHOULD KNOW, from its query's answer -- the one rule the viewer's drawing and Python's
 * frame say them by (the second audit's P-3: Python's frame dropped them): a period's value one of several rows at its
 * latest time, the database's to pick (the review's Q7); a line whose dates are no frequency to annualize by, by its
 * name (the review's Q3; the second audit's P-2a). `table`: its answer's columns, its counted rows and its periods a year
 * among them (a host may send those alone); `first`: the source it reads first.
 */
export function seriesNotes(ts: TimeseriesSpec, s: TsSeries, table: ResultTable, first: string | undefined): string[] {
  const notes: string[] = [];
  const column = (name: string): readonly unknown[] | undefined => table.columns.find((c) => c.name === name)?.values;
  const rows = column(TS_ROWS);
  if (rows && rows.some((n) => n !== null && Number(n) > 1)) {
    const key = ts.sources.find((x) => x.alias === first)?.key;
    notes.push(`${seriesName(s)}: several rows share a date's latest time, and one was taken -- match one (a column's [...])`
      + `${key !== undefined && s.split === undefined ? ` or split it by ${key}` : ''}, so each date has one.`);
  }
  const perYear = column(TS_PER_YEAR);
  if (perYear && perYear.length > 0) {
    const splits = column(TS_SPLIT);
    const lines = new Map<string, { split: unknown; annualized: boolean }>();
    perYear.forEach((n, i) => {
      const split = splits ? splits[i] ?? null : undefined;
      const k = split === undefined ? '' : JSON.stringify(split);
      const line = lines.get(k) ?? { split, annualized: false };
      if (n !== null) line.annualized = true;
      lines.set(k, line);
    });
    const none = [...lines.values()].filter((l) => !l.annualized).map((l) => splitName(s, l.split))
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    if (none.length > 0) {
      notes.push(`${none.join(', ')}: ${none.length === 1 ? 'its' : 'their'} dates are no frequency it can be annualized by (daily, weekly, monthly, quarterly, yearly): choose the tile's frequency.`);
    }
  }
  return notes;
}

/** Whether a series' expression reads the first date shown (an index, a change from the start, compare's rebasing). */
export function readsAnchor(ts: TimeseriesSpec, series: TsSeries): boolean {
  const walk = (e: TsExpr): boolean => e.kind === 'call'
    ? ['index', 'rebase', 'change', 'percent_change'].includes(e.fn) || e.args.some(walk)
    : e.kind === 'binary' ? walk(e.left) || walk(e.right) : false;
  // a series that cannot be inlined (one reading itself) is refused here, as where it is drawn
  const expr = inlined(series.expr, ts.series);
  return walk(expr) || (ts.view?.compare !== undefined && !isBand(expr) && !isCandles(expr) && levelOf(expr));
}

/** The most values the instrument picker lists. */
export const KEY_VALUES_LIMIT = 2000;

/**
 * THE VALUES OF A COLUMN, to pick an instrument from (close[...]): each once, in order, at most KEY_VALUES_LIMIT, from
 * the grid's rows as the chart reads them -- the database's distinct values, as the grid's Filter Values window asks
 * them.
 */
export function keyValuesQuery(input: TsInput, src: TsSource, column: string): Lambda {
  const s = input.snapshot;
  const typeOf = (c: string): string | undefined => typeIn(s, c);
  let rel = sourceWithDerived(s);
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(column))];
  const filter = filterRead(src, input.filter);
  if (filter) conditions.push(filterNode(filter, 'x', typeOf));
  rel = rel.filter(row(and(...conditions)))
    .groupBy([column], [agg('__ts_n', row(lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .sort([asc(column)])
    .limit(KEY_VALUES_LIMIT);
  return rel.select([column]).lambda();
}
