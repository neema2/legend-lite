// A TIMESERIES' QUERIES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5; docs/TS_LANGUAGE_BUILD_2026_10_30.md P6):
// each series of a timeseries tile (timeseries-spec.ts) as ONE query (D2) -- of its grid, or, a series combining grids,
// of their tables over one model -- which the planner plans and the DATABASE computes. Nothing here computes a value.
//
//   1. the series is its #TS{...}# query (timeseries-ts.ts writes it from the spec, gs-quant's vocabulary the tile's
//      surface): each source bound by `let` to its grid's rows (their source, row-level calculated columns and filter
//      as the tile reads it), the tile's frequency its SAMPLE BY, its alignment its FILL -- every function, window,
//      period and line meeting the platform's #TS builder's, each figure a call of the timeseries library
//   2. over its answer, the tile's own columns (tsWriter): its line (the split's first MAX_SPLIT_LINES values, else
//      one), its period at the tile's frequency, its number in time, the rows each date has (several rows on a date are
//      several points: the language's ruling 8, which the drawing says), the periods a year a figure was annualized by
//   3. the view's: a compare (rebased, a % change) from the first date shown, a condition's runs, the statistics, the
//      scatter, seasonality, histogram, curve and calendar of a series, each the library's over its answer
//   4. the period, the split and the value(s), latest first, at most TS_POINT_LIMIT
//
// Every series reads ALL its history (an index's from-date aside): the window shown is the viewer's, zoomed and panned over
// what came back, so a rolling figure at the window's left edge has its history, as a person expects it to.

import {
  agg, and, asc, collection, colSpec, derive, desc, enumValue, fn, from, lambda, lit, or, property, variable,
  type AppliedFunction, type ColSpec, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { TS_ITEM, tsIsland, tsLets, tsQueryText } from './timeseries-ts.ts';
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
  type TimeseriesSpec, type TsMarks, type TsProjection,
  type TsExpr, type TsMatch, type TsRange, type TsRangeWord, type TsSeries, type TsSource, type TsTenor,
  TS_CURVE_DATES, TS_HIST_BINS, TS_SEASON_YEARS,
} from './timeseries-spec.ts';
import { inlined, isBand, isCandles, isCondition, levelOf, TsInlineError, seriesName, sourceAlias, sourcesOf, splitName, unitOf, type TsUnit } from './timeseries-expr.ts';
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
  /** Its sources bound by `let`, which its query's body starts with (queryOf). */
  readonly lets: readonly ValueSpecification[];
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


/** The column of a series' period (every window's order, the key two series are joined on), and its number in time. */
const PERIOD = '__ts_p0';
const ORD = '__ts_ord';

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
   * A BASE OVER THE WHOLE LINE, the library's -- `index`, `change` or `percentChange` of the line's first value, or,
   * with the first date the view shows (the tile's anchor), its `...From` of the value as of it (`valueAt`) -- `args`
   * after the date.
   */
  libraryBased(name: 'index' | 'change' | 'percentChange', what: string, value: string, args: readonly ValueSpecification[] = []): string {
    const anchor = this.shared.anchor;
    if (anchor === undefined) return this.libraryOfLine(name, what, value, args);
    return this.libraryOfLine(`${name}From`, what, value, [dateLiteral(anchor, this.shared.periodType!), ...args]);
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

  /** A column's value on a frame's rows, as a window reads it. */
  static read(column: string): ValueSpecification {
    return property(variable('r'), column);
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
  const expr = inlined(series.expr, ts.series);
  const read = sourcesOf(ts, expr);
  if (read.length === 0) throw new CubeRefusal(`series ${series.id} reads no column of a grid`);
  // `names`: a query of two series' relations names their columns apart (a scatter's)
  const sh: Shared = { ts, grain: ts.grain, series, inputs, split: series.split, first: read[0]!, anchor, periodType: undefined, names };
  // every source's dates of one type (a combination of a date's and a date-time's needs a frequency)
  for (const alias of read) {
    const { src, input } = sourceNamed(ts, inputs, alias);
    periodTyped(sh, periodOf(ts.grain, src.date, typeIn(input.snapshot, src.date)).type, alias);
  }
  const first = sourceNamed(ts, inputs, read[0]!).src;
  const split = series.split;
  if (split !== undefined && !readsEvery(ts, expr, first, split)) {
    throw new CubeRefusal(`series ${series.id} is split by ${split}, but reads one ${split}'s rows alone: a line per ${split} needs a column of every one`);
  }
  const w = tsWriter(sh, expr, first, read);
  const top = { v: w.answered };
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
    lets: w.lets,
  };
}

/**
 * A SERIES AS THE TIMESERIES LANGUAGE ANSWERS IT (docs/TS_LANGUAGE_BUILD_2026_10_30.md P6): its #TS{...}# query
 * (timeseries-ts.ts, every figure the platform's #TS builder's and the library's), each source bound by `let` -- and
 * over its answer the tile's own columns: its line (the split, else one), its period at the tile's frequency, its
 * number in time, the rows a date has (ruling 8: several rows on a date are several points, which the drawing says),
 * and the periods a year a figure was annualized by. The answer keeps every date the series' line has, a value or none
 * (the item `present`, never none: R6 keeps a row where an item has a value).
 */
function tsWriter(sh: Shared, expr: TsExpr, first: TsSource, read: readonly string[]): Writer & { readonly answered: Value; readonly lets: readonly ValueSpecification[] } {
  const island = tsIsland(tsQueryText(sh.ts, sh.series, sh.anchor, true));
  const lets = tsLets(sh.inputs, sh.ts, read);
  const at = freshName(sh, 'at');
  const fieldsOf = isBand(expr) ? ['lower', 'upper', 'width'] : isCandles(expr) ? ['open', 'high', 'low', 'close'] : [];
  const items = fieldsOf.length === 0 ? [TS_ITEM] : fieldsOf.map((f) => `${TS_ITEM}_${f}`);
  const own = items.map(() => freshName(sh, 'v'));
  let rel = from(island).rename(first.date, at);
  items.forEach((c, i) => { rel = rel.rename(c, own[i]!); });
  const split = sh.split;
  const part = split ?? freshName(sh, 'line');
  if (split === undefined) rel = rel.extend([derive(part, row(lit.string('all')))]);
  // its period at the tile's frequency, its number in time (a day's, or a date-time's second of no frequency)
  const grain = sh.grain;
  const seconds = grain === undefined && !isDay(sh.periodType!);
  rel = rel.extend([
    derive(PERIOD, row(grain === undefined || grain === 'day' ? col(at) : fn(GRAIN_FUNCTION[grain], one(at)))),
    derive(ORD, row(fn('dateDiff', literalNode('1970-01-01', 'StrictDate'), one(at), enumValue(DURATION, seconds ? 'SECONDS' : 'DAYS')))),
  ]);
  const rows: string[] = [];
  if (grain === undefined) {
    // the rows the line has on the date (ruling 8): more than one, and the drawing says so
    const n = freshName(sh, 'rows');
    rel = rel.extend([agg(n, lambda(['p', 'w', 'r'], Writer.read(at)), lambda(['y'], fn('count', variable('y'))))], fn('over', colSpecs([part, at])));
    rows.push(n);
  }
  const w = new Writer(rel, { part, split: split !== undefined, at, seconds, rows, years: [] }, sh);
  // the periods a year a figure was annualized by: the tile's frequency's, else those its dates infer
  if (annualizes(expr)) w.perYear();
  const answered: Value = fieldsOf.length === 0 ? { column: own[0]! } : fieldsOf.length === 3
    ? { lower: own[0]!, upper: own[1]!, width: own[2]! } : { open: own[0]!, high: own[1]!, low: own[2]!, close: own[3]! };
  return Object.assign(w, { answered, lets });
}

/** The functions whose figures are annualized by a line's periods a year (the library's perYear argument). */
const ANNUALIZED: ReadonlySet<string> = new Set(['annualize', 'volatility', 'sharpe_ratio', 'alpha', 'tracking_error',
  'information_ratio', 'exponential_volatility', 'exponential_spread_volatility']);

function annualizes(e: TsExpr): boolean {
  switch (e.kind) {
    case 'call': return ANNUALIZED.has(e.fn) || e.args.some(annualizes);
    case 'binary': return annualizes(e.left) || annualizes(e.right);
    default: return false;
  }
}

/** Whether `e` reads a column of `src` of every value of `split` -- one its matches pick no one value of (matchesSplit). */
function readsEvery(ts: TimeseriesSpec, e: TsExpr, src: TsSource, split: string): boolean {
  switch (e.kind) {
    case 'column':
      return sourceAlias(ts, e) === src.alias && !(e.where ?? []).some((m) => {
        const c = m.column ?? src.key;
        return c === split || (src.key !== undefined && c === src.key);
      });
    case 'call': return e.args.some((a) => readsEvery(ts, a, src, split));
    case 'binary': return readsEvery(ts, e.left, src, split) || readsEvery(ts, e.right, src, split);
    default: return false;
  }
}

/** The query of `rel`, a relation over the series' answers: each source's `let` (once, a combination's two series
 *  sharing their sources), then `rel`. */
function queryOf(rel: Relation, ...of: readonly Built[]): Lambda {
  const lets = new Map<string, ValueSpecification>();
  for (const b of of) for (const l of b.lets) lets.set(JSON.stringify((l as AppliedFunction).parameters[0]), l);
  return lambda([], ...lets.values(), rel.node);
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
    query: queryOf(rel, b),
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
    series: series.id, query: queryOf(rel, b), unit: b.unit, band: false, ...(b.split !== undefined ? { split: b.split } : {}),
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
  const side = (s: TsSeries): { readonly w: Writer; readonly v: string; readonly sources: readonly string[]; readonly b: Built } => {
    const expr = inlined(s.expr, ts.series);
    if (isBand(expr) || isCandles(expr) || isCondition(expr)) throw new CubeRefusal(`series ${s.id} is drawn, never correlated: a band, candles or a condition`);
    const built = seriesRelation(inputs, ts, s, view.from, names);
    if (periodType === undefined) periodType = built.periodType;
    else if (built.periodType !== periodType) throw new CubeRefusal(`series ${a.id} and ${b.id} are of dates of two types: give the tile a frequency (Daily) to read both by day`);
    const w = built.writer;
    // the series as it is, before a compare took it from its start
    const own = built.levelValue ?? built.value;
    const shown: ValueSpecification[] = [fn('isNotEmpty', col(own))];
    if (view.from !== undefined) shown.push(fn('greaterThanEqual', one(w.at), dateLiteral(view.from, built.periodType)));
    if (view.to !== undefined) shown.push(fn('lessThanEqual', one(w.at), dateLiteral(`${view.to}${isDay(built.periodType) ? '' : 'T23:59:59'}`, built.periodType)));
    w.rel = w.rel.filter(row(and(...shown)));
    return { w, v: levelOf(expr) ? w.simple(own) : own, sources: built.sources, b: built };
  };
  const A = side(a);
  const la = names.n++;
  const pa = `__ts_corr_pa_${la}`;
  const va = `__ts_corr_va_${la}`;
  const left = A.w.rel.select([A.w.part, PERIOD, A.v]).rename(A.w.part, pa).rename(A.v, va);
  let pairs: Relation;
  let sources = A.sources;
  let builts: Built[] = [A.b];
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
    builts = [A.b, B.b];
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
  return { series: a.id, query: queryOf(rel, ...builts), unit: 'number', band: false, level: false, counted: false, sources };
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
  return { series: y.id, query: queryOf(rel, X, Y), unit: Y.unit, band: false, level: false, counted: false, sources: [...new Set([...X.sources, ...Y.sources])] };
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
  return { series: s.id, query: queryOf(rel, b), unit: b.unit, band: false, level: b.level, counted: false, sources: b.sources };
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
  return { series: s.id, query: queryOf(rel, b), unit, band: false, level: b.level, counted: false, sources: b.sources };
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
  return { series: s.id, query: queryOf(rel, b), unit: b.unit, band: false, level: b.level, counted: false, sources: b.sources };
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
  return { series: s.id, query: queryOf(rel, b), unit: b.unit, band: false, level: b.level, split, counted: false, sources: b.sources };
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
  return { series: s.id, query: queryOf(rel, b), unit: 'ratio', band: false, level: false, counted: false, sources: b.sources, ...(b.split !== undefined ? { split: b.split } : {}) };
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
    query: queryOf(rel, b),
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
