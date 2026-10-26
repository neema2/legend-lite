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
  agg, and, asc, collection, derive, desc, enumValue, fn, lambda, lit, or, property, variable,
  type AppliedFunction, type ColSpec, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { filterNode, literalNode, sourceWithDerived } from './query.ts';
import { columnType, CubeRefusal, type CubeSnapshot, type FilterNode } from './snapshot.ts';
import { WINDOW_ALL_PREFIX } from './grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { isDay, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import {
  filterRead,
  MAX_SPLIT_LINES, PERIODS_PER_YEAR, RANGE_STEP, tenorOf, TS_POINT_LIMIT, TS_RANGES, type TimeseriesSpec, type TsAggregate,
  type TsExpr, type TsMatch, type TsRangeWord, type TsSeries, type TsSource, type TsTenor,
} from './timeseries-spec.ts';
import { inlined, isBand, isCandles, levelOf, paramOf, seriesName, sourceAlias, sourcesOf, splitName, unitOf, type TsUnit } from './timeseries-expr.ts';
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
  'count', 'from', 'to', 'first', 'last', 'change', 'pct', 'low', 'lowAt', 'high', 'highAt', 'mean', 'std', 'vol', 'mdd',
  'cagr', 'sharpe',
] as const;
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
/** A window's frame: running from the start, every period, the last n observations, or a calendar period. */
type Frame = 'running' | 'all' | number | TsTenor;
const isTenor = (f: Frame): f is TsTenor => typeof f === 'object';
/** A span of at least `k` observations (a deviation needs two); a calendar period as it is. */
const atLeast = (s: Span, k: number): Span => (typeof s === 'number' ? Math.max(s, k) : s);
/** `adjust(<date>, -n, <unit>)`: a calendar period before a date, the database's date arithmetic. */
const back = (date: ValueSpecification, t: TsTenor): ValueSpecification => fn('adjust', date, lit.integer(-t.n), enumValue(DURATION, t.unit));

/** A frame's read (`$r.c`, `rowMapper($r.a, $r.b)`) as a row of a self-join reads it: each `$r.c` the joined row's column
 *  `c` is under (`$x.<names[c]>`). */
function readOver(read: ValueSpecification, names: ReadonlyMap<string, string>): ValueSpecification {
  switch (read._type) {
    case 'property': {
      const on = read.parameters[0];
      if (read.parameters.length === 1 && on?._type === 'var' && on.name === 'r') {
        const name = names.get(read.property);
        if (name === undefined) throw new Error(`a window reads ${read.property}, which its self-join does not carry`);
        return property(x(), name);
      }
      return { ...read, parameters: read.parameters.map((v) => readOver(v, names)) };
    }
    case 'func':
      return { ...read, parameters: read.parameters.map((v) => readOver(v, names)) };
    case 'collection':
      return { ...read, values: read.values.map((v) => readOver(v, names)) };
    default:
      return read;
  }
}

/** The columns a frame's read reads of its rows (`$r.c`), each once. */
function rowColumnsOf(read: ValueSpecification): string[] {
  const out = new Set<string>();
  const walk = (v: ValueSpecification): void => {
    if (v._type === 'property') {
      const on = v.parameters[0];
      if (v.parameters.length === 1 && on?._type === 'var' && on.name === 'r') out.add(v.property);
      else v.parameters.forEach(walk);
    } else if (v._type === 'func') v.parameters.forEach(walk);
    else if (v._type === 'collection') v.values.forEach(walk);
  };
  walk(read);
  return [...out];
}

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
  #asOf: string | undefined;
  #perYear: string | undefined;

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

  /** `->extend(~[name: x|body])`. */
  row(what: string, body: ValueSpecification): string {
    const name = this.fresh(what);
    this.rel = this.rel.extend([derive(name, row(body))]);
    return name;
  }

  /** The window every function of the part runs over: its line's partition, the periods in order, a frame. */
  #over(frame: 'running' | 'all' | number | null): AppliedFunction {
    const args: ValueSpecification[] = [
      { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: [{ name: this.part }] } },
      collection([asc(PERIOD)]),
    ];
    if (frame === 'running') args.push(fn('rows', fn('unbounded'), lit.integer(0)));
    else if (frame === 'all') args.push(fn('rows', fn('unbounded'), fn('unbounded')));
    else if (frame !== null) args.push(fn('rows', lit.integer(-(Math.max(1, frame) - 1) || 0), lit.integer(0)));
    return fn('over', ...args);
  }

  /** An aggregate over a frame of the periods: `extend(over(...), ~[name:{p,w,r|<read>}:y|$y-><reduce>()])` -- of a
   *  calendar period, over the periods whose dates are within it (#calendar). */
  window(what: string, reduce: string, read: ValueSpecification, frame: Frame): string {
    if (isTenor(frame)) return this.#calendar(what, reduce, read, frame);
    const name = this.fresh(what);
    this.rel = this.rel.extend([agg(name, lambda(['p', 'w', 'r'], read), lambda(['y'], fn(reduce, variable('y'))))], this.#over(frame));
    return name;
  }

  /**
   * A WINDOW OF A CALENDAR PERIOD (gs-quant's `Window('3m')`, helper.rolling_offset: the dates after one that far back,
   * to this one): its line's periods joined to every one of its own whose date is in (adjust(date, -n, unit), date], the
   * read aggregated over each period's, and joined back by the period -- a self-join, the database's, since a window
   * frame counts rows, never dates.
   */
  #calendar(what: string, reduce: string, read: ValueSpecification, t: TsTenor): string {
    const name = this.fresh(what);
    const from = this.fresh('cal_from');
    const read_ = rowColumnsOf(read);
    const names = new Map(read_.map((c) => [c, this.fresh('cal_r')] as const));
    const rp = this.fresh('cal_rp');
    const ra = this.fresh('cal_ra');
    let right = this.rel.select([this.part, this.at, ...read_.filter((c) => c !== this.part && c !== this.at)]);
    // a read of the part or the date itself (a count of dates) reads the joined row's own
    right = right.rename(this.part, rp).rename(this.at, ra);
    const as = new Map(names);
    if (read_.includes(this.part)) as.set(this.part, rp);
    if (read_.includes(this.at)) as.set(this.at, ra);
    for (const c of read_) if (c !== this.part && c !== this.at) right = right.rename(c, names.get(c)!);
    const left = this.rel.select([this.part, PERIOD, this.at]).extend([derive(from, row(back(one(this.at), t)))]);
    const inWindow = lambda(['l', 'r'], and(
      fn('equal', property(variable('l'), this.part), property(variable('r'), rp)),
      fn('greaterThan', property(variable('r'), ra), property(variable('l'), from)),
      fn('lessThanEqual', property(variable('r'), ra), property(variable('l'), this.at)),
    ));
    const gp = this.fresh('cal_gp');
    const gper = this.fresh('cal_gper');
    const grouped = left.join(right, 'INNER', inWindow)
      .groupBy([this.part, PERIOD], [agg(name, row(readOver(read, as)), lambda(['y'], fn(reduce, variable('y'))))])
      .rename(this.part, gp).rename(PERIOD, gper);
    this.rel = this.rel.join(grouped, 'LEFT', lambda(['l', 'r'], and(
      fn('equal', property(variable('l'), this.part), property(variable('r'), gp)),
      fn('equal', property(variable('l'), PERIOD), property(variable('r'), gper)),
    )));
    return name;
  }

  /** A column `n` periods back: `$p->lag($r, n).<column>` -- or a calendar period back, as of that date (#valueBack). */
  lag(column: string, n: Span): string {
    if (typeof n !== 'number') return this.#valueBack(column, n);
    const name = this.fresh('lag');
    const call = fn('lag', variable('p'), variable('r'), ...(n !== 1 ? [lit.integer(n)] : []));
    this.rel = this.rel.extend([derive(name, lambda(['p', 'w', 'r'], property(call, column)))], this.#over(null));
    return name;
  }

  /**
   * A COLUMN AS OF A CALENDAR PERIOD BACK (gs-quant's lag by a tenor, as of): of its line's periods where it has a
   * value, the latest whose date is at or before adjust(date, -n, unit) -- an as-of join, the database's; none before
   * the line's first date that far back.
   */
  #valueBack(column: string, t: TsTenor): string {
    const target = this.fresh('back_at');
    const lp = this.fresh('back_part');
    const la = this.fresh('back_date');
    const value = this.fresh('back');
    const right = this.rel.filter(row(fn('isNotEmpty', col(column)))).select([this.part, this.at, column])
      .rename(this.part, lp).rename(this.at, la).rename(column, value);
    this.rel = this.rel.extend([derive(target, row(back(one(this.at), t)))]).asOfJoin(right,
      lambda(['l', 'r'], fn('greaterThanEqual', property(variable('l'), target), property(variable('r'), la))),
      lambda(['l', 'r'], fn('equal', property(variable('l'), this.part), property(variable('r'), lp))));
    return value;
  }

  /** A column's value on a frame's rows, as a window reads it. */
  static read(column: string): ValueSpecification {
    return property(variable('r'), column);
  }

  /** `w` periods of `column` (0: from the start), or a calendar period: its frame. */
  static frame(w: Span): 'running' | number | TsTenor {
    return w === 0 ? 'running' : w;
  }

  /**
   * A rolling figure drawn once its window holds `need` of `input`'s values: empty before (gs-quant's ramp), counted
   * over the figure's own frame -- the last `w`, or from the start (w 0). Of a calendar period, from `input`'s first date
   * that period on (gs-quant's apply_ramp of a DateOffset: x.loc[x.index[0] + r:]).
   */
  ramped(value: string, input: string, need: Span, frame: 'running' | number | TsTenor = need): string {
    if (typeof need !== 'number') {
      const first = this.window('ramp_first', 'min', Writer.read(this.row('ramp_at', when(fn('isNotEmpty', col(input)), col(this.at), none()))), 'all');
      return this.row('ramped', when(fn('lessThan', one(this.at), fn('adjust', one(first), lit.integer(need.n), enumValue(DURATION, need.unit))), none(), col(value)));
    }
    if (need <= 1) return value;
    const count = this.window('n', 'count', Writer.read(input), frame);
    return this.row('ramped', when(fn('lessThan', one(count), lit.integer(need)), none(), col(value)));
  }

  /** `a / b`, empty where b is zero. */
  ratio(what: string, a: ValueSpecification, b: string): string {
    return this.row(what, when(fn('equal', one(b), lit.integer(0)), none(), fn('divide', a, one(b))));
  }

  /**
   * THE VALUE AS OF THE FIRST DATE SHOWN, beside every row: of the periods where `column` has a value, the latest whose
   * value is of a date at or before the anchor (the viewer's left edge) -- a year's change from the last close before
   * it, a YTD from the year before's last -- or, the series starting after the anchor (or no anchor), its first; the
   * period the database's, and the value there, each a window over the whole partition. A week or a month is compared
   * by the date its value is of, its last observation (the review's Q6; the second audit's S3: a range starting just
   * after a month's last observation lost that month's move).
   */
  anchored(column: string): string {
    const anchor = this.shared.anchor;
    const has = fn('isNotEmpty', col(column));
    const first = this.window('anchor_first', 'min', Writer.read(this.row('anchor_any', when(has, col(PERIOD), none()))), 'all');
    if (anchor === undefined) return this.#valueAt(column, first);
    if (this.#asOf === undefined) {
      this.#asOf = this.row('asof', fn('lessThanEqual', one(this.at), dateLiteral(anchor, this.shared.periodType!)));
    }
    const before = this.window('anchor_before', 'max', Writer.read(this.row('anchor_asof', when(and(one(this.#asOf), has), col(PERIOD), none()))), 'all');
    return this.#valueAt(column, this.row('anchor_period', when(fn('isEmpty', col(before)), col(first), col(before))));
  }

  /** `column`'s value at the period `at` names, beside every row of the partition. */
  #valueAt(column: string, at: string): string {
    const value = this.row('anchor_mark', when(fn('equal', col(PERIOD), col(at)), col(column), none()));
    return this.window('anchor', 'max', Writer.read(value), 'all');
  }

  /** Returns over `obs` periods (or a calendar period back, as of its date), of a type (gs-quant's econometrics.returns). */
  returns(a: string, obs: Span, type: string): string {
    const p = this.lag(a, obs);
    switch (type) {
      case 'log':
        return this.row('ret', when(or(fn('lessThanEqual', one(a), lit.integer(0)), fn('lessThanEqual', one(p), lit.integer(0))), none(),
          op('minus', fn('log', one(a)), fn('log', one(p)))));
      case 'absolute':
        return this.row('ret', op('minus', one(a), one(p)));
      case 'simple':
        return this.row('ret', when(fn('equal', one(p), lit.integer(0)), none(), op('minus', fn('divide', one(a), one(p)), lit.integer(1))));
      default:
        throw new CubeRefusal(`there is no ${JSON.stringify(type)} return (simple, log or absolute)`);
    }
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
      const hi = this.window('ord_hi', 'max', Writer.read(ORD), 'all');
      const lo = this.window('ord_lo', 'min', Writer.read(ORD), 'all');
      const n = this.window('ord_n', 'count', Writer.read(ORD), 'all');
      const span = this.seconds ? fn('divide', op('minus', one(hi), one(lo)), lit.integer(86400)) : op('minus', one(hi), one(lo));
      const gap = this.row('gap', when(fn('lessThan', one(n), lit.integer(2)), none(), fn('divide', span, op('minus', one(n), lit.integer(1)))));
      const g = one(gap);
      const within = (from: number, to: number): ValueSpecification =>
        and(fn('greaterThanEqual', g, float(from)), fn('lessThan', g, float(to)));
      const buckets: readonly [number, number, number][] = [[1, 2.1, 252], [6, 8, 52], [14, 17, 26], [25, 35, 12], [85, 97, 4], [360, 386, 1]];
      const factor = buckets.reduceRight<ValueSpecification>((otherwise, [from, to, n]) => when(within(from, to), lit.integer(n), otherwise), none());
      this.#perYear = this.row('per_year', factor);
      this.years.push(this.#perYear);
    }
    return one(this.#perYear);
  }

  /** The square root of the periods in a year, as the database takes it. */
  annual(): ValueSpecification {
    return fn('sqrt', this.perYear());
  }

  /** A sample standard deviation over `w` periods (0 from the start, or a calendar period), drawn once its window holds
   *  two. */
  std(a: string, w: Span): string {
    return this.ramped(this.window('std', 'stdDevSample', Writer.read(a), Writer.frame(w)), a, atLeast(w, 2), Writer.frame(w));
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

/** An expression with each candle's (ohlc's) plain columns taken by their date as a candle's are (CANDLE) -- unless one
 *  says how itself (`open[AAPL].first`) -- so a day of several rows is its first open, highest high, lowest low and
 *  last close; a period of the tile's frequency then takes its dates' the same way (`candle`). */
function candled(e: TsExpr): TsExpr {
  switch (e.kind) {
    case 'call': {
      const args = e.args.map(candled);
      if (e.fn !== 'ohlc') return { ...e, args };
      return { ...e, args: args.map((a, i) => (a.kind === 'column' && a.fn === undefined ? { ...a, fn: CANDLE[i]! } : a)) };
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
 * a date on every row -- each row's period at the tile's frequency, grouped by it (and by the split, a column of the
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
  const conditions: ValueSpecification[] = [fn('isNotEmpty', col(src.date))];
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
 * PARTS JOINED on the period -- and on their line, both a line per value of the split -- the dates every one has
 * (gs-quant's intersect), at the operator or the function that reads them together (the review's Q1): each part's value
 * computed over its own dates first. A line of the split joined with one line (`close[SPY]`) is that line's per value;
 * each period dated at its latest part's last observation.
 */
function joined(sh: Shared, nodes: readonly { readonly w: Writer; readonly v: Value }[]): Writer {
  // the parts of a line per value first: theirs is the line the join is partitioned by
  const order = [...nodes].sort((a, b) => Number(b.w.split) - Number(a.w.split));
  let left = order[0]!.w;
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
  }
  return left;
}

const OPS: Readonly<Record<'+' | '-' | '*' | '/', string>> = { '+': 'plus', '-': 'minus', '*': 'times', '/': 'divide' };

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
      if ('constant' in l && 'constant' in r) return { constant: op(OPS[e.op], l.constant, r.constant) };
      const w = 'constant' in l ? (r as Exclude<Node, { constant: unknown }>).w
        : 'constant' in r ? l.w : joined(sh, [l, r as Exclude<Node, { constant: unknown }>]);
      if (e.op === '/') {
        const denominator = 'constant' in r ? w.row('num', r.constant) : columnOfValue(r.v);
        return { w, v: { column: w.ratio('div', asOperand(l), denominator) } };
      }
      return { w, v: { column: w.row('op', op(OPS[e.op], asOperand(l), asOperand(r))) } };
    }
    case 'call':
      return call(sh, e);
  }
}

/** A function's series argument, written: a part (a function reads a series, never a number alone). */
function seriesArg(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>, i: number): { readonly w: Writer; readonly v: Value } {
  const n = write(sh, e.args[i]!);
  if ('constant' in n) throw new CubeRefusal(`${e.fn} reads a series: its ${i === 0 ? 'first' : 'second'} argument is a number`);
  return n;
}

/**
 * A CANDLE (ohlc): its four series joined on their dates. At a frequency, each is computed on its sources' own dates
 * first -- its functions over them, its arithmetic on each one -- and each period then takes the first open, the
 * highest high, the lowest low and the last close of them, the database's (the review's N4: each column was taken by
 * the period first, so a candle of `close / 2` weekly was the week's last, four times over, flat).
 */
function candle(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node {
  const own: Shared = sh.grain === undefined ? sh : { ...sh, grain: undefined, periodType: undefined };
  const parts = [0, 1, 2, 3].map((i) => seriesArg(own, e, i));
  const inner = joined(own, parts);
  const [open, high, low, close] = parts.map((p) => columnOfValue(p.v)) as [string, string, string, string];
  if (sh.grain === undefined) return { w: inner, v: { open, high, low, close } };
  // each period of the tile's frequency, of the dates all four have
  const fine = own.periodType!;
  const coarse = periodOf(sh.grain, PERIOD, fine);
  periodTyped(sh, coarse.type, sh.first);
  const p = freshName(sh, 'cp');
  const a = freshName(sh, 'ca');
  const rel = inner.rel
    .filter(row(and(...[open, high, low, close].map((c) => fn('isNotEmpty', col(c))))))
    .extend([derive(p, row(coarse.body)), derive(a, row(periodOf(sh.grain, inner.at, fine).at))]);
  const y = (f: string): Lambda => lambda(['y'], fn(f, variable('y')));
  const v = { open: freshName(sh, 'open'), high: freshName(sh, 'high'), low: freshName(sh, 'low'), close: freshName(sh, 'close') };
  // the most rows a date's value took one of, and the periods a year a function inside annualized by, carried
  const rows = inner.rows.map((c) => [c, freshName(sh, 'rows')] as const);
  const years = inner.years.map((c) => [c, freshName(sh, 'py')] as const);
  const grouped = rel.groupBy([inner.part, p], [
    agg(v.open, row(fn('rowMapper', col(open), col(ORD))), y('minBy')),
    agg(v.high, row(col(high)), y('max')),
    agg(v.low, row(col(low)), y('min')),
    agg(v.close, row(fn('rowMapper', col(close), col(ORD))), y('maxBy')),
    agg(AT, row(col(a)), y('max')),
    ...[...rows, ...years].map(([from, to]) => agg(to, row(col(from)), y('max'))),
    agg(ORD, row(col(ORD)), y('max')),
  ]).rename(p, PERIOD);
  const w = new Writer(grouped, {
    part: inner.part, split: inner.split, at: AT, seconds: inner.seconds, rows: rows.map(([, c]) => c), years: years.map(([, c]) => c),
  }, sh);
  return { w, v };
}

function call(sh: Shared, e: Extract<TsExpr, { kind: 'call' }>): Node {
  const n = (name: string): number => paramOf(e, name) as number;
  // a window or a lag: a whole number of observations, or a calendar period (the spec's reader holds it to one)
  const span = (name: string): Span => {
    const v = paramOf(e, name);
    if (typeof v === 'number') return v;
    const t = tenorOf(v);
    if (t === undefined) throw new CubeRefusal(`${e.fn}'s ${name} ${JSON.stringify(v)} is neither a whole number nor a calendar period (3m, 1y)`);
    return t;
  };
  const word = (name: string): string => paramOf(e, name) as string;
  const R = Writer.read;
  // the pairs and the candle: each series on its own dates, then joined
  if (e.fn === 'correlation' || e.fn === 'beta' || e.fn === 'cov') {
    const a = seriesArg(sh, e, 0);
    const b = seriesArg(sh, e, 1);
    // gs-quant's correlation and beta read each series' returns, taken on its own dates, then aligned; cov the series
    const ra = e.fn === 'cov' ? columnOfValue(a.v) : a.w.simple(columnOfValue(a.v));
    const rb = e.fn === 'cov' ? columnOfValue(b.v) : b.w.simple(columnOfValue(b.v));
    const w = joined(sh, [{ w: a.w, v: { column: ra } }, { w: b.w, v: { column: rb } }]);
    const size = span('w');
    const frame = Writer.frame(size);
    const pair = fn('rowMapper', R(ra), R(rb));
    let v: string;
    if (e.fn === 'correlation') v = w.window('corr', 'corr', pair, frame);
    else if (e.fn === 'cov') v = w.window('cov', 'covarSample', pair, frame);
    else {
      const cov = w.window('cov', 'covarSample', pair, frame);
      const variance = w.window('var', 'varianceSample', R(rb), frame);
      v = w.ratio('beta', one(cov), variance);
    }
    return { w, v: { column: w.ramped(v, ra, atLeast(size, 2), frame) } };
  }
  if (e.fn === 'ohlc') return candle(sh, e);
  const x = seriesArg(sh, e, 0);
  const w = x.w;
  const a = columnOfValue(x.v);
  const done = (column: string): Node => ({ w, v: { column } });
  switch (e.fn) {
    case 'returns':
      return done(w.returns(a, span('obs'), word('type')));
    case 'prices': {
      const initial = float(n('initial'));
      const type = word('type');
      // initial at the first date, and every other from there: gs-quant's product (and sum) over the returns to date
      // skips an empty one, as pandas' prod does -- a return of none (the first, which has no date before it) leaves the
      // level where it was (the second audit's S5: the first date was none)
      const summed = (c: string): ValueSpecification => {
        const total = w.window('cum', 'plus', R(c), 'running');
        return when(fn('isEmpty', col(total)), float(0), one(total));
      };
      const at = (v: ValueSpecification): string => w.row('px', v);
      if (type === 'absolute') return done(at(op('plus', initial, summed(a))));
      if (type === 'log') return done(at(op('times', initial, fn('exp', summed(a)))));
      // initial times the product of (1 + r), gs-quant's: a factor of 0 (a return of -100%) zero from there on, each
      // factor below 0 turning the sign -- the magnitude the sum of the logs of the others (the review's Q8)
      const g = w.row('g', op('plus', lit.integer(1), one(a)));
      const zeros = w.window('zeros', 'count', R(w.row('z', when(fn('equal', one(g), lit.integer(0)), lit.integer(1), none()))), 'running');
      const negatives = w.window('negs', 'count', R(w.row('neg', when(fn('lessThan', one(g), lit.integer(0)), lit.integer(1), none()))), 'running');
      const logs = summed(w.row('lg', when(fn('equal', one(g), lit.integer(0)), none(), fn('log', fn('abs', one(g))))));
      return done(at(when(fn('greaterThan', one(zeros), lit.integer(0)), float(0),
        op('times', op('times', initial, fn('pow', float(-1), one(negatives))), fn('exp', logs)))));
    }
    case 'diff':
      return done(w.row('diff', op('minus', one(a), one(w.lag(a, span('obs'))))));
    case 'lag':
      return done(w.lag(a, span('obs')));
    case 'annualize':
      return done(w.row('ann', op('times', one(a), w.annual())));
    case 'index':
    case 'rebase': {
      const base = w.anchored(a);
      const scale = float(n(e.fn === 'index' ? 'initial' : 'base'));
      return done(w.row('idx', when(fn('equal', one(base), lit.integer(0)), none(), op('times', fn('divide', one(a), one(base)), scale))));
    }
    case 'change':
      return done(w.row('chg', op('minus', one(a), one(w.anchored(a)))));
    case 'percent_change': {
      const base = w.anchored(a);
      return done(w.row('pchg', when(fn('equal', one(base), lit.integer(0)), none(), op('minus', fn('divide', one(a), one(base)), lit.integer(1)))));
    }
    case 'mean':
    case 'sum':
    case 'min':
    case 'max': {
      const size = span('w');
      const reduce = e.fn === 'mean' ? 'average' : e.fn === 'sum' ? 'plus' : e.fn;
      return done(w.ramped(w.window(e.fn, reduce, R(a), Writer.frame(size)), a, size, Writer.frame(size)));
    }
    case 'range': {
      const size = span('w');
      const hi = w.window('max', 'max', R(a), Writer.frame(size));
      const lo = w.window('min', 'min', R(a), Writer.frame(size));
      return done(w.ramped(w.row('rng', op('minus', one(hi), one(lo))), a, size, Writer.frame(size)));
    }
    case 'std':
      return done(w.std(a, span('w')));
    case 'var': {
      const size = span('w');
      return done(w.ramped(w.window('var', 'varianceSample', R(a), Writer.frame(size)), a, atLeast(size, 2), Writer.frame(size)));
    }
    case 'zscores': {
      const size = span('w');
      const frame = size === 0 ? 'all' : size;
      const mean = w.window('mean', 'average', R(a), frame);
      const sd = w.window('std', 'stdDevSample', R(a), frame);
      const z = w.ratio('z', op('minus', one(a), one(mean)), sd);
      return done(size === 0 ? z : w.ramped(z, a, size));
    }
    case 'winsorize': {
      const limit = float(n('limit'));
      const mean = w.window('mean', 'average', R(a), 'all');
      const sd = w.window('std', 'stdDevSample', R(a), 'all');
      const hi = w.row('hi', op('plus', one(mean), op('times', limit, one(sd))));
      const lo = w.row('lo', op('minus', one(mean), op('times', limit, one(sd))));
      return done(w.row('wins', when(fn('greaterThan', one(a), one(hi)), col(hi), when(fn('lessThan', one(a), one(lo)), col(lo), col(a)))));
    }
    case 'volatility': {
      const r = w.returns(a, 1, word('type'));
      const sd = w.std(r, span('w'));
      return done(w.row('vol', op('times', op('times', one(sd), w.annual()), lit.integer(100))));
    }
    case 'drawdown': {
      const size = span('w');
      const peak = w.window('peak', 'max', R(a), Writer.frame(size));
      const dd = w.row('dd', when(fn('equal', one(peak), lit.integer(0)), none(), op('minus', fn('divide', one(a), one(peak)), lit.integer(1))));
      return done(size === 0 ? dd : w.ramped(dd, a, size));
    }
    case 'max_drawdown': {
      const size = span('w');
      const peak = w.window('peak', 'max', R(a), Writer.frame(size));
      const dd = w.row('dd', when(fn('equal', one(peak), lit.integer(0)), none(), op('minus', fn('divide', one(a), one(peak)), lit.integer(1))));
      return done(w.ramped(w.window('mdd', 'min', R(dd), Writer.frame(size)), a, size));
    }
    case 'sharpe_ratio': {
      const size = span('w');
      const r = w.simple(a);
      const mean = w.window('mean', 'average', R(r), Writer.frame(size));
      const sd = w.std(r, size);
      return done(w.ramped(w.row('sharpe', when(fn('equal', one(sd), lit.integer(0)), none(),
        op('times', fn('divide', one(mean), one(sd)), w.annual()))), r, size, Writer.frame(size)));
    }
    case 'bollinger_bands': {
      const size = span('w');
      const k = float(n('k'));
      const mean = w.ramped(w.window('mean', 'average', R(a), Writer.frame(size)), a, size, Writer.frame(size));
      const sd = w.std(a, size);
      const lower = w.row('lower', op('minus', one(mean), op('times', k, one(sd))));
      const upper = w.row('upper', op('plus', one(mean), op('times', k, one(sd))));
      return { w, v: { lower, upper, width: w.row('width', op('minus', one(upper), one(lower))) } };
    }
    case 'rsi': {
      const size = span('w');
      const d = w.row('d', op('minus', one(a), one(w.lag(a, 1))));
      const gain = w.row('gain', when(fn('isEmpty', col(d)), none(), when(fn('greaterThan', one(d), lit.integer(0)), one(d), float(0))));
      const loss = w.row('loss', when(fn('isEmpty', col(d)), none(), when(fn('lessThan', one(d), lit.integer(0)), op('minus', float(0), one(d)), float(0))));
      const up = w.window('up', 'average', R(gain), Writer.frame(size));
      const down = w.window('down', 'average', R(loss), Writer.frame(size));
      const rsi = w.row('rsi', when(fn('equal', one(down), lit.integer(0)), float(100),
        op('minus', float(100), fn('divide', float(100), op('plus', lit.integer(1), fn('divide', one(up), one(down)))))));
      return done(w.ramped(rsi, gain, size, Writer.frame(size)));
    }
    case 'log':
      return done(w.row('ln', when(fn('lessThanEqual', one(a), lit.integer(0)), none(), fn('log', one(a)))));
    case 'exp':
      return done(w.row('exp', fn('exp', one(a))));
    case 'sqrt':
      return done(w.row('sqrt', when(fn('lessThan', one(a), lit.integer(0)), none(), fn('sqrt', one(a)))));
    case 'abs':
      return done(w.row('abs', fn('abs', one(a))));
    case 'power':
      return done(w.row('pow', fn('pow', one(a), float(n('y')))));
    case 'floor': {
      const v = float(n('value'));
      return done(w.row('floor', when(fn('lessThan', one(a), v), v, one(a))));
    }
    case 'ceil': {
      const v = float(n('value'));
      return done(w.row('ceil', when(fn('greaterThan', one(a), v), v, one(a))));
    }
  }
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
function seriesRelation(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, anchor: string | undefined): Built {
  const expr = candled(inlined(series.expr, ts.series));
  const read = sourcesOf(ts, expr);
  if (read.length === 0) throw new CubeRefusal(`series ${series.id} reads no column of a grid`);
  const sh: Shared = { ts, grain: ts.grain, series, inputs, split: series.split, first: read[0]!, anchor, periodType: undefined, names: { n: 0 } };
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
    // its change over its start's size, as the statistics' % change is: a start below zero rising is a rise (the
    // second audit's S1: a yield of -1 rising to -0.5 was a fall of 50%); of a start above zero, x / x0 as gs-quant's
    const how = ts.view?.compare;
    if (how !== undefined && level) {
      const base = w.anchored(value);
      const change = w.row('cmp_chg', when(fn('equal', one(base), lit.integer(0)), none(),
        fn('divide', op('minus', one(value), one(base)), fn('abs', one(base)))));
      value = how === 'rebase' ? w.row('cmp', op('times', op('plus', lit.integer(1), one(change)), lit.integer(100))) : change;
      unit = how === 'rebase' ? 'index' : 'ratio';
    }
    built = { value, ...(value !== own ? { levelValue: own } : {}) };
  }
  // the most rows any of its columns took one of, at a period: more than one, and the drawing says so
  const rows = w.rows.length === 0 ? undefined : w.rows.length === 1 ? w.rows[0]!
    : w.row('rows', w.rows.slice(1).reduce<ValueSpecification>((most, c) => when(fn('greaterThan', one(c), most), one(c), most), one(w.rows[0]!)));
  // the periods a year it was annualized by: of a combination, none when any part's is none (the second audit's S4: a
  // part of no frequency, every value none, and the first part's 252 said nothing was wrong)
  const perYear = w.years.length <= 1 ? w.years[0]
    : w.row('per_year_all', when(or(...w.years.map((c) => fn('isEmpty', col(c)))), none(), one(w.years[0]!)));
  return {
    rel: w.rel, period: PERIOD, ord: ORD, at: w.at, periodType: sh.periodType!, ...built, ...(split !== undefined ? { split } : {}),
    unit, level, ...(rows !== undefined ? { rows } : {}), ...(perYear !== undefined ? { perYear } : {}), sources: read, writer: w,
  };
}

/** The sources a series reads, by alias, in the order it reads them: one, run where its grid runs; more, a combination. */
export function seriesSources(ts: TimeseriesSpec, series: TsSeries): string[] {
  return sourcesOf(ts, inlined(series.expr, ts.series));
}

/**
 * A SERIES' QUERY: its periods (and split values), latest first, each with its value -- or a band's lower and upper
 * -- at most TS_POINT_LIMIT of them. `view.from`: the first date shown, an index's base (readsAnchor). `clip`: the dates
 * its answer keeps (a host reading a window: Python's Timeseries.frame) -- kept by the database after every value is
 * computed over every date, so a window's first values have their history (a 63-day volatility its 63 days); absent,
 * every date (the viewer's, which zooms over them).
 */
export function seriesQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries, view: TsWindow = {}, clip: TsWindow = {},
  limit: number | null = TS_POINT_LIMIT + 1): TsQuery {
  const b = seriesRelation(inputs, ts, series, view.from);
  const out: ColSpec[] = [
    derive(TS_DATE, row(col(b.at))),
    derive(TS_VALUE, row(col(b.value))),
    ...(b.lower !== undefined ? [derive(TS_LOWER, row(col(b.lower)))] : []),
    ...(b.upper !== undefined ? [derive(TS_UPPER, row(col(b.upper)))] : []),
    ...(b.width !== undefined ? [derive(TS_WIDTH, row(col(b.width)))] : []),
    ...(b.open !== undefined ? [derive(TS_OPEN, row(col(b.open))), derive(TS_HIGH, row(col(b.high!))), derive(TS_LOW, row(col(b.low!)))] : []),
    ...(b.split !== undefined ? [derive(TS_SPLIT, row(col(b.split)))] : []),
    ...(b.rows !== undefined ? [derive(TS_ROWS, row(col(b.rows)))] : []),
    ...(b.perYear !== undefined ? [derive(TS_PER_YEAR, row(col(b.perYear)))] : []),
  ];
  let rel = b.rel.extend(out);
  const within: ValueSpecification[] = [];
  if (clip.from !== undefined) within.push(fn('greaterThanEqual', one(b.at), dateLiteral(clip.from, b.periodType)));
  if (clip.to !== undefined) within.push(fn('lessThanEqual', one(b.at), dateLiteral(`${clip.to}${isDay(b.periodType) ? '' : 'T23:59:59'}`, b.periodType)));
  if (within.length > 0) rel = rel.filter(row(and(...within)));
  rel = rel.select(out.map((c) => c.name)).sort([desc(TS_DATE)]);
  if (limit !== null) rel = rel.limit(limit);
  return {
    series: series.id,
    query: rel.lambda(),
    unit: b.unit,
    band: b.lower !== undefined,
    ...(b.open !== undefined ? { candles: true } : {}),
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
  const perYear = level ? w.perYear() : undefined;
  const yearly = inferred ? w.row('stat_py', perYear!) : undefined;
  // the dates shown: each period's value's date, its last observation (the review's Q6)
  const shown: ValueSpecification[] = [fn('isNotEmpty', col(v))];
  if (view.from !== undefined) shown.push(fn('greaterThanEqual', one(b.at), dateLiteral(view.from, b.periodType)));
  if (view.to !== undefined) shown.push(fn('lessThanEqual', one(b.at), dateLiteral(`${view.to}${isDay(b.periodType) ? '' : 'T23:59:59'}`, b.periodType)));
  w.rel = w.rel.filter(row(and(...shown)));
  // a level's figures of its returns, its drawdown and its growth: of the level itself, compared or not
  const lv = b.levelValue ?? v;
  const rLog = level ? w.returns(lv, 1, 'log') : undefined;
  const rSimple = level ? w.simple(lv) : undefined;
  const peak = level ? w.window('peak', 'max', Writer.read(lv), 'running') : undefined;
  const dd = peak !== undefined
    ? w.row('dd', when(fn('lessThanEqual', one(peak), lit.integer(0)), none(), op('minus', fn('divide', one(lv), one(peak)), lit.integer(1))))
    : undefined;
  const y = (f: string): Lambda => lambda(['y'], fn(f, variable('y')));
  const S = statName;
  // a candle's least and most are its lows' and highs' (the review's Q9), a line's its values'
  const lows = b.low ?? v;
  const highs = b.high ?? v;
  const aggs: ColSpec[] = [
    agg(S('count'), row(col(v)), y('count')),
    agg(S('from'), row(col(b.at)), y('min')),
    agg(S('to'), row(col(b.at)), y('max')),
    agg(S('first'), row(fn('rowMapper', col(v), col(b.ord))), y('minBy')),
    agg(S('last'), row(fn('rowMapper', col(v), col(b.ord))), y('maxBy')),
    agg(S('low'), row(col(lows)), y('min')),
    agg(S('lowAt'), row(fn('rowMapper', col(b.at), col(lows))), y('minBy')),
    agg(S('high'), row(col(highs)), y('max')),
    agg(S('highAt'), row(fn('rowMapper', col(b.at), col(highs))), y('maxBy')),
    agg(S('mean'), row(col(v)), y('average')),
    agg(S('std'), row(col(v)), y('stdDevSample')),
    ...(rLog !== undefined ? [agg('__ts_stat_logsd', row(col(rLog)), y('stdDevSample'))] : []),
    ...(rSimple !== undefined ? [
      agg('__ts_stat_rmean', row(col(rSimple)), y('average')),
      agg('__ts_stat_rsd', row(col(rSimple)), y('stdDevSample')),
    ] : []),
    ...(dd !== undefined ? [agg('__ts_stat_mdd_all', row(col(dd)), y('min'))] : []),
    ...(yearly !== undefined ? [agg(TS_STAT_PER_YEAR, row(col(yearly)), y('max'))] : []),
    ...(level ? [
      agg(TS_STAT_LEVEL_LOW, row(col(lv)), y('min')),
      agg('__ts_stat_lfirst', row(fn('rowMapper', col(lv), col(b.ord))), y('minBy')),
      agg('__ts_stat_llast', row(fn('rowMapper', col(lv), col(b.ord))), y('maxBy')),
    ] : []),
  ];
  const keys = b.split !== undefined ? [b.split] : [w.part];
  let rel = w.rel.groupBy(keys, aggs);
  const first = one(S('first'));
  const last = one(S('last'));
  const derived: ColSpec[] = [
    derive(S('change'), row(op('minus', last, first))),
    // the change over its start's size: a start below zero rising is a rise (the review's Q11)
    derive(S('pct'), row(when(fn('equal', first, lit.integer(0)), none(), fn('divide', op('minus', last, first), fn('abs', first))))),
  ];
  if (level) {
    const days = fn('dateDiff', one(S('from')), one(S('to')), enumValue(DURATION, 'DAYS'));
    const perYear = fn('sqrt', yearly !== undefined ? one(TS_STAT_PER_YEAR) : w.perYear());
    // a level's figures of its returns, its drawdown and its growth, over a window it is above zero in throughout:
    // gone to zero or below (a yield), none of them -- never one of the part above zero, never a sign turned (the
    // second audit's S1), and the viewer says why (its statistics' blanks)
    const above = (x: ValueSpecification): ValueSpecification => when(fn('lessThanEqual', one(TS_STAT_LEVEL_LOW), lit.integer(0)), none(), x);
    const lfirst = one('__ts_stat_lfirst');
    const llast = one('__ts_stat_llast');
    derived.push(
      derive(S('vol'), row(above(op('times', op('times', one('__ts_stat_logsd'), perYear), lit.integer(100))))),
      derive(S('mdd'), row(above(one('__ts_stat_mdd_all')))),
      // compound annual growth over a calendar year or more, the database's own date arithmetic deciding (a leap
      // year's 366 days a year): under a year, none -- a return of under a year is never annualized (GIPS; the
      // review's N3: 8,507% over four days), its % change its growth, and the viewer says so
      derive(S('cagr'), row(when(or(fn('lessThan', one(S('to')), fn('adjust', one(S('from')), lit.integer(1), enumValue(DURATION, 'YEARS'))),
        fn('lessThanEqual', one(TS_STAT_LEVEL_LOW), lit.integer(0))), none(),
        op('minus', fn('pow', fn('divide', llast, lfirst), fn('divide', float(365.25), days)), lit.integer(1))))),
      derive(S('sharpe'), row(above(when(fn('equal', one('__ts_stat_rsd'), lit.integer(0)), none(),
        op('times', fn('divide', one('__ts_stat_rmean'), one('__ts_stat_rsd')), perYear))))),
    );
  }
  rel = rel.extend(derived);
  const kept = [...TS_STATS.filter((st) => level || !['vol', 'mdd', 'cagr', 'sharpe'].includes(st)).map(S),
    ...(yearly !== undefined ? [TS_STAT_PER_YEAR] : []), ...(level ? [TS_STAT_LEVEL_LOW] : [])];
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
