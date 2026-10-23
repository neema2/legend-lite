// PRESETS: the calculations a chart (and the grid) offers by name, as the calculated columns DataCube already has
// (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2). Nothing here computes a value: each preset is a `DerivedColumn`
// -- an expression over the row, or a window -- that the planner plans and the database computes, exactly as a column
// typed into the column editor is. The chart keeps a preset as its WORDS (`grain: 'month'`,
// `showAs: { kind: 'runningTotal' }`) and expands it here when its query is built, so Python writes the same words and
// never a copy of this (one-platform rule 1); the grid's header menu adds the same columns to the cube.
//
//   grain      a date taken to its period: the compiler's firstDayOfWeek / Month / Quarter / Year, or the day itself
//   bins       a number taken to its bin: floor(v / width) * width, or `count` bins over the column's range
//   show as    over the chart's rows, after grouping: a running total, a share of the total, the change from the row
//              before (absolute or as a ratio), a moving average, a rank
//   stats      a measure's average, minimum or maximum over the chart's rows: a reference line's value

import {
  collection,
  divide,
  enumValue,
  fn,
  lambda,
  lit,
  minus,
  plus,
  property,
  times,
  variable,
  type Lambda,
  type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import type { Bins, LayerStat, ShowAs, ShowAsKind, TimeGrain } from './chart-spec.ts';
import { isNumeric } from '../../engine-client/src/types.ts';
import type { DerivedColumn, FilterNode, SortSpec, WindowSpec } from './snapshot.ts';

const x = (): ValueSpecification => variable('x');
const col = (name: string): ValueSpecification => property(x(), name);
/** A column's value, `$x.<name>->toOne()`: a column read from a frame may be empty, and arithmetic takes one value. */
const one = (name: string): ValueSpecification => fn('toOne', col(name));
const row = (body: ValueSpecification): Lambda => lambda(['x'], body);
const number = (n: number): ValueSpecification => (Number.isInteger(n) ? lit.integer(n) : lit.float(n));

// ---------------------------------------------------------------- grain

/** The compiler's function taking a date to its period's first day; `day` of a date is the date itself. */
const GRAIN_FUNCTION: Readonly<Record<TimeGrain, string>> = {
  day: 'datePart',
  week: 'firstDayOfWeek',
  month: 'firstDayOfMonth',
  quarter: 'firstDayOfQuarter',
  year: 'firstDayOfYear',
};

/** Words for a grain, as a menu says them. */
export const GRAIN_LABELS: Readonly<Record<TimeGrain, string>> = {
  day: 'Day', week: 'Week', month: 'Month', quarter: 'Quarter', year: 'Year',
};

/** The column a grain makes of `column`: its name. */
export function grainName(column: string, grain: TimeGrain): string {
  return `${column}__${grain}`;
}

/**
 * A date column taken to its period, a row-stage dimension: `$x.d->toOne()->firstDayOfMonth()`; by day, its date part
 * (a DateTime's day; a StrictDate's itself).
 */
export function grainColumn(column: string, grain: TimeGrain, name = grainName(column, grain)): DerivedColumn {
  return { name, lambda: row(fn(GRAIN_FUNCTION[grain], one(column))), kind: 'dimension' };
}

/** One period of each grain, as the compiler's `adjust` takes it: how many of which DurationUnit. */
const GRAIN_STEP: Readonly<Record<TimeGrain, readonly [number, string]>> = {
  day: [1, 'DAYS'], week: [7, 'DAYS'], month: [1, 'MONTHS'], quarter: [3, 'MONTHS'], year: [1, 'YEARS'],
};

/** The column that names each row's period's END: its name. */
export function upperName(axis: string): string {
  return `${axis}__upper`;
}

/**
 * The end of each row's period, exclusive -- the next period's first day: `adjust(<its first day>, 1, MONTHS)`. The
 * DATABASE's date arithmetic, so a click on a month filters to the month the database meant (the browser compares
 * with it, never computes it).
 */
export function grainEndColumn(column: string, grain: TimeGrain, name: string): DerivedColumn {
  const [n, unit] = GRAIN_STEP[grain];
  const start = fn(GRAIN_FUNCTION[grain], one(column));
  return {
    name: upperName(name),
    lambda: row(fn('adjust', start, lit.integer(n), enumValue('meta::pure::functions::date::DurationUnit', unit))),
    kind: 'measure',
  };
}

// ---------------------------------------------------------------- bins

/** The column bins make of `column`: its name. */
export function binName(column: string): string {
  return `${column}__bin`;
}

/**
 * A number column taken to its bin, as two row-stage columns: its lower edge (a dimension: the bin) and its upper edge
 * (exclusive). THE BIN AND ITS FILTER AGREE BY CONSTRUCTION (the DataCube + Python line's review): a row is in bin `k`
 * exactly when `edge(k) <= v < edge(k + 1)` -- the same edges a click's or a brush's conditions compare against
 * (rangeConditions) -- so a bar's rows are the rows its filter keeps. The database guesses `k` by a division and moves
 * it by comparing with those edges (`placed`), since a float quotient may land either side of a whole number.
 *
 * `width`: the edges are the width's own decimals -- `0.05` is `5 / 100`, so edge 3 is `(3 * 5) / 100`, the double
 * nearest 0.15, the very double a filter's literal 0.15 is (a product `3 * 0.05` is 0.15000000000000002, and would put
 * 0.15 in the bin below). `count`: that many bins over the column's range -- its minimum and maximum windows over every
 * row (before the grid's filter, so the bins stay put as the filter changes); the last bin keeps the maximum, its upper
 * edge one width past it.
 */
export function binColumns(column: string, bins: Bins, name = binName(column)): DerivedColumn[] {
  const v = one(column);
  if (bins.width !== undefined) {
    if (!(bins.width > 0)) throw new Error('a bin is wider than nothing');
    const [whole, scale] = exactly(bins.width);
    const edge = (k: ValueSpecification): ValueSpecification => divide(times(k, lit.integer(whole)), lit.integer(scale));
    const k = placed(fn('floor', divide(v, number(bins.width))), v, edge);
    return [
      { name, lambda: row(edge(k)), kind: 'dimension' },
      { name: upperName(name), lambda: row(edge(plus(k, lit.integer(1)))), kind: 'measure' },
    ];
  }
  const count = Math.floor(bins.count ?? 0);
  if (!(count >= 1)) throw new Error('bins: a width, or a count of at least one');
  const lo = `${name}__min`;
  const hi = `${name}__max`;
  const whole = { partition: [] as string[], order: [] as SortSpec[], frame: 'partition' as const };
  const width = divide(minus(one(hi), one(lo)), lit.integer(count));
  const edge = (k: ValueSpecification): ValueSpecification => plus(one(lo), times(k, width));
  const k = placed(fn('floor', divide(minus(v, one(lo)), width)), v, edge);
  // the maximum, and any value placed past the last bin, is in the last bin
  const last = fn('greaterThanEqual', k, lit.integer(count - 1));
  const flat = fn('equal', one(hi), one(lo));
  return [
    { name: lo, window: { ...whole, fn: 'min', column }, kind: 'measure' },
    { name: hi, window: { ...whole, fn: 'max', column }, kind: 'measure' },
    {
      name,
      lambda: row(fn('if', flat, lambda([], one(lo)),
        lambda([], fn('if', last, lambda([], edge(lit.integer(count - 1))), lambda([], edge(k)))))),
      kind: 'dimension',
    },
    {
      name: upperName(name),
      lambda: row(fn('if', flat, lambda([], plus(one(lo), lit.integer(1))),
        lambda([], fn('if', last, lambda([], plus(one(hi), width)), lambda([], edge(plus(k, lit.integer(1)))))))),
      kind: 'measure',
    },
  ];
}

/**
 * A bin's index, placed by comparison: `guess` (a division's floor) moved down one when the value is below its edge,
 * up one when at or past the next -- so the bin a row is drawn in is the one its edges say.
 */
function placed(
  guess: ValueSpecification,
  v: ValueSpecification,
  edge: (k: ValueSpecification) => ValueSpecification,
): ValueSpecification {
  return fn('if', fn('lessThan', v, edge(guess)), lambda([], minus(guess, lit.integer(1))),
    lambda([], fn('if', fn('greaterThanEqual', v, edge(plus(guess, lit.integer(1)))),
      lambda([], plus(guess, lit.integer(1))), lambda([], guess))));
}

/**
 * A width as a whole number over a power of ten, from its own decimals: 0.05 is [5, 100], 250 is [250, 1]. The build
 * reads the number the spec says (model text, not a value the query computes). The edge's product `k * whole` is a
 * 64-bit integer in the database: it holds while |value| * 10^decimals stays under 9.2e18 -- values to 9.2e6 at 12
 * decimals, to 9.2e16 at 2. A width finer than its values can carry is refused by the database (an overflow), never
 * drawn wrong.
 */
export function exactly(width: number): readonly [number, number] {
  const text = String(width);
  if (/e/i.test(text)) throw new Error(`a bin width of ${text} has too many places: give a width of 12 decimals or fewer`);
  const decimals = text.includes('.') ? text.length - text.indexOf('.') - 1 : 0;
  if (decimals > 12) throw new Error(`a bin width of ${text} has too many places: give a width of 12 decimals or fewer`);
  const scale = 10 ** decimals;
  return [Math.round(width * scale), scale];
}

// ---------------------------------------------------------------- show as

/** Words for each show-as, as a menu says them. */
export const SHOW_AS_LABELS: Readonly<Record<ShowAsKind, string>> = {
  runningTotal: 'Running total',
  percentOfTotal: '% of total',
  difference: 'Difference from previous',
  percentChange: '% change from previous',
  movingAverage: 'Moving average',
  rank: 'Rank',
};

/** Whether a show-as is a ratio (a share, a change): it reads as a percentage. */
export function isRatio(kind: ShowAsKind): boolean {
  return kind === 'percentOfTotal' || kind === 'percentChange';
}

/** The name a measure shown as `kind` has: `runningTotal_sum_notional`. */
export function showAsName(kind: ShowAsKind, base: string): string {
  return `${kind}_${base}`;
}

/**
 * A measure shown as something computed from it, as GROUP-LEVEL columns over the chart's rows: `base` is the
 * measure's column after grouping, `along` the order the rows run in (the column across, ascending), `within` the
 * columns that restart it (a split: each series its own running total). The last column returned is the one plotted,
 * `name`; any it reads are named after it (`<name>__total`, `<name>__previous`).
 */
export function showAsColumns(
  show: ShowAs,
  base: string,
  along: readonly string[],
  within: readonly string[],
  name = showAsName(show.kind, base),
  panels: readonly string[] = [],
): DerivedColumn[] {
  const order: SortSpec[] = along.map((column) => ({ column, direction: 'asc' }));
  // each series restarts it, and each small chart (a facet's panels) is a whole of its own
  const partition = [...within, ...panels];
  switch (show.kind) {
    case 'runningTotal':
      return [{ name, window: { fn: 'sum', column: base, partition, order, frame: 'running' } }];
    case 'movingAverage':
      return [{ name, window: { fn: 'average', column: base, partition, order, frame: { lastRows: Math.max(1, Math.floor(show.rows ?? 3)) } } }];
    case 'rank':
      return [{ name, window: { fn: 'rank', partition, order: [{ column: base, direction: 'desc' }] } }];
    case 'percentOfTotal': {
      const total = `${name}__total`;
      return [
        { name: total, window: wholeOf('sum', base, panels) },
        { name, lambda: row(divide(one(base), one(total))) },
      ];
    }
    case 'difference':
    case 'percentChange': {
      const before = `${name}__previous`;
      // the first row has no row before: its change is empty (SQL's NULL through toOne), never a guess; and a change
      // from nothing has no ratio -- empty too, where a database would refuse a division by zero
      const change = minus(one(base), one(before));
      const ratio = fn('if', fn('equal', one(before), lit.integer(0)), lambda([], collection([])),
        lambda([], divide(change, one(before))));
      return [
        { name: before, window: { fn: 'lag', column: base, partition, order } },
        { name, lambda: row(show.kind === 'difference' ? change : ratio) },
      ];
    }
  }
}

// ---------------------------------------------------------------- stats

/** The name a measure's statistic over the chart's rows has: `average_of_sum_notional`. */
export function statName(stat: LayerStat, measure: string): string {
  return `${stat}_of_${measure}`;
}

/** A measure's average, minimum or maximum over every row of the chart -- of each small chart, given `panels` (a
 *  facet's columns): a group-level window over all of them. */
export function statColumn(stat: LayerStat, measure: string, panels: readonly string[] = []): DerivedColumn {
  return { name: statName(stat, measure), window: wholeOf(stat, measure, panels) };
}

/**
 * A figure over EVERY row of the chart -- a total, an average -- as a window: the whole partition, in the order of the
 * column it reads. That order is said, not left out: a group-level window with no order takes the level's (query.ts
 * extendDerived), which may be the very column being computed (a chart sorted by its share of the total).
 */
function wholeOf(fn: 'sum' | LayerStat, column: string, panels: readonly string[] = []): WindowSpec {
  return { fn, column, partition: [...panels], order: [{ column, direction: 'asc' }], frame: 'partition' };
}

// ---------------------------------------------------------------- the kinds' own figures (batch 3)

/** A waterfall's columns: each bar runs from where the one before ended (`start`) to where it ends (`end`). */
export function waterfallNames(measure: string): { readonly start: string; readonly end: string } {
  return { start: `waterfall_start_${measure}`, end: `waterfall_end_${measure}` };
}

/**
 * A WATERFALL'S BARS, the database's: each category's end is the running total of `measure` along the chart's own
 * order (`order`, which names every column it needs to be total: ties broken by the category), its start that end
 * less its own value. The chart draws each bar from its start to its end; nothing is added up in the page.
 */
export function waterfallColumns(measure: string, order: readonly SortSpec[]): DerivedColumn[] {
  const { start, end } = waterfallNames(measure);
  return [
    { name: end, window: { fn: 'sum', column: measure, partition: [], order: [...order], frame: 'running' } },
    { name: start, lambda: row(minus(one(end), one(measure))) },
  ];
}

/** A Pareto's cumulative share's column. */
export function paretoName(measure: string): string {
  return `pareto_share_${measure}`;
}

/**
 * A PARETO'S LINE, the database's: the running total of `measure` from the largest down (ties by the category
 * `across`), as a share of the whole.
 */
export function paretoColumns(measure: string, across: string): DerivedColumn[] {
  const name = paretoName(measure);
  const running = `${name}__running`;
  const total = `${name}__total`;
  return [
    {
      name: running,
      window: {
        fn: 'sum', column: measure, partition: [], frame: 'running',
        order: [{ column: measure, direction: 'desc' }, { column: across, direction: 'asc' }],
      },
    },
    { name: total, window: wholeOf('sum', measure) },
    { name, lambda: row(divide(one(running), one(total))) },
  ];
}

/** A two-ring pie's inner ring: each category's total over its split's values. */
export function ringTotalName(measure: string): string {
  return `ring_total_${measure}`;
}

/** THE INNER RING of a pie split in two rings, the database's: `measure` summed over each value of `across`. */
export function ringTotalColumn(measure: string, across: string): DerivedColumn {
  return {
    name: ringTotalName(measure),
    window: { fn: 'sum', column: measure, partition: [across], order: [{ column: measure, direction: 'asc' }], frame: 'partition' },
  };
}

/**
 * A part over a whole that may be nothing: empty when the whole is zero (a P&L that nets to nothing), as a change from
 * nothing is (showAsColumns' percentChange) -- where a database would refuse the division, or answer infinity.
 */
function share(part: ValueSpecification, whole: string): ValueSpecification {
  return fn('if', fn('equal', one(whole), lit.integer(0)), lambda([], collection([])), lambda([], divide(part, one(whole))));
}

/** A Marimekko's columns: where each column starts across and how wide it is, where each cell starts up and ends. */
export function marimekkoNames(measure: string): {
  readonly x0: string; readonly x1: string; readonly width: string; readonly y0: string; readonly y1: string;
} {
  return {
    x0: `mekko_x0_${measure}`, x1: `mekko_x1_${measure}`, width: `mekko_width_${measure}`,
    y0: `mekko_y0_${measure}`, y1: `mekko_y1_${measure}`,
  };
}

/**
 * A MARIMEKKO'S CELLS, the database's (batch 3's leftovers): each column across as wide as its share of the whole, each
 * cell up it as tall as its share of the column -- every figure a share between 0 and 1. A column starts where the
 * ones before it (in the order across, `direction`) end: the running total through the cell, less the running total
 * within its column through the cell, over the whole, and ends that start plus its own total over the whole; a cell
 * starts where the cells below it in its column (in the split's order) end. The chart draws each cell from its four
 * edges; nothing is added up in the page.
 */
export function marimekkoColumns(measure: string, across: string, split: string, direction: 'asc' | 'desc'): DerivedColumn[] {
  const { x0, x1, width, y0, y1 } = marimekkoNames(measure);
  const total = `${width}__column`;
  const all = `${width}__all`;
  const run = `${x0}__running`;
  const within = `${y1}__running`;
  return [
    { name: total, window: { fn: 'sum', column: measure, partition: [across], order: [{ column: measure, direction: 'asc' }], frame: 'partition' } },
    { name: all, window: wholeOf('sum', measure) },
    {
      name: run,
      window: { fn: 'sum', column: measure, partition: [], frame: 'running', order: [{ column: across, direction }, { column: split, direction: 'asc' }] },
    },
    { name: within, window: { fn: 'sum', column: measure, partition: [across], frame: 'running', order: [{ column: split, direction: 'asc' }] } },
    { name: x0, lambda: row(share(minus(one(run), one(within)), all)) },
    { name: x1, lambda: row(share(plus(minus(one(run), one(within)), one(total)), all)) },
    { name: width, lambda: row(share(one(total), all)) },
    { name: y0, lambda: row(share(minus(one(within), one(measure)), total)) },
    { name: y1, lambda: row(share(one(within), total)) },
  ];
}

/** A waffle's columns: the first of its hundred cells a category fills, and the one after its last. */
export function waffleNames(measure: string): { readonly from: string; readonly to: string } {
  return { from: `waffle_from_${measure}`, to: `waffle_to_${measure}` };
}

/** A waffle's cells: a hundred, ten by ten. */
export const WAFFLE_CELLS = 100;

/**
 * A WAFFLE'S CELLS, the database's (batch 3's leftovers): each category fills the cells from where the category before
 * it ended to its running share through it, each a hundredth, rounded -- a running total rounded, never each share on
 * its own, so the ranges meet and add up to a hundred exactly. In the chart's own order (`order`, ties broken by the
 * category); a whole of nothing has no cells (empty, as a share of nothing is). The chart colours each cell by the
 * category whose range holds it, comparing, never counting.
 */
export function waffleColumns(measure: string, order: readonly SortSpec[]): DerivedColumn[] {
  const { from, to } = waffleNames(measure);
  const run = `${to}__running`;
  const all = `${to}__all`;
  const before = `${from}__before`;
  // a range starts where the one before it ended (the same column, the row before: a lag), never at its own share
  // rounded again -- two roundings of nearly one figure can part by a cell; the first starts at the first cell
  return [
    { name: run, window: { fn: 'sum', column: measure, partition: [], order: [...order], frame: 'running' } },
    { name: all, window: wholeOf('sum', measure) },
    { name: to, lambda: row(fn('if', fn('equal', one(all), lit.integer(0)), lambda([], collection([])),
      lambda([], fn('round', times(divide(one(run), one(all)), lit.integer(WAFFLE_CELLS)))))) },
    { name: before, window: { fn: 'lag', column: to, partition: [], order: [...order] } },
    { name: from, lambda: row(fn('if', fn('isEmpty', col(before)), lambda([], lit.integer(0)), lambda([], one(before)))) },
  ];
}

// ---------------------------------------------------------------- a clicked period or bin

/**
 * A period or a bin as conditions on the cube's own column: from its start to `upper`, the database's end of it
 * (exclusive: the next period's first day, the next bin's edge). A literal is written by the column's type: a day for
 * a StrictDate, a timestamp for a DateTime, the number for a number (a big integer's digits as they are, which the
 * literal writer spells by the type) -- another type has no range here, and is refused.
 */
export function rangeConditions(
  column: string,
  type: string | undefined,
  from: string | number | boolean,
  upper: string | number | boolean,
): FilterNode[] {
  const literal = (v: string | number | boolean): string | number | boolean => {
    if (typeof v !== 'string') return v;
    if (type !== undefined && /StrictDate$/.test(type)) return v.slice(0, 10);
    if (type !== undefined && /(DateTime|Timestamp)$/.test(type)) return v.length <= 10 ? `${v}T00:00:00` : v;
    if (isNumeric(type) && /^-?\d+$/.test(v)) return v;
    throw new Error(`a range of '${column}' (${type ?? 'untyped'}) has no literal for ${JSON.stringify(v)}`);
  };
  return [
    { kind: 'condition', column, operator: 'greaterThanEqual', value: literal(from) },
    { kind: 'condition', column, operator: 'lessThan', value: literal(upper) },
  ];
}
