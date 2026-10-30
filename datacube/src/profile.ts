// A BOX'S PROFILE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M5: Alteryx's Browse profile, its Field Summary): each column of
// the rows a box gives -- how many of them have a value, how many different values, the smallest and the largest --
// asked of the database as one query over those rows, a row of aggregates (`profileLambda`), and read back off its one
// row (`profileOf`). Nothing is counted here: the database counts -- each column's empties and its share filled too, and
// its one row even over no rows (a keyless `aggregate`, never a grouping, which gives no group of none). Asked when the
// profile is shown, never before; a column's most frequent values (`topValuesLambda`) when that column is opened.
//
// PURE: no DOM, no planner.

import {
  agg, asc, colSpecs, derive, desc, divide, eq, fn, from, lambda, lit, minus, property, variable, type ColSpec, type Lambda, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { familyOf } from '../../engine-client/src/types.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';

/** A column of the rows profiled: its name and its type, as the compiler typed it. */
export interface ProfiledColumn {
  readonly name: string;
  readonly type: string;
}

/** What the profile says of a column. `smallest` and `largest` are absent for a column with no order (a Boolean, JSON). */
export interface ColumnProfile {
  readonly name: string;
  readonly type: string;
  /** How many rows have a value in it, how many have none, and the share that have one (0 to 1; 0 of no rows). */
  readonly filled: number;
  readonly empty: number;
  readonly share: number;
  /** How many different values it has. */
  readonly distinct: number;
  readonly smallest?: Scalar;
  readonly largest?: Scalar;
  /** Its name among the rows asked, where it is handed on by another (`name`): what its top values are asked of. */
  readonly column?: string;
}

/** The profile of a box's rows: how many, each column's, and how its number columns move together. */
export interface Profile {
  readonly rows: number;
  readonly columns: readonly ColumnProfile[];
  readonly correlations?: Correlations;
}

/**
 * HOW A BOX'S NUMBER COLUMNS MOVE TOGETHER (Alteryx's Pearson Correlation): each pair's Pearson correlation, from -1 to 1,
 * the database's `corr` -- a matrix, each column with itself too (1, or empty where it does not vary), empty where a
 * column does not vary.
 */
export interface Correlations {
  readonly columns: readonly string[];
  readonly r: readonly (readonly (number | null)[])[];
}

/** How many number columns a profile correlates at most: the first of them, so its question stays one row of a few. */
export const CORRELATED = 10;

const ROWS = '__profile_rows__';
/** Each aggregate's column: the column's place, and what it is (`c3_filled`), never its name, which may be anything. */
const named = (i: number, what: 'filled' | 'empty' | 'share' | 'distinct' | 'smallest' | 'largest'): string => `c${i}_${what}`;

/** Whether a column's values have an order its smallest and largest are taken in. */
const ordered = (type: string): boolean => {
  const family = familyOf(type);
  return family === 'numeric' || family === 'text' || family === 'temporal';
};

/**
 * THE PROFILE'S QUESTION over `rows` (a relation: the box's rows, query.ts classLambda's): one row -- every row
 * aggregated, no key (`aggregate`: a row of no rows too) -- its count of rows, and for each column its values counted
 * (`count`, which counts the values there are), its different values counted (`distinct`, then `count`), and, where its
 * values have an order, its `min` and `max`; then each column's empties (its rows less its values) and its share filled
 * (its values over its rows, 0 of none).
 */
export function profileLambda(rows: ValueSpecification, columns: readonly ProfiledColumn[]): Lambda {
  const x = (c: string): ValueSpecification => property(variable('x'), c);
  const y = variable('y');
  const specs: ColSpec[] = [agg(ROWS, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', y)))];
  columns.forEach((c, i) => {
    specs.push(agg(named(i, 'filled'), lambda(['x'], x(c.name)), lambda(['y'], fn('count', y))));
    specs.push(agg(named(i, 'distinct'), lambda(['x'], x(c.name)), lambda(['y'], fn('count', fn('distinct', y)))));
    if (ordered(c.type)) {
      specs.push(agg(named(i, 'smallest'), lambda(['x'], x(c.name)), lambda(['y'], fn('min', y))));
      specs.push(agg(named(i, 'largest'), lambda(['x'], x(c.name)), lambda(['y'], fn('max', y))));
    }
  });
  const counted = (c: string): ValueSpecification => property(variable('x'), c);
  return from(rows)
    .apply('aggregate', colSpecs(specs))
    .extend(columns.flatMap((_, i) => [
      derive(named(i, 'empty'), lambda(['x'], minus(counted(ROWS), counted(named(i, 'filled'))))),
      derive(named(i, 'share'), lambda(['x'], fn('if', eq(counted(ROWS), lit.integer(0)), lambda([], lit.float(0)),
        lambda([], divide(counted(named(i, 'filled')), counted(ROWS)))))),
    ]))
    .lambda();
}

/** A value of a column and how many rows have it: one of its most frequent (Alteryx's Field Summary's top values). */
export interface TopValue {
  readonly value: Scalar;
  readonly count: number;
}

/** How many of a column's most frequent values are asked. */
export const TOP_VALUES = 10;

const TOP_COUNT = '__top_count__';

/**
 * A COLUMN'S MOST FREQUENT VALUES over `rows`: its rows grouped by it, each value's rows counted by the database, the
 * most first and, of as many, the smallest value first (an empty last, as Pure's empty is the largest) -- `n` of them.
 * An empty value is a value here: how many rows have none is among them.
 */
export function topValuesLambda(rows: ValueSpecification, column: string, n: number = TOP_VALUES): Lambda {
  return from(rows)
    .groupBy([column], [agg(TOP_COUNT, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .sort([desc(TOP_COUNT), asc(column)])
    .limit(n)
    .lambda();
}

/** The top values read back off their question's rows (`table`), the column's values named `column`: thrown where a
 *  count is none (AGENTS.md invariant 4). */
export function topValuesOf(table: ResultTable, column: string): TopValue[] {
  const values = table.columns.find((c) => c.name === column)?.values;
  const counts = table.columns.find((c) => c.name === TOP_COUNT)?.values;
  if (values === undefined || counts === undefined) throw new Error(`the top values' answer has no column ${values === undefined ? column : TOP_COUNT}`);
  return counts.map((n, i) => {
    if (n === null || !Number.isFinite(Number(n))) throw new Error(`the top values' answer has no count for its row ${i + 1}`);
    return { value: values[i] ?? null, count: Number(n) };
  });
}

/** Each pair's correlation's column: the two columns' places, never their names. */
const pair = (i: number, j: number): string => `r${i}_${j}`;

/**
 * THE CORRELATIONS' QUESTION over `rows`: one row -- every row aggregated, no key -- each pair of `columns` (numbers, by
 * name), each with itself too, mapped a row each and reduced by `corr`, as a grid's paired measure is (query.ts
 * aggregateSpec): whether a column varies is the database's to say (its own correlation none where it does not).
 */
export function correlationLambda(rows: ValueSpecification, columns: readonly string[]): Lambda {
  const x = (c: string): ValueSpecification => property(variable('x'), c);
  const specs: ColSpec[] = [];
  for (let i = 0; i < columns.length; i++) {
    for (let j = i; j < columns.length; j++) {
      specs.push(agg(pair(i, j), lambda(['x'], fn('rowMapper', x(columns[i]!), x(columns[j]!))), lambda(['y'], fn('corr', variable('y')))));
    }
  }
  return from(rows).apply('aggregate', colSpecs(specs)).lambda();
}

/** The one row a question of aggregates answers (`table`), or thrown: a keyless aggregate has one row, of no rows too. */
function oneRow(table: ResultTable, what: string): (name: string) => Scalar {
  if (table.rowCount !== 1) throw new Error(`${what} is one row, not ${table.rowCount}`);
  return (name) => {
    const c = table.columns.find((col) => col.name === name);
    if (c === undefined) throw new Error(`${what} has no column ${name}`);
    return c.values[0] ?? null;
  };
}

/** The correlations read back off their question's one row (`table`), each column named as given (`names`). */
export function correlationsOf(table: ResultTable, names: readonly string[]): Correlations {
  const row = oneRow(table, "the correlations' answer");
  const value = (name: string): number | null => {
    const v = row(name);
    return v === null || !Number.isFinite(Number(v)) ? null : Number(v);
  };
  // a column with itself: 1, as a correlation is with itself, where the database says it varies -- its own `corr` is
  // that 1 to a double's last place (0.9999999999999998), so compared, never shown -- and none where it does not
  return {
    columns: names,
    r: names.map((_, i) => names.map((__, j) => (i === j ? (value(pair(i, i)) === null ? null : 1) : value(i < j ? pair(i, j) : pair(j, i))))),
  };
}

/** The question that types the rows profiled: none of them (`limit(0)`), its columns typed by the plan -- as an
 *  Output's (query.ts columnsLambda), written here so a Profile fetches no query code. */
export function columnsLambda(rows: ValueSpecification): Lambda {
  return from(rows).limit(0).lambda();
}

/** The profile read back off its question's one row (`table`): thrown when the answer is not that row, or a count in it
 *  is none (AGENTS.md invariant 4: never read as 0). */
export function profileOf(table: ResultTable, columns: readonly ProfiledColumn[]): Profile {
  const value = oneRow(table, "the profile's answer");
  const count = (name: string): number => {
    const v = value(name);
    if (v === null || !Number.isFinite(Number(v))) throw new Error(`the profile's answer has no count ${name}`);
    return Number(v);
  };
  return {
    rows: count(ROWS),
    columns: columns.map((c, i) => ({
      name: c.name,
      type: c.type,
      filled: count(named(i, 'filled')),
      empty: count(named(i, 'empty')),
      share: count(named(i, 'share')),
      distinct: count(named(i, 'distinct')),
      ...(ordered(c.type) ? { smallest: value(named(i, 'smallest')), largest: value(named(i, 'largest')) } : {}),
    })),
  };
}
