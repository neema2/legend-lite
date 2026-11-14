// A SAMPLE BY ROW NUMBER (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Sample box, Alteryx's Sample -- its N per
// group, Skip first N and 1 of every N): what the grid's Top N cannot say, said as Unique says a key's first row -- a
// row number in an order (a window column, `rowNumber`, restarting for each group when there are groups), a filter on
// it, and the number hidden: its first or last N of each group (`<= n`, numbered rising or falling), every row after
// the first N (`> n`), or one row of every N (a remainder of the number, hidden too: `mod($x.row, n) == 1`). The
// first or last N of all the rows is the grid's Top N (ui/sample-form.ts), which keeps rows that tie. Or, Alteryx's
// Select Records, the rows at some places (`1-10, 20, 30+`): a filter on the number of each range, or-ed. Or a Random %
// Sample, the same rows each time it runs: each row's number in an order of every column, its chance the database's hash
// of it and a seed (`mod(abs(hashCode([n, seed])), 100)`, hidden), kept under the percent. Or Alteryx's Oversample Field:
// every row of a value of interest kept, and of the others as many as make it that percent of the rows -- each other row
// numbered in the order of its hash and the seed, kept while its number is at most the count of interest's share
// (`interest == 1 || rank <= count * (100 - p) / p`, the count a window sum, all hidden). No value is computed here: the
// database numbers, hashes, counts and keeps the rows. What a box says is read back off its query (`rowSampleOf`).
//
// One rule for the box's window and Python's grid.sample (headless gridPreset).
//
// PURE: no DOM, no planner.

import { and, collection, divide, eq, fn, lambda, le, lit, or, property, times, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { literalNode } from './query.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn, type FilterNode, type FilterValue, type SortSpec } from './snapshot.ts';
import { ORDERED, rowNumberBy, rowNumberOrder } from './unique.ts';

/** How it samples: the first or last `n` of each group, every row after the first `n`, one row of every `n`, or the rows
 *  at some places (`ranges`). */
export type RowSampleKind = 'first' | 'last' | 'skip' | 'every' | 'records' | 'random' | 'oversample';
export const ROW_SAMPLE_KINDS: readonly RowSampleKind[] = ['first', 'last', 'skip', 'every', 'records', 'random', 'oversample'];

/** The rows from one place to another (its first row 1), or from it on: `20` is from 20 to 20, `30+` from 30 on. */
export interface RecordRange {
  readonly from: number;
  readonly to?: number;
}

/** A sample by row number, as its window and Python set it: its rows numbered in the order of `by`, within `groups`. */
export interface RowSample {
  readonly kind: RowSampleKind;
  readonly n: number;
  readonly by: string;
  readonly groups: readonly string[];
  /** The places a Select Records keeps; no other kind's. */
  readonly ranges?: readonly RecordRange[];
  /** A random sample's seed, an oversample's: the same seed, the same rows (its `n` its percent); no other kind's. */
  readonly seed?: number;
  /** An oversample's value of interest, of the column `by`: every row of it kept, the others sampled so it is `n`% of
   *  the rows; no other kind's. */
  readonly value?: FilterValue;
}

/** Ranges as they are written: `1-10, 20, 30+`, `-5` the first 5 -- refused in words. */
export function readRanges(text: string): RecordRange[] {
  const parts = text.split(',').map((p) => p.trim()).filter((p) => p !== '');
  if (parts.length === 0) throw new CubeRefusal('name the rows it keeps: 1-10, 20, 30+');
  return parts.map((p) => {
    const m = /^(\d*)\s*(-|\+)?\s*(\d*)$/.exec(p);
    const [from, sign, to] = m === null ? [] : [m[1]!, m[2], m[3]!];
    if (m !== null && sign === undefined && from !== '' && to === '') return { from: Number(from), to: Number(from) };
    if (m !== null && sign === '+' && from !== '' && to === '') return { from: Number(from) };
    if (m !== null && sign === '-' && (from !== '' || to !== '') && to !== '') return { from: from === '' ? 1 : Number(from), to: Number(to) };
    throw new CubeRefusal(`'${p}' is no range of rows: write 1-10, 20, 30+ or -5`);
  });
}

/** Ranges written as they are read: `1-10, 20, 30+`. */
export function rangesWords(ranges: readonly RecordRange[]): string {
  return ranges.map((r) => (r.to === undefined ? `${r.from}+` : r.to === r.from ? `${r.from}` : `${r.from}-${r.to}`)).join(', ');
}

/** What its row number is called (else this and `_2`, `_3`... where the grid has the name), and its remainder. */
export const SAMPLE_COLUMN = 'SampleRow';
const EVERY_COLUMN = 'SampleEvery';
const CHANCE_COLUMN = 'SampleChance';
const EMPTY_COLUMN = 'SampleEmpty';
const INTEREST_COLUMN = 'SampleInterest';
const RANK_COLUMN = 'SampleRank';
const COUNT_COLUMN = 'SampleCount';
const KEEP_COLUMN = 'SampleKeep';

/** What is wrong with a row sample's settings, in words, if anything: the grid's columns are `s`'s. */
export function rowSampleProblem(s: CubeSnapshot, t: RowSample): string | undefined {
  if (!ROW_SAMPLE_KINDS.includes(t.kind)) return `a sample is ${ROW_SAMPLE_KINDS.join(', ')}, not ${JSON.stringify(t.kind)}`;
  if (t.kind === 'oversample') {
    if (!Number.isInteger(t.n) || t.n < 1 || t.n > 99) return `an oversample makes its value a whole percent of the rows, from 1 to 99, not ${t.n}`;
    if (t.seed === undefined || !Number.isInteger(t.seed) || t.seed < 0) return `its seed is a whole number from 0, not ${String(t.seed)}`;
    if (t.groups.length > 0) return 'an oversample is of all the rows';
    if (!rowColumns(s).some((c) => c.name === t.by)) return `the grid has no column '${t.by}'`;
    if (t.value === undefined || t.value === null) return 'name the value it keeps every row of';
    if (!rowColumns(s).some((c) => ORDERED.has(familyOf(c.type) ?? ''))) return 'it numbers its rows by a column that sorts: the grid has none';
    return undefined;
  }
  if (t.kind === 'random') {
    if (!Number.isInteger(t.n) || t.n < 1 || t.n > 99) return `a random sample keeps a whole percent of the rows, from 1 to 99, not ${t.n}`;
    if (t.seed === undefined || !Number.isInteger(t.seed) || t.seed < 0) return `its seed is a whole number from 0, not ${String(t.seed)}`;
    if (t.groups.length > 0) return 'a random sample is of all the rows: the same percent of each group is the same percent of all';
    if (!rowColumns(s).some((c) => ORDERED.has(familyOf(c.type) ?? ''))) return 'it numbers its rows by a column that sorts: the grid has none';
    return undefined;
  }
  if (!Number.isInteger(t.n) || t.n < 1) return `it counts rows by a whole number from 1, not ${t.n}`;
  if (t.kind === 'every' && t.n < 2) return 'one row of every 1 is every row: take a number from 2';
  const have = new Set(rowColumns(s).map((c) => c.name));
  const odd = [t.by, ...t.groups].filter((c) => !have.has(c));
  if (odd.length > 0) return `the grid has no column ${odd.map((c) => `'${c}'`).join(', ')}`;
  if (new Set(t.groups).size !== t.groups.length) return 'it groups by each column once';
  if ((t.kind === 'first' || t.kind === 'last') && t.groups.length === 0) {
    return 'the first or last rows of all of them are its Top N: give it a group, or use the grid\'s Top N';
  }
  if (t.kind === 'records') {
    if (t.ranges === undefined || t.ranges.length === 0) return 'name the rows it keeps: 1-10, 20, 30+';
    const odd = t.ranges.find((r) => !Number.isInteger(r.from) || r.from < 1 || (r.to !== undefined && (!Number.isInteger(r.to) || r.to < r.from)));
    if (odd !== undefined) return `${rangesWords([odd])} is no range of rows: each from row 1, each to a row at or after its first`;
  }
  return undefined;
}

/**
 * The commands that make a grid a row sample (`s`: the grid as it is): its row number (per group), any remainder, its
 * filter (and-ed with any the grid has), the numbers hidden. Refused in words (rowSampleProblem).
 */
export function rowSampleCommands(s: CubeSnapshot, t: RowSample): GridCommand[] {
  const problem = rowSampleProblem(s, t);
  if (problem !== undefined) throw new CubeRefusal(problem);
  const taken = new Set([...rowColumns(s).map((c) => c.name), ...(s.groupDerived ?? []).map((d) => d.name)]);
  const free = (base: string): string => {
    let name = base;
    for (let i = 2; taken.has(name); i += 1) name = `${base}_${i}`;
    taken.add(name);
    return name;
  };
  const row = free(SAMPLE_COLUMN);
  // a random sample's rows numbered in an order of every column that sorts: the same number each time it runs; any
  // other's by its column, its empties last and its ties broken (unique.ts rowNumberOrder)
  const { order, flag } = t.kind === 'random' || t.kind === 'oversample'
    ? { order: rowColumns(s).filter((c) => ORDERED.has(familyOf(c.type) ?? '')).map((c): SortSpec => ({ column: c.name, direction: 'asc' })), flag: undefined }
    : rowNumberOrder(s, { column: t.by, direction: t.kind === 'last' ? 'desc' : 'asc' }, t.groups, () => free(EMPTY_COLUMN));
  const numbered: DerivedColumn = { name: row, window: { fn: 'rowNumber', partition: [...t.groups], order } };
  const commands: GridCommand[] = [
    ...(flag !== undefined ? [{ kind: 'calculate', level: 'row', column: flag } as const] : []),
    { kind: 'calculate', level: 'row', column: numbered },
  ];
  const hidden: Record<string, { readonly hidden: true }> = { [row]: { hidden: true }, ...(flag !== undefined ? { [flag.name]: { hidden: true } } : {}) };
  let condition: FilterNode;
  if (t.kind === 'oversample') {
    const x = (c: string): ValueSpecification => fn('toOne', property(variable('x'), c));
    const row_ = (name: string, body: ValueSpecification, kind: 'dimension' | 'measure' = 'dimension'): GridCommand =>
      ({ kind: 'calculate', level: 'row', column: { name, kind, lambda: lambda(['x'], body) } });
    // its order: the database's hash of each row's number and the seed
    const chance = free(CHANCE_COLUMN);
    commands.push(row_(chance, fn('abs', fn('hashCode', collection([x(row), lit.integer(t.seed!)])))));
    // 1 for a row of the value of interest, else 0 (an empty one, the others')
    const interest = free(INTEREST_COLUMN);
    const type = rowColumns(s).find((c) => c.name === t.by)?.type;
    const of = property(variable('x'), t.by);
    commands.push(row_(interest, fn('if', and(fn('isNotEmpty', of), eq(of, literalNode(t.value!, type))),
      lambda([], lit.integer(1)), lambda([], lit.integer(0)))));
    // each of the two numbered apart, in that order; the count of interest across every row
    const rank = free(RANK_COLUMN);
    commands.push({ kind: 'calculate', level: 'row', column: { name: rank, window: { fn: 'rowNumber', partition: [interest],
      order: [{ column: chance, direction: 'asc' }, { column: row, direction: 'asc' }] } } });
    const count = free(COUNT_COLUMN);
    commands.push({ kind: 'calculate', level: 'row', column: { name: count, window: { fn: 'sum', column: interest, partition: [], order: [], frame: 'partition' } } });
    // kept: every row of interest, and the others while their number is at most its share
    const keep = free(KEEP_COLUMN);
    commands.push(row_(keep, or(eq(x(interest), lit.integer(1)),
      le(x(rank), divide(times(x(count), lit.integer(100 - t.n)), lit.integer(t.n))))));
    for (const c of [chance, interest, rank, count, keep]) hidden[c] = { hidden: true };
    condition = { kind: 'condition', column: keep, operator: 'equal', value: true };
  } else if (t.kind === 'random') {
    // its chance, 0 to 99: the database's hash of its number and the seed
    const chance = free(CHANCE_COLUMN);
    commands.push({ kind: 'calculate', level: 'row', column: {
      name: chance, kind: 'dimension', lambda: lambda(['x'], fn('mod', fn('abs', fn('hashCode',
        collection([fn('toOne', property(variable('x'), row)), lit.integer(t.seed!)]))), lit.integer(100))),
    } });
    hidden[chance] = { hidden: true };
    condition = { kind: 'condition', column: chance, operator: 'lessThan', value: t.n };
  } else if (t.kind === 'records') {
    // each range a condition on the number -- one row, from one on, between two -- any of them keeping a row
    const of = (r: RecordRange): FilterNode => (r.to === r.from ? { kind: 'condition', column: row, operator: 'equal', value: r.from }
      : r.to === undefined ? { kind: 'condition', column: row, operator: 'greaterThanEqual', value: r.from }
        : { kind: 'and', children: [{ kind: 'condition', column: row, operator: 'greaterThanEqual', value: r.from },
          { kind: 'condition', column: row, operator: 'lessThanEqual', value: r.to }] });
    const each = t.ranges!.map(of);
    condition = each.length === 1 ? each[0]! : { kind: 'or', children: each };
  } else if (t.kind === 'every') {
    const every = free(EVERY_COLUMN);
    // the number's remainder by n: 1 for the first row and every nth after it (n is 2 or more)
    commands.push({ kind: 'calculate', level: 'row', column: {
      name: every, lambda: lambda(['x'], fn('mod', fn('toOne', property(variable('x'), row)), lit.integer(t.n))), kind: 'dimension',
    } });
    hidden[every] = { hidden: true };
    condition = { kind: 'condition', column: every, operator: 'equal', value: 1 };
  } else {
    condition = { kind: 'condition', column: row, operator: t.kind === 'skip' ? 'greaterThan' : 'lessThanEqual', value: t.n };
  }
  // one condition added as the screen adds one; a Select Records' ranges, a tree, and-ed in the same way (refilter)
  const filtered: GridCommand = condition.kind === 'condition' ? { kind: 'addFilter', condition } : { kind: 'refilter', remove: [], add: [condition] };
  return [...commands, filtered, { kind: 'configure', patch: { columns: hidden } }];
}

/**
 * A grid made a row sample, read off its query: its settings and its columns (the row number, any remainder) -- when its
 * filter is the sample's condition alone, over a row number in one order (and for one of every N, its remainder).
 */
export function rowSampleOf(query: { readonly derived: readonly DerivedColumn[]; readonly filter?: FilterNode | undefined }):
  (RowSample & { readonly columns: readonly string[] }) | undefined {
  const f = query.filter;
  // a row number in rowNumberOrder's order: its column, and any flag of its empties (its own column too)
  interface Numbered { readonly w: NonNullable<DerivedColumn['window']>; readonly by: SortSpec; readonly flag?: string }
  const numberOf = (name: string): Numbered | undefined => {
    const w = query.derived.find((d) => d.name === name)?.window;
    const ordered = w?.fn === 'rowNumber' ? rowNumberBy(w.order, query.derived) : undefined;
    return w === undefined || ordered === undefined ? undefined : { w, ...ordered };
  };
  const sample = (kind: RowSampleKind, n: number, r: Numbered, columns: string[]) =>
    ({ kind, n, by: r.by.column, groups: r.w.partition, columns: [...(r.flag !== undefined ? [r.flag] : []), ...columns] });
  // Select Records: its ranges, each over one row number numbered rising, any of them (an `or` of them, or one)
  const records = f === undefined ? undefined : recordsOf(f);
  const counted = records === undefined ? undefined : numberOf(records.column);
  if (records !== undefined && counted !== undefined) {
    return counted.by.direction === 'desc' ? undefined
      : { ...sample('records', records.ranges.length, counted, [records.column]), ranges: records.ranges };
  }
  // an oversample: its keep, true -- read back off its columns (oversampleOf)
  if (f?.kind === 'condition' && f.operator === 'equal' && f.value === true) return oversampleOf(query.derived, f.column);
  if (f?.kind !== 'condition' || typeof f.value !== 'number') return undefined;
  // a random sample: its chance under the percent -- the hash of a row number and its seed
  if (f.operator === 'lessThan') {
    const read = chanceOf(query.derived.find((d) => d.name === f.column));
    const w = read === undefined ? undefined : query.derived.find((d) => d.name === read.row)?.window;
    return read === undefined || w?.fn !== 'rowNumber' ? undefined
      : { kind: 'random', n: f.value, by: '', groups: [], seed: read.seed, columns: [read.row, f.column] };
  }
  if (f.operator === 'lessThanEqual' || f.operator === 'greaterThan') {
    const r = numberOf(f.column);
    if (r === undefined) return undefined;
    const desc = r.by.direction === 'desc';
    if (f.operator === 'greaterThan') return desc ? undefined : sample('skip', f.value, r, [f.column]);
    // the first of all the rows is the Top N's; a first of each group, a sample's
    return r.w.partition.length === 0 ? undefined : sample(desc ? 'last' : 'first', f.value, r, [f.column]);
  }
  if (f.operator !== 'equal' || f.value !== 1) return undefined;
  // one of every N: its remainder is mod(toOne($x.row), n) of a row number
  const d = query.derived.find((c) => c.name === f.column);
  const body = d?.lambda?.body;
  const param = d?.lambda?.parameters[0]?.name;
  if (body?.length !== 1 || param === undefined) return undefined;
  const call = body[0]!;
  if (call._type !== 'func' || call.function !== 'mod' || call.parameters.length !== 2) return undefined;
  const [one, by] = call.parameters;
  if (by?._type !== 'integer' || one?._type !== 'func' || one.function !== 'toOne') return undefined;
  const p = one.parameters[0];
  if (p?._type !== 'property' || p.parameters[0]?._type !== 'var' || (p.parameters[0] as { name: string }).name !== param) return undefined;
  const r = numberOf(p.property);
  return r === undefined || r.by.direction === 'desc' ? undefined : sample('every', Number(by.value), r, [p.property, f.column]);
}

/**
 * An oversample read back off its keep column (`keep`): `toOne($x.interest) == 1 || toOne($x.rank) <= [toOne($x.count),
 * 100 - n]->times() / n`, its interest `if($x.by->isNotEmpty() && $x.by == value, 1, 0)`, its rank a row number apart by
 * interest in the order of its chance and its row, its count a sum of the interest, its chance the hash of the row and
 * the seed -- each as rowSampleCommands writes it; undefined for anything else.
 */
function oversampleOf(derived: readonly DerivedColumn[], keep: string):
  (RowSample & { readonly columns: readonly string[] }) | undefined {
  const named = (name: string | undefined): DerivedColumn | undefined => (name === undefined ? undefined : derived.find((d) => d.name === name));
  const call = (v: ValueSpecification | undefined, name: string, arity: number): ValueSpecification[] | undefined =>
    (v?._type === 'func' && v.function === name && v.parameters.length === arity ? [...v.parameters] : undefined);
  const column = (v: ValueSpecification | undefined): string | undefined => {
    const [p] = call(v, 'toOne', 1) ?? [v];
    return p?._type === 'property' && p.parameters[0]?._type === 'var' ? p.property : undefined;
  };
  const int = (v: ValueSpecification | undefined): number | undefined => (v?._type === 'integer' ? Number((v as { value: unknown }).value) : undefined);
  const k = named(keep)?.lambda?.body;
  if (k?.length !== 1) return undefined;
  const [isIt, share] = call(k[0], 'or', 2) ?? [];
  const [interestRef, one] = call(isIt, 'equal', 2) ?? [];
  const [rankRef, part] = call(share, 'lessThanEqual', 2) ?? [];
  const [timesOf, n] = call(part, 'divide', 2) ?? [];
  const [product] = call(timesOf, 'times', 1) ?? [];
  const [countRef, rest] = product?._type === 'collection' && product.values.length === 2 ? product.values : [];
  const interest = named(column(interestRef));
  const rank = named(column(rankRef));
  const count = named(column(countRef));
  const pct = int(n);
  if (int(one) !== 1 || pct === undefined || int(rest) !== 100 - pct || interest === undefined || rank === undefined || count === undefined) return undefined;
  if (count.window?.fn !== 'sum' || count.window.column !== interest.name) return undefined;
  const w = rank.window;
  if (w?.fn !== 'rowNumber' || w.partition.length !== 1 || w.partition[0] !== interest.name || w.order.length !== 2) return undefined;
  const chance = named(w.order[0]!.column);
  const row = named(w.order[1]!.column);
  // its chance: abs(hashCode([toOne($x.row), seed]))
  const [hash] = call(chance?.lambda?.body[0], 'abs', 1) ?? [];
  const [list] = call(hash, 'hashCode', 1) ?? [];
  const seed = list?._type === 'collection' && column(list.values[0]) === row?.name ? int(list.values[1]) : undefined;
  // its interest: if(isNotEmpty($x.by) && $x.by == value, 1, 0)
  const [test] = call(interest.lambda?.body[0], 'if', 3) ?? [];
  const [, same] = call(test, 'and', 2) ?? [];
  const [by, value] = call(same, 'equal', 2) ?? [];
  const on = column(by);
  const literal = value !== undefined && (value._type === 'string' || value._type === 'integer' || value._type === 'float' || value._type === 'boolean')
    ? (value as { value: unknown }).value : undefined;
  if (seed === undefined || on === undefined || row?.window?.fn !== 'rowNumber' || literal === undefined) return undefined;
  const given: FilterValue = typeof literal === 'object' ? String(literal) : literal as FilterValue;
  return { kind: 'oversample', n: pct, by: on, groups: [], seed, value: given, columns: [row.name, chance!.name, interest.name, rank.name, count.name, keep] };
}

/** A random sample's chance read back: `mod(abs(hashCode([toOne($x.<row>), <seed>])), 100)` -- its row number's column
 *  and its seed; undefined for any other column. */
function chanceOf(d: DerivedColumn | undefined): { readonly row: string; readonly seed: number } | undefined {
  const param = d?.lambda?.parameters[0]?.name;
  const body = d?.lambda?.body;
  if (param === undefined || body?.length !== 1) return undefined;
  const call = (v: ValueSpecification | undefined, name: string, arity: number): ValueSpecification[] | undefined =>
    (v?._type === 'func' && v.function === name && v.parameters.length === arity ? [...v.parameters] : undefined);
  const [abs, by] = call(body[0], 'mod', 2) ?? [];
  if (by?._type !== 'integer' || Number((by as { value: unknown }).value) !== 100) return undefined;
  const [hash] = call(abs, 'abs', 1) ?? [];
  const [list] = call(hash, 'hashCode', 1) ?? [];
  if (list?._type !== 'collection') return undefined;
  const [one, seed] = list.values;
  const [p] = call(one, 'toOne', 1) ?? [];
  if (p?._type !== 'property' || p.parameters[0]?._type !== 'var' || (p.parameters[0] as { name: string }).name !== param) return undefined;
  return seed?._type === 'integer' ? { row: p.property, seed: Number((seed as { value: unknown }).value) } : undefined;
}

/** A Select Records' filter read back: its ranges and the column they are of -- an equal (one row), a `>=` (from one
 *  on), a `>=` and a `<=` (between two), or an `or` of them, all of one column; undefined for any other filter. The
 *  first N alone (`<=`) and all but the first N (`>`) are the other kinds'. */
function recordsOf(f: FilterNode): { readonly column: string; readonly ranges: RecordRange[] } | undefined {
  const one = (n: FilterNode): { column: string; range: RecordRange } | undefined => {
    if (n.kind === 'condition' && typeof n.value === 'number' && n.operator === 'equal') return { column: n.column, range: { from: n.value, to: n.value } };
    if (n.kind === 'condition' && typeof n.value === 'number' && n.operator === 'greaterThanEqual') return { column: n.column, range: { from: n.value } };
    if (n.kind !== 'and' || n.children.length !== 2) return undefined;
    const [a, b] = n.children as [FilterNode, FilterNode];
    return a.kind === 'condition' && b.kind === 'condition' && a.column === b.column && a.operator === 'greaterThanEqual'
      && b.operator === 'lessThanEqual' && typeof a.value === 'number' && typeof b.value === 'number'
      ? { column: a.column, range: { from: a.value, to: b.value } } : undefined;
  };
  const each = (f.kind === 'or' ? f.children : [f]).map(one);
  const column = each[0]?.column;
  if (column === undefined || each.some((e) => e === undefined || e.column !== column)) return undefined;
  // one row alone, a remainder's 1, is one of every N's: its column says which (a row number is a Select Records')
  return { column, ranges: each.map((e) => e!.range) };
}

/** The commands that take a row sample off (`was`: rowSampleOf's): its filter's condition, then its columns. */
export function rowSampleOffCommands(query: { readonly filter?: FilterNode | undefined }, was: { readonly columns: readonly string[] }): GridCommand[] {
  return [
    ...(query.filter === undefined ? [] : [{ kind: 'refilter', remove: [query.filter], add: [] } as const]),
    ...[...was.columns].reverse().map((name): GridCommand => ({ kind: 'removeCalculated', name })),
  ];
}

/**
 * A GRID SAMPLED AS ASKED, from whatever sample it has (`s`): the sample it has taken off (a Top N, a row sample), then
 * the one asked -- the first or last `n` of all its rows its Top N (which keeps rows that tie: the smallest first), any
 * other a row sample (rowSampleCommands) -- or none (`t` null): every row again. The window's OK and Keep every row, and
 * Python's grid.sample.
 */
export function sampleCommands(s: CubeSnapshot, t: RowSample | null): GridCommand[] {
  const was = rowSampleOf(s);
  const off: GridCommand[] = [
    ...(s.topN !== undefined ? [{ kind: 'topN', topN: null } as const] : []),
    ...(was === undefined ? [] : rowSampleOffCommands(s, was)),
  ];
  if (t === null) return off;
  if ((t.kind === 'first' || t.kind === 'last') && t.groups.length === 0) {
    if (!Number.isInteger(t.n) || t.n < 1) throw new CubeRefusal(`it counts rows by a whole number from 1, not ${t.n}`);
    if (!rowColumns(s).some((c) => c.name === t.by)) throw new CubeRefusal(`the grid has no column '${t.by}'`);
    return [...off, { kind: 'topN', topN: { by: t.by, n: t.n, ...(t.kind === 'first' ? { bottom: true } : {}) } }];
  }
  // its names judged as the grid will be once the old sample's columns are gone
  const without = was === undefined ? s : { ...s, derived: s.derived.filter((d) => !was.columns.includes(d.name)) };
  return [...off, ...rowSampleCommands(without, t)];
}

/** A row sample's words, as its box says it: `first 3 of each region by notional`, `one of every 10 by trade_id`. */
export function rowSampleWords(t: RowSample): string {
  const of = t.groups.length === 0 ? '' : ` of each ${t.groups.join(', ')}`;
  switch (t.kind) {
    case 'first': return `first ${t.n}${of} by ${t.by}`;
    case 'last': return `last ${t.n}${of} by ${t.by}`;
    case 'skip': return `all but the first ${t.n}${of} by ${t.by}`;
    case 'every': return `one of every ${t.n}${of} by ${t.by}`;
    case 'records': return `rows ${rangesWords(t.ranges ?? [])}${of} by ${t.by}`;
    case 'random': return `${t.n}% at random, seed ${t.seed ?? 0}`;
    case 'oversample': return `${t.by} ${String(t.value)} made ${t.n}% of the rows, seed ${t.seed ?? 0}`;
  }
}

/** A sample read from JSON (Python's): `{kind: 'sample', mode, n, by, groups?}`, or `{kind: 'sample', mode: null}` for
 *  every row again (null) -- or refused in words. */
export function readRowSample(raw: unknown): RowSample | null {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'sample') throw new CubeRefusal(`a sample is {kind: 'sample', mode, n, by, groups}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'mode', 'n', 'by', 'groups', 'ranges', 'seed', 'value'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a sample takes mode, n, by, groups, ranges, seed and value, not ${odd.join(', ')}`);
  const mode = o['mode'];
  if (mode === null) {
    if (Object.keys(o).some((k) => k !== 'kind' && k !== 'mode')) throw new CubeRefusal('every row again takes nothing more');
    return null;
  }
  if (!(ROW_SAMPLE_KINDS as readonly unknown[]).includes(mode)) throw new CubeRefusal(`its mode is ${ROW_SAMPLE_KINDS.join(', ')}, not ${JSON.stringify(mode)}`);
  // a random sample: its percent and its seed, nothing it is ordered by
  if (mode === 'random') {
    const n = o['n'];
    const seed = o['seed'] ?? 0;
    if (typeof n !== 'number' || typeof seed !== 'number') throw new CubeRefusal(`a random sample's n is its percent and its seed a number, not ${JSON.stringify({ n, seed })}`);
    return { kind: 'random', n, by: '', groups: [], seed };
  }
  // an oversample: its percent, the column and its value of interest, its seed
  if (mode === 'oversample') {
    const { n, by, value } = o;
    const seed = o['seed'] ?? 0;
    if (typeof n !== 'number' || typeof seed !== 'number' || typeof by !== 'string') {
      throw new CubeRefusal(`an oversample's n is its percent, its by a column's name and its seed a number, not ${JSON.stringify({ n, by, seed })}`);
    }
    if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') {
      throw new CubeRefusal(`its value is the value it keeps every row of: text, a number or true or false, not ${JSON.stringify(value)}`);
    }
    return { kind: 'oversample', n, by, groups: [], seed, value };
  }
  // Select Records: its ranges written as text (1-10, 20, 30+), no n
  const ranges = o['ranges'];
  if (mode === 'records' || ranges !== undefined) {
    if (mode !== 'records') throw new CubeRefusal(`only a Select Records ('records') keeps ranges of rows, not '${String(mode)}'`);
    if (typeof ranges !== 'string') throw new CubeRefusal(`its ranges are written as text, 1-10, 20, 30+, not ${JSON.stringify(ranges)}`);
    const by = o['by'];
    if (typeof by !== 'string') throw new CubeRefusal(`its by is a column's name, not ${JSON.stringify(by)}`);
    const groups = o['groups'] ?? [];
    if (!Array.isArray(groups) || !groups.every((g): g is string => typeof g === 'string')) {
      throw new CubeRefusal(`its groups are a list of column names, not ${JSON.stringify(groups)}`);
    }
    const read = readRanges(ranges);
    return { kind: 'records', n: read.length, by, groups, ranges: read };
  }
  const n = o['n'];
  if (typeof n !== 'number') throw new CubeRefusal(`its n is a number, not ${JSON.stringify(n)}`);
  const by = o['by'];
  if (typeof by !== 'string') throw new CubeRefusal(`its by is a column's name, not ${JSON.stringify(by)}`);
  const groups = o['groups'] ?? [];
  if (!Array.isArray(groups) || !groups.every((g): g is string => typeof g === 'string')) {
    throw new CubeRefusal(`its groups are a list of column names, not ${JSON.stringify(groups)}`);
  }
  return { kind: mode as RowSampleKind, n, by, groups };
}
