// A FLOW'S GENERATE ROWS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3, Alteryx's Generate Rows): each row repeated once for
// each whole number from one to another -- numbers given, or the row's own columns -- or once for each day between two of
// its date columns, a column of each repeat's number or day beside it. Said as what a grid already does: an exploded
// calculated column (DerivedColumn.unnest: `lateral(x|<list>->flatten(~name))`) of the database's own `range` of
// integers, a day the first one `adjust`ed by each; a row whose bound is empty repeated no time. Nothing is counted
// here: the database makes every row. Read back off its columns (`generatedOf`): rebuilt and compared whole.
//
// One rule for the box's window and Python's grid.generate_rows (headless gridPreset).
//
// PURE: no DOM, no planner.

import { collection, enumValue, fn, lambda, lit, plus, property, toJson, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, hasTimeOfDay, isFractional, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** A bound of the numbers: a whole number given, or a whole-number column of the row's. */
export type Bound = number | { readonly column: string };

/** Generate Rows, as its window sets it: its new column's name, and its numbers or its days. */
export type GenerateRows =
  | { readonly kind: 'numbers'; readonly name: string; readonly from: Bound; readonly to: Bound; readonly step?: number }
  | { readonly kind: 'days'; readonly name: string; readonly from: string; readonly to: string };

/** The most rows one row is repeated into by numbers given: a run of a few, not a table of its own. */
export const MAX_REPEATS = 100_000;

const x = (column: string): ValueSpecification => property(variable('x'), column);
const one = (column: string): ValueSpecification => fn('toOne', x(column));
const DAYS = enumValue('meta::pure::functions::date::DurationUnit', 'DAYS');
/** A days' repeat's number, from 0: hidden. */
export const dayName = (name: string): string => `${name}__day`;

/** `x | if(<a bound empty>, |[], |<list>)`: no repeat of a row whose bound is empty; a list when none can be. */
function guarded(columns: readonly string[], list: ValueSpecification): Lambda {
  if (columns.length === 0) return lambda(['x'], list);
  const empty = columns.map((c) => fn('isEmpty', x(c)));
  const any = empty.length === 1 ? empty[0]! : fn('or', empty[0]!, empty[1]!);
  return lambda(['x'], fn('if', any, lambda([], collection([])), lambda([], list)));
}

const boundOf = (b: Bound): ValueSpecification => (typeof b === 'number' ? lit.integer(b) : one(b.column));

/** The columns a Generate Rows makes, by the rule (none of them checked: generateRowsCommands judges what it is given). */
export function generatedColumns(g: GenerateRows): DerivedColumn[] {
  if (g.kind === 'numbers') {
    const step = g.step ?? 1;
    const stop = plus(boundOf(g.to), lit.integer(1));
    const list = step === 1 ? fn('range', boundOf(g.from), stop) : fn('range', boundOf(g.from), stop, lit.integer(step));
    const columns = [g.from, g.to].flatMap((b) => (typeof b === 'number' ? [] : [b.column]));
    return [{ name: g.name, lambda: guarded([...new Set(columns)], list), unnest: true, kind: 'dimension' }];
  }
  const days = fn('range', lit.integer(0), plus(fn('dateDiff', one(g.from), one(g.to), DAYS), lit.integer(1)));
  return [
    { name: dayName(g.name), lambda: guarded([...new Set([g.from, g.to])], days), unnest: true, kind: 'dimension' },
    { name: g.name, lambda: lambda(['x'], fn('adjust', one(g.from), one(dayName(g.name)), DAYS)), kind: 'dimension' },
  ];
}

/**
 * The commands that repeat a grid's rows (`given`: the grid as it is): its columns (a days' number hidden) -- one step; a
 * Generate Rows already, set again, its own columns taken off first.
 * Refused in words: a name no column may have or one taken, a bound that is no whole number or no whole-number column,
 * numbers given the wrong way round or too many, a step no whole number from 1, a days' bound that is no date column.
 */
export function generateRowsCommands(given: CubeSnapshot, g: GenerateRows): GridCommand[] {
  // set again: its own columns taken off first, in the same step
  const was = generatedOf(given.derived);
  const own = new Set(was === undefined ? [] : generatedColumns(was).map((c) => c.name));
  const s: CubeSnapshot = { ...given, derived: given.derived.filter((d) => !own.has(d.name)) };
  const off = given.derived.filter((d) => own.has(d.name)).reverse().map((d): GridCommand => ({ kind: 'removeCalculated', name: d.name }));
  const name = g.name.trim();
  // a name the grid has not (its calculate refuses any other name a column may not have, in the same words)
  if (name === '') throw new CubeRefusal('a calculated column needs a name');
  const types = new Map(rowColumns(s).map((c) => [c.name, c.type] as const));
  if (types.has(name) || (s.groupDerived ?? []).some((d) => d.name === name)) throw new CubeRefusal(`'${name}' is already a column`);
  const has = (column: string): string | undefined => {
    if (!types.has(column)) throw new CubeRefusal(`the grid has no column '${column}'`);
    return types.get(column);
  };
  if (g.kind === 'numbers') {
    for (const b of [g.from, g.to]) {
      if (typeof b === 'number') {
        if (!Number.isInteger(b)) throw new CubeRefusal(`a bound is a whole number, not ${b}`);
      } else {
        const type = has(b.column);
        if (familyOf(type) !== 'numeric' || isFractional(type)) throw new CubeRefusal(`'${b.column}' is ${type ?? 'not typed yet'}: a bound is a whole number`);
      }
    }
    const step = g.step ?? 1;
    if (!Number.isInteger(step) || step < 1) throw new CubeRefusal(`a step is a whole number from 1, not ${step}`);
    if (typeof g.from === 'number' && typeof g.to === 'number') {
      if (g.from > g.to) throw new CubeRefusal(`from ${g.from} to ${g.to}: its first is after its last`);
      if ((g.to - g.from) / step + 1 > MAX_REPEATS) throw new CubeRefusal(`from ${g.from} to ${g.to}: more than ${MAX_REPEATS.toLocaleString('en-US')} rows of each`);
    }
  } else {
    for (const c of [g.from, g.to]) {
      const type = has(c);
      if (!isTemporal(type) || isTimeOfDay(type)) throw new CubeRefusal(`'${c}' is ${type ?? 'not typed yet'}: the days are between two date columns`);
      // a moment's days counted from its time of day would run a day past the last (23:00 to 01:00 the next day: two
      // days, the second after it -- the correctness audit's 12, 2026-10-26): its date taken first
      if (hasTimeOfDay(type)) throw new CubeRefusal(`'${c}' is a date and a time: the days are between two days -- take it to its day first (a DateTime box, by Day)`);
    }
    const helper = dayName(name);
    if (types.has(helper)) throw new CubeRefusal(`'${helper}' is already a column`);
  }
  const columns = generatedColumns({ ...g, name });
  return [
    ...off,
    ...columns.map((column): GridCommand => ({ kind: 'calculate', level: 'row', column })),
    ...(g.kind === 'days' ? [{ kind: 'configure', patch: { columns: { [dayName(name)]: { hidden: true } } } } as const] : []),
  ];
}

/** A bound read off a `range`'s argument: a number given, or `toOne($x.c)`. */
function boundRead(v: ValueSpecification | undefined): Bound | undefined {
  const o = v as { _type?: string; value?: unknown; function?: string; parameters?: ValueSpecification[] } | undefined;
  if (o?._type === 'integer' && typeof o.value === 'number') return o.value;
  if (o?._type === 'func' && o.function === 'minus' && o.parameters?.length === 1) {
    const n = boundRead(o.parameters[0]);
    return typeof n === 'number' ? -n : undefined;
  }
  const p = o?.function === 'toOne' ? o.parameters?.[0] as { _type?: string; property?: string } | undefined : undefined;
  return p?._type === 'property' && typeof p.property === 'string' ? { column: p.property } : undefined;
}

/** A sum's first term: `plus([a, 1])`'s `a`. */
function firstTerm(v: ValueSpecification | undefined): ValueSpecification | undefined {
  const sum = v as { function?: string; parameters?: ValueSpecification[] } | undefined;
  const terms = sum?.function === 'plus' ? sum.parameters?.[0] as { values?: ValueSpecification[] } | undefined : undefined;
  return terms?.values?.[0];
}

/** The `range` a lambda makes, guarded or not. */
function rangeOf(l: Lambda): { readonly parameters: readonly ValueSpecification[] } | undefined {
  let body = l.body[0] as { function?: string; parameters?: ValueSpecification[]; body?: ValueSpecification[] } | undefined;
  if (body?.function === 'if') body = (body.parameters?.[2] as { body?: ValueSpecification[] } | undefined)?.body?.[0] as typeof body;
  return body?.function === 'range' && body.parameters !== undefined ? { parameters: body.parameters } : undefined;
}

/** A grid's rows repeated, read off its columns: what made them -- undefined for any other columns. */
export function generatedOf(derived: readonly DerivedColumn[]): GenerateRows | undefined {
  const same = (g: GenerateRows): boolean => generatedColumns(g).every((c) => {
    const had = derived.find((d) => d.name === c.name);
    if (had === undefined) return false;
    const { type: _learned, ...declared } = had;
    return toJson(declared as never) === toJson(c as never);
  });
  for (const d of derived) {
    if (d.unnest !== true || d.lambda === undefined) continue;
    const range = rangeOf(d.lambda);
    if (range === undefined) continue;
    // days: its number from 0 to the days between its two dates, the day beside it
    if (d.name.endsWith('__day')) {
      const name = d.name.slice(0, -'__day'.length);
      const day = derived.find((c) => c.name === name)?.lambda?.body[0] as { function?: string; parameters?: ValueSpecification[] } | undefined;
      const from = boundRead(day?.parameters?.[0]);
      const diff = firstTerm(range.parameters[1]) as { parameters?: ValueSpecification[] } | undefined;
      const to = boundRead(diff?.parameters?.[1]);
      if (typeof from !== 'object' || typeof to !== 'object') continue;
      const g: GenerateRows = { kind: 'days', name, from: from.column, to: to.column };
      if (same(g)) return g;
      continue;
    }
    const from = boundRead(range.parameters[0]);
    const to = boundRead(firstTerm(range.parameters[1]));
    const step = range.parameters[2] === undefined ? undefined : boundRead(range.parameters[2]);
    if (from === undefined || to === undefined || (step !== undefined && typeof step !== 'number')) continue;
    const g: GenerateRows = { kind: 'numbers', name: d.name, from, to, ...(typeof step === 'number' ? { step } : {}) };
    if (same(g)) return g;
  }
  return undefined;
}

/** A Generate Rows in words: "N from 1 to 10", "N from start to finish, by 2", "day from opened to closed". */
export function generatedWords(g: GenerateRows): string {
  const said = (b: Bound): string => (typeof b === 'number' ? String(b) : b.column);
  return g.kind === 'days' ? `${g.name}: each day from ${g.from} to ${g.to}`
    : `${g.name} from ${said(g.from)} to ${said(g.to)}${g.step !== undefined && g.step !== 1 ? `, by ${g.step}` : ''}`;
}

/** The columns a Generate Rows makes besides its new one: a days' number. */
export const generatedHelpers = (g: GenerateRows | undefined): string[] => (g?.kind === 'days' ? [dayName(g.name)] : []);

/** A Generate Rows read from JSON (Python's): `{kind: 'generateRows', name, from, to, step?}` -- a bound a number or
 *  `{column}` -- or `{kind: 'generateRows', name, days: {from, to}}`; refused in words. */
export function readGenerateRows(raw: unknown): GenerateRows {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'generateRows' || typeof o['name'] !== 'string') {
    throw new CubeRefusal(`a Generate Rows is {kind: 'generateRows', name, from, to} or {..., days: {from, to}}, not ${JSON.stringify(raw)}`);
  }
  const days = o['days'];
  if (days !== undefined) {
    const d = days !== null && typeof days === 'object' ? days as Record<string, unknown> : {};
    if (typeof d['from'] !== 'string' || typeof d['to'] !== 'string') throw new CubeRefusal('its days are {from, to}, two date columns');
    return { kind: 'days', name: o['name'], from: d['from'], to: d['to'] };
  }
  const bound = (b: unknown): Bound => {
    if (typeof b === 'number') return b;
    if (b !== null && typeof b === 'object' && typeof (b as Record<string, unknown>)['column'] === 'string') return { column: (b as { column: string }).column };
    throw new CubeRefusal(`a bound is a whole number or {column}, not ${JSON.stringify(b)}`);
  };
  const step = o['step'];
  if (step !== undefined && typeof step !== 'number') throw new CubeRefusal(`a step is a whole number, not ${JSON.stringify(step)}`);
  return { kind: 'numbers', name: o['name'], from: bound(o['from']), to: bound(o['to']), ...(step !== undefined ? { step } : {}) };
}
