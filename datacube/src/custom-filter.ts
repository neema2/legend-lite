// A FLOW'S CUSTOM FILTER (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12, "filter by a Pure expression", §17.6 item 6;
// Alteryx's Filter, its Custom filter): the rows a condition written in Pure holds for, the database judging -- said as
// what a grid already does: a calculated column of the condition (`CustomFilter`, hidden), parsed and typed by the
// compiler, and the grid's filter keeping the rows it is true for, in place of every condition it had. Its False port is
// a filter's (the rows it leaves out). Read back off its column and its filter (`customFilterOf`), never guessed at.
//
// One rule for the box's window and Python's grid.custom_filter (headless gridPreset).
//
// PURE: no DOM, no planner.

import type { Lambda, ValueSpecification } from '../../pure-protocol/src/index.ts';
import { isBoolean } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, sameValue, type CubeShape, type CubeSnapshot, type DerivedColumn, type FilterNode } from './snapshot.ts';

/** The condition's column: hidden, its value true for each row kept. */
export const CUSTOM_FILTER_COLUMN = 'CustomFilter';

/** The filter that keeps the rows its condition is true for: that alone. */
const KEPT: FilterNode = { kind: 'condition', column: CUSTOM_FILTER_COLUMN, operator: 'equal', value: true };

/** Its text as the compiler parses it: a lambda as written (`x|...`), or an expression over `$x`, the row -- a name and
 *  `||` its expression's `or`, never a lambda's head (`true || $x.flag`: the review of 2026-10-26). */
export const customFilterText = (expression: string): string => {
  const e = expression.trim();
  return /^\s*[A-Za-z_][\w]*\s*\|(?!\|)/.test(e) ? e : `x|${e}`;
};

/** Its column, the condition parsed (`lambda`). */
const columnOf = (lambda: Lambda): DerivedColumn => ({ name: CUSTOM_FILTER_COLUMN, lambda, kind: 'dimension' });

/** The grid with its condition's column as it would be (`s` as it is), for the compiler to type before it is set: a
 *  condition is true or false (customFilterCommands' `type`). */
export function customFilterProbe(s: CubeSnapshot, lambda: Lambda): CubeSnapshot {
  return { ...s, derived: [...s.derived.filter((d) => d.name !== CUSTOM_FILTER_COLUMN), columnOf(lambda)] };
}

/**
 * The commands that make a grid's filter its condition (`lambda`, parsed by the compiler; `type`, the compiler's type of
 * it, as the probe typed it): its column added -- or, a Custom Filter already, set again -- the filter that alone, the
 * column hidden; one step. Refused in words: a condition that is no Boolean, a column of its name not its own.
 */
export function customFilterCommands(s: CubeSnapshot, lambda: Lambda, type: string | undefined): GridCommand[] {
  if (!isBoolean(type)) throw new CubeRefusal(`a condition is true or false, a Boolean: this one is ${type ?? 'not typed'}`);
  const had = s.derived.find((d) => d.name === CUSTOM_FILTER_COLUMN);
  const taken = s.columns.some((c) => c.name === CUSTOM_FILTER_COLUMN) || (s.groupDerived ?? []).some((d) => d.name === CUSTOM_FILTER_COLUMN);
  if (taken || (had !== undefined && had.lambda === undefined)) throw new CubeRefusal(`'${CUSTOM_FILTER_COLUMN}' is already a column`);
  const column = columnOf(lambda);
  return [
    had === undefined ? { kind: 'calculate', level: 'row', column } : { kind: 'editCalculated', name: CUSTOM_FILTER_COLUMN, level: 'row', column },
    { kind: 'filter', filter: KEPT },
    { kind: 'configure', patch: { columns: { [CUSTOM_FILTER_COLUMN]: { hidden: true } } } },
  ];
}

/** The columns of the row a condition reads (`$x.c`, wherever it is in it), in the order it first reads them. */
function reads(lambda: Lambda): string[] {
  const row = lambda.parameters[0]?.name;
  const found: string[] = [];
  const walk = (v: unknown): void => {
    if (v === null || typeof v !== 'object') return;
    if (Array.isArray(v)) {
      v.forEach(walk);
      return;
    }
    const o = v as { _type?: string; property?: string; parameters?: ValueSpecification[] };
    const of = o.parameters?.[0] as { _type?: string; name?: string } | undefined;
    if (o._type === 'property' && typeof o.property === 'string' && of?._type === 'var' && of.name === row && !found.includes(o.property)) {
      found.push(o.property);
    }
    for (const x of Object.values(o)) walk(x);
  };
  lambda.body.forEach(walk);
  return found;
}

/** A grid filtered by a condition, read off its query: its condition and the columns it reads -- undefined for any
 *  other grid (a filter of other conditions, or none). */
export function customFilterOf(q: Pick<CubeShape, 'derived' | 'filter'>): { readonly lambda: Lambda; readonly reads: readonly string[] } | undefined {
  const d = q.derived.find((c) => c.name === CUSTOM_FILTER_COLUMN);
  if (d?.lambda === undefined || q.filter === undefined || !sameValue(q.filter, KEPT)) return undefined;
  return { lambda: d.lambda, reads: reads(d.lambda) };
}

/** A Custom Filter in words: "a condition on notional, region". */
export const customFilterWords = (c: { readonly reads: readonly string[] }): string =>
  (c.reads.length === 0 ? 'a condition' : `a condition on ${c.reads.join(', ')}`);

/** A Custom Filter read from JSON (Python's): `{kind: 'customFilter', expression}` -- or refused in words. */
export function readCustomFilter(raw: unknown): { readonly expression: string } {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'customFilter' || typeof o['expression'] !== 'string' || o['expression'].trim() === '') {
    throw new CubeRefusal(`a custom filter is {kind: 'customFilter', expression}, its condition in Pure, not ${JSON.stringify(raw)}`);
  }
  const odd = Object.keys(o).filter((k) => k !== 'kind' && k !== 'expression');
  if (odd.length > 0) throw new CubeRefusal(`a custom filter takes no ${odd.join(', ')}`);
  return { expression: o['expression'] };
}
