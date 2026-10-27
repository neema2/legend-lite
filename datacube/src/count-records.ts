// COUNT RECORDS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M4, Alteryx's Count Records): how many rows a box's input has --
// one row, one column -- said as what a grid already does: a column of one value it groups by (hidden, so the box after
// it reads none of it: box-columns.ts), a column of 1 for each row, counted, and every other column hidden. The database
// counts: a grouping by a column of one value is every row aggregated (query.ts), so no rows are one row, 0. What a box says is read back off its columns (`countRecordsOf`).
//
// One rule for the box (it has no settings: made, it counts) and Python's grid.count_records (headless gridPreset).
//
// PURE: no DOM, no planner.

import { lambda, lit, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeShape, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** What its count is called, unless it is given another name; and the column of one value its rows are grouped by. */
export const COUNT_COLUMN = 'Count';
export const ALL_COLUMN = 'CountAll';

/** The one value every row has in the column it groups by. */
const ALL = 'all';

/** Whether a calculated column is `x | <value>`: one value for every row. */
const constant = (d: DerivedColumn | undefined): ValueSpecification | undefined => {
  const body = d?.lambda?.body;
  return d?.window === undefined && body?.length === 1 && d?.lambda?.parameters.length === 1 ? body[0] : undefined;
};

/**
 * The commands that make a grid count its rows (`s`: the grid as it is): any grouping taken off, the column of one value
 * and the column of 1 (`name`), grouped by the first, the second counted, every other column hidden -- one step. Refused
 * in words: a name taken, a grid pivoted (its columns are its values), a grid counting already.
 */
export function countRecordsCommands(s: CubeSnapshot, name: string = COUNT_COLUMN): GridCommand[] {
  if (countRecordsOf(s) !== undefined) throw new CubeRefusal('it counts its rows already');
  if (s.pivotOn.length > 0) throw new CubeRefusal('a pivoted grid\'s columns are its values: take its pivot off to count its rows');
  if (name.trim() === '') throw new CubeRefusal('its count is called something: give it a name');
  const columns = rowColumns(s).map((c) => c.name);
  for (const n of [name, ALL_COLUMN]) {
    if (columns.includes(n) || (s.groupDerived ?? []).some((d) => d.name === n)) throw new CubeRefusal(`'${n}' is already a column`);
  }
  return [
    ...(s.rows.length > 0 ? [{ kind: 'group', rows: [] } as const] : []),
    { kind: 'calculate', level: 'row', column: { name: ALL_COLUMN, lambda: lambda(['x'], lit.string(ALL)), kind: 'dimension' } },
    { kind: 'calculate', level: 'row', column: { name, lambda: lambda(['x'], lit.integer(1)), kind: 'measure' } },
    { kind: 'group', rows: [ALL_COLUMN] },
    // counted, not summed: a count of no rows is 0, a sum of none empty (the review of 2026-10-26's C4)
    { kind: 'column', name, change: { aggregate: 'count' } },
    { kind: 'configure', patch: { columns: Object.fromEntries([...columns, ALL_COLUMN].map((c) => [c, { hidden: true }])) } },
  ];
}

/** A grid counting its rows, read off its query: its count's name, and the column of one value -- undefined for any
 *  other grid. */
export function countRecordsOf(q: Pick<CubeShape, 'derived' | 'rows'>): { readonly name: string; readonly all: string } | undefined {
  if (q.rows.length !== 1 || q.rows[0] !== ALL_COLUMN) return undefined;
  const all = constant(q.derived.find((d) => d.name === ALL_COLUMN));
  if (all?._type !== 'string' || (all as { value?: unknown }).value !== ALL) return undefined;
  const count = q.derived.find((d) => d.name !== ALL_COLUMN && d.kind === 'measure' && constant(d)?._type === 'integer'
    && Number((constant(d) as { value?: unknown }).value) === 1);
  return count === undefined ? undefined : { name: count.name, all: ALL_COLUMN };
}

/** A count read from JSON (Python's): `{kind: 'countRecords', name?}` -- or refused in words. */
export function readCountRecords(raw: unknown): { readonly name: string } {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'countRecords') throw new CubeRefusal(`a count is {kind: 'countRecords', name}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'name'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a count takes name, not ${odd.join(', ')}`);
  const name = o['name'] ?? COUNT_COLUMN;
  if (typeof name !== 'string') throw new CubeRefusal(`its count's name is text, not ${JSON.stringify(name)}`);
  return { name };
}
