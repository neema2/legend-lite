// UNIQUE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Unique box, Alteryx's, with its U and D outputs): a row of
// each key -- the first, in an order -- said as what a grid already does: a row number restarting for each key
// (a window column, `rowNumber` partitioned by the key's columns), a filter keeping the rows numbered 1, and that
// column hidden. Its Unique output is the grid's rows; its Duplicates the filter's False port (FL7: the same query, the
// filter turned round) -- every row after a key's first. No value is computed here: the database numbers the rows.
//
// One rule for the box's window and Python's grid.unique (headless gridPreset). What a box says is read back off its
// query (`uniqueOf`), never stored beside it.
//
// PURE: no DOM, no planner.

import { fn, lambda, property, variable } from '../../pure-protocol/src/index.ts';
import { familyOf } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn, type FilterNode, type SortSpec } from './snapshot.ts';

/** What Unique is set to: the columns a key is, and the order a key's first row is first in (its first column, rising,
 *  when none is given). */
export interface Unique {
  readonly keys: readonly string[];
  readonly by?: SortSpec;
}

/** What Unique's row number is called: this, else this and `_2`, `_3`... where the grid has a column of the name. */
export const UNIQUE_COLUMN = 'UniqueRow';
/** What a falling order's flag of its empties is called (rowNumberOrder), named as the row number is. */
export const UNIQUE_EMPTY_COLUMN = 'UniqueEmpty';

/** The families a row number's ties are broken by: each that sorts. */
export const ORDERED = new Set(['text', 'numeric', 'temporal', 'boolean']);

/**
 * A ROW NUMBER'S ORDER, as Unique and a Sample number rows (the correctness audit's 3 and 14, 2026-10-26): `by` first,
 * its empties after every value whichever way it goes -- Pure's empty is the largest, first in a falling order, so a
 * falling order is led by whether its value is empty (`flag`, a column of its own: `isEmpty`, rising: a value first) --
 * then every other column that sorts (but `skip`'s, a partition's), rising, so no two rows tie unless they are alike
 * and each query of a box (its rows, its other port, its preview, its Output) numbers its rows the same.
 */
export function rowNumberOrder(s: CubeSnapshot, by: SortSpec, skip: readonly string[], flag: () => string):
  { readonly order: readonly SortSpec[]; readonly flag?: DerivedColumn } {
  const ties = rowColumns(s).filter((c) => c.name !== by.column && !skip.includes(c.name) && ORDERED.has(familyOf(c.type) ?? ''))
    .map((c): SortSpec => ({ column: c.name, direction: 'asc' }));
  if (by.direction === 'asc') return { order: [by, ...ties] };
  const name = flag();
  const empty: DerivedColumn = { name, kind: 'dimension', lambda: lambda(['x'], fn('isEmpty', property(variable('x'), by.column))) };
  return { order: [{ column: name, direction: 'asc' }, by, ...ties], flag: empty };
}

/** A row number's order read back (rowNumberOrder's): its `by`, and its flag of its empties when it falls -- its first
 *  column but that flag, the rest rising and each another column; undefined for any other order. */
export function rowNumberBy(order: readonly SortSpec[], derived: readonly DerivedColumn[]):
  { readonly by: SortSpec; readonly flag?: string } | undefined {
  const lead = order[0];
  const flagged = lead !== undefined && lead.direction === 'asc' && order[1]?.direction === 'desc'
    && emptyOf(derived.find((d) => d.name === lead.column)) === order[1].column;
  const by = order[flagged ? 1 : 0];
  if (by === undefined || (!flagged && by.direction === 'desc')) return undefined;
  const ties = order.slice(flagged ? 2 : 1);
  if (ties.some((t) => t.direction !== 'asc' || t.column === by.column || (flagged && t.column === lead!.column))) return undefined;
  return flagged ? { by, flag: lead!.column } : { by };
}

/** The column `isEmpty($x.c)` reads, of a calculated column that is it: undefined for any other. */
function emptyOf(d: DerivedColumn | undefined): string | undefined {
  const param = d?.lambda?.parameters[0]?.name;
  const body = d?.lambda?.body;
  if (param === undefined || body?.length !== 1 || d?.window !== undefined) return undefined;
  const call = body[0]!;
  if (call._type !== 'func' || call.function !== 'isEmpty' || call.parameters.length !== 1) return undefined;
  const p = call.parameters[0]!;
  return p._type === 'property' && p.parameters[0]?._type === 'var' && (p.parameters[0] as { name: string }).name === param ? p.property : undefined;
}

/**
 * The commands that make a grid Unique (`s`: the grid as it is): its row number per key, its filter (and-ed with any
 * filter the grid has), the number's column hidden. Refused in words: no key, a key or an order the grid has no column
 * of, a key twice.
 */
export function uniqueCommands(s: CubeSnapshot, u: Unique): GridCommand[] {
  const have = new Set(rowColumns(s).map((c) => c.name));
  if (u.keys.length === 0) throw new CubeRefusal('a key is at least one column');
  const odd = [...u.keys, ...(u.by !== undefined ? [u.by.column] : [])].filter((c) => !have.has(c));
  if (odd.length > 0) throw new CubeRefusal(`the grid has no column ${odd.map((c) => `'${c}'`).join(', ')}`);
  if (new Set(u.keys).size !== u.keys.length) throw new CubeRefusal('a key names each column once');
  const taken = new Set([...have, ...(s.groupDerived ?? []).map((d) => d.name)]);
  const free = (base: string): string => {
    let name = base;
    for (let i = 2; taken.has(name); i += 1) name = `${base}_${i}`;
    taken.add(name);
    return name;
  };
  const name = free(UNIQUE_COLUMN);
  const { order, flag } = rowNumberOrder(s, u.by ?? { column: u.keys[0]!, direction: 'asc' }, u.keys, () => free(UNIQUE_EMPTY_COLUMN));
  const column: DerivedColumn = { name, window: { fn: 'rowNumber', partition: [...u.keys], order } };
  return [
    ...(flag !== undefined ? [{ kind: 'calculate', level: 'row', column: flag } as const] : []),
    { kind: 'calculate', level: 'row', column },
    { kind: 'addFilter', condition: { kind: 'condition', column: name, operator: 'equal', value: 1 } },
    { kind: 'configure', patch: { columns: { [name]: { hidden: true }, ...(flag !== undefined ? { [flag.name]: { hidden: true } } : {}) } } },
  ];
}

/** A grid made Unique, read off its query: its keys and its order, and its columns (its row number, any flag of its
 *  empties) -- when its filter is that number's first row, alone, and the number is per key in rowNumberOrder's order. */
export function uniqueOf(query: { readonly derived: readonly DerivedColumn[]; readonly filter?: FilterNode | undefined }):
  { readonly keys: readonly string[]; readonly by: SortSpec; readonly column: string; readonly columns: readonly string[] } | undefined {
  const f = query.filter;
  if (f?.kind !== 'condition' || f.operator !== 'equal' || f.value !== 1) return undefined;
  const d = query.derived.find((c) => c.name === f.column);
  const w = d?.window;
  if (w?.fn !== 'rowNumber' || w.partition.length === 0) return undefined;
  const ordered = rowNumberBy(w.order, query.derived);
  if (ordered === undefined) return undefined;
  return { keys: w.partition, by: ordered.by, column: f.column, columns: [...(ordered.flag !== undefined ? [ordered.flag] : []), f.column] };
}

/** Unique read from JSON (Python's): `{kind: 'unique', keys, by?}`, `by` a column (rising) or `{column, direction}`. */
export function readUnique(raw: unknown): Unique {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'unique') throw new CubeRefusal(`a Unique is {kind: 'unique', keys, by?}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'keys', 'by'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a Unique takes keys and by, not ${odd.join(', ')}`);
  const keys = o['keys'];
  if (!Array.isArray(keys) || !keys.every((k): k is string => typeof k === 'string')) {
    throw new CubeRefusal(`its keys are a list of column names, not ${JSON.stringify(keys)}`);
  }
  const by = o['by'];
  if (by === undefined) return { keys };
  if (typeof by === 'string') return { keys, by: { column: by, direction: 'asc' } };
  const b = by !== null && typeof by === 'object' ? by as Record<string, unknown> : undefined;
  if (typeof b?.['column'] !== 'string' || (b['direction'] !== 'asc' && b['direction'] !== 'desc')) {
    throw new CubeRefusal(`its order is a column, or {column, direction: 'asc' | 'desc'}, not ${JSON.stringify(by)}`);
  }
  return { keys, by: { column: b['column'], direction: b['direction'] } };
}
