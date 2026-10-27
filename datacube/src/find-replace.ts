// FIND REPLACE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2, Alteryx's Find Replace): each row's value of a column found
// among another input's -- a Join box's, all of mine on my column equal to theirs (the Join window's) -- and replaced by
// the value of theirs it found, where it found one: a calculated column, `coalesce($x.<with>, $x.<column>)`, handed on in
// place of the column (box-columns.ts: the column hidden, the calculated column given its name), theirs hidden. Nothing
// is computed here: the database joins and chooses. What a box says is read back off its column (`findReplaceOf`).
//
// One rule for the box's window and Python's page.find_replace (headless gridPreset).
//
// PURE: no DOM, no planner.

import { fn, lambda, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { inPlaceCommand } from './box-columns.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** A Find Replace, as its window sets it: the column whose values are replaced, and the column of theirs they are
 *  replaced with where it has one. */
export interface FindReplace {
  readonly column: string;
  readonly with: string;
}

/** What a replaced column is called: its column's name and this. */
export const REPLACED_SUFFIX = '_replaced';

/**
 * The commands that replace a grid's column (`s`: the grid as it is) by another where that has a value: any replacing
 * it has taken off (its column and theirs shown again), the replaced column, then it handed on in place of the column,
 * and the column it was replaced with hidden -- one step. Refused in words: a column the grid has not, a column replaced
 * by itself, a name taken.
 */
export function findReplaceCommands(s: CubeSnapshot, t: FindReplace): GridCommand[] {
  // set again: what it replaced before taken off first
  const before = s.derived.flatMap((d) => { const r = findReplaceOf(d); return r === undefined ? [] : [{ name: d.name, ...r }]; });
  const gone = new Set(before.map((b) => b.name));
  const names = new Set([...rowColumns(s).map((c) => c.name), ...(s.groupDerived ?? []).map((d) => d.name)].filter((n) => !gone.has(n)));
  for (const c of [t.column, t.with]) if (!names.has(c)) throw new CubeRefusal(`the grid has no column '${c}'`);
  if (t.column === t.with) throw new CubeRefusal(`'${t.column}' is replaced by another column, not by itself`);
  const made = `${t.column}${REPLACED_SUFFIX}`;
  if (names.has(made)) throw new CubeRefusal(`'${made}' is already a column`);
  const undone: GridCommand[] = before.length === 0 ? [] : [
    ...before.map((b): GridCommand => ({ kind: 'removeCalculated', name: b.name })),
    { kind: 'configure', patch: { columns: Object.fromEntries(before.flatMap((b) => [[b.column, { hidden: null }], [b.with, { hidden: null }]])) } },
  ];
  return [
    ...undone,
    { kind: 'calculate', level: 'row', column: { name: made, lambda: lambda(['x'], fn('coalesce', property(variable('x'), t.with), property(variable('x'), t.column))) } },
    { kind: 'configure', patch: { columns: { [t.with]: { hidden: true } } } },
    inPlaceCommand([{ column: t.column, made }]),
  ];
}

/** A replaced column, read off its expression and its name: the column it replaces and what with -- undefined for any
 *  other column. */
export function findReplaceOf(d: DerivedColumn): FindReplace | undefined {
  const param = d.lambda?.parameters[0]?.name;
  const body = d.lambda?.body;
  if (param === undefined || body?.length !== 1 || d.window !== undefined) return undefined;
  const call = body[0]!;
  if (call._type !== 'func' || call.function !== 'coalesce' || call.parameters.length !== 2) return undefined;
  const read = (v: ValueSpecification): string | undefined => {
    if (v._type !== 'property' || v.parameters.length !== 1) return undefined;
    const owner = v.parameters[0]!;
    return owner._type === 'var' && owner.name === param ? v.property : undefined;
  };
  const [w, c] = call.parameters.map(read);
  return w !== undefined && c !== undefined && d.name === `${c}${REPLACED_SUFFIX}` ? { column: c, with: w } : undefined;
}

/** A Find Replace read from JSON (Python's): `{kind: 'findReplace', column, with}` -- or refused in words. */
export function readFindReplace(raw: unknown): FindReplace {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'findReplace') throw new CubeRefusal(`a find replace is {kind: 'findReplace', column, with}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'column', 'with'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a find replace takes column and with, not ${odd.join(', ')}`);
  const [column, by] = [o['column'], o['with']];
  if (typeof column !== 'string' || column === '' || typeof by !== 'string' || by === '') {
    throw new CubeRefusal(`a find replace names its column and the column it is replaced with, not ${JSON.stringify(raw)}`);
  }
  return { column, with: by };
}
