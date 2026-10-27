// TRANSPOSE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M4: a flow's Transpose box, Alteryx's): columns made rows -- each row
// once for each of its data columns, a Name column saying which and a Value column holding its value, its key columns
// beside them -- said as what a grid already does: an exploded calculated column over the list of the columns' names
// (`lateral(x|['revenue', 'cost']->flatten(~Name))`), a calculated column choosing each row's value by its name
// (`if($x.Name == 'revenue', |$x.revenue, |$x.cost)`), and every column but the keys hidden, so the box after it reads
// the keys, Name and Value. The database repeats the rows and chooses the values. What a box says is read back off its
// columns (`transposeOf`), never off their names.
//
// One rule for the box's window and Python's grid.transpose (headless gridPreset).
//
// PURE: no DOM, no planner.

import { fn, lambda, lit, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, type TypeFamily } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** Transpose, as its window and Python set it: the columns made rows, the columns kept beside them, its two names. */
export interface Transpose {
  readonly columns: readonly string[];
  readonly keys: readonly string[];
  readonly name: string;
  readonly value: string;
}

/** A kind of value, as a refusal says it. */
const FAMILY_WORDS: Readonly<Record<TypeFamily, string>> = {
  numeric: 'numbers', text: 'text', boolean: 'true or false', temporal: 'dates', binary: 'bytes', variant: 'JSON', any: 'any value',
};

/** Its two columns' names, unless named. */
export const TRANSPOSE_NAME = 'Name';
export const TRANSPOSE_VALUE = 'Value';

/**
 * The commands that transpose a grid (`s`: the grid as it is): its Name column (each row once per data column), its
 * Value column, and every column but its keys hidden. Refused in words: no column, one the grid has not, a key made a
 * row, columns of more than one kind (one Value holds one kind: numbers, text, dates), a name taken.
 */
export function transposeCommands(s: CubeSnapshot, t: Transpose): GridCommand[] {
  if (t.columns.length === 0) throw new CubeRefusal('pick a column to make rows of');
  const columns = rowColumns(s);
  const types = new Map(columns.map((c) => [c.name, c.type] as const));
  const odd = [...t.columns, ...t.keys].filter((c) => !types.has(c));
  if (odd.length > 0) throw new CubeRefusal(`the grid has no column ${odd.map((c) => `'${c}'`).join(', ')}`);
  if (new Set(t.columns).size !== t.columns.length) throw new CubeRefusal('it makes rows of each column once');
  const both = t.columns.filter((c) => t.keys.includes(c));
  if (both.length > 0) throw new CubeRefusal(`'${both[0]}' is a key: it stays beside the rows, it is not made one`);
  const families = new Set(t.columns.map((c) => familyOf(types.get(c))));
  if (families.size > 1) {
    const words = [...families].map((f) => (f === undefined ? 'values not typed yet' : FAMILY_WORDS[f]));
    throw new CubeRefusal(`its columns hold ${words.join(' and ')}: one Value holds one kind -- transpose columns of one kind, or make them text first (a Formula)`);
  }
  const name = t.name.trim();
  const value = t.value.trim();
  if (name === '' || value === '') throw new CubeRefusal('its two columns need a name each');
  if (name === value) throw new CubeRefusal(`its two columns need two names, not '${name}' twice`);
  const taken = new Set([...types.keys(), ...(s.groupDerived ?? []).map((d) => d.name)]);
  for (const n of [name, value]) if (taken.has(n)) throw new CubeRefusal(`'${n}' is already a column`);
  const x = variable('x');
  // its value chosen by its name: the last column's when no other's is
  const last = t.columns.at(-1)!;
  const chosen = t.columns.slice(0, -1).reduceRight<ValueSpecification>(
    (rest, c) => fn('if', fn('equal', property(x, name), lit.string(c)), lambda([], property(x, c)), lambda([], rest)), property(x, last));
  const hidden = Object.fromEntries(columns.filter((c) => !t.keys.includes(c.name)).map((c) => [c.name, { hidden: true }]));
  return [
    { kind: 'calculate', level: 'row', column: {
      name, lambda: lambda(['x'], { _type: 'collection', multiplicity: { lowerBound: t.columns.length, upperBound: t.columns.length },
        values: t.columns.map((c) => lit.string(c)) }), unnest: true, kind: 'dimension',
    } },
    { kind: 'calculate', level: 'row', column: {
      name: value, lambda: lambda(['x'], chosen), kind: families.has('numeric') ? 'measure' : 'dimension',
    } },
    { kind: 'configure', patch: { columns: hidden } },
  ];
}

/**
 * A grid transposed, read off its calculated columns: its Name column (exploded over a list of names) and, right after
 * it, its Value column (each name's column chosen by it, the last the rest) -- the columns made rows, in order.
 */
export function transposeOf(derived: readonly DerivedColumn[]):
  { readonly name: string; readonly value: string; readonly columns: readonly string[] } | undefined {
  for (let i = 0; i + 1 < derived.length; i++) {
    const n = derived[i]!;
    const list = n.unnest === true && n.lambda?.body.length === 1 ? n.lambda.body[0] : undefined;
    if (list?._type !== 'collection' || list.values.length === 0 || !list.values.every((v) => v._type === 'string')) continue;
    const columns = list.values.map((v) => (v as { value: string }).value);
    const v = derived[i + 1]!;
    const param = v.lambda?.parameters[0]?.name;
    if (param === undefined || v.lambda?.body.length !== 1 || v.unnest === true) continue;
    const read = (e: ValueSpecification | undefined): string | undefined =>
      e?._type === 'property' && e.parameters.length === 1 && e.parameters[0]?._type === 'var' && (e.parameters[0] as { name: string }).name === param
        ? e.property : undefined;
    // each name but the last: if($x.Name == 'c', |$x.c, |<the rest>)
    let e: ValueSpecification | undefined = v.lambda.body[0];
    let ok = true;
    for (const c of columns.slice(0, -1)) {
      if (e?._type !== 'func' || e.function !== 'if' || e.parameters.length !== 3) { ok = false; break; }
      const [test, then, rest] = e.parameters;
      const thenBody = then?._type === 'lambda' && then.body.length === 1 ? then.body[0] : undefined;
      const restBody = rest?._type === 'lambda' && rest.body.length === 1 ? rest.body[0] : undefined;
      const [lhs, rhs] = test?._type === 'func' && test.function === 'equal' ? test.parameters : [];
      if (read(lhs) !== n.name || rhs?._type !== 'string' || rhs.value !== c || read(thenBody) !== c) { ok = false; break; }
      e = restBody;
    }
    if (ok && read(e) === columns.at(-1)) return { name: n.name, value: v.name, columns };
  }
  return undefined;
}

/** Transpose read from JSON (Python's): `{kind: 'transpose', columns, keys?, name?, value?}` -- or refused in words. */
export function readTranspose(raw: unknown): Transpose {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'transpose') throw new CubeRefusal(`a transpose is {kind: 'transpose', columns, keys, name, value}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'columns', 'keys', 'name', 'value'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a transpose takes columns, keys, name and value, not ${odd.join(', ')}`);
  const names = (key: string): string[] => {
    const v = o[key] ?? [];
    if (!Array.isArray(v) || !v.every((c): c is string => typeof c === 'string')) throw new CubeRefusal(`its ${key} are a list of column names, not ${JSON.stringify(v)}`);
    return v;
  };
  const text = (key: string, otherwise: string): string => {
    const v = o[key] ?? otherwise;
    if (typeof v !== 'string') throw new CubeRefusal(`its ${key} is text, not ${JSON.stringify(v)}`);
    return v;
  };
  return { columns: names('columns'), keys: names('keys'), name: text('name', TRANSPOSE_NAME), value: text('value', TRANSPOSE_VALUE) };
}
