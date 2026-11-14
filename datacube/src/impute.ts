// IMPUTATION (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Imputation box, Alteryx's): a number column's empty
// values filled -- with a value given, or with the column's own average, median, smallest or largest -- each a calculated column
// beside it (`<column>_filled`), as Data Cleansing's are: `coalesce($x.c, value)`, or `coalesce($x.c, $x.c__average)`
// over a window column the database computes across every row (its frame the whole of them), hidden. Nothing is
// computed here: the database averages and fills. What a box says is read back off its columns (`imputedOf`).
//
// One rule for the box's window and Python's grid.impute (headless gridPreset).
//
// PURE: no DOM, no planner.

import { fn, lambda, lit, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, isFractional } from '../../engine-client/src/types.ts';
import { inPlaceCommand } from './box-columns.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn, type WindowFunction } from './snapshot.ts';

/** What an empty value is filled with: a value given, or the column's own average, median, smallest or largest. */
export type Filler = { readonly kind: 'value'; readonly value: number } | { readonly kind: 'average' | 'median' | 'smallest' | 'largest' };

/** Imputation, as its window sets it: its columns, and what their empties are filled with. */
export interface Imputation {
  readonly columns: readonly string[];
  readonly with: Filler;
  /** Each filled column handed on by its column's name, the column hidden (box-columns.ts), as Alteryx's Imputation does. */
  readonly inPlace?: boolean;
}

/** What a filled column is called: its column's name and this. */
export const FILLED_SUFFIX = '_filled';

/** The window function each filler is: the column across every row. */
const WINDOW: Readonly<Record<Exclude<Filler['kind'], 'value'>, WindowFunction>> = {
  average: 'average', median: 'median', smallest: 'min', largest: 'max',
};

/** What a filler's window column is called: its column's name, two underscores, and the filler. */
const helperName = (column: string, kind: string): string => `${column}__${kind}`;

/**
 * The commands that make a grid's columns filled (`s`: the grid as it is): for each column a window column of its
 * average, smallest or largest where the filler is one (hidden), then its filled column. Refused in words: no column, one
 * the grid has not or that is no number, a whole-number column filled with a fraction, a name taken.
 */
export function imputeCommands(s: CubeSnapshot, t: Imputation): GridCommand[] {
  if (t.columns.length === 0) throw new CubeRefusal('pick a column to fill');
  const types = new Map(rowColumns(s).map((c) => [c.name, c.type] as const));
  const taken = new Set([...types.keys(), ...(s.groupDerived ?? []).map((d) => d.name)]);
  const commands: GridCommand[] = [];
  const hidden: Record<string, { readonly hidden: true }> = {};
  for (const c of t.columns) {
    const type = types.get(c);
    if (!types.has(c)) throw new CubeRefusal(`the grid has no column '${c}'`);
    if (familyOf(type) !== 'numeric') throw new CubeRefusal(`'${c}' is ${type ?? 'not typed yet'}: only a number's empties are filled`);
    const name = `${c}${FILLED_SUFFIX}`;
    let filler: ValueSpecification;
    if (t.with.kind === 'value') {
      const v = t.with.value;
      if (!Number.isFinite(v)) throw new CubeRefusal(`an empty is filled with a number, not ${v}`);
      const whole = !isFractional(type);
      if (whole && !Number.isInteger(v)) throw new CubeRefusal(`'${c}' holds whole numbers: fill it with one, not ${v}`);
      filler = whole ? lit.integer(v) : lit.float(v);
    } else {
      const helper = helperName(c, t.with.kind);
      if (taken.has(helper)) throw new CubeRefusal(`'${helper}' is already a column`);
      commands.push({ kind: 'calculate', level: 'row', column: {
        name: helper, window: { fn: WINDOW[t.with.kind], column: c, partition: [], order: [], frame: 'partition' },
      } });
      hidden[helper] = { hidden: true };
      taken.add(helper);
      filler = property(variable('x'), helper);
    }
    // what fills it judged first, then its name
    if (taken.has(name)) throw new CubeRefusal(`'${name}' is already a column`);
    commands.push({ kind: 'calculate', level: 'row', column: {
      name, lambda: lambda(['x'], fn('coalesce', property(variable('x'), c), filler)),
    } });
    taken.add(name);
  }
  const done = Object.keys(hidden).length === 0 ? commands : [...commands, { kind: 'configure', patch: { columns: hidden } } as const];
  // in place: each filled column handed on by its column's name
  return t.inPlace === true ? [...done, inPlaceCommand(t.columns.map((c) => ({ column: c, made: `${c}${FILLED_SUFFIX}` })))] : done;
}

/** A column filled, read off its expression: the column it fills, and what with -- a value, or (its window column
 *  among `derived`) the column's average, smallest or largest; the window column's name, to say nothing of it alone. */
export function imputedOf(d: DerivedColumn, derived: readonly DerivedColumn[]):
  { readonly column: string; readonly with: Filler; readonly helper?: string } | undefined {
  const param = d.lambda?.parameters[0]?.name;
  const body = d.lambda?.body;
  if (param === undefined || body?.length !== 1) return undefined;
  const call = body[0]!;
  if (call._type !== 'func' || call.function !== 'coalesce' || call.parameters.length !== 2) return undefined;
  const [value, filler] = call.parameters as [ValueSpecification, ValueSpecification];
  const read = (v: ValueSpecification): string | undefined => {
    if (v._type !== 'property' || v.parameters.length !== 1) return undefined;
    const owner = v.parameters[0]!;
    return owner._type === 'var' && owner.name === param ? v.property : undefined;
  };
  const column = read(value);
  if (column === undefined) return undefined;
  // a number given: an integer's, a float's, or a negative's (`minus(1)`, as the protocol writes -1)
  const number = (v: ValueSpecification): number | undefined => {
    if (v._type === 'integer' || v._type === 'float') return Number((v as { value: unknown }).value);
    if (v._type !== 'func' || v.function !== 'minus' || v.parameters.length !== 1) return undefined;
    const n = number(v.parameters[0]!);
    return n === undefined ? undefined : -n;
  };
  const given = number(filler);
  if (given !== undefined) return { column, with: { kind: 'value', value: given } };
  const helper = read(filler);
  const w = helper === undefined ? undefined : derived.find((x) => x.name === helper)?.window;
  if (w === undefined || w.column !== column || w.partition.length > 0 || w.frame !== 'partition') return undefined;
  const kind = (Object.keys(WINDOW) as (keyof typeof WINDOW)[]).find((k) => WINDOW[k] === w.fn);
  return kind === undefined ? undefined : { column, with: { kind }, helper: helper! };
}

/** Imputation read from JSON (Python's): `{kind: 'impute', columns, with: 'average' | 'median' | 'smallest' | 'largest' | number}`. */
export function readImputation(raw: unknown): Imputation {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'impute') throw new CubeRefusal(`an imputation is {kind: 'impute', columns, with}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['kind', 'columns', 'with', 'inPlace'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`an imputation takes columns, with and inPlace, not ${odd.join(', ')}`);
  const inPlace = o['inPlace'];
  if (inPlace !== undefined && typeof inPlace !== 'boolean') throw new CubeRefusal(`its inPlace is true or false, not ${JSON.stringify(inPlace)}`);
  const place = inPlace === true ? { inPlace } : {};
  const columns = o['columns'];
  if (!Array.isArray(columns) || !columns.every((c): c is string => typeof c === 'string')) {
    throw new CubeRefusal(`its columns are a list of names, not ${JSON.stringify(columns)}`);
  }
  const w = o['with'];
  if (typeof w === 'number') return { columns, with: { kind: 'value', value: w }, ...place };
  if (w === 'average' || w === 'median' || w === 'smallest' || w === 'largest') return { columns, with: { kind: w }, ...place };
  throw new CubeRefusal(`its empties are filled with a number, or 'average', 'median', 'smallest' or 'largest', not ${JSON.stringify(w)}`);
}
