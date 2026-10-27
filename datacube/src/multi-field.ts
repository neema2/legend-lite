// MULTI-FIELD FORMULA (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Multi-Field Formula box, Alteryx's): one
// expression over several columns -- written once, over `$v`, each column's value in turn (Alteryx's [_CurrentField_]),
// parsed by the compiler as `v|...` -- made a calculated column per column (`<column><suffix>`), `$v` its column. The
// compiler types each, as it types a Formula's: `$v->trim()` over a number is refused in its words. What a box says is
// read back off its columns' shapes (`multiFieldOf`): two or more whose expressions are one with a column in each place
// (a shape a tool of its own makes is said as that tool: `$v->toOne()->trim()` over two columns is Data Cleansing's).
//
// One rule for the box's window and Python's grid.multi_field (headless gridPreset): its commands in
// multi-field-columns.ts, fetched with the first; what a box reads back here, with the canvas.
//
// PURE: no DOM, no planner.

import { toJson, variable, walk, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { DerivedColumn } from './snapshot.ts';

/** The name of each column's value in the expression, as the person writes it: `$v`. */
export const VALUE = 'v';

/** What a new column is called by default: its column's name and this. */
export const DEFAULT_SUFFIX = '_new';

/** A Multi-Field Formula, as its window and Python set it: its columns, its expression (parsed), its new columns' suffix. */
export interface MultiField {
  readonly columns: readonly string[];
  /** One parameter, the value; its body the expression. */
  readonly lambda: Lambda;
  readonly suffix: string;
  /** Each new column handed on by its column's name, the column hidden (box-columns.ts). */
  readonly inPlace?: boolean;
}

/** A Multi-Field Formula read from JSON (Python's): `{kind: 'multiField', columns, expression, suffix?}` -- its expression
 *  text, the compiler's to parse (multiFieldText). */
export interface MultiFieldAsked {
  readonly columns: readonly string[];
  readonly expression: string;
  readonly suffix: string;
  readonly inPlace?: boolean;
}

/** The row's columns a calculated column's expression reads (`$x.<column>`), each once, in the order read. */
function columnsRead(d: DerivedColumn): string[] {
  const param = d.lambda?.parameters[0]?.name;
  const read: string[] = [];
  if (param === undefined || d.lambda?.body.length !== 1) return read;
  walk(d.lambda.body[0]!, (n) => {
    if (n._type !== 'property' || n.parameters.length !== 1) return;
    const owner = n.parameters[0]!;
    if (owner._type === 'var' && owner.name === param && !read.includes(n.property)) read.push(n.property);
  });
  return read;
}

/** A calculated column's expression with `column` (one it reads) made the value: what it is, whichever column it is over. */
function shapeOver(d: DerivedColumn, column: string): string {
  const param = d.lambda!.parameters[0]!.name;
  const at = (n: ValueSpecification): ValueSpecification => {
    if (n._type === 'property' && n.property === column && n.parameters.length === 1) {
      const owner = n.parameters[0]!;
      if (owner._type === 'var' && owner.name === param) return variable(VALUE);
    }
    if (n._type === 'var' && n.name === param) return variable('x');
    if (n._type === 'lambda') return { ...n, body: n.body.map(at) };
    if (n._type === 'func' || n._type === 'property') return { ...n, parameters: n.parameters.map(at) } as ValueSpecification;
    if (n._type === 'collection') return { ...n, values: n.values.map(at) };
    return n;
  };
  return `${d.kind ?? ''}|${toJson(at(d.lambda!.body[0]!))}`;
}

/** A Multi-Field Formula's columns, as a box reads them: a column made by one, the column it is over and its fellows. */
export interface MultiFieldColumn {
  /** The column its expression is over. */
  readonly over: string;
  /** Every column of the same formula, by the columns they are over, in order. */
  readonly all: readonly string[];
}

/**
 * THE MULTI-FIELD FORMULAS among `derived`, read off their shapes: a run of two or more row columns, side by side, whose
 * expressions are one with a different column in each place (each column reading it, its name that column's and one
 * suffix). Each column of one, by its name. A lone column is a Formula's.
 */
export function multiFieldOf(derived: readonly DerivedColumn[]): Map<string, MultiFieldColumn> {
  const found = new Map<string, MultiFieldColumn>();
  // each column, the columns it may be over: one it reads whose name its own starts with
  const over = (d: DerivedColumn): readonly { column: string; suffix: string; shape: string }[] =>
    d.lambda === undefined || d.window !== undefined ? [] : columnsRead(d)
      .filter((c) => d.name.startsWith(c) && d.name.length > c.length)
      .map((c) => ({ column: c, suffix: d.name.slice(c.length), shape: shapeOver(d, c) }));
  let i = 0;
  while (i < derived.length) {
    const first = over(derived[i]!);
    let best: { readonly run: string[]; readonly names: string[] } | undefined;
    for (const f of first) {
      const run = [f.column];
      const names = [derived[i]!.name];
      for (let j = i + 1; j < derived.length; j++) {
        const next = over(derived[j]!).find((o) => o.shape === f.shape && o.suffix === f.suffix && !run.includes(o.column));
        if (next === undefined) break;
        run.push(next.column);
        names.push(derived[j]!.name);
      }
      if (run.length > (best?.run.length ?? 1)) best = { run, names };
    }
    if (best === undefined) {
      i += 1;
      continue;
    }
    best.names.forEach((n, k) => found.set(n, { over: best.run[k]!, all: best.run }));
    i += best.names.length;
  }
  return found;
}
