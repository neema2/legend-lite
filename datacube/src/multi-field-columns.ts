// MULTI-FIELD FORMULA'S COMMANDS (multi-field.ts, docs/DATACUBE_FLOW_MODE_2026_10_24.md M3): its expression's text as
// the compiler parses it, its columns made from the parsed expression, and its settings read from Python's JSON --
// fetched with the first Multi-Field Formula, as text-tool-columns.ts is with the first text tool.
//
// PURE: no DOM, no planner.

import { property, variable, walk, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { inPlaceCommand } from './box-columns.ts';
import type { GridCommand } from './grid-commands.ts';
import { DEFAULT_SUFFIX, VALUE, type MultiField, type MultiFieldAsked } from './multi-field.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot } from './snapshot.ts';

/** The expression's text as the compiler parses it: a lambda of the value (a head the person typed, kept). */
export const multiFieldText = (expression: string): string => {
  const e = expression.trim();
  return /^\s*[A-Za-z_][\w]*\s*\|/.test(e) ? e : `${VALUE}|${e}`;
};

/**
 * The commands that add a Multi-Field Formula's columns (`s`: the grid as it is): for each column, its expression with
 * the value made that column (`$v` → `$x.<column>`), a row-stage `calculate`, its kind its column's. Refused in words: no
 * column, one the grid has not, an expression of no one value, one that names its row `x` inside, a name taken.
 */
export function multiFieldCommands(s: CubeSnapshot, m: MultiField): GridCommand[] {
  if (m.columns.length === 0) throw new CubeRefusal('pick a column to calculate over');
  const param = m.lambda.parameters[0]?.name;
  if (m.lambda.parameters.length !== 1 || param === undefined) {
    throw new CubeRefusal(`its expression is over one value, $${VALUE}: not ${m.lambda.parameters.length} of them`);
  }
  // the row is $x, as a Formula's: its other columns read by it ($v / $x.total)
  if (param === 'x') throw new CubeRefusal(`its value is $${VALUE}, and $x the row: write the expression over $${VALUE}`);
  if (m.lambda.body.length !== 1) throw new CubeRefusal('its expression is one expression');
  const body = m.lambda.body[0]!;
  // a lambda inside it naming its own parameter the value or the row: the value in it would be another's
  walk(body, (n) => {
    if (n._type !== 'lambda') return;
    const bound = n.parameters.map((p) => p.name).find((p) => p === param || p === 'x');
    if (bound !== undefined) throw new CubeRefusal(`a lambda in it names its parameter ${bound}: call it something else`);
  });
  const suffix = m.suffix.trim();
  if (suffix === '') throw new CubeRefusal('its new columns are named with a suffix: type one, as _new');
  const columns = new Map(rowColumns(s).map((c) => [c.name, c] as const));
  const taken = new Set([...columns.keys(), ...(s.groupDerived ?? []).map((d) => d.name)]);
  const commands = m.columns.map((c): GridCommand => {
    const column = columns.get(c);
    if (column === undefined) throw new CubeRefusal(`the grid has no column '${c}'`);
    const name = `${c}${suffix}`;
    if (taken.has(name)) throw new CubeRefusal(`'${name}' is already a column`);
    taken.add(name);
    return { kind: 'calculate', level: 'row', column: {
      name, lambda: { ...m.lambda, parameters: [variable('x')], body: [valueAs(body, param, c)] },
      kind: column.kind === 'measure' ? 'measure' : 'dimension',
    } };
  });
  // in place: each new column handed on by its column's name (Alteryx's "change existing fields")
  return m.inPlace === true ? [...commands, inPlaceCommand(m.columns.map((c) => ({ column: c, made: `${c}${suffix}` })))] : commands;
}

/** `body` with the value `param` made the row's column `column`. */
function valueAs(body: ValueSpecification, param: string, column: string): ValueSpecification {
  const at = (n: ValueSpecification): ValueSpecification => {
    if (n._type === 'var') return n.name === param ? property(variable('x'), column) : n;
    if (n._type === 'lambda') return { ...n, body: n.body.map(at) };
    if (n._type === 'func' || n._type === 'property') return { ...n, parameters: n.parameters.map(at) } as ValueSpecification;
    if (n._type === 'collection') return { ...n, values: n.values.map(at) };
    return n;
  };
  return at(body);
}

/** A Multi-Field Formula read from JSON (Python's): its columns, its expression's text, its suffix -- or refused in words. */
export function readMultiField(raw: unknown): MultiFieldAsked {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'multiField') {
    throw new CubeRefusal(`a multi-field formula is {kind: 'multiField', columns, expression, suffix}, not ${JSON.stringify(raw)}`);
  }
  const odd = Object.keys(o).filter((k) => !['kind', 'columns', 'expression', 'suffix', 'inPlace'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a multi-field formula takes columns, expression, suffix and inPlace, not ${odd.join(', ')}`);
  const inPlace = o['inPlace'];
  if (inPlace !== undefined && typeof inPlace !== 'boolean') throw new CubeRefusal(`its inPlace is true or false, not ${JSON.stringify(inPlace)}`);
  const columns = o['columns'];
  if (!Array.isArray(columns) || !columns.every((c): c is string => typeof c === 'string')) {
    throw new CubeRefusal(`its columns are a list of names, not ${JSON.stringify(columns)}`);
  }
  const expression = o['expression'];
  if (typeof expression !== 'string' || expression.trim() === '') {
    throw new CubeRefusal(`its expression is over each column's value, $${VALUE}: not ${JSON.stringify(expression)}`);
  }
  const suffix = o['suffix'] ?? DEFAULT_SUFFIX;
  if (typeof suffix !== 'string') throw new CubeRefusal(`its suffix is text, not ${JSON.stringify(suffix)}`);
  return { columns, expression, suffix, ...(inPlace === true ? { inPlace } : {}) };
}
