// DYNAMIC REPLACE'S RULES (dynamic-replace.ts, Alteryx's Dynamic Replace): its rules read from a box's rows, and the
// commands that pin them. The rules are PINNED, as a Cross Tab's values are (FL2): read once from the rules box's rows
// (the database reading them), each condition (each value, as expressions) parsed by the compiler, and written into the
// box as a calculated column per column the rules name, handed on in its place (box-columns.ts). The rules box is read
// again only when they are loaded again; a load sets every replaced column the box has again, those of a column no rule
// names now taken off. Nothing is computed here: the database judges each condition.
//
// One rule for the box's window and Python's grid.dynamic_replace (headless gridPreset). Fetched with the first.
//
// PURE: no DOM, no planner.

import { fn, lambda, lit, ProtocolError, property, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { familyOf, isBoolean, plainType } from '../../engine-client/src/types.ts';
import { inPlaceCommand } from './box-columns.ts';
import { MAX_RULES, REPLACED_SUFFIX, replacedOf, type DynamicReplace, type ReplaceRule, type RuleColumns } from './dynamic-replace.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** The row's name in a rule's condition and value, as the person writes them: `$x`. */
const ROW = 'x';

/** A rule's condition or value as the compiler parses it: an expression over `$x`. */
export const ruleText = (expression: string): string => `${ROW}|${expression.trim()}`;

/**
 * The rules a box's rows hold (`table`, as the database answered it), each row one, its parts by the columns `by` names --
 * in its rows' order. Refused in words: a column `by` names the rows have not, a rule with no column or no condition, a
 * value that is empty.
 */
export function rulesOf(table: Pick<ResultTable, 'columns' | 'rowCount'>, by: RuleColumns): ReplaceRule[] {
  const at = (name: string, part: string): readonly Scalar[] => {
    const c = table.columns.find((x) => x.name === name);
    if (c === undefined) throw new CubeRefusal(`its rules have no column '${name}' for ${part} (${table.columns.map((x) => x.name).join(', ')})`);
    return c.values;
  };
  const [columns, conditions, values] = [at(by.column, 'the column each replaces'), at(by.condition, 'its condition'), at(by.value, 'its value')];
  if (table.rowCount === 0) throw new CubeRefusal('its rules box has no rows: a rule is a row of it');
  if (table.rowCount > MAX_RULES) throw new CubeRefusal(`its rules box has more than ${MAX_RULES} rows: a Dynamic Replace takes ${MAX_RULES} rules at most`);
  const rules: ReplaceRule[] = [];
  for (let i = 0; i < table.rowCount; i++) {
    const n = i + 1;
    const column = columns[i];
    const condition = conditions[i];
    const value = values[i];
    if (typeof column !== 'string' || column.trim() === '') throw new CubeRefusal(`rule ${n} names no column: its ${by.column} is ${JSON.stringify(column ?? null)}`);
    if (typeof condition !== 'string' || condition.trim() === '') throw new CubeRefusal(`rule ${n} has no condition: its ${by.condition} is ${JSON.stringify(condition ?? null)}`);
    if (value === null || value === undefined) throw new CubeRefusal(`rule ${n} has no value: its ${by.value} is empty`);
    rules.push({ column: column.trim(), condition, value: typeof value === 'bigint' ? value.toString() : value });
  }
  return rules;
}

/** What is wrong with a Dynamic Replace over the grid (`s`), in words, if anything -- before its rules are parsed. */
export function dynamicReplaceProblem(s: CubeSnapshot, d: DynamicReplace): string | undefined {
  if (d.rules.length === 0) return 'it has no rules: load them from a box of rules';
  if (d.rules.length > MAX_RULES) return `it has ${d.rules.length} rules: a Dynamic Replace takes ${MAX_RULES} at most`;
  const types = new Map(rowColumns(s).map((c) => [c.name, c.type] as const));
  for (const [i, r] of d.rules.entries()) {
    if (!types.has(r.column)) return `rule ${i + 1} replaces '${r.column}', which the grid has not`;
    if (d.expressions && typeof r.value !== 'string') return `rule ${i + 1}'s value is an expression, its text: not ${JSON.stringify(r.value)}`;
  }
  return undefined;
}

/** The columns the rules replace, each once, in the order a rule first names it. */
export const replacedColumns = (d: DynamicReplace): string[] => [...new Set(d.rules.map((r) => r.column))];

/** A rule's value written as a literal of its column's type (`type`): the cell's own spelling, refused in words where it
 *  is no value of that type -- never turned into one. */
function valueLiteral(value: string | number | boolean, type: string | undefined, n: number, column: string): ValueSpecification {
  if (type === undefined) throw new CubeRefusal(`the compiler has not typed '${column}' yet`);
  const says = `rule ${n}'s value ${JSON.stringify(value)} is no ${plainType(type)}, as '${column}' is`;
  if (isBoolean(type)) {
    if (typeof value === 'boolean') return lit.boolean(value);
    if (value === 'true' || value === 'false') return lit.boolean(value === 'true');
    throw new CubeRefusal(says);
  }
  // a text column takes a text, a number column a number or its digits, a date its text: each as the cell spells it
  if (familyOf(type) === 'text' && typeof value !== 'string') throw new CubeRefusal(says);
  if (familyOf(type) !== 'text' && typeof value === 'boolean') throw new CubeRefusal(says);
  try {
    return lit.of(plainType(type), typeof value === 'string' && familyOf(type) === 'numeric' ? value.trim() : value);
  } catch (e) {
    if (e instanceof ProtocolError) throw new CubeRefusal(says);
    throw e;
  }
}

/** Each rule's condition, and (as expressions) its value, as the compiler parsed them (`ruleText`): one parameter each. */
export interface ParsedRules {
  readonly conditions: readonly Lambda[];
  readonly values?: readonly Lambda[];
}

/** A parsed rule's expression over the row: its one parameter `$x`, its one expression. */
function over(l: Lambda, n: number, what: string): ValueSpecification {
  if (l.parameters.length !== 1 || l.parameters[0]!.name !== ROW || l.body.length !== 1) {
    throw new CubeRefusal(`rule ${n}'s ${what} is one expression over $${ROW}`);
  }
  return l.body[0]!;
}

/** The replaced column of `column`: its rules (each with its number) as the chain of `if`s, the column itself last. */
function replacedColumn(column: string, rules: readonly { readonly n: number; readonly when: ValueSpecification; readonly then: ValueSpecification }[]): DerivedColumn {
  let body: ValueSpecification = property(variable(ROW), column);
  for (const r of [...rules].reverse()) body = fn('if', r.when, lambda([], r.then), lambda([], body));
  return { name: `${column}${REPLACED_SUFFIX}`, lambda: lambda([ROW], body), kind: 'dimension' };
}

/**
 * The replaced columns a Dynamic Replace makes over the grid as it is (`s`), its rules parsed (`parsed`, in its rules'
 * order) -- for the compiler to type before they are set (dynamicReplaceProbe), and for its commands.
 */
export function replacedColumnsOf(s: CubeSnapshot, d: DynamicReplace, parsed: ParsedRules): DerivedColumn[] {
  const problem = dynamicReplaceProblem(s, d);
  if (problem !== undefined) throw new CubeRefusal(problem);
  if (parsed.conditions.length !== d.rules.length || (d.expressions && parsed.values?.length !== d.rules.length)) {
    throw new Error('a Dynamic Replace is given a parsed condition (and value) for each rule');
  }
  const types = new Map(rowColumns(s).map((c) => [c.name, c.type] as const));
  const ruled = d.rules.map((r, i) => ({
    column: r.column, n: i + 1, when: over(parsed.conditions[i]!, i + 1, 'condition'),
    then: d.expressions ? over(parsed.values![i]!, i + 1, 'value') : valueLiteral(r.value, types.get(r.column), i + 1, r.column),
  }));
  return replacedColumns(d).map((c) => replacedColumn(c, ruled.filter((r) => r.column === c)));
}

/** The grid with a Dynamic Replace's columns as they would be (`s` as it is), for the compiler to type before they are
 *  set: each replaced column is of its column's type (dynamicReplaceCommands' `types`). */
export function dynamicReplaceProbe(s: CubeSnapshot, made: readonly DerivedColumn[]): CubeSnapshot {
  const names = new Set(made.map((m) => m.name));
  return { ...s, derived: [...s.derived.filter((d) => !names.has(d.name)), ...made] };
}

/**
 * The commands that set a grid's values replaced (`made`: replacedColumnsOf's columns; `types`, the compiler's type of
 * each, as the probe typed it): each replaced column added -- or, replaced already, set again -- handed on in its
 * column's place, the column hidden; those of a column the rules name no more taken off, it shown again; one step.
 * Refused in words: a replaced column of another type than its column's, a name taken by a column not its own.
 */
export function dynamicReplaceCommands(s: CubeSnapshot, made: readonly DerivedColumn[], types: ReadonlyMap<string, string | undefined>): GridCommand[] {
  const columns = new Map(rowColumns(s).map((c) => [c.name, c.type] as const));
  const had = new Map(s.derived.flatMap((d) => {
    const r = replacedOf(d);
    return r === undefined ? [] : [[d.name, r.column] as const];
  }));
  const commands: GridCommand[] = [];
  for (const m of made) {
    const column = m.name.slice(0, -REPLACED_SUFFIX.length);
    const want = columns.get(column);
    const got = types.get(m.name);
    if (got === undefined || want === undefined || plainType(got) !== plainType(want)) {
      throw new CubeRefusal(`'${column}' replaced is ${got === undefined ? 'not typed' : plainType(got)}: its values are ${want === undefined ? 'not typed' : plainType(want)}, and so are its rules'`);
    }
    if (had.has(m.name)) {
      commands.push({ kind: 'editCalculated', name: m.name, level: 'row', column: m });
      continue;
    }
    if (columns.has(m.name) || (s.groupDerived ?? []).some((d) => d.name === m.name)) throw new CubeRefusal(`'${m.name}' is already a column`);
    commands.push({ kind: 'calculate', level: 'row', column: m });
  }
  // a column no rule names now: its replaced column taken off, the column shown in its place again
  const gone = [...had].filter(([name]) => !made.some((m) => m.name === name));
  if (gone.length > 0) {
    commands.push({ kind: 'configure', patch: { columns: Object.fromEntries(gone.flatMap(([name, column]) => [
      [column, { hidden: null }], [name, { displayName: null }],
    ])) } });
    for (const [name] of gone) commands.push({ kind: 'removeCalculated', name });
  }
  return [...commands, inPlaceCommand(made.map((m) => ({ column: m.name.slice(0, -REPLACED_SUFFIX.length), made: m.name })))];
}



/** A Dynamic Replace read from JSON (Python's): `{kind: 'dynamicReplace', rules: [{column, condition, value}], expressions?}`
 *  -- or refused in words. */
export function readDynamicReplace(raw: unknown): DynamicReplace {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'dynamicReplace' || !Array.isArray(o['rules'])) {
    throw new CubeRefusal(`a dynamic replace is {kind: 'dynamicReplace', rules: [{column, condition, value}], expressions}, not ${JSON.stringify(raw)}`);
  }
  const odd = Object.keys(o).filter((k) => !['kind', 'rules', 'expressions'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a dynamic replace takes no ${odd.join(', ')}`);
  const expressions = o['expressions'] ?? false;
  if (typeof expressions !== 'boolean') throw new CubeRefusal(`its expressions is true or false, not ${JSON.stringify(expressions)}`);
  const rules = (o['rules'] as unknown[]).map((r, i): ReplaceRule => {
    const x = r !== null && typeof r === 'object' && !Array.isArray(r) ? r as Record<string, unknown> : undefined;
    const n = i + 1;
    if (x === undefined) throw new CubeRefusal(`rule ${n} is {column, condition, value}, not ${JSON.stringify(r)}`);
    const extra = Object.keys(x).filter((k) => !['column', 'condition', 'value'].includes(k));
    if (extra.length > 0) throw new CubeRefusal(`rule ${n} takes no ${extra.join(', ')}`);
    const { column, condition, value } = x;
    if (typeof column !== 'string' || column.trim() === '') throw new CubeRefusal(`rule ${n} names no column: ${JSON.stringify(column ?? null)}`);
    if (typeof condition !== 'string' || condition.trim() === '') throw new CubeRefusal(`rule ${n} has no condition: ${JSON.stringify(condition ?? null)}`);
    if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') {
      throw new CubeRefusal(`rule ${n}'s value is a text, a number or true or false, not ${JSON.stringify(value ?? null)}`);
    }
    return { column: column.trim(), condition, value };
  });
  return { rules, expressions };
}
