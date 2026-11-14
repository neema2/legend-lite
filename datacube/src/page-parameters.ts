// A PAGE'S PARAMETERS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M8, F5; Alteryx's interface tools, an analytic app's inputs;
// docs/BI_AND_ETL_PLAN_2026_09_29.md B3's "parameters bound into queries"): named values of the page -- a number, a
// text, a day, true or false, or one of a list -- that any Pure a person writes on it reads as `$name` (a Custom Filter's
// condition, a Formula, a Pure box, a Dynamic Replace's rules). A parameter is bound where a query is sent, as the page's
// filters are (runner.ts QueryLens): each `$name` the query reads, free of a lambda of its own of that name, is put in
// as the parameter's literal -- a shape rewritten, its value the cell's spelling, nothing computed. So a grid exported
// alone, its undo and its copies are the grid alone, and a value changed runs every grid again with it.
//
// One rule for the tab (page-app.ts #lensOf) and Python's (headless lensOf). PURE: no DOM, no planner.

import { children, lit, ProtocolError, rewrite, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, hasTimeOfDay, isBoolean, isDay, isTemporal } from '../../engine-client/src/types.ts';
import type { QueryLens } from './runner.ts';
import { CubeRefusal } from './snapshot.ts';

/** The types a parameter is: the literals a person types. */
export const PARAMETER_TYPES = ['String', 'Integer', 'Float', 'Decimal', 'Boolean', 'StrictDate', 'DateTime'] as const;
export type ParameterType = typeof PARAMETER_TYPES[number];

/** A parameter's value as the document keeps it: a text, a number, or true or false -- a day and a time their text, a
 *  decimal its digits. */
export type ParameterValue = string | number | boolean;

/** One parameter of the page. */
export interface PageParameter {
  /** What Pure reads it as: `$name`. */
  readonly name: string;
  readonly type: ParameterType;
  readonly value: ParameterValue;
  /** The values it may take, a pick-list's (Alteryx's Drop Down); absent, any of its type. */
  readonly choices?: readonly ParameterValue[];
  /** What its input says it is; absent, its name. */
  readonly label?: string;
}

/** The names DataCube's own expressions give what they read -- a row (`$x`), a value (`$v`), a box's input (`$input`):
 *  a parameter of one of them would never be read. */
const RESERVED = new Set(['x', 'v', 'input', 'this']);

/** A value spelt as its type's literal: refused in words where it is no value of that type -- never turned into one. */
function literal(type: ParameterType, value: ParameterValue, says: string): ValueSpecification {
  if (isBoolean(type)) {
    if (typeof value !== 'boolean') throw new CubeRefusal(`${says} is true or false, not ${JSON.stringify(value)}`);
    return lit.boolean(value);
  }
  if (typeof value === 'boolean') throw new CubeRefusal(`${says} is a ${type}, not ${value}`);
  if (familyOf(type) === 'text') {
    if (typeof value !== 'string') throw new CubeRefusal(`${says} is a text, not ${JSON.stringify(value)}`);
    return lit.string(value);
  }
  if (isTemporal(type) && typeof value !== 'string') throw new CubeRefusal(`${says} is a ${type}'s text, not ${value}`);
  if (isDay(type) && !/^\d{4}-\d{2}-\d{2}$/.test(value as string)) throw new CubeRefusal(`${says} is a day, as 2024-01-31: not ${JSON.stringify(value)}`);
  if (hasTimeOfDay(type) && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?$/.test(value as string)) {
    throw new CubeRefusal(`${says} is a time, as 2024-01-31T09:30:00: not ${JSON.stringify(value)}`);
  }
  try {
    return lit.of(type, typeof value === 'string' ? value.trim() : value);
  } catch (e) {
    if (e instanceof ProtocolError) throw new CubeRefusal(`${says} is no ${type}: ${JSON.stringify(value)}`);
    throw e;
  }
}

/** A parameter's value as its literal (`$name` put in as this). */
export function parameterLiteral(p: PageParameter): ValueSpecification {
  return literal(p.type, p.value, `${p.name}'s value`);
}

/** What is wrong with a parameter among the page's others (`others`: every other one), in words, if anything. */
export function parameterProblem(p: PageParameter, others: readonly PageParameter[]): string | undefined {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(p.name)) return `a parameter's name is a word Pure reads as $name -- letters, digits and _ -- not ${JSON.stringify(p.name)}`;
  if (RESERVED.has(p.name)) return `'${p.name}' is what DataCube's own expressions call what they read: name the parameter another way`;
  if (others.some((o) => o.name === p.name)) return `the page has a parameter '${p.name}' already`;
  if (!(PARAMETER_TYPES as readonly string[]).includes(p.type)) return `a parameter is a ${PARAMETER_TYPES.join(', ')}, not ${JSON.stringify(p.type)}`;
  try {
    parameterLiteral(p);
    for (const [i, c] of (p.choices ?? []).entries()) literal(p.type, c, `${p.name}'s choice ${i + 1}`);
  } catch (e) {
    if (e instanceof CubeRefusal) return e.message;
    throw e;
  }
  const choices = p.choices;
  if (choices !== undefined) {
    if (choices.length === 0) return `${p.name}'s choices are none: give it some, or none at all for any value`;
    if (new Set(choices.map((c) => JSON.stringify(c))).size !== choices.length) return `${p.name}'s choices name one twice`;
    if (!choices.some((c) => c === p.value)) return `${p.name}'s value ${JSON.stringify(p.value)} is none of its choices`;
  }
  if (p.label !== undefined && p.label.trim() === '') return `${p.name}'s label is words, or none`;
  return undefined;
}

/** `node` with each `$name` of `values` it reads put in as its literal -- but under a lambda of a parameter of that name,
 *  which reads its own. */
function bound(node: ValueSpecification, values: ReadonlyMap<string, ValueSpecification>): ValueSpecification {
  return rewrite(node, (n) => {
    if (n._type === 'var') return values.get(n.name);
    if (n._type !== 'lambda' || !n.parameters.some((p) => values.has(p.name))) return undefined;
    const rest = new Map(values);
    for (const p of n.parameters) rest.delete(p.name);
    return { ...n, body: n.body.map((b) => (rest.size === 0 ? b : bound(b, rest))) };
  });
}

/** The page's parameters a query reads (`$name`, free of a lambda of its own of that name), in the page's order. */
export function parametersRead(query: Lambda, parameters: readonly PageParameter[]): string[] {
  const names = new Set(parameters.map((p) => p.name));
  const read = new Set<string>();
  const visit = (n: ValueSpecification, shadowed: ReadonlySet<string>): void => {
    if (n._type === 'var') {
      if (names.has(n.name) && !shadowed.has(n.name)) read.add(n.name);
      return;
    }
    if (n._type === 'lambda') {
      const inner = new Set([...shadowed, ...n.parameters.map((p) => p.name)]);
      for (const b of n.body) visit(b, inner);
      return;
    }
    for (const c of children(n)) visit(c, shadowed);
  };
  for (const b of query.body) visit(b, new Set(query.parameters.map((p) => p.name)));
  return parameters.map((p) => p.name).filter((p) => read.has(p));
}

/** What the page's parameters make of every query a grid sends: each it reads bound (undefined: the page has none). Its
 *  key, what a grid's lens is compared by: a value changed, every grid runs again. */
export function parametersLens(parameters: readonly PageParameter[]): { readonly lens?: QueryLens; readonly key: string } {
  if (parameters.length === 0) return { key: '' };
  const values = new Map(parameters.map((p) => [p.name, parameterLiteral(p)] as const));
  return {
    // a lambda's own parameters its own, never the page's: a stored one's too (a Pure box's `rows|$rows->limit(3)`, a
    // calculated column's `r|$r.amount > 1`), each bound by its own lambda (the roadmap audit of 2026-10-26)
    lens: (query) => {
      const free = new Map([...values].filter(([name]) => !query.parameters.some((p) => p.name === name)));
      return free.size === 0 ? query : { ...query, body: query.body.map((b) => bound(b, free)) };
    },
    key: JSON.stringify(parameters.map((p) => [p.name, p.type, p.value])),
  };
}

/**
 * `value` -- a source resolved to be opened, its inputs' copies and a Pure box's function -- with each lambda in it bound
 * (parametersLens): what a box's base is read as, its inputs' parameters with their values now (live-inputs.ts
 * resolvedSource). Anything else as it is.
 */
export function boundIn<T>(value: T, parameters: readonly PageParameter[]): T {
  const { lens } = parametersLens(parameters);
  if (lens === undefined) return value;
  const walk = (v: unknown): unknown => {
    if (Array.isArray(v)) return v.map(walk);
    if (v === null || typeof v !== 'object' || Object.getPrototypeOf(v) !== Object.prototype) return v;
    const o = v as Record<string, unknown>;
    if (o['_type'] === 'lambda') return lens(o as unknown as Lambda);
    return Object.fromEntries(Object.entries(o).map(([k, x]) => [k, walk(x)]));
  };
  return walk(value) as T;
}

/** Two lenses, `a` then `b`, as one; either absent, the other. */
export function bothLenses(a: QueryLens | undefined, b: QueryLens | undefined): QueryLens | undefined {
  return a === undefined ? b : b === undefined ? a : (query) => b(a(query));
}

/** A parameter in words, as its input says it: "min_notional = 1000". */
export const parameterWords = (p: PageParameter): string => `${p.label ?? p.name} = ${String(p.value)}`;

/** A page's parameters read from JSON (its document's, Python's): each `{name, type, value, choices?, label?}` -- or
 *  refused in words. */
export function readParameters(raw: unknown): PageParameter[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) throw new CubeRefusal(`a page's parameters are a list, not ${JSON.stringify(raw)}`);
  const out: PageParameter[] = [];
  for (const r of raw) {
    const p = readParameter(r);
    const problem = parameterProblem(p, out);
    if (problem !== undefined) throw new CubeRefusal(problem);
    out.push(p);
  }
  return out;
}

/** One parameter read from JSON, its fields judged by parameterProblem. */
export function readParameter(raw: unknown): PageParameter {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined) throw new CubeRefusal(`a parameter is {name, type, value}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => !['name', 'type', 'value', 'choices', 'label'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a parameter takes no ${odd.join(', ')}`);
  const { name, type, value, choices, label } = o;
  if (typeof name !== 'string') throw new CubeRefusal(`a parameter's name is a word, not ${JSON.stringify(name ?? null)}`);
  if (typeof type !== 'string' || !(PARAMETER_TYPES as readonly string[]).includes(type)) {
    throw new CubeRefusal(`${name} is a ${PARAMETER_TYPES.join(', ')}, not ${JSON.stringify(type ?? null)}`);
  }
  const isValue = (v: unknown): v is ParameterValue => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean';
  if (!isValue(value)) throw new CubeRefusal(`${name}'s value is a text, a number or true or false, not ${JSON.stringify(value ?? null)}`);
  if (choices !== undefined && choices !== null && (!Array.isArray(choices) || !choices.every(isValue))) {
    throw new CubeRefusal(`${name}'s choices are a list of its values, not ${JSON.stringify(choices)}`);
  }
  if (label !== undefined && label !== null && typeof label !== 'string') throw new CubeRefusal(`${name}'s label is words, not ${JSON.stringify(label)}`);
  return {
    name, type: type as ParameterType, value,
    ...(Array.isArray(choices) ? { choices: choices as ParameterValue[] } : {}),
    ...(typeof label === 'string' ? { label } : {}),
  };
}
