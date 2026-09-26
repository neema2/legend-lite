// What is inside a JSON column, and the calculated columns that reach it.
//
// A Variant column is declared SEMISTRUCTURED and nothing in the model
// says what its documents hold, so the shape is INFERRED from a sample
// of them -- fetched through the ordinary query path, which is why it
// works on every plane (in the tab, the warehouse, the engine) with no
// API of its own. The inferred type only chooses which `to(@T)` a
// generated column writes; the compiler then types the column, as it
// types any calculated column, so a wrong guess is a refusal or a
// visibly wrong column -- never a silently wrong one.
//
// Numbers are classified from their TEXT. `JSON.parse` makes `1` and
// `1.0` the same value and rounds integers past 2^53, so a sample read
// through it would call a float column an integer, or an id column a
// float. The reader below keeps each number's own spelling.

import { literal } from './serialize.ts';

// ---- reading -------------------------------------------------------

/** A JSON value, numbers kept as written. */
export type JsonNode =
  | { readonly t: 'object'; readonly entries: readonly [string, JsonNode][] }
  | { readonly t: 'array'; readonly items: readonly JsonNode[] }
  | { readonly t: 'number'; readonly text: string }
  | { readonly t: 'string'; readonly value: string }
  | { readonly t: 'boolean'; readonly value: boolean }
  | { readonly t: 'null' };

/** Parse one JSON document; throws on malformed text. */
export function parseJson(text: string): JsonNode {
  let i = 0;
  const ws = (): void => {
    while (i < text.length && ' \t\n\r'.includes(text[i]!)) i += 1;
  };
  const fail = (what: string): never => {
    throw new SyntaxError(`${what} at ${i} in JSON`);
  };
  const str = (): string => {
    // Delegate escapes to JSON.parse on the exact string token: strings
    // lose nothing through it, only numbers do.
    const start = i;
    i += 1;
    while (i < text.length && text[i] !== '"') i += text[i] === '\\' ? 2 : 1;
    if (i >= text.length) fail('unterminated string');
    i += 1;
    return JSON.parse(text.slice(start, i)) as string;
  };
  const value = (): JsonNode => {
    ws();
    const c = text[i];
    if (c === '{') {
      i += 1;
      const entries: [string, JsonNode][] = [];
      ws();
      if (text[i] === '}') { i += 1; return { t: 'object', entries }; }
      for (;;) {
        ws();
        if (text[i] !== '"') fail('expected a key');
        const k = str();
        ws();
        if (text[i] !== ':') fail('expected :');
        i += 1;
        entries.push([k, value()]);
        ws();
        if (text[i] === ',') { i += 1; continue; }
        if (text[i] === '}') { i += 1; return { t: 'object', entries }; }
        fail('expected , or }');
      }
    }
    if (c === '[') {
      i += 1;
      const items: JsonNode[] = [];
      ws();
      if (text[i] === ']') { i += 1; return { t: 'array', items }; }
      for (;;) {
        items.push(value());
        ws();
        if (text[i] === ',') { i += 1; continue; }
        if (text[i] === ']') { i += 1; return { t: 'array', items }; }
        fail('expected , or ]');
      }
    }
    if (c === '"') return { t: 'string', value: str() };
    const m = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(text.slice(i));
    if (m) { i += m[0].length; return { t: 'number', text: m[0] }; }
    for (const [word, node] of [['true', { t: 'boolean', value: true }],
      ['false', { t: 'boolean', value: false }], ['null', { t: 'null' }]] as const) {
      if (text.startsWith(word, i)) { i += word.length; return node; }
    }
    return fail('unexpected character');
  };
  const out = value();
  ws();
  if (i !== text.length) fail('trailing text');
  return out;
}

// ---- inferring -----------------------------------------------------

/** What a scalar looked like: the choice of `to(@T)`. */
export type ScalarKind =
  | 'integer' | 'float' | 'boolean' | 'date' | 'datetime' | 'text';

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const DATETIME =
  /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$/;

function scalarKind(n: JsonNode): ScalarKind | undefined {
  switch (n.t) {
    case 'number': return /^-?\d+$/.test(n.text) ? 'integer' : 'float';
    case 'boolean': return 'boolean';
    case 'string':
      return DATE.test(n.value) ? 'date'
        : DATETIME.test(n.value) ? 'datetime' : 'text';
    default: return undefined;
  }
}

/** The text a scalar is shown and compared by. */
function scalarText(n: JsonNode): string {
  switch (n.t) {
    case 'number': return n.text;
    case 'string': return n.value;
    case 'boolean': return String(n.value);
    default: return '';
  }
}

/** Everything seen at ONE position across the sample. */
export interface Shape {
  /** Documents that had this position at all (null included). */
  present: number;
  nulls: number;
  readonly scalars: Map<ScalarKind, number>;
  /** Distinct scalar values and how often, capped (see MAX_VALUES). */
  readonly values: Map<string, number>;
  objects: number;
  readonly fields: Map<string, Shape>;
  arrays: number;
  /** The shape of every element of every array seen here. */
  element?: Shape;
  minLength: number;
  maxLength: number;
}

const MAX_VALUES = 50;

function emptyShape(): Shape {
  return { present: 0, nulls: 0, scalars: new Map(), values: new Map(),
    objects: 0, fields: new Map(), arrays: 0,
    minLength: Infinity, maxLength: 0 };
}

function observe(shape: Shape, n: JsonNode): void {
  shape.present += 1;
  if (n.t === 'null') { shape.nulls += 1; return; }
  if (n.t === 'object') {
    shape.objects += 1;
    for (const [k, v] of n.entries) {
      let f = shape.fields.get(k);
      if (!f) { f = emptyShape(); shape.fields.set(k, f); }
      observe(f, v);
    }
    return;
  }
  if (n.t === 'array') {
    shape.arrays += 1;
    shape.minLength = Math.min(shape.minLength, n.items.length);
    shape.maxLength = Math.max(shape.maxLength, n.items.length);
    shape.element ??= emptyShape();
    for (const item of n.items) observe(shape.element, item);
    return;
  }
  const kind = scalarKind(n)!;
  shape.scalars.set(kind, (shape.scalars.get(kind) ?? 0) + 1);
  const text = scalarText(n);
  if (shape.values.has(text) || shape.values.size < MAX_VALUES) {
    shape.values.set(text, (shape.values.get(text) ?? 0) + 1);
  }
}

export interface Sample {
  /** The inferred shape of the column's documents. */
  readonly shape: Shape;
  /** Rows sampled, empty ones included. */
  readonly rows: number;
  /** Documents that could not be read as JSON. */
  readonly unreadable: number;
}

/** Infer a shape from a column's cells, as the grid receives them. */
export function inferShape(cells: readonly unknown[]): Sample {
  const shape = emptyShape();
  let unreadable = 0;
  for (const cell of cells) {
    if (cell === null || cell === undefined) continue;
    // A JSON column arrives as its text; an object is what a nested
    // column looks like when a driver has already decoded it.
    const text = typeof cell === 'string' ? cell : JSON.stringify(cell);
    try {
      observe(shape, parseJson(text));
    } catch {
      unreadable += 1;
    }
  }
  return { shape, rows: cells.length, unreadable };
}

/**
 * The one scalar type a position takes. Integers and floats together
 * are floats; dates and datetimes together are datetimes; anything
 * else mixed is text, which every value converts to.
 */
export function scalarTypeOf(shape: Shape): ScalarKind | undefined {
  const kinds = [...shape.scalars.keys()];
  if (kinds.length === 0) return undefined;
  if (kinds.length === 1) return kinds[0];
  const only = (...ks: ScalarKind[]) => kinds.every((k) => ks.includes(k));
  if (only('integer', 'float')) return 'float';
  if (only('date', 'datetime')) return 'datetime';
  return 'text';
}

// ---- what can be extracted -----------------------------------------

const PURE_TYPE: Record<ScalarKind, string> = {
  integer: 'Integer', float: 'Float', boolean: 'Boolean',
  date: 'StrictDate', datetime: 'DateTime', text: 'String',
};

/** One calculated column the picker can create. */
export interface Extraction {
  /** A default column name, e.g. `customer_tier`. */
  readonly name: string;
  /** What the picker shows for it. */
  readonly label: string;
  /** The row-stage expression body, `$x` the row. */
  readonly expression: string;
  /** The Pure type it will have. */
  readonly type: string;
  readonly kind: 'dimension' | 'measure';
}

/** A position in the documents, with what can be made of it. */
export interface Field {
  /** Keys from the column, e.g. ['customer', 'contact', 'email']. */
  readonly path: readonly string[];
  /** What was seen: 'text', 'integer', 'object', 'array of object'... */
  readonly description: string;
  /** Share of sampled documents that had it, 0..1. */
  readonly presence: number;
  readonly extractions: readonly Extraction[];
  readonly children: readonly Field[];
}

/** Pure to reach a path from the row: `$x.col->get('a')->get('b')`. */
function reach(columnRef: string, keys: readonly string[]): string {
  return keys.reduce((e, k) => `${e}->get(${literal(k)})`, columnRef);
}

function nameOf(parts: readonly string[]): string {
  return parts.join('_').replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/_+/g, '_').replace(/^_|_$/g, '') || 'value';
}

function describe(shape: Shape): string {
  const parts: string[] = [];
  const scalar = scalarTypeOf(shape);
  if (scalar) parts.push(scalar);
  if (shape.objects > 0) parts.push('object');
  if (shape.arrays > 0) {
    const el = shape.element;
    const inner = !el || el.present === el.nulls ? 'nothing'
      : describe(el);
    parts.push(`array of ${inner}`);
  }
  return parts.length > 0 ? parts.join(' or ') : 'always null';
}

/** The most frequent values of a scalar position, most frequent first. */
export function topValues(shape: Shape, n = 12): string[] {
  return [...shape.values.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, n).map(([v]) => v);
}

/** A literal of a scalar kind, for `contains`. */
function valueLiteral(kind: ScalarKind, text: string): string {
  return kind === 'integer' || kind === 'float' ? text
    : kind === 'boolean' ? text
    : literal(text);
}

/**
 * The fields of a column's documents, and what each can become.
 *
 * `columnRef` is how the row reaches the column (`$x.customer`). The
 * top level is the column itself: an object column lists its keys, an
 * array column offers its array extractions directly.
 */
export function fieldsOf(column: string, columnRef: string, sample: Sample): Field[] {
  const total = Math.max(1, sample.rows);
  const build = (shape: Shape, path: readonly string[], parentPresent: number): Field => {
    const expr = reach(columnRef, path);
    const names = [column, ...path];
    const extractions: Extraction[] = [];
    const scalar = scalarTypeOf(shape);
    if (scalar && shape.objects === 0 && shape.arrays === 0) {
      extractions.push({
        name: nameOf(names), label: `as ${PURE_TYPE[scalar]}`,
        expression: `${expr}->to(@${PURE_TYPE[scalar]})`, type: PURE_TYPE[scalar],
        kind: scalar === 'float' ? 'measure' : 'dimension',
      });
    }
    if (shape.arrays > 0) extractions.push(...arrayExtractions(shape, expr, names));
    const children = shape.objects > 0
      ? [...shape.fields.entries()]
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([k, f]) => build(f, [...path, k], shape.objects))
      : [];
    return {
      path, description: describe(shape),
      presence: Math.min(1, (shape.present - shape.nulls) / Math.max(1, parentPresent)),
      extractions, children,
    };
  };
  return [build(sample.shape, [], total)];
}

function arrayExtractions(shape: Shape, expr: string, names: readonly string[]): Extraction[] {
  const out: Extraction[] = [];
  const many = `${expr}->toMany(@Variant)`;
  out.push({ name: nameOf([...names, 'count']), label: 'number of elements',
    expression: `${many}->size()`, type: 'Integer', kind: 'measure' });
  const el = shape.element;
  if (!el) return out;
  const elScalar = scalarTypeOf(el);
  if (elScalar && el.objects === 0 && el.arrays === 0) {
    const t = PURE_TYPE[elScalar];
    out.push({ name: nameOf([...names, 'list']), label: 'all values, as text',
      expression: `${expr}->toMany(@String)->joinStrings(', ')`,
      type: 'String', kind: 'dimension' });
    // "contains" compares a typed-in literal: text, numbers, booleans.
    const comparable = elScalar !== 'date' && elScalar !== 'datetime';
    for (const v of comparable ? topValues(el, 8) : []) {
      out.push({ name: nameOf([...names, 'has', v]), label: `contains ${v}`,
        expression: `${expr}->toMany(@${t})->contains(${valueLiteral(elScalar, v)})`,
        type: 'Boolean', kind: 'dimension' });
    }
  }
  if (el.objects > 0) {
    for (const [k, f] of [...el.fields.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const s = scalarTypeOf(f);
      if (!s || f.objects > 0 || f.arrays > 0) continue;
      const t = PURE_TYPE[s];
      const key = literal(k);
      out.push({ name: nameOf([...names, k, 'list']), label: `every ${k}, as text`,
        expression: `${many}->map(e | $e->get(${key})->to(@String)->toOne())->joinStrings(', ')`,
        type: 'String', kind: 'dimension' });
      out.push({ name: nameOf([...names, 'first', k]), label: `first element's ${k}`,
        expression: `${expr}->get(0)->get(${key})->to(@${t})`,
        type: t, kind: s === 'float' ? 'measure' : 'dimension' });
      if (s === 'integer' || s === 'float') {
        out.push({ name: nameOf([...names, k, 'total']), label: `total of ${k}`,
          expression: `${many}->map(e | $e->get(${key})->to(@${t})->toOne())->sum()`,
          type: t, kind: 'measure' });
      }
    }
  }
  return out;
}
